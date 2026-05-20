# Runbook opérationnel — Pipeline TMDB

Référentiel exhaustif des **commandes, opérations et procédures de debug** pour le projet. Sert à :
- **reproduire le projet** à partir de zéro (section 3)
- **exploiter** la pipeline au quotidien (section 4)
- **inspecter** les données et métriques (section 6)
- **diagnostiquer** un incident (section 8)

Pour la vue conceptuelle (architecture, décisions, modèle de données), voir [`doc.md`](doc.md).
Pour la démo soutenance, voir [`notice_démo.md`](notice_démo.md).

---

## Table des matières

1. [Référence rapide](#1-référence-rapide)
2. [Variables d'environnement](#2-variables-denvironnement)
3. [Setup initial (one-time)](#3-setup-initial-one-time)
4. [Exploitation courante](#4-exploitation-courante)
5. [Commandes par phase](#5-commandes-par-phase)
6. [Inspection des données par couche](#6-inspection-des-données-par-couche)
7. [Reset & idempotence](#7-reset--idempotence)
8. [Debug & troubleshooting](#8-debug--troubleshooting)
9. [CI GitHub Actions](#9-ci-github-actions)
10. [Exécution manuelle bout-en-bout (sans Airflow)](#10-exécution-manuelle-bout-en-bout-sans-airflow)

---

## 1. Référence rapide

### 1.1 Services & endpoints

| Service | URL | Credentials | Rôle |
|---|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin | Orchestration |
| MinIO console | http://localhost:9001 | minioadmin / minioadmin | Data Lake |
| MinIO API S3 | http://localhost:9000 | minioadmin / minioadmin | S3-compatible |
| Grafana | http://localhost:3000 | admin / admin | Dashboards |
| Prometheus | http://localhost:9090 | — | Métriques TSDB |
| statsd-exporter | http://localhost:9102/metrics | — | Métriques brutes |
| Snowsight | https://app.snowflake.com | (compte perso) | Warehouse UI |
| GitHub repo | https://github.com/Mael8zinsou/tmdb-data-pipeline | — | Code source |

### 1.2 Containers de la stack (`docker compose ps`)

```
finalpipelinev1-postgres-1            : Airflow metadata DB
finalpipelinev1-minio-1               : Data Lake (S3-compatible)
finalpipelinev1-airflow-webserver-1   : Airflow UI (port 8080)
finalpipelinev1-airflow-scheduler-1   : Orchestrateur + spawn Spark
finalpipelinev1-statsd-exporter-1     : Métriques Airflow → Prom
finalpipelinev1-prometheus-1          : TSDB + scrape (port 9090)
finalpipelinev1-grafana-1             : Dashboards (port 3000)
```

+ container éphémère `apache/spark:3.5.1-python3` spawné par Airflow pendant `spark_staging` et `spark_curated`.

### 1.3 Réseau Docker

`finalpipelinev1_default` — auto-créé par Docker Compose à partir du nom du dossier projet.

---

## 2. Variables d'environnement

Fichier `.env` (à créer à partir de `.env.example`, **jamais committé**) :

```bash
# ─── TMDB ─────────────────────────────────────────────────────────────────────
TMDB_API_KEY=<clé_TMDB>
TMDB_MAX_PAGES=3                              # 3 = 60 films démo, 500 max

# ─── MinIO (Data Lake local) ──────────────────────────────────────────────────
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=tmdb-lake

# ─── Airflow → spawn de containers Spark ──────────────────────────────────────
PROJECT_HOST_PATH=C:/Users/<vous>/.../Final pipeline v1    # Windows : forward slashes
DOCKER_NETWORK=finalpipelinev1_default

# ─── Snowflake (auth par paire de clés RSA) ───────────────────────────────────
SNOWFLAKE_ACCOUNT=<account_locator>
SNOWFLAKE_USER=<user>
SNOWFLAKE_PRIVATE_KEY_PATH=config/keys/snowflake_rsa_key.p8
SNOWFLAKE_DATABASE=TMDB_DW
SNOWFLAKE_SCHEMA=RAW
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_ROLE=PIPELINE_ROLE
```

> ℹ️ `PROJECT_HOST_PATH` doit utiliser des **forward slashes** (`C:/Users/...`) sur Windows, pas des backslashes ni le format Git-Bash `/c/Users/...`. Voir [§8.2](#82-airflow--dag).

> ℹ️ La connection à MinIO se fait via `http://minio:9000` depuis les containers, et `http://localhost:9000` depuis l'hôte. Le code Python lit `MINIO_ENDPOINT` (non listé ici car défini dans `docker-compose.yml` côté Airflow, et exporté à la main si run hors Airflow).

---

## 3. Setup initial (one-time)

### 3.1 Prérequis

- Docker Desktop ≥ 4.x (avec socket exposé, activé par défaut)
- Python 3.10+ (pour scripts manuels hors container)
- `gh` CLI (pour Phase 7)
- OpenSSL (pour génération RSA)
- Compte Snowflake trial actif (sinon `snowflake.com/start`)
- Clé API TMDB (sinon `themoviedb.org/settings/api`)

### 3.2 Configuration `.env`

```bash
cp .env.example .env
# Éditer .env avec tes valeurs réelles (voir §2)
```

### 3.3 Génération de la paire RSA Snowflake

```bash
mkdir -p config/keys

# Clé privée PKCS8 non chiffrée (format attendu par snowflake-connector-python)
openssl genrsa 2048 \
  | openssl pkcs8 -topk8 -inform PEM -out config/keys/snowflake_rsa_key.p8 -nocrypt

# Clé publique correspondante
openssl rsa -in config/keys/snowflake_rsa_key.p8 -pubout \
  -out config/keys/snowflake_rsa_key.pub

# Récupérer la clé publique sans les lignes BEGIN/END (pour ALTER USER)
grep -v "PUBLIC KEY" config/keys/snowflake_rsa_key.pub | tr -d '\n'

# Calculer le fingerprint local (doit matcher RSA_PUBLIC_KEY_FP côté Snowflake)
openssl rsa -in config/keys/snowflake_rsa_key.p8 -pubout -outform DER 2>/dev/null \
  | openssl dgst -sha256 -binary | openssl enc -base64
```

### 3.4 Setup SQL Snowflake (Snowsight, rôle ACCOUNTADMIN)

```sql
USE ROLE ACCOUNTADMIN;

-- ─── Warehouse ──────────────────────────────────────────────────────────────
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
  WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60
       AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;

-- ─── Base + schémas medallion ───────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS TMDB_DW;
USE DATABASE TMDB_DW;
CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS MARTS;

-- ─── Rôle dédié + permissions ───────────────────────────────────────────────
CREATE ROLE IF NOT EXISTS PIPELINE_ROLE;
GRANT ROLE PIPELINE_ROLE TO ROLE SYSADMIN;
GRANT USAGE, OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE PIPELINE_ROLE;
GRANT USAGE ON DATABASE TMDB_DW TO ROLE PIPELINE_ROLE;
GRANT USAGE ON ALL SCHEMAS IN DATABASE TMDB_DW TO ROLE PIPELINE_ROLE;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE TMDB_DW TO ROLE PIPELINE_ROLE;
GRANT CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT
  ON ALL SCHEMAS IN DATABASE TMDB_DW TO ROLE PIPELINE_ROLE;
GRANT CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT
  ON FUTURE SCHEMAS IN DATABASE TMDB_DW TO ROLE PIPELINE_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
  ON ALL TABLES IN DATABASE TMDB_DW TO ROLE PIPELINE_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
  ON FUTURE TABLES IN DATABASE TMDB_DW TO ROLE PIPELINE_ROLE;
GRANT USAGE ON ALL STAGES IN DATABASE TMDB_DW TO ROLE PIPELINE_ROLE;
GRANT USAGE ON FUTURE STAGES IN DATABASE TMDB_DW TO ROLE PIPELINE_ROLE;
GRANT USAGE ON ALL FILE FORMATS IN DATABASE TMDB_DW TO ROLE PIPELINE_ROLE;
GRANT USAGE ON FUTURE FILE FORMATS IN DATABASE TMDB_DW TO ROLE PIPELINE_ROLE;

-- ─── Attribution rôle + défauts user ────────────────────────────────────────
GRANT ROLE PIPELINE_ROLE TO USER <USER>;
ALTER USER <USER> SET
  DEFAULT_ROLE = PIPELINE_ROLE
  DEFAULT_WAREHOUSE = COMPUTE_WH
  DEFAULT_NAMESPACE = TMDB_DW.RAW;

-- ─── Enrôlement de la clé publique RSA ──────────────────────────────────────
ALTER USER <USER> SET RSA_PUBLIC_KEY='<contenu_clé_pub_sans_BEGIN_END>';
DESC USER <USER>;   -- vérifier RSA_PUBLIC_KEY_FP
```

### 3.5 Premier démarrage de la stack

```bash
# Build de l'image Airflow custom (Dockerfile.airflow)
docker compose build

# Démarrage des 7 services (postgres → minio → airflow-init → reste)
docker compose up -d

# Attendre que l'init Airflow termine (~30 sec)
docker compose logs airflow-init | tail -20
# → "User 'admin' created with role 'Admin'"

# Vérifier que tout est up
docker compose ps
```

---

## 4. Exploitation courante

### 4.1 Démarrer / arrêter la stack

```bash
# Démarrer tous les services (idempotent)
docker compose up -d

# Arrêter sans détruire les volumes (données persistées)
docker compose stop

# Arrêter et supprimer les containers (volumes préservés)
docker compose down

# Tout supprimer y compris les volumes (DANGER : perte des données)
docker compose down -v
```

### 4.2 Vérifier l'état de la stack

```bash
# Status containers (attendu : 7 services Up)
docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"

# Healthchecks
curl -s http://localhost:8080/health | python -m json.tool          # Airflow
curl -s -u admin:admin http://localhost:3000/api/health             # Grafana
curl -s http://localhost:9090/-/healthy                             # Prometheus
curl -s http://localhost:9000/minio/health/live                     # MinIO

# Logs d'un service
docker compose logs -f --tail=50 airflow-scheduler
```

### 4.3 Déclencher la pipeline via l'UI Airflow

1. http://localhost:8080 → login `admin/admin`
2. Activer `tmdb_pipeline` (toggle ON)
3. ▶ Trigger DAG

### 4.4 Déclencher la pipeline via CLI

```bash
# Unpause + trigger
docker exec finalpipelinev1-airflow-scheduler-1 \
  airflow dags unpause tmdb_pipeline
docker exec finalpipelinev1-airflow-scheduler-1 \
  airflow dags trigger tmdb_pipeline

# Lister les runs récents
docker exec finalpipelinev1-airflow-scheduler-1 \
  airflow dags list-runs -d tmdb_pipeline --no-backfill

# Lister les états des tasks d'un run
docker exec finalpipelinev1-airflow-scheduler-1 \
  airflow tasks states-for-dag-run tmdb_pipeline <run_id>
```

> ⚠️ **`airflow dags test` ≠ trigger via UI** : la commande `dags test` bypasse l'executor et n'hérite pas de toutes les env vars du container. Pour valider un DAG, **toujours trigger via UI ou `dags trigger`** (qui passe par le scheduler).

### 4.5 Surveiller un run en cours

```bash
# Vue UI : http://localhost:8080 → tmdb_pipeline → Graph (rafraîchissement auto)
# Vue Grafana temps réel : http://localhost:3000 → "TMDB Pipeline — Monitoring"

# Vue CLI : poll des états
while sleep 5; do
  docker exec finalpipelinev1-airflow-scheduler-1 \
    airflow tasks states-for-dag-run tmdb_pipeline <run_id>
done
```

### 4.6 Inspecter les logs d'une task

```bash
# Via UI : DAG → run → task → bouton "Log"

# Via CLI : récupérer le fichier de log directement
docker exec finalpipelinev1-airflow-scheduler-1 \
  cat /opt/airflow/logs/dag_id=tmdb_pipeline/run_id=<run_id>/task_id=spark_staging/attempt=1.log
```

---

## 5. Commandes par phase

Cette section conserve la trace historique de chaque phase : commandes utilisées, erreurs rencontrées et fixes appliqués.

### 5.1 Phase 1 — Ingestion TMDB

**Lignes directrices** : pagination `/discover/movie`, retry exponentiel (`requests.Session` + `Retry(total=5, backoff_factor=1.5)`), Parquet snappy partitionné par `ingestion_date`, upload via `boto3`.

```bash
# Démarrer juste MinIO + bucket auto-créé
docker compose up -d minio minio-init

# Vérifier création bucket
docker logs finalpipelinev1-minio-init-1
# → "Bucket created successfully `local/tmdb-lake`"

# Test extraction manuelle (3 pages = 60 films)
set -a && source .env && set +a
export MINIO_ENDPOINT=http://localhost:9000
export TMDB_MAX_PAGES=3
python ingestion/extract_tmdb.py
```

**Erreurs & fixes** :

| Erreur | Cause | Fix |
|---|---|---|
| `the attribute 'version' is obsolete` (Compose) | Compose v2+ ignore l'ancienne syntaxe `version: "3.9"` | Suppression de la ligne dans `docker-compose.yml` |
| `RemoteDisconnected` sur TMDB | Drops intermittents côté API | `requests.Session` + `Retry(total=5, backoff_factor=1.5)` + boucle interne 4 essais |

**Sortie validée** (3 pages) :

| Dataset | Lignes | Taille |
|---|---:|---:|
| `raw/movies/` | 60 | 30 KB |
| `raw/genres/` | 19 | 2.5 KB |
| `raw/countries/` | 251 | 9.7 KB |
| `raw/languages/` | 187 | 6.9 KB |

### 5.2 Phase 2 — Spark Staging

**Lignes directrices** : PySpark 3.5.1 + connecteur S3A (`hadoop-aws:3.3.4` + `aws-java-sdk-bundle:1.12.262`). Container Docker `apache/spark:3.5.1-python3` avec `--user 0` (root, pour accès au cache Ivy).

```bash
# Exécution manuelle (hors Airflow)
docker run --rm --user 0 --network finalpipelinev1_default \
  -v "C:/Users/maelz/Downloads/Documents/Cours_Documents_YNOV/Stock et Traitement de Data/Final pipeline v1:/app" \
  -w /app \
  -e MINIO_ENDPOINT=http://minio:9000 \
  -e MINIO_ACCESS_KEY=minioadmin -e MINIO_SECRET_KEY=minioadmin \
  -e MINIO_BUCKET=tmdb-lake \
  -e INGESTION_DATE=2026-05-19 \
  -e PYTHONPATH=/app \
  apache/spark:3.5.1-python3 \
  /opt/spark/bin/spark-submit \
    --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    spark/staging.py
```

> Sur Git Bash, préfixer la commande avec `MSYS_NO_PATHCONV=1` pour empêcher la conversion automatique de `/app` en `C:/Program Files/Git/app`.

**Erreurs & fixes** :

| Erreur | Cause | Fix |
|---|---|---|
| `HADOOP_HOME and hadoop.home.dir are unset` | Spark sous Windows natif (besoin de `winutils.exe`) | Bascule via Docker `apache/spark:3.5.1-python3` |
| `bitnami/spark:3.5.1 not found` | Image disparue de Docker Hub | Switch sur `apache/spark:3.5.1-python3` (officielle) |
| `/home/spark/.ivy2/cache : No such file or directory` | User `spark` non-root, pas de droit cache | Ajout de `--user 0` |
| Git Bash convertit `/app` en `C:/Program Files/Git/app` | Auto-conversion MSYS | Préfixer avec `MSYS_NO_PATHCONV=1` |

**Sortie validée** : `staging/{movies,genres,countries,languages}/` → mêmes counts que RAW (aucune perte).

### 5.3 Phase 3 — Spark Curated

**Lignes directrices** : 5 datasets produits, jointures sur référentiels, pont N-N films/genres via `explode_outer(genre_ids)`, colonnes dérivées (`release_decade`, `popularity_tier`, `vote_tier`, `has_release_date`).

```bash
# Même pattern Docker que Phase 2, juste changer le script
docker run --rm --user 0 --network finalpipelinev1_default \
  -v "${PROJECT_HOST_PATH}:/app" -w /app \
  -e MINIO_ENDPOINT=http://minio:9000 \
  -e MINIO_ACCESS_KEY=$MINIO_ACCESS_KEY \
  -e MINIO_SECRET_KEY=$MINIO_SECRET_KEY \
  -e MINIO_BUCKET=$MINIO_BUCKET \
  -e INGESTION_DATE=2026-05-19 -e PYTHONPATH=/app \
  apache/spark:3.5.1-python3 \
  /opt/spark/bin/spark-submit \
    --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    spark/curated.py
```

**Décision documentée** : **budget/revenue absents** de `/discover/movie`. Inclus nécessiterait `/movie/{id}` × N films. Reporté post-soutenance.

**Sortie validée** :

| Dataset | Lignes |
|---|---:|
| `curated/dim_genre/` | 19 |
| `curated/dim_country/` | 251 |
| `curated/dim_language/` | 187 |
| `curated/movies_enriched/` | 60 |
| `curated/movie_genres/` | 161 (relation N-N explosée) |

### 5.4 Phase 4 — Snowflake (COPY INTO)

**Lignes directrices** : auth par paire de clés RSA (PKCS8 unencrypted), internal stage (MinIO local inaccessible depuis Snowflake Cloud), `COPY INTO` avec `MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE`, `CREATE OR REPLACE` + `TRUNCATE` pour idempotence.

```bash
# Exécution manuelle
set -a && source .env && set +a
export MINIO_ENDPOINT=http://localhost:9000
python snowflake_load/load.py
```

**Erreurs & fixes** :

| Erreur | Cause | Fix |
|---|---|---|
| `MFA is required for this account` | Snowflake trial force la MFA | Bascule auth password → paire de clés RSA |
| `JWT token is invalid` (1er essai) | Cache de session côté Snowflake | Réessai après quelques secondes |
| `ModuleNotFoundError: snowflake.connector` | Dossier local `snowflake/` shadow le package PyPI | Renommage `snowflake/` → `snowflake_load/` |
| `Insufficient privileges` (DBT, voir Phase 5) | Pas de `CREATE SCHEMA` sur futur schémas | Pré-création des schémas + macro `generate_schema_name` |

**Sortie validée** :

| Table | Lignes |
|---|---:|
| `RAW.DIM_GENRE` | 19 |
| `RAW.DIM_COUNTRY` | 251 |
| `RAW.DIM_LANGUAGE` | 187 |
| `RAW.MOVIES_ENRICHED` | 60 |
| `RAW.MOVIE_GENRES` | 161 |

### 5.5 Phase 5 — DBT (Star Schema)

**Lignes directrices** : 3 couches medallion (staging → intermediate → marts), 12 modèles, 51 tests (`unique`, `not_null`, `accepted_values`, `relationships`, `dbt_utils.unique_combination_of_columns`), auth via clé RSA réutilisée du `.env`.

```bash
# Installation locale
pip install 'dbt-core==1.8.3' 'dbt-snowflake==1.8.3'

# Setup env
set -a && source .env && set +a
export DBT_PROFILES_DIR=./dbt

# Workflow standard
dbt deps --project-dir dbt          # installe dbt_utils
dbt debug --project-dir dbt         # vérifie la connexion Snowflake
dbt build --project-dir dbt         # run + test sur tous les modèles
dbt build --project-dir dbt --select dim_date+   # un modèle + ses descendants
```

**Erreurs & fixes** :

| Erreur | Cause | Fix |
|---|---|---|
| `python -m dbt deps` → `'dbt' is a package and cannot be directly executed` | dbt n'a pas de `__main__.py` | Utiliser le binaire `dbt` directement |
| `Insufficient privileges to operate on database 'TMDB_DW'` | DBT tente de créer `PUBLIC_staging` (préfixé) | Macro custom `generate_schema_name` → utilise `STAGING`/`MARTS` directement |
| `argument 1 to function GENERATOR needs to be constant` (dim_date) | Snowflake `GENERATOR(rowcount =>)` exige un littéral | Remplacement par `dbt_utils.date_spine` |
| Warning `tests config has been renamed to data_tests` | Deprecation DBT 1.8 | Acceptable, à migrer ultérieurement |

**Macro custom** (`dbt/macros/generate_schema_name.sql`) :

```jinja
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

**Sortie validée** : **63 / 63 PASS** (12 modèles + 51 tests).

| Modèle | Type | Lignes |
|---|---|---:|
| `fct_movies` | table | 60 |
| `dim_date` | table | 47 846 |
| `dim_genre` | table | 19 |
| `dim_country` | table | 251 |
| `dim_language` | table | 187 |
| `bridge_movie_genre` | table | 161 |

### 5.6 Phase 6 — DAG Airflow

**Lignes directrices** : image Airflow custom (`apache/airflow:2.9.1-python3.11` + `docker-ce-cli`), socket Docker monté + `user: "0:0"` pour spawn de containers Spark, 8 tasks (2 PythonOps + 6 BashOps), `PROJECT_HOST_PATH` format Windows obligatoire.

```bash
# Build de l'image custom (après modif Dockerfile.airflow)
docker compose build airflow-webserver

# Démarrage stack complète
docker compose up -d

# Forcer la recréation après modif .env ou image
docker compose up -d --force-recreate airflow-scheduler airflow-webserver

# Trigger (UI ou CLI, voir §4.3 et §4.4)

# Diagnostic accès Docker depuis scheduler
docker exec finalpipelinev1-airflow-scheduler-1 docker ps
```

**Erreurs & fixes** :

| Erreur | Cause | Fix |
|---|---|---|
| `Cannot install --user` dans Dockerfile | pip refuse `--user` quand root | Suppression du flag dans `pip install` |
| `docker.io package version 1.41 < 1.44 required` | Paquet Debian trop ancien | Remplacement par `docker-ce-cli` du dépôt officiel docker.com |
| `spark_staging` exit 125 (en CLI `dags test`) | `dags test` bypasse l'executor, env vars incomplètes | Utiliser UI ou `dags trigger` (passe par scheduler) |
| Volume mount `/c/Users/...` refusé par Docker daemon | Le daemon Docker Desktop attend `C:/...` côté Windows | `PROJECT_HOST_PATH=C:/Users/...` (forward slashes) dans `.env` |

**Validation finale** : DAG `tmdb_pipeline` déclenché via UI → toutes les 8 tasks ✅ en 5-7 min.

### 5.7 Phase 7 — GitHub + CI

**Lignes directrices** : repo public, `.gitignore` strict (secrets, artefacts DBT, clés RSA), `.gitattributes` LF cross-platform, CI minimaliste (ruff + DAG parse + dbt parse), ~1 min, pas de secrets en CI (dummy env vars).

```bash
# 1. Init repo local + sanity check
git init -b main
git add .
git status --short
git status --ignored --short | grep -E "\.env|/keys/"    # vérifier exclusions

# 2. Premier commit
git commit -m "Initial commit: TMDB data engineering pipeline"

# 3. Création GitHub via gh CLI
gh auth status
gh repo create tmdb-data-pipeline --public \
  --source=. --remote=origin \
  --description "Pipeline data engineering end-to-end TMDB → MinIO → Spark → Snowflake → DBT" \
  --push

# 4. Surveiller CI
gh run list --limit 3
gh run watch <RUN_ID> --exit-status
gh run view <RUN_ID> --log-failed                    # si échec
```

**Erreurs & fixes CI** :

| Erreur | Cause | Fix |
|---|---|---|
| `ruff F401: 'io' imported but unused` (load.py) | Import laissé après refactor | Suppression manuelle |
| `ruff F401: IntegerType, DateType` (staging.py) | Imports prévus puis non utilisés | Suppression manuelle |
| `KeyError: 'TMDB_API_KEY'` au DAG parse | `extract_tmdb.py` lit la clé au module-level | Injection de dummy env vars dans l'étape `Parse Airflow DAG` |
| `.claude/settings.local.json` stagé | Config user-local Claude Code | Ajout à `.gitignore` + `git rm --cached` |

**Structure de la CI** (`.github/workflows/ci.yml`) :

```yaml
1. Lint (ruff)         → ruff check --select E,F --ignore E501 sur src/
2. Parse Airflow DAG   → DagBag().import_errors == {} (avec env vars dummy)
3. Parse DBT           → dbt deps && dbt parse (avec env vars dummy)
```

### 5.8 Phase 8 — Monitoring (Prometheus + Grafana)

**Lignes directrices** : chaîne Airflow (StatsD UDP 9125) → statsd-exporter (HTTP 9102) → Prometheus → Grafana, auto-provisionnement datasource + dashboard 5 panels, retention Prometheus 7 jours.

```bash
# Démarrage : le monitoring fait partie de la stack normale
docker compose up -d

# Sanity check : métriques Airflow visibles dans statsd-exporter
curl -s http://localhost:9102/metrics | grep "^airflow_" | head -20

# Vérifier les targets scrapés par Prometheus
curl -s "http://localhost:9090/api/v1/query?query=up" | python -m json.tool

# Tester une query depuis l'extérieur
curl -sG "http://localhost:9090/api/v1/query" \
  --data-urlencode 'query=rate(airflow_scheduler_heartbeat[1m]) * 60'

# Vérifier provisioning Grafana
curl -s -u admin:admin http://localhost:3000/api/datasources | python -m json.tool
curl -s -u admin:admin http://localhost:3000/api/search?type=dash-db | python -m json.tool
```

**Erreurs & fixes** :

| Erreur | Cause | Fix |
|---|---|---|
| `rate(airflow_scheduler_heartbeat[1m])` retourne `[]` | Métrique réelle Airflow 2.9 = `airflow.scheduler_heartbeat` (underscore) | Mapping ajusté dans `statsd_mapping.yml` |
| Grafana ne charge pas le dashboard JSON | YAML provider et JSON dashboard dans le même dossier → Grafana scanne le YAML comme dashboard | Séparation `provisioning/dashboards/` (YAML) ↔ `dashboards/` (JSON) |
| Prometheus `rate(...)` vide après restart de statsd-exporter | Besoin de ≥ 2 samples scrapés (>15s) | Attendre ~30s |

**Validation** : `airflow_scheduler_heartbeat` rate ≈ 12/min, datasource + dashboard visibles dans l'API Grafana.

---

## 6. Inspection des données par couche

### 6.1 MinIO (Data Lake)

**Console web** : http://localhost:9001 → bucket `tmdb-lake`.

**CLI via `mc` (MinIO Client)** :

```bash
# Lister tout le bucket
docker run --rm --network finalpipelinev1_default --entrypoint sh minio/mc:latest \
  -c "mc alias set l http://minio:9000 minioadmin minioadmin && mc ls --recursive l/tmdb-lake"

# Lister une couche spécifique
docker run --rm --network finalpipelinev1_default --entrypoint sh minio/mc:latest \
  -c "mc alias set l http://minio:9000 minioadmin minioadmin && mc ls --recursive l/tmdb-lake/raw/"

# Compter les fichiers d'une partition
docker run --rm --network finalpipelinev1_default --entrypoint sh minio/mc:latest \
  -c "mc alias set l http://minio:9000 minioadmin minioadmin && \
      mc ls l/tmdb-lake/curated/movies_enriched/ingestion_date=2026-05-19/ | wc -l"

# Télécharger un Parquet pour inspection locale
docker run --rm --network finalpipelinev1_default \
  -v "$(pwd):/out" --entrypoint sh minio/mc:latest \
  -c "mc alias set l http://minio:9000 minioadmin minioadmin && \
      mc cp l/tmdb-lake/raw/movies/ingestion_date=2026-05-19/movies_part_0000.parquet /out/"

# Inspecter un Parquet avec Python
python -c "import pandas as pd; print(pd.read_parquet('movies_part_0000.parquet').head())"
```

### 6.2 Snowflake (Warehouse)

**Snowsight** : https://app.snowflake.com → rôle `PIPELINE_ROLE`, DB `TMDB_DW`.

**Row counts par couche** :

```sql
USE ROLE PIPELINE_ROLE;
USE DATABASE TMDB_DW;

-- RAW (peuplé par snowflake_load.py)
SELECT 'MOVIES_ENRICHED' AS table_name, COUNT(*) FROM RAW.MOVIES_ENRICHED
UNION ALL SELECT 'MOVIE_GENRES',         COUNT(*) FROM RAW.MOVIE_GENRES
UNION ALL SELECT 'DIM_GENRE',            COUNT(*) FROM RAW.DIM_GENRE
UNION ALL SELECT 'DIM_COUNTRY',          COUNT(*) FROM RAW.DIM_COUNTRY
UNION ALL SELECT 'DIM_LANGUAGE',         COUNT(*) FROM RAW.DIM_LANGUAGE;

-- MARTS (peuplé par DBT)
SELECT 'fct_movies' AS model, COUNT(*) FROM MARTS.fct_movies
UNION ALL SELECT 'dim_date',           COUNT(*) FROM MARTS.dim_date
UNION ALL SELECT 'dim_genre',          COUNT(*) FROM MARTS.dim_genre
UNION ALL SELECT 'dim_country',        COUNT(*) FROM MARTS.dim_country
UNION ALL SELECT 'dim_language',       COUNT(*) FROM MARTS.dim_language
UNION ALL SELECT 'bridge_movie_genre', COUNT(*) FROM MARTS.bridge_movie_genre;
```

**Query analytique d'exemple** (Top 5 genres par nombre de films + note moyenne) :

```sql
SELECT
    g.genre_name,
    COUNT(DISTINCT b.movie_id) AS nb_films,
    ROUND(AVG(f.vote_average), 2) AS note_moyenne
FROM MARTS.fct_movies f
JOIN MARTS.bridge_movie_genre b USING (movie_id)
JOIN MARTS.dim_genre g USING (genre_id)
GROUP BY g.genre_name
ORDER BY nb_films DESC
LIMIT 5;
```

**Inspection de l'internal stage** :

```sql
LIST @TMDB_DW.RAW.TMDB_STAGE;
```

### 6.3 Métriques (Prometheus / Grafana)

**Grafana** : http://localhost:3000 → dashboard "TMDB Pipeline — Monitoring".

**Prometheus** (queries directes) :

```bash
# Heartbeat actuel
curl -sG http://localhost:9090/api/v1/query \
  --data-urlencode 'query=rate(airflow_scheduler_heartbeat[1m]) * 60'

# Tasks running
curl -sG http://localhost:9090/api/v1/query \
  --data-urlencode 'query=airflow_executor_running_tasks'

# DAG runs succeeded sur 24h
curl -sG http://localhost:9090/api/v1/query \
  --data-urlencode 'query=sum(increase(airflow_dagrun_succeeded_total[24h]))'

# Durée par task (p99) du dernier run
curl -sG http://localhost:9090/api/v1/query \
  --data-urlencode 'query=airflow_task_duration_seconds{dag_id="tmdb_pipeline", quantile="0.99"}'

# Lister toutes les métriques disponibles
curl -s http://localhost:9090/api/v1/label/__name__/values | python -m json.tool | head -30
```

**statsd-exporter** (métriques brutes, utile pour debug du mapping) :

```bash
curl -s http://localhost:9102/metrics | grep "^airflow_" | head -30
curl -s http://localhost:9102/metrics | grep "airflow_unmapped"   # métriques non mappées
```

---

## 7. Reset & idempotence

### 7.1 Reset MinIO

```bash
# Vider tout le bucket
docker run --rm --network finalpipelinev1_default --entrypoint sh minio/mc:latest \
  -c "mc alias set l http://minio:9000 minioadmin minioadmin && \
      mc rm --recursive --force l/tmdb-lake/"

# Vider une partition spécifique
docker run --rm --network finalpipelinev1_default --entrypoint sh minio/mc:latest \
  -c "mc alias set l http://minio:9000 minioadmin minioadmin && \
      mc rm --recursive --force l/tmdb-lake/curated/movies_enriched/ingestion_date=2026-05-19/"
```

### 7.2 Reset Snowflake

```sql
USE ROLE PIPELINE_ROLE;
USE DATABASE TMDB_DW;

-- Vider les RAW (re-peuplées par snowflake_load.py)
TRUNCATE TABLE RAW.MOVIES_ENRICHED;
TRUNCATE TABLE RAW.MOVIE_GENRES;
TRUNCATE TABLE RAW.DIM_GENRE;
TRUNCATE TABLE RAW.DIM_COUNTRY;
TRUNCATE TABLE RAW.DIM_LANGUAGE;

-- Vider le stage interne (artefacts PUT précédents)
REMOVE @TMDB_DW.RAW.TMDB_STAGE;

-- Tout drop côté MARTS (re-créé par dbt build)
DROP SCHEMA IF EXISTS MARTS CASCADE;
CREATE SCHEMA MARTS;
DROP SCHEMA IF EXISTS STAGING CASCADE;
CREATE SCHEMA STAGING;
```

### 7.3 Reset DBT (artefacts locaux)

```bash
# Supprimer les artefacts de compilation/run
rm -rf dbt/target/
rm -rf dbt/dbt_packages/
rm -rf dbt/logs/

# Re-installer dbt_utils
dbt deps --project-dir dbt
```

### 7.4 Reset Prometheus / Grafana

```bash
# Supprimer les volumes (efface l'historique métriques + tout dashboard non provisionné)
docker compose stop prometheus grafana
docker volume rm finalpipelinev1_prometheus_data finalpipelinev1_grafana_data
docker compose up -d prometheus grafana
```

### 7.5 Reset complet (nuke and rebuild)

```bash
# DANGER : supprime tous les containers ET tous les volumes
docker compose down -v

# Rebuild de l'image custom (au cas où)
docker compose build --no-cache

# Restart from scratch
docker compose up -d
```

---

## 8. Debug & troubleshooting

### 8.1 Stack Docker

| Symptôme | Diagnostic | Fix |
|---|---|---|
| `docker compose up` ne termine pas | `docker compose logs airflow-init` | Si "waiting for postgres" : attendre 30s |
| Containers s'arrêtent en boucle | `docker compose logs <service>` | Lire l'erreur applicative, souvent env var manquante |
| Port déjà utilisé | `netstat -ano \| findstr "8080"` (Windows) | Tuer le process ou changer le port dans `docker-compose.yml` |
| Image custom obsolète | — | `docker compose build --no-cache airflow-webserver` |

### 8.2 Airflow / DAG

| Symptôme | Diagnostic | Fix |
|---|---|---|
| DAG n'apparaît pas dans l'UI | `docker exec airflow-scheduler airflow dags list-import-errors` | Corriger les imports / env vars manquantes |
| Task `spark_staging` exit 125 | Vérifier `PROJECT_HOST_PATH` dans `.env` | Format `C:/Users/...` (forward slashes Windows) |
| Task `spark_staging` ne trouve pas le socket Docker | `docker exec airflow-scheduler docker ps` | Si KO : vérifier mount `/var/run/docker.sock` dans `docker-compose.yml` |
| Task `snowflake_load` : `JWT token is invalid` | Cache de session côté Snowflake | Réessayer (Clear + retry dans l'UI) |
| DAG bloqué en "queued" | `docker compose logs airflow-scheduler \| tail -50` | Souvent : scheduler en train de parser, attendre 30s |

### 8.3 Snowflake / DBT

| Symptôme | Diagnostic | Fix |
|---|---|---|
| `dbt debug` échoue sur la connexion | Vérifier les env vars `SNOWFLAKE_*` | Re-source `.env`, vérifier fingerprint RSA |
| `Insufficient privileges` | Vérifier les grants sur `PIPELINE_ROLE` | Re-run les grants `ON FUTURE SCHEMAS/TABLES` |
| Test `relationships` échoue | Un FK pointe vers une PK manquante | Vérifier le mapping COPY INTO ou la jointure DBT |
| `dim_date` vide | dbt_utils non installé | `dbt deps --project-dir dbt` |

### 8.4 Monitoring

| Symptôme | Diagnostic | Fix |
|---|---|---|
| Grafana dashboard "No data" | `curl http://localhost:9102/metrics \| head` | Vérifier que statsd-exporter reçoit (sinon : vérifier env Airflow `AIRFLOW__METRICS__STATSD_*`) |
| Métrique manquante côté Prometheus | `curl http://localhost:9102/metrics \| grep <metric>` | Si présente dans statsd-exporter mais pas Prom : ajouter scrape config ou attendre 15s |
| Métrique apparaît dans `airflow_unmapped` | — | Ajouter une règle de mapping dans `statsd_mapping.yml` puis `docker compose restart statsd-exporter` |
| `rate()` retourne vide après restart | Besoin de ≥ 2 samples (>15s) | Attendre ~30s |

---

## 9. CI GitHub Actions

```bash
# Lister les runs récents
gh run list --limit 10

# Watch en temps réel
gh run watch <RUN_ID> --exit-status

# Voir les logs d'un job (utile pour les échecs)
gh run view <RUN_ID>
gh run view <RUN_ID> --log
gh run view <RUN_ID> --log-failed

# Re-déclencher manuellement un workflow
gh workflow run ci.yml

# Voir le statut du dernier run
gh run list --workflow=ci.yml --limit 1
```

**Workflow local pour tester avant push** :

```bash
# Lint (même règles que la CI)
pip install ruff==0.4.4
ruff check --select E,F --ignore E501 ingestion/ spark/ snowflake_load/ dags/

# Parse Airflow DAG (en local, sans CI)
export PROJECT_HOST_PATH=/tmp/ci-dummy
export TMDB_API_KEY=ci-dummy
# ... autres env vars dummy
python -c "from airflow.models import DagBag; \
  dagbag = DagBag(dag_folder='dags', include_examples=False); \
  print('OK' if not dagbag.import_errors else dagbag.import_errors)"

# Parse DBT
cd dbt && dbt parse
```

---

## 10. Exécution manuelle bout-en-bout (sans Airflow)

Utile pour debug isolé d'une étape, ou pour reproduire la pipeline hors orchestrateur.

```bash
# Pré-requis : MinIO + Snowflake up, .env configuré
set -a && source .env && set +a
export MINIO_ENDPOINT=http://localhost:9000
export TMDB_MAX_PAGES=3
export INGESTION_DATE=$(date +%Y-%m-%d)

# ─── 1. Ingestion (TMDB → MinIO) ──────────────────────────────────────────────
python ingestion/extract_tmdb.py

# ─── 2. Spark Staging (MinIO raw → MinIO staging) ─────────────────────────────
docker run --rm --user 0 --network finalpipelinev1_default \
  -v "${PROJECT_HOST_PATH}:/app" -w /app \
  -e MINIO_ENDPOINT=http://minio:9000 \
  -e MINIO_ACCESS_KEY=$MINIO_ACCESS_KEY \
  -e MINIO_SECRET_KEY=$MINIO_SECRET_KEY \
  -e MINIO_BUCKET=$MINIO_BUCKET \
  -e INGESTION_DATE=$INGESTION_DATE -e PYTHONPATH=/app \
  apache/spark:3.5.1-python3 \
  /opt/spark/bin/spark-submit \
    --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    spark/staging.py

# ─── 3. Spark Curated (MinIO staging → MinIO curated) ─────────────────────────
docker run --rm --user 0 --network finalpipelinev1_default \
  -v "${PROJECT_HOST_PATH}:/app" -w /app \
  -e MINIO_ENDPOINT=http://minio:9000 \
  -e MINIO_ACCESS_KEY=$MINIO_ACCESS_KEY \
  -e MINIO_SECRET_KEY=$MINIO_SECRET_KEY \
  -e MINIO_BUCKET=$MINIO_BUCKET \
  -e INGESTION_DATE=$INGESTION_DATE -e PYTHONPATH=/app \
  apache/spark:3.5.1-python3 \
  /opt/spark/bin/spark-submit \
    --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    spark/curated.py

# ─── 4. Snowflake load (MinIO curated → Snowflake RAW) ────────────────────────
python snowflake_load/load.py

# ─── 5. DBT (Snowflake RAW → STAGING → MARTS) ─────────────────────────────────
export DBT_PROFILES_DIR=./dbt
dbt deps --project-dir dbt
dbt build --project-dir dbt

# ─── 6. Vérification finale ───────────────────────────────────────────────────
# Snowsight :
#   SELECT COUNT(*) FROM TMDB_DW.MARTS.fct_movies;  -- attendu : 60
#   SELECT COUNT(*) FROM TMDB_DW.MARTS.bridge_movie_genre;  -- attendu : 161
```

---

**Dernière mise à jour :** 2026-05-19
**Statut :** runbook opérationnel pour Phases 1-8.
