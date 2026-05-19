# Cahier de Commandes & Décisions — Pipeline TMDB

Référentiel des commandes clés, erreurs rencontrées et ajustements opérés au fil des phases.
Sert de **runbook** pour reproduire le projet ou debugger.

---

## 🌐 Variables d'environnement requises (`.env`)

```bash
# TMDB
TMDB_API_KEY=<ta_clé_TMDB>

# MinIO (Data Lake local)
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=tmdb-lake

# Snowflake (auth par clé RSA, pas de password)
SNOWFLAKE_ACCOUNT=KHCLOMT-ZC06384
SNOWFLAKE_USER=MIAORGANA
SNOWFLAKE_PRIVATE_KEY_PATH=config/keys/snowflake_rsa_key.p8
SNOWFLAKE_DATABASE=TMDB_DW
SNOWFLAKE_SCHEMA=RAW
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_ROLE=PIPELINE_ROLE
```

---

## 📦 Phase 1 — Infrastructure & Ingestion

### Lignes directrices
- **Stack Docker** : Airflow + PostgreSQL (metadata) + MinIO (S3-compatible local)
- **Ingestion Python** : `requests` avec retry exponentiel + `pandas` + `boto3` + `pyarrow`
- **Pagination TMDB** : `/discover/movie` jusqu'à 500 pages (limite TMDB)
- **Format Parquet** snappy partitionné par `ingestion_date=YYYY-MM-DD`

### Commandes clés
```bash
# Démarrer MinIO + bucket auto
docker compose up -d minio minio-init

# Vérifier création bucket
docker logs finalpipelinev1-minio-init-1
# → "Bucket created successfully `local/tmdb-lake`"

# Test extraction (3 pages = 60 films)
set -a && source .env && set +a
export MINIO_ENDPOINT=http://localhost:9000
export TMDB_MAX_PAGES=3
python ingestion/extract_tmdb.py

# Vérifier les fichiers Parquet uploadés
docker run --rm --network finalpipelinev1_default --entrypoint sh minio/mc:latest \
  -c "mc alias set l http://minio:9000 minioadmin minioadmin && mc ls --recursive l/tmdb-lake"
```

### Erreurs & ajustements
| Erreur | Cause | Fix |
|---|---|---|
| `the attribute 'version' is obsolete` | Compose v2+ | Suppression de la ligne `version: "3.9"` du `docker-compose.yml` |
| `RemoteDisconnected` sur TMDB | Drops intermittents API | Ajout de `requests.Session` + `Retry` (5 tentatives, backoff 1.5×) + boucle interne 4 essais |

### Sortie validée (3 pages)
- `raw/movies/`     : 60 films (30 KB)
- `raw/genres/`     : 19 (2.5 KB)
- `raw/countries/`  : 251 (9.7 KB)
- `raw/languages/`  : 187 (6.9 KB)

---

## ⚙️ Phase 2 — Spark Staging

### Lignes directrices
- **PySpark 3.5.1** + connecteur S3A (`hadoop-aws:3.3.4` + `aws-java-sdk-bundle:1.12.262`)
- **Transformations movies** : typage, `to_date`, `trim`, drop colonnes inutiles, dropDuplicates(movie_id), fillna stratégique, ajout `release_year`
- **Transformations lookups** (genres / countries / languages) : dédup sur clé, filtrage NaN

### Commandes clés
```bash
# ❌ Spark sous Windows natif → KO (manque winutils.exe / HADOOP_HOME)
# ✅ Solution : exécution via container Docker

MSYS_NO_PATHCONV=1 docker run --rm --user 0 \
  --network finalpipelinev1_default \
  -v "/c/Users/maelz/Downloads/Documents/Cours_Documents_YNOV/Stock et Traitement de Data/Final pipeline v1:/app" \
  -w /app \
  -e MINIO_ENDPOINT=http://minio:9000 \
  -e MINIO_ACCESS_KEY=minioadmin -e MINIO_SECRET_KEY=minioadmin \
  -e MINIO_BUCKET=tmdb-lake \
  -e INGESTION_DATE=2026-04-27 \
  -e PYTHONPATH=/app \
  apache/spark:3.5.1-python3 \
  /opt/spark/bin/spark-submit \
    --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    spark/staging.py
```

### Erreurs & ajustements
| Erreur | Cause | Fix |
|---|---|---|
| `HADOOP_HOME and hadoop.home.dir are unset` | Spark sous Windows | Bascule via Docker `apache/spark:3.5.1-python3` |
| `bitnami/spark:3.5.1 not found` | Image disparue de Docker Hub | Switch sur `apache/spark:3.5.1-python3` (image officielle) |
| `/home/spark/.ivy2/cache : No such file or directory` | User `spark` non root, pas de droit cache | Ajout de `--user 0` |
| `MSYS_NO_PATHCONV=1` requis | Git Bash convertit `/app` en `C:/Program Files/Git/app` | Préfixe pour désactiver la conversion |

### Sortie validée
`staging/{movies,genres,countries,languages}/` → mêmes counts que RAW (aucune perte).

---

## 🔗 Phase 3 — Spark Curated

### Lignes directrices
- **5 datasets produits** : `dim_genre`, `dim_country`, `dim_language`, `movies_enriched`, `movie_genres`
- **Jointure langue** : `movies` ⋈ `dim_language` sur `original_language`
- **Pont N-N film/genre** : `explode_outer(genre_ids)` puis jointure libellé
- **Colonnes dérivées** : `release_decade`, `popularity_tier`, `vote_tier`, `has_release_date`

### Commande clé
```bash
# Même pattern Docker que Phase 2, juste changer le script
docker run ... apache/spark:3.5.1-python3 \
  /opt/spark/bin/spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  spark/curated.py
```

### Décision documentée
**Budget / revenue absents** de `/discover/movie` → ROI/profit non calculés ici.
Pour les inclure, il faudrait `/movie/{id}` (×500 calls par page) → reporté post-soutenance.

### Sortie validée
- `curated/dim_genre/`        : 19
- `curated/dim_country/`      : 251
- `curated/dim_language/`     : 187
- `curated/movies_enriched/`  : 60
- `curated/movie_genres/`     : 161 (relation N-N explosée)

---

## ❄️ Phase 4 — Snowflake (COPY INTO)

### Lignes directrices
- **Auth par paire de clés RSA** (PKCS8 unencrypted) au lieu de password (MFA obligatoire sur trial Snowflake)
- **Internal stage** (`TMDB_DW.RAW.TMDB_STAGE`) car MinIO local inaccessible depuis Snowflake Cloud
- **Stratégie** : Download MinIO → PUT vers stage → COPY INTO `MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE`
- **Tables** : `CREATE OR REPLACE` idempotent, `TRUNCATE` avant chaque chargement

### Setup Snowflake (Snowsight, en ACCOUNTADMIN)
```sql
USE ROLE ACCOUNTADMIN;

-- Warehouse
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
  WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;

-- DB + schémas medallion
CREATE DATABASE IF NOT EXISTS TMDB_DW;
USE DATABASE TMDB_DW;
CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS MARTS;

-- Rôle dédié
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

-- Attribution rôle + défauts user
GRANT ROLE PIPELINE_ROLE TO USER MIAORGANA;
ALTER USER MIAORGANA SET
  DEFAULT_ROLE = PIPELINE_ROLE
  DEFAULT_WAREHOUSE = COMPUTE_WH
  DEFAULT_NAMESPACE = TMDB_DW.RAW;
```

### Génération de la paire RSA + enrôlement
```bash
# Générer la paire (PKCS8 non chiffré, format attendu par Snowflake connector)
mkdir -p config/keys
openssl genrsa 2048 \
  | openssl pkcs8 -topk8 -inform PEM -out config/keys/snowflake_rsa_key.p8 -nocrypt
openssl rsa -in config/keys/snowflake_rsa_key.p8 -pubout -out config/keys/snowflake_rsa_key.pub

# Récupérer la clé publique sans BEGIN/END (format ALTER USER)
grep -v "PUBLIC KEY" config/keys/snowflake_rsa_key.pub | tr -d '\n'

# Vérifier le fingerprint local (doit matcher RSA_PUBLIC_KEY_FP côté Snowflake)
openssl rsa -in config/keys/snowflake_rsa_key.p8 -pubout -outform DER 2>/dev/null \
  | openssl dgst -sha256 -binary | openssl enc -base64
```

```sql
-- Dans Snowsight (en ACCOUNTADMIN)
ALTER USER MIAORGANA SET RSA_PUBLIC_KEY='<contenu_clé_publique_sans_BEGIN_END>';
DESC USER MIAORGANA;  -- vérifier RSA_PUBLIC_KEY_FP
```

### Commande d'exécution
```bash
set -a && source .env && set +a
export MINIO_ENDPOINT=http://localhost:9000
python snowflake_load/load.py
```

### Erreurs & ajustements
| Erreur | Cause | Fix |
|---|---|---|
| `MFA is required for this account` | Snowflake trial force MFA | Bascule auth password → clé RSA |
| `JWT token is invalid` (1er essai) | Propagation cache de session | Réessai après quelques secondes |
| `ModuleNotFoundError: snowflake.connector` initial | Dossier local `snowflake/` shadow le namespace pkg | Renommage `snowflake/` → `snowflake_load/` |
| `SQL access control error: Insufficient privileges` (DBT, plus tard) | Pas de `CREATE SCHEMA` | Pré-création schémas + macro `generate_schema_name` |

### Sortie validée
| Table | Lignes |
|---|---:|
| `RAW.DIM_GENRE`        | 19 |
| `RAW.DIM_COUNTRY`      | 251 |
| `RAW.DIM_LANGUAGE`     | 187 |
| `RAW.MOVIES_ENRICHED`  | 60 |
| `RAW.MOVIE_GENRES`     | 161 |

---

## 🏛️ Phase 5 — DBT (Star Schema)

### Lignes directrices
- **3 couches medallion** : `staging/` (views) → `intermediate/` (views) → `marts/` (tables)
- **Star schema** : `fct_movies` + dimensions + `bridge_movie_genre`
- **Tests** : unique, not_null, accepted_values, relationships, dbt_utils.unique_combination_of_columns
- **Auth DBT** : `private_key_path` réutilise la clé RSA du `.env`

### Commandes clés
```bash
# Installation
pip install 'dbt-core==1.8.3' 'dbt-snowflake==1.8.3'

# Setup env DBT (profiles.yml dans ./dbt)
set -a && source .env && set +a
export DBT_PROFILES_DIR=./dbt

# Installer dbt_utils
dbt deps --project-dir dbt

# Vérifier connexion
dbt debug --project-dir dbt

# Build complet (modèles + tests)
dbt build --project-dir dbt

# Build sélectif (un modèle + ses descendants)
dbt build --project-dir dbt --select dim_date+
```

### Erreurs & ajustements
| Erreur | Cause | Fix |
|---|---|---|
| `python -m dbt deps` → `'dbt' is a package and cannot be directly executed` | dbt n'a pas de `__main__.py` | Utiliser le binaire `dbt` directement (pas `python -m dbt`) |
| `Insufficient privileges to operate on database 'TMDB_DW'` | DBT tente de créer schéma `PUBLIC_staging` | Override `generate_schema_name` → utilise directement `STAGING` / `MARTS` |
| `argument 1 to function GENERATOR needs to be constant` (dim_date) | Snowflake `GENERATOR(rowcount =>)` exige littéral | Remplacement par `dbt_utils.date_spine` |
| Warning `tests config has been renamed to data_tests` | DBT 1.8 deprecation | Acceptable, à migrer plus tard (pas bloquant) |

### Macro custom : `dbt/macros/generate_schema_name.sql`
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

### Sortie validée
**63/63 PASS** (12 modèles + 51 tests).

| Modèle | Type | Lignes |
|---|---|---:|
| `fct_movies` | table | 60 |
| `dim_date` | table | 47 846 |
| `dim_genre` | table | 19 |
| `dim_country` | table | 251 |
| `dim_language` | table | 187 |
| `bridge_movie_genre` | table | 161 |

---

## 🚀 Phase 6 — DAG Airflow (Orchestration complète)

### Lignes directrices
- **Image Airflow custom** : `Dockerfile.airflow` basée sur `apache/airflow:2.9.1-python3.11`
- **docker-ce-cli** installé dans l'image (dépôt officiel docker.com) pour spawner des containers Spark
- **Socket Docker monté** (`/var/run/docker.sock`) + `user: "0:0"` (root) dans Compose
- **Tasks** : 2 PythonOperators (extract + snowflake_load) + 6 BashOperators (spark × 2, dbt × 3, notify)
- **Spark spawning** : BashOperator génère une commande `docker run apache/spark:3.5.1-python3 spark-submit ...`
- **`PROJECT_HOST_PATH`** : chemin Windows-style `C:/Users/...` (forward slashes) requis pour que Docker Desktop comprenne le bind mount depuis un container Linux appelant le daemon via socket

### Commandes clés
```bash
# Build de l'image Airflow custom
docker compose build airflow-webserver

# Démarrage stack complète
docker compose up -d

# Re-démarrage après modification image ou .env
docker compose up -d --force-recreate airflow-scheduler airflow-webserver

# Trigger du DAG depuis l'UI
# http://localhost:8080 → tmdb_pipeline → ▶ Trigger DAG

# Vérifier les logs d'une task (depuis UI ou CLI)
docker exec airflow-scheduler \
  airflow tasks logs tmdb_pipeline spark_staging <run_id>

# Test accès Docker depuis scheduler (diagnostic)
docker exec airflow-scheduler docker ps
```

### Erreurs & ajustements
| Erreur | Cause | Fix |
|---|---|---|
| `Cannot install --user` dans Dockerfile | pip refuse `--user` en root | Suppression du flag `--user` dans `pip install` |
| `docker.io package version 1.41 < 1.44 required` | Paquet Debian `docker.io` trop ancien | Remplacement par `docker-ce-cli` du dépôt officiel docker.com dans `Dockerfile.airflow` |
| `spark_staging` exit 125 (lors des tests CLI) | `airflow dags test` bypasse l'executor, env vars incomplètes | Utiliser uniquement l'UI Airflow pour trigger (vrai chemin d'exécution) |
| Volume mount `/c/Users/...` refusé par Docker daemon | Le daemon Docker Desktop (Windows) attend `C:/...`, pas `/c/...` | `PROJECT_HOST_PATH=C:/Users/...` (forward slashes, style Windows) dans `.env` |

### Validation finale
DAG `tmdb_pipeline` déclenché manuellement depuis l'UI Airflow → **toutes les 8 tasks ✅** :
```
extract_tmdb ✅ → spark_staging ✅ → spark_curated ✅ → snowflake_load ✅
  → dbt_deps ✅ → dbt_run ✅ → dbt_test ✅ → notify_success ✅
```

---

## 🐙 Phase 7 — Publication GitHub + CI

### Lignes directrices
- **Repo public** : https://github.com/Mael8zinsou/tmdb-data-pipeline
- **`.gitignore` strict** : `.env`, `config/keys/`, `*.p8`, `dbt/target/`, `dbt/dbt_packages/`, `dbt/logs/`, `dbt/.user.yml`, `__pycache__/`, parquets, PDF du sujet
- **`.gitattributes`** : LF forcé sur fichiers texte (cohérence Windows ↔ Linux containers)
- **CI minimaliste** (`.github/workflows/ci.yml`) : ruff + Airflow DAG parse + dbt parse, ~1 min
- **Pas de tests d'intégration** : pas de secrets exposés en CI, dummy env vars suffisent pour les parse steps

### Commandes clés (setup initial)
```bash
# 1. Init repo local + premier add
git init -b main
git add .
git status --short                            # vérifier ce qui est stagé
git status --ignored --short | grep -E "\.env|/keys/"   # sanity check secrets

# 2. Premier commit
git commit -m "Initial commit: TMDB data engineering pipeline"

# 3. Création repo distant + push (via gh CLI)
gh auth status                                # vérifier authentification
gh repo create tmdb-data-pipeline --public \
  --source=. --remote=origin \
  --description "..." --push

# 4. Surveiller la CI
gh run list --limit 3
gh run watch <RUN_ID> --exit-status
gh run view <RUN_ID> --log-failed             # si échec
```

### Erreurs & ajustements CI
| Erreur | Cause | Fix |
|---|---|---|
| `ruff F401: 'io' imported but unused` (load.py) | Import laissé après refactor | Suppression manuelle |
| `ruff F401: IntegerType, DateType imported but unused` (staging.py) | Imports prévus puis non utilisés | Suppression manuelle |
| `KeyError: 'TMDB_API_KEY'` au DAG parse | `extract_tmdb.py` lit la clé au module-level → exception à l'import dans CI | Injection de dummy env vars dans l'étape `Parse Airflow DAG` |
| `.claude/settings.local.json` initialement stagé | Config user-local Claude Code | Ajouté à `.gitignore` + `git rm --cached` |

### Structure de la CI
```yaml
# .github/workflows/ci.yml (3 étapes)
1. Lint (ruff)        → ruff check --select E,F --ignore E501 sur src/
2. Parse Airflow DAG  → DagBag().import_errors == {} (avec env vars dummy)
3. Parse DBT          → dbt deps && dbt parse (avec env vars dummy)
```

### Validation finale
3 commits initiaux poussés, CI verte en 1m01s :
```
aaeb583 Initial commit: TMDB data engineering pipeline       ❌ ruff
100914c fix(lint): remove unused imports flagged by ruff     ❌ DAG parse
82e1421 ci: provide dummy env vars for DAG parse step        ✅ ALL PASS
```

---

## 📊 Phase 8 — Monitoring Prometheus + Grafana

### Lignes directrices
- **Chaîne** : Airflow (StatsD natif) → statsd-exporter (UDP 9125 → HTTP 9102) → Prometheus → Grafana
- **3 services** ajoutés au `docker-compose.yml` (statsd-exporter, prometheus, grafana)
- **Auto-provisionnement Grafana** : datasource Prometheus + dashboard 5 panels, zéro clic au démarrage
- **Stockage** : 2 volumes Docker (`prometheus_data`, `grafana_data`), retention Prometheus = 7 jours

### Commandes clés
```bash
# Démarrage complet (incluant monitoring)
docker compose up -d

# Vérifier que les 7 containers sont up
docker compose ps

# Sanity check : métriques Airflow visibles dans statsd-exporter
curl -s http://localhost:9102/metrics | grep "^airflow_" | head -20

# Vérifier les targets scrapés par Prometheus
curl -s http://localhost:9090/api/v1/query?query=up | python -m json.tool

# Tester une query Grafana en CLI
curl -sG http://localhost:9090/api/v1/query \
  --data-urlencode 'query=rate(airflow_scheduler_heartbeat[1m]) * 60'

# Datasources Grafana provisionnés
curl -s -u admin:admin http://localhost:3000/api/datasources | python -m json.tool

# Dashboards Grafana provisionnés
curl -s -u admin:admin http://localhost:3000/api/search?type=dash-db | python -m json.tool
```

### Erreurs & ajustements
| Erreur | Cause | Fix |
|---|---|---|
| Query `rate(airflow_scheduler_heartbeat[1m])` retourne `[]` | La métrique réelle Airflow 2.9 est `airflow.scheduler_heartbeat` (underscore), pas `scheduler.heartbeat` (dot) | Mapping ajusté dans `statsd_mapping.yml` |
| Grafana ne charge pas le dashboard JSON | Le YAML du provider provisioning était dans le même dossier que le JSON → Grafana le scannait comme dashboard | Séparation `provisioning/dashboards/` (YAML provider) ↔ `dashboards/` (JSON dashboards) |
| Prometheus rate vide après restart de statsd-exporter | Besoin de ≥ 2 samples scrapés (>15s) pour calculer un rate | Attendre 30s après restart |

### Architecture du dashboard "TMDB Pipeline — Monitoring"
```
Row 1 (top, h=5)
  ┌──────────────┬──────────────┬──────────────┬──────────────┐
  │ Heartbeat    │ DAG succ 24h │ DAG fail 24h │ Tasks running│
  │ (rate/min)   │ (counter)    │ (counter)    │ (gauge)      │
  └──────────────┴──────────────┴──────────────┴──────────────┘
Row 2 (bottom, h=12)
  ┌─────────────────────────────────────────────────────────────┐
  │ Durée par task du tmdb_pipeline (timeseries, p99, en sec)  │
  └─────────────────────────────────────────────────────────────┘
```

### Validation
- `rate(airflow_scheduler_heartbeat[1m]) * 60` ≈ 12/min (heartbeat toutes les 5s) ✅
- Datasource Prometheus listée par l'API Grafana ✅
- Dashboard "TMDB Pipeline — Monitoring" listé par l'API Grafana ✅

---

## 🔧 Commandes de debug récurrentes

### Re-générer la paire RSA Snowflake
```bash
rm -f config/keys/snowflake_rsa_key.*
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out config/keys/snowflake_rsa_key.p8 -nocrypt
openssl rsa -in config/keys/snowflake_rsa_key.p8 -pubout -out config/keys/snowflake_rsa_key.pub
```

### Reset complet du Data Lake MinIO
```bash
docker run --rm --network finalpipelinev1_default --entrypoint sh minio/mc:latest \
  -c "mc alias set l http://minio:9000 minioadmin minioadmin && mc rm --recursive --force l/tmdb-lake/"
```

### Reset complet Snowflake (TRUNCATE des RAW)
```sql
USE ROLE PIPELINE_ROLE;
USE DATABASE TMDB_DW;
TRUNCATE TABLE RAW.MOVIES_ENRICHED;
TRUNCATE TABLE RAW.MOVIE_GENRES;
TRUNCATE TABLE RAW.DIM_GENRE;
TRUNCATE TABLE RAW.DIM_COUNTRY;
TRUNCATE TABLE RAW.DIM_LANGUAGE;
```

### Relance du pipeline complet (manuelle, en attendant Airflow)
```bash
set -a && source .env && set +a
export MINIO_ENDPOINT=http://localhost:9000
export TMDB_MAX_PAGES=3   # ou plus pour run réel

# 1. Ingestion
python ingestion/extract_tmdb.py

# 2. Spark staging
MSYS_NO_PATHCONV=1 docker run --rm --user 0 --network finalpipelinev1_default \
  -v "/c/Users/maelz/Downloads/Documents/Cours_Documents_YNOV/Stock et Traitement de Data/Final pipeline v1:/app" \
  -w /app -e MINIO_ENDPOINT=http://minio:9000 -e MINIO_ACCESS_KEY=minioadmin \
  -e MINIO_SECRET_KEY=minioadmin -e MINIO_BUCKET=tmdb-lake -e PYTHONPATH=/app \
  apache/spark:3.5.1-python3 /opt/spark/bin/spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  spark/staging.py

# 3. Spark curated (même pattern, juste changer le script)
... spark/curated.py

# 4. Snowflake load
python snowflake_load/load.py

# 5. DBT
export DBT_PROFILES_DIR=./dbt
dbt build --project-dir dbt
```

---

## 📌 Points d'attention pour la suite

- **Sécurité** : la clé RSA `config/keys/snowflake_rsa_key.p8` doit **rester en local**. Déjà ajoutée à `.claudeignore` et `.gitignore` (à créer pour le repo final).
- **Sur le repo GitHub final** : ne jamais committer `.env` ni `config/keys/`. Documenter le setup dans le README.
- **Pour la soutenance** : prévoir un seed de test (movies fictifs si on veut éviter de hit l'API TMDB en live).
- **`TMDB_MAX_PAGES`** : mettre à 3 pour la démo live (60 films, ~30s d'ingestion). Monter à 50-100 pour un run "réel" avant soutenance.
