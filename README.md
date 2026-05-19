# Pipeline TMDB — Data Engineering End-to-End

[![CI](https://github.com/Mael8zinsou/tmdb-data-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/Mael8zinsou/tmdb-data-pipeline/actions/workflows/ci.yml)

> Projet final M2 Data Engineer · YNOV · Maël Zinsou · Soutenance 19 mai 2026

Pipeline de données distribuée complète : ingestion paginée d'une API publique → Data Lake → traitement distribué → Data Warehouse → modélisation dimensionnelle, orchestrée par Apache Airflow.

**Repo :** https://github.com/Mael8zinsou/tmdb-data-pipeline

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Apache Airflow  (orchestration, 8 tasks)                 │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
         ┌─────────────────────────▼──────────────────────────┐
         │  1. INGESTION                                       │
         │     TMDB API (/discover/movie, /genre, /config)     │
         │     Python · requests · pagination jusqu'à 500p     │
         │     → Parquet snappy partitionné par ingestion_date  │
         └─────────────────────────┬──────────────────────────┘
                                   │ boto3 / S3
         ┌─────────────────────────▼──────────────────────────┐
         │  2. DATA LAKE  (MinIO — S3-compatible local)        │
         │     raw/movies/          raw/genres/                │
         │     raw/countries/       raw/languages/             │
         └──────────┬──────────────────────────┬──────────────┘
                    │ S3A (hadoop-aws 3.3.4)    │
         ┌──────────▼──────────┐    ┌──────────▼──────────────┐
         │  3. SPARK STAGING   │    │  4. SPARK CURATED        │
         │     PySpark 3.5.1   │ →  │     PySpark 3.5.1        │
         │     typage, dédup,  │    │     jointures, colonnes  │
         │     nettoyage       │    │     dérivées, pont N-N   │
         │  (Docker container) │    │  (Docker container)      │
         └─────────────────────┘    └──────────┬───────────────┘
                                               │ Parquet (curated/)
                                    ┌──────────▼───────────────┐
                                    │  5. SNOWFLAKE  TMDB_DW   │
                                    │     schéma RAW           │
                                    │     PUT + COPY INTO      │
                                    │     auth RSA key-pair    │
                                    └──────────┬───────────────┘
                                               │ SQL (Jinja2)
                                    ┌──────────▼───────────────┐
                                    │  6. DBT  →  MARTS        │
                                    │  ┌─ fct_movies      (60) │
                                    │  ├─ bridge_movie_genre    │
                                    │  │    (161, pont N-N)     │
                                    │  ├─ dim_genre        (19) │
                                    │  ├─ dim_date      (47 k)  │
                                    │  ├─ dim_country    (251)  │
                                    │  └─ dim_language   (187)  │
                                    │  63 / 63 tests PASS      │
                                    └──────────────────────────┘
```

### Stack

| Couche | Technologie | Version |
|---|---|---|
| Ingestion | Python + `requests` (retry exponentiel) | 3.11 |
| Data Lake | MinIO S3-compatible | latest |
| Traitement | PySpark (container `apache/spark`) | 3.5.1 |
| Warehouse | Snowflake (auth RSA key-pair) | trial |
| Modélisation | DBT Core + dbt-snowflake + dbt_utils | 1.8.3 |
| Orchestration | Apache Airflow (LocalExecutor) | 2.9.1 |
| Infra | Docker Compose | v2 |

---

## Prérequis

- **Docker Desktop** ≥ 4.x (avec socket exposé — activé par défaut)
- **Python** 3.10+ (pour les scripts manuels hors Docker)
- Compte **Snowflake** (trial gratuit sur snowflake.com)
- Clé **TMDB API** (gratuite sur themoviedb.org)
- **OpenSSL** (pour générer la paire RSA Snowflake)

---

## Setup

### 1. Cloner / entrer dans le dossier

```bash
cd "Final pipeline v1"
```

### 2. Configurer les secrets

```bash
cp .env.example .env
```

Renseigner dans `.env` :

```bash
TMDB_API_KEY=<votre_clé_tmdb>

# MinIO — laisser les valeurs par défaut pour le local
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=tmdb-lake

# Chemin HOST du projet (Windows : forward slashes obligatoires)
PROJECT_HOST_PATH=C:/Users/<vous>/chemin/vers/Final pipeline v1
DOCKER_NETWORK=finalpipelinev1_default
TMDB_MAX_PAGES=3      # 3 = 60 films (test), monter à 50+ pour un run réel

# Snowflake (voir section suivante)
SNOWFLAKE_ACCOUNT=<account_locator>
SNOWFLAKE_USER=<user>
SNOWFLAKE_PRIVATE_KEY_PATH=config/keys/snowflake_rsa_key.p8
SNOWFLAKE_DATABASE=TMDB_DW
SNOWFLAKE_SCHEMA=RAW
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_ROLE=PIPELINE_ROLE
```

### 3. Configurer Snowflake

#### a. Créer les objets dans Snowsight (rôle ACCOUNTADMIN)

```sql
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
  WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;

CREATE DATABASE IF NOT EXISTS TMDB_DW;
CREATE SCHEMA IF NOT EXISTS TMDB_DW.RAW;
CREATE SCHEMA IF NOT EXISTS TMDB_DW.STAGING;
CREATE SCHEMA IF NOT EXISTS TMDB_DW.MARTS;

CREATE ROLE IF NOT EXISTS PIPELINE_ROLE;
GRANT ROLE PIPELINE_ROLE TO ROLE SYSADMIN;
GRANT USAGE, OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE PIPELINE_ROLE;
GRANT USAGE ON DATABASE TMDB_DW TO ROLE PIPELINE_ROLE;
GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT
  ON ALL SCHEMAS IN DATABASE TMDB_DW TO ROLE PIPELINE_ROLE;
GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT
  ON FUTURE SCHEMAS IN DATABASE TMDB_DW TO ROLE PIPELINE_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
  ON ALL TABLES IN DATABASE TMDB_DW TO ROLE PIPELINE_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
  ON FUTURE TABLES IN DATABASE TMDB_DW TO ROLE PIPELINE_ROLE;

GRANT ROLE PIPELINE_ROLE TO USER <VOTRE_USER>;
ALTER USER <VOTRE_USER> SET
  DEFAULT_ROLE = PIPELINE_ROLE
  DEFAULT_WAREHOUSE = COMPUTE_WH
  DEFAULT_NAMESPACE = TMDB_DW.RAW;
```

#### b. Générer la paire RSA (authentification sans password)

```bash
mkdir -p config/keys

# Clé privée PKCS8 non chiffrée
openssl genrsa 2048 \
  | openssl pkcs8 -topk8 -inform PEM -out config/keys/snowflake_rsa_key.p8 -nocrypt

# Clé publique
openssl rsa -in config/keys/snowflake_rsa_key.p8 -pubout \
  -out config/keys/snowflake_rsa_key.pub

# Extraire le contenu sans les lignes BEGIN/END (pour ALTER USER)
grep -v "PUBLIC KEY" config/keys/snowflake_rsa_key.pub | tr -d '\n'
```

```sql
-- Dans Snowsight
ALTER USER <VOTRE_USER> SET RSA_PUBLIC_KEY='<contenu_clé_pub_sans_BEGIN_END>';
DESC USER <VOTRE_USER>;   -- vérifier RSA_PUBLIC_KEY_FP
```

### 4. Lancer l'infrastructure

```bash
docker compose up -d
```

Services exposés :

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| MinIO console | http://localhost:9001 | minioadmin / minioadmin |

Attendre ~30 secondes que l'init Airflow termine (vérifier avec `docker compose logs airflow-init`).

---

## Exécuter le pipeline

Depuis l'**Airflow UI** (http://localhost:8080) :

1. Activer le DAG `tmdb_pipeline` (toggle ON)
2. Cliquer **Trigger DAG ▶**
3. Suivre l'avancement dans la vue Graph

Durée typique (3 pages / 60 films) : **~5–8 minutes** (dont ~4 min de téléchargement des packages Spark au premier run).

### Séquence des tâches

```
extract_tmdb → spark_staging → spark_curated → snowflake_load
    → dbt_deps → dbt_run → dbt_test → notify_success
```

### Résultats attendus

| Couche | Contenu |
|---|---|
| MinIO `raw/` | 4 datasets Parquet (movies, genres, countries, languages) |
| MinIO `staging/` | Films nettoyés + lookups dédupliqués |
| MinIO `curated/` | 5 datasets enrichis (movies_enriched, movie_genres, dim_*) |
| Snowflake `RAW.*` | 5 tables chargées via COPY INTO |
| Snowflake `MARTS.*` | 6 modèles DBT matérialisés, 63/63 tests PASS |

---

## Modèle de données (Star Schema)

```
                  bridge_movie_genre (161)
                  ┌──────────────────────┐
                  │ movie_id  genre_id   │──► dim_genre (19)
                  └──────┬───────────────┘     genre_id · genre_name
                         │
fct_movies (60) ─────────┤
  movie_id (PK)          │
  date_id ──────────────────────────────────► dim_date (47 846)
  language_id ─────────────────────────────► dim_language (187)
  title                                        language_id · name · iso
  original_language
  popularity / vote_average / vote_count
  popularity_tier / vote_tier
  has_release_date / release_decade
  is_recent

dim_country (251)   [référentiel — non liée à fct dans v1]
  country_id · iso_3166_1 · country_name
```

---

## Structure du projet

```
Final pipeline v1/
│
├── ingestion/                  Phase 1 — Extraction TMDB
│   ├── extract_tmdb.py         Pagination + upload Parquet → MinIO
│   └── __init__.py
│
├── spark/                      Phases 2-3 — Traitement distribué
│   ├── utils.py                SparkSession configurée pour S3A / MinIO
│   ├── staging.py              Nettoyage, typage, dédup
│   └── curated.py              Jointures, enrichissement, pont N-N
│
├── snowflake_load/             Phase 4 — Chargement Warehouse
│   └── load.py                 RSA auth · internal stage · PUT + COPY INTO
│
├── dbt/                        Phase 5 — Modélisation dimensionnelle
│   ├── dbt_project.yml
│   ├── profiles.yml            Auth Snowflake (RSA, via env vars)
│   ├── packages.yml            dbt_utils 1.1.1
│   ├── macros/
│   │   └── generate_schema_name.sql  Override → STAGING/MARTS directs
│   └── models/
│       ├── staging/            stg_movies · stg_genres · stg_countries · stg_languages
│       ├── intermediate/       int_movies_with_metrics
│       └── marts/              fct_movies · dim_* · bridge_movie_genre
│
├── dags/                       Phase 6 — Orchestration Airflow
│   └── tmdb_pipeline.py        DAG 8 tâches, schedule=None (manuel)
│
├── .github/workflows/
│   └── ci.yml                  GitHub Actions : ruff + DAG parse + dbt parse
│
├── config/
│   └── keys/                   Clés RSA Snowflake (non committées)
│
├── Dockerfile.airflow          Image custom Airflow + docker-ce-cli
├── docker-compose.yml          Stack locale complète
├── requirements-airflow.txt    Dépendances Python embarquées dans l'image
├── .env.example                Template secrets (committer ✅)
├── .env                        Secrets réels (ne jamais committer ❌)
├── .gitignore                  Exclusions (.env, keys, target/, parquet…)
├── .gitattributes              Normalisation LF cross-platform
├── doc.md                      Documentation technique complète
└── key_command.md              Runbook commandes + erreurs + fixes
```

---

## Intégration continue (GitHub Actions)

Workflow `.github/workflows/ci.yml` — déclenché à chaque push sur `main` ou pull request :

| Étape | Outil | Vérifie |
|---|---|---|
| **Lint** | ruff (rules E, F) | Pas d'imports inutilisés, pas d'erreurs syntaxe |
| **DAG parse** | `DagBag` | DAG Airflow importable, sans erreurs de parsing |
| **DBT parse** | `dbt parse` | Modèles DBT syntaxiquement valides, refs cohérentes |

Pas de tests d'intégration en CI (nécessiterait des secrets Snowflake/TMDB). Le DAG et DBT sont parsés avec des env vars dummy — aucune exécution réelle.

Durée typique : **~1 minute**.

---

## Décisions techniques clés

| Problème | Solution retenue |
|---|---|
| MFA obligatoire sur Snowflake trial | Authentification par paire de clés RSA (PKCS8 non chiffré) |
| Spark impossible sous Windows natif | Spark dans container Docker `apache/spark:3.5.1-python3` |
| MinIO inaccessible depuis Snowflake Cloud | Internal stage Snowflake (PUT local → COPY INTO) |
| `snowflake/` shadow le package PyPI | Dossier renommé `snowflake_load/` |
| `GENERATOR(rowcount =>)` Snowflake exige constante | `dbt_utils.date_spine` pour `dim_date` |
| DBT crée `PUBLIC_staging` au lieu de `STAGING` | Macro `generate_schema_name` overridée |
| Path Windows `/c/Users/...` rejeté par Docker daemon | `PROJECT_HOST_PATH=C:/Users/...` (forward slashes) dans `.env` |

---

## Troubleshooting

**`docker compose up` — airflow-init ne se termine pas**
```bash
docker compose logs airflow-init
# Si "waiting for postgres" : attendre 30s, postgres démarre parfois lentement
```

**spark_staging échoue — exit 125**
```bash
# Vérifier que le socket Docker est accessible depuis le scheduler
docker exec airflow-scheduler docker ps
# Vérifier le nom du réseau
docker network ls | grep pipeline
```

**Snowflake — `JWT token is invalid`**
```bash
# Vérifier le fingerprint local vs Snowsight
openssl rsa -in config/keys/snowflake_rsa_key.p8 -pubout -outform DER 2>/dev/null \
  | openssl dgst -sha256 -binary | openssl enc -base64
# Dans Snowsight : DESC USER <USER>; → RSA_PUBLIC_KEY_FP doit matcher
```

**DBT — `Insufficient privileges`**
```sql
-- S'assurer que PIPELINE_ROLE a CREATE sur les schémas futurs
GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE STAGE, CREATE FILE FORMAT
  ON FUTURE SCHEMAS IN DATABASE TMDB_DW TO ROLE PIPELINE_ROLE;
```

**Reset complet (repartir de zéro)**
```bash
# Vider MinIO
docker run --rm --network finalpipelinev1_default --entrypoint sh minio/mc:latest \
  -c "mc alias set l http://minio:9000 minioadmin minioadmin && mc rm --recursive --force l/tmdb-lake/"

# Truncate Snowflake RAW (dans Snowsight)
# TRUNCATE TABLE RAW.MOVIES_ENRICHED; -- etc.
```

---

## État du projet

| Phase | Description | Statut |
|---|---|---|
| 1 | Infrastructure & Ingestion TMDB | ✅ |
| 2 | Spark Staging (nettoyage) | ✅ |
| 3 | Spark Curated (enrichissement) | ✅ |
| 4 | Snowflake — COPY INTO | ✅ |
| 5 | DBT — Star Schema (63/63 tests) | ✅ |
| 6 | DAG Airflow — orchestration E2E | ✅ |
| 7 | Documentation finale + repo public GitHub + CI | ✅ |
| 8 | Bonus — Monitoring / Dashboard | ⏳ |
