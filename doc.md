# Documentation Technique — Pipeline TMDB Data Engineering

**Auteur :** Maël Zinsou
**Projet :** M2 Data Engineer · YNOV · *Stockage et Traitement des Données Distribuées*
**Soutenance :** 2026-05-19
**Repo public :** https://github.com/Mael8zinsou/tmdb-data-pipeline
**Statut :** Phases 1-8 complètes, CI verte.

---

## Table des matières

1. [Contexte](#1-contexte)
2. [Architecture](#2-architecture)
3. [Structure du projet](#3-structure-du-projet)
4. [Configuration & secrets](#4-configuration--secrets)
5. [Phases de développement](#5-phases-de-développement)
6. [Pipeline en production (run réel)](#6-pipeline-en-production-run-réel)
7. [Tests & qualité](#7-tests--qualité)
8. [Monitoring & observabilité](#8-monitoring--observabilité)
9. [Critères d'évaluation](#9-critères-dévaluation)
10. [Approfondissement — décisions techniques détaillées](#10-approfondissement--décisions-techniques-détaillées)
11. [Limitations & travaux futurs](#11-limitations--travaux-futurs)

---

## 1. Contexte

### 1.1 Sujet

Concevoir et implémenter une **pipeline de données distribuée complète** respectant l'architecture *medallion* (RAW → STAGING → CURATED → MARTS) et couvrant l'intégralité du cycle de vie de la donnée : **ingestion → traitement → modélisation analytique → restitution**.

### 1.2 Source de données : TMDB

[TMDB](https://www.themoviedb.org/) (The Movie Database) — API REST publique :

| Caractéristique | Valeur |
|---|---|
| Volume exploitable | ~500 000 films |
| Endpoints utilisés | `/discover/movie`, `/genre/movie/list`, `/configuration/countries`, `/configuration/languages` |
| Pagination | 500 pages × 20 résultats max |
| Authentification | API key (gratuite, sans quota strict) |
| Rate limiting | ~50 req/sec, géré par `requests.Session` + retry exponentiel |

### 1.3 Livrables

| Livrable | Statut |
|---|---|
| Pipeline fonctionnelle end-to-end | ✅ |
| Code structuré + versionné GitHub | ✅ |
| Documentation technique exhaustive | ✅ |
| DAG Airflow exécutable | ✅ |
| Star schema DBT + tests | ✅ |
| Monitoring temps réel (bonus) | ✅ |

### 1.4 Repo & CI

- **GitHub public** : https://github.com/Mael8zinsou/tmdb-data-pipeline
- **CI GitHub Actions** : ruff lint + Airflow DAG parse + dbt parse, exécutée à chaque push sur `main` ou pull request
- Badge CI affiché en tête du README

---

## 2. Architecture

### 2.1 Vue globale

```
                      ┌─────────────────────────────────────┐
                      │   Apache Airflow (orchestration)    │
                      │   DAG 8 tasks, schedule=None        │
                      └────────────────┬────────────────────┘
                                       │
   ┌──────────┐    REST API   ┌────────▼─────────┐    S3A    ┌─────────────────┐
   │ TMDB API │──────────────►│  MinIO (raw/)    │──────────►│ Spark Staging   │
   │ /discover│   pagination  │  Parquet snappy  │           │ (Docker, ephem.)│
   └──────────┘   retry expo. └──────────────────┘           └────────┬────────┘
                                                                      │ Parquet
                                                       ┌──────────────▼──────────────┐
                                                       │  MinIO (staging/)           │
                                                       └──────────────┬──────────────┘
                                                                      │ S3A
                                                       ┌──────────────▼──────────────┐
                                                       │  Spark Curated (Docker)     │
                                                       │  jointures + enrichissement │
                                                       └──────────────┬──────────────┘
                                                                      │ Parquet
                                                       ┌──────────────▼──────────────┐
                                                       │  MinIO (curated/)           │
                                                       └──────────────┬──────────────┘
                                                                      │ PUT + COPY INTO
                                                                      │ (RSA key auth)
                                                       ┌──────────────▼──────────────┐
                                                       │  Snowflake TMDB_DW.RAW      │
                                                       └──────────────┬──────────────┘
                                                                      │ DBT (SQL)
                                                       ┌──────────────▼──────────────┐
                                                       │  TMDB_DW.STAGING (views)    │
                                                       │  TMDB_DW.MARTS (tables)     │
                                                       │  star schema + 51 tests     │
                                                       └─────────────────────────────┘

         ┌─ Monitoring (transverse) ──────────────────────────────────────────┐
         │   Airflow ─StatsD UDP─► statsd-exporter ─scrape─► Prometheus       │
         │                                                       ▼            │
         │                                                    Grafana         │
         │                              (5 panels auto-provisionnés)          │
         └─────────────────────────────────────────────────────────────────────┘
```

### 2.2 Stack technologique

| Couche | Technologie | Version | Rôle |
|---|---|---|---|
| **Ingestion** | Python + `requests` | 3.11 | Extraction TMDB paginée, retry exponentiel |
| **Data Lake** | MinIO | latest | Stockage S3-compatible local (raw/staging/curated) |
| **Traitement** | PySpark (container Docker) | 3.5.1 | Nettoyage, dédup, jointures distribuées |
| **Warehouse** | Snowflake (trial, auth RSA) | — | Base analytique cloud, séparation compute/storage |
| **Modélisation** | DBT Core + dbt-snowflake + dbt_utils | 1.8.3 / 1.1.1 | Transformations SQL versionnées + tests |
| **Orchestration** | Apache Airflow (LocalExecutor) | 2.9.1 | DAG, retry, observabilité |
| **Monitoring** | Prometheus + Grafana + statsd-exporter | 2.51.2 / 10.4.2 / 0.26.1 | Métriques temps réel |
| **Infra** | Docker Compose v2 | — | 7 services orchestrés |
| **CI** | GitHub Actions | — | Lint + DAG parse + dbt parse |

### 2.3 Modèle de données — Star schema

Schéma final dans `TMDB_DW.MARTS` :

```
              fct_movies (60)
              ┌────┴────────────────┬─────────────┐
              ▼                     ▼             ▼
       dim_date (47 846)     dim_language    bridge_movie_genre
                              (187)            (161)
                                                 │
                                                 ▼
                                          dim_genre (19)

       dim_country (251)  [référentiel, non lié à fct dans la version actuelle]
```

**Couches DBT** (12 modèles total) :

| Schéma | Modèles | Type | Rôle |
|---|---|---|---|
| `STAGING` | `stg_movies`, `stg_genres`, `stg_countries`, `stg_languages`, `stg_movie_genres` | views | Typage propre depuis `RAW.*` |
| `STAGING` | `int_movies_with_metrics` | view | Score composite + flag `is_recent` |
| `MARTS` | `fct_movies` | table | Faits par film |
| `MARTS` | `dim_date`, `dim_genre`, `dim_country`, `dim_language` | tables | Dimensions |
| `MARTS` | `bridge_movie_genre` | table | Pont N-N films ↔ genres |

---

## 3. Structure du projet

```
Final pipeline v1/
│
├── README.md                       Doc utilisateur (setup, run, troubleshooting)
├── doc.md                          Cette doc technique
├── key_command.md                  Runbook commandes + erreurs + fixes
├── notice_démo.md                  Script de la démo soutenance
│
├── .env.example                    Template secrets
├── .env                            (gitignored) Secrets réels
├── .claudeignore                   Exclusions pour assistants IA
├── .gitignore                      Exclusions repo
├── .gitattributes                  Normalisation LF cross-platform
│
├── docker-compose.yml              Stack 7 services
├── Dockerfile.airflow              Image custom Airflow + docker-ce-cli
├── requirements.txt                Dépendances dev locales
├── requirements-airflow.txt        Dépendances embarquées dans l'image
│
├── ingestion/
│   ├── __init__.py
│   └── extract_tmdb.py             TMDB → Parquet MinIO (raw/)
│
├── spark/
│   ├── __init__.py
│   ├── utils.py                    SparkSession + helpers S3A/MinIO
│   ├── staging.py                  RAW → STAGING (nettoyage, dédup, typage)
│   └── curated.py                  STAGING → CURATED (jointures, enrichissement)
│
├── snowflake_load/
│   ├── __init__.py
│   └── load.py                     CURATED → Snowflake RAW (PUT + COPY INTO)
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml                Auth Snowflake via env_var()
│   ├── packages.yml                dbt_utils 1.1.1
│   ├── package-lock.yml
│   ├── macros/
│   │   └── generate_schema_name.sql  Override : utilise STAGING/MARTS directement
│   └── models/
│       ├── sources.yml             Sources RAW déclarées
│       ├── staging/
│       │   ├── _staging__models.yml
│       │   ├── stg_movies.sql
│       │   ├── stg_genres.sql
│       │   ├── stg_countries.sql
│       │   ├── stg_languages.sql
│       │   └── stg_movie_genres.sql
│       ├── intermediate/
│       │   ├── _intermediate__models.yml
│       │   └── int_movies_with_metrics.sql
│       └── marts/
│           ├── _marts__models.yml
│           ├── fct_movies.sql
│           ├── dim_date.sql
│           ├── dim_genre.sql
│           ├── dim_country.sql
│           ├── dim_language.sql
│           └── bridge_movie_genre.sql
│
├── dags/
│   ├── __init__.py
│   └── tmdb_pipeline.py            DAG Airflow 8 tasks
│
├── monitoring/
│   ├── prometheus.yml              Config scrape Prometheus
│   ├── statsd_mapping.yml          Translation StatsD → Prom (labels)
│   └── grafana/
│       ├── provisioning/
│       │   ├── datasources/prometheus.yml
│       │   └── dashboards/dashboards.yml
│       └── dashboards/
│           └── tmdb-pipeline.json  Dashboard 5 panels
│
├── config/
│   └── keys/                       (gitignored) Clés RSA Snowflake
│       ├── snowflake_rsa_key.p8
│       └── snowflake_rsa_key.pub
│
└── .github/
    └── workflows/
        └── ci.yml                  CI : ruff + DAG parse + dbt parse
```

---

## 4. Configuration & secrets

### 4.1 Fichier `.env` (template)

```bash
# ─── TMDB ─────────────────────────────────────────────────────────────────────
TMDB_API_KEY=<clé_TMDB>
TMDB_MAX_PAGES=3                              # 3 = 60 films (démo), 500 max

# ─── MinIO ────────────────────────────────────────────────────────────────────
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=tmdb-lake

# ─── Airflow → spawn Spark containers ─────────────────────────────────────────
PROJECT_HOST_PATH=C:/Users/<vous>/.../Final pipeline v1   # forward slashes Windows
DOCKER_NETWORK=finalpipelinev1_default

# ─── Snowflake (auth par paire de clés RSA, pas de password) ──────────────────
SNOWFLAKE_ACCOUNT=<account_locator>
SNOWFLAKE_USER=<user>
SNOWFLAKE_PRIVATE_KEY_PATH=config/keys/snowflake_rsa_key.p8
SNOWFLAKE_DATABASE=TMDB_DW
SNOWFLAKE_SCHEMA=RAW
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_ROLE=PIPELINE_ROLE
```

### 4.2 Génération de la paire RSA Snowflake

```bash
mkdir -p config/keys
openssl genrsa 2048 \
  | openssl pkcs8 -topk8 -inform PEM -out config/keys/snowflake_rsa_key.p8 -nocrypt
openssl rsa -in config/keys/snowflake_rsa_key.p8 -pubout \
  -out config/keys/snowflake_rsa_key.pub

# Récupérer le contenu sans BEGIN/END pour ALTER USER
grep -v "PUBLIC KEY" config/keys/snowflake_rsa_key.pub | tr -d '\n'
```

Puis dans Snowsight (rôle ACCOUNTADMIN) :
```sql
ALTER USER <USER> SET RSA_PUBLIC_KEY='<contenu_clé_pub_sans_BEGIN_END>';
DESC USER <USER>;   -- vérifier RSA_PUBLIC_KEY_FP
```

### 4.3 Setup Snowflake (one-shot)

```sql
USE ROLE ACCOUNTADMIN;

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

GRANT ROLE PIPELINE_ROLE TO USER <USER>;
ALTER USER <USER> SET
  DEFAULT_ROLE = PIPELINE_ROLE
  DEFAULT_WAREHOUSE = COMPUTE_WH
  DEFAULT_NAMESPACE = TMDB_DW.RAW;
```

---

## 5. Phases de développement

### Phase 1 — Infrastructure & Ingestion ✅

**Périmètre :** Stack Docker locale (Airflow + PostgreSQL + MinIO), script d'ingestion TMDB → Parquet MinIO.

**Livrables :**
- `docker-compose.yml` (Airflow + Postgres + MinIO + minio-init)
- `ingestion/extract_tmdb.py` : pagination, retry exponentiel, upload Parquet
- `.env.example`

**Sortie validée** (3 pages = 60 films) :
- `raw/movies/` : 60 lignes (30 KB)
- `raw/genres/` : 19 lignes
- `raw/countries/` : 251 lignes
- `raw/languages/` : 187 lignes

### Phase 2 — Spark Staging ✅

**Périmètre :** Nettoyage, dédup, typage des datasets RAW → STAGING.

**Transformations** :
- `movies` : typage, parsing dates, dédup sur `movie_id`, filtrage NaN sur clés, ajout `release_year`
- Lookups (`genres`/`countries`/`languages`) : dédup sur clé, filtrage NaN

**Sortie validée** : `staging/{movies,genres,countries,languages}/` → mêmes counts que RAW (aucune perte).

### Phase 3 — Spark Curated ✅

**Périmètre :** Jointures et enrichissement STAGING → CURATED.

**5 datasets produits** :
- `dim_genre`, `dim_country`, `dim_language` (référentiels propres)
- `movies_enriched` : films avec libellé langue, `release_decade`, `popularity_tier`, `vote_tier`
- `movie_genres` : pont N-N exploded (`explode_outer(genre_ids)`)

**Sortie validée** :
| Dataset | Lignes |
|---|---:|
| `curated/dim_genre/` | 19 |
| `curated/dim_country/` | 251 |
| `curated/dim_language/` | 187 |
| `curated/movies_enriched/` | 60 |
| `curated/movie_genres/` | 161 |

### Phase 4 — Snowflake (COPY INTO) ✅

**Périmètre :** Chargement CURATED → Snowflake `RAW.*` via internal stage.

**Stratégie** :
1. Download Parquet MinIO → tmp local
2. PUT vers internal stage `TMDB_DW.RAW.TMDB_STAGE`
3. `COPY INTO` avec `MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE` (mapping auto Parquet ↔ colonnes table)

**5 tables peuplées** : `RAW.DIM_GENRE` (19), `RAW.DIM_COUNTRY` (251), `RAW.DIM_LANGUAGE` (187), `RAW.MOVIES_ENRICHED` (60), `RAW.MOVIE_GENRES` (161).

### Phase 5 — DBT (Star Schema) ✅

**Périmètre :** 12 modèles DBT répartis sur 3 couches (staging/intermediate/marts), 51 tests automatiques.

**Build runtime** : `dbt build` → **63/63 PASS** (12 modèles + 51 tests).

**Types de tests** : `unique`, `not_null`, `accepted_values` (tiers), `relationships` (FK), `dbt_utils.unique_combination_of_columns`.

### Phase 6 — Airflow (Orchestration) ✅

**Périmètre :** DAG end-to-end orchestrant les 5 phases précédentes.

**Structure** (8 tasks séquentielles) :
```
extract_tmdb (PythonOp)
  → spark_staging (BashOp → docker run apache/spark)
  → spark_curated (BashOp → docker run apache/spark)
  → snowflake_load (PythonOp)
  → dbt_deps (BashOp)
  → dbt_run (BashOp)
  → dbt_test (BashOp)
  → notify_success (BashOp)
```

**Image Airflow custom** (`Dockerfile.airflow`) : base `apache/airflow:2.9.1-python3.11` + installation de `docker-ce-cli` depuis le dépôt officiel docker.com pour permettre au scheduler de spawner des containers Spark via le socket Docker monté.

### Phase 7 — Documentation, GitHub & CI ✅

**Documentation** : README utilisateur réécrit (architecture ASCII, setup step-by-step, troubleshooting), doc technique (ce fichier), runbook commandes (`key_command.md`), notice démo (`notice_démo.md`).

**Repo GitHub public** : https://github.com/Mael8zinsou/tmdb-data-pipeline. `.gitignore` strict (secrets, clés RSA, artefacts DBT). `.gitattributes` pour normalisation LF cross-platform.

**CI GitHub Actions** (`.github/workflows/ci.yml`, ~1 min) :
1. Lint `ruff` (rules E, F) sur `ingestion/ spark/ snowflake_load/ dags/`
2. Parse Airflow DAG via `DagBag` (avec env vars dummy)
3. `dbt deps && dbt parse` (avec env vars dummy)

### Phase 8 — Monitoring Prometheus + Grafana ✅

**Architecture** :
```
Airflow ─StatsD UDP─► statsd-exporter ─HTTP─► Prometheus ─query─► Grafana
       (port 9125)                  (port 9102)       (port 9090)
```

**Config Airflow** (env vars dans `docker-compose.yml`) :
- `AIRFLOW__METRICS__STATSD_ON=True`
- `AIRFLOW__METRICS__STATSD_HOST=statsd-exporter`
- `AIRFLOW__METRICS__STATSD_PORT=9125`
- `AIRFLOW__METRICS__STATSD_PREFIX=airflow`

**Dashboard "TMDB Pipeline — Monitoring"** (auto-provisionné, 5 panels) : scheduler heartbeat, DAG runs succeeded/failed 24h, tasks running, durée par task (timeseries p99).

**Mapping StatsD → Prometheus** (`monitoring/statsd_mapping.yml`) : conversion des métriques pointées en métriques avec labels (`dag_id`, `task_id`).

**Validation** : `airflow_scheduler_heartbeat` rate ≈ 12/min, datasource + dashboard provisionnés au démarrage de Grafana.

---

## 6. Pipeline en production (run réel)

### 6.1 Workflow complet

Triggerable manuellement depuis l'UI Airflow (http://localhost:8080) ou en CLI :

```bash
docker exec finalpipelinev1-airflow-scheduler-1 \
  airflow dags trigger tmdb_pipeline
```

### 6.2 Durées par étape (3 pages = 60 films)

| Task | Durée typique |
|---|---|
| `extract_tmdb` | 30-60 sec |
| `spark_staging` | ~90 sec |
| `spark_curated` | ~120 sec |
| `snowflake_load` | ~30 sec |
| `dbt_deps` | ~10 sec |
| `dbt_run` | ~30-60 sec |
| `dbt_test` | ~20-30 sec |
| `notify_success` | < 1 sec |
| **Total** | **5-7 min** |

### 6.3 Volumes traités (mode démo, `TMDB_MAX_PAGES=3`)

| Couche | Dataset | Lignes |
|---|---|---:|
| MinIO RAW | movies | 60 |
| MinIO RAW | genres | 19 |
| MinIO RAW | countries | 251 |
| MinIO RAW | languages | 187 |
| Snowflake RAW | MOVIES_ENRICHED | 60 |
| Snowflake RAW | MOVIE_GENRES | 161 |
| MARTS | fct_movies | 60 |
| MARTS | dim_date | 47 846 |
| MARTS | dim_genre | 19 |
| MARTS | dim_country | 251 |
| MARTS | dim_language | 187 |
| MARTS | bridge_movie_genre | 161 |

---

## 7. Tests & qualité

### 7.1 CI GitHub Actions

Déclenchée à chaque push sur `main` ou pull request, ~1 minute :

| Étape | Outil | Vérifie |
|---|---|---|
| Lint | `ruff check --select E,F` | Pas d'imports inutilisés, pas d'erreurs syntaxe |
| DAG parse | `DagBag` (env dummy) | DAG Airflow importable, sans erreurs de parsing |
| DBT parse | `dbt parse` (env dummy) | Modèles DBT syntaxiquement valides, refs cohérentes |

### 7.2 Tests DBT (63/63 PASS)

51 tests automatiques répartis :

| Type de test | Cible typique | Exemples |
|---|---|---|
| `unique` | Clés primaires | `fct_movies.movie_id`, `dim_genre.genre_id` |
| `not_null` | FK + colonnes critiques | `fct_movies.date_id`, `fct_movies.movie_id` |
| `accepted_values` | Énumérations | `popularity_tier ∈ {low, medium, high}` |
| `relationships` | Intégrité référentielle | `fct_movies.date_id → dim_date.date_id` |
| `dbt_utils.unique_combination_of_columns` | Clés composites | `bridge_movie_genre (movie_id, genre_id)` |

Commande : `dbt test --project-dir dbt`.

### 7.3 Idempotence

Re-jouable à l'identique pour une même date d'ingestion :
- **MinIO** : partitions `ingestion_date=YYYY-MM-DD` (l'écriture écrase la partition)
- **Snowflake** : `TRUNCATE TABLE` avant `COPY INTO`
- **DBT** : `materialized='table'` → `CREATE OR REPLACE`

### 7.4 Validation runtime end-to-end

Pipeline déclenchée 3 fois successives sur la même date, résultats finaux identiques (vérifié `SELECT COUNT(*)` sur tables MARTS).

---

## 8. Monitoring & observabilité

### 8.1 Endpoints

| Service | URL | Credentials |
|---|---|---|
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | — |
| statsd-exporter (raw metrics) | http://localhost:9102/metrics | — |

### 8.2 Dashboard "TMDB Pipeline — Monitoring"

5 panels auto-provisionnés (provisioning via fichiers JSON/YAML, aucun clic au démarrage) :

| Panel | Métrique sous-jacente | Seuils |
|---|---|---|
| Scheduler heartbeat (rate/min) | `rate(airflow_scheduler_heartbeat[1m]) * 60` | vert ≥5, jaune ≥1, rouge =0 |
| DAG runs succeeded (24h) | `sum(increase(airflow_dagrun_succeeded_total[24h]))` | — |
| DAG runs failed (24h) | `sum(increase(airflow_dagrun_failed_total[24h]))` | rouge si ≥1 |
| Tasks running | `airflow_executor_running_tasks` | — |
| Durée par task (p99, timeseries) | `airflow_task_duration_seconds{dag_id="tmdb_pipeline", quantile="0.99"}` | — |

### 8.3 Métriques Airflow exposées (~80)

- `airflow_scheduler_*` : heartbeat, loop duration, tasks executable/running/starving
- `airflow_executor_*` : open slots, running, queued
- `airflow_dagrun_duration_success_seconds{dag_id="..."}` (avec label `dag_id`)
- `airflow_task_duration_seconds{dag_id="...", task_id="..."}` (avec labels)
- `airflow_dag_processing_*` : import errors, file path queue, last duration

---

## 9. Approfondissement — décisions techniques détaillées

Cette section regroupe le *pourquoi* derrière les choix non-évidents, et les pièges rencontrés en cours de développement.

### 9.1 Authentification Snowflake par paire de clés RSA

**Pourquoi** : le compte trial Snowflake impose la MFA → un password seul ne fonctionne pas pour de l'authentification automatique.

**Solution** : génération d'une paire RSA PKCS8 non chiffrée, clé publique enrôlée côté Snowflake via `ALTER USER ... SET RSA_PUBLIC_KEY=...`, clé privée référencée dans `.env` (`SNOWFLAKE_PRIVATE_KEY_PATH`).

**Avantage transverse** : la même clé est réutilisée par `snowflake_load/load.py` et par DBT (`profiles.yml` lit `private_key_path` via `env_var()`).

### 9.2 Internal stage Snowflake vs external stage S3

**Pourquoi** : MinIO tourne en `localhost:9000`, donc inaccessible depuis le Cloud Snowflake. Un external stage pointant vers MinIO échouerait.

**Solution** : utiliser un *internal stage* Snowflake (`TMDB_DW.RAW.TMDB_STAGE`). Le script `load.py` télécharge d'abord les Parquet de MinIO vers un dossier temporaire local, puis fait `PUT` vers le stage interne, puis `COPY INTO`.

**Alternative possible en prod** : remplacer MinIO par S3/GCS → external stage natif.

### 9.3 Spark dans un container Docker (vs Windows natif)

**Pourquoi** : Spark sous Windows natif requiert `winutils.exe` et `HADOOP_HOME` correctement configurés. Setup fragile et non-portable.

**Solution** : exécution via container `apache/spark:3.5.1-python3` avec `--user 0` (root) pour accéder au cache Ivy. Volume `/app` monté pour exposer les scripts.

**Conséquence côté Airflow** : le DAG utilise un BashOperator qui appelle `docker run ...` via le socket Docker monté (`/var/run/docker.sock`). Le scheduler Airflow doit donc avoir le client `docker` installé (voir §10.7).

### 9.4 Macro DBT `generate_schema_name` custom

**Pourquoi** : par défaut, DBT préfixe les schémas custom : `<target_schema>_<custom>`. Avec `target.schema = RAW` et un modèle ayant `+schema: staging`, DBT crée `RAW_STAGING` au lieu de `STAGING`. Pollution du namespace Snowflake.

**Solution** : macro custom dans `dbt/macros/generate_schema_name.sql` qui retourne directement le `custom_schema_name` quand il est défini. Résultat : les schémas `STAGING` et `MARTS` sont utilisés tels quels.

### 9.5 `dbt_utils.date_spine` vs `GENERATOR(rowcount =>)`

**Pourquoi** : Snowflake exige que l'argument de `GENERATOR(rowcount => N)` soit un littéral constant. Impossible de faire `rowcount => DATEDIFF(day, start_date, end_date)`.

**Solution** : utiliser `dbt_utils.date_spine` (package dbt_utils) qui génère une séquence de dates portable cross-warehouse via une CTE récursive.

### 9.6 Renommage `snowflake/` → `snowflake_load/`

**Pourquoi** : un dossier local nommé `snowflake/` avec un `__init__.py` *shadow* le package PyPI `snowflake.connector` lors de l'import → `ModuleNotFoundError`.

**Solution** : renommage du dossier en `snowflake_load/`. Pas d'incidence fonctionnelle, juste un fix d'import resolution.

### 9.7 `docker-ce-cli` vs `docker.io` dans l'image Airflow

**Pourquoi** : le paquet Debian standard `docker.io` est figé à la version 1.41, incompatible avec Docker Desktop ≥ 26 (qui exige client ≥ 1.44).

**Solution** : dans `Dockerfile.airflow`, ajout du dépôt officiel docker.com et installation de `docker-ce-cli` au lieu de `docker.io`.

### 9.8 `PROJECT_HOST_PATH` au format Windows (forward slashes)

**Pourquoi** : quand le scheduler Airflow (container Linux) appelle `docker run -v "/c/Users/...:/app"` via le socket Docker, le daemon Docker Desktop (Windows) reçoit `/c/Users/...` mais s'attend à `C:/Users/...` (Windows-style avec forward slashes).

**Solution** : `PROJECT_HOST_PATH=C:/Users/maelz/.../Final pipeline v1` dans `.env`. Le DAG injecte cette variable telle quelle dans la commande `docker run -v "${PROJECT_HOST_PATH}:/app"`.

### 9.9 StatsD intermédiaire vs Prometheus client direct

**Pourquoi** : Airflow émet nativement en StatsD (protocole UDP simple, présent dans Airflow depuis la 1.x). Il n'existe pas d'exporteur Prometheus officiel intégré côté scheduler/worker.

**Solution** : intercaler `statsd-exporter` qui écoute en UDP sur 9125 et expose les métriques en HTTP sur 9102 (format Prometheus). Pattern recommandé par la doc Airflow.

**Bénéfice** : mapping configurable via `statsd_mapping.yml` pour extraire les `dag_id` / `task_id` en labels Prometheus (au lieu de les avoir embarqués dans le nom de métrique).

### 9.10 Métrique `scheduler_heartbeat` : underscore et non point

**Pourquoi** : la majorité des métriques Airflow utilisent une notation pointée (`airflow.dagrun.duration.success.<dag_id>`), mais `scheduler_heartbeat` est en *un seul segment avec underscore* (`airflow.scheduler_heartbeat`). Piège classique.

**Solution** : règle de mapping explicite dans `statsd_mapping.yml` pour ce cas particulier.

### 9.11 Budget / revenue absents de `/discover/movie`

**Limitation TMDB** : l'endpoint `/discover/movie` (utilisé pour la pagination en ingestion) **ne retourne pas** `budget` ni `revenue`. Pour avoir ces colonnes, il faudrait appeler `/movie/{id}` pour chaque film (×500 calls par page).

**Décision** : reporté en post-soutenance. Conséquence : pas de calculs ROI/profit dans `movies_enriched` ni dans `fct_movies`. Le pipeline reste fonctionnel et démontrable.

**Évolution possible** : ajouter une task `enrich_with_details` après `extract_tmdb` qui itère sur les films extraits et fait des appels `/movie/{id}` avec rate limiting strict.

### 9.12 `airflow dags test` (CLI) vs trigger via UI

**Piège** : `airflow dags test` exécute les tasks dans le process courant (hors executor), donc **n'hérite pas de toutes les env vars** injectées par Docker Compose au démarrage des containers Airflow. Symptôme : DAG qui marche en UI mais échoue en CLI test.

**Solution** : pour valider un DAG, **toujours trigger depuis l'UI** (ou via `airflow dags trigger`, qui passe par le scheduler) — c'est le seul chemin qui reproduit l'environnement de prod.

---

## 10. Limitations & travaux futurs

| Limitation actuelle | Impact | Évolution proposée |
|---|---|---|
| Budget/revenue manquants (TMDB) | Pas de KPI ROI dans MARTS | Ajouter `enrich_movie_details` task → `/movie/{id}` |
| Pas de tests unitaires Python | Couverture des transformations limitée à DBT | pytest sur `extract_tmdb.py`, `spark/staging.py` |
| Pas d'Alertmanager | Pas de notif sur échec/scheduler down | Ajouter Alertmanager, route Slack/Email |
| Spark non-monitoré | Pas de métriques driver/executor | Pertinent uniquement si cluster Spark managé |
| MinIO local | Non-distribué, non-prod-ready | Migrer vers S3/GCS pour le cloud |
| Pas de data viz | Star schema sous-exploité | Brancher Metabase / Superset / Power BI sur MARTS |
| Pipeline manuelle (`schedule=None`) | Pas d'ingestion auto | Activer `schedule="@daily"` une fois en prod |

---

**Dernière mise à jour :** 2026-05-19
**Statut :** Phases 1-8 complètes, CI verte
