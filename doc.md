# Documentation Projet — Pipeline TMDB Data Engineering

**Auteur :** Maël Zinsou  
**Date :** 2026-04-27  
**Deadline soutenance :** 2026-05-19  
**Sujet :** Stockage et Traitement des Données Distribuées (M2 Data Engineer)

---

## 1. Contexte & Objectif

### Sujet
Concevoir et implémenter une **pipeline de données distribuée complète** respectant l'architecture medallion (RAW → STAGING → CURATED) et couvrant l'ensemble du cycle de vie de la donnée : **ingestion → traitement → analytique**.

### Données sources
**API TMDB** (The Movie Database) :
- ~500k films exploitables
- Rich dataset : titre, genre, production, budget, revenue, votes, popularité, langue, pays
- Pagination native (max 500 pages × 20 résultats)
- Gratuit, sans quota restrictif

### Livrables attendus
1. ✅ Pipeline fonctionnelle bout-en-bout
2. ✅ Code structuré et lisible
3. ✅ Documentation complète (README + schéma architecture)
4. ✅ DAG Airflow exécutable
5. ✅ Démonstration le 19 mai (exécution + résultats + architecture)

**Bonus :** Monitoring (logs/métriques) + Dashboard analytique

---

## 2. Architecture

```
TMDB API
   ↓ (Python + requests, pagination)
┌─────────────────────────────────────┐
│  Data Lake (MinIO S3-compatible)    │
│  RAW: parquet partitionné           │  ingestion_date=YYYY-MM-DD/
│  • movies_part_0000.parquet         │
│  • genres.parquet                   │
│  • countries.parquet                │
│  • languages.parquet                │
└─────────────────────────────────────┘
   ↓
┌─────────────────────────────────────┐
│  Spark (PySpark Local Mode)         │
│  STAGING: nettoyage + dédup         │
│  • renommage colonnes               │
│  • typage schéma                    │
│  • suppression doublons             │
│  • gestion valeurs manquantes       │
└─────────────────────────────────────┘
   ↓
┌─────────────────────────────────────┐
│  Spark (PySpark Local Mode)         │
│  CURATED: jointures + enrichissement│
│  • fusion movies + genres           │
│  • fusion movies + countries        │
│  • fusion movies + languages        │
│  • calculs dérivés (ROI, rating_cat)│
└─────────────────────────────────────┘
   ↓ (COPY INTO)
┌─────────────────────────────────────┐
│  Data Warehouse (Snowflake)         │
│  Schéma PUBLIC (ou STAGING)         │
│  Tables intermédiaires              │
└─────────────────────────────────────┘
   ↓
┌─────────────────────────────────────┐
│  DBT (Data Build Tool)              │
│  Transformations dans Snowflake     │
│  staging/ → intermediate/ → marts/  │
│  STAR SCHEMA :                      │
│  • fact_movies                      │
│  • dim_genre, dim_country, etc      │
└─────────────────────────────────────┘
   ↓
┌─────────────────────────────────────┐
│  Marts Analytiques                  │
│  Prêts pour BI/Dashboard            │
└─────────────────────────────────────┘

Orchestration : Apache Airflow (DAG, scheduling, monitoring)
```

### Stack technologique

| Composant | Technologie | Rôle |
|-----------|---|---|
| **Ingestion** | Python + requests | Extraction TMDB paginée |
| **Data Lake** | MinIO (Docker) | Stockage S3-compatible local |
| **Traitement** | PySpark 3.5.1 | Nettoyage + jointures distribuées |
| **Warehouse** | Snowflake | Base de données analytique |
| **Transformation** | DBT + Jinja2 | Modélisation dimensionnelle |
| **Orchestration** | Apache Airflow | DAG + scheduling + monitoring |
| **Infrastructure** | Docker Compose | Airflow + PostgreSQL + MinIO |

---

## 3. Plan de développement (par phase)

### Phase 1️⃣ : Infrastructure & Ingestion ✅ COMPLÈTE
**Deadline :** 2026-04-28

- [x] `docker-compose.yml` (Airflow + MinIO + PostgreSQL)
- [x] `.env.example` template
- [x] `requirements.txt` (dépendances)
- [x] `ingestion/extract_tmdb.py` (pagination + upload Parquet)
- [x] `.claudeignore` (exclusions contexte)
- [ ] Tests unitaires ingestion (optionnel)
- [ ] README section setup

**Sortie attendue :** Docker stack running, TMDB data brute dans MinIO/raw/

---

### Phase 2️⃣ : Spark Staging (Nettoyage)
**Deadline :** 2026-04-30

**Fichier :** `spark/staging.py`

Transformations :
- Lecture Parquet depuis MinIO (RAW)
- Renommage colonnes (snake_case)
- Typage schéma (int, string, double, timestamp)
- Suppression doublons (movie_id + ingestion_date)
- Gestion NaN : suppression colonnes inutiles, fillna stratégique
- Filtrage : films avec budget > 0 ET revenue > 0 (analytique)
- Partition output : `staging/ingestion_date=YYYY-MM-DD/`

**KPI :** nombre de lignes avant/après nettoyage

---

### Phase 3️⃣ : Spark Curated (Enrichissement)
**Deadline :** 2026-05-02

**Fichier :** `spark/curated.py`

Transformations :
- Jointure movies + genres (explode genres, many-to-many)
- Jointure movies + countries (production_countries)
- Jointure movies + languages (spoken_languages)
- Colonnes dérivées :
  - `roi = revenue / budget` (si budget > 0)
  - `profit = revenue - budget`
  - `popularity_tier = CASE WHEN popularity > 100 THEN 'high' ELSE 'low' END`
  - `release_year = YEAR(release_date)`
- Sortie : Parquet à charger dans Snowflake
- Partition output : `curated/ingestion_date=YYYY-MM-DD/`

---

### Phase 4️⃣ : Snowflake + COPY INTO
**Deadline :** 2026-05-05

**Fichier :** `snowflake_load/load.py`

- Connection Snowflake (credentials .env)
- Create temporary staging tables (raw imports)
- COPY INTO depuis curated/ (Parquet)
- Validation row counts

---

### Phase 5️⃣ : DBT Dimensionnel
**Deadline :** 2026-05-10

**Dossier :** `dbt/models/`

**Staging models (`staging/`)** :
- `stg_movies` : typage + nettoyage colonne par colonne
- `stg_genres`
- `stg_countries`
- `stg_languages`

**Intermediate models (`intermediate/`)** :
- `int_movies_with_genres` : jointure movies × genres explodée
- `int_movies_with_financials` : revenue/budget/roi calcs

**Marts (`marts/`)** :
- `fact_movies` : film_id, genre_id, country_id, year_id, budget, revenue, roi, ...
- `dim_genre` : genre_id, genre_name
- `dim_country` : country_id, country_name, iso_code
- `dim_language` : language_id, language_name
- `dim_year` : year, month range, ...

**Tests dbt :**
- Unicité sur clés primaires
- Pas de NaN dans colonnes clés
- Contraintes de référence (FK)

---

### Phase 6️⃣ : DAG Airflow ✅ COMPLÈTE
**Deadline :** 2026-05-12

**Fichier :** `dags/tmdb_pipeline.py`

Structure du DAG (8 tasks, exécution séquentielle) :

```
extract_tmdb (PythonOperator)
    ↓
spark_staging (BashOperator → docker run apache/spark:3.5.1-python3)
    ↓
spark_curated (BashOperator → docker run apache/spark:3.5.1-python3)
    ↓
snowflake_load (PythonOperator)
    ↓
dbt_deps (BashOperator)
    ↓
dbt_run (BashOperator)
    ↓
dbt_test (BashOperator)
    ↓
notify_success (BashOperator)
```

**Décisions techniques Phase 6 :**
- **Image Airflow custom** (`tmdb-airflow:local`) : base `apache/airflow:2.9.1-python3.11` + `docker-ce-cli` (dépôt officiel docker.com, pas `docker.io` Debian trop ancien)
- **Socket Docker monté** : `/var/run/docker.sock:/var/run/docker.sock` → scheduler Airflow peut spawner des containers Spark
- **`user: "0:0"` (root)** dans docker-compose pour accéder au socket sans permission error
- **`PROJECT_HOST_PATH` format Windows** : `C:/Users/...` (forward slashes, pas `/c/Users/...`) obligatoire pour que le daemon Docker Desktop comprenne le bind mount depuis un container Linux
- **Validation** : `airflow dags test` (CLI) ≠ trigger via UI Airflow → CLI bypasse l'executor et n'hérite pas de toutes les env vars. Le vrai test = trigger manuel depuis l'UI.

---

### Phase 7️⃣ : Documentation & Publication GitHub ✅ COMPLÈTE
**Deadline :** 2026-05-15

**Documentation :**
- README réécrit complet : setup step-by-step, schéma architecture ASCII, star schema, structure projet, troubleshooting
- Data dictionary implicite (noms de colonnes dans le schéma)
- Exemples d'exécution (commandes complètes)
- Troubleshooting (5 cas couverts)

**Publication GitHub :**
- Repo public : **https://github.com/Mael8zinsou/tmdb-data-pipeline**
- `.gitignore` couvrant secrets (.env, config/keys/, *.p8), artefacts DBT (target/, dbt_packages/, logs/), caches Python, IDE
- `.gitattributes` pour normalisation LF cross-platform
- 3 commits initiaux : pipeline complète + fix lint + ajustement CI env vars

**CI (GitHub Actions, `.github/workflows/ci.yml`) :**
- 3 étapes : ruff (lint) → DAG parse (DagBag) → dbt parse
- Pas de tests d'intégration (pas de secrets en CI)
- Env vars dummy injectées pour les `os.environ[...]` lus au module-level
- Durée typique : ~1 minute
- Badge CI affiché dans README

---

### Phase 8️⃣ : Monitoring Prometheus + Grafana ✅ COMPLÈTE
**Deadline :** 2026-05-18

**Architecture du monitoring :**
```
Airflow (StatsD client natif) → statsd-exporter → Prometheus → Grafana
                              (UDP 9125)        (scrape 15s)
```

**3 services ajoutés à `docker-compose.yml` :**
- `statsd-exporter` (prom/statsd-exporter:v0.26.1) — UDP 9125 entrée, HTTP 9102 sortie
- `prometheus` (prom/prometheus:v2.51.2) — scrape, retention 7 jours
- `grafana` (grafana/grafana:10.4.2) — auto-provisionné (datasource + dashboard)

**Config Airflow** (env vars dans `x-airflow-common`) :
- `AIRFLOW__METRICS__STATSD_ON=True`
- `AIRFLOW__METRICS__STATSD_HOST=statsd-exporter`
- `AIRFLOW__METRICS__STATSD_PORT=9125`
- `AIRFLOW__METRICS__STATSD_PREFIX=airflow`

**Dashboard "TMDB Pipeline — Monitoring"** (5 panels) :
1. Scheduler heartbeat (rate/min, seuils vert ≥5, jaune ≥1, rouge =0)
2. DAG runs succeeded (24h)
3. DAG runs failed (24h)
4. Tasks running actuellement
5. Durée par task du `tmdb_pipeline` (timeseries, p99)

**Mapping StatsD → Prometheus** (`monitoring/statsd_mapping.yml`) :
- Conversion des métriques pointées (`airflow.dagrun.duration.success.<dag_id>`) en métriques Prometheus avec labels (`dag_id`, `task_id`)
- ~10 règles + fallback `airflow_unmapped{raw_metric="..."}` pour les exceptions

**Validation runtime :**
- `airflow_scheduler_heartbeat` rate ≈ 12/min ✅
- `airflow_executor_open_slots`, `airflow_executor_running_tasks` exposés ✅
- Datasource Prometheus + dashboard auto-provisionnés au démarrage ✅

**Décision technique :**
- Métrique réelle Airflow 2.9 = `airflow.scheduler_heartbeat` (underscore), pas `scheduler.heartbeat` → mapping ajusté
- Métriques Spark hors scope (containers éphémères) — démontrable en soutenance
- Pas d'alerting (Alertmanager) : démonstrable visuellement via le dashboard

**Data Visualization (hors scope, post-soutenance possible) :**
- Metabase / Superset connecté à `TMDB_DW.MARTS` pour KPIs analytiques

---

## 4. Structure de projet

```
Final pipeline v1/
├── doc.md                      ← Ce fichier
├── README.md                   ← À remplir (setup, architecture, run)
├── .env.example                ✅ Créé
├── .env                        ← À créer (secrets, ne pas committer)
├── .claudeignore               ✅ Créé
├── requirements.txt            ✅ Créé
├── docker-compose.yml          ✅ Créé
│
├── config/                     ← Configs (Airflow, DBT, etc)
│   └── (vides pour l'instant)
│
├── ingestion/                  ✅ Phase 1
│   ├── __init__.py
│   ├── extract_tmdb.py         ✅ Créé
│   └── validate.py             ← À créer (optionnel)
│
├── spark/                      Phase 2-3
│   ├── __init__.py
│   ├── staging.py              ← À créer
│   ├── curated.py              ← À créer
│   └── utils.py                ← À créer (helpers Spark)
│
├── snowflake/                  Phase 4
│   └── load.py                 ← À créer
│
├── dbt/                        Phase 5
│   ├── dbt_project.yml         ← À créer
│   ├── profiles.yml            ← À créer (.gitignore)
│   ├── models/
│   │   ├── staging/            ← À créer
│   │   │   ├── stg_movies.sql
│   │   │   ├── stg_genres.sql
│   │   │   └── ...
│   │   ├── intermediate/       ← À créer
│   │   │   ├── int_movies_with_genres.sql
│   │   │   └── ...
│   │   └── marts/              ← À créer
│   │       ├── fact_movies.sql
│   │       ├── dim_genre.sql
│   │       └── ...
│   ├── tests/                  ← À créer (dbt tests)
│   └── macros/                 ← À créer (si helpers DBT)
│
├── dags/                       Phase 6
│   ├── __init__.py
│   └── tmdb_pipeline.py        ← À créer (DAG Airflow)
│
└── Projet final Stock et...pdf  (référence sujet)
```

---

## 5. Environnement & Secrets

**`.env`** (à créer, ne pas committer) :

```bash
# TMDB
TMDB_API_KEY=your_key_here
TMDB_MAX_PAGES=500

# MinIO
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=tmdb-lake

# Snowflake (à remplir après création compte)
SNOWFLAKE_ACCOUNT=xy12345.us-east-1
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=TMDB_DW
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_ROLE=SYSADMIN
```

---

## 6. Commandes clés

### Startup
```bash
docker compose up -d
# Airflow UI : http://localhost:8080 (admin/admin)
# MinIO console : http://localhost:9001 (minioadmin/minioadmin)
```

### Tests phase 1
```bash
# Depuis le container Airflow
docker exec -it airflow-webserver bash
cd /opt/airflow
python ingestion/extract_tmdb.py
# Vérifier MinIO : http://localhost:9001 → bucket tmdb-lake/raw/
```

### Tests Spark (phase 2-3)
```bash
# Local mode
python spark/staging.py
python spark/curated.py
```

### DAG Airflow (phase 6)
```bash
# Déclencher le DAG manuellement via UI
# ou en CLI :
airflow dags test tmdb_pipeline 2026-04-27
```

---

## 7. Critères d'évaluation (d'après sujet)

| Critère | Description | Poids |
|---------|---|---|
| **Architecture** | Cohérence et clarté de la pipeline | 20% |
| **Pipeline** | Fonctionnement complet bout-en-bout | 25% |
| **Traitement** | Qualité des transformations Spark | 20% |
| **Orchestration** | Mise en œuvre DAG Airflow lisible | 15% |
| **Documentation** | README clair + schéma architecture | 10% |
| **Démonstration** | Exécution réelle + présentation (19 mai) | 10% |
| **Bonus** | Monitoring ou Data Visualization | +5% |

---

## 8. Notes importantes

- ⚠️ **Snowflake gratuit** : créer account trial (1 mois gratuit, credits généreux). Exporte credentials dans `.env`.
- ⚠️ **TMDB API** : gratuit, rate limiting respecté (0.25s entre requêtes).
- ⚠️ **Spark local mode** : suffisant pour ~10k films. Pas besoin de cluster.
- ⚠️ **DBT profiles.yml** : à exclure du git (secrets Snowflake).
- ⚠️ **Parquet compression** : `snappy` par défaut (bon compromis perf/size).

---

## 9. Checklist avant soutenance (19 mai)

- [x] Pipeline exécutable end-to-end
- [x] README complété (setup + architecture + troubleshooting)
- [x] Schéma architecture inclus (ASCII art dans README)
- [x] DAG Airflow visible + expliqué
- [x] Star schema DBT prêt (fact + dimensions)
- [x] Tests DBT (63/63 PASS)
- [ ] Démonstration préparée (run complet + résultats Snowflake + DBT)
- [x] GitHub public avec code complet + CI verte (sans .env ni config/keys/)
- [x] Bonus monitoring (Prometheus + Grafana, dashboard 5 panels auto-provisionné)

---

**Mise à jour :** 2026-05-19 — Phases 1-8 ✅  
**Prêt pour la soutenance.**

### Phase 5 : star schema final dans Snowflake (`TMDB_DW.MARTS`)

```
              fct_movies (60)
            ┌──────┴──────┐
            ▼             ▼
     dim_date (47.5k)  dim_language (187)
            
     bridge_movie_genre (161) ──► dim_genre (19)
     dim_country (251) [non liée pour l'instant]
```

**Build runtime :** `dbt build` → 12 modèles + 51 tests = 63/63 PASS  
**Tests fonctionnels :** unique, not_null, accepted_values (tiers), relationships (FK), dbt_utils.unique_combination_of_columns  
**Couches :**
- `staging.stg_*` (views) : typage depuis `RAW.*`
- `staging.int_movies_with_metrics` (view) : score composite + flag is_recent
- `marts.fct_movies` + `marts.dim_*` (tables matérialisées)
- `marts.bridge_movie_genre` : pont N-N

**Décisions techniques Phase 5 :**
- Macro `generate_schema_name` overridée → utilise directement `STAGING` / `MARTS` sans préfixe `<target>_<custom>`
- `dbt_utils.date_spine` au lieu de `GENERATOR(rowcount =>)` (Snowflake exige une constante littérale)
- Auth DBT via clé RSA réutilisée (`SNOWFLAKE_PRIVATE_KEY_PATH`)

### Phase 4 : choix techniques retenus
- **Auth Snowflake : paire de clés RSA** (PKCS8 unencrypted, dans `config/keys/`)
  → MFA obligatoire sur trial → password seul ne fonctionne pas en automatique
  → Clé publique enrôlée via `ALTER USER MIAORGANA SET RSA_PUBLIC_KEY=...`
- **Internal stage** (pas external S3) car MinIO local inaccessible depuis Snowflake Cloud
- **`MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE`** : mapping auto Parquet ↔ table
- **Dossier renommé `snowflake_load/`** pour ne pas shadow le package PyPI `snowflake.connector`

### Sortie Phase 3 (validée runtime, 60 films de test)
- `curated/dim_genre/`       : 19 lignes
- `curated/dim_country/`     : 251 lignes
- `curated/dim_language/`    : 187 lignes
- `curated/movies_enriched/` : 60 lignes (avec `release_decade`, `popularity_tier`, `vote_tier`, libellé langue)
- `curated/movie_genres/`    : 161 lignes (pont N-N film ↔ genre, exploded)

### ⚠️ Note Phase 3 : budget/revenue absents
L'endpoint `/discover/movie` (utilisé en ingestion) **ne retourne pas** budget ni revenue.
Pour avoir ces colonnes (et donc ROI/profit), il faudrait appeler `/movie/{id}` pour chaque film.
→ Décision : reporté en post-soutenance (sinon ×500 appels API par page).
→ Conséquence : pas de calculs ROI/profit dans `movies_enriched`.

## Notes runtime importantes

- **Spark sous Windows natif → KO** (manque `winutils.exe`/`HADOOP_HOME`).  
  → On exécute Spark via Docker (`apache/spark:3.5.1-python3`, `--user 0`).  
  → Pour Airflow plus tard : utiliser le `DockerOperator` ou `BashOperator` qui appelle `docker run`.

- **Commande de validation Phase 2** :
  ```bash
  docker run --rm --user 0 --network finalpipelinev1_default \
    -v "$(pwd):/app" -w /app \
    -e MINIO_ENDPOINT=http://minio:9000 \
    -e MINIO_ACCESS_KEY=minioadmin -e MINIO_SECRET_KEY=minioadmin \
    -e MINIO_BUCKET=tmdb-lake -e INGESTION_DATE=2026-04-27 \
    -e PYTHONPATH=/app \
    apache/spark:3.5.1-python3 \
    /opt/spark/bin/spark-submit \
      --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
      spark/staging.py
  ```
