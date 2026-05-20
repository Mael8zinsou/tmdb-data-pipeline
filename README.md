# 🎬 TMDB Data Engineering Pipeline

[![CI](https://github.com/Mael8zinsou/tmdb-data-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/Mael8zinsou/tmdb-data-pipeline/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.11-blue.svg)
![Airflow](https://img.shields.io/badge/airflow-2.9.1-017CEE.svg)
![Spark](https://img.shields.io/badge/spark-3.5.1-E25A1C.svg)
![DBT](https://img.shields.io/badge/dbt-1.8.3-FF694B.svg)
![Snowflake](https://img.shields.io/badge/warehouse-snowflake-29B5E8.svg)

> **Pipeline data engineering distribuée end-to-end** : ingestion API → Data Lake → traitement Spark → Data Warehouse → modélisation dimensionnelle → monitoring temps réel.
>
> Projet final M2 Data Engineer · YNOV · Maël Zinsou · 2026


## ✨ Ce que fait ce projet

Une pipeline complète qui démontre l'ensemble du cycle de vie de la donnée, des outils du marché jusqu'aux bonnes pratiques de production :

| Métrique | Valeur |
|---|---|
| **Durée end-to-end** | ~6 minutes (60 films de démo) |
| **Couches medallion** | RAW → STAGING → CURATED → MARTS |
| **Modèles DBT** | 12 modèles, 51 tests, **63 / 63 PASS** |
| **Tasks Airflow** | 8 tasks séquentielles, déclenchables on-demand |
| **Services Docker** | 7 containers orchestrés (Airflow, Postgres, MinIO, Spark, Prometheus, Grafana, statsd-exporter) |
| **CI GitHub Actions** | ~1 min (ruff + DAG parse + dbt parse) |
| **Métriques exposées** | ~80 métriques Airflow temps réel |


## 🏗️ Architecture

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
                                                                      │
                                                       ┌──────────────▼──────────────┐
                                                       │  MinIO (staging/)           │
                                                       └──────────────┬──────────────┘
                                                                      │
                                                       ┌──────────────▼──────────────┐
                                                       │  Spark Curated (Docker)     │
                                                       │  jointures + enrichissement │
                                                       └──────────────┬──────────────┘
                                                                      │
                                                       ┌──────────────▼──────────────┐
                                                       │  MinIO (curated/)           │
                                                       └──────────────┬──────────────┘
                                                                      │ PUT + COPY INTO
                                                                      │ (RSA key auth)
                                                       ┌──────────────▼──────────────┐
                                                       │  Snowflake TMDB_DW.RAW      │
                                                       └──────────────┬──────────────┘
                                                                      │ DBT
                                                       ┌──────────────▼──────────────┐
                                                       │  STAGING (views) + MARTS    │
                                                       │  star schema + 51 tests     │
                                                       └─────────────────────────────┘

         ┌─ Monitoring (transverse) ──────────────────────────────────────────┐
         │   Airflow ─StatsD UDP─► statsd-exporter ─scrape─► Prometheus       │
         │                                                       ▼            │
         │                                                    Grafana         │
         │                              (5 panels auto-provisionnés)          │
         └─────────────────────────────────────────────────────────────────────┘
```

> Architecture détaillée + diagramme de flux + modèle de données : [`doc.md`](doc.md).


## 🛠️ Stack technologique

| Couche | Techno | Pourquoi ce choix |
|---|---|---|
| **Ingestion** | Python 3.11 + `requests` | Pagination native TMDB, retry exponentiel `Session+Retry` |
| **Data Lake** | MinIO (S3-compatible) | Mêmes APIs que S3/GCS prod, gratuit en local |
| **Traitement** | PySpark 3.5.1 (Docker) | Distribué, scalable, isole les déps Hadoop |
| **Warehouse** | Snowflake (auth RSA) | Standard industrie, séparation compute/storage |
| **Modélisation** | DBT Core 1.8.3 + `dbt_utils` | SQL-as-code, lineage explicite, tests automatiques |
| **Orchestration** | Apache Airflow 2.9.1 | DAG, retry, observabilité native StatsD |
| **Monitoring** | Prometheus 2.51 + Grafana 10.4 | Standard cloud-native, métriques + dashboards |
| **Infra** | Docker Compose v2 | 7 services en une commande |
| **CI** | GitHub Actions | Ruff lint + Airflow DAG parse + dbt parse |


## 📊 Modèle de données - Star schema

```
              fct_movies (60)
              ┌────┴───────────┬─────────────┐
              ▼                ▼             ▼
       dim_date          dim_language   bridge_movie_genre (161)
       (47 846)            (187)               │
                                               ▼
                                          dim_genre (19)

       dim_country (251)  [référentiel]
```

Star schema classique avec un pont N-N (`bridge_movie_genre`) pour la relation films ↔ genres. Modélisé en DBT (`marts/`), matérialisé en tables Snowflake, testé automatiquement.


## 📈 Monitoring temps réel

Dashboard Grafana **auto-provisionné** au démarrage de la stack - aucune config manuelle :

| Panel | Métrique sous-jacente |
|---|---|
| Scheduler heartbeat (rate/min) | `rate(airflow_scheduler_heartbeat[1m]) * 60` |
| DAG runs succeeded (24h) | `sum(increase(airflow_dagrun_succeeded_total[24h]))` |
| DAG runs failed (24h) | `sum(increase(airflow_dagrun_failed_total[24h]))` |
| Tasks running | `airflow_executor_running_tasks` |
| Durée par task (p99) | `airflow_task_duration_seconds{dag_id="tmdb_pipeline"}` |

Chaîne : Airflow émet en StatsD (UDP 9125) → `statsd-exporter` traduit en Prometheus → scrape toutes les 15s → Grafana query.

> Accès : http://localhost:3000 (admin / admin) après `docker compose up -d`.


## 🎯 Décisions techniques notables

7 décisions qui ont structuré le projet - un résumé honnête des trade-offs :

| Décision | Pourquoi |
|---|---|
| **Auth Snowflake par paire RSA** | MFA obligatoire sur trial → password seul incompatible avec l'automation |
| **Internal stage Snowflake** | MinIO local inaccessible depuis Snowflake Cloud → pas d'external stage possible |
| **Spark dans Docker** | Spark sous Windows natif KO (`HADOOP_HOME`, `winutils.exe`) → container portable |
| **Image Airflow custom** | `docker.io` Debian trop ancien (v1.41 < 1.44) → installation de `docker-ce-cli` officiel |
| **Macro `generate_schema_name` custom** | DBT préfixe par défaut (`<target>_<schema>`) → override pour utiliser `STAGING`/`MARTS` directs |
| **`dbt_utils.date_spine` pour `dim_date`** | Snowflake `GENERATOR(rowcount =>)` exige un littéral constant |
| **StatsD intermédiaire** | Airflow émet nativement en StatsD, pas en Prometheus → `statsd-exporter` traduit avec labels |

> 12 décisions détaillées (avec le *pourquoi* approfondi) : [`doc.md`](doc.md#10-approfondissement--décisions-techniques-détaillées).


## 🚀 Quick start

### Prérequis

- Docker Desktop ≥ 4.x
- Compte [Snowflake](https://snowflake.com/start) (trial gratuit)
- Clé [TMDB API](https://themoviedb.org/settings/api) (gratuite)
- OpenSSL (pour la paire RSA)

### 3 étapes

```bash
# 1. Configuration
cp .env.example .env
# Éditer .env : TMDB_API_KEY + SNOWFLAKE_ACCOUNT + SNOWFLAKE_USER + PROJECT_HOST_PATH

# 2. Setup Snowflake (création DB + rôle + clé RSA)
# → Voir key_command.md §3.3 et §3.4

# 3. Démarrage de la stack
docker compose up -d
```

Services exposés une fois la stack démarrée :

| Service | URL | Login |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| MinIO console | http://localhost:9001 | minioadmin / minioadmin |
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | - |

**Déclencher la pipeline** : http://localhost:8080 → `tmdb_pipeline` → toggle ON → ▶ Trigger.

> Setup détaillé pas-à-pas : [`key_command.md`](key_command.md#3-setup-initial-one-time).


## 📂 Structure du projet (vue haute)

```
Final pipeline v1/
├── ingestion/         Phase 1  - Extraction TMDB → Parquet
├── spark/             Phases 2-3 - Staging + Curated (PySpark)
├── snowflake_load/    Phase 4  - COPY INTO Snowflake
├── dbt/               Phase 5  - Star schema + 51 tests
├── dags/              Phase 6  - DAG Airflow (8 tasks)
├── monitoring/        Phase 8  - Prometheus + Grafana provisioning
├── .github/workflows/ CI       - ruff + DAG parse + dbt parse
├── doc.md             Documentation technique exhaustive
├── key_command.md     Runbook opérationnel (commandes, debug, reset)
└── notice_démo.md     Script de la démo soutenance
```

> Arborescence complète détaillée : [`doc.md`](doc.md#3-structure-du-projet).


## Tests & qualité

- **CI GitHub Actions** ([badge ci-dessus](https://github.com/Mael8zinsou/tmdb-data-pipeline/actions)) : ruff lint + Airflow DAG parse + dbt parse, ~1 min, vert sur `main`.
- **51 tests DBT** automatiques : unicité PK, not_null, accepted_values, relationships (FK), `dbt_utils.unique_combination_of_columns`.
- **Pipeline idempotente** : ré-exécutable sur la même date sans duplication (partitions MinIO par `ingestion_date`, `TRUNCATE` Snowflake, `CREATE OR REPLACE` DBT).
- **Secrets jamais committés** : `.env`, clés RSA, `target/`, `dbt_packages/` tous gitignored.


## Roadmap - Améliorations possibles

| Court terme | Impact |
|---|---|
| Ajout de `enrich_movie_details` task → endpoint `/movie/{id}` | Récupérer `budget` / `revenue` → KPIs ROI dans MARTS |
| Tests pytest sur `extract_tmdb.py` et `spark/staging.py` | Couverture des transformations Python |
| Bump des actions GitHub vers Node.js 24 | Anti-deprecation (Node 20 EOL juin 2026) |

| Moyen terme | Impact |
|---|---|
| **Alertmanager** (Prometheus) avec route Slack/Email | Notif automatique sur DAG failed / scheduler down |
| **Dashboard analytique** (Metabase / Superset / Power BI) sur MARTS | Visualisation des KPIs (top genres, distribution notes, etc.) |
| **Schedule automatique** (`@daily`) au lieu de `schedule=None` | Pipeline en mode "production" |

| Long terme | Impact |
|---|---|
| Migration cloud (S3 + EKS/Cloud Composer + Snowflake conservé) | Production-grade scalable |
| Cluster Spark managé (Databricks / Dataproc / EMR) | Volumes > 100k films |
| Secrets manager (Vault / AWS Secrets Manager) | Sécurité production |
| CDC / streaming (Kafka + Debezium) pour ingestion incrémentale | Vraie pipeline temps réel |


## 📚 Documentation

Trois documents complémentaires selon l'usage :

| Document | Pour quoi | Quand le consulter |
|---|---|---|
| [`doc.md`](doc.md) | Documentation technique exhaustive | Comprendre les décisions, l'archi, les phases |
| [`key_command.md`](key_command.md) | Runbook opérationnel | Reproduire, exploiter, debugger |
| [`notice_démo.md`](notice_démo.md) | Script démo soutenance | Préparation et déroulé de la présentation |


## 🎓 Contexte académique

Projet final du Master 2 Data Engineer, **YNOV**, dans le cadre du module *Stockage et Traitement des Données Distribuées*. L'objectif pédagogique : concevoir une pipeline data réaliste mobilisant l'ensemble du stack moderne (orchestration, data lake, distributed processing, warehouse, transformations SQL versionnées, observabilité).

**Auteur :** Maël Zinsou -
**Contact :** - [maelzinsou@proton.me](mailto:maelzinsou@proton.me) - [LinkedIn](https://www.linkedin.com/in/mael-mike-zinsou-data-engineer/)