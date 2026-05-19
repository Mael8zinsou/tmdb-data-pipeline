# Notice de démonstration — Pipeline TMDB

**Soutenance :** 2026-05-19 · M2 Data Engineer · YNOV
**Durée totale :** 30 minutes (20 min démonstration + 10 min questions)
**Format :** démonstration live, écran partagé

---

## Table des matières

1. [Préparation (T-30min)](#1-préparation-t-30min)
2. [Introduction et architecture (T+0, 3 min)](#2-introduction-et-architecture-t0-3-min)
3. [Démonstration live (15 min)](#3-démonstration-live-15-min)
4. [Synthèse de clôture (1 min)](#4-synthèse-de-clôture-1-min)
5. [Questions anticipées](#5-questions-anticipées)
6. [Procédures de remédiation](#6-procédures-de-remédiation)
7. [Annexes — valeurs de référence](#7-annexes--valeurs-de-référence)
8. [Liste de contrôle finale](#8-liste-de-contrôle-finale)

---

## 1. Préparation (T-30min)

### 1.1 Matériel et environnement

- Laptop branché secteur (consommation stack : ~3 Go RAM)
- Connexion internet stable (TMDB API, Snowflake Cloud)
- Docker Desktop actif depuis ≥ 5 minutes
- Cable HDMI/USB-C validé sur l'écran de la salle
- `TMDB_MAX_PAGES=3` dans `.env` (60 films, exécution ~6 min)
- Snowflake `TMDB_DW.RAW.*` et `TMDB_DW.MARTS.*` dans l'état attendu

### 1.2 Vérification de l'état de la stack

```bash
docker compose ps                                                    # 7 containers Up attendus
curl -s http://localhost:8080/health | python -m json.tool           # Airflow
curl -s -u admin:admin http://localhost:3000/api/health              # Grafana
curl -s http://localhost:9090/-/healthy                              # Prometheus
```

Si la stack est down :
```bash
docker compose up -d
```

Attendre 30 secondes que l'init Airflow termine avant de poursuivre.

### 1.3 Reset optionnel (si idempotence à démontrer)

**MinIO :**
```bash
docker run --rm --network finalpipelinev1_default --entrypoint sh minio/mc:latest \
  -c "mc alias set l http://minio:9000 minioadmin minioadmin && mc rm --recursive --force l/tmdb-lake/"
```

**Snowflake** (Snowsight, rôle `PIPELINE_ROLE`) :
```sql
TRUNCATE TABLE TMDB_DW.RAW.MOVIES_ENRICHED;
TRUNCATE TABLE TMDB_DW.RAW.MOVIE_GENRES;
TRUNCATE TABLE TMDB_DW.RAW.DIM_GENRE;
TRUNCATE TABLE TMDB_DW.RAW.DIM_COUNTRY;
TRUNCATE TABLE TMDB_DW.RAW.DIM_LANGUAGE;
```

### 1.4 Onglets navigateur (ouvrir dans cet ordre)

| # | URL | Login | Usage |
|---|---|---|---|
| 1 | https://github.com/Mael8zinsou/tmdb-data-pipeline | — | Repo + CI |
| 2 | http://localhost:8080 | admin / admin | Airflow UI (vue Graph) |
| 3 | http://localhost:3000 | admin / admin | Grafana — dashboard "TMDB Pipeline" |
| 4 | http://localhost:9001 | minioadmin / minioadmin | MinIO console |
| 5 | https://app.snowflake.com | (compte) | Snowsight |
| 6 | https://github.com/Mael8zinsou/tmdb-data-pipeline/actions | — | Historique CI |

### 1.5 Terminal

- Une fenêtre, deux onglets.
- Onglet 1 : positionné dans le répertoire projet.
- Onglet 2 : `key_command.md` ouvert pour copier les requêtes Snowflake.

---

## 2. Introduction et architecture (T+0, 3 min)

### 2.1 Contexte (30 s)

Énoncer :
- Source : API REST TMDB, ~500 000 films exploitables, pagination native.
- Objectif : pipeline distribuée medallion (RAW → STAGING → CURATED → MARTS) couvrant ingestion, traitement, modélisation analytique, restitution.

### 2.2 Schéma d'architecture (1 min 30)

Afficher le README sur GitHub et présenter les couches :

| # | Couche | Technologie | Justification |
|---|---|---|---|
| 1 | Ingestion | Python + `requests` | Pagination TMDB, retry exponentiel |
| 2 | Data Lake | MinIO (S3-compatible) | Mêmes APIs que S3/GCS, gratuit en local |
| 3 | Traitement | PySpark 3.5.1 (Docker) | Distribué, scalable |
| 4 | Warehouse | Snowflake (auth RSA) | Standard industrie |
| 5 | Modélisation | DBT + dbt_utils | SQL versionné, lineage, 51 tests |
| 6 | Marts | Star schema | Standard analytique |

### 2.3 Orchestration et observabilité (1 min)

Énoncer :
- Orchestration : Airflow, DAG 8 tasks, déclenchable on-demand.
- Observabilité : Prometheus + Grafana, dashboard auto-provisionné.
- CI : GitHub Actions, vérification automatique à chaque push.

---

## 3. Démonstration live (15 min)

### Étape 1 — Repo GitHub et CI (1 min, onglet 1)

**Actions :**
1. Ouvrir le repo public.
2. Pointer le badge CI vert.
3. Faire défiler le README (schéma, stack, décisions techniques).
4. Cliquer sur "Actions" pour montrer l'historique des runs.

**Points à énoncer :**
- Repo public, code versionné, historique de commits explicite.
- CI : ruff lint + Airflow DAG parse + dbt parse, exécution ~1 min.
- Aucun secret committé (`.env`, clés RSA, profiles gitignored).

### Étape 2 — Vue d'ensemble Airflow (1 min, onglet 2)

**Actions :**
1. Ouvrir `tmdb_pipeline` dans l'UI Airflow.
2. Basculer sur l'onglet "Graph".
3. Énumérer les 8 tasks dans l'ordre :
   `extract_tmdb → spark_staging → spark_curated → snowflake_load → dbt_deps → dbt_run → dbt_test → notify_success`

**Points à énoncer :**
- DAG explicite, dépendances déclarées, `schedule=None` (déclenchement manuel).
- 2 PythonOperators + 6 BashOperators.
- Les tasks Spark spawnent des containers via le socket Docker monté dans Airflow.

### Étape 3 — Déclenchement du DAG (30 s, onglet 2)

**Actions :**
1. Activer le toggle ON.
2. Cliquer "Trigger DAG".
3. Vérifier que le run apparaît en état "running".

**Point à énoncer :**
- Durée attendue pour 60 films : ~6 minutes.
- L'exécution sera suivie en temps réel sur Grafana.

### Étape 4 — Monitoring temps réel (3 min, onglet 3)

**Actions :**
1. Basculer sur Grafana, dashboard "TMDB Pipeline — Monitoring".
2. Pointer le panel "Tasks running" qui s'incrémente.
3. Vérifier le panel "Scheduler heartbeat" (~12/min, indicateur vert).
4. Observer le peuplement progressif du panel "Durée par task".

**Points à énoncer :**
- Métriques natives Airflow émises en StatsD, traduites en Prometheus avec labels `dag_id` et `task_id`.
- 5 panels essentiels, ~80 métriques Airflow disponibles.
- Alertmanager non implémenté dans cette version — évolution identifiée.

> Le DAG continue à tourner pendant les étapes 5 et 6.

### Étape 5 — Data Lake MinIO (2 min, onglet 4)

**Actions :**
1. Ouvrir le bucket `tmdb-lake`.
2. Naviguer dans `raw/movies/ingestion_date=<date_du_jour>/`.
3. Visiter ensuite `staging/` puis `curated/`.

**Points à énoncer :**
- 3 niveaux de qualité : RAW (brut, immuable), STAGING (nettoyé), CURATED (enrichi).
- Partitionnement par `ingestion_date` : ré-exécution idempotente et historisation.
- Format Parquet snappy : colonnaire, compressé, interopérable Spark/Snowflake/Pandas.

### Étape 6 — Snowflake (3 min, onglet 5)

**Actions :**
1. Ouvrir `TMDB_DW.RAW` et lister les 5 tables.
2. Vérifier : `SELECT COUNT(*) FROM TMDB_DW.RAW.MOVIES_ENRICHED;` → 60.
3. Ouvrir `TMDB_DW.MARTS` et lister `fct_movies`, `bridge_movie_genre`, `dim_*`.
4. Exécuter la requête analytique :

```sql
USE ROLE PIPELINE_ROLE;
USE DATABASE TMDB_DW;

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

**Points à énoncer :**
- Authentification par paire de clés RSA — MFA obligatoire sur trial, password seul inopérant.
- Internal stage Snowflake — MinIO local inaccessible depuis Snowflake Cloud.
- `COPY INTO` avec `MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE` — mapping automatique Parquet/table.

### Étape 7 — DBT et star schema (2 min, Snowsight)

**Actions :**
1. Afficher la table `bridge_movie_genre` (relation N-N films/genres).
2. Mentionner la macro custom `generate_schema_name`.
3. Optionnel — exécuter en terminal :
   ```bash
   docker exec finalpipelinev1-airflow-scheduler-1 bash -c "cd /opt/airflow/dbt && dbt test"
   ```

**Points à énoncer :**
- 12 modèles répartis en 3 couches : staging (views) → intermediate (views) → marts (tables).
- 51 tests : unicité PK, not_null, accepted_values, relationships (intégrité référentielle).
- Macro `generate_schema_name` overridée pour cibler directement `STAGING` et `MARTS`.

### Étape 8 — Pipeline terminée (1 min, onglet 2)

**Actions :**
1. Revenir sur l'UI Airflow — le DAG doit être en état "success" (vert).
2. Ouvrir la task `notify_success`, onglet "Log".
3. Revenir sur Grafana — le panel "Durée par task" affiche maintenant les valeurs finales.

**Points à énoncer :**
- 8 tasks séquentielles, 60 films, ~6 minutes.
- Exécution tracée, observable, idempotente.

---

## 4. Synthèse de clôture (1 min)

Énoncer :

> Pipeline complète bout-en-bout, code versionné, tests automatisés à toutes les couches, observabilité temps réel. Les décisions techniques sont documentées dans `doc.md` et `key_command.md`. Évolutions identifiées : déploiement cloud (EKS + S3), Alertmanager, dashboard analytique sur les MARTS.

---

## 5. Questions anticipées

### Architecture et conception

**Pourquoi MinIO et pas S3 AWS directement ?**
> Développement local sans coût ni dépendance cloud. MinIO expose la même API S3 — `boto3` et `hadoop-aws` fonctionnent à l'identique en local et en production.

**Pourquoi Spark pour 60 films ?**
> Démonstration de la capacité scalable. `TMDB_MAX_PAGES=500` donne 10 000 films ; à ce volume Pandas devient inadapté. Spark conserve le même code à scale linéaire.

**Pourquoi Snowflake et pas PostgreSQL ?**
> Séparation compute/storage, scaling automatique, standard de facto du data engineering moderne.

**Pourquoi DBT plutôt que des views Snowflake natives ?**
> Versionning Git, tests automatiques (51 ici), lineage explicite, modularité staging/intermediate/marts.

### Implémentation technique

**Comment Airflow lance Spark si Spark n'est pas dans son image ?**
> Le socket Docker `/var/run/docker.sock` est monté dans le container Airflow. Le BashOperator exécute `docker run apache/spark:3.5.1-python3 spark-submit ...` qui spawne un container Spark éphémère. Pattern équivalent en production : `DockerOperator` ou `KubernetesPodOperator`.

**Comment sont gérés les secrets ?**
> `.env` gitignored, clés RSA dans `config/keys/` gitignored, variables injectées par docker-compose côté Airflow, DBT lit via `env_var()`. En production : Vault ou AWS Secrets Manager.

**Comment est garantie l'idempotence ?**
> MinIO : partitions `ingestion_date=YYYY-MM-DD`. Snowflake : `TRUNCATE` avant `COPY INTO`. DBT : `CREATE OR REPLACE`. Pipeline réexécutable sans duplication.

**Que se passe-t-il si TMDB devient inaccessible en cours d'exécution ?**
> Trois niveaux de retry :
> 1. `requests.Session` + `Retry(total=5, backoff_factor=1.5)` — 5 tentatives par requête HTTP.
> 2. Boucle interne 4 essais par page TMDB.
> 3. Airflow : `retries=1, retry_delay=2min`.

### Qualité et production

**Quelle est la couverture des tests ?**
> CI GitHub Actions : ruff lint, parse DAG, parse DBT à chaque push. DBT : 51 tests automatiques. Tests unitaires Python non implémentés — trade-off assumé pour le périmètre soutenance.

**Quel serait le déploiement en production ?**
> Airflow : Helm sur Kubernetes (ou Cloud Composer / MWAA). Spark : Databricks / EKS / Dataproc. Stockage : S3 ou GCS. Snowflake conservé tel quel. Secrets : Vault / Secrets Manager. Monitoring : Prometheus + Grafana scalables, Thanos pour la rétention longue.

**Que changerait à 1 milliard de films ?**
> Ingestion : parallélisée sur plusieurs workers, split par range de pages. Spark : cluster managé, partitionnement plus fin. Snowflake : warehouse plus volumineux pendant `COPY INTO`, clustering key sur `release_year`. Code DBT et structure du DAG inchangés.

### Monitoring

**Pourquoi StatsD intermédiaire et pas un client Prometheus direct ?**
> Airflow émet nativement en StatsD (UDP). Il n'existe pas de client Prometheus officiel intégré au scheduler. `statsd-exporter` traduit avec mapping configurable — pattern recommandé par la documentation Airflow.

**Pourquoi Spark n'est-il pas monitoré ?**
> Containers Spark éphémères dans cette version (un par task). En production sur cluster managé, Spark expose ses propres métriques (driver UI, executor) directement scrapables par Prometheus.

---

## 6. Procédures de remédiation

### 6.1 `extract_tmdb` échoue (API TMDB inaccessible)

1. Annoncer : exécution sur l'état précédent (déjà chargé dans Snowflake).
2. Basculer sur Snowsight et exécuter la requête analytique de l'étape 6.
3. Montrer la stratégie de retry dans le DAG.

### 6.2 `spark_staging` ou `spark_curated` échoue (exit 125)

1. Vérifier `PROJECT_HOST_PATH` dans `.env` (format `C:/Users/...` avec forward slashes).
2. Si besoin : `docker compose restart airflow-scheduler`.

### 6.3 `snowflake_load` échoue (JWT invalide)

1. Dans Airflow UI : "Clear" sur la task pour relance automatique.
2. Si récurrent : vérifier l'horloge système (la JWT est sensible au time skew).

### 6.4 Stack défaillante globalement

1. `docker compose restart` (~30 s).
2. Si échec persistant : basculer en mode "présentation statique" — repo GitHub, CI verte, screenshots de référence.

### 6.5 Grafana affiche "No data"

- Cause probable : aucun run récent.
- Action : déclencher un DAG avant la démonstration, ou commenter le panel "Scheduler heartbeat" qui reste actif en permanence.

### 6.6 Résolution écran inadaptée

- Grafana : zoom navigateur à 80 %.
- Snowsight : mode plein écran (F11).
- Airflow Graph : ajuster via le contrôle de zoom intégré.

---

## 7. Annexes — valeurs de référence

### 7.1 Volumes par couche (3 pages, 60 films)

| Couche | Dataset | Lignes |
|---|---|---:|
| RAW (MinIO) | movies | 60 |
| RAW (MinIO) | genres | 19 |
| RAW (MinIO) | countries | 251 |
| RAW (MinIO) | languages | 187 |
| RAW (Snowflake) | MOVIES_ENRICHED | 60 |
| RAW (Snowflake) | MOVIE_GENRES | 161 |
| MARTS | fct_movies | 60 |
| MARTS | dim_date | 47 846 |
| MARTS | dim_genre | 19 |
| MARTS | dim_country | 251 |
| MARTS | dim_language | 187 |
| MARTS | bridge_movie_genre | 161 |

### 7.2 Métriques Grafana en run sain

| Métrique | Valeur attendue |
|---|---|
| `airflow_scheduler_heartbeat` rate | ≈ 12/min |
| `airflow_executor_open_slots` | 32 |
| `airflow_executor_running_tasks` | 0 (idle) / 1 (pendant un run) |
| Durée `extract_tmdb` | 30-60 s |
| Durée `spark_staging` | ~90 s |
| Durée `spark_curated` | ~120 s |
| Durée `snowflake_load` | ~30 s |
| Durée `dbt_run` | 30-60 s |
| Durée totale DAG | 5-7 min |

### 7.3 Endpoints et identifiants

| Service | URL | User | Password |
|---|---|---|---|
| Airflow | http://localhost:8080 | admin | admin |
| MinIO console | http://localhost:9001 | minioadmin | minioadmin |
| Grafana | http://localhost:3000 | admin | admin |
| Prometheus | http://localhost:9090 | — | — |
| statsd-exporter | http://localhost:9102/metrics | — | — |
| GitHub | https://github.com/Mael8zinsou/tmdb-data-pipeline | — | — |

### 7.4 Containers de la stack

```
finalpipelinev1-postgres-1            Airflow metadata DB
finalpipelinev1-minio-1               Data Lake
finalpipelinev1-airflow-webserver-1   Airflow UI
finalpipelinev1-airflow-scheduler-1   Orchestrateur, spawn Spark
finalpipelinev1-statsd-exporter-1     Métriques Airflow → Prometheus
finalpipelinev1-prometheus-1          TSDB et scrape
finalpipelinev1-grafana-1             Dashboards
```

Container éphémère `apache/spark:3.5.1-python3` spawné pendant `spark_staging` et `spark_curated`.

---

## 8. Liste de contrôle finale

- [ ] Stack Docker active depuis ≥ 5 min
- [ ] Run récent visible dans Airflow (Grafana doit afficher des durées)
- [ ] Onglets navigateur ouverts dans l'ordre 1 → 6
- [ ] Terminal positionné sur le répertoire projet
- [ ] Snowsight connecté avec rôle `PIPELINE_ROLE`
- [ ] Affichage écran de la salle validé
- [ ] Documentation de référence accessible (`doc.md`, `key_command.md`)

---

**Dernière mise à jour :** 2026-05-19
