"""
Chargement Curated (MinIO) → Snowflake (schéma RAW).

Stratégie :
  1. Télécharger les Parquet depuis MinIO vers un tmp local
  2. PUT vers un internal stage Snowflake (TMDB_DW.RAW.TMDB_STAGE)
  3. COPY INTO avec MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
     → laisse Snowflake mapper colonnes Parquet ↔ colonnes table

Pourquoi internal stage et pas external (S3) ?
  → MinIO est en localhost:9000, donc inaccessible depuis Snowflake Cloud.
    L'internal stage est la seule option viable pour cette archi locale.

Tables créées (idempotent CREATE OR REPLACE) :
  TMDB_DW.RAW.DIM_GENRE
  TMDB_DW.RAW.DIM_COUNTRY
  TMDB_DW.RAW.DIM_LANGUAGE
  TMDB_DW.RAW.MOVIES_ENRICHED
  TMDB_DW.RAW.MOVIE_GENRES
"""

import os
import io
import logging
import tempfile
from datetime import date
from pathlib import Path

import boto3
from botocore.client import Config
from cryptography.hazmat.primitives import serialization
import snowflake.connector

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────

MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS   = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET   = os.environ["MINIO_SECRET_KEY"]
MINIO_BUCKET   = os.environ["MINIO_BUCKET"]

SF_ACCOUNT   = os.environ["SNOWFLAKE_ACCOUNT"]
SF_USER      = os.environ["SNOWFLAKE_USER"]
SF_KEY_PATH  = os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]
SF_DATABASE  = os.environ["SNOWFLAKE_DATABASE"]
SF_WAREHOUSE = os.environ["SNOWFLAKE_WAREHOUSE"]
SF_ROLE      = os.environ["SNOWFLAKE_ROLE"]
SF_SCHEMA    = "RAW"  # cible des COPY INTO

INGESTION_DATE = os.getenv("INGESTION_DATE", str(date.today()))

STAGE_NAME       = "TMDB_STAGE"
FILE_FORMAT_NAME = "PARQUET_FORMAT"

# Mapping dataset curated → DDL Snowflake
TABLE_DDL = {
    "dim_genre": """
        CREATE OR REPLACE TABLE RAW.DIM_GENRE (
            genre_id   NUMBER,
            genre_name VARCHAR
        )
    """,
    "dim_country": """
        CREATE OR REPLACE TABLE RAW.DIM_COUNTRY (
            country_code        VARCHAR,
            country_name        VARCHAR,
            country_native_name VARCHAR
        )
    """,
    "dim_language": """
        CREATE OR REPLACE TABLE RAW.DIM_LANGUAGE (
            language_code        VARCHAR,
            language_name        VARCHAR,
            language_native_name VARCHAR
        )
    """,
    "movies_enriched": """
        CREATE OR REPLACE TABLE RAW.MOVIES_ENRICHED (
            movie_id         NUMBER,
            title            VARCHAR,
            original_title   VARCHAR,
            overview         VARCHAR,
            language_code    VARCHAR,
            language_name    VARCHAR,
            release_date     DATE,
            release_year     NUMBER,
            release_decade   NUMBER,
            has_release_date BOOLEAN,
            popularity       FLOAT,
            popularity_tier  VARCHAR,
            vote_average     FLOAT,
            vote_count       NUMBER,
            vote_tier        VARCHAR,
            ingestion_date   VARCHAR
        )
    """,
    "movie_genres": """
        CREATE OR REPLACE TABLE RAW.MOVIE_GENRES (
            movie_id   NUMBER,
            genre_id   NUMBER,
            genre_name VARCHAR
        )
    """,
}

# ─── Helpers ──────────────────────────────────────────────────────────────────

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

def load_private_key() -> bytes:
    """Charge la clé RSA PKCS8 (non chiffrée) au format DER attendu par le connecteur."""
    with open(SF_KEY_PATH, "rb") as f:
        pkey = serialization.load_pem_private_key(f.read(), password=None)
    return pkey.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

def get_snowflake_conn():
    return snowflake.connector.connect(
        account=SF_ACCOUNT,
        user=SF_USER,
        private_key=load_private_key(),
        database=SF_DATABASE,
        schema=SF_SCHEMA,
        warehouse=SF_WAREHOUSE,
        role=SF_ROLE,
    )

def download_parquets(s3, dataset: str, dest_dir: Path) -> list[Path]:
    """Télécharge tous les Parquet d'un dataset curated dans dest_dir."""
    prefix = f"curated/{dataset}/ingestion_date={INGESTION_DATE}/"
    resp = s3.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=prefix)
    files = []
    for obj in resp.get("Contents", []):
        key = obj["Key"]
        if not key.endswith(".parquet"):
            continue
        local = dest_dir / Path(key).name
        s3.download_file(MINIO_BUCKET, key, str(local))
        files.append(local)
        logger.info(f"  ↓ {key} → {local.name} ({obj['Size']:,} B)")
    return files

# ─── Setup Snowflake ──────────────────────────────────────────────────────────

def setup_snowflake(cur):
    """Crée le file format, le stage et les tables (idempotent)."""
    logger.info("=== Setup Snowflake (file format + stage + tables) ===")

    cur.execute(f"USE WAREHOUSE {SF_WAREHOUSE}")
    cur.execute(f"USE DATABASE {SF_DATABASE}")
    cur.execute(f"USE SCHEMA {SF_SCHEMA}")

    cur.execute(f"""
        CREATE OR REPLACE FILE FORMAT {FILE_FORMAT_NAME}
        TYPE = PARQUET
        COMPRESSION = SNAPPY
    """)
    logger.info(f"  ✓ FILE FORMAT {FILE_FORMAT_NAME}")

    cur.execute(f"""
        CREATE STAGE IF NOT EXISTS {STAGE_NAME}
        FILE_FORMAT = {FILE_FORMAT_NAME}
    """)
    logger.info(f"  ✓ STAGE {STAGE_NAME}")

    for dataset, ddl in TABLE_DDL.items():
        cur.execute(ddl)
        logger.info(f"  ✓ TABLE RAW.{dataset.upper()}")

# ─── Chargement par dataset ───────────────────────────────────────────────────

def load_dataset(cur, s3, dataset: str, tmp_dir: Path) -> int:
    """PUT + COPY INTO d'un dataset."""
    table = dataset.upper()
    logger.info(f"=== Load {dataset} → RAW.{table} ===")

    # 1. Download MinIO → local
    dest = tmp_dir / dataset
    dest.mkdir(parents=True, exist_ok=True)
    files = download_parquets(s3, dataset, dest)
    if not files:
        logger.warning(f"  Aucun fichier pour {dataset}, skip.")
        return 0

    # 2. PUT vers stage
    stage_path = f"@{STAGE_NAME}/{dataset}/ingestion_date={INGESTION_DATE}/"
    cur.execute(f"REMOVE {stage_path}")  # idempotence : nettoie les anciens fichiers
    for f in files:
        # AUTO_COMPRESS=FALSE car déjà snappy
        put_path = f.as_posix()
        cur.execute(
            f"PUT 'file://{put_path}' {stage_path} "
            f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        )
        logger.info(f"  ↑ PUT {f.name}")

    # 3. COPY INTO avec mapping automatique des colonnes
    cur.execute(f"TRUNCATE TABLE RAW.{table}")
    cur.execute(f"""
        COPY INTO RAW.{table}
        FROM {stage_path}
        FILE_FORMAT = (FORMAT_NAME = {FILE_FORMAT_NAME})
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = 'ABORT_STATEMENT'
    """)

    # 4. Validation count
    cur.execute(f"SELECT COUNT(*) FROM RAW.{table}")
    n = cur.fetchone()[0]
    logger.info(f"  ✓ RAW.{table} : {n:,} lignes chargées")
    return n

# ─── Point d'entrée ───────────────────────────────────────────────────────────

def run() -> dict:
    s3 = get_s3_client()
    conn = get_snowflake_conn()
    cur = conn.cursor()
    try:
        setup_snowflake(cur)

        with tempfile.TemporaryDirectory(prefix="tmdb_load_") as tmp:
            tmp_dir = Path(tmp)
            counts = {}
            for dataset in TABLE_DDL.keys():
                counts[dataset] = load_dataset(cur, s3, dataset, tmp_dir)

        summary = {
            "ingestion_date": INGESTION_DATE,
            **counts,
            "status": "success",
        }
        logger.info(f"Snowflake load terminé : {summary}")
        return summary
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    run()
