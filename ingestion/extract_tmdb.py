"""
Extraction TMDB API → MinIO (Data Lake RAW)

Endpoints extraits :
  - /discover/movie  : catalogue principal (avec pagination)
  - /genre/movie/list : référentiel genres
  - /configuration/countries : référentiel pays
  - /configuration/languages : référentiel langues

Sortie : fichiers Parquet partitionnés par ingestion_date dans MinIO
  tmdb-lake/raw/movies/ingestion_date=YYYY-MM-DD/movies_part_NNN.parquet
  tmdb-lake/raw/genres/ingestion_date=YYYY-MM-DD/genres.parquet
  tmdb-lake/raw/countries/ingestion_date=YYYY-MM-DD/countries.parquet
  tmdb-lake/raw/languages/ingestion_date=YYYY-MM-DD/languages.parquet
"""

import os
import io
import time
import logging
from datetime import date
from typing import Generator

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
import boto3
from botocore.client import Config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────

TMDB_API_KEY   = os.environ["TMDB_API_KEY"]
TMDB_BASE_URL  = "https://api.themoviedb.org/3"
TMDB_LANGUAGE  = "en-US"
TMDB_MAX_PAGES = int(os.getenv("TMDB_MAX_PAGES", "500"))  # max 500 (limite TMDB)
TMDB_RATE_SLEEP = 0.25  # secondes entre requêtes (TMDB : 40 req/10s)

MINIO_ENDPOINT  = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS    = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET    = os.environ["MINIO_SECRET_KEY"]
MINIO_BUCKET    = os.environ["MINIO_BUCKET"]

INGESTION_DATE  = os.getenv("INGESTION_DATE", str(date.today()))

# ─── Client S3 / MinIO ────────────────────────────────────────────────────────

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

def upload_parquet(s3, df: pd.DataFrame, s3_key: str) -> None:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow", compression="snappy")
    buf.seek(0)
    s3.put_object(Bucket=MINIO_BUCKET, Key=s3_key, Body=buf.getvalue())
    logger.info(f"Uploaded {len(df):,} rows → s3://{MINIO_BUCKET}/{s3_key}")

# ─── TMDB helpers ─────────────────────────────────────────────────────────────

_session = None

def get_session() -> requests.Session:
    global _session
    if _session is None:
        s = requests.Session()
        retry = Retry(
            total=5,
            backoff_factor=1.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
            raise_on_status=False,
        )
        s.mount("https://", HTTPAdapter(max_retries=retry))
        _session = s
    return _session

def tmdb_get(endpoint: str, params: dict = None) -> dict:
    url = f"{TMDB_BASE_URL}{endpoint}"
    p = {"api_key": TMDB_API_KEY, "language": TMDB_LANGUAGE}
    if params:
        p.update(params)
    last_err = None
    for attempt in range(4):
        try:
            resp = get_session().get(url, params=p, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except (requests.ConnectionError, requests.Timeout) as e:
            last_err = e
            wait = 2 ** attempt
            logger.warning(f"TMDB {endpoint} failed (attempt {attempt+1}/4): {e}. Retrying in {wait}s")
            time.sleep(wait)
    raise RuntimeError(f"TMDB {endpoint} failed after retries") from last_err

def paginate_discover(extra_params: dict = None) -> Generator[list, None, None]:
    """Itère sur toutes les pages de /discover/movie."""
    params = {
        "sort_by": "popularity.desc",
        "include_adult": "false",
        "include_video": "false",
        **(extra_params or {}),
    }
    first = tmdb_get("/discover/movie", {**params, "page": 1})
    total_pages = min(first.get("total_pages", 1), TMDB_MAX_PAGES)
    logger.info(f"Total pages disponibles : {total_pages}")
    yield first["results"]

    for page in range(2, total_pages + 1):
        time.sleep(TMDB_RATE_SLEEP)
        data = tmdb_get("/discover/movie", {**params, "page": page})
        yield data["results"]
        if page % 50 == 0:
            logger.info(f"  → page {page}/{total_pages}")

# ─── Extracteurs ──────────────────────────────────────────────────────────────

def extract_movies(s3) -> int:
    logger.info("=== Extraction movies (paginated) ===")
    batch_size = 100  # pages regroupées par fichier Parquet
    batch, part = [], 0
    total_rows = 0

    def flush(b, p):
        df = pd.DataFrame(b)
        df["ingestion_date"] = INGESTION_DATE
        key = f"raw/movies/ingestion_date={INGESTION_DATE}/movies_part_{p:04d}.parquet"
        upload_parquet(s3, df, key)
        return len(df)

    for page_results in paginate_discover():
        batch.extend(page_results)
        if len(batch) >= batch_size * 20:  # ~20 résultats/page × 100 pages
            total_rows += flush(batch, part)
            batch, part = [], part + 1

    if batch:
        total_rows += flush(batch, part)

    logger.info(f"Movies extraits : {total_rows:,}")
    return total_rows

def extract_genres(s3) -> None:
    logger.info("=== Extraction genres ===")
    data = tmdb_get("/genre/movie/list")
    df = pd.DataFrame(data["genres"])
    df["ingestion_date"] = INGESTION_DATE
    key = f"raw/genres/ingestion_date={INGESTION_DATE}/genres.parquet"
    upload_parquet(s3, df, key)

def extract_countries(s3) -> None:
    logger.info("=== Extraction countries ===")
    data = tmdb_get("/configuration/countries")
    df = pd.DataFrame(data)
    df["ingestion_date"] = INGESTION_DATE
    key = f"raw/countries/ingestion_date={INGESTION_DATE}/countries.parquet"
    upload_parquet(s3, df, key)

def extract_languages(s3) -> None:
    logger.info("=== Extraction languages ===")
    data = tmdb_get("/configuration/languages")
    df = pd.DataFrame(data)
    df["ingestion_date"] = INGESTION_DATE
    key = f"raw/languages/ingestion_date={INGESTION_DATE}/languages.parquet"
    upload_parquet(s3, df, key)

# ─── Point d'entrée ───────────────────────────────────────────────────────────

def run() -> dict:
    s3 = get_s3_client()
    extract_genres(s3)
    extract_countries(s3)
    extract_languages(s3)
    n_movies = extract_movies(s3)
    summary = {
        "ingestion_date": INGESTION_DATE,
        "movies_extracted": n_movies,
        "status": "success",
    }
    logger.info(f"Extraction terminée : {summary}")
    return summary

if __name__ == "__main__":
    run()
