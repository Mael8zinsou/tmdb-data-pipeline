"""
Spark Staging : RAW → STAGING

Transformations appliquées par dataset :
  movies     : typage, parsing dates, dédup sur movie_id, filtrage NaN sur clés
  genres     : typage simple
  countries  : typage simple
  languages  : typage simple

Lecture  : s3a://<bucket>/raw/<dataset>/ingestion_date=YYYY-MM-DD/
Écriture : s3a://<bucket>/staging/<dataset>/ingestion_date=YYYY-MM-DD/
"""

import os
import logging
from datetime import date

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, BooleanType, ArrayType, DateType,
)

from spark.utils import get_spark, s3a_path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

MINIO_BUCKET   = os.environ["MINIO_BUCKET"]
INGESTION_DATE = os.getenv("INGESTION_DATE", str(date.today()))

# ─── Schémas attendus ─────────────────────────────────────────────────────────

MOVIES_RAW_SCHEMA = StructType([
    StructField("adult",             BooleanType(),       True),
    StructField("backdrop_path",     StringType(),        True),
    StructField("genre_ids",         ArrayType(LongType()), True),
    StructField("id",                LongType(),          True),
    StructField("original_language", StringType(),        True),
    StructField("original_title",    StringType(),        True),
    StructField("overview",          StringType(),        True),
    StructField("popularity",        DoubleType(),        True),
    StructField("poster_path",       StringType(),        True),
    StructField("release_date",      StringType(),        True),
    StructField("title",             StringType(),        True),
    StructField("video",             BooleanType(),       True),
    StructField("vote_average",      DoubleType(),        True),
    StructField("vote_count",        LongType(),          True),
    StructField("ingestion_date",    StringType(),        True),
])

# ─── Helpers ──────────────────────────────────────────────────────────────────

def read_raw(spark, dataset: str) -> DataFrame:
    path = s3a_path(MINIO_BUCKET, f"raw/{dataset}/ingestion_date={INGESTION_DATE}/")
    logger.info(f"Lecture : {path}")
    return spark.read.parquet(path)

def write_staging(df: DataFrame, dataset: str) -> int:
    path = s3a_path(MINIO_BUCKET, f"staging/{dataset}/ingestion_date={INGESTION_DATE}/")
    n = df.count()
    df.coalesce(1).write.mode("overwrite").parquet(path)
    logger.info(f"Écrit {n:,} lignes → {path}")
    return n

# ─── Transformations ──────────────────────────────────────────────────────────

def clean_movies(spark) -> int:
    df = read_raw(spark, "movies")
    n_raw = df.count()
    logger.info(f"Movies RAW : {n_raw:,} lignes")

    df = (
        df
        # Typage / renommage
        .withColumnRenamed("id", "movie_id")
        .withColumn("release_date", F.to_date("release_date", "yyyy-MM-dd"))
        .withColumn("popularity",   F.col("popularity").cast("double"))
        .withColumn("vote_average", F.col("vote_average").cast("double"))
        .withColumn("vote_count",   F.col("vote_count").cast("long"))
        # Trim des strings
        .withColumn("title",          F.trim("title"))
        .withColumn("original_title", F.trim("original_title"))
        .withColumn("overview",       F.trim("overview"))
        # Suppression colonnes inutiles pour l'analytique
        .drop("backdrop_path", "poster_path", "video", "adult")
        # Filtrage des films sans clé (movie_id null)
        .filter(F.col("movie_id").isNotNull())
        # Déduplication sur movie_id (garde le plus récent par ingestion_date)
        .dropDuplicates(["movie_id"])
        # Gestion NaN : remplace les nulls texte par "" / les nulls numériques par 0
        .fillna({
            "title": "",
            "original_title": "",
            "overview": "",
            "original_language": "xx",
            "popularity": 0.0,
            "vote_average": 0.0,
            "vote_count": 0,
        })
        # Colonne dérivée utile dès le staging
        .withColumn("release_year", F.year("release_date"))
    )

    n_clean = write_staging(df, "movies")
    logger.info(f"Movies cleaned : {n_raw:,} → {n_clean:,} (supprimés : {n_raw - n_clean:,})")
    return n_clean

def clean_lookup(spark, dataset: str, id_col: str) -> int:
    """Nettoyage générique pour genres / countries / languages."""
    df = read_raw(spark, dataset)
    df = (
        df
        .filter(F.col(id_col).isNotNull())
        .dropDuplicates([id_col])
    )
    return write_staging(df, dataset)

# ─── Point d'entrée ───────────────────────────────────────────────────────────

def run() -> dict:
    spark = get_spark("tmdb-staging")
    try:
        n_movies    = clean_movies(spark)
        n_genres    = clean_lookup(spark, "genres",    "id")
        n_countries = clean_lookup(spark, "countries", "iso_3166_1")
        n_languages = clean_lookup(spark, "languages", "iso_639_1")
        summary = {
            "ingestion_date": INGESTION_DATE,
            "movies":    n_movies,
            "genres":    n_genres,
            "countries": n_countries,
            "languages": n_languages,
            "status":    "success",
        }
        logger.info(f"Staging terminé : {summary}")
        return summary
    finally:
        spark.stop()

if __name__ == "__main__":
    run()
