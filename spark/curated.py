"""
Spark Curated : STAGING → CURATED

Construit les datasets enrichis prêts à être chargés dans Snowflake.

Lecture  : s3a://<bucket>/staging/<dataset>/ingestion_date=YYYY-MM-DD/
Écriture : s3a://<bucket>/curated/<dataset>/ingestion_date=YYYY-MM-DD/

Sorties produites :
  curated/movies_enriched/  : 1 ligne par film, avec tous les libellés et calculs dérivés
  curated/movie_genres/     : pont film ↔ genre (relation N-N explosée)
  curated/dim_genre/        : référentiel genres
  curated/dim_country/      : référentiel pays
  curated/dim_language/     : référentiel langues
"""

import os
import logging
from datetime import date

from pyspark.sql import DataFrame, functions as F

from spark.utils import get_spark, s3a_path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

MINIO_BUCKET   = os.environ["MINIO_BUCKET"]
INGESTION_DATE = os.getenv("INGESTION_DATE", str(date.today()))

POPULARITY_HIGH_THRESHOLD = 100.0

# ─── I/O ──────────────────────────────────────────────────────────────────────

def read_staging(spark, dataset: str) -> DataFrame:
    path = s3a_path(MINIO_BUCKET, f"staging/{dataset}/ingestion_date={INGESTION_DATE}/")
    logger.info(f"Lecture : {path}")
    return spark.read.parquet(path)

def write_curated(df: DataFrame, dataset: str) -> int:
    path = s3a_path(MINIO_BUCKET, f"curated/{dataset}/ingestion_date={INGESTION_DATE}/")
    n = df.count()
    df.coalesce(1).write.mode("overwrite").parquet(path)
    logger.info(f"Écrit {n:,} lignes → {path}")
    return n

# ─── Transformations ──────────────────────────────────────────────────────────

def build_dim_genre(genres: DataFrame) -> DataFrame:
    return (
        genres
        .select(
            F.col("id").alias("genre_id"),
            F.col("name").alias("genre_name"),
        )
    )

def build_dim_country(countries: DataFrame) -> DataFrame:
    return (
        countries
        .select(
            F.col("iso_3166_1").alias("country_code"),
            F.col("english_name").alias("country_name"),
            F.col("native_name").alias("country_native_name"),
        )
    )

def build_dim_language(languages: DataFrame) -> DataFrame:
    return (
        languages
        .select(
            F.col("iso_639_1").alias("language_code"),
            F.col("english_name").alias("language_name"),
            F.col("name").alias("language_native_name"),
        )
    )

def build_movies_enriched(movies: DataFrame, dim_language: DataFrame) -> DataFrame:
    """
    Film enrichi avec :
      - jointure langue (libellé)
      - colonnes dérivées : release_year (déjà là), decade, has_release_date,
        popularity_tier, vote_tier
    Note : budget/revenue ne sont pas dans /discover/movie (endpoint léger).
           Ils seraient récupérés via /movie/{id} (TODO post-soutenance).
           ROI/profit ne sont donc pas calculés ici.
    """
    return (
        movies.alias("m")
        .join(
            dim_language.alias("l"),
            F.col("m.original_language") == F.col("l.language_code"),
            "left",
        )
        .select(
            F.col("m.movie_id"),
            F.col("m.title"),
            F.col("m.original_title"),
            F.col("m.overview"),
            F.col("m.original_language").alias("language_code"),
            F.col("l.language_name"),
            F.col("m.release_date"),
            F.col("m.release_year"),
            (F.floor(F.col("m.release_year") / 10) * 10).cast("int").alias("release_decade"),
            F.col("m.release_date").isNotNull().alias("has_release_date"),
            F.col("m.popularity"),
            F.when(F.col("m.popularity") >= POPULARITY_HIGH_THRESHOLD, F.lit("high"))
              .when(F.col("m.popularity") >= 20.0, F.lit("medium"))
              .otherwise(F.lit("low"))
              .alias("popularity_tier"),
            F.col("m.vote_average"),
            F.col("m.vote_count"),
            F.when(F.col("m.vote_average") >= 7.5, F.lit("excellent"))
              .when(F.col("m.vote_average") >= 6.0, F.lit("good"))
              .when(F.col("m.vote_average") >= 4.0, F.lit("average"))
              .otherwise(F.lit("poor"))
              .alias("vote_tier"),
            F.col("m.ingestion_date"),
        )
    )

def build_movie_genres(movies: DataFrame, dim_genre: DataFrame) -> DataFrame:
    """
    Pont film ↔ genre (table de relation N-N).
    Explose le tableau genre_ids → 1 ligne par couple (movie_id, genre_id).
    """
    return (
        movies
        .select(
            F.col("movie_id"),
            F.explode_outer("genre_ids").alias("genre_id"),
        )
        .filter(F.col("genre_id").isNotNull())
        .join(dim_genre, on="genre_id", how="left")
        .select("movie_id", "genre_id", "genre_name")
        .dropDuplicates(["movie_id", "genre_id"])
    )

# ─── Point d'entrée ───────────────────────────────────────────────────────────

def run() -> dict:
    spark = get_spark("tmdb-curated")
    try:
        # Lecture des sources staging
        movies    = read_staging(spark, "movies")
        genres    = read_staging(spark, "genres")
        countries = read_staging(spark, "countries")
        languages = read_staging(spark, "languages")

        # Construction des dimensions
        dim_genre    = build_dim_genre(genres)
        dim_country  = build_dim_country(countries)
        dim_language = build_dim_language(languages)

        # Construction des datasets enrichis
        movies_enriched = build_movies_enriched(movies, dim_language)
        movie_genres    = build_movie_genres(movies, dim_genre)

        # Écriture
        n_genre    = write_curated(dim_genre,        "dim_genre")
        n_country  = write_curated(dim_country,      "dim_country")
        n_language = write_curated(dim_language,     "dim_language")
        n_movies   = write_curated(movies_enriched,  "movies_enriched")
        n_mg       = write_curated(movie_genres,     "movie_genres")

        summary = {
            "ingestion_date":  INGESTION_DATE,
            "dim_genre":       n_genre,
            "dim_country":     n_country,
            "dim_language":    n_language,
            "movies_enriched": n_movies,
            "movie_genres":    n_mg,
            "status":          "success",
        }
        logger.info(f"Curated terminé : {summary}")
        return summary
    finally:
        spark.stop()

if __name__ == "__main__":
    run()
