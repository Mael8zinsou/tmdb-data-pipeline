"""
Helpers Spark : SparkSession configurée pour MinIO (S3A).
"""
import os
from pyspark.sql import SparkSession

def get_spark(app_name: str = "tmdb-pipeline") -> SparkSession:
    """SparkSession local mode + connecteur S3A vers MinIO."""

    minio_endpoint = os.environ["MINIO_ENDPOINT"]
    minio_access   = os.environ["MINIO_ACCESS_KEY"]
    minio_secret   = os.environ["MINIO_SECRET_KEY"]

    # Packages Maven : hadoop-aws + aws-sdk (compat hadoop 3.3.x livré avec Spark 3.5)
    packages = ",".join([
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    ])

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.jars.packages", packages)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def s3a_path(bucket: str, key: str) -> str:
    return f"s3a://{bucket}/{key.lstrip('/')}"
