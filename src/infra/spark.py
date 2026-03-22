from __future__ import annotations

from typing import Optional

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def build_spark(app_name: str, aws_access_key_id: str | None = None, aws_secret_access_key: str | None = None, aws_region: str | None = None) -> SparkSession:
    """
    Cria SparkSession com Delta Lake habilitado.

    Observação: a escrita/leitura S3 depende do ambiente (hadoop-aws).
    Este helper apenas injeta credenciais via Hadoop conf quando informadas.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    hconf = spark._jsc.hadoopConfiguration()
    if aws_access_key_id and aws_secret_access_key:
        hconf.set("fs.s3a.access.key", aws_access_key_id)
        hconf.set("fs.s3a.secret.key", aws_secret_access_key)
    if aws_region:
        hconf.set("fs.s3a.endpoint.region", aws_region)

    return spark

