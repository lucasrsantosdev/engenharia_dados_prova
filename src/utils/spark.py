from pyspark.sql import SparkSession


def build_spark(
    app_name: str,
    aws_access_key_id: str | None = None,
    aws_secret_access_key: str | None = None,
    aws_region: str | None = None,
) -> SparkSession:
    """
    Cria sessão Spark configurada para S3.
    """

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
    )

    # Configuração para S3
    if aws_access_key_id and aws_secret_access_key:
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id)
            .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key)
            .config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        )

    spark = builder.getOrCreate()

    return spark