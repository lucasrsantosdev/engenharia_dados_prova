from pyspark.sql import SparkSession


def build_spark(app_name: str, **kwargs) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
    )

    # 🔥 CONFIG S3 (ESSENCIAL)
    if kwargs.get("aws_access_key_id"):
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.access.key", kwargs["aws_access_key_id"])
            .config("spark.hadoop.fs.s3a.secret.key", kwargs["aws_secret_access_key"])
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        )

    # 🔧 Windows fix (mantém)
    builder = (
        builder
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
        .config("spark.hadoop.fs.file.impl.disable.cache", "true")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    return spark