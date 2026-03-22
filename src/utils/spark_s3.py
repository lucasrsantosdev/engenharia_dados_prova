from pyspark.sql import SparkSession


def build_spark(app_name: str, **kwargs) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")

        # 🔥 ESSENCIAL PRA S3
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4"
        )
    )

    # =========================
    # CONFIG S3
    # =========================
    if kwargs.get("aws_access_key_id"):
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.access.key", kwargs["aws_access_key_id"])
            .config("spark.hadoop.fs.s3a.secret.key", kwargs["aws_secret_access_key"])
            .config("spark.hadoop.fs.s3a.endpoint", "s3.sa-east-1.amazonaws.com")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
        )

    # =========================
    # WINDOWS FIX
    # =========================
    builder = (
        builder
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
        .config("spark.hadoop.fs.file.impl.disable.cache", "true")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    return spark