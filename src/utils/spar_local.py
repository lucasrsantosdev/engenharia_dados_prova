# c/utils/spark.py
from pyspark.sql import SparkSession

def build_spark(app_name="engenharia_dados_prova_stage",
                aws_access_key_id=None,
                aws_secret_access_key=None,
                aws_region=None):
    """
    Cria uma SparkSession configurada para rodar localmente ou integrada com S3.
    - Se não passar credenciais AWS, roda local.
    - Se passar credenciais AWS, configura acesso via s3a.
    """

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.warehouse.dir", "C:/tmp")  # pasta temporária local
        .master("local[*]")
    )

    # =========================
    # Configura S3 se credenciais forem fornecidas
    # =========================
    if aws_access_key_id and aws_secret_access_key and aws_region:
        builder = (
            builder
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
            .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id)
            .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key)
            .config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
        )

    # =========================
    # Fix para Windows
    # =========================
    builder = (
        builder
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
        .config("spark.hadoop.fs.file.impl.disable.cache", "true")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    return spark