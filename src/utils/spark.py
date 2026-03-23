from pyspark.sql import SparkSession
import os

def build_spark(
    app_name: str,
    storage_mode: str = "local",
    aws_access_key_id: str | None = None,
    aws_secret_access_key: str | None = None,
    aws_region: str | None = None
) -> SparkSession:
    """
    Cria uma SparkSession configurada para Windows, com suporte opcional a S3.
    
    Parâmetros:
    - app_name: Nome do aplicativo Spark
    - storage_mode: "local" ou "s3"
    - aws_access_key_id, aws_secret_access_key, aws_region: usados se storage_mode="s3"
    """

    # Força Spark a ignorar NativeIO/Parquet/ORC do Windows
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ["PATH"]

    # Cria builder base
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")  # roda localmente usando todos os núcleos
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.warehouse.dir", r"C:\tmp\hive")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
        .config("spark.hadoop.fs.file.impl.disable.cache", "true")
    )

    if storage_mode.lower() == "s3":
        if not all([aws_access_key_id, aws_secret_access_key, aws_region]):
            raise ValueError("Para S3, aws_access_key_id, aws_secret_access_key e aws_region são obrigatórios")

        # Configura S3 com Hadoop
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id)
            .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key)
            .config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
        )
    
    # ⚠️ Certifique-se de instalar hadoop-aws e aws-java-sdk-bundle compatíveis:
    # pip install pyspark
    # e adicionar os jars:
    #   C:\path_to_spark\jars\hadoop-aws-3.x.x.jar
    #   C:\path_to_spark\jars\aws-java-sdk-bundle-1.11.x.jar
    # ou usar SparkSession.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.5,com.amazonaws:aws-java-sdk-bundle:1.12.523")

    spark = builder.getOrCreate()
    return spark

# src/utils/spark.py

from pyspark.sql import SparkSession

def build_spark(app_name: str) -> SparkSession:
    """
    Cria uma SparkSession local, totalmente independente de S3/Hadoop.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")  # usa todos os núcleos
        .config("spark.sql.warehouse.dir", "C:/tmp/spark-warehouse")  # diretório local
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.driver.host", "localhost")
        .getOrCreate()
    )
    return spark