# src/utils/spark.py
from pathlib import Path
import os
from pyspark.sql import SparkSession

def build_spark(app_name: str,
                aws_access_key_id: str | None = None,
                aws_secret_access_key: str | None = None,
                aws_region: str | None = None,
                java_home: str | None = None,
                hadoop_home: str | None = None,
                warehouse_dir: str | None = None,
                enable_s3: bool | None = None,
                s3_endpoints: dict | None = None):
    # Injetar envs só se vierem no SETTINGS
    if java_home and not os.environ.get("JAVA_HOME"):
        os.environ["JAVA_HOME"] = java_home
        os.environ["PATH"] = os.environ["PATH"] + os.pathsep + str(Path(java_home) / "bin")

    if hadoop_home and not os.environ.get("HADOOP_HOME"):
        os.environ["HADOOP_HOME"] = hadoop_home
        os.environ["PATH"] = os.environ["PATH"] + os.pathsep + str(Path(hadoop_home) / "bin")

    # warehouse local por padrão
    if not warehouse_dir:
        warehouse_dir = str(Path(".") / ".spark_local" / "warehouse")
    Path(warehouse_dir).mkdir(parents=True, exist_ok=True)

    builder = (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.ui.showConsoleProgress", "true")
    )

    # S3A só se for habilitado
    if enable_s3:
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
        )
        if aws_region:
            builder = builder.config("spark.hadoop.fs.s3a.region", aws_region)
        if aws_access_key_id and aws_secret_access_key:
            builder = builder.config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
            )
            os.environ["AWS_ACCESS_KEY_ID"] = aws_access_key_id
            os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret_access_key
        if s3_endpoints and "endpoint" in s3_endpoints:
            builder = builder.config("spark.hadoop.fs.s3a.endpoint", s3_endpoints["endpoint"])

    spark = builder.getOrCreate()
    return spark