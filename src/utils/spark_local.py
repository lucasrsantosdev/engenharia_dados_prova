from pyspark.sql import SparkSession


def build_spark(app_name: str, **kwargs) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        # 🔥 ESSENCIAL PRA WINDOWS
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
        .config("spark.hadoop.fs.file.impl.disable.cache", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    return spark