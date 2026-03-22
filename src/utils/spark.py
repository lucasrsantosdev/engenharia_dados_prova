# spark_local.py
from pyspark.sql import SparkSession

def build_spark(app_name="engenharia_dados_prova_stage"):
    spark = (
        SparkSession
        .builder
        .appName(app_name)
        .config("spark.sql.warehouse.dir", "C:/tmp")  # local temp dir
        .master("local[*]")
        .getOrCreate()
    )
    return spark