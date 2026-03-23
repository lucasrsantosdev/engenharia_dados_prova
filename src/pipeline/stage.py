# src/pipeline/stage.py
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Caminhos dos arquivos Parquet
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw"))
CLIENTES_PATH = os.path.join(BASE_DIR, "clientes", "clientes.parquet")
ENDERECOS_PATH = os.path.join(BASE_DIR, "enderecos", "enderecos.parquet")

def build_spark(app_name="stage_pipeline_windows"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.warehouse.dir", "C:/tmp")  # diretório temporário Windows
        .getOrCreate()
    )
    return spark

def process_clientes(spark):
    logger.info(f"Lendo clientes de: {CLIENTES_PATH}")
    df = spark.read.option("mergeSchema", "true").parquet(CLIENTES_PATH)
    return df

def process_enderecos(spark):
    logger.info(f"Lendo endereços de: {ENDERECOS_PATH}")
    df = spark.read.option("mergeSchema", "true").parquet(ENDERECOS_PATH)
    return df

def run_stage():
    logger.info("Iniciando pipeline stage (Windows friendly)")
    spark = build_spark()
    logger.info("SparkSession criada com sucesso")

    clientes = process_clientes(spark)
    enderecos = process_enderecos(spark)

    logger.info("Fazendo join entre clientes e endereços")
    joined = clientes.join(enderecos, on="cliente_id", how="left")
    
    logger.info(f"Total de registros após join: {joined.count()}")
    joined.show(truncate=False)

    spark.stop()
    logger.info("SparkSession finalizada com sucesso")

if __name__ == "__main__":
    run_stage()