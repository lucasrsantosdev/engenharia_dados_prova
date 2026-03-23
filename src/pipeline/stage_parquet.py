from __future__ import annotations
import logging
from pathlib import Path
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.utils.spark import build_spark
from config import SETTINGS

# Configura logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

def process_clientes(spark: SparkSession):
    """Lê os dados de clientes do raw"""
    raw_clientes_path = SETTINGS.raw_clientes
    logger.info(f"🚀 Lendo clientes de: {raw_clientes_path}")
    df = spark.read.option("mergeSchema", "true").parquet(raw_clientes_path)
    return df

def process_enderecos(spark: SparkSession):
    """Lê os dados de endereços do raw"""
    raw_enderecos_path = SETTINGS.raw_enderecos
    logger.info(f"🚀 Lendo endereços de: {raw_enderecos_path}")
    df = spark.read.option("mergeSchema", "true").parquet(raw_enderecos_path)
    return df

def run_stage():
    """Executa o stage de leitura e escrita dos dados localmente"""
    data_processamento = datetime.today().strftime("%Y-%m-%d")
    
    # Cria SparkSession igual ao raw
    spark = build_spark(
        app_name="engenharia_dados_prova_stage",
        aws_access_key_id=SETTINGS.aws_access_key_id,
        aws_secret_access_key=SETTINGS.aws_secret_access_key,
        aws_region=SETTINGS.aws_region
    )
    logger.info("SparkSession criada com sucesso")

    # Processamento clientes
    clientes = process_clientes(spark)
    logger.info(f"Clientes lidos: {clientes.count()} registros")

    # Processamento endereços
    enderecos = process_enderecos(spark)
    logger.info(f"Endereços lidos: {enderecos.count()} registros")

    # Cria diretórios stage locais se não existirem
    Path(SETTINGS.stage_clientes).mkdir(parents=True, exist_ok=True)
    Path(SETTINGS.stage_enderecos).mkdir(parents=True, exist_ok=True)

    # Salva parquet stage localmente **particionado por data_processamento**
    clientes.withColumn("data_processamento", F.lit(data_processamento)) \
        .write.mode("overwrite").partitionBy("data_processamento").parquet(SETTINGS.stage_clientes)
    enderecos.withColumn("data_processamento", F.lit(data_processamento)) \
        .write.mode("overwrite").partitionBy("data_processamento").parquet(SETTINGS.stage_enderecos)

    logger.info("Stage finalizado com sucesso")

if __name__ == "__main__":
    run_stage()