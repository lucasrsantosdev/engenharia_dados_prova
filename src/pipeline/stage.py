# src/pipeline/stage.py

import os
import logging
from pyspark.sql import SparkSession
from src.utils.spark import build_spark
from config import SETTINGS

# Configura logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)


def process_clientes(spark: SparkSession):
    """Lê os dados de clientes do raw local"""
    raw_clientes_path = SETTINGS.raw_clientes
    logger.info(f"🚀 Lendo clientes de: {raw_clientes_path}")
    
    if not os.path.exists(raw_clientes_path):
        raise FileNotFoundError(f"Diretório raw de clientes não existe: {raw_clientes_path}")
    
    df = spark.read.parquet(raw_clientes_path)
    return df


def process_enderecos(spark: SparkSession):
    """Lê os dados de endereços do raw local"""
    raw_enderecos_path = SETTINGS.raw_enderecos
    logger.info(f"🚀 Lendo endereços de: {raw_enderecos_path}")
    
    if not os.path.exists(raw_enderecos_path):
        raise FileNotFoundError(f"Diretório raw de endereços não existe: {raw_enderecos_path}")
    
    df = spark.read.parquet(raw_enderecos_path)
    return df


def run_stage():
    """Executa o stage de leitura e escrita dos dados localmente"""
    # Cria SparkSession local
    spark = build_spark(app_name="engenharia_dados_prova_stage")
    logger.info("SparkSession criada com sucesso")

    # Processamento clientes
    clientes = process_clientes(spark)
    logger.info(f"Clientes lidos: {clientes.count()} registros")

    # Processamento endereços
    enderecos = process_enderecos(spark)
    logger.info(f"Endereços lidos: {enderecos.count()} registros")

    # Cria diretórios stage locais se não existirem
    os.makedirs(SETTINGS.stage_clientes, exist_ok=True)
    os.makedirs(SETTINGS.stage_enderecos, exist_ok=True)

    # Salva parquet stage localmente
    clientes.write.mode("overwrite").parquet(SETTINGS.stage_clientes)
    enderecos.write.mode("overwrite").parquet(SETTINGS.stage_enderecos)

    logger.info("Stage finalizado com sucesso")


if __name__ == "__main__":
    run_stage()