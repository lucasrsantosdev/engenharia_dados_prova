# src/pipeline/stage.py

import os
import logging
from pyspark.sql import Window
from pyspark.sql.functions import row_number, col, desc, current_timestamp

from src.utils.spark import build_spark
from config import SETTINGS  # Certifique-se de que config.py está na raiz do projeto

# Configura logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)


def process_clientes(spark):
    """Lê os dados de clientes do raw path"""
    path_clientes = SETTINGS.raw_clientes
    if not os.path.exists(path_clientes):
        raise FileNotFoundError(f"Diretório de clientes não encontrado: {path_clientes}")

    logger.info(f"🚀 Lendo clientes de: {path_clientes}")
    df = spark.read.parquet(path_clientes)
    return df


def process_enderecos(spark):
    """Lê os dados de endereços do raw path"""
    path_enderecos = SETTINGS.raw_enderecos
    if not os.path.exists(path_enderecos):
        raise FileNotFoundError(f"Diretório de endereços não encontrado: {path_enderecos}")

    logger.info(f"🚀 Lendo endereços de: {path_enderecos}")
    df = spark.read.parquet(path_enderecos)
    return df


def run_stage():
    """Executa o stage de leitura e escrita dos dados"""
    spark = build_spark(app_name="engenharia_dados_prova_stage")
    logger.info("SparkSession criada com sucesso")

    # Processamento clientes
    clientes = process_clientes(spark)
    logger.info(f"Clientes lidos: {clientes.count()} registros")

    # Processamento endereços
    enderecos = process_enderecos(spark)
    logger.info(f"Endereços lidos: {enderecos.count()} registros")

    # Cria diretórios stage se não existirem
    stage_clientes_path = SETTINGS.stage_clientes
    stage_enderecos_path = SETTINGS.stage_enderecos

    os.makedirs(stage_clientes_path, exist_ok=True)
    os.makedirs(stage_enderecos_path, exist_ok=True)

    # Salva parquet stage
    clientes.write.mode("overwrite").parquet(stage_clientes_path)
    enderecos.write.mode("overwrite").parquet(stage_enderecos_path)

    logger.info("Stage finalizado com sucesso")


if __name__ == "__main__":
    run_stage()