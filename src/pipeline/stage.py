# stage.py
from src.utils.spark import build_spark
from src.utils.config import SETTINGS
import logging
import os

# Configura logging simples
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_clientes(spark):
    path_clientes = SETTINGS.raw_clientes
    if not os.path.exists(path_clientes):
        raise FileNotFoundError(f"Diretório de clientes não encontrado: {path_clientes}")
    
    logger.info(f"🚀 Lendo clientes de: {path_clientes}")
    df = spark.read.parquet(path_clientes)
    return df

def process_enderecos(spark):
    path_enderecos = SETTINGS.raw_enderecos
    if not os.path.exists(path_enderecos):
        raise FileNotFoundError(f"Diretório de endereços não encontrado: {path_enderecos}")

    logger.info(f"🚀 Lendo endereços de: {path_enderecos}")
    df = spark.read.parquet(path_enderecos)
    return df

def run_stage():
    spark = build_spark()
    logger.info("SparkSession criada com sucesso")

    # Processamento clientes
    clientes = process_clientes(spark)
    logger.info(f"Clientes lidos: {clientes.count()} registros")

    # Processamento endereços
    enderecos = process_enderecos(spark)
    logger.info(f"Endereços lidos: {enderecos.count()} registros")

    # Exemplo: salvar stage local
    stage_clientes_path = SETTINGS.stage_clientes
    stage_enderecos_path = SETTINGS.stage_enderecos

    os.makedirs(stage_clientes_path, exist_ok=True)
    os.makedirs(stage_enderecos_path, exist_ok=True)

    clientes.write.mode("overwrite").parquet(stage_clientes_path)
    enderecos.write.mode("overwrite").parquet(stage_enderecos_path)
    logger.info("Stage finalizado com sucesso")

if __name__ == "__main__":
    run_stage()