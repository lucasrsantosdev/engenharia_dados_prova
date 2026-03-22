from pyspark.sql import Window
from pyspark.sql.functions import row_number, col, desc, current_timestamp

from src.utils.spark import build_spark
from config_aws import SETTINGS


def process_clientes(spark):
    df = spark.read.parquet(SETTINGS.raw_clientes)

    window = Window.partitionBy("id_cliente").orderBy(desc("data_evento"))

    df = (
        df
        .withColumn("rn", row_number().over(window))
        .filter(col("rn") == 1)
        .drop("rn")
        .withColumn("data_atualizacao", current_timestamp())
    )

    return df


def process_enderecos(spark):
    df = spark.read.parquet(SETTINGS.raw_enderecos)

    window = Window.partitionBy("id_endereco").orderBy(desc("data_evento"))

    df = (
        df
        .withColumn("rn", row_number().over(window))
        .filter(col("rn") == 1)
        .drop("rn")
        .withColumn("data_atualizacao", current_timestamp())
    )

    return df


def write_stage(df, path: str):
    """
    Escrita idempotente (SCD Type 1 simplificado)
    """

    (
        df
        .write
        .mode("overwrite")  # 🔥 garante idempotência
        .parquet(path)
    )

    print(f"[STAGE OK] Dados salvos em: {path}")


def run_stage():
    spark = build_spark("engenharia_dados_prova_stage")

    print("🚀 Processando clientes...")
    clientes = process_clientes(spark)

    print("🚀 Processando endereços...")
    enderecos = process_enderecos(spark)

    print("💾 Salvando stage clientes...")
    write_stage(clientes, SETTINGS.stage_clientes)

    print("💾 Salvando stage endereços...")
    write_stage(enderecos, SETTINGS.stage_enderecos)

    print("✅ Stage finalizada com sucesso!")


if __name__ == "__main__":
    run_stage()