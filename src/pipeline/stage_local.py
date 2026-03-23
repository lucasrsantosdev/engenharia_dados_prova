import logging
from utils.spark_local import build_spark
from config import RAW_CLIENTES, RAW_ENDERECOS, STAGE_CLIENTES, STAGE_ENDERECOS, ANALYTICS_CLIENTES
from pyspark.sql.functions import col, current_date, datediff, to_date, current_timestamp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_stage():
    logger.info("=== Iniciando pipeline Stage (Windows friendly) ===")
    spark = build_spark()

    # Lê raw
    logger.info(f"Lendo clientes de: {RAW_CLIENTES}")
    df_clientes = spark.read.option("mergeSchema", "true").parquet(RAW_CLIENTES)

    logger.info(f"Lendo endereços de: {RAW_ENDERECOS}")
    df_enderecos = spark.read.option("mergeSchema", "true").parquet(RAW_ENDERECOS)

    # Mantém apenas último evento (SCD Type 1)
    df_clientes_stage = df_clientes.orderBy("data_evento").dropDuplicates(["id_cliente"]) \
                                   .withColumn("data_atualizacao", current_timestamp())
    df_enderecos_stage = df_enderecos.orderBy("data_evento").dropDuplicates(["id_endereco"]) \
                                    .withColumn("data_atualizacao", current_timestamp())

    # Salva stage
    df_clientes_stage.write.format("delta").mode("overwrite").save(STAGE_CLIENTES)
    df_enderecos_stage.write.format("delta").mode("overwrite").save(STAGE_ENDERECOS)
    logger.info(f"Stage concluído: clientes={df_clientes_stage.count()}, endereços={df_enderecos_stage.count()}")

    # --- Analytics ---
    logger.info("=== Iniciando pipeline Analytics ===")

    # Apenas clientes ativos
    df_clientes_ativos = df_clientes_stage.filter(col("status") == "ativo")

    # Join LEFT
    df_join = df_clientes_ativos.join(df_enderecos_stage, on="id_cliente", how="left")

    # Calcula idade
    df_join = df_join.withColumn("idade", (datediff(current_date(), to_date(col("data_nascimento"))) / 365).cast("int"))

    # Salva analytics
    df_join.write.mode("overwrite").parquet(ANALYTICS_CLIENTES)
    logger.info(f"Analytics concluído: total registros={df_join.count()}")

    spark.stop()
    logger.info("=== Pipeline finalizado com sucesso ===")


if __name__ == "__main__":
    run_stage()