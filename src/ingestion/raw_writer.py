from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import json
import os

from config import SETTINGS


def write_raw_parquet(spark: SparkSession, df, path: str, data_processamento: str):
    """
    Escreve DataFrame em Parquet.

    - LOCAL → usa pandas (sem Spark / sem Hadoop)
    - S3 → usa Spark (particionado)
    """

    if df is None or df.empty:
        return

    # =========================
    # MODO LOCAL (SEM SPARK)
    # =========================
    if SETTINGS.storage_mode == "local":
        df["data_processamento"] = data_processamento

        output_path = os.path.join(
            path,
            f"data_processamento={data_processamento}"
        )

        os.makedirs(output_path, exist_ok=True)

        file_path = os.path.join(output_path, "data.parquet")

        df.to_parquet(file_path, index=False)

        print(f"[OK] Dados salvos em: {file_path}")
        return

    # =========================
    # MODO S3 (SPARK)
    # =========================
    spark_df = spark.createDataFrame(df)

    spark_df = spark_df.withColumn(
        "data_processamento",
        lit(data_processamento)
    )

    (
        spark_df
        .write
        .mode("append")
        .partitionBy("data_processamento")
        .parquet(path)
    )


def write_validation_log(errors, file_path: str):
    """
    Salva erros de validação em JSONL.
    """
    if not errors:
        return

    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    with open(file_path, "w", encoding="utf-8") as f:
        for err in errors:
            if hasattr(err, "as_dict"):
                f.write(json.dumps(err.as_dict(), ensure_ascii=False) + "\n")
            else:
                f.write(json.dumps(err, ensure_ascii=False) + "\n")