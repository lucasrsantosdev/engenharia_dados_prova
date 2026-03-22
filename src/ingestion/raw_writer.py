from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import json
import os


def write_raw_parquet(spark: SparkSession, df, s3_path: str, data_processamento: str):
    """
    Escreve DataFrame em Parquet no S3 com particionamento.
    """
    if df is None or df.empty:
        return

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
        .parquet(s3_path)
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