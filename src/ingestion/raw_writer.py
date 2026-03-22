from pyspark.sql import SparkSession
import json
import os

from config import SETTINGS
from src.utils.s3_client import S3Client


def write_raw_parquet(spark: SparkSession, df, path: str, data_processamento: str):
    """
    Escreve DataFrame em Parquet.

    Estratégia:
    - Sempre salva LOCAL
    - Depois faz upload para S3 via boto3
    """

    if df is None or df.empty:
        return

    # =========================
    # GARANTE QUE PATH É LOCAL
    # =========================
    if path.startswith(("s3://", "s3a://")):
        # converte para estrutura local equivalente
        path = path.replace(
            f"s3://{SETTINGS.s3_bucket}/{SETTINGS.user_folder}/",
            "data/"
        )

    # =========================
    # SALVA LOCAL
    # =========================
    df = df.copy()
    df["data_processamento"] = data_processamento

    output_path = os.path.join(
        path,
        f"data_processamento={data_processamento}"
    )

    os.makedirs(output_path, exist_ok=True)

    file_path = os.path.join(output_path, "data.parquet")

    df.to_parquet(file_path, index=False)

    print(f"[OK] Dados salvos localmente: {file_path}")

    # =========================
    # UPLOAD PARA S3
    # =========================
    try:
        s3 = S3Client()

        # monta chave S3 corretamente
        relative_path = file_path.replace("\\", "/").split("data/")[-1]

        s3_key = f"{SETTINGS.user_folder}/{relative_path}"

        s3.upload_file(file_path, s3_key)

        print(f"[S3 OK] Upload realizado: s3://{SETTINGS.s3_bucket}/{s3_key}")

    except Exception as e:
        print(f"[ERRO S3] Falha no upload: {e}")


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