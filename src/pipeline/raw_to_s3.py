import os
import boto3
from config import RAW_CLIENTES, RAW_ENDERECOS, DATA_PROCESSAMENTO, S3_BUCKET

def upload_parquet_s3(local_path: str, s3_path: str):
    s3 = boto3.client("s3")
    for root, dirs, files in os.walk(local_path):
        for file in files:
            if file.endswith(".parquet"):
                local_file = os.path.join(root, file)
                key = os.path.join(s3_path, os.path.relpath(local_file, local_path)).replace("\\","/")
                s3.upload_file(local_file, S3_BUCKET, key)
                print(f"Upload S3: {key}")

# Upload clientes
upload_parquet_s3(
    f"{RAW_CLIENTES}/data_processamento={DATA_PROCESSAMENTO}",
    f"raw/clientes/data_processamento={DATA_PROCESSAMENTO}"
)

# Upload endereços
upload_parquet_s3(
    f"{RAW_ENDERECOS}/data_processamento={DATA_PROCESSAMENTO}",
    f"raw/enderecos/data_processamento={DATA_PROCESSAMENTO}"
)