import boto3
import os
from dotenv import load_dotenv

load_dotenv()


class S3Client:
    def __init__(self):
        self.bucket = os.getenv("S3_BUCKET")
        self.region = os.getenv("AWS_REGION")

        self.client = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=self.region,
        )

    def upload_file(self, local_path: str, s3_path: str):
        """
        Faz upload de um arquivo local para o S3.
        """

        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Arquivo não encontrado: {local_path}")

        print(f"[UPLOAD] {local_path} -> s3://{self.bucket}/{s3_path}")

        self.client.upload_file(
            Filename=local_path,
            Bucket=self.bucket,
            Key=s3_path,
        )

        print("[OK] Upload concluído")