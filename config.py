"""
Configurações centralizadas do projeto.

As variáveis abaixo podem ser sobrescritas via variáveis de ambiente (.env):
- S3_BUCKET
- AWS_REGION
- AWS_ACCESS_KEY_ID
- AWS_SECRET_ACCESS_KEY
- USER_FOLDER (formato: nome_sobrenome, em minúsculo)
"""
from dotenv import load_dotenv

import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


def _env(key: str, default: str | None = None) -> str | None:
    value = os.getenv(key)
    if value is None or str(value).strip() == "":
        return default
    return value


@dataclass(frozen=True)
class Settings:
    s3_bucket: str
    aws_region: str
    aws_access_key_id: str | None
    aws_secret_access_key: str | None
    user_folder: str

    @property
    def s3_base(self) -> str:
        return f"s3://{self.s3_bucket}/{self.user_folder}"

    @property
    def raw_clientes(self) -> str:
        return f"{self.s3_base}/raw/clientes/"

    @property
    def raw_enderecos(self) -> str:
        return f"{self.s3_base}/raw/enderecos/"

    @property
    def stage_clientes(self) -> str:
        return f"{self.s3_base}/stage/clientes/"

    @property
    def stage_enderecos(self) -> str:
        return f"{self.s3_base}/stage/enderecos/"

    @property
    def analytics_clientes(self) -> str:
        return f"{self.s3_base}/analytics/clientes/"

    @property
    def athena_results(self) -> str:
        return f"{self.s3_base}/athena_results/"


SETTINGS = Settings(
    s3_bucket=_env("S3_BUCKET", "bkt-dev1-data-avaliacoes") or "bkt-dev1-data-avaliacoes",
    aws_region=_env("AWS_REGION", "sa-east-1") or "sa-east-1",
    aws_access_key_id=_env("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=_env("AWS_SECRET_ACCESS_KEY"),
    user_folder=_env("USER_FOLDER", "nome_sobrenome") or "nome_sobrenome",
)

# Compat: exports simples (pra notebooks)
S3_BUCKET = SETTINGS.s3_bucket
AWS_REGION = SETTINGS.aws_region
AWS_ACCESS_KEY_ID = SETTINGS.aws_access_key_id
AWS_SECRET_ACCESS_KEY = SETTINGS.aws_secret_access_key
USER_FOLDER = SETTINGS.user_folder