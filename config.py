# src/utils/config.py

"""
Configurações centralizadas do projeto.

Suporta dois modos:
- LOCAL (default) → salva em disco
- S3 → salva na AWS

Variáveis de ambiente (.env):
- STORAGE_MODE = local | s3
- BASE_PATH (para local)
- S3_BUCKET
- AWS_REGION
- AWS_ACCESS_KEY_ID
- AWS_SECRET_ACCESS_KEY
- USER_FOLDER
"""

import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


def _env(key, default=None):
    value = os.getenv(key)
    if value is None or str(value).strip() == "":
        return default
    return value


@dataclass(frozen=True)
class Settings:
    storage_mode: str
    base_path: str

    s3_bucket: str
    aws_region: str
    aws_access_key_id: str
    aws_secret_access_key: str
    user_folder: str

    # =========================
    # BASE PATH
    # =========================
    @property
    def base(self):
        if self.storage_mode.lower() == "s3":
            return f"s3a://{self.s3_bucket}/{self.user_folder}"
        # garante path absoluto no Windows
        return os.path.abspath(self.base_path)

    # =========================
    # CAMADAS
    # =========================
    @property
    def raw_clientes(self):
        return os.path.join(self.base, "raw", "clientes")

    @property
    def raw_enderecos(self):
        return os.path.join(self.base, "raw", "enderecos")

    @property
    def stage_clientes(self):
        return os.path.join(self.base, "stage", "clientes")

    @property
    def stage_enderecos(self):
        return os.path.join(self.base, "stage", "enderecos")

    @property
    def analytics_clientes(self):
        return os.path.join(self.base, "analytics", "clientes")

    @property
    def athena_results(self):
        if self.storage_mode.lower() == "s3":
            return f"{self.base}/athena_results"
        return os.path.join(self.base, "athena_results")


SETTINGS = Settings(
    storage_mode=_env("STORAGE_MODE", "local").lower(),
    base_path=_env("BASE_PATH", "data"),

    s3_bucket=_env("S3_BUCKET", "bkt-dev1-data-avaliacoes"),
    aws_region=_env("AWS_REGION", "sa-east-1"),
    aws_access_key_id=_env("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=_env("AWS_SECRET_ACCESS_KEY"),
    user_folder=_env("USER_FOLDER", "nome_sobrenome"),
)

# Compatibilidade para notebooks
S3_BUCKET = SETTINGS.s3_bucket
AWS_REGION = SETTINGS.aws_region
AWS_ACCESS_KEY_ID = SETTINGS.aws_access_key_id
AWS_SECRET_ACCESS_KEY = SETTINGS.aws_secret_access_key
USER_FOLDER = SETTINGS.user_folder

# config.py
class SETTINGS:
    raw_clientes = r"C:\Users\lucas_um1\engenharia_dados_prova\engenharia_dados_prova\data\raw\clientes"
    raw_enderecos = r"C:\Users\lucas_um1\engenharia_dados_prova\engenharia_dados_prova\data\raw\enderecos"
    stage_clientes = r"C:\Users\lucas_um1\engenharia_dados_prova\engenharia_dados_prova\data\stage\clientes"
    stage_enderecos = r"C:\Users\lucas_um1\engenharia_dados_prova\engenharia_dados_prova\data\stage\enderecos"

    import os
from datetime import datetime

# Configurações gerais
S3_BUCKET = os.getenv("S3_BUCKET", "local")
AWS_REGION = os.getenv("AWS_REGION", "sa-east-1")
USER_FOLDER = os.getenv("USER_FOLDER", "lucas_cordeiro")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

# Paths Raw
RAW_CLIENTES = os.path.join(DATA_DIR, "raw", "clientes")
RAW_ENDERECOS = os.path.join(DATA_DIR, "raw", "enderecos")

# Paths Stage
STAGE_CLIENTES = os.path.join(DATA_DIR, "stage", "clientes")
STAGE_ENDERECOS = os.path.join(DATA_DIR, "stage", "enderecos")

# Paths Analytics
ANALYTICS_CLIENTES = os.path.join(DATA_DIR, "analytics", "clientes")

# Particionamento
DATA_PROCESSAMENTO = datetime.now().strftime("%Y-%m-%d")