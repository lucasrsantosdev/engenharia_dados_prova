from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from config import SETTINGS
from src.ingestion.excel_reader import read_excel
from src.ingestion.raw_writer import write_raw_parquet, write_validation_log
from src.ingestion.validators import validar_clientes, validar_enderecos
from src.utils.logger import log_step, setup_logging
from src.utils.spark import build_spark


@dataclass(frozen=True)
class RawIngestionOutput:
    data_processamento: str
    clientes_rows: int
    enderecos_rows: int
    clientes_errors: int
    enderecos_errors: int
    clientes_log_path: str
    enderecos_log_path: str


def run_raw_ingestion(excel_path: str, logs_dir: str = "logs") -> RawIngestionOutput:
    """
    Parte 1 – Ingestão e Armazenamento Raw:
    - lê Excel (pandas/openpyxl)
    - valida qualidade e integridade referencial
    - rejeita inválidos e registra erros (JSONL)
    - grava Parquet (snappy) particionado por data_processamento em `raw/`
    """
    logger = setup_logging()
    step = "raw_ingestion"
    log_step(logger, step, "start", excel_path=excel_path)

    data_processamento = datetime.today().strftime("%Y-%m-%d")

    spark = build_spark(
        "engenharia_dados_prova_raw",
        aws_access_key_id=SETTINGS.aws_access_key_id,
        aws_secret_access_key=SETTINGS.aws_secret_access_key,
        aws_region=SETTINGS.aws_region,
    )

    excel = read_excel(excel_path)

    res_clientes = validar_clientes(excel.clientes)
    clientes_ids = set(map(int, res_clientes.valid["id_cliente"].tolist())) if not res_clientes.valid.empty else set()
    res_enderecos = validar_enderecos(excel.enderecos, clientes_ids_validos=clientes_ids)

    clientes_log = str(Path(logs_dir) / f"validacao_clientes_{data_processamento}.jsonl")
    enderecos_log = str(Path(logs_dir) / f"validacao_enderecos_{data_processamento}.jsonl")
    write_validation_log(res_clientes.errors, clientes_log)
    write_validation_log(res_enderecos.errors, enderecos_log)

    write_raw_parquet(
        spark=spark,
        df=res_clientes.valid,
        s3_path=SETTINGS.raw_clientes,
        data_processamento=data_processamento,
    )
    write_raw_parquet(
        spark=spark,
        df=res_enderecos.valid,
        s3_path=SETTINGS.raw_enderecos,
        data_processamento=data_processamento,
    )

    log_step(
        logger,
        step,
        "end",
        data_processamento=data_processamento,
        clientes_rows=int(len(res_clientes.valid)),
        enderecos_rows=int(len(res_enderecos.valid)),
        clientes_errors=int(len(res_clientes.errors)),
        enderecos_errors=int(len(res_enderecos.errors)),
    )

    return RawIngestionOutput(
        data_processamento=data_processamento,
        clientes_rows=int(len(res_clientes.valid)),
        enderecos_rows=int(len(res_enderecos.valid)),
        clientes_errors=int(len(res_clientes.errors)),
        enderecos_errors=int(len(res_enderecos.errors)),
        clientes_log_path=clientes_log,
        enderecos_log_path=enderecos_log,
    )

if __name__ == "__main__":
    output = run_raw_ingestion(
        excel_path="dados_entrada.xlsx"
    )

    print(output)