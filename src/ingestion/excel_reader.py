excel reader

from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class ExcelInput:
    clientes: pd.DataFrame
    enderecos: pd.DataFrame


def read_excel(path: str | Path) -> ExcelInput:
    """
    Lê o Excel de entrada com as abas `clientes` e `enderecos`.
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Arquivo Excel não encontrado: {p}")

    clientes = pd.read_excel(p, sheet_name="clientes", engine="openpyxl")
    enderecos = pd.read_excel(p, sheet_name="enderecos", engine="openpyxl")
    return ExcelInput(clientes=clientes, enderecos=enderecos)

