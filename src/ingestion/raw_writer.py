from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

import pandas as pd

from src.utils.validacoes import (
    ValidationError,
    campos_obrigatorios,
    normalizar_cep,
    parse_date,
    parse_datetime,
    validar_cpf,
    validar_email,
    validar_status,
    validar_uf,
)


@dataclass(frozen=True)
class ValidationResult:
    valid: pd.DataFrame
    errors: list[ValidationError]


def _add_row_number(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.insert(0, "linha_arquivo", range(1, len(out) + 1))
    return out


def validar_clientes(df_clientes: pd.DataFrame) -> ValidationResult:
    """
    Valida a aba `clientes` e rejeita registros inválidos.

    Regras:
    - campos obrigatórios
    - CPF formato
    - e-mail simples
    - data_nascimento (YYYY-MM-DD)
    - status (ativo/inativo/suspenso)
    - data_evento (YYYY-MM-DD HH:MM:SS)
    """
    df = _add_row_number(df_clientes)
    errors: list[ValidationError] = []
    valid_rows: list[dict[str, Any]] = []

    required = ["id_cliente", "nome", "email", "cpf", "data_nascimento", "status", "data_evento"]

    for _, row in df.iterrows():
        linha = int(row["linha_arquivo"])
        d = row.to_dict()
        ok = True

        if not campos_obrigatorios(d, required):
            ok = False
            for campo in required:
                if campo not in d or pd.isna(d.get(campo)) or str(d.get(campo)).strip() == "":
                    errors.append(ValidationError(linha, "clientes", campo, d.get(campo), "campo obrigatório ausente/vazio"))

        if not validar_cpf(d.get("cpf")):
            ok = False
            errors.append(ValidationError(linha, "clientes", "cpf", d.get("cpf"), "CPF inválido (formato XXX.XXX.XXX-XX)"))

        if not validar_email(d.get("email")):
            ok = False
            errors.append(ValidationError(linha, "clientes", "email", d.get("email"), "e-mail inválido"))

        dn = parse_date(d.get("data_nascimento"))
        if dn is None:
            ok = False
            errors.append(ValidationError(linha, "clientes", "data_nascimento", d.get("data_nascimento"), "data inválida (YYYY-MM-DD)"))
        else:
            d["data_nascimento"] = dn

        if not validar_status(d.get("status")):
            ok = False
            errors.append(ValidationError(linha, "clientes", "status", d.get("status"), "status inválido (ativo|inativo|suspenso)"))
        else:
            d["status"] = str(d.get("status")).strip().lower()

        dt = parse_datetime(d.get("data_evento"))
        if dt is None:
            ok = False
            errors.append(ValidationError(linha, "clientes", "data_evento", d.get("data_evento"), "data_evento inválido (YYYY-MM-DD HH:MM:SS)"))
        else:
            d["data_evento"] = dt

        if ok:
            valid_rows.append(d)

    valid_df = pd.DataFrame(valid_rows)
    return ValidationResult(valid=valid_df, errors=errors)


def validar_enderecos(df_enderecos: pd.DataFrame, clientes_ids_validos: set[int]) -> ValidationResult:
    """
    Valida a aba `enderecos` e rejeita registros inválidos.

    Regras:
    - campos obrigatórios
    - CEP válido (aceita XXXXX-XXX ou XXXXXXXX, normaliza para XXXXX-XXX)
    - UF (2 letras)
    - integridade referencial: id_cliente deve existir em clientes (válidos)
    - data_evento (YYYY-MM-DD HH:MM:SS)
    """
    df = _add_row_number(df_enderecos)
    errors: list[ValidationError] = []
    valid_rows: list[dict[str, Any]] = []

    required = ["id_endereco", "id_cliente", "cep", "logradouro", "numero", "bairro", "cidade", "estado", "data_evento"]

    for _, row in df.iterrows():
        linha = int(row["linha_arquivo"])
        d = row.to_dict()
        ok = True

        if not campos_obrigatorios(d, required):
            ok = False
            for campo in required:
                if campo not in d or pd.isna(d.get(campo)) or str(d.get(campo)).strip() == "":
                    errors.append(ValidationError(linha, "enderecos", campo, d.get(campo), "campo obrigatório ausente/vazio"))

        cep_norm = normalizar_cep(d.get("cep"))
        if cep_norm is None:
            ok = False
            errors.append(ValidationError(linha, "enderecos", "cep", d.get("cep"), "CEP inválido (XXXXX-XXX ou XXXXXXXX)"))
        else:
            d["cep"] = cep_norm

        if not validar_uf(d.get("estado")):
            ok = False
            errors.append(ValidationError(linha, "enderecos", "estado", d.get("estado"), "UF inválida (2 letras)"))
        else:
            d["estado"] = str(d.get("estado")).strip().upper()

        try:
            id_cliente = int(d.get("id_cliente"))
        except Exception:
            id_cliente = None

        if id_cliente is None or id_cliente not in clientes_ids_validos:
            ok = False
            errors.append(ValidationError(linha, "enderecos", "id_cliente", d.get("id_cliente"), "integridade referencial: id_cliente inexistente em clientes"))
        else:
            d["id_cliente"] = id_cliente

        dt = parse_datetime(d.get("data_evento"))
        if dt is None:
            ok = False
            errors.append(ValidationError(linha, "enderecos", "data_evento", d.get("data_evento"), "data_evento inválido (YYYY-MM-DD HH:MM:SS)"))
        else:
            d["data_evento"] = dt

        if ok:
            valid_rows.append(d)

    valid_df = pd.DataFrame(valid_rows)
    return ValidationResult(valid=valid_df, errors=errors)

