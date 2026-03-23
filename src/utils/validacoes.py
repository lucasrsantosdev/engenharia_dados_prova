from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Iterable, Mapping

import pandas as pd


STATUS_VALIDOS = {"ativo", "inativo", "suspenso"}
UF_REGEX = re.compile(r"^[A-Z]{2}$")
CPF_REGEX = re.compile(r"^\d{3}\.\d{3}\.\d{3}-\d{2}$")
EMAIL_REGEX = re.compile(r"^[^@]+@[^@]+\.[^@]+$")
CEP_REGEX_HIFEN = re.compile(r"^\d{5}-\d{3}$")
CEP_REGEX_SEM_HIFEN = re.compile(r"^\d{8}$")


@dataclass(frozen=True)
class ValidationError:
    linha: int
    entidade: str
    campo: str
    valor: Any
    motivo: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "linha": self.linha,
            "entidade": self.entidade,
            "campo": self.campo,
            "valor": self.valor,
            "motivo": self.motivo,
        }


def _is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
    return str(value).strip() == ""


def validar_cpf(cpf: Any) -> bool:
    """Valida CPF no formato XXX.XXX.XXX-XX (apenas formato)."""
    if _is_blank(cpf):
        return False
    return bool(CPF_REGEX.match(str(cpf)))


def normalizar_cep(cep: Any) -> str | None:
    """
    Normaliza CEP para o formato XXXXX-XXX.
    Aceita entrada no formato XXXXX-XXX ou XXXXXXXX.
    """
    if _is_blank(cep):
        return None
    s = str(cep).strip()
    if CEP_REGEX_HIFEN.match(s):
        return s
    if CEP_REGEX_SEM_HIFEN.match(s):
        return f"{s[:5]}-{s[5:]}"
    return None


def validar_cep(cep: Any) -> bool:
    """Valida CEP aceitando XXXXX-XXX ou XXXXXXXX."""
    return normalizar_cep(cep) is not None


def validar_email(email: Any) -> bool:
    """Valida e-mail simples (contém @ e domínio)."""
    if _is_blank(email):
        return False
    return bool(EMAIL_REGEX.match(str(email).strip()))


def validar_data_yyyy_mm_dd(value: Any) -> bool:
    """Valida data no formato YYYY-MM-DD (apenas formato/valor)."""
    if _is_blank(value):
        return False
    try:
        datetime.strptime(str(value)[:10], "%Y-%m-%d")
        return True
    except ValueError:
        return False


def validar_status(status: Any) -> bool:
    """Valida se o status está dentro dos valores válidos."""
    if _is_blank(status):
        return False
    return str(status).strip().lower() in STATUS_VALIDOS


def validar_uf(uf: Any) -> bool:
    """Valida UF (2 letras)."""
    if _is_blank(uf):
        return False
    return bool(UF_REGEX.match(str(uf).strip().upper()))


def campos_obrigatorios(dados: Mapping[str, Any], campos: Iterable[str]) -> bool:
    """Verifica se os campos obrigatórios não são nulos ou vazios."""
    for campo in campos:
        if campo not in dados or _is_blank(dados.get(campo)):
            return False
    return True


def parse_date(value: Any) -> date | None:
    """Converte para date quando possível (YYYY-MM-DD)."""
    if _is_blank(value):
        return None
    if isinstance(value, (datetime, date)):
        return value.date() if isinstance(value, datetime) else value
    try:
        return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def parse_datetime(value: Any) -> datetime | None:
    """Converte para datetime quando possível (YYYY-MM-DD HH:MM:SS)."""
    if _is_blank(value):
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.strptime(str(value).strip(), "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None

import re
from datetime import datetime

def validar_cpf(cpf: str) -> bool:
    return bool(re.match(r"^\d{3}\.\d{3}\.\d{3}-\d{2}$", cpf))

def validar_cep(cep: str) -> bool:
    return bool(re.match(r"^\d{5}-\d{3}$", cep))

def validar_email(email: str) -> bool:
    return "@" in email and "." in email.split("@")[-1]

def validar_status(status: str) -> bool:
    return status in {"ativo", "inativo", "suspenso"}

def validar_data(data_str: str) -> bool:
    try:
        datetime.strptime(data_str, "%Y-%m-%d")
        return True
    except:
        return False