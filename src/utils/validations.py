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