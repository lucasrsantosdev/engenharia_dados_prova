import logging
import os
import pandas as pd
from datetime import datetime
from config import RAW_CLIENTES, RAW_ENDERECOS, DATA_PROCESSAMENTO
from utils.validations import validar_cpf, validar_cep, validar_email, validar_status, validar_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info("Iniciando pipeline Analytics")



def salvar_parquet(df, path):
    os.makedirs(path, exist_ok=True)
    df.to_parquet(os.path.join(path, f"data_processamento={DATA_PROCESSAMENTO}", "part-00000.parquet"), index=False)
    logger.info(f"Salvo Parquet em: {path}")

def process_raw():
    excel_path = os.path.join(os.path.dirname(RAW_CLIENTES), "dados_entrada.xlsx")
    logger.info(f"Lendo Excel: {excel_path}")
    
    clientes = pd.read_excel(excel_path, sheet_name="clientes")
    enderecos = pd.read_excel(excel_path, sheet_name="enderecos")
    
    # Validações básicas
    def validar_clientes(row):
        erros = []
        if not validar_cpf(row.get("cpf", "")):
            erros.append("CPF inválido")
        if not validar_email(row.get("email", "")):
            erros.append("E-mail inválido")
        if not validar_status(row.get("status", "")):
            erros.append("Status inválido")
        if not validar_data(str(row.get("data_nascimento", ""))):
            erros.append("Data nascimento inválida")
        return erros

    clientes["erros"] = clientes.apply(validar_clientes, axis=1)
    clientes_validos = clientes[clientes["erros"].apply(lambda x: len(x) == 0)].drop(columns=["erros"])
    logger.info(f"Clientes válidos: {len(clientes_validos)} / {len(clientes)}")
    
    # Endereços
    def validar_enderecos(row):
        erros = []
        if not validar_cep(str(row.get("cep", ""))):
            erros.append("CEP inválido")
        return erros
    enderecos["erros"] = enderecos.apply(validar_enderecos, axis=1)
    enderecos_validos = enderecos[enderecos["erros"].apply(lambda x: len(x) == 0)].drop(columns=["erros"])
    logger.info(f"Endereços válidos: {len(enderecos_validos)} / {len(enderecos)}")
    
    salvar_parquet(clientes_validos, RAW_CLIENTES)
    salvar_parquet(enderecos_validos, RAW_ENDERECOS)
# Logs finais para verificar total de registros
logger.info(f"Total de clientes processados: {len(RAW_CLIENTES)}")
logger.info(f"Total de endereços processados: {len(RAW_ENDERECOS)}")    
logger.info("Analytics finalizado com sucesso")