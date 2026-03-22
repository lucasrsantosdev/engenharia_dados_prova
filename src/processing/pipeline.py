# pipeline.py

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from utils.validacoes import validar_cep, campos_obrigatorios

def validar_enderecos(df_enderecos: DataFrame, df_clientes: DataFrame):
    """
    Valida endereços:
    - Campos obrigatórios
    - CEP válido
    - Integridade referencial (id_cliente existe em clientes)
    
    Retorna:
    - df_valido: DataFrame apenas com registros válidos
    - erros: lista de dicionários com id_endereco e motivos
    """
    erros = []

    # Obter lista de clientes válidos
    clientes_validos = df_clientes.select("id_cliente").rdd.flatMap(lambda x: x).collect()
    
    def validar_linha(row):
        linha_erros = []
        d = row.asDict()

        # Campos obrigatórios
        if not campos_obrigatorios(d, ["id_endereco", "id_cliente", "cep", "logradouro", "bairro", "cidade", "estado"]):
            linha_erros.append("Campos obrigatórios faltando")
        
        # CEP válido
        if not validar_cep(d.get("cep")):
            linha_erros.append("CEP inválido")
        
        # Integridade referencial
        if d.get("id_cliente") not in clientes_validos:
            linha_erros.append("id_cliente não encontrado em clientes")
        
        if linha_erros:
            erros.append({"id_endereco": d.get("id_endereco"), "erros": linha_erros})
            return False
        return True

    df_valido = df_enderecos.rdd.filter(validar_linha).toDF(df_enderecos.columns)
    return df_valido, erros