import boto3
import time
import pandas as pd

def executar_query():
    athena = boto3.client("athena", region_name="sa-east-1")
    db = "seu_nome_sobrenome"
    output = f"s3://bkt-dev1-data-avaliacoes/seu_nome_sobrenome/athena_results/"
    query = "SELECT * FROM clientes"

    res = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": db},
        ResultConfiguration={"OutputLocation": output}
    )
    query_id = res["QueryExecutionId"]
    print("Query Athena iniciada, ID:", query_id)

    # Espera a execução
    while True:
        status = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]["Status"]["State"]
        if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break
        time.sleep(2)
    print("Status da query:", status)

    # Baixa resultados
    resultado = athena.get_query_results(QueryExecutionId=query_id)
    df = pd.DataFrame([r["Data"] for r in resultado["ResultSet"]["Rows"][1:]])  # p/ simplificar
    df.to_csv("resultado_athena.csv", index=False)
    print("Arquivo resultado_athena.csv gerado.")