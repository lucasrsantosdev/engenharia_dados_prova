import boto3

def criar_crawler():
    client = boto3.client("glue", region_name="sa-east-1")
    response = client.create_crawler(
        Name="seu_nome_sobrenome_crawler",
        Role="arn:aws:iam::ACCOUNT_ID:role/seu_nome_sobrenome_glue_crawler_role",
        DatabaseName="seu_nome_sobrenome",
        Targets={"S3Targets":[{"Path":"s3://bkt-dev1-data-avaliacoes/seu_nome_sobrenome/analytics/clientes/"}]},
        TablePrefix="clientes_"
    )
    print("Crawler criado:", response)