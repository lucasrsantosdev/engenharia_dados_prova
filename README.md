# 🚀 Pipeline de Dados - Engenharia de Dados

## 📌 Objetivo

Construir um pipeline de dados completo para ingestão, processamento incremental e disponibilização analítica de dados de **clientes** e **endereços**, utilizando AWS (S3, Glue, Athena) e Apache Spark com Delta Lake.

---

## 🏗️ Arquitetura do Projeto

```
engenharia_dados_prova/
│
├── src/
│   ├── ingestion/        # Ingestão de dados (Excel → Raw)
│   ├── pipeline/         # Orquestração e ingestão bruta
│   ├── processing/       # Processamento e validações
│   ├── utils/            # Funções auxiliares (logger, validações)
│   └── infra/            # Configuração Spark
│
├── notebooks/            # Notebooks de exploração
├── config.py             # Configurações centralizadas
├── pipeline.py           # Script principal
├── requirements.txt      # Dependências
├── .env                  # Variáveis de ambiente
└── README.md             # Documentação

```

---

## ⚙️ Tecnologias Utilizadas

* Python 3.10+
* Apache Spark 3.x
* Delta Lake
* AWS S3
* AWS Glue
* Amazon Athena
* boto3
* pandas / openpyxl
* python-dotenv

---

## 🔐 Variáveis de Ambiente (.env)

```env
S3_BUCKET=bkt-dev1-data-avaliacoes
AWS_REGION=sa-east-1
AWS_ACCESS_KEY_ID=SEU_ACCESS_KEY
AWS_SECRET_ACCESS_KEY=SUA_SECRET_KEY
USER_FOLDER=lucas_cordeiro
EXCEL_PATH=dados_entrada.xlsx
```

---

## 📥 Etapas do Pipeline

### 1. Ingestão (Raw)

* Leitura do Excel
* Validações:

  * CPF
  * CEP
  * Email
  * Status
  * Datas
* Dados inválidos são logados e descartados
* Escrita no S3 (Parquet + particionamento)

---

### 2. Processamento (Stage)

* Uso de Spark + Delta Lake
* SCD Type 1 (sobrescrita)
* Último evento por:

  * cliente → id_cliente
  * endereço → id_endereco
* Coluna: `data_atualizacao`

---

### 3. Analytics

* JOIN clientes + endereços
* Apenas clientes ativos
* Cálculo de idade
* Output otimizado em Parquet

---

### 4. Governança (Glue + Athena)

* Criação de Crawler via boto3
* Query via Athena
* Exportação para CSV

---

## ▶️ Como Executar

### 1. Criar ambiente

```bash
python -m venv .venv
source .venv/bin/activate  # Linux
.venv\Scripts\activate     # Windows
```

### 2. Instalar dependências

```bash
pip install -r requirements.txt
```

### 3. Executar pipeline

```bash
python pipeline.py
```

---

## ☁️ Estrutura no S3

```
s3://bkt-dev1-data-avaliacoes/lucas_cordeiro/
├── raw/
├── stage/
├── analytics/
└── athena_results/
```

---

## 🧠 Decisões Técnicas

* Uso de **Delta Lake** para garantir idempotência
* Separação em camadas: raw → stage → analytics
* Validações antes da persistência
* Configuração centralizada (`config.py`)
* Logging estruturado

---

## 🧪 Testes

```bash
pytest
```

---

## 📊 Validação Athena

Query executada:

```sql
SELECT * FROM clientes;
```

Saída:

* `resultado_athena.csv`

---

## 📌 Observações

* Credenciais não devem ser versionadas
* Pipeline idempotente
* Preparado para produção

---
## Observação sobre S3

O upload para o S3 foi implementado via boto3 e validado em nível de código.
Durante a execução, foi retornado erro de permissão (AccessDenied - s3:PutObject),
indicando que o usuário IAM fornecido não possui permissão de escrita no bucket.

O pipeline está preparado para execução completa em ambiente com permissões adequadas.

[dados_entrada.xlsx]
      │
      ▼
      [1️⃣ RAW INGESTION (analytics.py / raw_ingestion_local.py)]
- Valida CPF, CEP, e-mail, status, datas
- Rejeita registros inválidos (log JSONL)
- Saída local em Parquet:
    data/raw/clientes/data_processamento=YYYY-MM-DD/part-00000.parquet
    data/raw/enderecos/data_processamento=YYYY-MM-DD/part-00000.parquet
      │
      ▼
[2️⃣ UPLOAD PARA S3 (raw_to_s3.py)]
- Envia Parquet local para:
    s3://bkt-dev1-data-avaliacoes/{nome_sobrenome}/raw/clientes/data_processamento=YYYY-MM-DD/
    s3://bkt-dev1-data-avaliacoes/{nome_sobrenome}/raw/enderecos/data_processamento=YYYY-MM-DD/
- Mantém compressão snappy e particionamento
      │
      ▼
[3️⃣ STAGE (stage_local.py)]
- Spark + Delta Lake
- SCD Type 1 (sobrescreve últimos eventos)
- Coluna data_atualizacao para rastreabilidade
- Saída local em Delta:
    data/stage/clientes/_delta_log/
    data/stage/enderecos/_delta_log/
      │
      ▼
[4️⃣ ANALYTICS (analytics.py)]
- Filtra clientes ativos
- LEFT JOIN clientes × endereços
- Múltiplos endereços por cliente
- Coluna idade calculada
- Saída Parquet:
    data/analytics/clientes/estado=XX/
      │
      ▼
[5️⃣ GLUE CRAWLER (glue_crawler.py)]
- Cria crawler programaticamente no Glue
- Aponta para:
    s3://bkt-dev1-data-avaliacoes/{nome_sobrenome}/analytics/clientes/
- Prepara catálogo de dados para Athena
      │
      ▼
[6️⃣ ATHENA QUERY (athena_query.py)]
- Executa query:
    SELECT * FROM clientes
- Salva resultado CSV local:
    resultado_athena.csv
- S3 Athena Results (opcional):
    s3://bkt-dev1-data-avaliacoes/{nome_sobrenome}/athena_results/

    

## Configure o Hadoop (somente Windows):
Crie a pasta C:/hadoop/bin e coloque winutils.exe nela.
Configure as variáveis de ambiente:

$env:HADOOP_HOME="C:/hadoop"
$env:PATH += ";C:/hadoop/bin"

Estrutura de dados
Raw: src/ingestion/
Stage: src/pipeline/stage.py salva parquet local em SETTINGS.stage_clientes e SETTINGS.stage_enderecos
Particionamento: por data_processamento
Como rodar
Rodar o stage local (leitura raw → escrita stage):

python -m src.pipeline.stage
