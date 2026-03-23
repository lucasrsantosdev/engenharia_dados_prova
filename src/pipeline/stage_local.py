from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.window import Window
import pyspark.sql.functions as F

spark_builder = SparkSession.builder.appName("stage_pipeline").config(
    "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
).config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(spark_builder).getOrCreate()

# Paths
RAW_CLIENTES_PATH = "data/raw/clientes/"
RAW_ENDERECOS_PATH = "data/raw/enderecos/"
STAGE_CLIENTES_PATH = "data/stage/clientes/"
STAGE_ENDERECOS_PATH = "data/stage/enderecos/"

# Clientes SCD1
df_clientes = spark.read.parquet(RAW_CLIENTES_PATH)
w_cli = Window.partitionBy("id_cliente").orderBy(F.desc("data_evento"))
df_clientes_latest = df_clientes.withColumn("rn", F.row_number().over(w_cli)).filter("rn=1").drop("rn")
df_clientes_latest = df_clientes_latest.withColumn("data_atualizacao", F.current_timestamp())
df_clientes_latest.write.format("delta").mode("overwrite").save(STAGE_CLIENTES_PATH)

# Endereços SCD1
df_enderecos = spark.read.parquet(RAW_ENDERECOS_PATH)
w_end = Window.partitionBy("id_endereco").orderBy(F.desc("data_evento"))
df_enderecos_latest = df_enderecos.withColumn("rn", F.row_number().over(w_end)).filter("rn=1").drop("rn")
df_enderecos_latest = df_enderecos_latest.withColumn("data_atualizacao", F.current_timestamp())
df_enderecos_latest.write.format("delta").mode("overwrite").save(STAGE_ENDERECOS_PATH)