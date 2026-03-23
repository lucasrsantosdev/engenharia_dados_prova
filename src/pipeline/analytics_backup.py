from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import col, current_date, year

spark_builder = SparkSession.builder.appName("analytics_pipeline").config(
    "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
).config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
)
spark = configure_spark_with_delta_pip(spark_builder).getOrCreate()

STAGE_CLIENTES_PATH = "data/stage/clientes/"
STAGE_ENDERECOS_PATH = "data/stage/enderecos/"
ANALYTICS_PATH = "data/analytics/clientes/"

df_cli = spark.read.format("delta").load(STAGE_CLIENTES_PATH).filter(col("status") == "ativo")
df_end = spark.read.format("delta").load(STAGE_ENDERECOS_PATH)

# LEFT JOIN
df_analytics = df_cli.join(df_end, on="id_cliente", how="left")

# Coluna idade
df_analytics = df_analytics.withColumn("idade", year(current_date()) - year(col("data_nascimento")))

df_analytics.write.parquet(ANALYTICS_PATH, mode="overwrite")
print("Analytics finalizado e salvo em parquet.")