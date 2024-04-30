# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import window
from pyspark.sql.types import *
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, to_timestamp


# COMMAND ----------

df_silver = spark.read.format("delta").load("/mnt/datalake/silver/silver_cotas.dep_data_history")

# Realizar os tratamentos nos dados
df_gold = df_silver.select(
    col("ano").cast("int"),
    col("cnpjCPF").cast("string"),
    col("codigoLegislatura").cast("string"),
    col("cpf").cast("string"),
    to_timestamp(col("dataEmissao"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("dataEmissao"),
    col("descricao").cast("string"),
    col("descricaoEspecificacao").cast("string"),
    col("fornecedor").cast("string"),
    col("idDeputado").cast("string"),
    col("idDocumento").cast("string"),
    col("mes").cast("int"),
    col("nomeParlamentar").cast("string"),
    col("numero").cast("string"),
    col("numeroCarteiraParlamentar").cast("string"),
    col("numeroDeputadoID").cast("string"),
    col("numeroSubCota").cast("int"),
    col("parcela").cast("int"),
    col("ressarcimento").cast("string"),
    col("restituicao").cast("string"),
    col("siglaPartido").cast("string"),
    col("siglaUF").cast("string"),
    col("tipoDocumento").cast("string"),
    col("urlDocumento").cast("string"),
    (col("valorDocumento").cast("decimal(18,2)").alias("valorDocumento")),
    (col("valorGlosa").cast("decimal(18,2)").alias("valorGlosa")),
    (col("valorLiquido").cast("decimal(18,2)").alias("valorLiquido"))
)

# COMMAND ----------

#Grava os dados da camada raw/bronze na tabela criada no db da camada silver
#df_gold.write.option("mergeSchema", "true").mode("overwrite").format("delta").saveAsTable("gold_cotas.dep_data_history")
df_gold.write.option("mergeSchema", "true").mode("overwrite").format("delta").save("/mnt/datalake/gold/gold_cotas.dep_data_history")
