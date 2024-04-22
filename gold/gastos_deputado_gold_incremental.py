# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import window
from pyspark.sql.types import *
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, current_date, date_sub, to_timestamp, regexp_replace,

# COMMAND ----------

from pyspark.sql.functions import col, year, month, current_date, date_sub, to_timestamp

# Obter a data atual e calcular o mês anterior
data_atual = current_date()
data_mes_anterior = date_sub(data_atual, 30)

# Filtrar os dados para o mês anterior inteiro
df_gold_mensal = df_silver.filter((year(col("dataEmissao")) == year(data_mes_anterior)) & (month(col("dataEmissao")) == month(data_mes_anterior)))

# Realizar os tratamentos nos dados
df_gold_mensal = df_gold_mensal.select(
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
    col("legislatura").cast("int"),
    col("lote").cast("string"),
    col("mes").cast("int"),
    col("nomeParlamentar").cast("string"),
    col("numero").cast("string"),
    col("numeroCarteiraParlamentar").cast("string"),
    col("numeroDeputadoID").cast("string"),
    col("numeroSubCota").cast("int"),
    col("parcela").cast("int"),
    col("passageiro").cast("string"),
    col("ressarcimento").cast("string"),
    col("restituicao").cast("string"),
    col("siglaPartido").cast("string"),
    col("siglaUF").cast("string"),
    col("tipoDocumento").cast("string"),
    col("trecho").cast("string"),
    col("urlDocumento").cast("string"),
    (col("valorDocumento").cast("decimal(18,2)").alias("valorDocumento")),
    (col("valorGlosa").cast("decimal(18,2)").alias("valorGlosa")),
    (col("valorLiquido").cast("decimal(18,2)").alias("valorLiquido"))
)

# COMMAND ----------

# Gravar os dados na tabela na camada Gold
df_gold_mensal.write.format("delta").option("mergeSchema", "true").mode("append").save("/mnt/datalake/gold/gold_cotas.dep_data_gastos_teste")
