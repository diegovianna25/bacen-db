# Databricks notebook source
dbutils.fs.ls("/mnt/datalake")

# COMMAND ----------

dbutils.fs.mkdirs("/mnt/datalake/bronze")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import window
from pyspark.sql.types import *
from delta.tables import *

# COMMAND ----------

df = spark.read.format("json").load("/mnt/datalake/raw")

# COMMAND ----------

df.display()

# COMMAND ----------

df.schema

# COMMAND ----------

df = spark.read.format("json").load("/mnt/datalake/raw/")

# COMMAND ----------

# MAGIC %sql 
# MAGIC create schema bronze_cotas location "/mnt/datalake/bronze"

# COMMAND ----------

df.write.mode("overwrite").format("delta").saveAsTable("bronze_cotas.bronze")

# COMMAND ----------

stream_schema = StructType([StructField("ano", LongType(), True), 
StructField("cnpjCPF", StringType(), True), 
StructField("codigoLegislatura", LongType(), True), 
StructField("cpf", StringType(), True), 
StructField("datPagamentoRestituicao", StringType(), True), 
StructField("dataEmissao", StringType(), True), 
StructField("descricao", StringType(), True), 
StructField("descricaoEspecificacao", StringType(), True), 
StructField("fornecedor", StringType(), True), 
StructField("idDeputado", DoubleType(), True), 
StructField("idDocumento", LongType(), True), 
StructField("legislatura", LongType(), True), 
StructField("lote", StringType(), True), 
StructField("mes", LongType(), True), 
StructField("nomeParlamentar", StringType(), True), 
StructField("numero", StringType(), True), 
StructField("numeroCarteiraParlamentar", StringType(), True), 
StructField("numeroDeputadoID", LongType(), True), 
StructField("numeroEspecificacaoSubCota", LongType(), True), 
StructField("numeroSubCota", LongType(), True), 
StructField("parcela", LongType(), True), 
StructField("passageiro", StringType(), True), 
StructField("ressarcimento", StringType(), True), 
StructField("restituicao", StringType(), True), 
StructField("siglaPartido", StringType(), True), 
StructField("siglaUF", StringType(), True), 
StructField("tipoDocumento", StringType(), True), 
StructField("trecho", StringType(), True), 
StructField("urlDocumento",StringType(), True), 
StructField("valorDocumento", StringType(), True), 
StructField("valorGlosa", StringType(), True), 
StructField("valorLiquido", StringType(), True)])

# COMMAND ----------

# MAGIC %sqlCONVERT TO DELTA bronze_cotas.dep_data_history;

# COMMAND ----------

bronzeDeltaTable = DeltaTable.forPath(spark, "/mnt/datalake/bronze/bronze_cotas.dep_data_history")

# COMMAND ----------

bronzeDeltaTable = DeltaTable.forPath(spark, "/mnt/datalake/bronze/bronze_cotas.dep_data_history")

def upsertToDelta(df, batchId):
    windowSpec = window.Window.partitionBy("idDocumento").orderBy("dataEmissao")
    df_new = ( df.withColumn("row_number",F.row_number().over(windowSpec))
                 .filter("row_number = 1"))

    ( bronzeDeltaTable.alias("delta")
                      .merge(df_new.alias("raw"), "delta.idDocumento = raw.idDocumento")
                      .whenMatchedUpdateAll()
                      .whenNotMatchedInsertAll()
                      .execute() )

df_stream = ( spark.readStream
                   .format('cloudFiles')
                   .option('cloudFiles.format', 'json')
                   .schema(stream_schema)
                   .load("/mnt/datalake/raw/") )

stream = (df_stream.writeStream
                   .foreachBatch(upsertToDelta)
                   .option('checkpointLocation', "/mnt/datalake/bronze/dep_data_history_checkpoint")
                   .outputMode("update")
                   .start()
          )

# COMMAND ----------

df_stream = ( spark.readStream
                   .format('cloudFiles')
                   .option('cloudFiles.format', 'json')
                   .option('header','true')
                   .schema(stream_schema)
                   .load("/mnt/datalake/raw/") )
