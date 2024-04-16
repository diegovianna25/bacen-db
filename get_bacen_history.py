# Databricks notebook source
import requests
from pyspark.sql.types import StringType
import json
import pandas as pd
import zipfile
import io
from datetime import datetime
from dateutil.parser import isoparse

# COMMAND ----------

#dbutils.fs.ls("mnt")

# COMMAND ----------

#response = requests.get(url)

# COMMAND ----------

#dbutils.fs.mkdirs("/mnt/datalake/raw")

# COMMAND ----------

#dbutils.fs.ls("/mnt/datalake/raw/")

# COMMAND ----------

def obter_dados_cotas_ano(ano):
    formato = "json"
    url = f"http://www.camara.leg.br/cotas/Ano-{ano}.{formato}.zip"
    
    # Fazer o download do arquivo ZIP
    response = requests.get(url)
    
    # Verificar se a solicitação foi bem-sucedida (código de status 200)
    if response.status_code == 200:
        # Ler o conteúdo do arquivo ZIP
        with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
            # Iterar sobre os arquivos no ZIP
            for filename in zip_file.namelist():
                # Verificar se o arquivo é do ano atual ou anterior
                if filename.startswith(f"Ano-{ano}."):
                    # Extrair e carregar os dados do arquivo JSON em um DataFrame
                    with zip_file.open(filename) as json_file:
                        df = pd.read_json(json_file)
                    
                    # Expandir os dados da coluna "dados"
                    df_expandido = pd.json_normalize(df['dados'])
                    
                    # Converter a coluna "dataEmissao" para o tipo datetime
                    df_expandido["dataEmissao"] = pd.to_datetime(df_expandido["dataEmissao"])
                    
                    # Salvar os dados em um arquivo local
                    salvar_dados_local(df_expandido, ano)
                    
                    return df_expandido
        
        print(f"Nenhum dado disponível para o ano {ano}.")
        return None
    else:
        print(f"Erro ao baixar o arquivo ZIP para o ano {ano}.")
        return None

def salvar_dados_local(df, ano):
    # Salvar os dados em um arquivo local
    df.to_json(f"dados_{ano}.json", orient="records")

# Baixar dados para os anos de 2023 até o mês anterior ao atual
ano_atual = datetime.now().year
mes_atual = datetime.now().month

for ano in range(2023, ano_atual):
    print(f"Obtendo dados para o ano {ano}...")
    obter_dados_cotas_ano(ano)

# Para o ano atual, baixar apenas até o mês anterior ao atual
print(f"Obtendo dados para o ano {ano_atual} até o mês {mes_atual - 1}...")
obter_dados_cotas_ano(ano_atual)

# COMMAND ----------

import requests
import zipfile
import io
import datetime
import pandas as pd
from pyspark.sql import SparkSession

def baixar_dados_cotas_ano(ano):
    formato = "json"
    url = f"http://www.camara.leg.br/cotas/Ano-{ano}.{formato}.zip"
    
    # Fazer o download do arquivo ZIP
    response = requests.get(url)
    
    # Verificar se a solicitação foi bem-sucedida (código de status 200)
    if response.status_code == 200:
        # Ler o conteúdo do arquivo ZIP
        with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
            # Extrair e listar os arquivos do ZIP
            zip_file.extractall()
            # Listar os arquivos extraídos
            arquivos_extraidos = zip_file.namelist()
            print(f"Arquivos extraídos para o ano {ano}: {arquivos_extraidos}")
            # Ler o arquivo JSON extraído e carregar os dados em um DataFrame
            for arquivo in arquivos_extraidos:
                if arquivo.endswith('.json'):
                    df = pd.read_json(arquivo)
                    # Expandir os dados da coluna "dados"
                    df_expandido = pd.json_normalize(df['dados'])
                    # Converter a string de data em objeto datetime
                    try:
                        df_expandido["dataEmissao"] = pd.to_datetime(df_expandido["dataEmissao"])
                    except ValueError:
                        print(f"Erro ao converter a data no arquivo {arquivo}.")
                        continue
                    # Filtrar os dados até o mês anterior ao atual
                    ultimo_dia_mes_atual = datetime.datetime.now().replace(day=1) - datetime.timedelta(days=1)
                    df_expandido = df_expandido[df_expandido["dataEmissao"] < ultimo_dia_mes_atual]
                    
                    # Salvar o DataFrame como um arquivo JSON usando Spark DataFrame API
                    spark = SparkSession.builder.getOrCreate()
                    spark_df = spark.createDataFrame(df_expandido)
                    caminho = "/mnt/datalake/raw"
                    spark_df.repartition(1).write.format("json").mode("append").save(caminho)
                    
                    print(f"Dados salvos em '{caminho}'")
    else:
        print(f"Erro ao baixar o arquivo ZIP para o ano {ano}.")

# Ano atual e mês atual
ano_atual = datetime.datetime.now().year

# Iterar para os anos a partir de 2023 até o ano anterior ao atual
for ano in range(2023, ano_atual):
    print(f"Obtendo dados para o ano {ano}...")
    baixar_dados_cotas_ano(ano)

# Baixar dados para o ano atual até o mês anterior ao atual
print(f"Obtendo dados para o ano {ano_atual} até o mês anterior ao atual...")
baixar_dados_cotas_ano(ano_atual)

