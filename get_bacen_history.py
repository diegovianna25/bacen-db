# Databricks notebook source
import requests
from pyspark.sql.types import StringType
import json
import pandas as pd
import zipfile

# COMMAND ----------

ano = 2022
formato = "json"

url = f"http://www.camara.leg.br/cotas/Ano-{ano}.{formato}.zip"
response = requests.get(url)


# COMMAND ----------

ano = 2022
formato = "json"
with zipfile.ZipFile(response, 'r') as zip_ref:
    zip_ref.extract(response)

# Carregar o arquivo JSON em um DataFrame
df = pd.read_json(f"Ano-{ano}")

# COMMAND ----------

import pandas as pd
import zipfile
import io
import requests

# Definir o ano e o formato
ano = 2022
formato = "json"

# URL para download do arquivo ZIP
url = f"http://www.camara.leg.br/cotas/Ano-{ano}.{formato}.zip"

# Fazer o download do arquivo ZIP
response = requests.get(url)

# Verificar se a solicitação foi bem-sucedida (código de status 200)
if response.status_code == 200:
    # Ler o conteúdo do arquivo ZIP
    with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
        # Extrair o arquivo JSON do ZIP
        json_filename = f"Ano-{ano}.json"
        with zip_file.open(json_filename) as json_file:
            # Carregar o JSON em um DataFrame
            df = pd.read_json(json_file)
    
    # Exibir o DataFrame
    print(df.head())
else:
    print("Erro ao baixar o arquivo ZIP.")

# COMMAND ----------

df.display()

# COMMAND ----------

dbutils.fs.ls("mnt")

# COMMAND ----------

response = requests.get(url)

# COMMAND ----------

dbutils.fs.mkdirs("/mnt/datalake/raw")

# COMMAND ----------

dbutils.fs.ls("/mnt/datalake/raw/despesas")

# COMMAND ----------

import pandas as pd
import zipfile
import io
import requests
from datetime import datetime
from pyspark.sql import SparkSession
from dateutil.parser import isoparse

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
                # Verificar se o arquivo é do ano atual ou posterior
                if filename.startswith(f"Ano-{ano}."):
                    # Extrair e carregar os dados do arquivo JSON em um DataFrame
                    with zip_file.open(filename) as json_file:
                        df = pd.read_json(json_file)
                    
                    # Expandir os dados da coluna "dados"
                    df_expandido = pd.json_normalize(df['dados'])
                    
                    # Converter a coluna "dataEmissao" para o tipo datetime
                    df_expandido["dataEmissao"] = pd.to_datetime(df_expandido["dataEmissao"])
                    
                    # Testar os dados antes de salvar no S3
                    testar_dados(df_expandido)
                    
                    # Salvar todos os dados no S3
                    salvar_dados_s3(df_expandido, ano)
                    
                    return df_expandido
        print(f"Nenhum dado disponível para o ano {ano}.")
        return None
    else:
        print(f"Erro ao baixar o arquivo ZIP para o ano {ano}.")
        return None

def testar_dados(df):
    print("Testando os dados antes de salvar no S3:")
    print("Primeiras linhas do DataFrame expandido:")
    print(df.head())
    print("\nInformações básicas sobre o DataFrame expandido:")
    print(df.info())
    # Adicione qualquer outra verificação de dados que você deseje realizar aqui

def salvar_dados_s3(df, ano):
    # Assuming df is a Pandas DataFrame
    spark = SparkSession.builder.getOrCreate()
    spark_df = spark.createDataFrame(df)

    # Repartition and save the DataFrame
    spark_df.repartition(1).write.format("json").mode("append").save(f"/mnt/datalake/raw/despesas/ano={ano}")

def obter_dados_cotas_atualizados():
    # Ano e mês atual
    ano_atual = datetime.now().year
    mes_atual = datetime.now().month
    
    # Baixar dados apenas para o ano e mês atual
    print(f"Obtendo dados atualizados para o mês {mes_atual} de {ano_atual}...")
    df = obter_dados_cotas_ano(ano_atual)
    
    if df is not None:
        # Verificar se a coluna 'dados' está presente
        if 'dados' in df.columns:
            # Extrair os dados da coluna 'dados'
            df = df['dados']
            
            # Converter a string de data em objeto datetime
            df["dataEmissao"] = df["dataEmissao"].apply(isoparse)
            
            # Filtrar os dados do mês atual
            df_mes_atual = df[df["dataEmissao"].dt.month == mes_atual]
            
            # Salvar dados do mês atual no S3
            if not df_mes_atual.empty:
                salvar_dados_s3(df_mes_atual, ano_atual)
            else:
                print(f"Nenhum dado disponível para o mês {mes_atual} de {ano_atual}.")
        else:
            print("A coluna 'dados' não está presente no DataFrame.")

# Iterar para os anos de 2023 até o ano atual
for ano in range(2023, datetime.now().year + 1):
    print(f"Obtendo dados para o ano {ano}...")
    obter_dados_cotas_ano(ano)

# Executar rotina de obtenção de dados atualizados mensalmente
obter_dados_cotas_atualizados()


# COMMAND ----------

import pandas as pd
import zipfile
import io
import requests
from datetime import datetime
from pyspark.sql import SparkSession
from dateutil.parser import isoparse

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
                # Verificar se o arquivo é do ano atual ou posterior
                if filename.startswith(f"Ano-{ano}."):
                    # Extrair e carregar os dados do arquivo JSON em um DataFrame
                    with zip_file.open(filename) as json_file:
                        df = pd.read_json(json_file)
                    
                    # Expandir os dados da coluna "dados"
                    df_expandido = pd.json_normalize(df['dados'])
                    
                    # Converter a coluna "dataEmissao" para o tipo datetime
                    df_expandido["dataEmissao"] = pd.to_datetime(df_expandido["dataEmissao"])
                    
                    # Salvar todos os dados no S3
                    salvar_dados_s3(df_expandido, ano)
                    
                    return df_expandido
        print(f"Nenhum dado disponível para o ano {ano}.")
        return None
    else:
        print(f"Erro ao baixar o arquivo ZIP para o ano {ano}.")
        return None

def salvar_dados_s3(df, ano):
    # Assuming df is a Pandas DataFrame
    spark = SparkSession.builder.getOrCreate()
    spark_df = spark.createDataFrame(df)

    # Repartition and save the DataFrame
    spark_df.repartition(1).write.format("json").mode("append").save(f"/mnt/datalake/raw/despesas/ano={ano}")

def obter_dados_cotas_atualizados():
    # Ano e mês atual
    ano_atual = datetime.now().year
    mes_atual = datetime.now().month
    
    # Baixar dados apenas para o ano e mês atual
    print(f"Obtendo dados atualizados para o mês {mes_atual} de {ano_atual}...")
    df = obter_dados_cotas_ano(ano_atual)
    
    if df is not None:
        # Verificar se a coluna 'dados' está presente
        if 'dados' in df.columns:
            # Extrair os dados da coluna 'dados'
            df = df['dados']
            
            # Converter a string de data em objeto datetime
            df["dataEmissao"] = df["dataEmissao"].apply(isoparse)
            
            # Filtrar os dados do mês atual
            df_mes_atual = df[df["dataEmissao"].dt.month == mes_atual]
            
            # Salvar dados do mês atual no S3
            if not df_mes_atual.empty:
                salvar_dados_s3(df_mes_atual, ano_atual)
            else:
                print(f"Nenhum dado disponível para o mês {mes_atual} de {ano_atual}.")
        else:
            print("A coluna 'dados' não está presente no DataFrame.")

# Iterar para os anos de 2023 até o ano atual
for ano in range(2023, datetime.now().year + 1):
    print(f"Obtendo dados para o ano {ano}...")
    obter_dados_cotas_ano(ano)

# Executar rotina de obtenção de dados atualizados mensalmente
obter_dados_cotas_atualizados()
