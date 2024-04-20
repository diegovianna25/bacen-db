# Databricks notebook source
import requests
import zipfile
import io
import datetime
import pandas as pd
from pyspark.sql import SparkSession

# COMMAND ----------

def baixar_dados_cotas_mes_anterior():
    formato = "json"
    ano_atual = datetime.datetime.now().year
    mes_atual = datetime.datetime.now().month
    
    # Definir o ano e mês para a busca
    ano_mes_anterior = ano_atual if mes_atual != 1 else ano_atual - 1
    mes_anterior = mes_atual - 1 if mes_atual != 1 else 12
    
    url = f"http://www.camara.leg.br/cotas/Ano-{ano_mes_anterior}.{formato}.zip"
    
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
            print(f"Arquivos extraídos para o ano {ano_mes_anterior}: {arquivos_extraidos}")
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
                    # Filtrar os dados do mês anterior ao atual
                    data_inicio_mes_anterior = datetime.datetime(ano_mes_anterior, mes_anterior, 1)
                    data_fim_mes_anterior = datetime.datetime(ano_atual, mes_atual, 1) - datetime.timedelta(days=1)
                    df_expandido = df_expandido[(df_expandido["dataEmissao"] >= data_inicio_mes_anterior) & (df_expandido["dataEmissao"] <= data_fim_mes_anterior)]
                    
                    # Salvar o DataFrame como um arquivo JSON usando Spark DataFrame API
                    spark = SparkSession.builder.getOrCreate()
                    spark_df = spark.createDataFrame(df_expandido)
                    caminho = "/mnt/datalake/raw"
                    spark_df.repartition(1).write.format("json").mode("append").save(caminho)
                    
                    print(f"Dados salvos em '{caminho}'")
    else:
        print(f"Erro ao baixar o arquivo ZIP para o ano {ano_mes_anterior}.")

# Baixar dados para o mês anterior ao atual
print(f"Obtendo dados para o mês anterior ao atual...")
baixar_dados_cotas_mes_anterior()

