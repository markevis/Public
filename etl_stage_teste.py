'''
INSTALAR BIBLIOTECAS ABAIXO:

pip install python-certifi-win32

'''

import requests
import json
import csv
from google.cloud import storage
from google.cloud import bigquery
import os
from datetime import datetime,date, timedelta
import time
import ssl

# Importação dos results de apis_instagram
from apis_instagram import full_get_stories_id
from apis_instagram import full_get_midias_id
from apis_instagram import get_midias


import sys
sys.path.append('C:/ETL/Modulos/')
from pubsup_controle_cargas import envio_padrao_modelo_erros
from get_token_secret import access_secret_version
from disparo_email import disparo_email
from truncate_table import truncate_tabela

'''
Declaração de dada padrao d-1
'''
sysdate_d_1 = date.today() - timedelta(1)
hoje = date.today()
data_busca = str(sysdate_d_1)

# Get secrets
secret = access_secret_version()

# Google Storage + Nome CSV, Dataset name, prd
bucket_name = 'dados-prd'
dataset_STAGE = 'STAGE_AREA'
caminho_cliente = 'C:/conta-servico/gcp_cliente/cliente-prd.json'
projeto_gcp = 'cliente-prd'

'''
DECLARAÇÃO ARQUIVO DE CONTROLE CHAMADA
'''
ARQUIVO_CONTROLE_MIDIAS = 'C:/ETL/comunicacao/instagram/stage/controle/lista_midias.json'
ARQUIVO_CONTROLE_STORIES = 'C:ETL/comunicacao/instagram/stage/controle/lista_stories.json'

'''
DECLARAÇÃO DE TABELAS - BIG QUERY
'''

# FATO_INSTAGRAM_MIDIAS
tabela_FATO_INSTAGRAM_MIDIAS = 'FATO_INSTAGRAM_MIDIAS'
arquivo_FATO_INSTAGRAM_MIDIAS = 'FATO_INSTAGRAM_MIDIAS.csv'
uri_csv_FATO_INSTAGRAM_MIDIAS = 'gs://' + str(bucket_name) + '/FATO_INSTAGRAM_MIDIAS.csv'





def current_hour():
    now = datetime.now()
    dt_string = now.strftime("%Y-%m-%dT%H:%M:%S")
    return dt_string


def tratamento_texto(texto):
    texto_tratamento = str(texto)
    # texto_tratamento = texto_tratamento[1:-1]
    texto_tratamento = texto_tratamento.replace("\r", " ")
    texto_tratamento = texto_tratamento.replace('\n', ' ')
    #texto_tratamento = texto_tratamento.strip('""')
    texto_tratamento = texto_tratamento.replace('"', '')
    texto_tratamento = texto_tratamento.replace(';', ',')
    texto_tratamento = texto_tratamento.strip('\"')
    return texto_tratamento


def upload_arquivo(bucket_name,arquivo):
    """ Upload data to a bucket"""
    path_to_file = arquivo
    blob_name = arquivo
    # Explicitly use service account credentials by specifying the private key
    # file.
    storage_client = storage.Client.from_service_account_json(caminho_cliente)

    #print(buckets = list(storage_client.list_buckets())

    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(path_to_file)

    #returns a public url
    return blob.public_url


def inserir_tabela_bigquery(uri_csv, tabela_insert):
    # from google.cloud import bigquery
    client = bigquery.Client.from_service_account_json(caminho_cliente)
    dataset_id = 'STAGE_AREA'
    table_ref = client.dataset(dataset_STAGE).table(tabela_insert)

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    # Ambiente cliente
    job_config.labels = {"env": "cliente-bi"}
    job_config.field_delimiter = ";"
    # The source format defaults to CSV, so the line below is optional.
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1
    uri = uri_csv
    load_job = client.load_table_from_uri(
        uri, table_ref, job_config=job_config
    )  # API request
    print("Starting job {}".format(load_job.job_id))

    load_job.result()  # Waits for table load to complete.
    print("Job finished.")

    destination_table = client.get_table(table_ref)
    print("Loaded {} rows in {}.{}.".format(destination_table.num_rows,dataset_STAGE,tabela_insert))


def leitura_api_midias():
    full_get_midias_id(token)

    # abrir a lista de controle para guardar IDs em 'data[]'
    with open(ARQUIVO_CONTROLE_MIDIAS) as json_file:
        data = json.load(json_file)

    data_extracao = current_hour()
    linhas = []
    linhas_json = []
    # Iteração sobre os IDs
    for y in data:
        #print(data)
        midia_id = y['id']
        resultado, status = get_midias(midia_id)
        #print(resultado)

        for x in resultado:

            id_midia = midia_id

            try:
                comments_count = x['comments_count']
            except:
                comments_count = ''

            try:
                is_comment_enabled = x['is_comment_enabled']
            except:
                is_comment_enabled = ''

            try:
                like_count = x['like_count']
            except:
                like_count = ''

            try:
                media_product_type = x['media_product_type']
            except:
                media_product_type = ''

            try:
                media_type = x['media_type']
            except:
                media_type = ''

            try:
                media_url = x['media_url']
            except:
                media_url = ''

            try:
                owner = x['owner']['id']
            except:
                owner = ''

            try:
                permalink = x['permalink']
            except:
                permalink = ''

            try:
                thumbnail_url = x['thumbnail_url']
            except:
                thumbnail_url = ''

            try:
                timestamp = x['timestamp']
            except:
                timestamp = ''

            # tramento de dados
            comments_count = tratamento_texto(comments_count)
            is_comment_enabled = tratamento_texto(is_comment_enabled)
            like_count = tratamento_texto(like_count)
            media_product_type = tratamento_texto(media_product_type)
            media_type = tratamento_texto(media_type)
            media_url = tratamento_texto(media_url)
            owner = tratamento_texto(owner)
            permalink = tratamento_texto(permalink)
            thumbnail_url = tratamento_texto(thumbnail_url)
            timestamp = tratamento_texto(timestamp)

            resultado = [id_midia, comments_count, is_comment_enabled, like_count, media_product_type, media_type, media_url, owner, permalink, timestamp, data_extracao, thumbnail_url]
            resultado_json = {"id": id_midia, 'media_type': media_type}
            linhas.append(resultado)
            linhas_json.append(resultado_json)

        with open(ARQUIVO_CONTROLE_MIDIAS, 'r+') as json_file:
            json.dump(linhas_json, json_file)
        json_file.close

        with open(arquivo_FATO_INSTAGRAM_MIDIAS, "w", newline='', encoding="utf-8") as csvfile:
            csvwriter = csv.writer(csvfile, delimiter=';')
            # Write field name header line
            fields = ['id_midia', 'comments_count', 'is_comment_enabled', 'like_count', 'media_product_type', 'media_type', 'media_url', 'owner', 'permalink', 'thumbnail_url', 'timestamp', 'data_extracao', 'thumbnail_url']
            csvwriter.writerow(fields)
            for x in linhas:
                csvwriter.writerow(x)

# leitura_api_midias()


def main_FATO_INSTAGRAM_MIDIAS():



     try:

          #truncate_tabela(tabela_FATO_INSTAGRAM_MIDIAS)

    	  leitura_api_midias()
 
    	  upload_arquivo(bucket_name, arquivo_FATO_INSTAGRAM_MIDIAS)
  
          inserir_tabela_bigquery(uri_csv_FATO_INSTAGRAM_MIDIAS, tabela_FATO_INSTAGRAM_MIDIAS)
  

     except Exception as e:


   	titulo = 'Cliente - STAGE_AREA ' + tabela_FATO_INSTAGRAM_MIDIAS + '- python'
     	mensagem = '''
                     ERRO STAGE AREA - CRIACAO @tabela - python

                     ERRO:
                    @ERROR
                     '''
     	mensagem = mensagem.replace('@ERROR', str(e))
     	mensagem = mensagem.replace('@tabela', tabela_FATO_INSTAGRAM_MIDIAS)
    	disparo_email(titulo, mensagem)
     	caminho_arquivo = __file__
     	envio_padrao_modelo_erros('Builders', 'GCP', projeto_gcp, str(dataset_STAGE),
                                str(tabela_FATO_INSTAGRAM_MIDIAS), str(e),
                                  str(caminho_arquivo))





# Função para pegar tokens do Facebook
#C:/ETL/comunicacao/instagram/stage/output/access_token.json

def exchange_token():

    token_path = 'C:/ETL/comunicacao/instagram/stage/output/access_token.json'

    with open(token_path) as f:
        lines = f.readlines()
    token = lines[0]
    url = f"https://graph.facebook.com/v11.0/oauth/access_token?client_id={secret['fb_client_id']}&grant_type=fb_exchange_token&client_secret={secret['fb_client_secret']}&fb_exchange_token={token}"

    resultado = requests.request(url = url,method="GET")
    print(resultado.text)
    resultado = resultado.text.encode('utf8')
    resultado = json.loads(resultado)
    token = resultado['access_token']
    
    with open(token_path, "w") as access_token_file:
         access_token_file.write(token)
    return token
    

#token = exchange_token()
token = "EAAI3J1TFZCNgBAJvsvsw0vDhhuq60AJejVS0eNspn5izuuYJ50ZBFWxRMxr0SW1VUsSa9AmnrzXmbuoNTPJIXXBZAQ5GswmZAQ21z9DFwzvJllxbXv0aYFFRw3FYvqXPlOqx7p5fSaz5A1YCd8JIMfhbFqUmJ1xGs4I2nyuj2upizU0fgvkJtgDYGfcqURjq2xQlYFdlmAZDZD"

   

"""
MAINs CONSOLIDADAS

"""


main_FATO_INSTAGRAM_MIDIAS()


