'''
INSTALAR BIBLIOTECAS ABAIXO:

pip install python-certifi-win32

'''
import requests
import json

import sys
sys.path.append('C:/marketclub_etl/ETL/Modulos/')
from pubsup_controle_cargas import envio_padrao_modelo_erros
from get_token_secret import access_secret_version
from disparo_email import disparo_email

# Get secrets
secret = access_secret_version()

ARQUIVO_CONTROLE_MIDIAS = 'C:/ETL/comunicacao/instagram/stage/controle/lista_midias.json'
ARQUIVO_CONTROLE_STORIES = 'C:/ETL/comunicacao/instagram/stage/controle/lista_stories.json'



# Função para pegar todos os IDs de mídias do Instagram


def get_midias_id(token, proxima_pagina='SEM_PAGINA'):

	if proxima_pagina == 'SEM_PAGINA':
		url = "https://graph.facebook.com/v11.0/623592018935000/media?access_token=" + token

	elif proxima_pagina != 'SEM_PAGINA':
		url = "https://graph.facebook.com/v11.0/623592018935000/media?access_token=" + token + "&pretty=1&limit=25&after=" + str(proxima_pagina)

	# print(url)

	payload = {}

	resultado = requests.request("GET", url, data=payload)
	status = resultado.status_code
	resultado = resultado.text.encode('utf8')
	resultado = json.loads(resultado)
	resultado = json.dumps(resultado, indent=4, sort_keys=True)
	resultado = json.loads(resultado)
	# print(resultado)

	try:
		retorno_dados = resultado['data']
	except:
		retorno_dados = []

	try:
		# next = resultado['paging']['next']
		proxima_pagina = resultado['paging']['cursors']['after']
	except:
		proxima_pagina = 'SEM_PAGINA'

	# print(proxima_pagina)

	return retorno_dados, proxima_pagina, status


def full_get_midias_id(token):
	dados_completo = []
	resultado, pagina, status = get_midias_id(token)
	print(resultado)
	for x in resultado:
		dados_completo.append(x)
	while pagina != 'SEM_PAGINA':
		resultado, pagina, status = get_midias_id(token, pagina)
		for y in resultado:
			dados_completo.append(y)
	#print(dados_completo)

	with open(ARQUIVO_CONTROLE_MIDIAS, "w", newline='', encoding='utf-8') as outfile:
		json.dump(dados_completo, outfile)

	return dados_completo, status

#full_get_midias_id()





def get_midias(midia_id):
	lista_resultado = []

	url = "https://graph.facebook.com/v11.0/" + str(midia_id) + "?fields=comments_count,id,is_comment_enabled,like_count,media_product_type,media_type,media_url,owner,permalink,thumbnail_url,timestamp,video_title&access_token=" + token
	# print(url)

	payload = {}

	resultado = requests.request("GET", url, data=payload)
	resultado = requests.get('https://graph.facebook.com', verify=false) # Driblar Firewall#
	status = resultado.status_code
	resultado = resultado.text.encode('utf8')
	resultado = json.loads(resultado)
	resultado = json.dumps(resultado, indent=4, sort_keys=True)
	resultado = json.loads(resultado)
	lista_resultado.append(resultado)
	resultado = lista_resultado
	#print(resultado)

	return resultado, status

#get_midias('')




