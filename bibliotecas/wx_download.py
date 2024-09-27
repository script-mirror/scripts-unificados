import os
import time
import requests

def downloadByRequest(url, pathSaida, filename='', delay=60*15):
	""" Faz o download de produtos utilizando a lib REQUEST
	:param url: [STR] URL pronta para o download
	:param pathSaida: [STR] Diretorio de saida
	:param filename: [STR] Nome do arquivo de saida
	:param delay: [int] Delay entre cada tentativa de download
	:return filePath: [STR] Caminho completo do arquivo que acabou de ser baixado
	"""

	if not os.path.exists(pathSaida):
		os.makedirs(pathSaida)

	# 20 tentativas de baixar o produto
	for tentativa in range(20):
		try:
			file = requests.get(url, timeout=10*60)
			if file.status_code == 200:
				if filename == '':
					filename = url.split('/')[-1]
				filePath = os.path.join(pathSaida, filename)
				with open(filePath, 'wb') as f:
					f.write(file.content)

					print("\nURL: {}".format(url))
					print("Produto: {}".format(filename))
					print("Tipo: {}".format(file.headers['content-type']))
					print("Path: {}".format(filePath))
					return filePath
				break
			else:
				print("Nova tentiva em 2 minutos")
				time.sleep(delay)
		except:
			print('(! )Erro ao tentar baixar, nova tentativa em 2 minutos!')
			time.sleep(delay)
			continue

	return ''