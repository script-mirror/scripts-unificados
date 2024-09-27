import json
from pathlib import Path

def createConfgFile(config_file_name, default_configs=''):
	""" Cria o arquivo de configuracao de entrada para o script
	:param config_file_name: nome do arquivo no qual as configuracoes serao armazenados
	:return None: 
	"""

	#Configuracoes defaults
	if default_configs == '':
		default_configs = {
			"paths":{
				"pastaTmp":"/WX2TB/Documentos/fontes/PMO/enviar_produtos/arquivos/tmp",
				"arquivosHidrologia": "/WX2TB/Documentos/fontes/PMO/monitora_ONS/plan_acomph_rdh",
				"listaProdutosBbce": ""

			},
			"whatsapp":{
				'scriptEnviaMsg': '/WX2TB/Documentos/fontes/outros/bot_whatsapp',
				'destinatario': 'WX - Meteorologia'
			}
		}

	config_file = open(config_file_name, 'w')

	# Salvar as configuracoes com identacao e sortido 
	json.dump(default_configs, config_file, sort_keys=True, indent=4)
	print("Criado o arquivo '{0}' com as configuracoes padroes.".format(config_file_name))

	config_file.close()

def getConfiguration(config_file_name='config.json'):
	""" Verifica se existe o arquivo de configuracoes (config_file_name). Caso nao exista um novo e criado com as configuracoes padroes
	:param config_file_name: 
	:return configs: Dicionario contendo todas as configuracoes armazenadas no arquivo config_file_name
	"""
	
	if not Path(config_file_name).exists():
		createConfgFile(config_file_name)

	with open(config_file_name) as configs_file:
		configs = json.load(configs_file)

	return configs



if __name__ == '__main__':
	configs = getConfiguration()
	print(configs)


