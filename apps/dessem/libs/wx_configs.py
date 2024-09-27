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
			"previvaz" : {

			}, 
			"paths":{
				"dropbox_middle":"/home/wxenergy/Dropbox/WX - Middle/",
				"dessemNW":"/WX4TB/Documentos/fontes/PMO/dessem/"
			},
			# "configuracao_cv":{
			# 	"modelos":['PRECMEDIA_ETA40.GEFS', 'GEFS', 'GFS', 'PZERADA', "ETA40", "EC", "EC-ens"],
			# 	"rodadas":['00', '06'],
			# 	# numero minimo de arquivos de preciptacao (NovoSmap) necessarios para rodar o modelo
			# 	"min_arq_precip":1
			# },
			# "paths":{
			# 	"dropbox_middle":"/home/wxenergy/Dropbox/WX - Middle/"
			# },
			# "aws":{
			# 	# comando para o aws CLI (executavel)
			# 	"cli":"/usr/local/aws-cli/v2/2.0.4/bin/aws",
			# 	# configuracoes para o EC2
			# 	'public_dns':'ec2-3-228-191-240.compute-1.amazonaws.com',
			# 	'username_ssh':'Administrator',
			# 	'password_ssh':'hO$yJfqhKPJJt.g3MhG6Qy(=b3G&.(r*',
			# 	'instance_id':'i-0c8b51044f4f0580e',
			# 	# configuracoes para o S3
			# 	"bucket_cv":"wx-chuva-vazao",
			# 	"path_dropbox_middle": "C:/Users/Administrator/wx_workspace/wx_smap/arquivos/WX Energy Dropbox/WX - Middle/"
			# },
			# "whatsapp":{
			# 	'scriptEnviaMsg': '/WX2TB/Documentos/fontes/outros/bot_whatsapp',
			# 	'destinatario':['Resultados CV']
			# },
			"ssh":{
				"user":"wxenergy",
				"host":"wxe.ddns.net",
				"portaMaster": 8122,
				"portaNW": 8022,
				"pathKey": "/home/wxenergy/.ssh/id_rsa_edson"
			},
			"selenium":{
				"downloadDirectory": "",
				"seleniumDriverPath": "",
				"brownser": "chrome"
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


