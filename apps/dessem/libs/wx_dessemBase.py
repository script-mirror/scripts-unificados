import os
import pdb
import sys
import time
import shutil
import zipfile
import datetime
import requests

import pandas as pd


sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.bibliotecas import wx_opweek, wx_download


path_decks = os.path.abspath("/WX2TB/Documentos/fontes/PMO/decks")

def downloadArquivoBase(pathArquivos, dtAtual=None):

	print('Baixando arquivo base do Dessem')

	if dtAtual == None:
		dtAtual = datetime.datetime.now()

	dtFinal_str = dtAtual.strftime('%d/%m/%Y')
	dtInicial_str = wx_opweek.getLastFriday((dtAtual - datetime.timedelta(days=28))).strftime('%d/%m/%Y')

	ultimoSabado = wx_opweek.getLastSaturday(dtAtual)
	semanaEletrica = wx_opweek.ElecData(ultimoSabado.date())

	headers = {
	    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:94.0) Gecko/20100101 Firefox/94.0',
	    'Accept': '*/*',
	    'Accept-Language': 'pt-BR,en-US;q=0.7,en;q=0.3',
	    'X-Requested-With': 'XMLHttpRequest',
	    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
	    'ADRUM': 'isAjax:true',
	    'Origin': 'https://www.ccee.org.br',
	    'Connection': 'keep-alive',
	    'Referer': 'https://www.ccee.org.br/web/guest/acervo-ccee',
	    'Sec-Fetch-Dest': 'empty',
	    'Sec-Fetch-Mode': 'cors',
	    'Sec-Fetch-Site': 'same-origin',
	}

	params = (
	    ('p_p_id', 'org_ccee_acervo_portlet_CCEEAcervoPortlet_INSTANCE_tixm'),
	    ('p_p_lifecycle', '2'),
	    ('p_p_state', 'normal'),
	    ('p_p_mode', 'view'),
	    ('p_p_cacheability', 'cacheLevelPage'),
	    ('_org_ccee_acervo_portlet_CCEEAcervoPortlet_INSTANCE_tixm_param1', 'Value1'),
	)

	data = {
	  '_org_ccee_acervo_portlet_CCEEAcervoPortlet_INSTANCE_tixm_resultadosPagina': '10',
	  '_org_ccee_acervo_portlet_CCEEAcervoPortlet_INSTANCE_tixm_keyword': 'deck dessem',
	  '_org_ccee_acervo_portlet_CCEEAcervoPortlet_INSTANCE_tixm_numberPage': '0',
	  '_org_ccee_acervo_portlet_CCEEAcervoPortlet_INSTANCE_tixm_initialDate': '{}'.format(dtInicial_str),
	  '_org_ccee_acervo_portlet_CCEEAcervoPortlet_INSTANCE_tixm_finalDate': '{}'.format(dtFinal_str)
	}

	response = requests.post('https://www.ccee.org.br/web/guest/acervo-ccee', headers=headers, params=params, data=data)
	data = response.json()

	nome_complementar_esperado = 'Dessem - {:0>2}/{}'.format(semanaEletrica.mesReferente, semanaEletrica.anoReferente)
	for result in data['results']:
		if result['nomeComplementar'] == nome_complementar_esperado:
			url = result['url']

	arquivoDessem = url.strip().split('/')[-2]

	if not os.path.exists(pathArquivos):
		os.makedirs(pathArquivos)

	return wx_download.downloadByRequest(url=url, pathSaida=pathArquivos, filename=arquivoDessem)

def organizarArquivoBase(data, pathDessemZip):
	""" Organiza os arquivos do DESSEM baixados da CCEE
	:param data: [DATETIME] Data atual
	:param pathDessemZip: [STR] Path do arquivo ZIP com os arquivos dessem
	:return None: 
	"""

	ultimoSabado = wx_opweek.getLastSaturday(data)
	semanaEletrica = wx_opweek.ElecData(ultimoSabado.date())

	pathEntrada = os.path.dirname(pathDessemZip)
	pathEntrada = os.path.join(pathEntrada, (data).strftime('%Y%m%d'), 'entrada')

	pathDessemZip = os.path.abspath(pathDessemZip)
	pathDessem = os.path.splitext(pathDessemZip)[0]

	if os.path.exists(pathDessem):
		shutil.rmtree(pathDessem)
		time.sleep(1)

	with zipfile.ZipFile(pathDessemZip, 'r') as zip_ref:
		zip_ref.extractall(pathDessem)

	fileName = 'DS_CCEE_{:0>2}{}_SEMREDE_RV{}D{}.zip'.format(semanaEletrica.mesReferente, semanaEletrica.anoReferente, semanaEletrica.atualRevisao, data.strftime('%d'))
	deckDiaZip = os.path.join(pathDessem, fileName)
	
	if not os.path.exists(deckDiaZip):
		return False

	folderName, _zipextension = fileName.split('.')

	pathDeckExtract = os.path.join(pathEntrada, 'ccee_entrada')
	pathDeckExtract2 = os.path.join(path_decks, 'ccee', 'ds', folderName)

	for pathDst in [pathDeckExtract, pathDeckExtract2]:

		if os.path.exists(pathDst):
			shutil.rmtree(pathDst) 

		with zipfile.ZipFile(deckDiaZip, 'r') as zip_ref:
			zip_ref.extractall(pathDst)
		print(pathDst)
		
	resultDiaZip = 'Resultado_DS_CCEE_{:0>2}{}_SEMREDE_RV{}D{}.zip'.format(semanaEletrica.mesReferente, semanaEletrica.anoReferente, semanaEletrica.atualRevisao, data.strftime('%d'))
	resultDiaZip = os.path.join(pathDessem, resultDiaZip)
	resultDiaPath = os.path.join(pathEntrada, 'ccee_saida')

	for path in [resultDiaPath]:
		if os.path.exists(path):
			shutil.rmtree(path)

		with zipfile.ZipFile(resultDiaZip, 'r') as zip_ref:
			zip_ref.extractall(path)

	shutil.rmtree(pathDessem)
	os.remove(pathDessemZip)

	return True


def readPdoOper(path):
	""" Faz a leitura do arquivo PDO_OPERACAO e retorna um dicionario com todas as infos
	:param path: [STR] Caminho do arquivo de PDO_OPERACAO.DAT
	:return dicionario: [DIC] Informacoes contidas no PDO_OPERACAO
	"""

	file = open(path, 'r', encoding="latin1")
	fileText = file.readlines()

	datetimeInicio = ''

	balancoHidraulico = pd.DataFrame()
	afluencuasDefluencias = pd.DataFrame()
	geracaoHidro = pd.DataFrame()
	geracaoTermo = pd.DataFrame()
	

	periodo = 0
	for i in range(len(fileText)):
		line = fileText[i]
		if 'PERIODO:' in line:
			line = line.split()

			if datetimeInicio == '':
				inicio = '{} {}'.format(line[3], line[5])
				datetimeInicio = datetime.datetime.strptime(inicio, '%d/%m/%Y %H:%M')

			periodo = int(line[1])

		elif '1 - BALANCO HIDRICO POR USINA (hm3):' in line:
			i += 5
			numeroUsinasHidro = 0
			while line != '\n':
				numeroUsinasHidro += 1
				line = fileText[i+numeroUsinasHidro]

			# Nomes das usinas podem ser espaçadas, por isso o numero e o nome tem delimitadores de espaço
			df_bloco = pd.DataFrame([[x[0:4].strip(), x[5:19].strip()] + x[19:].split() for x in fileText[i:i+numeroUsinasHidro]])
			df_bloco['PERIODO'] = periodo
			balancoHidraulico = balancoHidraulico.append(df_bloco)
			i += numeroUsinasHidro


		elif '2 - AFLUENCIAS E DEFLUENCIAS POR USINA (m3/s):' in line:
			i += 4
			df_bloco = pd.DataFrame([[x[0:4].strip(), x[5:19].strip()] + x[19:].split() for x in fileText[i:i+numeroUsinasHidro]])
			df_bloco['periodo'] = periodo
			afluencuasDefluencias = afluencuasDefluencias.append(df_bloco)
			i += numeroUsinasHidro

		elif '3 - GERACAO HIDROELETRICA:' in line:
			i += 5
			df_bloco = pd.DataFrame([[x[0:3].strip(), x[4:18].strip()] + x[18:].split() for x in fileText[i:i+numeroUsinasHidro]])
			df_bloco['periodo'] = periodo
			geracaoHidro = geracaoHidro.append(df_bloco)
			i += numeroUsinasHidro

		elif '4 - GERACAO TERMOELETRICA (MW):' in line:
			i += 4

			numeroUsinasTermo = 0
			while line != '\n':
				numeroUsinasTermo += 1
				line = fileText[i+numeroUsinasTermo]

			df_bloco = pd.DataFrame([[x[0:4].strip(), x[5:18].strip()] + x[18:].split() for x in fileText[i:i+numeroUsinasTermo]])
			df_bloco['periodo'] = periodo
			geracaoTermo = geracaoTermo.append(df_bloco)
			i += numeroUsinasTermo

	
	balancoHidraulico.columns = ['IND', 'NOME', 'SIST', 'P_VOLI', 'VOLI', 'VINC', 'VMONTV', 'VMONT', 'VTUR', 'VERT', 'VDESV', 'VDESC', 'VEVAP', 'VALT', 'VBOMB', 'P_VOLF', 'VOLF', 'PERIODO']
	balancoHidraulico['IND'] = balancoHidraulico['IND'].astype(int)
	balancoHidraulico.set_index(['IND', 'PERIODO'], drop=True, inplace=True)

	afluencuasDefluencias.columns = ['IND',  'NOME',  'SIST',  'QINC',  'QMONTV',  'QMONT',  'QTUR',  'QVERT',  'QDESV',  'QDESC',  'QALT',  'VBOMB',  'DEFMIN',  'DEFMAX', 'PERIODO']
	afluencuasDefluencias['IND'] = afluencuasDefluencias['IND'].astype(int)
	afluencuasDefluencias.set_index(['IND', 'PERIODO'], drop=True, inplace=True)

	geracaoHidro.columns = ['IND', 'NOME', 'SIST', 'QTUR', 'QMAX', 'GHID', 'RESER', 'GHMAX', 'VT', 'PIAGUA', 'HQUEDA', 'PRODUT', 'PERIODO']
	geracaoHidro['IND'] = geracaoHidro['IND'].astype(int)
	geracaoHidro.set_index(['IND', 'PERIODO'], drop=True, inplace=True)

	geracaoTermo.columns = ['IND', 'NOME', 'SIST', 'GTMIN', 'GTER', 'RESER', 'GTMAX', 'CAPAC', 'PERIODO']
	geracaoTermo['IND'] = geracaoTermo['IND'].astype(int)
	geracaoTermo.set_index(['IND', 'PERIODO'], drop=True, inplace=True)

	return {'balancoHidraulico':balancoHidraulico ,'afluencuasDefluencias':afluencuasDefluencias,'geracaoHidro':geracaoHidro,'geracaoTermo':geracaoTermo}


if __name__ == '__main__':
	pass