# -*- coding: utf-8 -*-
import os
import re
import sys
import pdb
import json
import time
import glob
import codecs
import shutil
import datetime

import pandas as pd

sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.bibliotecas import wx_selenium,wx_opweek
from PMO.scripts_unificados.apps.dessem.libs import wx_dessemBase



def downloadRestricoes(data, pathArquivos):
	""" Baixa do site do ONS (FSARH) o arquivo de restricoes pronto para rodar o DESSEM
	:param data: [DATETIME] Data anterior a data desejada no dessem
	:param pathArquivos: [STR] Path aonde irao ficar os aquivos baixados
	:return pathDestino: Path do arquivo baixado
	"""
	pathArqEntrada = os.path.join(pathArquivos, data.strftime('%Y%m%d'), 'entrada')
	pathTemp = os.path.join(pathArquivos, 'temporarios')

	operuhFiles = glob.glob(r'{}/OPERUH_*.dat'.format(pathTemp))
	for file in operuhFiles:
		os.remove(file)

	driver = wx_selenium.openSelenium()
	wx_selenium.fazer_login_ONS(driver)
	driver.get('https://integracaoagentes.ons.org.br/FSAR-H/SitePages/Exibir_Forms_FSARH.aspx#')

	for i in range(5):
		try:
			btn_gerarDessem = driver.find_element_by_id("buttonGerarArq")
			btn_gerarDessem.click()
			break
		except:
			time.sleep(5)


	inp_dataInicial = driver.find_element_by_id("dessemDataInicial")
	inp_dataInicial.clear()
	inp_dataInicial.send_keys(data.strftime('%d/%m/%Y'))
	time.sleep(1)

	proximoSabado = data + datetime.timedelta(days=1)
	while proximoSabado.weekday() != 5:
		proximoSabado += datetime.timedelta(days=1)

	inp_dataFinal = driver.find_element_by_id("dessemDataFinal")
	inp_dataFinal.clear()
	inp_dataFinal.send_keys(proximoSabado.strftime('%d/%m/%Y'))
	time.sleep(1)
	
	btn_gerarTxtDessem = driver.find_element_by_id("gerarTxtDessem")
	btn_gerarTxtDessem.click()
	time.sleep(1)
	
	for i in range(5):
		newOperuh = glob.glob(r'{}/OPERUH_*.dat'.format(pathTemp))
		if len(newOperuh) == 0:
			time.sleep(5)
			continue
		else:
			newOperuh = newOperuh[0]

	pathDestino = os.path.join(pathArqEntrada, os.path.basename(newOperuh))
	shutil.move(newOperuh, pathDestino)

	driver.quit()

	return pathDestino



def getFromFile(path):
	file = open(path, 'r', encoding='latin')
	return file.readlines()

def leituraArquivo(filePath):

	arquivo = getFromFile(filePath)
	dados = {}
	for iLine in range(len(arquivo)):
		line = arquivo[iLine]

		if line[0] == '&'  or line[0] == '\n':
			continue
		else:
			mnemonico = line.split()[1]		
			if mnemonico not in dados:
				dados[mnemonico] = []
			dados[mnemonico].append(line)

	return dados

def getInfoBlocos():
	blocos = {}

	blocos['REST'] = {'campos':[
						'peruh',
						'mnemonico',
						'id',
						'tipo',
						'f_incluir',
						'justificativa',
						'ValInicial',
				],
				'regex':'(.{6}) (.{6}) (.{5})  (.{1})  (.{1})  (.{0,12}).{0,1}(.{0,10})(.*)',
				'formatacao':'{:>6} {:>6} {:>5}  {:>1}  {:>1}  {:<12}{:>10}'}

	blocos['ELEM'] = {'campos':[
						'peruh',
						'mnemonico',
						'id',
						'numUsina',
						'nomeUsina',
						'H',
						'variavel',
						'fator',
				],
				'regex':'(.{6}) (.{6}) (.{5}) (.{3}) (.{13}) (.{1}) (.{2}) (.{5})(.*)',
				'formatacao':'{:>6} {:>6} {:>5} {:>3} {:<13} {:>1} {:>2} {:>5}'}

	blocos['LIM'] = {'campos':[
						'peruh',
						'mnemonico',
						'id',
						'di',
						'hi',
						'mi',
						'df',
						'hf',
						'mf',
						'linf',
						'lsup',
				],
				'regex':'(.{6}) (.{6}) (.{5}) (.{2}) (.{2}) (.{1}) (.{2}) (.{2}) (.{1})   (.{0,10})(.{0,10})(.*)',
				'formatacao':'{:>6} {:>6} {:>5} {:>2} {:>2} {:>1} {:>2} {:>2} {:>1}   {:>10}{:>10}'}

	blocos['VAR'] = {'campos':[
						'peruh',
						'mnemonico',
						'id',
						'di',
						'hi',
						'mi',
						'df',
						'hf',
						'mf',
						'dec',
						'acre',
						'decAnt',
						'acreAnt',
				],
				'regex':'(.{6}) (.{6}) (.{5})(.{2}) (.{2}) (.{1}) (.{2}) (.{2}) (.{1})   (.{0,10})(.{0,10})(.{0,10})(.{0,10})(.*)',
				'formatacao':'{:>6} {:>6} {:>5}{:>2} {:>2} {:>1} {:>2} {:>2} {:>1}   {:>10}{:>10}{:>10}{:>10}'}

	blocos['COND'] = {'campos':[
						'peruh',
						'mnemonico',
						'id',
						'di',
						'hi',
						'mi',
						'df',
						'hf',
						'mf',
						'dec',
						'acre',
						'decAnt',
						'acreAnt',
				],
				'regex':'(.{6}) (.{6}) (.{5}) (.{10}) (.{5})  (.{10})(.*)',
				'formatacao':'{:>6} {:>6} {:>5} {:>10} {:>5}  {:>10}'}


	return blocos

def extrairInfoBloco(listaLinhas, mnemonico, regex):
	blocos = []
	if mnemonico in listaLinhas:
		for i, linha in enumerate(listaLinhas[mnemonico]):
			infosLinha = re.split(regex, linha)
			if infosLinha == []:
				pdb.set_trace()
			blocos.append(infosLinha[1:-2])   # ultimo termo da lista e o que sobra da expressao regex (/n)

	return blocos

def atualizarValoresIniciais(restricoesRest, restricoesElem, pdoOperacao):
	""" Atualiza os valores iniciais daS variaveis de retricao do tipo variacao 
	:param restricoesRest: Restricoes REST 
	:param restricoesElem: Restricoes ELEM
	:param pdoOperacao: Informacoes da ultima operacao contidas no PDO_OPERACAO
	:return restricoes: Resticoes atualizadas com os valores iniciais
	"""

	resultadoOperacao = wx_dessemBase.readPdoOper(pdoOperacao)
	balanHidro = resultadoOperacao['balancoHidraulico']
	afluDeflu = resultadoOperacao['afluencuasDefluencias']
	gerHidro = resultadoOperacao['geracaoHidro']
	gerTermo = resultadoOperacao['geracaoTermo']

	for numRestricao, restricao in restricoesRest[restricoesRest['tipo'].str.strip() == 'V'].iterrows():

		informacoesElem = restricoesElem[restricoesElem['id'] == restricao['id']]
		varRestrita = int(informacoesElem['variavel'].to_list()[0])
		id_UHE = int(informacoesElem['numUsina'].to_list()[0])

		# Duvida
		if varRestrita == 1:
			restricoesRest.at[numRestricao, 'ValInicial'] = float(balanHidro.loc[(id_UHE, 48)]['VOLI'])
		# Duvida
		elif varRestrita == 2:
			restricoesRest.at[numRestricao, 'ValInicial'] = float(balanHidro.loc[(id_UHE, 48)]['P_VOLI'])
		elif varRestrita == 3:
			restricoesRest.at[numRestricao, 'ValInicial'] = float(afluDeflu.loc[(id_UHE, 48)]['QTUR'])
		elif varRestrita == 4:
			restricoesRest.at[numRestricao, 'ValInicial'] = float(afluDeflu.loc[(id_UHE, 48)]['QVERT'])
		elif varRestrita == 5:
			restricoesRest.at[numRestricao, 'ValInicial'] = float(afluDeflu.loc[(id_UHE, 48)]['QDESV'])
		# Duvida
		elif varRestrita == 6:
			restricoesRest.at[numRestricao, 'ValInicial'] = float(afluDeflu.loc[(id_UHE, 48)]['QTUR']) + float(afluDeflu.loc[(id_UHE, 48)]['QVERT'])
		elif varRestrita == 7:
			restricoesRest.at[numRestricao, 'ValInicial'] = float(gerHidro.loc[(id_UHE, 48)]['GHID'])
		elif varRestrita == 8:
			restricoesRest.at[numRestricao, 'ValInicial'] = float(afluDeflu.loc[(id_UHE, 48)]['VBOMB'])
		elif varRestrita == 9:
			restricoesRest.at[numRestricao, 'ValInicial'] = float(afluDeflu.loc[(id_UHE, 48)]['QINC'])

	return restricoesRest

def getCabecalhos(mnemonico):

	cabecalho = []

	if mnemonico == 'REST':
		cabecalho.append('&-----------------------------------------------------------------------------------------')
		cabecalho.append('&PERUH REST Ind T Descricao VL Inicial')
		cabecalho.append('&XXXXX XXXXXX xxxxx x xxxxxxxxxxxx xxxxxxxxxx')
		cabecalho.append('&-----------------------------------------------------------------------------------------')

	return cabecalho

def formatarBloco(df_bloco, formatacao):

	bloco = []
	if isinstance(df_bloco, pd.DataFrame):
		for index, row in df_bloco.iterrows():
			bloco.append(formatacao.format(*row.values))
	elif isinstance(df_bloco, pd.Series):
		bloco.append(formatacao.format(*df_bloco.values))

	return bloco


def gravarArquivo(pathSaida, valores):
	fileOut = codecs.open(pathSaida, 'a+', 'utf-8')

	fileOut.write('&\n')
	for linha in valores:
		fileOut.write('{}\n'.format(linha.strip()))
	fileOut.write('&\n')

	fileOut.close()



def gerarOperuh(data, pathArquivos):

	try:
		arquivoMudancasOnsCcee = os.path.join(pathArquivos, 'diferencasOnsCcee.xlsx')
		df_mudancasCcee = pd.ExcelFile(arquivoMudancasOnsCcee)
		mudancasCcee_restricoes = df_mudancasCcee.parse('restricoes', header=None, skiprows=[0,1,2,3,4]).convert_dtypes()
	except:
		mudancasCcee_restricoes = pd.DataFrame()
		print('Não foi encontrado nenhum arquivo com as configurações que se aplicam apenas na CCEE\n{}'.format(arquivoMudancasOnsCcee))

	pathArqEntrada = os.path.join(pathArquivos, data.strftime('%Y%m%d'), 'entrada')

	pathArqSaida = os.path.join(pathArquivos, data.strftime('%Y%m%d'), 'saida')
	if not os.path.exists(pathArqSaida):
		os.makedirs(pathArqSaida)

	dataEletrica = wx_opweek.getLastSaturday(data)
	dataEletrica = wx_opweek.ElecData(dataEletrica)

	diaReferente = (data + datetime.timedelta(days=1)).day
	diasSemanaPassado = []
	diasSemanaFuturo = []
	primeiroDiaSemanaEletrica = dataEletrica.inicioSemana
	for i in range(7):
		dia = primeiroDiaSemanaEletrica + datetime.timedelta(days=i)
		if dia <= data.date():
			diasSemanaPassado.append(dia.day)
		else:
			diasSemanaFuturo.append(dia.day)
	# calendarioAux = {str(data.day):str(diaReferente)}

	operuhDiaAnterior = leituraArquivo(os.path.join(pathArqEntrada, 'ccee_entrada', 'operuh.dat'))

	pathOperuhOut = os.path.join(pathArqSaida,'operuh.dat')
	fileOut = open(pathOperuhOut, 'w')
	fileOut.close()

	infoBlocos = getInfoBlocos()

	restricoesDiaAnterior = {}
	blocoREST = extrairInfoBloco(operuhDiaAnterior, 'REST', infoBlocos['REST']['regex'])
	restricoesDiaAnterior['REST'] = pd.DataFrame(blocoREST, columns=infoBlocos['REST']['campos'])
	
	blocoELEM = extrairInfoBloco(operuhDiaAnterior, 'ELEM', infoBlocos['ELEM']['regex'])
	restricoesDiaAnterior['ELEM'] = pd.DataFrame(blocoELEM, columns=infoBlocos['ELEM']['campos'])

	blocoLIM = extrairInfoBloco(operuhDiaAnterior, 'LIM', infoBlocos['LIM']['regex'])
	restricoesDiaAnterior['LIM'] = pd.DataFrame(blocoLIM, columns=infoBlocos['LIM']['campos'])

	blocoVAR = extrairInfoBloco(operuhDiaAnterior, 'VAR', infoBlocos['VAR']['regex'])
	restricoesDiaAnterior['VAR'] = pd.DataFrame(blocoVAR, columns=infoBlocos['VAR']['campos'])

	blocoCOND = extrairInfoBloco(operuhDiaAnterior, 'COND', infoBlocos['COND']['regex'])
	restricoesDiaAnterior['COND'] = pd.DataFrame(blocoCOND, columns=infoBlocos['COND']['campos'])

	restricoesDiaAnterior['LIM']['df'] = restricoesDiaAnterior['LIM']['df'].apply(pd.to_numeric, errors='ignore')
	restFinalizadas = restricoesDiaAnterior['LIM'][restricoesDiaAnterior['LIM']['df'].isin(diasSemanaPassado+[diaReferente])]
	for  idRest in restFinalizadas['id'].unique():
		restFinalizadasMesmoId = restFinalizadas[restFinalizadas['id'] == idRest]
		restricoesDiaAnteriorMesmoId = restricoesDiaAnterior['LIM'][restricoesDiaAnterior['LIM']['id'] == idRest]

		# Retira todas as restricoes com o mesmo ID
		if restFinalizadasMesmoId.shape[0] == restricoesDiaAnteriorMesmoId.shape[0]:
			indexRest = restricoesDiaAnterior['REST'][restricoesDiaAnterior['REST']['id'] == idRest].index
			restricoesDiaAnterior['REST'].drop(indexRest, inplace=True)

		# Retira apenas o periodo finalizado
		else:
			restricoesDiaAnterior['LIM'].drop(restFinalizadasMesmoId.index, inplace=True)

		restricoesDiaAnteriorMesmoId = restricoesDiaAnterior['LIM'][restricoesDiaAnterior['LIM']['id'] == idRest]

	# Restricoes exclusivas da ccee
	for index, rest in mudancasCcee_restricoes.iterrows():
		if rest.iloc[1] < data:
			continue

		mnemonico = rest.iloc[2].strip()
		idRestricao = rest.iloc[3]
		if rest.iloc[0] == 'Comentar':
			restricoesDiaAnterior[mnemonico] = restricoesDiaAnterior[mnemonico].drop(restricoesDiaAnterior[mnemonico].loc[restricoesDiaAnterior[mnemonico]['id'] == idRestricao].index)
		elif rest.iloc[0] == 'Adicionar':
			if mnemonico == 'REST':
				newRow = dict(zip(restricoesDiaAnterior[mnemonico].columns,['OPERUH', 'REST  ']+rest.fillna('')[3:8].tolist()))
			elif mnemonico == 'ELEM':
				newRow = dict(zip(restricoesDiaAnterior[mnemonico].columns,['OPERUH', 'ELEM  ']+rest.fillna('')[3:9].tolist()))
			elif mnemonico == 'LIM':
				newRow = dict(zip(restricoesDiaAnterior[mnemonico].columns,['OPERUH', 'LIM   ']+rest.fillna('')[3:12].tolist()))
			elif mnemonico == 'VAR':
				newRow = dict(zip(restricoesDiaAnterior[mnemonico].columns,['OPERUH', 'VAR   ']+rest.fillna('')[3:14].tolist()))
			elif mnemonico == 'COND':
				newRow = dict(zip(restricoesDiaAnterior[mnemonico].columns,['OPERUH', 'COND ']+rest.fillna('')[3:14].tolist()))
			restricoesDiaAnterior[mnemonico] = restricoesDiaAnterior[mnemonico].append(newRow, ignore_index=True)
		else:
			print("Sem tratamento para a acao {} no index {}".format(rest.iloc[0], index+1))

	# Formatacao da coluna de ID para conter 5 digitos
	for mnemonico in ['REST','ELEM','LIM','VAR','COND']:
		# restricoesDiaAnterior[mnemonico]['id'] = restricoesDiaAnterior[mnemonico]['id'].apply(lambda x: "{:0>5}".format(x))
		for index, row in restricoesDiaAnterior[mnemonico].iterrows():
			row['id'] = "{:0>5}".format(row['id'])


	listaRestricoes = []
	for index, row in restricoesDiaAnterior['REST'].iterrows():
		for tipo in restricoesDiaAnterior:
			df_rest = restricoesDiaAnterior[tipo][restricoesDiaAnterior[tipo]['id'] == row['id']]
			listaRestricoes += formatarBloco(df_rest, infoBlocos[tipo]['formatacao'])
	
	gravarArquivo(pathOperuhOut, listaRestricoes)

	print('operuh.dat: {}'.format(pathOperuhOut))
	return pathOperuhOut

def gerarOperuhDef(data, pathArquivos):

	try:
		arquivoMudancasOnsCcee = os.path.join(pathArquivos, 'diferencasOnsCcee.xlsx')
		df_mudancasCcee = pd.ExcelFile(arquivoMudancasOnsCcee)
		mudancasCcee_restricoes = df_mudancasCcee.parse('restricoes', header=None, skiprows=[0,1,2,3,4]).convert_dtypes()
	except:
		mudancasCcee_restricoes = pd.DataFrame()
		print('Não foi encontrado nenhum arquivo com as configurações que se aplicam apenas na CCEE\n{}'.format(arquivoMudancasOnsCcee))

	dataRodada = data
	data = data + datetime.timedelta(days=1)
	pathArqEntrada = os.path.join(pathArquivos, data.strftime('%Y%m%d'), 'entrada')

	pathArqSaida = os.path.join(pathArquivos, dataRodada.strftime('%Y%m%d'), 'definitiva')
	if not os.path.exists(pathArqSaida):
		os.makedirs(pathArqSaida)

	pathOperuhOut = os.path.join(pathArqSaida,'operuh.dat')
	fileOut = open(pathOperuhOut, 'w')
	fileOut.close()

	pathOperuhIn = os.path.join(pathArqEntrada, 'ons_entrada_saida', 'operuh.dat')
	restricoes = leituraArquivo(pathOperuhIn)

	infoBlocos = getInfoBlocos()

	todasRestricoes = {}
	blocoREST = extrairInfoBloco(restricoes, 'REST', infoBlocos['REST']['regex'])
	todasRestricoes['REST'] = pd.DataFrame(blocoREST, columns=infoBlocos['REST']['campos'])
	todasRestricoes['REST']['id'] = todasRestricoes['REST']['id'].apply(pd.to_numeric, errors='coerce')
	
	blocoELEM = extrairInfoBloco(restricoes, 'ELEM', infoBlocos['ELEM']['regex'])
	todasRestricoes['ELEM'] = pd.DataFrame(blocoELEM, columns=infoBlocos['ELEM']['campos'])
	todasRestricoes['ELEM']['id'] = todasRestricoes['ELEM']['id'].apply(pd.to_numeric, errors='coerce')
	
	blocoLIM = extrairInfoBloco(restricoes, 'LIM', infoBlocos['LIM']['regex'])
	todasRestricoes['LIM'] = pd.DataFrame(blocoLIM, columns=infoBlocos['LIM']['campos'])
	todasRestricoes['LIM']['id'] = todasRestricoes['LIM']['id'].apply(pd.to_numeric, errors='coerce')

	blocoVAR = extrairInfoBloco(restricoes, 'VAR', infoBlocos['VAR']['regex'])
	todasRestricoes['VAR'] = pd.DataFrame(blocoVAR, columns=infoBlocos['VAR']['campos'])
	todasRestricoes['VAR']['id'] = todasRestricoes['VAR']['id'].apply(pd.to_numeric, errors='coerce')

	blocoCOND = extrairInfoBloco(restricoes, 'COND', infoBlocos['COND']['regex'])
	todasRestricoes['COND'] = pd.DataFrame(blocoCOND, columns=infoBlocos['COND']['campos'])
	todasRestricoes['COND']['id'] = todasRestricoes['COND']['id'].apply(pd.to_numeric, errors='coerce')

	# Restricoes exclusivas da ccee
	for index, rest in mudancasCcee_restricoes.iterrows():
		if rest.iloc[1] < data:
			continue

		mnemonico = rest.iloc[2].strip()
		idRestricao = rest.iloc[3]
		if rest.iloc[0] == 'Comentar':
			todasRestricoes[mnemonico] = todasRestricoes[mnemonico].drop(todasRestricoes[mnemonico].loc[todasRestricoes[mnemonico]['id'] == idRestricao].index)
		elif rest.iloc[0] == 'Adicionar':
			if mnemonico == 'REST':
				newRow = dict(zip(todasRestricoes[mnemonico].columns,['OPERUH', 'REST  ']+rest.fillna('')[3:8].tolist()))
			elif mnemonico == 'ELEM':
				newRow = dict(zip(todasRestricoes[mnemonico].columns,['OPERUH', 'ELEM  ']+rest.fillna('')[3:9].tolist()))
			elif mnemonico == 'LIM':
				newRow = dict(zip(todasRestricoes[mnemonico].columns,['OPERUH', 'LIM   ']+rest.fillna('')[3:12].tolist()))
			elif mnemonico == 'VAR':
				newRow = dict(zip(todasRestricoes[mnemonico].columns,['OPERUH', 'VAR   ']+rest.fillna('')[3:14].tolist()))
			elif mnemonico == 'COND':
				newRow = dict(zip(todasRestricoes[mnemonico].columns,['OPERUH', 'COND ']+rest.fillna('')[3:14].tolist()))
			todasRestricoes[mnemonico] = todasRestricoes[mnemonico].append(newRow, ignore_index=True)
		else:
			print("Sem tratamento para a acao {} no index {}".format(rest.iloc[0], index+1))

	# Formatacao da coluna de ID para conter 5 digitos
	for mnemonico in ['REST','ELEM','LIM','VAR','COND']:
		todasRestricoes[mnemonico]['id'] = todasRestricoes[mnemonico]['id'].apply(lambda x: "{:0>5}".format(x))

	listaRestricoes = []
	for index, row in todasRestricoes['REST'].iterrows():
		for tipo in todasRestricoes:
			df_rest = todasRestricoes[tipo][todasRestricoes[tipo]['id'] == row['id']]
			listaRestricoes += formatarBloco(df_rest, infoBlocos[tipo]['formatacao'])
	
	gravarArquivo(pathOperuhOut, listaRestricoes)

	print('operuh.dat: {}'.format(pathOperuhOut))
	return pathOperuhOut




def gerarOperuh_baixado(data, pathArquivos):

	pathArqEntrada = os.path.join(pathArquivos, data.strftime('%Y%m%d'), 'entrada')

	pathArqSaida = os.path.join(pathArquivos, data.strftime('%Y%m%d'), 'saida')
	if not os.path.exists(pathArqSaida):
		os.makedirs(pathArqSaida)


	onsOperuhPath = downloadRestricoes(data, pathArquivos)
	operuhBaixado = leituraArquivo(onsOperuhPath)
	
	operuhDiaAnterior = leituraArquivo(os.path.join(pathArqEntrada, 'ccee_entrada', 'operuh.dat'))
	# operuhDiaAnterior = leituraArquivo(os.path.join(pathArqEntrada, 'ons_entrada_saida', 'operuh.dat'))


	pathOperuhOut = os.path.join(pathArqSaida,'operuh.dat')
	fileOut = open(pathOperuhOut, 'w')
	fileOut.close()

	infoBlocos = getInfoBlocos()

	# Operuh atual baixado da pagina do ONS
	blocoREST = extrairInfoBloco(operuhBaixado, 'REST', infoBlocos['REST']['regex'])
	df_restBaixado = pd.DataFrame(blocoREST, columns=infoBlocos['REST']['campos'])

	restricoes = {}
	blocoELEM = extrairInfoBloco(operuhBaixado, 'ELEM', infoBlocos['ELEM']['regex'])
	restricoes['ELEM'] = pd.DataFrame(blocoELEM, columns=infoBlocos['ELEM']['campos'])

	blocoLIM = extrairInfoBloco(operuhBaixado, 'LIM', infoBlocos['LIM']['regex'])
	restricoes['LIM'] = pd.DataFrame(blocoLIM, columns=infoBlocos['LIM']['campos'])

	blocoVAR = extrairInfoBloco(operuhBaixado, 'VAR', infoBlocos['VAR']['regex'])
	restricoes['VAR'] = pd.DataFrame(blocoVAR, columns=infoBlocos['VAR']['campos'])

	blocoCOND = extrairInfoBloco(operuhBaixado, 'COND', infoBlocos['COND']['regex'])
	restricoes['COND'] = pd.DataFrame(blocoCOND, columns=infoBlocos['COND']['campos'])

	# Operuh do dia anterior
	blocoREST = extrairInfoBloco(operuhDiaAnterior, 'REST', infoBlocos['REST']['regex'])
	df_restDiaAnterior = pd.DataFrame(blocoREST, columns=infoBlocos['REST']['campos'])

	restricoesDiaAnterior = {}
	blocoELEM = extrairInfoBloco(operuhDiaAnterior, 'ELEM', infoBlocos['ELEM']['regex'])
	restricoesDiaAnterior['ELEM'] = pd.DataFrame(blocoELEM, columns=infoBlocos['ELEM']['campos'])

	blocoLIM = extrairInfoBloco(operuhDiaAnterior, 'LIM', infoBlocos['LIM']['regex'])
	restricoesDiaAnterior['LIM'] = pd.DataFrame(blocoLIM, columns=infoBlocos['LIM']['campos'])

	blocoVAR = extrairInfoBloco(operuhDiaAnterior, 'VAR', infoBlocos['VAR']['regex'])
	restricoesDiaAnterior['VAR'] = pd.DataFrame(blocoVAR, columns=infoBlocos['VAR']['campos'])

	blocoCOND = extrairInfoBloco(operuhDiaAnterior, 'COND', infoBlocos['COND']['regex'])
	restricoesDiaAnterior['COND'] = pd.DataFrame(blocoCOND, columns=infoBlocos['COND']['campos'])

	df_restDiaAnterior = df_restDiaAnterior[df_restDiaAnterior['id'].astype(int) > 90000]

	df_restBaixado = atualizarValoresIniciais(df_restBaixado, restricoesDiaAnterior['ELEM'], os.path.join(pathArqEntrada, 'ccee_saida', 'pdo_operacao.dat'))
	df_restDiaAnterior = atualizarValoresIniciais(df_restDiaAnterior, restricoesDiaAnterior['ELEM'], os.path.join(pathArqEntrada, 'ccee_saida', 'pdo_operacao.dat'))


	listaRestricoes = []
	for index, row in df_restBaixado.iterrows():
		listaRestricoes += formatarBloco(row, infoBlocos['REST']['formatacao'])
		for tipo in restricoes:
			df_rest = restricoes[tipo][restricoes[tipo]['id'] == row['id']]
			listaRestricoes += formatarBloco(df_rest, infoBlocos[tipo]['formatacao'])

	for index, row in df_restDiaAnterior.iterrows():
		listaRestricoes += formatarBloco(row, infoBlocos['REST']['formatacao'])
		for tipo in restricoesDiaAnterior:
			df_rest = restricoesDiaAnterior[tipo][restricoesDiaAnterior[tipo]['id'] == row['id']]
			listaRestricoes += formatarBloco(df_rest, infoBlocos[tipo]['formatacao'])
	
	gravarArquivo(pathOperuhOut, listaRestricoes)

	print('operuh.dat: {}'.format(pathOperuhOut))
	return pathOperuhOut

if __name__ ==  "__main__":


	diretorioRaiz = os.path.abspath('../../../')
	pathLibUniversal = os.path.join(diretorioRaiz,'bibliotecas')
	sys.path.insert(1, pathLibUniversal)

	# import wx_opweek
	# import wx_dbLib
	import wx_selenium
	import wx_dessemBase
	import wx_opweek

	data = datetime.datetime.now()
	# data = datetime.datetime(2021,1,29)
	path = os.path.abspath(r'C:\Users\thiag\Documents\git\wx_alpha_\apps\dessem\arquivos')
	gerarOperuh(data, path)
