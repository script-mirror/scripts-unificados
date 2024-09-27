import os
import re
import sys
import pdb
import shutil
import codecs
import datetime

import pandas as pd


sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.apps.dessem.libs import wx_ptoper

def getInfoBlocos():
	blocos = {}

	blocos['condicoesIniciais'] = {'campos':[
						'us',
						'nome',
						'ug',
						'st',
						'GerInic',
						'tempo',
						'MH',
						'A_D',
				],
				'regex':'(.{3})  (.{12}) (.{3})   (.{2})   (.{10})  (.{5})  (.{1})  (.{1})(.*)',
				'formatacao':'{:>3}  {:>12} {:>3}   {:>2}   {:>10}  {:>5}  {:>1}  {:>1}'}


	blocos['limitesCondicoesOper'] = {'campos':[
						'us',
						'nome',
						'un',
						'di',
						'hi',
						'm',
						'df',
						'hf',
						'm',
						'Gmin',
						'Gmax',
						'Custo',
				],
				'regex':'(.{3}) (.{12}) (.{2}) (.{2}) (.{2}) (.{1}) (.{2}) (.{2}) (.{1}) (.{10})(.{10})(.{10})(.*)',
				'formatacao':'{:>3} {:>12} {:>2} {:>2} {:>2} {:>1} {:>2} {:>2} {:>1} {:>10}{:>10}{:>10}'}
	
	return blocos

def getFromFile(path):
	file = open(path, 'r', encoding='utf-8')
	return file.readlines()

def leituraArquivoAux(filePath):

	arquivo = getFromFile(filePath)

	dados = {'cometariosIniciais':[], 'condicoesIniciais':[], 'limitesCondicoesOper':[]}
	iLine = 0
	while iLine != len(arquivo)-1:
		line = arquivo[iLine]

		if '& ARQUIVO COM RESTRICOES OPERACIONAIS PARA AS UNIDADES TERMICAS' in line:
			bloco = []
			while 'CONDICOES INICIAIS DAS UNIDADES' not in line:
				bloco.append(line)
				iLine += 1
				line = arquivo[iLine]
			dados['cometariosIniciais'] = bloco
			continue

		elif 'CONDICOES INICIAIS DAS UNIDADES' in line:
			bloco = []
			while 'FIM' != line[0:3]:
				bloco.append(line)
				iLine += 1
				line = arquivo[iLine]

			# Retirada das linhas sem interesse (headers e rodapes do bloco)
			dados['condicoesIniciais'] = bloco[5:]
			continue

		elif 'LIMITES E CONDICOES OPERACIONAIS DAS UNIDADES' in line:
			bloco = []
			while 'FIM' != line[0:3]:
				bloco.append(line)
				iLine += 1
				line = arquivo[iLine]
			# Retirada das linhas sem interesse (headers e rodapes do bloco)
			dados['limitesCondicoesOper'] = bloco[5:]
			continue

		else:
			iLine += 1

	return dados

def leituraArquivo(filePath):

	arquivo = getFromFile(filePath)

	dados = {'cometariosIniciais':[], 'condicoesIniciais':[], 'limitesCondicoesOper':[]}
	iLine = 0
	while iLine != len(arquivo)-1:
		line = arquivo[iLine]

		if iLine == 0:
		# if '& ARQUIVO COM RESTRICOES OPERACIONAIS PARA AS UNIDADES TERMICAS' in line:
			bloco = []
			while iLine == 0 or line[0:4] != 'INIT':
			# while 'CONDICOES INICIAIS DAS UNIDADES' not in line:
				bloco.append(line)
				iLine += 1
				line = arquivo[iLine]
			if 'CONDICOES INICIAIS DAS UNIDADE' in bloco[-2]:
				bloco = bloco[:-2]
			dados['cometariosIniciais'] = bloco
			continue

		elif line[0:4] == 'INIT':
			bloco = []
			while 'FIM' != line[0:3]:
				bloco.append(line)
				iLine += 1
				line = arquivo[iLine]

			# Retirada das linhas sem interesse (headers e rodapes do bloco)
			dados['condicoesIniciais'] = bloco[3:]
			continue

		elif 'LIMITES E CONDICOES OPERACIONAIS DAS UNIDADES' in line:
			bloco = []
			while 'FIM' != line[0:3]:
				bloco.append(line)
				iLine += 1
				line = arquivo[iLine]
			# Retirada das linhas sem interesse (headers e rodapes do bloco)
			dados['limitesCondicoesOper'] = bloco[5:]
			continue

		else:
			iLine += 1

	return dados

def extrairInfoBloco(listaLinhas, mnemonico, regex):
	blocos = []
	if mnemonico in listaLinhas:
		for i, linha in enumerate(listaLinhas[mnemonico]):
			if linha[0] == '&':
				continue
			infosLinha = re.split(regex, linha)
			blocos.append(infosLinha[1:-2])   # ultimo termo da lista e o que sobra da expressao regex (/n)

	return blocos

def gravarArquivo(pathSaida, valores):
	fileOut = codecs.open(pathSaida, 'a+', 'utf-8')

	for linha in valores:
		fileOut.write('{}\n'.format(linha.replace('\n','')))

	fileOut.close()

def getCabecalhos(mnemonico):

	cabecalho = []

	if mnemonico == 'condicoesIniciais':
		cabecalho.append('& CONDICOES INICIAIS DAS UNIDADES                ')
		cabecalho.append('&                                                ')
		cabecalho.append('INIT                                             ')
		cabecalho.append('&us     nome       ug   st   GerInic     tempo MH A/D')
		cabecalho.append('&XX XXXXXXXXXXXX  XXX   XX   XXXXXXXXXX  XXXXX  X  X ')

	if mnemonico == 'limitesCondicoesOper':
		cabecalho.append('&')
		cabecalho.append('&')
		cabecalho.append('& LIMITES E CONDICOES OPERACIONAIS DAS UNIDADES')
		cabecalho.append('&')
		cabecalho.append('OPER')
		cabecalho.append('&us    nome      un di hi m df hf m Gmin     Gmax       Custo')
		cabecalho.append('&XX XXXXXXXXXXXX XX XX XX X XX XX X XXXXXXXXXXxxxxxxxxxxXXXXXXXXXX')

	return cabecalho

def getRodapes(mnemonico):
	rodape = []
	if mnemonico == 'condicoesIniciais':
		rodape.append('FIM')

	if mnemonico == 'limitesCondicoesOper':
		rodape.append('FIM')
	return rodape

def formatarBloco(df_bloco, formatacao):

	bloco = []
	for index, row in df_bloco.iterrows():
		bloco.append(formatacao.format(*row.values))

	return bloco

def gerarOperut(data, pathArquivos):

	pathArqEntrada = os.path.join(pathArquivos, data.strftime('%Y%m%d'), 'entrada')
	pathArqSaida = os.path.join(pathArquivos, data.strftime('%Y%m%d'), 'saida')
	if not os.path.exists(pathArqSaida):
		os.makedirs(pathArqSaida)

	pathOperutSaida = os.path.join(pathArqSaida,'operut.dat')
	fileOut = open(pathOperutSaida, 'w')
	fileOut.close()

	infoBlocos = getInfoBlocos()

	pathOperutIn = os.path.join(pathArqEntrada, 'ccee_entrada', 'operut.dat')
	operut = leituraArquivo(pathOperutIn)

	pathOperutAuxIn = os.path.join(pathArqEntrada, 'ons_entrada_saida', 'operut.aux')
	operutAux = leituraArquivoAux(pathOperutAuxIn)
	
	gravarArquivo(pathOperutSaida, operut['cometariosIniciais'])

	ptoperPath = os.path.join(pathArqEntrada, 'ccee_entrada', 'ptoper.dat')
	ptoper = wx_ptoper.leituraPtoper(ptoperPath)
	ptoper['id'] = ptoper['id'].apply(pd.to_numeric, errors='ignore')
	ptoper['di'] = ptoper['di'].apply(pd.to_numeric, errors='ignore')
	ptoper['hi'] = ptoper['hi'].apply(pd.to_numeric, errors='ignore')
	ptoper['df'] = ptoper['df'].apply(pd.to_numeric, errors='ignore')
	ptoper['hf'] = ptoper['hf'].apply(pd.to_numeric, errors='ignore')
	ptoper['valorvar'] = ptoper['valorvar'].apply(pd.to_numeric, errors='ignore')

	# Valores do dia seguinte vindo no deck 
	blocoCI = extrairInfoBloco(operutAux, 'condicoesIniciais', infoBlocos['condicoesIniciais']['regex'])
	df_blocoCI = pd.DataFrame(blocoCI, columns=infoBlocos['condicoesIniciais']['campos'])

	for index, row in ptoper.iterrows():
		if row['tp.var'].strip() == 'GERA':
			if row['valorvar'] == 0:
				df_blocoCI.loc[df_blocoCI['us'].apply(pd.to_numeric, errors='ignore') == row['id'], 'st'] = 0
			df_blocoCI.loc[df_blocoCI['us'].apply(pd.to_numeric, errors='ignore') == row['id'], 'GerInic'] = row['valorvar']

	bloco_ci = getCabecalhos('condicoesIniciais')
	bloco_ci += formatarBloco(df_blocoCI, infoBlocos['condicoesIniciais']['formatacao'])
	bloco_ci += getRodapes('condicoesIniciais')
	gravarArquivo(pathOperutSaida, bloco_ci)


	blocoLC = extrairInfoBloco(operut, 'limitesCondicoesOper', infoBlocos['limitesCondicoesOper']['regex'])
	df_blocoLC = pd.DataFrame(blocoLC, columns=infoBlocos['limitesCondicoesOper']['campos'])
	bloco_lc = getCabecalhos('limitesCondicoesOper')
	bloco_lc += formatarBloco(df_blocoLC, infoBlocos['limitesCondicoesOper']['formatacao'])
	bloco_lc += getRodapes('limitesCondicoesOper')
	gravarArquivo(pathOperutSaida, bloco_lc)

	print('dadvaz.dat: {}'.format(pathOperutSaida))
	return pathOperutSaida



def gerarOperutDef(data, pathArquivos):

	dataRodada = data
	data = data + datetime.timedelta(days=1)
	pathArqEntrada = os.path.join(pathArquivos, data.strftime('%Y%m%d'), 'entrada')
	pathArqEntradaAnt = os.path.join(pathArquivos, dataRodada.strftime('%Y%m%d'), 'entrada')

	pathArqSaida = os.path.join(pathArquivos, dataRodada.strftime('%Y%m%d'), 'definitiva')
	if not os.path.exists(pathArqSaida):
		os.makedirs(pathArqSaida)

	pathOperutOut = os.path.join(pathArqSaida,'operut.dat')
	fileOut = open(pathOperutOut, 'w')
	fileOut.close()

	pathOperutIn = os.path.join(pathArqEntrada, 'ons_entrada_saida', 'operut.dat')
	operut = leituraArquivo(pathOperutIn)

	infoBlocos = getInfoBlocos()


	gravarArquivo(pathOperutOut, operut['cometariosIniciais'])


	ptoperPath = os.path.join(pathArqEntradaAnt, 'ccee_entrada', 'ptoper.dat')
	ptoper = wx_ptoper.leituraPtoper(ptoperPath)
	ptoper['id'] = ptoper['id'].apply(pd.to_numeric, errors='ignore')
	ptoper['di'] = ptoper['di'].apply(pd.to_numeric, errors='ignore')
	ptoper['hi'] = ptoper['hi'].apply(pd.to_numeric, errors='ignore')
	ptoper['df'] = ptoper['df'].apply(pd.to_numeric, errors='ignore')
	ptoper['hf'] = ptoper['hf'].apply(pd.to_numeric, errors='ignore')
	ptoper['valorvar'] = ptoper['valorvar'].apply(pd.to_numeric, errors='ignore')


	blocoCI = extrairInfoBloco(operut, 'condicoesIniciais', infoBlocos['condicoesIniciais']['regex'])
	df_blocoCI = pd.DataFrame(blocoCI, columns=infoBlocos['condicoesIniciais']['campos'])

	for index, row in ptoper.iterrows():
		if row['tp.var'].strip() == 'GERA':
			if row['valorvar'] == 0:
				df_blocoCI.loc[df_blocoCI['us'].apply(pd.to_numeric, errors='ignore') == row['id'], 'st'] = 0
				df_blocoCI.loc[df_blocoCI['us'].apply(pd.to_numeric, errors='ignore') == row['id'], 'GerInic'] = row['valorvar']
			
	bloco_ci = getCabecalhos('condicoesIniciais')
	bloco_ci += formatarBloco(df_blocoCI, infoBlocos['condicoesIniciais']['formatacao'])
	bloco_ci += getRodapes('condicoesIniciais')
	gravarArquivo(pathOperutOut, bloco_ci)

	blocoLC = extrairInfoBloco(operut, 'limitesCondicoesOper', infoBlocos['limitesCondicoesOper']['regex'])
	df_blocoLC = pd.DataFrame(blocoLC, columns=infoBlocos['limitesCondicoesOper']['campos'])
	bloco_lc = getCabecalhos('limitesCondicoesOper')
	bloco_lc += formatarBloco(df_blocoLC, infoBlocos['limitesCondicoesOper']['formatacao'])
	bloco_lc += getRodapes('limitesCondicoesOper')
	gravarArquivo(pathOperutOut, bloco_lc)

	print('dadvaz.dat: {}'.format(pathOperutOut))
	return pathOperutOut

if __name__ == '__main__':

	diretorioRaiz = os.path.abspath('../../../')
	pathLibUniversal = os.path.join(diretorioRaiz,'bibliotecas')
	sys.path.insert(1, pathLibUniversal)

	import wx_ptoper

	data = datetime.datetime.now()
	# pathArquivo = os.path.abspath(r'C:\Users\thiag\Documents\git\wx_alpha_\apps\dessem\arquivos\baixados_manualmente\DS_ONS_012021_RV4D27\operut.dat')
	pathArquivo = os.path.abspath(r'C:\Users\thiag\Documents\git\wx_alpha_\apps\dessem\arquivos')
	gerarOperut(data, pathArquivo)
