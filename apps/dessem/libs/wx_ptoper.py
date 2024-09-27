# -*- coding: utf-8 -*-
import re
import os
import sys
import pdb
import datetime
import pandas as pd


def getInfoBlocos():
	blocos = {}

	blocos['PTOPER'] = {'campos':[
						'mnemonico',
						'tpelem',
						'id',
						'tp.var',
						'di',
						'hi',
						'm',
						'df',
						'hf',
						'm',
						'valorvar',
				],
				'regex':'(.{6}) (.{6}) (.{3}) (.{6}) (.{2}) (.{2}) (.{1}) (.{2}) (.{2}) (.{1}) (.{10})(.*)',
				'formatacao':'{:>6} {:>6} {:>3} {:>6} {:>2} {:>2} {:>1} {:>2} {:>2} {:>1} {:>10}'}

	return blocos

def getFromFile(path):
	file = open(path, 'r')
	return file.readlines()

def leituraArquivo(filePath):

	arquivo = getFromFile(filePath)

	dados = {}
	for iLine in range(len(arquivo)):
		line = arquivo[iLine]

		if line[0] == '&':
			continue
		elif line[0] == '\n':
			continue

		mnemonico = line.split()[0]	
		if mnemonico not in dados:
			dados[mnemonico] = []
		dados[mnemonico].append(line)

	return dados

def extrairInfoBloco(listaLinhas, mnemonico, regex):
	blocos = []
	if mnemonico in listaLinhas:
		for i, linha in enumerate(listaLinhas[mnemonico]):
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

	if mnemonico == 'PTOPER':
		cabecalho.append('&   PONTO DE OPERCAO')
		cabecalho.append('&TOPER TPELEM ID  TP.VAR DI HI M DF HF M  VALORVAR ')
		cabecalho.append('&TOPER xxxxxx xxx xxxxxx xx xx x xx xx x xxxxxxxxxx')

	return cabecalho

def formatarBloco(df_bloco, formatacao):

	bloco = []
	for index, row in df_bloco.iterrows():
		bloco.append(formatacao.format(*row.values))

	return bloco


def leituraPtoper(ptoperPath):

	infoBlocos = getInfoBlocos()

	ptoper = leituraArquivo(ptoperPath)

	operacao = extrairInfoBloco(ptoper, 'PTOPER', infoBlocos['PTOPER']['regex'])
	df_operacao = pd.DataFrame(operacao, columns=infoBlocos['PTOPER']['campos'])

	return df_operacao


if __name__ == '__main__':

	diretorioRaiz = os.path.abspath('../../../')
	pathLibUniversal = os.path.join(diretorioRaiz,'bibliotecas')
	sys.path.insert(1, pathLibUniversal)

	# import wx_opweek
	# import wx_dbLib


	data = datetime.datetime.now()
	data = datetime.datetime(2021,2,23)
	path = os.path.abspath('/home/thiago/workspace/wx/alpha/apps/dessem/arquivos')
	# gerarEntdados(data, path)

	leituraPtoper(data, path)