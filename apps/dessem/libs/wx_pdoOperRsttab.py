import os
import re
import pdb

import pandas as pd

def getFromFile(path):

	file = open(path, 'r', encoding="latin-1")
	return file.readlines()

def leituraArquivo(filePath):

	arquivo = getFromFile(filePath)

	dados = {}
	iLine = 0
	while iLine != len(arquivo)-1:
		line = arquivo[iLine]
		if 'IPER ;'  in line:
			iLine += 3
			break
		else:
			iLine += 1
	bloco = arquivo[iLine:]
	dados['OPERRSTTAB'] = bloco

	return dados

def getInfoBlocos():
	blocos = {}

	blocos['operrsttab'] = {'campos':[
						'iper',
						'num',
						'nome',
						'resp',
						'numre',
						'somflux',
						'limitetab',
						'limiteuti',
						'tippar',
						'numpar',
						'valparam',
						'multip',
				],
				'regex':'(.{5});(.{7});(.{52});(.{7});(.{9});(.{12});(.{13});(.{13});(.{9});(.{7});(.{12});(.{20});(.*)',
				'formatacao':'{:>5};{:>7};{:>52};{:>7};{:>9};{:>12};{:>13};{:>13};{:>9};{:>7};{:>12};{:>20};'}
	return blocos


def extrairInfoBloco(listaLinhas, mnemonico, regex):

	blocos = []
	if mnemonico in listaLinhas:
		for i, linha in enumerate(listaLinhas[mnemonico]):
			infosLinha = linha.split(";")
			blocos.append(infosLinha[:-1])

	return blocos

def leituraOperRsttab(pdoOperRsttabPath):
	
	infoBlocos = getInfoBlocos()

	pdoOperRsttab = leituraArquivo(pdoOperRsttabPath)

	OperRsttab = extrairInfoBloco(pdoOperRsttab, 'OPERRSTTAB', infoBlocos['operrsttab']['regex'])
	df_OperRsttab = pd.DataFrame(OperRsttab, columns=infoBlocos['operrsttab']['campos'])
	return df_OperRsttab.dropna()


if '__main__' == __name__:

	# diretorioRaiz = os.path.abspath('../../../')
	# pathLibUniversal = os.path.join(diretorioRaiz,'bibliotecas')
	# sys.path.insert(1, pathLibUniversal)

	# import wx_dbLib

	path = os.path.abspath(r'C:\Users\thiag\Documents\git\wx_alpha_\apps\dessem\arquivos\20210202\entrada\ccee_saida\PDO_OPER_RSTTAB.DAT')
	operRsttab = leituraOperRsttab(path)
	pdb.set_trace()