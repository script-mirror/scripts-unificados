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

		if '-----;-------;----------------------------------------------------;-------;---------;------------;-------------;-------------;-------;-------;------------;--------------------;'  in line:
			iLine += 4
			line = arquivo[iLine]
			bloco = []
			while iLine != len(arquivo)-1:
				bloco.append(line)
				iLine += 1
				line = arquivo[iLine]
			dados['OPERRSTTAB'] = bloco
			continue
		else:
			iLine += 1

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
				'regex':'(.{5});(.{7});(.{52});(.{7});(.{9});(.{12});(.{13});(.{13});(.{7});(.{7});(.{12});(.{20});(.*)',
				'formatacao':'{:>5};{:>7};{:>52};{:>7};{:>9};{:>12};{:>13};{:>13};{:>7};{:>7};{:>12};{:>20};'}
	return blocos


def extrairInfoBloco(listaLinhas, mnemonico, regex):

	blocos = []
	if mnemonico in listaLinhas:
		for i, linha in enumerate(listaLinhas[mnemonico]):
			infosLinha = re.split(regex, linha)
			blocos.append(infosLinha[1:-2])   # ultimo termo da lista e o que sobra da expressao regex (/n)

	return blocos

def leituraOperRsttab(pdoOperRsttabPath):

	infoBlocos = getInfoBlocos()

	pdoOperRsttab = leituraArquivo(pdoOperRsttabPath)

	OperRsttab = extrairInfoBloco(pdoOperRsttab, 'OPERRSTTAB', infoBlocos['operrsttab']['regex'])
	df_OperRsttab = pd.DataFrame(OperRsttab, columns=infoBlocos['operrsttab']['campos'])

	return df_OperRsttab


if '__main__' == __name__:

	# diretorioRaiz = os.path.abspath('../../../')
	# pathLibUniversal = os.path.join(diretorioRaiz,'bibliotecas')
	# sys.path.insert(1, pathLibUniversal)

	# import wx_dbLib

	path = os.path.abspath(r'C:\Users\thiag\Documents\git\wx_alpha_\apps\dessem\arquivos\20210202\entrada\ccee_saida\PDO_OPER_RSTTAB.DAT')
	operRsttab = leituraOperRsttab(path)
	pdb.set_trace()