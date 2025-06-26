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
			line = arquivo[iLine]
			bloco = []
			while iLine != len(arquivo)-1:
				bloco.append(line)
				iLine += 1
				line = arquivo[iLine]
			dados['OPERLPP'] = bloco
			continue
		else:
			iLine += 1

	return dados

def getInfoBlocos():
	blocos = {}

	blocos['operlpp'] = {'campos':[
						'iper',
						'num_lpp',
						'resp',
						'numRe',
						'vlrAtu',
						'limCalc',
						'indice',
						'coeflin',
						'nresparam',
						'tipo_rest_var',
						'vlrparam',
						'coefang',
						'multip',
				],
				'regex':'(.{5});(.{8});(.{7});(.{9});(.{12});(.{12});(.{8});(.{12});(.{12});(.{12});(.{12});(.{12});(.{20});(.*)',
				'formatacao':'{:>5};{:>8};{:>7};{:>9};{:>12};{:>12};{:>8};{:>12};{:>12};{:>12};{:>12};{:>12};{:>20};'}
	return blocos


def extrairInfoBloco(listaLinhas, mnemonico, regex):

	blocos = []
	if mnemonico in listaLinhas:
		for i, linha in enumerate(listaLinhas[mnemonico]):
			infosLinha = re.split(regex, linha)
			blocos.append(infosLinha[1:-2])   # ultimo termo da lista e o que sobra da expressao regex (/n)

	return blocos

def leituraOperLpp(pdoOperLppPath):
	infoBlocos = getInfoBlocos()

	pdoOperLpp = leituraArquivo(pdoOperLppPath)

	operLpp = extrairInfoBloco(pdoOperLpp, 'OPERLPP', infoBlocos['operlpp']['regex'])
	df_operLpp = pd.DataFrame(operLpp, columns=infoBlocos['operlpp']['campos'])

	return df_operLpp


if '__main__' == __name__:

	# diretorioRaiz = os.path.abspath('../../../')
	# pathLibUniversal = os.path.join(diretorioRaiz,'bibliotecas')
	# sys.path.insert(1, pathLibUniversal)

	# import wx_dbLib

	path = os.path.abspath(r'C:\Users\thiag\Documents\git\wx_alpha_\apps\dessem\arquivos\20210202\entrada\ccee_saida\PDO_OPER_LPP.DAT')
	operLpp = leituraOperLpp(path)
	pdb.set_trace()