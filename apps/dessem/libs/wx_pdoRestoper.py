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

		if 'trata' in line.lower():
			iLine += 2
			line = arquivo[iLine]
			bloco = []
			while iLine != len(arquivo)-1:
				bloco.append(line)
				iLine += 1
				line = arquivo[iLine]
			dados['RESTOPER'] = bloco
			continue
		else:
			iLine += 1

	return dados

def getInfoBlocos():
	blocos = {}

	blocos['restoper'] = {'campos':[
						'trata',
						'stat',
						'iper',
						'pat',
						'iusi',
						'nome',
						'sist',
						'tipo',
						'cod',
						'num',
						'fator',
      					'aplic',
      					'valor',
						'linf',
						'valor_rest',
						'lsup',
						'multipl',
						'obs',
						'justificativa',
				],
				'regex': '(.{7});(.{9});(.{5});(.{7});(.{7});(.{14});(.{6});(.{6});(.{6});(.{7});(.{9});(.{6});(.{14});(.{12});(.{12});(.{12});(.{16});(.{52});(.{13});(.*)',
			    'formatacao': '{:>7};{:>9};{:>5};{:>7};{:>7};{:>14};{:>6};{:>6};{:>6};{:>7};{:>9};{:>6};{:>14};{:>12};{:>12};{:>12};{:>16};{:>52};{:>13};'}
	return blocos


def extrairInfoBloco(listaLinhas, mnemonico, regex):

	blocos = []
	if mnemonico in listaLinhas:
		for i, linha in enumerate(listaLinhas[mnemonico]):
			infosLinha = re.split(regex, linha)
			blocos.append(infosLinha[1:-2])   # ultimo termo da lista e o que sobra da expressao regex (/n)

	return blocos

def leituraRestOper(pdoRestoperPath):
	infoBlocos = getInfoBlocos()
	pdoRestoper = leituraArquivo(pdoRestoperPath)
	rest = extrairInfoBloco(pdoRestoper, 'RESTOPER', infoBlocos['restoper']['regex'])
	df_rest = pd.DataFrame(rest, columns=infoBlocos['restoper']['campos'])

	return df_rest



def somaRestricaoPorPeriodo(restricoes):

	# Ate aqui todas as colunas estao como string.
	# Para converter uma coluna para int utilize o comando abaixo, alterando o nome da coluna
	restricoes['iper'] = restricoes['iper'].apply(pd.to_numeric, errors='ignore')
	restricoes['num'] = restricoes['num'].apply(pd.to_numeric, errors='ignore')
	restricoes['fator'] = restricoes['fator'].apply(pd.to_numeric, errors='ignore')
	restricoes['valor'] = restricoes['valor'].apply(pd.to_numeric, errors='ignore')
	restricoes['linf'] = restricoes['linf'].apply(pd.to_numeric, errors='ignore')
	restricoes['lsup'] = restricoes['lsup'].apply(pd.to_numeric, errors='ignore')

	resumoRestricoes = pd.DataFrame(columns=['linf','lsup'])
	for grupo, rests in restricoes.groupby(['iper', 'num']).__iter__():
		resumoRestricoes.loc[grupo[1], grupo[0]] = (rests['fator']*rests['valor']).sum()
		if pd.isnull(resumoRestricoes.loc[1, 'linf']):
			resumoRestricoes.loc[1, 'linf'] = rests.iloc[0]['linf']
		if pd.isnull(resumoRestricoes.loc[1, 'lsup']):
			resumoRestricoes.loc[1, 'lsup'] = rests.iloc[0]['lsup']

	# resumoRestricoes.to_csv(path_or_buf='resumoRestricoes.csv', sep='\t')
	return resumoRestricoes

if '__main__' == __name__:

	# diretorioRaiz = os.path.abspath('../../../')
	# pathLibUniversal = os.path.join(diretorioRaiz,'bibliotecas')
	# sys.path.insert(1, pathLibUniversal)

	# import wx_dbLib

	path = os.path.abspath(r'C:\Users\thiag\Documents\git\wx_alpha_\apps\dessem\arquivos\20210202\entrada\ccee_saida\PDO_RESTOPER.DAT')
	restricoes = leituraRestOper(path)

	resumoRestricoes = somaRestricaoPorPeriodo(restricoes)
	pdb.set_trace()







