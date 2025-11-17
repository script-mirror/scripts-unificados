import os
import re
import sys
import pdb
import datetime

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


sys.path.insert(1,"/WX2TB/Documentos/fontes/PMO/scripts_unificados/")
from bibliotecas import rz_dir_tools


DIR_TOOLS = rz_dir_tools.DirTools()

def leituraArquivo(filePath):

	filePath = DIR_TOOLS.get_name_insentive_name(filePath)
	file = open(filePath, 'r')
	arquivo = file.readlines()
	file.close()
	dados = {'CMO':[]}
	iLine = 0
	while iLine != len(arquivo)-1:
		line = arquivo[iLine]

		if re.search(r"[-]+;[-]+;[-]+;[-]+;[-]+;", line):
			iLine += 3
			line = arquivo[iLine]
			bloco = []
			while not re.search(r"[-]+;[-]+;[-]+;[-]+;[-]+;", line):
				bloco.append(line)
				iLine += 1
				line = arquivo[iLine]
			dados['CMO'] = bloco
			continue
		else:
			iLine += 1

	
	return dados

def getInfoBlocos():
	blocos = {}

	blocos['CMO'] = {'campos':[
						'iper',
						'pat',
						'sist',
						'cmarg',
						'pi_demanda',
				],
				'regex':'\\s*([^;]*);\\s*([^;]*);\\s*([^;]*);\\s*([^;]*);\\s*([^;]*);(.*)',
				'formatacao':'{:>6};{:>7};{:>6};{:>11};{:>11};'}
	return blocos


def extrairInfoBloco(listaLinhas, mnemonico, regex):

	blocos = []
	if mnemonico in listaLinhas:
		for i, linha in enumerate(listaLinhas[mnemonico]):
			infosLinha = re.split(regex, linha)
			blocos.append(infosLinha[1:-2])   # ultimo termo da lista e o que sobra da expressao regex (/n)

	return blocos

def leituraCmo(pdoCmoPath, data):

	infoBlocos = getInfoBlocos()

	pdoCmo = leituraArquivo(pdoCmoPath)

	precos = extrairInfoBloco(pdoCmo, 'CMO', infoBlocos['CMO']['regex'])
	df_precos = pd.DataFrame(precos, columns=infoBlocos['CMO']['campos'])

	df_precos['cmarg'] = df_precos['cmarg'].astype(float)
	df_precos['pi_demanda'] = df_precos['pi_demanda'].astype(float)
	df_precos['iper'] = df_precos['iper'].astype(int)

	df_precos['pat'] = df_precos['pat'].str.strip()
	df_precos['sist'] = df_precos['sist'].str.strip()

	custoMarginal = df_precos.pivot(index='iper', columns='sist', values='cmarg')
	
	custoMarginalDia = custoMarginal.loc[1:48]
	custoMarginalDia = custoMarginalDia.groupby(np.arange(len(custoMarginalDia))//2).mean()

	data_hora = []
	di = data
	for i in range(custoMarginalDia.shape[0]):
		data_hora.append(di + datetime.timedelta(hours=i))

	custoMarginalDia.index = data_hora

	return custoMarginalDia


if '__main__' == __name__:
	pass

