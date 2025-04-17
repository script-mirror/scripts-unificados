import os
import re
import sys
import pdb
import datetime

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.bibliotecas import rz_dir_tools


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


def autolabelMediaPld(ax, linhas, pontoInicio=0, size=6, cor='black'):
	"""Attach a text label above each bar in *rects*, displaying its height."""
	eixo_x = linhas.get_data()[0]
	eixo_y = linhas.get_data()[1]

	y = eixo_y[0] # altura sera a vazao
	ax.annotate('R${:.2f}'.format(y),
				xy=(pontoInicio, y),
				xytext=(0, 2),  # 3 points vertical offset
				textcoords="offset points",
				ha='center', va='bottom', size=size, color=cor)

def gerarCurvasPrecos(pdoCmoPaths, data =''):

	pathSaida = os.path.abspath('')
	
	if data == '':
		data = datetime.datetime.combine(datetime.date.today(), datetime.datetime.min.time())

	pld_diario_max = 583.88
	pld_horario_min = 49.77
	pld_horario_max = 1197.87

	resumoRodadas = {}
	custoMarginalDia = {}
	for cmo in pdoCmoPaths:
		preco_df = leituraCmo(pdoCmoPaths[cmo], data)
		preco_df.drop('FC', axis=1, inplace=True)
		preco_df.index = preco_df.index.strftime('%H:%M')

		preco_df[preco_df<pld_horario_min] = pld_horario_min
		preco_df[preco_df>pld_horario_max] = pld_horario_max

		resumo = preco_df.agg(['min', 'max', 'mean']).T

		for sub in resumo.index:
			if resumo.loc[sub, 'mean'] > pld_diario_max:
				fator = resumo.loc[sub, 'mean']/pld_diario_max
				preco_df[sub] = preco_df[sub]/fator

		resumoRodadas[cmo] = preco_df.agg(['min', 'max', 'mean']).T
		custoMarginalDia[cmo] = preco_df

		print('\nResumo da rodada {}:'.format(cmo))
		print(resumoRodadas[cmo])


	fig, ax =  plt.subplots(2,2)
	plt.gcf().set_size_inches(20, 10)

	plt.rcParams['axes.grid'] = True
	fig.suptitle('PLD horário', fontsize=16)
	
	cores = ['blue', 'coral', 'cadetblue', 'darkslategray']
	submercados = ['Sudeste', 'Sul', 'Nordeste', 'Norte']
	submercados = ['SE', 'S', 'NE', 'N']

	for i, sub in enumerate(submercados):
		ax[int(i/2), i%2].set_title(sub)
		for ii, cmo in enumerate(custoMarginalDia):
			x = custoMarginalDia[cmo].index
			y = custoMarginalDia[cmo][sub]

			ax[int(i/2), i%2].plot(x, y, label=cmo, color=cores[ii])

			x_media = [y.mean()]*len(x)
			media = ax[int(i/2), i%2].plot(x, x_media, color=cores[ii], linestyle='--', linewidth=.7)
			autolabelMediaPld(ax[int(i/2), i%2], media[0], 0, size=8, cor=cores[ii])
			

		ax[int(i/2), i%2].legend()
		ax[int(i/2), i%2].tick_params(axis='x', labelrotation=-40)
		ax[int(i/2), i%2].set_ylabel('Preço [R$]')
		ax[int(i/2), i%2].grid(color = 'gray', linestyle = '--', linewidth = 0.2)

	pathFileOut = os.path.join(pathSaida, 'PLD_HORARIO.PNG')
	plt.savefig(pathFileOut)
	print('Grafico salvo em:\n{}'.format(pathFileOut))


if '__main__' == __name__:

	# diretorioRaiz = os.path.abspath('../../../')
	# pathLibUniversal = os.path.join(diretorioRaiz,'bibliotecas')
	# sys.path.insert(1, pathLibUniversal)

	pdoCmos = {}
	pdoCmos['20210627'] = r'C:\Users\thiag\Documents\git\wx_alpha_\apps\dessem\arquivos\20210627\entrada\ons_entrada_saida\pdo_cmosist.dat'
	pdoCmos['20210628'] = r'C:\Users\thiag\Documents\git\wx_alpha_\apps\dessem\arquivos\20210628\entrada\ons_entrada_saida\pdo_cmosist.dat'
	gerarCurvasPrecos(pdoCmos)
	# leituraCmo(data, path, comentatio = '', fonte='wx')