# -*- coding: utf-8 -*-
import os
import re
import pdb
import sys
import shutil
import codecs
import datetime

import pandas as pd


def getInfoBlocos():
	blocos = {}

	blocos['COTR11'] = {'campos':[
						'dd',
						'hh',
						'm',
						'cotR11',
				],
				'regex':'(.{2}) (.{2}) (.{1})         (.{10})(.*)',
				'formatacao':'{:>2} {:>2} {:>1}         {:>10}'}
	return blocos

def getFromFile(path):
	file = open(path, 'r', encoding='utf-8')
	return file.readlines()

def leituraArquivo(filePath):

	arquivo = getFromFile(filePath)
	dados = {}
	mnemonico = 'COTR11'	

	for iLine in range(len(arquivo)):
		line = arquivo[iLine]

		if line[0] == '&'  or line[0] == '\n':
			continue
		else:
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

def getCabecalhos(mnemonico):
	cabecalho = []
	if mnemonico == 'COTR11':
		cabecalho.append('&d hh m             cotR11')
		cabecalho.append('&X XX X         XXXXXXXXXX')
	return cabecalho

def formatarBloco(df_bloco, formatacao):

	bloco = []
	for index, row in df_bloco.iterrows():
		bloco.append(formatacao.format(*row.values))
	return bloco

def gravarArquivo(pathSaida, valores):
	fileOut = codecs.open(pathSaida, 'a+', 'utf-8')
	for linha in valores:
		fileOut.write('{}\n'.format(linha))
	fileOut.close()

def gerarCotasr11(data, pathArquivos):

	pathArqEntrada = os.path.join(pathArquivos, data.strftime('%Y%m%d'), 'entrada')

	pathArqSaida = os.path.join(pathArquivos, data.strftime('%Y%m%d'), 'saida')
	if not os.path.exists(pathArqSaida):
		os.makedirs(pathArqSaida)

	pathCotR11In = os.path.join(pathArqEntrada, 'ccee_entrada', 'cotasr11.dat')
	cotR11In = leituraArquivo(pathCotR11In)

	pathCotR11Out = os.path.join(pathArqSaida,'cotasr11.dat')
	fileOut = open(pathCotR11Out, 'w')
	fileOut.close()

	infoBlocos = getInfoBlocos()

	dataAnterior = data - datetime.timedelta(days=1)
	blocoR11_in = extrairInfoBloco(cotR11In, 'COTR11', infoBlocos['COTR11']['regex'])
	df_blocoR11_in = pd.DataFrame(blocoR11_in, columns=infoBlocos['COTR11']['campos'])
	df_blocoR11_in['dd'] = df_blocoR11_in['dd'].astype(int)
	df_blocoR11_in['dd'] = df_blocoR11_in['dd'].replace({dataAnterior.day: data.day})
	bloco_r11 = getCabecalhos('COTR11')
	bloco_r11 += formatarBloco(df_blocoR11_in, infoBlocos['COTR11']['formatacao'])
	gravarArquivo(pathCotR11Out, bloco_r11)

	print('cotasr11.dat: {}'.format(pathCotR11Out))

def gerarCotasr11Def(data, pathArquivos):
	dataRodada = data
	data = data + datetime.timedelta(days=1)

	pathArqEntrada = os.path.join(pathArquivos, data.strftime('%Y%m%d'), 'entrada')

	pathArqSaida = os.path.join(pathArquivos, dataRodada.strftime('%Y%m%d'), 'definitiva')
	if not os.path.exists(pathArqSaida):
		os.makedirs(pathArqSaida)

	src = os.path.join(pathArqEntrada, 'ons_entrada_saida', 'cotasr11.dat')
	dst = os.path.join(pathArqSaida,'cotasr11.dat')

	shutil.copy(src, dst)
	print('cotasr11.dat: {}'.format(dst))

if __name__ == '__main__':

	diretorioRaiz = os.path.abspath('../../../')
	pathLibUniversal = os.path.join(diretorioRaiz,'bibliotecas')
	sys.path.insert(1, pathLibUniversal)

	import wx_opweek
	import wx_dbLib


	data = datetime.datetime.now()
	data = datetime.datetime(2021,2,4)
	path = os.path.abspath(r'C:\Users\thiag\Documents\git\wx_alpha_\apps\dessem\arquivos')
	# gerarEntdados(data, path)

	gerarCotasr11(data, path)