# -*- coding: utf-8 -*-
import os
import sys
import pdb
import time
import shutil
import codecs
import datetime

import numpy as np
import pandas as pd

sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.bibliotecas import wx_dbLib
from PMO.scripts_unificados.apps.dessem.libs import wx_entdados



def getInfoBlocos():
	blocos = {}

	blocos['DEFANT'] = {'campos':[
						'Mont',
						'Jus',
						'TpJ',
						'di',
						'hi',
						'm',
						'df',
						'hf',
						'm',
						'defluencia',
				],
				'regex':'(.{2})       (.{3})  (.{3})  (.{1})    (.{2}) (.{2}) (.{1}) (.{2}) (.{2}) (.{1})     (.{10})(.*)',
				'formatacao':'{:>2}       {:>3}  {:>3}  {:>1}    {:>2} {:>2} {:>1} {:>2} {:>2} {:>1}     {:>10}'}

	return blocos


def getCabecalhos(mnemonico):
	cabecalho = []

	if mnemonico == 'DEFANT':
		cabecalho.append('&   DEFLUENCIAS ANTERIORES AO INICIO DO ESTUDO')
		cabecalho.append('&')
		cabecalho.append('&')
		cabecalho.append('&        Mont Jus TpJ   di hi m df hf m     defluencia')
		cabecalho.append('&X       XXX  XXX  X    XX XX X XX XX X     XXXXXXXXXX')

	return cabecalho

def gravarArquivo(pathSaida, valores):
	fileOut = codecs.open(pathSaida, 'w+', 'utf-8')

	for linha in valores:
		fileOut.write('{}\n'.format(linha.strip()))
	fileOut.close()

def gerarDeflant(data, pathArquivos):
	pathArqEntrada = os.path.join(pathArquivos, data.strftime('%Y%m%d'), 'entrada')
	if not os.path.exists(pathArqEntrada):
		os.makedirs(pathArqEntrada)

	pathArqSaida = os.path.join(pathArquivos, data.strftime('%Y%m%d'), 'saida')
	if not os.path.exists(pathArqSaida):
		os.makedirs(pathArqSaida)

	pathEntdadosIn = os.path.join(pathArqEntrada, 'ccee_entrada', 'entdados.dat')
	entdados = wx_entdados.leituraArquivo(pathEntdadosIn)
	infoBlocosEntdados = wx_entdados.getInfoBlocos()
	blocoTVIAG = wx_entdados.extrairInfoBloco(entdados, 'TVIAG', infoBlocosEntdados['TVIAG']['regex'])
	df_blocoTVIAG = pd.DataFrame(blocoTVIAG, columns=infoBlocosEntdados['TVIAG']['campos'])

	df_blocoTVIAG['diaAproxViagem'] =  np.ceil(df_blocoTVIAG['tempoViagem'].astype(int)/24).astype(int)

	# data = data - datetime.timedelta(days=1)
	wxdb = wx_dbLib.WxDataB()
	sql = '''SELECT
					CD_UHE,
					CD_POSTO
				FROM
					TB_POSTO_UHE
				ORDER BY
					CD_POSTO'''
	answer = wxdb.requestServer(sql)
	codigoUsinas = dict(answer)

	dataInial = data - datetime.timedelta(days=30)
	sql = '''WITH groups AS (
					SELECT
						CD_POSTO,
						DT_REFERENTE,
						VL_VAZ_DEF_CONSO,
						ROW_NUMBER() OVER ( PARTITION BY CD_POSTO,
						DT_REFERENTE
					ORDER BY
						DT_ACOMPH DESC ) AS [ROW NUMBER]
					FROM
						climenergy.dbo.TB_ACOMPH
					WHERE
						DT_REFERENTE >= \'{}\')
					SELECT
						CD_POSTO,
						DT_REFERENTE,
						VL_VAZ_DEF_CONSO
					FROM
						groups
					WHERE
						groups.[ROW NUMBER] = 1
					ORDER BY
						CD_POSTO,
						DT_REFERENTE'''.format(dataInial.strftime('%Y/%m/%d'))


	numeroVerificacoesDb = 0
	numeroMaximoVerificacoesDb = 50

	acomph = pd.DataFrame(columns=['CD_POSTO' , 'DT_REFERENTE' , 'VL_VAZ_DEFL'])
	numeroPostosAcomph = 148
	while acomph.loc[acomph['DT_REFERENTE']==pd.Timestamp(data-datetime.timedelta(days=1))].shape[0] < numeroPostosAcomph:

		if numeroVerificacoesDb != 0:
			time.sleep(60*5)

		answer = wxdb.requestServer(sql)
		acomph = pd.DataFrame(answer, columns=['CD_POSTO' , 'DT_REFERENTE' , 'VL_VAZ_DEFL'])

		if numeroVerificacoesDb == numeroMaximoVerificacoesDb:
			print()
			quit()

		numeroVerificacoesDb += 1


	sql = '''SELECT
				CD_UHE ,
				DT_REFERENTE ,
				VL_VAZ_DEFL 
			FROM
				TB_NIVEIS_DESSEM
			WHERE
				DT_REFERENTE = \'{}\'
			ORDER BY
				DT_REFERENTE'''.format(data.strftime('%Y/%m/%d'))


	numeroVerificacoesDb = 0
	niveisDessem = pd.DataFrame(columns=['CD_UHE' , 'DT_REFERENTE' , 'VL_VAZ_DEFL'])

	while niveisDessem.loc[niveisDessem['DT_REFERENTE']==pd.Timestamp(data)].shape[0] == 0:
		
		if numeroVerificacoesDb != 0:
			print(f"Não encontrado os níveis de partida do dessem (TB_NIVEIS_DESSEM) para a data de {data.strftime('%Y/%m/%d')}")
			time.sleep(60*5)

		answer = wxdb.requestServer(sql)
		niveisDessem = pd.DataFrame(answer, columns=['CD_UHE' , 'DT_REFERENTE' , 'VL_VAZ_DEFL'])

		if numeroVerificacoesDb == numeroMaximoVerificacoesDb:
			print()
			quit()

		numeroVerificacoesDb += 1


	bloco_defant = getCabecalhos('DEFANT')

	df_blocoTVIAG['usinaMontante'] = df_blocoTVIAG['usinaMontante'].astype(int)

	for index, row in df_blocoTVIAG.iterrows():
		usina = row['usinaMontante']

		# Usinas serao tratadas por ultimo com valores horarios
		if usina in [66, 83]:
			continue

		for dia in range(row['diaAproxViagem'],0,-1):
			dt = data - datetime.timedelta(days=dia-1)

			# Casos especiais para as usinas 107 e 110
			if usina == 107:
				if dia == 1:
					continue
				defluencia = acomph[(acomph['CD_POSTO'] == 161) & (acomph['DT_REFERENTE'] == dt)]['VL_VAZ_DEFL'].values[0]
			else:
				# Todos os valores de defluencia do passado (d-1) sao obtidos do ACOMPH
				if dia > 1:
					posto = codigoUsinas[usina]
					defluencia = acomph[(acomph['CD_POSTO'] == posto) & (acomph['DT_REFERENTE'] == dt)]['VL_VAZ_DEFL'].values[0]
				# Deflencias referentes ao dia d-0 sao obtidos dos niveis de partida do dessem
				else:
					defluencia = niveisDessem[(niveisDessem['CD_UHE'] == usina) & (niveisDessem['DT_REFERENTE'] == dt)]['VL_VAZ_DEFL'].values[0]

			bloco_defant.append('DEFANT   {:>3}  {:>3}  H    {:>2} 00 0  F          {:10.0f}\n'.format(usina,row['usinaJusante'],dt.strftime('%d'),defluencia))

	df_usinasHorarias = df_blocoTVIAG[df_blocoTVIAG['usinaMontante'].isin([66,83])]
	for index, row in df_usinasHorarias.iterrows():
		bloco_defant.append('&')
		usina = row['usinaMontante']
		dt = data
		dt_loop = data
		while dt_loop < (data + datetime.timedelta(days=1)):
			defluencia = niveisDessem[(niveisDessem['CD_UHE'] == usina) & (niveisDessem['DT_REFERENTE'] == dt)]['VL_VAZ_DEFL'].values[0]
			if dt_loop.strftime('%M') == '00':
				flagMh = 0
			else:
				flagMh = 1
			bloco_defant.append('DEFANT   {:>3}  {:>3}  S    {:>2} {: >2} {}  F          {:10.0f}\n'.format(usina, row['usinaJusante'], dt.strftime('%d'), int(dt_loop.strftime('%H')), flagMh, defluencia))
			dt_loop = dt_loop + datetime.timedelta(minutes=30)

	pathDeflantOut = os.path.join(pathArqSaida, 'deflant.dat')
	gravarArquivo(pathDeflantOut, bloco_defant)

	print('deflant.dat: {}'.format(pathDeflantOut))

def gerarDeflantDef(data, pathArquivos):

	dataRodada = data
	data = data + datetime.timedelta(days=1)

	pathArqEntrada = os.path.join(pathArquivos, data.strftime('%Y%m%d'), 'entrada')

	pathArqSaida = os.path.join(pathArquivos, dataRodada.strftime('%Y%m%d'), 'definitiva')
	if not os.path.exists(pathArqSaida):
		os.makedirs(pathArqSaida)

	src = os.path.join(pathArqEntrada, 'ons_entrada_saida', 'deflant.dat')
	dst = os.path.join(pathArqSaida,'deflant.dat')

	shutil.copy(src, dst)
	print('deflant.dat: {}'.format(dst))

if __name__ == '__main__':

	diretorioRaiz = os.path.abspath('../../../')
	pathLibUniversal = os.path.join(diretorioRaiz,'bibliotecas')
	sys.path.insert(1, pathLibUniversal)

	import wx_entdados
	import wx_dbLib

	data = datetime.datetime.now()
	data = datetime.datetime(2021,1,18)
	pathArquivos = os.path.abspath(r'C:\Users\thiag\Documents\git\wx_alpha_\apps\dessem\arquivos')

	gerarDeflant(data, pathArquivos)
