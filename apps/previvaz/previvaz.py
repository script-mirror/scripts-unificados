# -*- coding: utf-8 -*-
import os
import re
import sys
import pdb
import shutil
import datetime

import numpy as np
import pandas as pd

diretorioApp = os.path.abspath('.')
appsDir = os.path.dirname(diretorioApp)
diretorioRaiz = os.path.dirname(appsDir)

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")), ".env"))
PATH_PROJETO = os.getenv("PATH_PROJETO", "/WX2TB/Documentos/fontes/PMO")
sys.path.insert(1,f"{PATH_PROJETO}/scripts_unificados")

from bibliotecas import wx_dbLib
from bibliotecas import wx_opweek
from bibliotecas import wx_configs
from bibliotecas import wx_postosAux
from bibliotecas import wx_manipulaArqPrevivaz
from bibliotecas import wx_manipulaArqSmap
from bibliotecas import wx_manipulaArqCpins
from apps.previvaz.libs import wx_manipulacaoDados 

def rodar_previvaz(data, modelo, fechamentos, ultima_rv_date, prox_rv_date):

	global diretorioApp

	numeroSemanasPrevisaoSmap = 2

	configs = wx_configs.getConfiguration()

	pathArquivosEntrada = os.path.join(diretorioApp, 'arquivos')

	dropbox_path = os.path.abspath(configs['paths']['dropbox_middle'])
	cv_dropbox_path = os.path.join(dropbox_path, 'Chuva-vazão')
	novoSmap_dropbox_path = os.path.join(dropbox_path, 'NovoSMAP')

	dataExc = datetime.datetime.strptime(data,"%d/%m/%Y")
	if 'PRELIMINAR' in modelo:
		dataRef = dataExc - datetime.timedelta(days=1)
	else:
		dataRef = dataExc

	ultima_rv = wx_opweek.ElecData(ultima_rv_date)
	prox_rv = wx_opweek.ElecData(prox_rv_date)

	revisao_atual = ultima_rv.atualRevisao
	if revisao_atual == 0:
		ultimaRv = 'PMO'
	else:
		ultimaRv = 'REV{}'.format(revisao_atual)

	nomePastaUltimaRev = "Arq_Entrada_e_Saida_PREVIVAZ_{}{:0>2}_{}".format(ultima_rv.anoReferente, ultima_rv.mesReferente, ultimaRv)

	pathUltimaRev = os.path.join(pathArquivosEntrada, 'entrada', nomePastaUltimaRev)
	pathExecPrevivaz = os.path.join(pathArquivosEntrada, 'executavel', 'previvaz.exe')
	cpinsFile = os.path.join(dropbox_path, 'NovoSMAP', 'PlanilhaUSB.xls')

	nomeArquivoSmap = 'smap_nat_{}.txt'.format(modelo)
	if 'PRELIMINAR' in modelo:
		pathArquivoSmap = os.path.join(cv_dropbox_path, dataExc.strftime('%Y%m%d'), 'preliminar', nomeArquivoSmap)
	else:
		pathArquivoSmap = os.path.join(cv_dropbox_path, dataExc.strftime('%Y%m%d'), 'teste', nomeArquivoSmap)


	semanasPrevs = []
	for i in range(6):
		semanasPrevs.append(pd.Timestamp(prox_rv.primeiroDiaMes+datetime.timedelta(days=i*7)))

	ultimoSabadoAcomph = wx_opweek.getLastSaturday(dataRef)
	semanasCompletasAcomph = []
	for sem in semanasPrevs:
		if sem <= wx_opweek.getLastSaturday(dataRef):
			semanasCompletasAcomph.append(sem)

	previsoesSmap = []
	for sem in range(numeroSemanasPrevisaoSmap): # (GT-SMAP)
		previsoesSmap.append(ultimoSabadoAcomph+datetime.timedelta(days=(sem+1)*7))

	previsaoPrevivaz = []
	for sem in semanasPrevs:
		if sem not in semanasCompletasAcomph and sem not in previsoesSmap:
			previsaoPrevivaz.append(sem)

	previsoes = previsoesSmap + previsaoPrevivaz


	print("\nDiretorio base: {}".format(pathUltimaRev))
	print("Arquivo SMAP:   {}\n".format(pathArquivoSmap))

	postosIncPrevivaz = [34, 245, 246, 266, 239, 242, 243, 154, 161, 191, 253, 257, 273, 271, 275]
	postosIncSmap = [34, 89, 245, 246, 266, 239, 242, 243, 154, 161, 191, 253, 257, 273, 271, 275]
	postosPrevivaz = wx_manipulaArqPrevivaz.getListaPostosPrevivaz(pathUltimaRev)

	db_clime = wx_dbLib.DadosClime()
	postosRegredidos = db_clime.getPostosRegredidos()

	dbAnswer = db_clime.getInfoPostos()
	infoPostos = pd.DataFrame(dbAnswer, columns=['CD_POSTO', 'STR_POSTO', 'VL_PRODUTIBILIDADE', 'CD_SUBMERCADO', 'STR_SUBMERCADO', 'STR_SIGLA', 'CD_BACIA', 'STR_BACIA', 'CD_REE', 'STR_REE'])
	infoPostos.set_index('CD_POSTO', inplace=True)
	
	infoPostos['INCREMENTAL_PREVIVAZ'] = False
	infoPostos.loc[postosIncPrevivaz, 'INCREMENTAL_PREVIVAZ'] = True

	vazao_nat_smap, vazao_inc_smap = wx_manipulaArqSmap.importarSaidaSmap(pathArquivoSmap, dataRef, postosIncSmap)
	vazao_nat_smap = wx_postosAux.calcPostosArtificiais_df(vazao_nat_smap, ignorar_erros=True)
	vazao_inc_smap = wx_postosAux.calcPostosArtificiais_df(vazao_inc_smap, ignorar_erros=True)
	postosSmap = list(vazao_nat_smap.index) + list(vazao_inc_smap.index)

	columns = ['CD_POSTO',	'DT_REFERENTE',	'VL_VAZ_INC_CONSO',	'VL_VAZ_NAT_CONSO']

	# Historico utilizado para complemetar o ultimo acomph e atualizar as ultimas 6 antes do inicio da previsao
	acomphHistorico = db_clime.getAcomph(ultima_rv_date - datetime.timedelta(days=7*5), dtFim=dataRef-datetime.timedelta(days=31))
	df_acomphHistorico = pd.DataFrame(acomphHistorico)
	df_acomphHistorico = df_acomphHistorico[[0,1,2,3]]
	df_acomphHistorico.columns = columns

	ultimoAcomph = db_clime.getAcomphEspecifico(dataRef-datetime.timedelta(days=1))
	df_ultimoAcomph = pd.DataFrame(ultimoAcomph)
	df_ultimoAcomph = df_ultimoAcomph[[1,0,8,9]]
	df_ultimoAcomph.columns = columns

	df_vazaoAcomph = df_acomphHistorico.append(df_ultimoAcomph)
	vazao_nat_acomph = df_vazaoAcomph.pivot(index='CD_POSTO', columns='DT_REFERENTE', values='VL_VAZ_NAT_CONSO')
	vazao_nat_acomph = wx_postosAux.calcPostosArtificiais_df(vazao_nat_acomph, ignorar_erros=False)
	vazao_nat = vazao_nat_acomph.reindex(columns=vazao_nat_acomph.columns.union(vazao_nat_smap.columns))
	vazao_nat.update(vazao_nat_smap)
	
	vazao_inc_acomph = df_vazaoAcomph.pivot(index='CD_POSTO', columns='DT_REFERENTE', values='VL_VAZ_INC_CONSO')
	vazao_inc_acomph = wx_postosAux.calcPostosArtificiais_df(vazao_inc_acomph, ignorar_erros=False)
	idx = vazao_inc_smap.index.append(pd.Index([238, 240])) # Postos que sao usados para o calculo de vazao de outros postos
	vazao_inc = vazao_inc_acomph.reindex(idx, columns=vazao_inc_acomph.columns.union(vazao_inc_smap.columns))
	vazao_inc.update(vazao_inc_smap)

	cpins_df = wx_manipulaArqCpins.getCpins(cpinsFile, vazao_nat.columns.min(), vazao_nat.columns.max())
	# Atualiza todo o df com os valores do CPINS
	for p in [168, 169]:
		for d in vazao_nat.columns:
			if d in cpins_df.index:
				vazao_nat.loc[p,d] = cpins_df.loc[d,p]

		# preenchendo o restante dos valores NAN com o ultimo valor válido
		vazao_nat.loc[p].fillna(method='ffill', inplace=True)
		vazao_nat.loc[p].fillna(method='bfill', inplace=True)

	vazao_nat = wx_manipulacaoDados.propagarPostosAcomph(vazao_nat, vazao_inc, dataRef)

	# [ 161, 191, 253, 257, 273, 271, 275]
	postosTieteTocantins = {}
	postosTieteTocantins[239] = {'natural':[238],'incremental':[239]}
	postosTieteTocantins[242] = {'natural':[240],'incremental':[242]}
	postosTieteTocantins[243] = {'natural':[242],'incremental':[243]}
	postosTieteTocantins[34] = {'natural':[18,33,99,241,261],'incremental':[34]}
	postosTieteTocantins[245] = {'natural':[243,34],'incremental':[245]}
	postosTieteTocantins[154] = {'natural':[],'incremental':[154]}
	postosTieteTocantins[246] = {'natural':[245,154],'incremental':[246]}
	postosTieteTocantins[266] = {'natural':[246,63],'incremental':[266]}

	postosTieteTocantins[161] = {'natural':[],'incremental':[161]}

	# postosTieteTocantins[273] = {'natural':[257],'incremental':[273]}
	# postosTieteTocantins[253] = {'natural':[191],'incremental':[253]}

	# prevs_df.loc[191,previsoes] = prevsCompilado_df.loc[270,previsoes] + prevsCompilado_df.loc[253,previsoes]*0.504
	# prevs_df.loc[253,previsoes] = prevs_df.loc[191,previsoes] + prevsCompilado_df.loc[253,previsoes]*0.496
	# prevs_df.loc[257,previsoes] = prevs_df.loc[253,previsoes] + prevsCompilado_df.loc[273,previsoes]*0.488
	# prevs_df.loc[273,previsoes] = prevs_df.loc[257,previsoes] + prevsCompilado_df.loc[273,previsoes]*0.512
	# prevs_df.loc[271,previsoes] = prevs_df.loc[273,previsoes] + prevsCompilado_df.loc[271,previsoes]
	# prevs_df.loc[275,previsoes] = prevs_df.loc[271,previsoes] + prevsCompilado_df.loc[275,previsoes]

	for posto in postosTieteTocantins:
		dts = vazao_nat.loc[posto].isna()
		vazao_nat.loc[posto, dts] = vazao_nat.loc[postosTieteTocantins[posto]['natural'], dts].sum() + vazao_inc.loc[postosTieteTocantins[posto]['incremental'], dts].sum()

	vazao_nat = wx_manipulacaoDados.fechamentoSemanas(vazao_nat, ultima_rv_date, prox_rv_date, fechamentos)
	vazao_inc = wx_manipulacaoDados.fechamentoSemanas(vazao_inc, ultima_rv_date, prox_rv_date, fechamentos)

	calcuIncrementais = {239: [239, 238], 242: [242, 240], 273: [257, 273], 253: [191, 253]}
	for posto in calcuIncrementais:
		vazao_inc.loc[posto] = vazao_inc.reindex(calcuIncrementais[posto]).sum()


	medias_nat_semanais = pd.DataFrame()
	medias_inc_semanais = pd.DataFrame()

	dt = pd.Timestamp(prox_rv_date + datetime.timedelta(days=7))
	while medias_nat_semanais.shape[1] < 8 or dt > pd.Timestamp(ultima_rv_date):
		medias_nat_semanais[dt] = vazao_nat[pd.date_range(dt, dt+datetime.timedelta(days=6))].mean(axis=1)
		medias_inc_semanais[dt] = vazao_inc[pd.date_range(dt, dt+datetime.timedelta(days=6))].mean(axis=1)
		dt = dt - datetime.timedelta(days=7)

	medias_nat_semanais = medias_nat_semanais.reindex(sorted(medias_nat_semanais.columns), axis=1)
	medias_inc_semanais = medias_inc_semanais.reindex(sorted(medias_inc_semanais.columns), axis=1)

	compiladoStr = wx_manipulaArqPrevivaz.leituraStrs(pathUltimaRev)

	mediasSemanais = pd.DataFrame()
	for posto in compiladoStr:
		if posto in postosSmap:
			dtFinalStr = pd.Timestamp(prox_rv_date + datetime.timedelta(days=7))
		# problema na virada de ano de 2021 para 2022
		elif posto == 158 and prox_rv_date == datetime.date(2021, 12, 25):
			dtFinalStr = pd.Timestamp(prox_rv_date)
		else:
			dtFinalStr = pd.Timestamp(prox_rv_date - datetime.timedelta(days=7))

		dtAtualizar = medias_nat_semanais.columns[0]
		while dtAtualizar <= dtFinalStr:
			if not infoPostos.loc[posto, 'INCREMENTAL_PREVIVAZ']:
				mediasSemanais.loc[posto,dtAtualizar] = int(round(medias_nat_semanais.loc[posto,dtAtualizar]))
			else:
				mediasSemanais.loc[posto,dtAtualizar] = int(round(medias_inc_semanais.loc[posto,dtAtualizar]))
			dtAtualizar = dtAtualizar + datetime.timedelta(days=7)

	compiladoStr = wx_manipulaArqPrevivaz.atualizarStrs(compiladoStr, mediasSemanais)
	
	wx_manipulaArqPrevivaz.gerarArqEntradaPrevivaz(pathUltimaRev, compiladoStr, prox_rv_date, postosSmap, numeroSemanasPrevisaoSmap)
	wx_manipulaArqPrevivaz.criarBatFiles(pathUltimaRev, pathExecPrevivaz, compiladoStr.keys())
	wx_manipulaArqPrevivaz.executarBatFiles(pathUltimaRev)

	prevsCompilado_df = wx_manipulaArqPrevivaz.carregaFutFiles(pathUltimaRev)
	prevsCompiladoCompleto_df = mediasSemanais.reindex(columns=mediasSemanais.columns.union(prevsCompilado_df.columns.astype('datetime64[ns]')))
	prevsCompiladoCompleto_df.update(prevsCompilado_df)

	prevsCompilado_df = wx_manipulaArqPrevivaz.calcularRegressao(prevsCompiladoCompleto_df)

	prevs_df = pd.DataFrame(medias_nat_semanais[semanasCompletasAcomph], columns=semanasPrevs)
	prevs_df.loc[postosSmap, previsoesSmap] = medias_nat_semanais.loc[postosSmap,previsoesSmap]
	prevs_df[prevs_df.isnull()] = prevsCompilado_df

	prevs_df.loc[238,previsaoPrevivaz] = prevsCompilado_df.loc[237,previsaoPrevivaz] + prevsCompilado_df.loc[239,previsaoPrevivaz]*0.342
	prevs_df.loc[239,previsaoPrevivaz] = prevsCompilado_df.loc[[237,239],previsaoPrevivaz].sum()
	prevs_df.loc[240,previsaoPrevivaz] = prevs_df.loc[239,previsaoPrevivaz] + prevsCompilado_df.loc[242,previsaoPrevivaz]*0.717
	prevs_df.loc[242,previsaoPrevivaz] = prevsCompilado_df.loc[242,previsaoPrevivaz] + prevs_df.loc[239,previsaoPrevivaz]
	
	prevs_df.loc[243,previsaoPrevivaz] = prevs_df.loc[242,previsaoPrevivaz] + prevsCompilado_df.loc[243,previsoes]
	prevs_df.loc[34,previsaoPrevivaz] = prevsCompilado_df.loc[[34,18,33,99,241,261],previsaoPrevivaz].sum()
	prevs_df.loc[245,previsaoPrevivaz] = prevs_df.loc[[34,243],previsaoPrevivaz].sum() + prevsCompilado_df.loc[245,previsaoPrevivaz]
	prevs_df.loc[246,previsaoPrevivaz] = prevs_df.loc[[154,245],previsaoPrevivaz].sum() + prevsCompilado_df.loc[246,previsaoPrevivaz]
	prevs_df.loc[266,previsaoPrevivaz] = prevs_df.loc[[266,63,246],previsaoPrevivaz].sum()

	prevs_df.loc[191,previsoes] = prevsCompilado_df.loc[270,previsoes] + prevsCompilado_df.loc[253,previsoes]*0.504
	prevs_df.loc[253,previsoes] = prevs_df.loc[191,previsoes] + prevsCompilado_df.loc[253,previsoes]*0.496
	prevs_df.loc[257,previsoes] = prevs_df.loc[253,previsoes] + prevsCompilado_df.loc[273,previsoes]*0.488
	prevs_df.loc[273,previsoes] = prevs_df.loc[257,previsoes] + prevsCompilado_df.loc[273,previsoes]*0.512
	prevs_df.loc[271,previsoes] = prevs_df.loc[273,previsoes] + prevsCompilado_df.loc[271,previsoes]
	prevs_df.loc[275,previsoes] = prevs_df.loc[271,previsoes] + prevsCompilado_df.loc[275,previsoes]

	for dt in prevs_df.columns:
		if dt in medias_nat_semanais:
			prevs_df.loc[169,dt] = medias_nat_semanais.loc[169,dt]
		else:
			dtDefasada = dt - datetime.timedelta(days=14)
			prevs_df.loc[169,dt] = prevs_df.loc[156,dtDefasada] + prevs_df.loc[158,dtDefasada] + prevs_df.loc[168,dt]

	ordemPostos = [1, 2, 6, 7, 8, 9, 10, 11, 12, 14, 15, 16, 17, 18, 22, 23, 24, 25, 28]
	ordemPostos += [31, 32, 33, 34, 47, 48, 49, 50, 51, 52, 57, 61, 62, 63, 71, 72, 73]
	ordemPostos += [74, 76, 77, 78, 81, 88, 89, 92, 93, 94, 97, 98, 99, 101, 102, 103, 104]
	ordemPostos += [109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122]
	ordemPostos += [123, 125, 129, 130, 134, 135, 141, 144, 145, 148, 149, 154, 155, 156]
	ordemPostos += [158, 160, 161, 166, 168, 169, 171, 172, 173, 175, 176, 178, 183, 188]
	ordemPostos += [190, 191, 196, 197, 198, 201, 202, 203, 204, 205, 206, 207, 209, 211]
	ordemPostos += [215, 216, 217, 220, 222, 227, 228, 229, 230, 237, 238, 239, 240, 241]
	ordemPostos += [242, 243, 244, 245, 246, 247, 248, 249, 251, 252, 253, 254, 255, 257]
	ordemPostos += [259, 261, 262, 263, 266, 269, 270, 271, 273, 275, 277, 278, 279, 280]
	ordemPostos += [281, 283, 284, 285, 286, 287, 288, 290, 291, 294, 295, 296, 297, 301]
	ordemPostos += [320]

	prevs_df = wx_manipulaArqPrevivaz.calcularRegressao(prevs_df)

	# Remocao do fechamento para o calculo
	prevs_df.loc[244,previsoes] = np.nan
	prevs_df.loc[171,:] = 0
	prevs_df[prevs_df < -0] = 0 # remocao dos valores -0
	prevs_df = wx_manipulaArqPrevivaz.formulasPrevisao(prevs_df)

	dir_saida = os.path.join(diretorioApp, 'arquivos', 'saida', dataExc.strftime('%Y%m%d'))
	if not os.path.exists(dir_saida):
		os.makedirs(dir_saida)

	if '.ESTUDO' in modelo.upper():
		prevs_file_path = os.path.join(dir_saida, "PREVS-{}_{}{}.RV{}".format(modelo, prox_rv.anoReferente, prox_rv.mesReferente, prox_rv.atualRevisao))
	else:
		prevs_file_path = os.path.join(dir_saida, "PREVS-{}.RV{}".format(modelo, prox_rv.atualRevisao))
	
	prevs_file_out = open(prevs_file_path,'w')
	for i, posto in enumerate(ordemPostos):
		linha = "{index:>6d}{posto:5d}".format(index=i+1, posto=posto)
		for sem in prevs_df.columns[:6]:
			vazao = prevs_df.loc[posto,sem]
			linha = linha + "{vazao:10.0f}".format(vazao=round(vazao,0))
		linha = linha + "\n"
		prevs_file_out.write(linha)
	prevs_file_out.close()

	print('PREVS gerado com sucesso.')
	print('{}'.format(prevs_file_path))

def rodar_previvaz_encadeado(data, modelo, fechamentos, ultima_rv_date, prox_rv_date):

	global diretorioApp

	numeroSemanasPrevisaoSmap = 2

	configs = wx_configs.getConfiguration()

	pathArquivosEntrada = os.path.join(diretorioApp, 'arquivos')

	dropbox_path = os.path.abspath(configs['paths']['dropbox_middle'])
	cv_dropbox_path = os.path.join(dropbox_path, 'Chuva-vazão')
	novoSmap_dropbox_path = os.path.join(dropbox_path, 'NovoSMAP')

	dataExc = datetime.datetime.strptime(data,"%d/%m/%Y")
	if 'PRELIMINAR' in modelo:
		dataRef = dataExc - datetime.timedelta(days=1)
	else:
		dataRef = dataExc

	# ultima rv oficial
	ultima_rv = wx_opweek.ElecData(ultima_rv_date)
	# Rv desejada
	prox_rv = wx_opweek.ElecData(prox_rv_date)
	# RV anterior a desejada
	ante_rv = wx_opweek.ElecData(prox_rv_date-datetime.timedelta(days=7))

	revisao_atual = ultima_rv.atualRevisao
	if revisao_atual == 0:
		ultimaRv = 'PMO'
	else:
		ultimaRv = 'REV{}'.format(revisao_atual)

	nomePastaUltimaRev = "Arq_Entrada_e_Saida_PREVIVAZ_{}{:0>2}_{}".format(ultima_rv.anoReferente, ultima_rv.mesReferente, ultimaRv)

	pathUltimaRev = os.path.join(pathArquivosEntrada, 'entrada', nomePastaUltimaRev)
	pathExecPrevivaz = os.path.join(pathArquivosEntrada, 'executavel', 'previvaz.exe')
	cpinsFile = os.path.join(dropbox_path, 'NovoSMAP', 'PlanilhaUSB.xls')

	nomeArquivoSmap = 'smap_nat_{}.txt'.format(modelo)
	if 'PRELIMINAR' in modelo:
		pathArquivoSmap = os.path.join(cv_dropbox_path, dataExc.strftime('%Y%m%d'), 'preliminar', nomeArquivoSmap)
	else:
		pathArquivoSmap = os.path.join(cv_dropbox_path, dataExc.strftime('%Y%m%d'), 'teste', nomeArquivoSmap)

	semanasPrevs = []
	for i in range(6):
		semanasPrevs.append(pd.Timestamp(prox_rv.primeiroDiaMes+datetime.timedelta(days=i*7)))

	ultimoSabadoAcomph = wx_opweek.getLastSaturday(dataRef)
	semanasCompletasAcomph = []
	for sem in semanasPrevs:
		if sem <= wx_opweek.getLastSaturday(dataRef):
			semanasCompletasAcomph.append(sem)

	previsoesSmap = []
	for sem in range(numeroSemanasPrevisaoSmap): # (GT-SMAP)
		previsoesSmap.append(ultimoSabadoAcomph+datetime.timedelta(days=(sem+1)*7))

	previsaoPrevivaz = []
	for sem in semanasPrevs:
		if sem not in semanasCompletasAcomph and sem not in previsoesSmap:
			previsaoPrevivaz.append(sem)

	previsoes = previsoesSmap + previsaoPrevivaz


	print("\nDiretorio base: {}".format(pathUltimaRev))
	print("Arquivo SMAP:   {}\n".format(pathArquivoSmap))

	postosIncPrevivaz = [34, 245, 246, 266, 239, 242, 243, 154, 161, 191, 253, 257, 273, 271, 275]
	postosIncSmap = [34, 245, 246, 266, 239, 242, 243, 154, 161, 191, 253, 257, 273, 271, 275]
	postosPrevivaz = wx_manipulaArqPrevivaz.getListaPostosPrevivaz(pathUltimaRev)

	db_clime = wx_dbLib.DadosClime()
	postosRegredidos = db_clime.getPostosRegredidos()

	dbAnswer = db_clime.getInfoPostos()
	infoPostos = pd.DataFrame(dbAnswer, columns=['CD_POSTO', 'STR_POSTO', 'VL_PRODUTIBILIDADE', 'CD_SUBMERCADO', 'STR_SUBMERCADO', 'STR_SIGLA', 'CD_BACIA', 'STR_BACIA', 'CD_REE', 'STR_REE'])
	infoPostos.set_index('CD_POSTO', inplace=True)
	
	infoPostos['INCREMENTAL_PREVIVAZ'] = False
	infoPostos.loc[postosIncPrevivaz, 'INCREMENTAL_PREVIVAZ'] = True

	vazao_nat_smap, vazao_inc_smap = wx_manipulaArqSmap.importarSaidaSmap(pathArquivoSmap, dataRef, postosIncSmap)
	vazao_nat_smap = wx_postosAux.calcPostosArtificiais_df(vazao_nat_smap, ignorar_erros=True)
	vazao_inc_smap = wx_postosAux.calcPostosArtificiais_df(vazao_inc_smap, ignorar_erros=True)
	postosSmap = list(vazao_nat_smap.index) + list(vazao_inc_smap.index)

	columns = ['CD_POSTO',	'DT_REFERENTE',	'VL_VAZ_INC_CONSO',	'VL_VAZ_NAT_CONSO']

	# Historico utilizado para complemetar o ultimo acomph e atualizar as ultimas 6 antes do inicio da previsao
	acomphHistorico = db_clime.getAcomph(ultima_rv_date - datetime.timedelta(days=7*5), dtFim=dataRef-datetime.timedelta(days=31))
	df_acomphHistorico = pd.DataFrame(acomphHistorico)
	df_acomphHistorico = df_acomphHistorico[[0,1,2,3]]
	df_acomphHistorico.columns = columns

	ultimoAcomph = db_clime.getAcomphEspecifico(dataRef-datetime.timedelta(days=1))
	df_ultimoAcomph = pd.DataFrame(ultimoAcomph)
	df_ultimoAcomph = df_ultimoAcomph[[1,0,8,9]]
	df_ultimoAcomph.columns = columns

	df_vazaoAcomph = df_acomphHistorico.append(df_ultimoAcomph)
	vazao_nat_acomph = df_vazaoAcomph.pivot(index='CD_POSTO', columns='DT_REFERENTE', values='VL_VAZ_NAT_CONSO')
	vazao_nat_acomph = wx_postosAux.calcPostosArtificiais_df(vazao_nat_acomph, ignorar_erros=False)
	vazao_nat = vazao_nat_acomph.reindex(columns=vazao_nat_acomph.columns.union(vazao_nat_smap.columns))
	vazao_nat.update(vazao_nat_smap)
	
	vazao_inc_acomph = df_vazaoAcomph.pivot(index='CD_POSTO', columns='DT_REFERENTE', values='VL_VAZ_INC_CONSO')
	vazao_inc_acomph = wx_postosAux.calcPostosArtificiais_df(vazao_inc_acomph, ignorar_erros=False)
	idx = vazao_inc_smap.index.append(pd.Index([238, 240])) # Postos que sao usados para o calculo de vazao de outros postos
	vazao_inc = vazao_inc_acomph.reindex(idx, columns=vazao_inc_acomph.columns.union(vazao_inc_smap.columns))
	vazao_inc.update(vazao_inc_smap)

	cpins_df = wx_manipulaArqCpins.getCpins(cpinsFile, vazao_nat.columns.min(), vazao_nat.columns.max())
	# Atualiza todo o df com os valores do CPINS
	for p in [168, 169]:
		for d in vazao_nat.columns:
			if d in cpins_df.index:
				vazao_nat.loc[p,d] = cpins_df.loc[d,p]

	vazao_nat = vazao_nat.replace(-999, np.NaN)

	vazao_nat = wx_manipulacaoDados.propagarPostosAcomph(vazao_nat, vazao_inc, dataRef)

	# [ 161, 191, 253, 257, 273, 271, 275]
	postosTieteTocantins = {}
	postosTieteTocantins[239] = {'natural':[238],'incremental':[239]}
	postosTieteTocantins[242] = {'natural':[240],'incremental':[242]}
	postosTieteTocantins[243] = {'natural':[242],'incremental':[243]}
	postosTieteTocantins[34] = {'natural':[18,33,99,241,261],'incremental':[34]}
	postosTieteTocantins[245] = {'natural':[243,34],'incremental':[245]}
	postosTieteTocantins[154] = {'natural':[],'incremental':[154]}
	postosTieteTocantins[246] = {'natural':[245,154],'incremental':[246]}
	postosTieteTocantins[266] = {'natural':[246,63],'incremental':[266]}

	postosTieteTocantins[161] = {'natural':[],'incremental':[161]}

	for posto in postosTieteTocantins:
		dts = vazao_nat.loc[posto].isna()
		vazao_nat.loc[posto, dts] = vazao_nat.loc[postosTieteTocantins[posto]['natural'], dts].sum() + vazao_inc.loc[postosTieteTocantins[posto]['incremental'], dts].sum()

	while ante_rv.data > ultima_rv.data:
		dir_arquivosSimulacaoAnterior = os.path.join(diretorioApp, 'arquivos', 'saida', dataExc.strftime('%Y%m%d'), 'estudos', modelo, "{}{:0>2}_RV{}".format(ante_rv.anoReferente, ante_rv.mesReferente, ante_rv.atualRevisao))
		# if os.path.exists(dir_arquivosSimulacaoAnterior):
		if True:
			for posto in postosPrevivaz:
				if posto not in postosSmap:

					path_fut = os.path.join(dir_arquivosSimulacaoAnterior, "{}_fut.DAT".format(posto))
					arquivo_fut = open(path_fut, 'r').readlines()
					dados_fut = arquivo_fut[1].split()

					dt_inicial = ante_rv.data
					dt_final = ante_rv.data+datetime.timedelta(days=6)
					
					if posto in postosIncPrevivaz:
						vazao_inc.loc[posto,dt_inicial:dt_final] = float(dados_fut[4])
					else:
						vazao_nat.loc[posto,dt_inicial:dt_final] = float(dados_fut[4])
		
		ante_rv = wx_opweek.ElecData(ante_rv.data-datetime.timedelta(days=7))

	vazao_nat = wx_manipulacaoDados.fechamentoSemanas(vazao_nat, ultima_rv_date, prox_rv_date, fechamentos)
	vazao_inc = wx_manipulacaoDados.fechamentoSemanas(vazao_inc, ultima_rv_date, prox_rv_date, fechamentos)

	calcuIncrementais = {239: [239, 238], 242: [242, 240], 273: [257, 273], 253: [191, 253]}
	for posto in calcuIncrementais:
		vazao_inc.loc[posto] = vazao_inc.reindex(calcuIncrementais[posto]).sum()


	medias_nat_semanais = pd.DataFrame()
	medias_inc_semanais = pd.DataFrame()

	dt = pd.Timestamp(prox_rv_date + datetime.timedelta(days=7))
	while medias_nat_semanais.shape[1] < 8 or dt >= pd.Timestamp(ultima_rv_date):
		medias_nat_semanais[dt] = vazao_nat[pd.date_range(dt, dt+datetime.timedelta(days=6))].mean(axis=1)
		medias_inc_semanais[dt] = vazao_inc[pd.date_range(dt, dt+datetime.timedelta(days=6))].mean(axis=1)
		dt = dt - datetime.timedelta(days=7)

	medias_nat_semanais = medias_nat_semanais.reindex(sorted(medias_nat_semanais.columns), axis=1)
	medias_inc_semanais = medias_inc_semanais.reindex(sorted(medias_inc_semanais.columns), axis=1)

	compiladoStr = wx_manipulaArqPrevivaz.leituraStrs(pathUltimaRev)

	mediasSemanais = pd.DataFrame()
	for posto in compiladoStr:
		if posto in postosSmap:
			dtFinalStr = pd.Timestamp(prox_rv_date + datetime.timedelta(days=7))
		else:
			dtFinalStr = pd.Timestamp(prox_rv_date - datetime.timedelta(days=7))

		dtAtualizar = medias_nat_semanais.columns[0]
		while dtAtualizar <= dtFinalStr:
			if not infoPostos.loc[posto, 'INCREMENTAL_PREVIVAZ']:
				vaz = round(medias_nat_semanais.loc[posto,dtAtualizar])
			else:
				vaz = round(medias_inc_semanais.loc[posto,dtAtualizar])
			mediasSemanais.loc[posto,dtAtualizar] = vaz
			dtAtualizar = dtAtualizar + datetime.timedelta(days=7)

	compiladoStr = wx_manipulaArqPrevivaz.atualizarStrs(compiladoStr, mediasSemanais)
	
	wx_manipulaArqPrevivaz.gerarArqEntradaPrevivaz(pathUltimaRev, compiladoStr, prox_rv_date, postosSmap, numeroSemanasPrevisaoSmap)

	wx_manipulaArqPrevivaz.criarBatFiles(pathUltimaRev, pathExecPrevivaz, compiladoStr.keys())
	wx_manipulaArqPrevivaz.executarBatFiles(pathUltimaRev)

	prevsCompilado_df = wx_manipulaArqPrevivaz.carregaFutFiles(pathUltimaRev)
	prevsCompiladoCompleto_df = mediasSemanais.reindex(columns=mediasSemanais.columns.union(prevsCompilado_df.columns.astype('datetime64[ns]')))
	prevsCompiladoCompleto_df.update(prevsCompilado_df)

	prevsCompilado_df = wx_manipulaArqPrevivaz.calcularRegressao(prevsCompiladoCompleto_df)

	prevs_df = pd.DataFrame(medias_nat_semanais[semanasCompletasAcomph], columns=semanasPrevs)
	prevs_df.loc[postosSmap, previsoesSmap] = medias_nat_semanais.loc[postosSmap,previsoesSmap]
	prevs_df[prevs_df.isnull()] = prevsCompilado_df

	prevs_df.loc[238,previsaoPrevivaz] = prevsCompilado_df.loc[237,previsaoPrevivaz] + prevsCompilado_df.loc[239,previsaoPrevivaz]*0.342
	prevs_df.loc[239,previsaoPrevivaz] = prevsCompilado_df.loc[[237,239],previsaoPrevivaz].sum()
	prevs_df.loc[240,previsaoPrevivaz] = prevs_df.loc[239,previsaoPrevivaz] + prevsCompilado_df.loc[242,previsaoPrevivaz]*0.717
	prevs_df.loc[242,previsaoPrevivaz] = prevsCompilado_df.loc[242,previsaoPrevivaz] + prevs_df.loc[239,previsaoPrevivaz]
	
	prevs_df.loc[243,previsaoPrevivaz] = prevs_df.loc[242,previsaoPrevivaz] + prevsCompilado_df.loc[243,previsoes]
	prevs_df.loc[34,previsaoPrevivaz] = prevsCompilado_df.loc[[34,18,33,99,241,261],previsaoPrevivaz].sum()
	prevs_df.loc[245,previsaoPrevivaz] = prevs_df.loc[[34,243],previsaoPrevivaz].sum() + prevsCompilado_df.loc[245,previsaoPrevivaz]
	prevs_df.loc[246,previsaoPrevivaz] = prevs_df.loc[[154,245],previsaoPrevivaz].sum() + prevsCompilado_df.loc[246,previsaoPrevivaz]
	prevs_df.loc[266,previsaoPrevivaz] = prevs_df.loc[[266,63,246],previsaoPrevivaz].sum()

	prevs_df.loc[191,previsoes] = prevsCompilado_df.loc[270,previsoes] + prevsCompilado_df.loc[253,previsoes]*0.504
	prevs_df.loc[253,previsoes] = prevs_df.loc[191,previsoes] + prevsCompilado_df.loc[253,previsoes]*0.496
	prevs_df.loc[257,previsoes] = prevs_df.loc[253,previsoes] + prevsCompilado_df.loc[273,previsoes]*0.488
	prevs_df.loc[273,previsoes] = prevs_df.loc[257,previsoes] + prevsCompilado_df.loc[273,previsoes]*0.512
	prevs_df.loc[271,previsoes] = prevs_df.loc[273,previsoes] + prevsCompilado_df.loc[271,previsoes]
	prevs_df.loc[275,previsoes] = prevs_df.loc[271,previsoes] + prevsCompilado_df.loc[275,previsoes]

	for dt in prevs_df.columns:
		if dt in medias_nat_semanais:
			prevs_df.loc[169,dt] = medias_nat_semanais.loc[169,dt]
		else:
			dtDefasada = dt - datetime.timedelta(days=14)
			prevs_df.loc[169,dt] = prevs_df.loc[156,dtDefasada] + prevs_df.loc[158,dtDefasada] + prevs_df.loc[168,dt]

	ordemPostos = [1, 2, 6, 7, 8, 9, 10, 11, 12, 14, 15, 16, 17, 18, 22, 23, 24, 25, 28]
	ordemPostos += [31, 32, 33, 34, 47, 48, 49, 50, 51, 52, 57, 61, 62, 63, 71, 72, 73]
	ordemPostos += [74, 76, 77, 78, 81, 88, 89, 92, 93, 94, 97, 98, 99, 101, 102, 103, 104]
	ordemPostos += [109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122]
	ordemPostos += [123, 125, 129, 130, 134, 135, 141, 144, 145, 148, 149, 154, 155, 156]
	ordemPostos += [158, 160, 161, 166, 168, 169, 171, 172, 173, 175, 176, 178, 183, 188]
	ordemPostos += [190, 191, 196, 197, 198, 201, 202, 203, 204, 205, 206, 207, 209, 211]
	ordemPostos += [215, 216, 217, 220, 222, 227, 228, 229, 230, 237, 238, 239, 240, 241]
	ordemPostos += [242, 243, 244, 245, 246, 247, 248, 249, 251, 252, 253, 254, 255, 257]
	ordemPostos += [259, 261, 262, 263, 266, 269, 270, 271, 273, 275, 277, 278, 279, 280]
	ordemPostos += [281, 283, 284, 285, 286, 287, 288, 290, 291, 294, 295, 296, 297, 301]
	ordemPostos += [320]

	prevs_df = wx_manipulaArqPrevivaz.calcularRegressao(prevs_df)

	# Remocao do fechamento para o calculo
	prevs_df.loc[244,previsoes] = np.nan
	prevs_df.loc[171,:] = 0
	prevs_df[prevs_df < -0] = 0 # remocao dos valores -0
	prevs_df = wx_manipulaArqPrevivaz.formulasPrevisao(prevs_df)

	dir_saida = os.path.join(diretorioApp, 'arquivos', 'saida', dataExc.strftime('%Y%m%d'))
	if not os.path.exists(dir_saida):
		os.makedirs(dir_saida)

	posInsert = modelo.find('.estudo')
	nomePrevs = 'PREVS-{}-{}{:0>2}{}.RV{}'.format(modelo[0:posInsert], prox_rv.anoReferente, prox_rv.mesReferente, modelo[posInsert:], prox_rv.atualRevisao)
	prevs_file_path = os.path.join(dir_saida, nomePrevs)

	prevs_file_out = open(prevs_file_path,'w')
	for i, posto in enumerate(ordemPostos):
		linha = "{index:>6d}{posto:5d}".format(index=i+1, posto=posto)
		for sem in prevs_df.columns[:6]:
			vazao = prevs_df.loc[posto,sem]
			linha = linha + "{vazao:10.0f}".format(vazao=round(vazao,0))
		linha = linha + "\n"
		prevs_file_out.write(linha)
	prevs_file_out.close()

	print('PREVS gerado com sucesso.')
	print('{}'.format(prevs_file_path))

	dir_arquivosSimulacao = os.path.join(dir_saida, 'estudos', modelo, "{}{:0>2}_RV{}".format(prox_rv.anoReferente, prox_rv.mesReferente, prox_rv.atualRevisao))
	if not os.path.exists(dir_arquivosSimulacao):
		os.makedirs(dir_arquivosSimulacao)

	lista_arquivos_salvos = ['.inp', '_fut.DAT', '_str.DAT']
	lista_casos = [d for d in os.listdir(pathUltimaRev) if 'Caso' in d]
	for diretorio in lista_casos:
		for arquivo in lista_arquivos_salvos:
			posto = re.search('Caso([0-9]{1,})', diretorio)[1]
			src = os.path.join(pathUltimaRev, diretorio, '{}{}'.format(posto,arquivo))
			shutil.copy(src, dir_arquivosSimulacao)

def printHelper():
	""" Imprime na tela o helper da funcao 
	:param None: 
	:return None: 
	"""
	hoje = datetime.datetime.now()
	ultimoSabado = hoje
	while ultimoSabado.weekday() != 5:
		ultimoSabado -= datetime.timedelta(days=1)
	print("python {} modelos ['PCONJUNTO'] data {} ultima_rv {} prox_rv {} fechamentos 5 run 00 flag_pdp True".format(sys.argv[0], hoje.strftime("%d/%m/%Y"), ultimoSabado.strftime("%d/%m/%Y"), (ultimoSabado+datetime.timedelta(days=7)).strftime("%d/%m/%Y")))
	print("python {} encadeado modelos ['PCONJUNTO.estudo'] data {} ultima_rv {} prox_rv {} fechamentos 5 run 00 flag_pdp True".format(sys.argv[0], hoje.strftime("%d/%m/%Y"), ultimoSabado.strftime("%d/%m/%Y"), (ultimoSabado+datetime.timedelta(days=7)).strftime("%d/%m/%Y")))

def runWithParams():
	parametros = {}
	if len(sys.argv) > 1:
		# passar o print do help pra quando nao entrar com nenhum parametro
		if 'help' in sys.argv or '--h' in sys.argv or '-help' in sys.argv:
			printHelper()
			quit()

		# Valores defaults
		p_dataReferente = datetime.datetime.strptime(datetime.datetime.now().strftime('%d%m%Y'), '%d%m%Y')


		p_dataUltimaRev = p_dataReferente
		while p_dataUltimaRev.weekday() != 5:
			p_dataUltimaRev -= datetime.timedelta(days=1)

		p_dataProximaRev = p_dataUltimaRev+datetime.timedelta(days=7)

		p_modelos = ['PCONJUNTO']
		p_run = '00'
		p_fechamentos = 5
		p_flagPdp = ''
		flagPdp = 'False'
		
		tiposFechamentos = {1: 'Historico', 2: 'PREVS', 3: 'MLT', 4: 'Media do RDH', 5: 'Tendencia'}

		# Verificacao e substituicao dos valores defaults com 
		# os valores passados por parametro 
		for i in range(1, len(sys.argv[1:])):
			argumento = sys.argv[i].lower()

			if argumento in ['data','-data']:
				try:
					p_dataReferente = sys.argv[i+1]
					datetime.datetime.strptime(p_dataReferente, "%d/%m/%Y")
				except:
					print("Erro ao tentar converter {} em data!".format(p_dataReferente))
					quit()

			elif argumento in ['modelos','-modelos']:
				try:
					p_modelos = eval(sys.argv[i+1])
				except:
					print("Erro ao tentar reconhecer os modelos passados por parametros.\n Ex: ... modelos ['PCONJUNTO','GEFS','GFS'] ... ")
					quit()

			elif argumento in ['run','-run']:
				try:
					p_run = "{:0>2}".format(sys.argv[i+1])
				except:
					print("Erro ao tentar pegar run, valor encontrado:{}".format(sys.argv[i+1]))
					quit()

			elif argumento in ['ultima_rv','-ultima_rv']:
				try:
					p_dataUltimaRev = datetime.datetime.strptime(sys.argv[i+1], "%d/%m/%Y")
					p_dataUltimaRev = wx_opweek.getLastSaturday(p_dataUltimaRev.date())   
				except:
					print('Erro ao tentar converter {} em data! (ultima_rv)'.format(sys.argv[i+1]))
					quit()

			elif argumento in ['prox_rv','-prox_rv']:
				try:
					p_dataProximaRev = datetime.datetime.strptime(sys.argv[i+1], "%d/%m/%Y")
					p_dataProximaRev = wx_opweek.getLastSaturday(p_dataProximaRev.date())   
				except:
					print('Erro ao tentar converter {} em data! (prox_rv)'.format(sys.argv[i+1]))
					quit()

			elif argumento in ['fechamentos','-fechamentos']:
				try:
					if int(sys.argv[i+1]) not in tiposFechamentos:
						print('Tipo de fechamento '+sys.argv[i+1]+' nao implementado')
						print('Escolha uma das opcoes de fechamento: ')
						for i in tiposFechamentos:
							print('{}: {}'.format(i, tiposFechamentos[i]))
					else:
						p_fechamentos = int(sys.argv[i+1])
				except:
					print('Não e possivel selecionar o modo de fechamento {}!'.format(sys.argv[i+1]))
					quit()

			elif argumento in ['flag_pdp','-flag_pdp']:
				try:
					if sys.argv[i+1].lower() == 'true' or sys.argv[i+1] == '1':
						p_flagPdp = '.PDP'
						flagPdp = 'True'					

				except:
					print("Erro ao tentar pegar flag_pdp, valor encontrado:".format(sys.argv[i+1]))
					quit()

		if 'encadeado' in sys.argv or 'ENCADEADO' in sys.argv :

			p_modelo = p_modelos[0]

			print('\n ****************** PREVIVAZ ****************** ')
			print('Execução iniciada em: {}'.format(str(datetime.datetime.now())[:19]))
			print('Data          : {}'.format(p_dataReferente))
			print('Modelo        : {}'.format(p_modelo))
			print('Horario       : {}'.format(p_run))
			print('Ultima REV    : {}'.format(p_dataUltimaRev))
			print('Proxima REV   : {}'.format(p_dataProximaRev))
			print('Fechamento    : {} ({})'.format(p_fechamentos, tiposFechamentos[p_fechamentos]))
			print('Flag PDP      : {}'.format(flagPdp))
			print(' ********************************************** ')

			modelo_full = p_modelo + p_flagPdp + '_r' + p_run + 'z'
			rodar_previvaz_encadeado(data=p_dataReferente, modelo=modelo_full, fechamentos=p_fechamentos, ultima_rv_date=p_dataUltimaRev, prox_rv_date=p_dataProximaRev)

		else:

			for p_modelo in p_modelos:
				print('\n ****************** PREVIVAZ ****************** ')
				print('Execução iniciada em: {}'.format(str(datetime.datetime.now())[:19]))
				print('Data          : {}'.format(p_dataReferente))
				print('Modelo        : {}'.format(p_modelo))
				print('Horario       : {}'.format(p_run))
				print('Ultima REV    : {}'.format(p_dataUltimaRev))
				print('Proxima REV   : {}'.format(p_dataProximaRev))
				print('Fechamento    : {} ({})'.format(p_fechamentos, tiposFechamentos[p_fechamentos]))
				print('Flag PDP      : {}'.format(flagPdp))
				print(' ********************************************** ')

				modelo_full = p_modelo + p_flagPdp + '_r' + p_run + 'z'
				rodar_previvaz(data=p_dataReferente, modelo=modelo_full, fechamentos=p_fechamentos, ultima_rv_date=p_dataUltimaRev, prox_rv_date=p_dataProximaRev)
	else:
		printHelper()
		exit()

if __name__ == '__main__':
	runWithParams()
