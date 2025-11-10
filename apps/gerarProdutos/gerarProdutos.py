# -*- coding: utf-8 -*-
import os
import sys
import locale
import datetime
import requests
from dateutil import relativedelta
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pandas.plotting import register_matplotlib_converters
from matplotlib import gridspec
register_matplotlib_converters()

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")),'.env'))
from middle.utils import Constants

constants = Constants()
try:
	locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
except:
	locale.setlocale(locale.LC_ALL, '')

diretorioApp = os.path.abspath('.')
appsDir = os.path.dirname(diretorioApp)
pathArquivos = os.path.join(diretorioApp,'arquivos')

sys.path.insert(1, "/WX2TB/Documentos/fontes/PMO/scripts_unificados/bibliotecas/")
import configs
import wx_consultaProcessos
import rz_relatorio_bbce
import wx_dbLib
import wx_postosAux
import wx_emailSender
import wx_opweek

sys.path.insert(1,"/WX2TB/Documentos/fontes/PMO/scripts_unificados/")
from apps.gerarProdutos.utils import get_access_token


def is_first_bussines_day(data:datetime.date):
    return (data.day == 1 and (data.weekday() != 5 and data.weekday() != 6)) or ((data.day == 2 or data.day == 3) and data.weekday() == 0)


def geraRelatorioBbce(data = datetime.datetime.now()):
    
    if is_first_bussines_day(data.date()):
        
        quantidade_produtos = {}
        quantidade_produtos['mensal'] = 6
        quantidade_produtos['trimestral'] = 5
        quantidade_produtos['semestral'] = 4
        quantidade_produtos['anual'] = 5
        
        lista_produtos = []
        for graniularidade in quantidade_produtos:
            for i_produtos in range(quantidade_produtos[graniularidade]):
                if graniularidade == 'mensal':
                    dt_referente = data + relativedelta.relativedelta(months=i_produtos)
                    produto = f"SE CON MEN {dt_referente.strftime('%B').upper()[:3]}/{dt_referente.strftime('%y')} - Preço Fixo"

                elif graniularidade == 'trimestral':
                    dt_inicial = data
                    while dt_inicial.month%3 != 1:
                        dt_inicial = dt_inicial + relativedelta.relativedelta(months=1)
                    dt_inicial = dt_inicial + relativedelta.relativedelta(months=i_produtos*3)
                    dt_final = dt_inicial + relativedelta.relativedelta(months=2)
                    produto = f"SE CON TRI {dt_inicial.strftime('%B').upper()[:3]}/{dt_inicial.strftime('%y')} {dt_final.strftime('%B').upper()[:3]}/{dt_final.strftime('%y')} - Preço Fixo"
                elif graniularidade == 'semestral':
                    dt_inicial = data
                    if dt_inicial.month != 1:
                        while dt_inicial.month%6 != 0:
                            dt_inicial = dt_inicial + relativedelta.relativedelta(months=1)
                    dt_inicial = dt_inicial + relativedelta.relativedelta(months=i_produtos*6)
                    dt_final = dt_inicial + relativedelta.relativedelta(months=5)
                    produto = f"SE CON SEM {dt_inicial.strftime('%B').upper()[:3]}/{dt_inicial.strftime('%y')} {dt_final.strftime('%B').upper()[:3]}/{dt_final.strftime('%y')} - Preço Fixo"
                elif graniularidade == 'anual':
                    dt_inicial = data
                    while dt_inicial.month%12 != 0:
                        dt_inicial = dt_inicial + relativedelta.relativedelta(months=1)
                    dt_inicial = dt_inicial + relativedelta.relativedelta(months=1+i_produtos*12)
                    dt_final = dt_inicial + relativedelta.relativedelta(months=11)
                    produto = f"SE CON ANU {dt_inicial.strftime('%B').upper()[:3]}/{dt_inicial.strftime('%y')} {dt_final.strftime('%B').upper()[:3]}/{dt_final.strftime('%y')} - Preço Fixo"

                lista_produtos.append(produto)

        dados_a_inserir = [{'str_produto': produto} for produto in lista_produtos]
        dados_a_inserir = pd.DataFrame(dados_a_inserir).reset_index().rename(columns={'index':'ordem'}).to_dict('records')
        requests.post(
            f"{constants.BASE_URL}/api/v2/bbce/produtos-interesse",
            json=dados_a_inserir,
            headers={"Authorization": f"Bearer {get_access_token()}"})
        
  

    body, path_graficos = rz_relatorio_bbce.get_tabela_bbce(data)
    
    return body, path_graficos

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

def gerarResultadoDessem(data):

	idSubmercados = {1:'SE', 2:'S', 3:'NE', 4:'N'}
	siglaSubmercados = ['SE', 'S', 'NE', 'N']

	corpoEmail = '<h3>Resultado da rodada WX do DESSEM referente ao dia {}</h3>'.format(data.strftime('%d/%m/%Y'))

	corpoEmail += '<small> <ul>'
	corpoEmail += '<li><b>wx_p (preliminar):</b> Utiliza o deck do dia anterior como base dos arquivos de entrada</li>'
	corpoEmail += '<li><b>wx_d (definitivo):</b> Utiliza o deck do ONS do dia atual como base dos arquivos de entrada</li>'
	corpoEmail += '</ul> </small>'
	
	datab = wx_dbLib.WxDataB()
	sql = '''SELECT
				ID,
				DT_RODADA,
				DT_REFERENTE,
				TX_FONTE,
				TX_COMENTATIO
			FROM
				TB_CADASTRO_DESSEM
			WHERE
				DT_REFERENTE = \'{}\''''.format(data.strftime(datab.getDateFormat()))

	answer = datab.requestServer(sql)

	rodadas = {}
	dicionarioFonte = {}
	for row in answer:
		rodadas[row[0]] = {'dtRodada':row[1], 'dtReferente':row[2], 'fonte':row[3], 'comentario':row[4]}
		dicionarioFonte[row[0]] = row[3]

	sql = '''SELECT
				VL_ANO,
				VL_PLD_MIM,
				VL_PLD_MAX
			FROM
				TB_PLD
			WHERE
				VL_ANO = {};'''.format(data.strftime('%Y'))
	answer = datab.requestServer(sql)

	pld_min = answer[0][1]
	pld_max = answer[0][2]

	idRodadas = list(rodadas)

	sql = '''SELECT
					ID_RODADA,
					CD_SUBMERCADO,
					HORARIO,
					VL_PRECO
				FROM
					TB_RESULTADOS_DESSEM
				WHERE 
					ID_RODADA IN ({})'''.format(str(idRodadas)[1:-1])

	answer = datab.requestServer(sql)

	precos = pd.DataFrame(answer, columns=['ID_RODADA', 'SUBMERCADO', 'HORARIO', 'CMO'])
	precos['SUBMERCADO'] = precos['SUBMERCADO'].replace(idSubmercados)
	precos['HORARIO'] = precos['HORARIO'].astype(str).str[0:5]

	precos['FONTE'] = precos['ID_RODADA'].replace(dicionarioFonte)

	precos['PLD'] = precos['CMO'] 
	precos.loc[precos['PLD'] < pld_min, 'PLD'] = pld_min
	precos.loc[precos['PLD'] > pld_max, 'PLD'] = pld_max

	mmm = precos.groupby(['FONTE', 'SUBMERCADO']).agg({'PLD': ['mean', 'min', 'max']})

	pld_diario_max = 684.73
	for fonte in precos['FONTE'].unique():
		for sub in precos['SUBMERCADO'].unique():
			media = mmm.loc[fonte, 'PLD'].loc[sub, 'mean']
			if media > pld_diario_max:
				fator = media/pld_diario_max
				precos.loc[(precos['SUBMERCADO']==sub) & (precos['FONTE']==fonte), 'PLD'] = precos.loc[(precos['SUBMERCADO']==sub) & (precos['FONTE']==fonte),'PLD']/fator

	mmm = precos.groupby(['FONTE', 'SUBMERCADO']).agg({'PLD': ['mean', 'min', 'max']})
	mmm = mmm.reset_index()
	mmm = mmm.sort_values('SUBMERCADO', ascending=False)

	
	fig = plt.figure(figsize=(20, 10))
	gs = gridspec.GridSpec(2, 3)
	
	fig.suptitle('CMO horário ({})'.format(data.strftime('%d/%m/%Y')), fontsize=16)

	table_image = [['Submercado', 'Fonte', 'PLD']]
	tabela = []
	for index, row in mmm.iterrows():
		valMedia = ('R${:.2f}'.format(row['PLD']['mean'])).replace('.', ',')
		valMin = ('R${:.2f}'.format(row['PLD']['min'])).replace('.', ',')
		valMax = ('R${:.2f}'.format(row['PLD']['max'])).replace('.', ',')
		linhaTabela = [row.values[1], row.values[0], valMedia, valMin, valMax]

		table_image.append([row.values[1], row.values[0], valMedia])
		tabela.append(linhaTabela)


	ax = plt.subplot(gs[:, 2])
	ax.axis('off')
	table = ax.table(cellText=table_image, loc='upper center')

	table.set_fontsize(12)
	table.scale(0.7, 1.3)

	corpoEmail += '<p>Média diária</p>'
	header = ['Submercado', 'Fonte', 'Média', 'Mínima', 'Máxima']
	corpoEmail += wx_emailSender.gerarTabela(tabela, header, nthColor=len(idRodadas))

	precos = pd.pivot_table(precos, index=['HORARIO', 'FONTE'], columns='SUBMERCADO', values='CMO', aggfunc='first')

	precos = precos[siglaSubmercados]
	precos = precos.reset_index()

	
	cores = {'ccee':'blue', 'ons':'coral', 'wx_p':'cadetblue', 'wx_d':'darkslategray','ons_convertido':'cadetblue'}
	ordem = {'ccee':4, 'ons':1, 'wx_p':2, 'wx_d':3, 'ons_convertido':5}

	axes_list = [plt.subplot(gs[0,0]),plt.subplot(gs[0,1]), plt.subplot(gs[1,0]) ,plt.subplot(gs[1,1])]

	for i, sub in enumerate(siglaSubmercados):
		
		ax =axes_list[i]
		ax.set_title(sub)
		
		for index, simulacao in precos.groupby('FONTE').__iter__():
			x = simulacao['HORARIO']
			y = simulacao[sub]



			ax.plot(x, y, label=index, color=cores[index], zorder=ordem[index])

			x_media = [y.mean()]*len(x)
			media = ax.plot(x, x_media, color=cores[index], linestyle='--', linewidth=.7)
			autolabelMediaPld(ax, media[0], 0, size=8, cor=cores[index])
			

		ax.legend()
		ax.tick_params(axis='x', labelrotation=-40)
		ax.set_ylabel('Preço [R$]')
		ax.grid(color = 'gray', linestyle = '--', linewidth = 0.2)

	pathFileOut = os.path.join(pathArquivos, 'tmp', 'CMO_HORARIO.PNG')
	plt.subplots_adjust(hspace=0.5, wspace=0.2)
	plt.savefig(pathFileOut)

	tabela = []
	for index, row in precos.iterrows():
		linhaTabela = [row['HORARIO'], row['FONTE']]
		for sub in ['SE', 'S', 'NE', 'N']:
			linhaTabela.append(('R${:.2f}'.format(row[sub])).replace('.', ','))
		tabela.append(linhaTabela)

	

	corpoEmail += '<p>Preço horário</p>'
	header = ['Horario', 'Fonte', 'Sudeste', 'Sul', 'Nordeste', 'Norte']
	corpoEmail += wx_emailSender.gerarTabela(tabela, header, nthColor=len(idRodadas))

	return corpoEmail, pathFileOut

def gerarPrevisaoVazaoDs(data):
	idSubmercados = {1:'Sudeste', 2:'Sul', 3:'Nordeste', 4:'Norte'}

	datab = wx_dbLib.WxDataB(DBM='mysqlWx')
	datab.changeDatabase('climenergy')
	dtFormat = datab.getDateFormat()

	sql = '''SELECT
				CD_SUBMERCADO,
				DT_PREVISAO,
				DT_REFERENTE,
				VL_VAZAO,
				VL_PERC_MLT
			FROM
				TB_PREV_VAZAO_DS
			WHERE DT_PREVISAO = \'{}\';'''.format(data.strftime(dtFormat))
	answer = datab.requestServer(sql)

	previsaoVazao = pd.DataFrame(answer, columns=['CD_SUBMERCADO', 'DT_PREVISAO', 'DT_REFERENTE', 'VL_VAZAO', 'VL_PERC_MLT'])
	previsaoVazao['SUBMERCADO'] = previsaoVazao['CD_SUBMERCADO'].replace(idSubmercados)
	previsaoVazao['DT_REFERENTE'] = pd.to_datetime(previsaoVazao['DT_REFERENTE'] , format="%Y-%m-%d")
	previsaoVazao['VL_PERC_MLT'] = previsaoVazao['VL_PERC_MLT'].round(1)

	header = []
	tabela = []
	for sub in previsaoVazao['SUBMERCADO'].unique():
		filtro1 = previsaoVazao['SUBMERCADO'] == sub
		if header == []:
			header = ['Submercado']
			header += previsaoVazao.loc[filtro1,'DT_REFERENTE'].dt.strftime('%d/%m/%Y').to_list()

		linha = previsaoVazao.loc[filtro1,'VL_VAZAO'].to_list()
		tabela.append([sub] + linha)
		linha = previsaoVazao.loc[filtro1,'VL_PERC_MLT'].to_list()
		tabela.append(["<small>% MLT</small>"] + linha)

	tabela = wx_emailSender.gerarTabela(tabela, header, nthColor=2)
	
	texto = "<b>Relatório de Previsão de Vazões Diárias</b><br>"
	texto = texto + "ENAS Diárias ONS - DESSEM<br><br>"

	texto = texto + tabela
	texto = texto + "<br><br>Envio por WX"

	return texto

def autolabel(ax, linhas, pontoInicio=0, size=6):
	"""Attach a text label above each bar in *rects*, displaying its height."""
	eixo_x = linhas.get_data()[0]
	eixo_y = linhas.get_data()[1]

	for i, x in enumerate(eixo_x[:-1]):
		y = eixo_y[i] # altura sera a vazao
		ax.annotate('{}'.format(int(round(y))),
					xy=(pontoInicio + datetime.timedelta(days=i*7 + 4), y),
					xytext=(0, 2),  # 3 points vertical offset
					textcoords="offset points",
					ha='center', va='bottom', size=size)

def autolabelAcomph(ax, linhas, cor='black'):
	"""Attach a text label above each bar in *rects*, displaying its height."""
	eixo_x = linhas.get_data()[0]
	eixo_y = linhas.get_data()[1]

	for i, x in enumerate(eixo_x):
		if (i+1)%3 != 0:
			continue
		y = eixo_y[i] # altura sera a vazao
		ax.annotate('{}'.format(int(round(y))),
					xy=(x, y),
					xytext=(0, 2),  # 3 points vertical offset
					textcoords="offset points",
					ha='center', va='bottom', size=10, color=cor)

def autolabelMlt(ax, linhas, cor='black'):
	"""Attach a text label above each bar in *rects*, displaying its height."""
	eixo_x = linhas.get_data()[0]
	eixo_y = linhas.get_data()[1]

	for i, x in enumerate(eixo_x[:-1]):
		y = eixo_y[i] # altura sera a vazao
		localizacao = eixo_x[i] + (eixo_x[i+1] - eixo_x[i])/2
		ax.annotate('{}'.format(int(round(y))),
					xy=(localizacao, y),
					xytext=(0, 2),  # 3 points vertical offset
					textcoords="offset points",
					ha='center', va='bottom', size=10, color=cor)

def autolabelMedia(ax, linhas, percentuais, cor='black'):
	"""Attach a text label above each bar in *rects*, displaying its height."""
	eixo_x = linhas.get_data()[0]
	eixo_y = linhas.get_data()[1]

	for i, x in enumerate(eixo_x[:-1]):
		y = eixo_y[i] # altura sera a vazao
		localizacao = eixo_x[i] + (eixo_x[i+1] - eixo_x[i])/2
		ax.annotate('{} ({}%)'.format(int(round(y)), int(round(percentuais[i]))),
					xy=(localizacao, y),
					xytext=(0, 2),  # 3 points vertical offset
					textcoords="offset points",
					ha='center', va='bottom', size=11, color=cor)

def gerarGraficoAcomph(dt):

	pathLocal = os.path.abspath('.')
	dir_saida = os.path.join(pathLocal, 'arquivos', 'tmp')
	
	if not os.path.exists(dir_saida):
		os.makedirs(dir_saida)

	iMesAtual = (dt-datetime.timedelta(days=1)).replace(day=1)
	fMesAnterior = iMesAtual-datetime.timedelta(days=1)
	iMesAnterior = fMesAnterior.replace(day=1)

	db_clime = wx_dbLib.DadosClime()
	# Pega o ultimo valor no banco de cada dia desde o inicio do mes anterior
	acomph = db_clime.getAcomph(dtInicio=iMesAnterior)  
	df_acomph = pd.DataFrame(acomph, columns=['CD_POSTO', 'DT_REFERENTE', 'VL_VAZ_INC_CONSO', 'VL_VAZ_NAT_CONSO', 'DT_ACOMPH', 'ROW'])
	df_acomph_vazNat = df_acomph.pivot(index='CD_POSTO', columns='DT_REFERENTE', values='VL_VAZ_NAT_CONSO')
	df_acomph_vazNat = wx_postosAux.calcPostosArtificiais_df(df_acomph_vazNat, ignorar_erros=True)
	df_acomph_ena = wx_postosAux.gera_ena_df(df_acomph_vazNat)

	df_acomph_ena = df_acomph_ena.T
	ultimosTrintaDias = df_acomph_ena.loc[(dt-datetime.timedelta(days=30)).strftime('%Y-%m-%d'):]

	mediaMesAnterior = df_acomph_ena.loc[iMesAnterior.strftime('%Y-%m-%d'):fMesAnterior.strftime('%Y-%m-%d')].mean()
	mediaMesAtual = df_acomph_ena.loc[iMesAtual.strftime('%Y-%m-%d'):].mean()

	media = pd.DataFrame()
	media[dt-datetime.timedelta(days=31)] = mediaMesAnterior
	media[iMesAtual] = mediaMesAtual
	media[max(df_acomph_ena.index)] = np.nan

	mlt = db_clime.getMlt()
	df_mlt = pd.DataFrame(mlt, columns=['SUBMERCADO', 'MES', 'MLT'])
	# mesesMlt = list(ultimosTrintaDias.index.to_series().dt.month.unique())
	dtsMlt = [dt-datetime.timedelta(days=31), iMesAtual]
	mesesMlt = [d.month for d in dtsMlt]
	df_mlt = (df_mlt[df_mlt['MES'].isin(mesesMlt)]).copy()
	df_mlt['DT_REFERENTE'] = df_mlt['MES'].replace(dict(zip(mesesMlt, dtsMlt)))
	df_mlt['SUBMERCADO'].replace({1:'N', 2:'NE', 3:'SE', 4:'S'}, inplace=True)
	df_mlt = df_mlt.pivot(index='SUBMERCADO', columns='DT_REFERENTE', values='MLT')
	if max(df_acomph_ena.index) != iMesAtual:
		df_mlt[max(df_acomph_ena.index)] = np.nan


	# A data referente do Acomph e d-1. Para pegar o acomph do dia anterior e necessario pegar do d-2
	acomphDiaAnterior = db_clime.getAcomphEspecifico(dtAcomph=dt-datetime.timedelta(days=2))
	df_acomphDiaAnterior = pd.DataFrame(acomphDiaAnterior, columns=['DT_REFERENTE', 'CD_POSTO', 'VL_EAR_LIDO', 'VL_EAR_CONSO', 'VL_VAZ_DEF_LIDO', 'VL_VAZ_DEF_CONSO', 'VL_VAZ_AFL_LIDO', 'VL_VAZ_AFL_CONSO', 'VL_VAZ_INC_CONSO', 'VL_VAZ_NAT_CONSO', 'DT_ACOMPH'])
	df_acomphDiaAnterior_vazNat = df_acomphDiaAnterior.pivot(index='CD_POSTO', columns='DT_REFERENTE', values='VL_VAZ_NAT_CONSO')
	df_acomphDiaAnterior_vazNat = wx_postosAux.calcPostosArtificiais_df(df_acomphDiaAnterior_vazNat, ignorar_erros=True)
	df_acomphDiaAnterior_ena = wx_postosAux.gera_ena_df(df_acomphDiaAnterior_vazNat)
	df_acomphDiaAnterior_ena = df_acomphDiaAnterior_ena.T

	tituloGrafico = 'Acomph ({})'.format(dt.strftime('%d/%m/%Y'))
	fig, ax =  plt.subplots(2,2)
	plt.gcf().set_size_inches(20, 10)
	plt.rcParams['axes.grid'] = True
	fig.suptitle('{}'.format(tituloGrafico), fontsize=16)

	submercados = ['SE', 'S', 'NE', 'N']
	for i, sub in enumerate(submercados):
		ax[int(i/2), i%2].set_title(sub)
		
		curvaAcomph = ax[int(i/2), i%2].plot(ultimosTrintaDias[sub].index, ultimosTrintaDias[sub].values, label="ACOMPH", color="darkturquoise", zorder=3)
		autolabelAcomph(ax[int(i/2), i%2], curvaAcomph[0], cor="darkturquoise")

		curvaAcomphAnterior = ax[int(i/2), i%2].plot(df_acomphDiaAnterior_ena[sub].index, df_acomphDiaAnterior_ena[sub].values, label="ACOMPH (Ant)", color="slategrey", linestyle = '--', zorder=2)

		curvaMediaMensal = ax[int(i/2), i%2].step(media.loc[sub].index, media.loc[sub].values, where='post', label="Média mensal", color="midnightblue", linestyle = '--', linewidth = 1, zorder=4)
		percentuais = (media.loc[sub].values/df_mlt.loc[sub].values)*100
		autolabelMedia(ax[int(i/2), i%2], curvaMediaMensal[0], percentuais, cor="midnightblue")

		curvaMlt = ax[int(i/2), i%2].step(df_mlt.loc[sub].index, df_mlt.loc[sub].values, where='post', label="MLT", color="black", linewidth = .5, zorder=1)
		autolabelMlt(ax[int(i/2), i%2], curvaMlt[0])

		ax[int(i/2), i%2].legend()
		ax[int(i/2), i%2].tick_params(axis='x', labelrotation=-20)
		ax[int(i/2), i%2].set_ylabel('ENA (MW)')
		ax[int(i/2), i%2].grid(color = 'gray', linestyle = '--', linewidth = 0.2)

	pathFileOut = os.path.join(dir_saida, 'ACOMPH.PNG')
	plt.savefig(pathFileOut)
	print(pathFileOut)
	return pathFileOut

def gerarTabelasEmailAcomph(data):

	# corHexSubida = '#24c433'
	# corHexDecida = '#d44425'

	corHexSubida = 'blue'
	corHexDecida = 'red'

	numDiasNaLinha = 7
	submercados = {'SE':'SUDESTE','S':'SUL','NE':'NORDESTE','N':'NORTE'}
	bacias = ['GRANDE','PARANAÍBA','TIETÊ','PARANAPANEMA','ALTO PARANÁ','BAIXO PARANÁ','ALTO TIETÊ','PARAÍBA DO SUL']
	bacias += ['ITABAPOANA','MUCURI','DOCE','PARAGUAI','IGUAÇU','JACUÍ','URUGUAI','CAPIVARI','ITAJAÍ-AÇU']
	bacias += ['SÃO FRANCISCO','PARNAÍBA','PARAGUAÇU','JEQUITINHONHA','TOCANTINS','AMAZONAS','ARAGUARI','XINGU']

	iMesAtual = (data-datetime.timedelta(days=1)).replace(day=1)
	fMesAnterior = iMesAtual-datetime.timedelta(days=1)
	iMesAnterior = fMesAnterior.replace(day=1)

	db_clime = wx_dbLib.DadosClime()
	# Pega o ultimo valor no banco de cada dia desde o inicio do mes anterior
	acomph = db_clime.getAcomph(dtInicio=iMesAnterior)  
	df_acomph = pd.DataFrame(acomph, columns=['CD_POSTO', 'DT_REFERENTE', 'VL_VAZ_INC_CONSO', 'VL_VAZ_NAT_CONSO', 'DT_ACOMPH', 'ROW'])
	df_acomph_vazNat = df_acomph.pivot(index='CD_POSTO', columns='DT_REFERENTE', values='VL_VAZ_NAT_CONSO')
	df_acomph_vazNat = wx_postosAux.calcPostosArtificiais_df(df_acomph_vazNat, ignorar_erros=True)
	
	df_acomph_ena_sub = wx_postosAux.gera_ena_df(df_acomph_vazNat)
	df_acomph_ena_sub = df_acomph_ena_sub.T
	enaSubmercados = df_acomph_ena_sub.loc[(data-datetime.timedelta(days=numDiasNaLinha)).strftime('%Y-%m-%d'):].copy()

	mediaMesAnteriorSubm = df_acomph_ena_sub.loc[iMesAnterior.strftime('%Y-%m-%d'):fMesAnterior.strftime('%Y-%m-%d')].mean()
	mediaMesAtualSubm = df_acomph_ena_sub.loc[iMesAtual.strftime('%Y-%m-%d'):].mean()

	# A data referente do Acomph e d-1. Para pegar o acomph do dia anterior e necessario pegar do d-2
	acomphDiaAnterior = db_clime.getAcomphEspecifico(dtAcomph=data-datetime.timedelta(days=2))
	df_acomphDiaAnterior = pd.DataFrame(acomphDiaAnterior, columns=['DT_REFERENTE', 'CD_POSTO', 'VL_EAR_LIDO', 'VL_EAR_CONSO', 'VL_VAZ_DEF_LIDO', 'VL_VAZ_DEF_CONSO', 'VL_VAZ_AFL_LIDO', 'VL_VAZ_AFL_CONSO', 'VL_VAZ_INC_CONSO', 'VL_VAZ_NAT_CONSO', 'DT_ACOMPH'])
	df_acomphDiaAnterior_vazNat = df_acomphDiaAnterior.pivot(index='CD_POSTO', columns='DT_REFERENTE', values='VL_VAZ_NAT_CONSO')
	df_acomphDiaAnterior_vazNat = wx_postosAux.calcPostosArtificiais_df(df_acomphDiaAnterior_vazNat, ignorar_erros=True)
	
	df_acomph_ena_diaAnterior_sub = wx_postosAux.gera_ena_df(df_acomphDiaAnterior_vazNat)
	df_acomph_ena_diaAnterior_sub = df_acomph_ena_diaAnterior_sub.T
	enaSubmercadosDiaAnterior = df_acomph_ena_diaAnterior_sub.loc[(data-datetime.timedelta(days=numDiasNaLinha)).strftime('%Y-%m-%d'):].copy()

	difEnaSubmercadoDiaAnterior = (enaSubmercados - enaSubmercadosDiaAnterior).dropna()

	enaSubmercadoFormatado = pd.DataFrame()
	diferencaSubmercadoEnaFormatado = pd.DataFrame()
	for siglaSub in submercados:
		enaSubmercadoFormatado = enaSubmercadoFormatado.append(enaSubmercados[siglaSub])
		diferencaSubmercadoEnaFormatado = diferencaSubmercadoEnaFormatado.append(difEnaSubmercadoDiaAnterior[siglaSub])


	# Bacias
	df_acomph_ena_bacia = wx_postosAux.gera_ena_df(df_acomph_vazNat, divisao='bacia')
	df_acomph_ena_bacia = df_acomph_ena_bacia.T
	enaBacias = df_acomph_ena_bacia.loc[(data-datetime.timedelta(days=numDiasNaLinha)).strftime('%Y-%m-%d'):].copy()
	
	df_acomphDiaAnterior_ena = wx_postosAux.gera_ena_df(df_acomphDiaAnterior_vazNat, divisao='bacia')
	df_acomph_ena_bacia = df_acomphDiaAnterior_ena.T
	enaBaciasDiaAnterior = df_acomph_ena_bacia.loc[(data-datetime.timedelta(days=numDiasNaLinha)).strftime('%Y-%m-%d'):].copy()

	mediaMesAnteriorBacias = df_acomph_ena_bacia.loc[iMesAnterior.strftime('%Y-%m-%d'):fMesAnterior.strftime('%Y-%m-%d')].mean()
	mediaMesAtualBacias = df_acomph_ena_bacia.loc[iMesAtual.strftime('%Y-%m-%d'):].mean()

	diferencaEnaBaciasDiaAnterior = (enaBacias - enaBaciasDiaAnterior).dropna()

	enaBaciasFormatado = pd.DataFrame()
	diferencaBaciasEnaFormatado = pd.DataFrame()
	for bac in bacias:
		if bac == 'JEQUITINHONHA':
			mediaMesAnteriorBacias[bac] = mediaMesAnteriorBacias['JEQUITINHONHA (SE)'] + mediaMesAnteriorBacias['JEQUITINHONHA (NE)']
			mediaMesAtualBacias[bac] = mediaMesAtualBacias['JEQUITINHONHA (SE)'] + mediaMesAtualBacias['JEQUITINHONHA (NE)']
			
			enaBac = enaBacias['JEQUITINHONHA (SE)'] + enaBacias['JEQUITINHONHA (NE)']
			difEnaBac = diferencaEnaBaciasDiaAnterior['JEQUITINHONHA (SE)'] + diferencaEnaBaciasDiaAnterior['JEQUITINHONHA (NE)']

		elif bac == 'SÃO FRANCISCO':
			mediaMesAnteriorBacias[bac] = mediaMesAnteriorBacias['SÃO FRANCISCO (SE)'] + mediaMesAnteriorBacias['SÃO FRANCISCO (NE)']
			mediaMesAtualBacias[bac] = mediaMesAtualBacias['SÃO FRANCISCO (SE)'] + mediaMesAtualBacias['SÃO FRANCISCO (NE)']
			enaBac = enaBacias['SÃO FRANCISCO (SE)'] + enaBacias['SÃO FRANCISCO (NE)']
			difEnaBac = diferencaEnaBaciasDiaAnterior['SÃO FRANCISCO (SE)'] + diferencaEnaBaciasDiaAnterior['SÃO FRANCISCO (NE)']

		elif bac == 'TOCANTINS':
			mediaMesAnteriorBacias[bac] = mediaMesAnteriorBacias['TOCANTINS (N)'] + mediaMesAnteriorBacias['TOCANTINS (SE)']
			mediaMesAtualBacias[bac] = mediaMesAtualBacias['TOCANTINS (N)'] + mediaMesAtualBacias['TOCANTINS (SE)']
			enaBac = enaBacias['TOCANTINS (N)'] + enaBacias['TOCANTINS (SE)']
			difEnaBac = diferencaEnaBaciasDiaAnterior['TOCANTINS (N)'] + diferencaEnaBaciasDiaAnterior['TOCANTINS (SE)']

		elif bac == 'AMAZONAS':
			mediaMesAnteriorBacias[bac] = mediaMesAnteriorBacias['AMAZONAS (N)'] + mediaMesAnteriorBacias['AMAZONAS (SE)']
			mediaMesAtualBacias[bac] = mediaMesAtualBacias['AMAZONAS (N)'] + mediaMesAtualBacias['AMAZONAS (SE)']
			enaBac = enaBacias['AMAZONAS (N)'] + enaBacias['AMAZONAS (SE)']
			difEnaBac = diferencaEnaBaciasDiaAnterior['AMAZONAS (N)'] + diferencaEnaBaciasDiaAnterior['AMAZONAS (SE)']

		else:
			enaBac = enaBacias[bac]
			difEnaBac = diferencaEnaBaciasDiaAnterior[bac]

		enaBac.name = bac
		difEnaBac.name = bac

		enaBaciasFormatado = enaBaciasFormatado.append(enaBac)
		diferencaBaciasEnaFormatado = diferencaBaciasEnaFormatado.append(difEnaBac)

	mlt = db_clime.getMlt()
	df_mlt = pd.DataFrame(mlt, columns=['SUBMERCADO', 'MES', 'MLT'])
	mesesMlt = [iMesAnterior.month, iMesAtual.month]
	df_mlt = (df_mlt[df_mlt['MES'].isin(mesesMlt)]).copy()
	df_mlt['SUBMERCADO'].replace({1:'N', 2:'NE', 3:'SE', 4:'S'}, inplace=True)
	df_mlt = df_mlt.pivot(index='SUBMERCADO', columns='MES', values='MLT')

	mltBaciasPath = os.path.join(pathArquivos, 'ENAS_MLT_bacias.txt')
	mltBacias = pd.read_csv(mltBaciasPath, sep='\t', index_col='BACIA')
	mltBacias.columns = mltBacias.columns.astype(int)

	inicioSemanaEletrica = data
	while inicioSemanaEletrica.weekday() != 5:
		inicioSemanaEletrica = inicioSemanaEletrica-datetime.timedelta(days=1)

	tabelaEna = []
	for index, enaSub in enaSubmercadoFormatado.iterrows():
		enaAux = []
		for i, dt in enumerate(enaSub.index):
			if i == 0:
				enaAux.append('{}<br><small>{:.0f}% | {:.0f}%</small>'.format(submercados[index], 100*(mediaMesAnteriorSubm[index]/df_mlt.loc[index, iMesAnterior.month]), 100*(mediaMesAtualSubm[index]/df_mlt.loc[index, iMesAtual.month])))
				cor = corHexSubida
			elif enaSub.iloc[i-1] < enaSub.iloc[i]:
				cor = corHexSubida
			else:
				cor = corHexDecida

			enaAux.append('<span style="color: {}">{:.0f}<br><small>{:.0f}%</small></span>'.format(cor, enaSub[dt], 100*(enaSub[dt]/df_mlt.loc[index, dt.month])))
		
		enaAux.append('<span style="color: {}">{:.0f}<br><small>{:.0f}%</small></span>'.format("black", enaSub[inicioSemanaEletrica:].mean(), 100*(enaSub[inicioSemanaEletrica:].mean()/df_mlt.loc[index, dt.month])))
		tabelaEna.append(enaAux)

	html = "<h4>Submercado</h4>"
	header = ['Submercado <br><small>mês anter | mês atual</small>']+list(enaSubmercadoFormatado.columns.strftime('%d/%m/%Y')) + ['<span style="font-weight: bold;">Média</span><br><smal>(semana)</smal>']
	tamanhoColunas = [85] + [61 for i in range(len(header))]
	html += wx_emailSender.gerarTabela(body=tabelaEna, header=header, widthColunas=tamanhoColunas)

	
	tabelaEnaBacia = []
	for index, enaBac in enaBaciasFormatado.iterrows():
		enaAux = []
		for i, dt in enumerate(enaBac.index):
			if i == 0:
				enaAux.append('{}<br><small>{:.0f}% | {:.0f}%</small>'.format(index, 100*(mediaMesAnteriorBacias[index]/mltBacias.loc[index, iMesAnterior.month]), 100*(mediaMesAtualBacias[index]/mltBacias.loc[index, iMesAtual.month])))
				cor = corHexSubida
			elif enaBac.iloc[i-1] < enaBac.iloc[i]:
				cor = corHexSubida
			else:
				cor = corHexDecida

			enaAux.append('<span style="color: {}">{:.0f}<br><small>{:.0f}%</small></span>'.format(cor, enaBac[dt], 100*(enaBac[dt]/mltBacias.loc[index, dt.month])))
		
		enaAux.append('<span style="color: {}">{:.0f}<br><small>{:.0f}%</small></span>'.format("black", enaBac[inicioSemanaEletrica:].mean(), 100*(enaBac[inicioSemanaEletrica:].mean()/mltBacias.loc[index, dt.month])))
		tabelaEnaBacia.append(enaAux)

	html += "<h4>Bacias</h4>"
	header = ['Bacia <br><small>mês anter | mês atual</small>']+list(enaBaciasFormatado.columns.strftime('%d/%m/%Y')) + ['<span style="font-weight: bold;">Média</span><br><smal>(semana)</smal>']
	tamanhoColunas = [85] + [61 for i in range(len(header))]
	html += wx_emailSender.gerarTabela(body=tabelaEnaBacia, header=header, widthColunas=tamanhoColunas)


	tabelaDifEnaSub = []
	for index, difEnaSub in diferencaSubmercadoEnaFormatado.iterrows():
		enaAux = []
		for i, dt in enumerate(difEnaSub.index):
			if i == 0:
				enaAux.append('{}'.format(submercados[index]))
			if difEnaSub.iloc[i] >= 0:
				cor = corHexSubida
			else:
				cor = corHexDecida
			
			enaAux.append('<span style="color: {}">{:.0f}<br></span>'.format(cor, difEnaSub[dt]))
		tabelaDifEnaSub.append(enaAux)

	header = ['Submercado']+list(diferencaSubmercadoEnaFormatado.columns.strftime('%d/%m/%Y'))
	html += "<h3>Diferenças ACOMPH do dia {} com o acomph do dia {}</h3>".format(data.strftime('%d/%m/%Y'), (data-datetime.timedelta(days=1)).strftime('%d/%m/%Y'))
	html += "<h4>Submercados</h4>"
	html += wx_emailSender.gerarTabela(body=tabelaDifEnaSub, header=header)

	tabelaDifEnaBac = []
	for index, difEnaBac in diferencaBaciasEnaFormatado.iterrows():
		enaAux = []
		for i, dt in enumerate(difEnaBac.index):
			if i == 0:
				enaAux.append('{}'.format(index))
			if difEnaBac.iloc[i] >= 0:
				cor = corHexSubida
			else:
				cor = corHexDecida
			
			enaAux.append('<span style="color: {}">{:.0f}<br></span>'.format(cor, difEnaBac[dt]))
		tabelaDifEnaBac.append(enaAux)

	header = ['Bacia']+list(diferencaBaciasEnaFormatado.columns.strftime('%d/%m/%Y'))
	html += "<h4>Bacias</h4>"
	html += wx_emailSender.gerarTabela(body=tabelaDifEnaBac, header=header)

	return html


def enviar(parametros):

	confgs = configs.getConfiguration()

	# Configuracoes default
	flagEmail = False
	flagWhats = False
	remententeEmail = ''
	destinatarioEmail = ''
	assuntoEmail = ''
	corpoEmail = ''
	file = ''
	passwordEmail = ''

	msgWhats = ''
	fileWhats = ''

	print(parametros)

	if 'destinatarioemail' in parametros:
		destinatarioEmail = parametros['destinatarioemail']

	if 'assuntoemail' in parametros:
		assuntoEmail = parametros['assuntoemail']

	if 'corpoemail' in parametros:
		corpoEmail = parametros['corpoemail']


	dt = datetime.datetime.now()
	print("Tentativa as {}".format(dt.strftime('%d/%m/%Y %H:%M:%S')))

	# Tratamento para o produto ACOMPH
	if parametros["produto"] == 'ACOMPH':

		acomphXls = parametros["path"]
		fileName = os.path.basename(acomphXls)
		data = datetime.datetime.strptime(fileName,'ACOMPH_%d.%m.%Y.xls')
		corpoEmail = gerarTabelasEmailAcomph(data)

		remententeEmail = 'acomph@climenergy.com'
		assuntoEmail = '[ACOMPH] Atualização do dia {}'.format(data.strftime('%d/%m/%Y'))
		flagEmail = True

		fileWhats = gerarGraficoAcomph(dt=data)
		confgs['whatsapp']['destinatario'] = 'PMO'
		msgWhats = 'Acomph ({})'.format(data.strftime('%d/%m/%Y'))
		flagWhats = True

		file = [fileWhats, acomphXls]
		

	elif parametros["produto"] == 'GRAFICO_ACOMPH':
		fileWhats = gerarGraficoAcomph(dt=parametros['data'])
		confgs['whatsapp']['destinatario'] = 'PMO'
		msgWhats = 'Acomph ({})'.format(parametros["data"].strftime('%d/%m/%Y'))
		flagWhats = True

	elif parametros["produto"] == 'RDH':
		# dtReferente = parametros["data"] - datetime.timedelta(days=1)
		dtReferente = parametros["data"]
		file = "{}/RDH_{}.xlsx".format(confgs["paths"]["arquivosHidrologia"], (dtReferente.strftime('%d%b%Y')).upper())
		file_lowerCase = "{}/rdh_{}.xlsx".format(confgs["paths"]["arquivosHidrologia"], (dtReferente.strftime('%d%b%Y')))
		# Verifica se a pasta existe
		if os.path.exists(file) or os.path.exists(file_lowerCase):
				if os.path.exists(file):
						file = file
				else:
						file = file_lowerCase

		remententeEmail = 'rdh@climenergy.com'
		assuntoEmail = '[RDH] Atualização do dia {}'.format(parametros["data"].strftime('%d/%m/%Y'))
		flagEmail = True

	elif parametros["produto"] == 'RELATORIO_BBCE':
		corpoEmail, file = geraRelatorioBbce(parametros["data"])
		assuntoEmail = '[BBCE] Resumo das negociações do dia {}'.format(parametros["data"].strftime('%d/%m/%Y'))
		remententeEmail = 'info_bbce@climenergy.com'
		flagEmail = True

	elif parametros["produto"] == 'RESULTADO_DESSEM':
		corpoEmail, file = gerarResultadoDessem(parametros["data"])

		assuntoEmail = '[DESSEM] Rodada DESSEM {}'.format(parametros["data"].strftime('%d/%m/%Y'))
		remententeEmail = 'dessem@climenergy.com'
		destinatarioEmail = ['middle@wxe.com.br','front@wxe.com.br']
		flagEmail = True

		msgWhats = 'CMO {}'.format(parametros["data"].strftime('%d/%m/%Y'))
		fileWhats = file
		confgs['whatsapp']['destinatario'] = 'RZ - DESSEM'
		flagWhats = True

	elif parametros["produto"] == 'PREVISAO_VAZAO_DESSEM':
		corpoEmail = gerarPrevisaoVazaoDs(parametros["data"])
		assuntoEmail = '[ENAS] DESSEM ONS - {}'.format((parametros["data"]+datetime.timedelta(days=2)).strftime('%d/%m/%Y'))
		remententeEmail = 'rev_ena@climenergy.com'
		destinatarioEmail = ['middle@wxe.com.br','front@wxe.com.br']
		flagEmail = True

	elif parametros["produto"] == 'PREVISAO_CARGA_DESSEM':
		pathFileOut = os.path.join(pathArquivos, 'tmp')
		
		file,flag_preliminar = wx_dbLib.carga_dessem_plot(parametros["data"],pathFileOut)
		if flag_preliminar:
			msgWhats = 'Carga Horária Preliminar {}'.format(parametros["data"].strftime('%d/%m/%Y'))
		else:
			msgWhats = 'Carga Horária {}'.format(parametros["data"].strftime('%d/%m/%Y'))

		fileWhats = file
		confgs['whatsapp']['destinatario'] = 'RZ - DESSEM'
		flagWhats = True

	elif parametros["produto"] == 'PROCESSOS_ANA':
		assuntoEmail = '[Processos] Processos ANA ({})'.format(dt.strftime('%d/%m/%Y %H:00'))
		corpoEmail = wx_consultaProcessos.verificarProcessosAna(dt=parametros['data'])
		
		if corpoEmail != '':
			remententeEmail = 'processos@climenergy.com'
			destinatarioEmail = ['middle@wxe.com.br']
			flagEmail = True


	if flagEmail:

		serv_email = wx_emailSender.WxEmail()
		if remententeEmail != '':
			serv_email.username = remententeEmail
   
   	# Produto com password diferente do default (climenergy@climenergy.com)
		if passwordEmail != '':
			serv_email.password = passwordEmail

		# Lista de destinatarios diferentes do default (['middle@wxe.com.br','front@wxe.com.br'])
		if destinatarioEmail != '':
			serv_email.send_to = destinatarioEmail

		# Cria um titulo para o email caso nao for configurado anteriormente com a data e o nome do produto
		if assuntoEmail == '':
			assuntoEmail='{} ({})'.format(parametros["produto"], parametros["data"].strftime('%d/%m/%Y'))

		# Cria um corpo de email caso nao for configurado anteriormente
		if corpoEmail == '':
			corpoEmail = '<h3>{} referente a data {}</h3><br><h4>{}</h4>'.format(parametros["produto"], parametros["data"].strftime('%d/%m/%Y'), file.split('/')[-1])
		try:
			if file != '' and file != []:
				if type(file) == type(''):
					serv_email.sendEmail(texto=corpoEmail, assunto=assuntoEmail, anexos=[file])
				elif type(file) == type([]):
					serv_email.sendEmail(texto=corpoEmail, assunto=assuntoEmail, anexos=file)
			else:
				serv_email.sendEmail(texto=corpoEmail, assunto=assuntoEmail, anexos=[])
		except Exception as e:
			print("\033[91mErro ao enviar email: {}\033[0m".format(e))
			print("\033[91mArquivo: {}\033[0m".format(file))
			print("\033[91mCorpo do email: {}\033[0m".format(corpoEmail))
			print("\033[91mAssunto do email: {}\033[0m".format(assuntoEmail))
			print("\033[91mDestinatario do email: {}\033[0m".format(destinatarioEmail))
	# Envia o produto via whatsapp caso a  flagWhats estiver ativada
	if flagWhats:
		if msgWhats == '':
			msg = '{} ({})'.format(parametros["produto"], parametros["data"].strftime('%d/%m/%Y'))
		else:
			msg = msgWhats
		cmd = 'cd {} && python whatsapp_bot.py inserirMsgFila dst "{}" msg "{}" file "{}"'.format(os.path.abspath(confgs['whatsapp']['scriptEnviaMsg']), confgs['whatsapp']['destinatario'], msg, fileWhats)
		os.system(cmd)

def printHelper():
	""" Imprime na tela o helper da funcao 
	:param None: 
	:return None: 
	"""
	data = datetime.datetime.now()
	dt_proxRv = (wx_opweek.getLastSaturday(data)+datetime.timedelta(days=7)).strftime('%d/%m/%Y')
	proxRv = wx_opweek.ElecData(wx_opweek.getLastSaturday(data)+datetime.timedelta(days=7))
	mesRef = datetime.date(data.month,proxRv.mesReferente,1).strftime('%B')
	mesRef = mesRef[0].upper()+mesRef[1:]

	path_plan_acomph_rdh = os.path.abspath('/WX2TB/Documentos/fontes/PMO/monitora_ONS/plan_acomph_rdh')
	pathArquivosTmp = os.path.abspath('/WX2TB/Documentos/fontes/outros/webhook/arquivos/tmp')

	file_revisaoCargas = 'RV{}_PMO_{}_{}_carga_semanal.zip'.format(proxRv.atualRevisao,mesRef,proxRv.anoReferente)
	
	fileProspec1 = os.path.abspath('/WX2TB/Documentos/fontes/PMO/API_Prospec/DownloadResults/DC202105_rv1_rv5_MapaEC_Ext_Dia.zip')
	fileProspec2 = os.path.abspath('/WX2TB/Documentos/fontes/PMO/API_Prospec/DownloadResults/DC202107_rv1_rv5_MapaEC_Ext_Dia.zip')
	filesProspec = '["{}","{}"]'.format(fileProspec1, fileProspec2)

	deck_decomp = os.path.abspath('/WX2TB/Documentos/fontes/PMO/converte_dc/input')

	exit()

def runWithParams():
	parametros = {}
	
	# Caso nenhum parametro for passado, e mostrado o helper
	if not len(sys.argv) > 1:
		printHelper()

	# Tratamento dos parametros 
	else:
		# Caso um dos parametros for pedido de help
		if 'help' in sys.argv or '--h' in sys.argv or '-help' in sys.argv:
			printHelper()

		# Data padrao e o dia atual
		parametros["data"] = datetime.datetime.now()
		parametros['considerarprevs'] = ''
		parametros['considerarrv']    = ''
		parametros['gerarmatriz']     = 0
		parametros['fazer_media'] 	  = 0
		parametros['enviar_whats']    = 0

		# Verificacao e substituicao dos valores defaults com 
		# os valores passados por parametro 
		for i in range(1, len(sys.argv)):
			argumento = sys.argv[i].lower()

			if argumento == "produto":
				parametros[argumento] = sys.argv[i+1].upper()

			elif argumento == "data":
				try:
					parametros[argumento] = datetime.datetime.strptime(sys.argv[i+1], '%d/%m/%Y')
				except:
					print("Data no formato errado {}, utiliza o formato 'dd/mm/yyyy'".format())
					quit()

			elif argumento == "modelo":
				parametros[argumento] = sys.argv[i+1]
    	
			elif argumento == "dtrev":
				parametros[argumento] = datetime.datetime.strptime(sys.argv[i+1], '%d/%m/%Y')

			elif argumento == "preliminar":
				parametros[argumento] = int(sys.argv[i+1])

			elif argumento == "pdp":
				parametros[argumento] = int(sys.argv[i+1])
    
			elif argumento == "psat":
				parametros[argumento] = int(sys.argv[i+1])

			elif argumento == "url":
				parametros[argumento] = sys.argv[i+1]

			elif argumento == "path":
				parametros[argumento] = sys.argv[i+1]


	enviar(parametros)


if __name__ == '__main__':
	runWithParams()
