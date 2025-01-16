# -*- coding: utf-8 -*-
import os
import re
import pdb
import sys
import json
import time
import glob
import locale
import shutil
import zipfile
import datetime
import requests
from bs4 import BeautifulSoup
import sqlalchemy as db
from dateutil import relativedelta
import aspose.words as aw
from PIL import Image

import pandas as pd
import numpy as np
import dataframe_image as dfi
import matplotlib.pyplot as plt
import tabulate

from PIL import Image
from pdf2image import convert_from_path
from selenium import webdriver
from pandas.plotting import register_matplotlib_converters
from matplotlib import gridspec
register_matplotlib_converters()

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")),'.env'))
__HOST_SERVER = os.getenv('HOST_SERVIDOR') 

try:
	locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
except:
	locale.setlocale(locale.LC_ALL, '')


diretorioApp = os.path.abspath('.')
appsDir = os.path.dirname(diretorioApp)
diretorioRaiz = os.path.dirname(appsDir)

pathLibUniversal = os.path.join(diretorioRaiz,'bibliotecas')
pathLibLocal = os.path.join(diretorioApp,'libs')
pathArquivos = os.path.join(diretorioApp,'arquivos')


pathRodadasApp = os.path.join(appsDir,'rodadas')
sys.path.insert(1,pathRodadasApp)
import rz_rodadasModelos


sys.path.insert(1, pathLibUniversal)
import wx_dbLib
import wx_postosAux
import wx_emailSender
import wx_verificaCorImg
import wx_opweek

sys.path.insert(1, "/WX2TB/Documentos/fontes/PMO/scripts_unificados/bibliotecas")
import wx_dbClass

sys.path.insert(1, pathLibLocal)
import configs
import wx_consultaProcessos
import wx_resultadosProspec
import wx_previsaoGeadaInmet
import rz_cargaPatamar
import rz_deck_dc_preliminar
import rz_relatorio_bbce
import rz_produtos_chuva


def baixarProduto(data, produto):
	""" Faz o download de produtos externos a WX
	:param data: [DATETIME] Data do produtop
	:param produto: [STR] Nome do produto a ser baixado
	:return filePath: [STR] Caminho completo do arquivo baixado
	"""
	import requests
	print("Tentativa de baixar o produto {} da data {}".format(produto, data.strftime('%d/%m/%Y')))
	# Pega as configuracoes do config.json
	confgs = configs.getConfiguration()

	pastaTmp = confgs['paths']['pastaTmp']

	if not os.path.exists(pastaTmp):
		os.makedirs(pastaTmp)

	flag_baixarProduto = False

	if produto == 'SST':

		# Adiciona o numero maximo de dias do menor mes (fevereiro) possivel em seguida adiciona 1 ate o mes ser o seguinte
		data_final = data + datetime.timedelta(days=28)
		while data_final.month == data.month:
			data_final += datetime.timedelta(days=1)

		url = 'https://www.cpc.ncep.noaa.gov/products/people/mchen/CFSv2FCST/monthly/images/CFSv2.SST.{}.{}.gif'.format(data.strftime('%Y%m%d'), data_final.strftime('%Y%m'))

		flag_baixarProduto = True

	if flag_baixarProduto:

		# 20 tentativas de baixar o produto
		for i in range(20):
			file = requests.get(url)
			if file.status_code == 200:
				filename = url.split('/')[-1]
				filePath = '{}/{}'.format(pastaTmp, filename)
				with open(filePath, 'wb') as f:
					f.write(file.content)

					print("\nURL: {}".format(url))
					print("Produto: {}".format(filename))
					print("Tipo: {}".format(file.headers['content-type']))
					print("Path: {}".format(filePath))
					return filePath
				break
			else:
				print("Nova tentiva em 5 minutos")
				time.sleep(60*15)
	return ''

def extractFiles(compiladoZipado):

	zipNameFile = os.path.basename(compiladoZipado)
	path = os.path.dirname(compiladoZipado)
	dstFolder = os.path.join(path, zipNameFile.split('.')[0])

	if os.path.exists(dstFolder):
		shutil.rmtree(dstFolder)

	if not os.path.exists(dstFolder):
		os.makedirs(dstFolder)

	with zipfile.ZipFile(compiladoZipado, 'r') as zip_ref:
		zip_ref.extractall(dstFolder)

	return dstFolder

def gerarTabelaWhats(header,valores,path):
	df = pd.DataFrame(valores)
	df.columns = header
	dfi.export(df,path)

def pdfToJpeg(pathFile, pageNumber, pathOut):
	""" Converte PDF para JPG
	:param pathFile: [str] Path do arquivo PDF a ser convertido
	:param pathOut: [str] Path do arquivo JPG final
	:param pageNumber: [int] Numero da pagina a ser convertida
	:return None: 
	"""
	pages = convert_from_path(pathFile, 100)
	pages[pageNumber-1].save(pathOut, 'JPEG')


def gerarCompiladoPrevPrecipt(data, modelo, rodada):

	confgs = configs.getConfiguration()

	pastaTmp = confgs['paths']['pastaTmp']

	if not os.path.exists(pastaTmp):
		os.makedirs(pastaTmp)

	dtRodada = '{}{:0>2}'.format(data.strftime('%Y%m%d'), rodada)

	path = os.path.abspath('/WX2TB/Documentos/saidas-modelos')
	
	sufixosDesconnsiderar = ["_pn.gif","_lim.gif","-periodo_ec9d.gif","_est.gif","_est_anom.gif"]

	if modelo in ['ecmwf-orig','ecmwf-ens']:
		pattern = os.path.join(path,modelo,dtRodada,'semana-energ','[0-9]semana_energ-*{}*'.format(dtRodada))
	elif modelo == 'cfsv2':
		pattern = os.path.join(path,modelo,data.strftime('%Y%m%d'),dtRodada,'semana-energ','[0-9]semana_energ-{}_ensemble*dias*{}'.format(modelo,data.strftime('%Y%m%d')))
		sufixosDesconnsiderar = ["_pn.gif","_lim.gif","-periodo_ec9d.gif","_est.gif","_est_anom.gif"]
	elif modelo == 'gefs':
		pattern = os.path.join(path,modelo,dtRodada,'semana-energ','[0-9]semana_energ-r{}*'.format(dtRodada))
		sufixosDesconnsiderar = ["_pn.gif","_lim.gif","-periodo_ec9d.gif","_est.gif","_est_anom.gif","_m[0-9]{2}-ext_smap.jpeg","_m[0-9]{2}_smap.jpeg"]
	elif modelo == 'gefs-eta':
			pattern = os.path.join(path,modelo,dtRodada,'semana-energ','[0-9]semana_energ-r{}_smap*'.format(dtRodada))
			pattern2 = os.path.join(path,modelo,dtRodada,'semana-energ','[0-9]semana_energ_DIF_PCONJUNTO-PCONJUNTO_{}{:0>2}*'.format(data.strftime('%d%m%y'),rodada))
	else:
		pattern = os.path.join(path,modelo,dtRodada,'semana-energ','[0-9]semana_energ-r{}*'.format(dtRodada))
		
	print('Formato:',pattern)

	files = []
	if modelo =='gefs-eta':
		files +=glob.glob(pattern+'.jpeg')
		files += glob.glob(pattern2+'.png')
	
	else:
		for extensao in ['.gif','.jpeg']:
			files += glob.glob(pattern+extensao)

	arquivosDesconsiderar = []
	for i,f in enumerate(files):

		for sufix in sufixosDesconnsiderar:
			if re.search(sufix, f):
				arquivosDesconsiderar.append(f)
				break
  
			if sufix in f:
				arquivosDesconsiderar.append(f)
				break

		if not wx_verificaCorImg.imagemColorida(f):
			arquivosDesconsiderar.append(f)
			continue

	files = [f for f in files if f not in arquivosDesconsiderar]

	files.sort()

	numImgs_v = 2
	numImgs_h = int(len(files)/numImgs_v)

	if len(files)%numImgs_v:
		numImgs_h += 1

	im = Image.open(files[0])
	imgWidth = im.width
	imgHeight = im.height

	dst = Image.new('RGB', (imgWidth * numImgs_v, imgHeight*numImgs_h))
	for i, img in enumerate(files):
		# print(img)
		col = int(i % numImgs_v)
		row = int(i / numImgs_v)
		im = Image.open(img)
		dst.paste(im, (imgWidth*col, imgHeight*row))


	arquivoSaida = '{}/{}_{}z.jpg'.format(pastaTmp, modelo, dtRodada)
	dst.save(arquivoSaida)
	print('Arquivo salvo em :\n{}'.format(arquivoSaida))

	return arquivoSaida

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
        requests.post(f"http://{__HOST_SERVER}:8000/api/v2/bbce/produtos-interesse", json=dados_a_inserir)
        
  

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


def gerarRelatoriosResultCv(urlInteresse):

	downloadDirectory = os.path.join(os.getcwd(), 'arquivos', 'tmp')
	if not os.path.exists(downloadDirectory):
		os.makedirs(downloadDirectory)

	chrome_options = webdriver.ChromeOptions()
	settings = {
		"recentDestinations": [{
			"id": "Save as PDF",
			"origin": "local",
			"account": ""
		}],
		"selectedDestinationId": "Save as PDF",
		"version": 2,
		"isHeaderFooterEnabled": False,
		"customMargins": {},
		"marginsType": 2,
	}
	prefs = {}

	prefs["printing.print_preview_sticky_settings.appState"] = json.dumps(settings)
	prefs["savefile.default_directory"] = downloadDirectory
	prefs["download.default_directory"] = str(downloadDirectory)
	prefs["download.prompt_for_download"] = False
	prefs["download.directory_upgrade"] = True
	chrome_options.add_experimental_option('prefs', prefs)
	chrome_options.add_argument('--kiosk-printing')

	selenium = webdriver.Chrome(options=chrome_options)

	url = 'http://35.173.154.94:8090'
	selenium.get(url)
	inp_username = selenium.find_element_by_id('username')
	inp_username.send_keys('thiago')
	inp_password = selenium.find_element_by_id('password')
	inp_password.send_keys('12345678')

	xpath_btn = '//button[@type="submit"]'
	btn = selenium.find_element_by_xpath(xpath_btn)
	btn.click()

	selenium.set_window_size(1920, 1080, selenium.window_handles[0])
	selenium.get(urlInteresse)

	tituloPagina = selenium.execute_script("return document.title")
	pathPdf = os.path.join(downloadDirectory, tituloPagina+'.pdf')
	if os.path.exists(pathPdf):
		os.remove(pathPdf) 

	for i in range(90):
		print(selenium.execute_script("return $.xhrPool.length"))
		if selenium.execute_script("return $.xhrPool.length") == 0:
			break
		time.sleep(2)

	selenium.execute_script('window.print();')
	
	time.sleep(5)
	selenium.quit()

	return pathPdf

def diffChuvaVazao(data):

	pathMiddle = os.path.abspath('/home/wxenergy/WX Energy Dropbox/WX - Middle')

	numLinhasTabela = 10

	pathPrecSaida = 'arquivos/diffPrec.csv'
	pathVazaoSaida = 'arquivos/diffVazao.csv'

	pathResultados = os.path.join(pathMiddle, 'NovoSMAP', data.strftime('%Y%m%d'), 'SMAP*')
	pathsResultados = glob.glob(pathResultados)

	diferencaPrec = pd.DataFrame()
	diferencaPrec['bacia'] = np.nan

	diferencaVazao = pd.DataFrame()
	diferencaVazao['bacia'] = np.nan

	for pathResult in pathsResultados:
		fonte = open(os.path.join(os.path.join(pathResult, 'fonte.txt'))).read()

		if fonte not in diferencaPrec.columns:
			diferencaPrec[fonte] = np.nan

		if fonte not in diferencaVazao.columns:
			diferencaVazao[fonte] = np.nan

		bacias = os.listdir(pathResult)
		bacias.remove('fonte.txt')

		for bacia in bacias:
			pathFilesBacia = os.path.join(pathResult, bacia)

			files = os.listdir(os.path.join(pathFilesBacia, 'ARQ_ENTRADA'))

			regexFilePrec = '0{0,1}PSAT.*_C.txt$' 
			filesPreciptacao = [f for f in files if re.match(regexFilePrec, f)]
			filesVazao = [f for f in files if f not in filesPreciptacao]

			for f in filesPreciptacao:

				df = pd.read_csv(os.path.join(pathFilesBacia, 'ARQ_ENTRADA', f), header=None, delimiter=' ', skipinitialspace=True, usecols=[0,1,2,3])
				df.columns = ['posto', 'data', 'horario', 'mm']

				row = df.loc[df['data']==data.strftime('%d/%m/%Y')]
				posto = row['posto'].item()
				preciptacaoAtual = row['mm'].item()

				if posto not in diferencaPrec.index:
					diferencaPrec.loc[posto] = {'bacia':bacia, fonte:preciptacaoAtual}
				else:
					diferencaPrec.loc[posto, fonte] = preciptacaoAtual

			for f in filesVazao:
				posto = f.replace('.txt','')
				df = pd.read_csv(os.path.join(pathFilesBacia, 'ARQ_ENTRADA', f), delimiter='|', header=None)

				row = df.loc[df[4]==(data-datetime.timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')]
				vazao = row[5].item()

				if posto not in diferencaVazao.index:
					diferencaVazao.loc[posto, 'bacia'] = bacia

				diferencaVazao.loc[posto, fonte] = vazao


	diferencaPrec.to_csv(pathPrecSaida)
	diferencaVazao.to_csv(pathVazaoSaida)

	if 'pdp' in diferencaPrec.columns:
		referencia = 'pdp'
		ordem_colunas = ['bacia', 'pdp', 'psat', 'gpm']

		diferencaPrec['psat'] = diferencaPrec[referencia]-diferencaPrec['psat']
		diferencaVazao['psat'] = diferencaVazao[referencia]-diferencaVazao['psat']
	else:
		referencia = 'psat'
		ordem_colunas = ['bacia', 'psat', 'gpm']

	diferencaPrec['gpm'] = diferencaPrec[referencia]-diferencaPrec['gpm']
	diferencaVazao['gpm'] = diferencaVazao[referencia]-diferencaVazao['gpm']

	diferencaPrec = diferencaPrec[ordem_colunas]
	diferencaVazao = diferencaVazao[ordem_colunas]

	texto = "<b><h3>Diferenças do chuva-vazão</h3></b><br/>".format(data.strftime('%d/%m/%Y'))
	texto += "<p><small>A coluna {} do corpo do email é referente a diferença relativa ao {} </small></p><br/>".format( ' e '.join(ordem_colunas[2:]), referencia)

	header = ['Posto', 'Bacia'] + diferencaPrec.columns[1:].to_list()

	menoresDifPrec = diferencaPrec['gpm'].abs().nlargest(numLinhasTabela).index
	menoresDifVaz = diferencaVazao['gpm'].abs().nlargest(numLinhasTabela).index

	texto += "Diferença na preciptação com referencia o {}:<br/>".format(referencia)
	listaMaioresPrec = []
	for idx in menoresDifPrec:
		row = diferencaPrec.loc[idx]
		linha = [idx]
		for col in diferencaPrec.columns:
			if col != 'bacia':
				linha.append(round(row[col], 1))
			else:
				linha.append(row[col])
		listaMaioresPrec.append(linha)
	texto += wx_emailSender.gerarTabela(listaMaioresPrec, header, nthColor=1)

	texto += "<br/>Diferença na vazão com referencia o {}:<br/>".format(referencia)
	listaMaioresVaz = []
	for idx in menoresDifVaz:
		row = diferencaVazao.loc[idx]
		linha = [idx]
		for col in diferencaVazao.columns:
			if col != 'bacia':
				linha.append(round(row[col], 1))
			else:
				linha.append(row[col])
		listaMaioresVaz.append(linha)
	texto += wx_emailSender.gerarTabela(listaMaioresVaz, header, nthColor=1)
	texto = texto + "<br><br>Envio por WX"
	return texto, [pathPrecSaida, pathVazaoSaida]


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

def gerarPlotPrevivaz(dt, modelo, hr_rodada, dt_rev, flagPrel, flagpdp, flagPsat):
  
	rv = wx_opweek.ElecData(dt_rev).atualRevisao

	# Leitura do prevs e calculo dos postos artificiais
	dtInicioAcomph = dt - datetime.timedelta(days=31)
	dtInicioAcomph = dtInicioAcomph.replace(day=1)

	pathLocal = os.path.abspath('.')
	dir_saida = os.path.join(pathLocal, 'arquivos', 'tmp')

	if not os.path.exists(dir_saida):
		os.makedirs(dir_saida)
	subTitulo = ""
	if int(flagPrel):
		subTitulo = " (PRE)"
	elif int(flagpdp):
		subTitulo = " (PDP)"
	elif int(flagPsat):
		subTitulo = " (PSAT)"
	
	# Formatacao do titulo do grafico
	tituloGrafico = "{} {} - RV{} ({})".format(modelo,subTitulo, rv, dt.strftime('%d/%m/%Y'))

	db_clime = wx_dbLib.DadosClime()

	# Rodada de interesse
	vazao = db_clime.getPrevsCv(data=dt, modelo=modelo, horario_rodada=hr_rodada, preliminar=flagPrel, dt_rev=dt_rev, pdp=flagpdp, psat=flagPsat)
	df_vazao = pd.DataFrame(vazao, columns=['CD_POSTO', 'VL_VAZAO', 'DT_REFERENTE', 'STR_MODELO', 'VL_REV'])
	df_vazao['DT_REFERENTE'] = pd.to_datetime(df_vazao['DT_REFERENTE'], format="%Y-%m-%d")
	df_vazao = df_vazao.pivot(index='CD_POSTO', columns='DT_REFERENTE', values='VL_VAZAO')
	df_vazao = wx_postosAux.calcPostosArtificiais_df(df_vazao, ignorar_erros=True)
	df_ena = wx_postosAux.gera_ena_df(df_vazao)

	rodadas_00z_dia = []
	if hr_rodada != 0:
		rodadas_00z_dia = db_clime.getCadastrosPrevsCv(data=dt, modelo=modelo, dt_rev=dt_rev, horario_rodada=0)
	rodadas_00z_dia_anterior = db_clime.getCadastrosPrevsCv(data=(dt-datetime.timedelta(days=1)), modelo=modelo, dt_rev=dt_rev, horario_rodada=0)

	rodadasComp = []
	if len(rodadas_00z_dia):
		rodadasComp += [rodadas_00z_dia[0]]
	if len(rodadas_00z_dia_anterior):
		rodadasComp += [rodadas_00z_dia_anterior[0]]

	df_enaComp = {}
	for r in rodadasComp:
		data = r[0]
		if r[0].weekday() == 4:
			data += datetime.timedelta(days =1)
		else:
			data = wx_opweek.getLastSaturday(data)
			
		data += datetime.timedelta(days=7)

		dt_eletrica = wx_opweek.ElecData(data)
		dt_rev = dt_eletrica.primeiroDiaMes + datetime.timedelta(days=7*dt_eletrica.atualRevisao)
		vazoesComparacao = db_clime.getPrevsCv(data=r[0], modelo=modelo, horario_rodada=r[2], preliminar=int(r[3]), pdp=int(r[4]),dt_rev=dt_rev, psat = int(r[5]))
		df_vazoesComparacao = pd.DataFrame(vazoesComparacao, columns=['CD_POSTO', 'VL_VAZAO', 'DT_REFERENTE', 'STR_MODELO', 'VL_REV'])
		df_vazoesComparacao['DT_REFERENTE'] = pd.to_datetime(df_vazoesComparacao['DT_REFERENTE'], format="%Y-%m-%d")
		df_vazoesComparacao = df_vazoesComparacao.pivot(index='CD_POSTO', columns='DT_REFERENTE', values='VL_VAZAO')
		df_vazoesComparacao = wx_postosAux.calcPostosArtificiais_df(df_vazoesComparacao, ignorar_erros=True)
		labelCurva = '{}'.format(r[1])
		labelCurva += '_{:0>2}z'.format(r[2], r[4])
		if r[0]<dt.date():
			labelCurva += (r[0]).strftime(' (%d/%m)')
		elif r[3]:
			labelCurva += ' (PRE)'
		elif r[4]:
			labelCurva += ' (PDP)'
		df_enaComp[labelCurva] = wx_postosAux.gera_ena_df(df_vazoesComparacao)
		if len(df_enaComp) == 2:
			break

	acomph = db_clime.getAcomph(dtInicio=dtInicioAcomph)  
	df_acomph = pd.DataFrame(acomph, columns=['CD_POSTO', 'DT_REFERENTE', 'VL_VAZ_INC_CONSO', 'VL_VAZ_NAT_CONSO', 'DT_ACOMPH', 'ROW'])
	df_acomph_vazNat = df_acomph.pivot(index='CD_POSTO', columns='DT_REFERENTE', values='VL_VAZ_NAT_CONSO')
	df_acomph_vazNat = wx_postosAux.calcPostosArtificiais_df(df_acomph_vazNat, ignorar_erros=True)
	df_acomph_ena = wx_postosAux.gera_ena_df(df_acomph_vazNat)

	rev = db_clime.getRevUltima()
	valorUltimaRev = rev[0][2]
	df_rev = pd.DataFrame(rev, columns=['DT_INICIO_SEMANA', 'VL_ENA', 'CD_REVISAO', 'CD_SUBMERCADO'])
	df_rev['CD_SUBMERCADO'].replace({1:'N', 2:'NE', 3:'SE', 4:'S'}, inplace=True)
	df_rev = df_rev.pivot(index='CD_SUBMERCADO', columns='DT_INICIO_SEMANA', values='VL_ENA')

	dtInicialMlt = min(df_acomph_ena.columns)
	dtInicialMlt = dtInicialMlt.replace(day=1)

	dtFinalMlt = max(max(df_ena.columns), max(df_rev.columns))
	dtFinalMlt = dtFinalMlt.replace(day=1)
	dtFinalMlt = dtFinalMlt + datetime.timedelta(days=31)
	dtFinalMlt = dtFinalMlt.replace(day=1)
	dtsMlt = pd.date_range(start=dtInicialMlt, end=dtFinalMlt, freq='MS')
	mesesMlt = [m.month for m in dtsMlt]

	mlt = db_clime.getMlt()
	df_mlt = pd.DataFrame(mlt, columns=['SUBMERCADO', 'MES', 'MLT'])
	df_mlt = (df_mlt[df_mlt['MES'].isin(mesesMlt)]).copy()
	df_mlt['DT_REFERENTE'] = df_mlt['MES'].replace(dict(zip(mesesMlt, dtsMlt)))
	df_mlt['SUBMERCADO'].replace({1:'N', 2:'NE', 3:'SE', 4:'S'}, inplace=True)
	df_mlt = df_mlt.pivot(index='SUBMERCADO', columns='DT_REFERENTE', values='MLT')

	fig, ax =  plt.subplots(2,2)
	plt.gcf().set_size_inches(20, 10)
	plt.rcParams['axes.grid'] = True
	fig.suptitle('{}'.format(tituloGrafico), fontsize=16)

	submercados = ['SE', 'S', 'NE', 'N']
	for i, sub in enumerate(submercados):

		media_mensal = df_ena.loc[sub].mean()
		mes_referente_oper = wx_opweek.ElecData(min(df_ena.columns).date())
		media_mensal_percent = 100*media_mensal/df_mlt.loc[sub, datetime.datetime(mes_referente_oper.anoReferente, mes_referente_oper.mesReferente, 1)]
		
		ax[int(i/2), i%2].set_title("{} - {:.0f} MWm ({:.0f}%)".format(sub, media_mensal, media_mensal_percent))
		

		x = list(df_ena.loc[sub].index)
		y = list(df_ena.loc[sub].values)
		x = x + [x[-1] + datetime.timedelta(days=7)]
		y = y + [y[-1]]
		label = "{}_{:0>2}z".format(modelo,hr_rodada)
		if int(flagPrel):
			label += " (PRE)"
		elif int(flagpdp):
			label += " (PDP)"
		elif int(flagPsat):
			label += " (PSAT)"
		linesPrevista = ax[int(i/2), i%2].step(x, y, where='post', label=label, color="darkorange", linewidth=1.5, zorder=6)
		autolabel(ax[int(i/2), i%2], linesPrevista[0], pontoInicio=x[0], size=8)

		for j, labelRodada in enumerate(df_enaComp):
			x = list(df_enaComp[labelRodada].loc[sub].index)
			y = list(df_enaComp[labelRodada].loc[sub].values)
			x = x + [x[-1] + datetime.timedelta(days=7)]
			y = y + [y[-1]]
			color = ["burlywood", "antiquewhite"]
			linesPrevista = ax[int(i/2), i%2].step(x, y, where='post', label=labelRodada, color=color[j])

		x = list(df_rev.loc[sub].index)
		y = list(df_rev.loc[sub].values)
		x = x + [x[-1] + datetime.timedelta(days=7)]
		y = y + [np.nan]
		ax[int(i/2), i%2].step(x, y, where='post', label="RV{}".format(valorUltimaRev), color="slateblue")

		ax[int(i/2), i%2].step(df_mlt.loc[sub].index, df_mlt.loc[sub].values, where='post', label="MLT", color="black")
		ax[int(i/2), i%2].plot(df_acomph_ena.loc[sub].index, df_acomph_ena.loc[sub].values, label="ACOMPH", color="darkturquoise")
		
		ax[int(i/2), i%2].legend()
		ax[int(i/2), i%2].tick_params(axis='x', labelrotation=-20)
		ax[int(i/2), i%2].set_ylabel('ENA (MW)')
		ax[int(i/2), i%2].grid(color = 'gray', linestyle = '--', linewidth = 0.2)
	pathFileOut = os.path.join(dir_saida, 'ENA_'+modelo+'.PNG')
	plt.savefig(pathFileOut)
	return pathFileOut



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
	html += "<h3>Diferenças ACOMPH do dia {} com o comph do dia {}</h3>".format(data.strftime('%d/%m/%Y'), (data-datetime.timedelta(days=1)).strftime('%d/%m/%Y'))
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

	# Tratamento para o produto SST
	if parametros["produto"] == 'SST':
		for i in range(20):
			file = baixarProduto(parametros["data"], parametros["produto"])
			if file != '':
				break
			else:
				print("Nova tentiva em 5 minutos")
				time.sleep(60*15)
		flagEmail = True
		# flagWhats = True
		remententeEmail = 'sst@climenergy.com'
		assuntoEmail = '[SST] Atualização do dia {}'.format(parametros["data"].strftime('%d/%m/%Y'))

	# Tratamento para o produto preciptacao_modelo
	elif parametros["produto"] == 'PRECIPTACAO_MODELO':
		fileWhats = gerarCompiladoPrevPrecipt(parametros["data"], parametros["modelo"], parametros["rodada"])
		confgs['whatsapp']['destinatario'] = 'WX - Meteorologia'
		msgWhats = '{}_{:0>2}z ({})'.format(parametros["modelo"].upper(), parametros["rodada"], parametros["data"].strftime('%d/%m/%Y'))
		flagWhats = True

	# Tratamento para o produto IPDO
	elif parametros["produto"] == 'IPDO':
		dtReferente = parametros["data"]
		file = "{}/IPDO-{}.pdf".format(confgs["paths"]["arquivosHidrologia"], dtReferente.strftime('%d-%m-%Y')) 
		remententeEmail = 'ipdo@climenergy.com'
		assuntoEmail = '[IPDO] Atualização do dia {}'.format(parametros["data"].strftime('%d/%m/%Y'))
		
		pastaTmp = confgs['paths']['pastaTmp']
		fileWhats = os.path.join(pastaTmp, 'IPDO_{}.jpeg'.format(dtReferente.strftime('%Y%m%d')))
		pdfToJpeg(file, 2, fileWhats)
		confgs['whatsapp']['destinatario'] = 'PMO'
		flagWhats = True
		flagEmail = True

	# Tratamento para o produto ACOMPH
	elif parametros["produto"] == 'ACOMPH':

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

	elif parametros["produto"] == 'RESULTADO_CV':
		path = wx_resultadosCv.plotResultadosCv(parametros['data'], parametros['modelo'], parametros['rodada'], parametros['rev'], parametros['preliminar'], parametros['pdp'])


	elif parametros["produto"] == 'RELATORIO_CV':
		pathSaida = gerarRelatoriosResultCv(parametros['data'])
		print(pathSaida)
		# confgs['whatsapp']['destinatario'] = ['PMO']
		# flagWhats = True

	elif parametros["produto"] == 'DIFERENCA_CV':
		corpoArquivos = diffChuvaVazao(parametros["data"])
		corpoEmail = corpoArquivos[0]
		file = corpoArquivos[1]
		assuntoEmail = '[CV] Comparação de resultados {}'.format(parametros["data"].strftime('%d/%m/%Y'))
		destinatarioEmail = ['middle@wxe.com.br']
		flagEmail = True

	elif parametros["produto"] == 'RESULTADO_PREVS':
		fileWhats = gerarPlotPrevivaz(dt=parametros['data'], modelo=parametros['modelo'], hr_rodada=parametros['rodada'], dt_rev=parametros['dtrev'], flagPrel=parametros['preliminar'], flagpdp=parametros['pdp'], flagPsat=parametros['psat'])
		msgWhats = 'Resultados de {} - modelo {}'.format(parametros["data"].strftime('%d/%m/%Y'), parametros['modelo'])
		if parametros['preliminar']:
			msgWhats += '.PRELIMINAR'
		if parametros['pdp']:
			msgWhats += '.PDP'
		msgWhats += '_r{:0>2}z'.format(parametros['rodada'])
		flagWhats = True
		confgs['whatsapp']['destinatario'] = 'WX - Meteorologia'


	elif parametros["produto"] == 'PROCESSOS_ANA':
		assuntoEmail = '[Processos] Processos ANA ({})'.format(dt.strftime('%d/%m/%Y %H:00'))
		corpoEmail = wx_consultaProcessos.verificarProcessosAna(dt=parametros['data'])
		
		if corpoEmail != '':
			remententeEmail = 'processos@climenergy.com'
			destinatarioEmail = ['middle@wxe.com.br']
			flagEmail = True

	elif parametros["produto"] == 'RESULTADOS_PROSPEC':
		corpoE, assuntoE, file, msg_whats = wx_resultadosProspec.gerarEmailResultadosProspec(parametros['path'], parametros['nomerodadaoriginal'], parametros['considerarprevs'], parametros['considerarrv'], parametros['gerarmatriz'], parametros['fazer_media'])

		if corpoE != '':

			corpoEmail += corpoE
		
			remententeEmail = 'rodadas@climenergy.com'
			flagEmail = True

			if assuntoEmail == '':
				assuntoEmail += assuntoE

			if destinatarioEmail == '':
				destinatarioEmail = ['front@wxe.com.br', 'middle@wxe.com.br']
    
		if parametros['enviar_whats']:

			destinatarioWhats = 'Preco'

			if 'front@wxe.com.br' not in parametros['destinatarioemail']:
				destinatarioWhats = os.getenv('NUM_GILSEU')

			html_str = "<div><br>"+corpoEmail
			soup = BeautifulSoup(html_str, 'lxml')
			tabela = soup.find('table')
			print('Enviado whatsapp para o destinatario:', destinatarioWhats)
			
			fileWhats = wx_emailSender.api_html_to_image(str(tabela),path_save='out_put.png')
			fields={
					"destinatario": destinatarioWhats,
					"mensagem": parametros['assuntoemail'],
				}
			files={}
			if fileWhats:
				files={
					"arquivo": (os.path.basename(fileWhats), open(fileWhats, "rb"))
				}
			response = requests.post(os.getenv('WHATSAPP_API'), data=fields, files=files)
			print("Status Code:", response.status_code)
				

			flagWhats = False
    

	elif parametros["produto"] == 'PREVISAO_GEADA':

		if 'path' in parametros:
			pathSaida = parametros['path']
		else:
			pathSaida = os.path.join(pathArquivos, 'tmp')

		dt = parametros['data']
		file = wx_previsaoGeadaInmet.getPrevisaoGeada(dataPrevisao=dt, pathSaida=pathSaida)


	elif parametros["produto"] == 'REVISAO_CARGA':
		# verificar se o diferencaCarga está vazia.
		diferencaCarga, dataRvAtual,dfCargaAtual_xlsx,dfCargaAtual_txt,difCarga = rz_cargaPatamar.cargaPatamar(parametros['path'])
		
		if not diferencaCarga.empty:
			diferencaCarga.columns = diferencaCarga.columns.str.replace(r'/\d{4}', '', regex=True)  # adicionando filtro para remover qualquer ano da coluna exemplo /2024 será removido

   
			diferencaCargaPMO = rz_cargaPatamar.cargaPatamarPMO(parametros['path'],diferencaCarga,dfCargaAtual_xlsx,dfCargaAtual_txt,difCarga)
			diferencaCargaPMO.columns = diferencaCargaPMO.columns.str.replace(r'/\d{4}', '', regex=True)  # adicionando filtro para remover qualquer ano da coluna exemplo /2024 será removido


			diferencaCarga.rename({'Sistema Interligado Nacional':'SIN'}, inplace=True)
			diferencaCarga.index.name = None
			diferencaCarga = diferencaCarga.loc[['SE','S','NE','N','SIN']]
		
			diferencaCargaPMO.rename({'Sistema Interligado Nacional':'SIN'}, inplace=True)
			diferencaCargaPMO.index.name = None
			diferencaCargaPMO = diferencaCargaPMO.loc[['SE','S','NE','N','SIN']]
			difCargaREV = diferencaCargaPMO - diferencaCarga

			diferencaCarga_estilizada = diferencaCarga.style.format('{:.0f}')
			diferencaCarga_estilizada_PMO = diferencaCargaPMO.style.format('{:.0f}')
		
			dataRvAnterior = wx_opweek.ElecData(dataRvAtual.data - datetime.timedelta(days=7))
			diferencaCarga_estilizada.set_caption(f"Atualizacao de carga DC (RV{dataRvAtual.atualRevisao} - RV{dataRvAnterior.atualRevisao})")
			diferencaCarga_estilizada_PMO.set_caption(f"Atualizacao de carga DC (RV{dataRvAtual.atualRevisao} - PMO)")

			css = '<style type="text/css">'
			css += 'caption {background-color: #781e77;color: white;}'
			css += 'th {background-color: #781e77;color: white;}'
			css += 'table {text-align: center;}'
			css += 'th {min-width: 50px;}'
			css += 'td.col'+str(dataRvAtual.atualRevisao)+' {background: #dac2da}'
			css += '</style>'
		
			# Verifica se a diferença dos dataframes são iguais com zero e Nan.
			if dataRvAtual.atualRevisao == 0:
				html = diferencaCarga_estilizada.to_html()
				print(f"Será enviado apenas a tabela: RV{dataRvAtual.atualRevisao} - RV{dataRvAnterior.atualRevisao}, a diferença da RV1 para RV0 é igual.")
				destinatarioEmail = ['middle@wxe.com.br']
				flagWhats = True
				grupoWhats = 'Modelos' 
			elif difCargaREV.isin([0, pd.np.nan]).all().all():
				html = diferencaCarga_estilizada.to_html()
				destinatarioEmail = ['middle@wxe.com.br', 'front@wxe.com.br','fabio.Marcelino@raizen.com','camila.Lourenco@raizen.com','eder.Freitas@raizen.com']
				flagWhats = True
				grupoWhats = 'PMO'
			else:
				html = diferencaCarga_estilizada.to_html()
				html += '<br>'
				html += diferencaCarga_estilizada_PMO.to_html()
				destinatarioEmail = ['middle@wxe.com.br', 'front@wxe.com.br','fabio.Marcelino@raizen.com','camila.Lourenco@raizen.com','eder.Freitas@raizen.com']

				flagWhats = True
				grupoWhats = 'PMO'

			html = html.replace('<style type="text/css">\n</style>\n', css)

			# Criando documento 
			doc = aw.Document()
			builder = aw.DocumentBuilder(doc)
			# inserindo conteudo HTML para fazer a imagem
			builder.insert_html(html)

			# Salvando o documento em JPG
			doc.save(f"/WX/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/gerarProdutos/arquivos/tmp/Revisao_carga_DC_(RV{dataRvAtual.atualRevisao}.jpg")

			# Abrir a imagem PNG
			imagem = Image.open(f"/WX/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/gerarProdutos/arquivos/tmp/Revisao_carga_DC_(RV{dataRvAtual.atualRevisao}.jpg")

			# Coordenadas da área que você quer cortar (x1, y1, x2, y2)
			# Isso representa um retângulo com canto superior esquerdo (x1, y1) e canto inferior direito (x2, y2)
			coordenadas = (111, 112, 707, 482)  # Ajuste essas coordenadas conforme necessário

			# Cortar a parte da imagem
			parte_cortada = imagem.crop(coordenadas)

			# Salvar a parte cortada como uma nova imagem
			parte_cortada.save(f'/WX/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/gerarProdutos/arquivos/tmp/Revisao_carga_DC_(RV{dataRvAtual.atualRevisao}.jpg')

    
			fileWhats = f'/WX/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/gerarProdutos/arquivos/tmp/Revisao_carga_DC_(RV{dataRvAtual.atualRevisao}.jpg'
  
			##Configuraçoes do gmail
			flagEmail = True
			remententeEmail = 'rev_carga@climenergy.com'
			corpoEmail = '<div>'+html+'</div>'
			assuntoEmail = f'Revisão carga - DC (RV{dataRvAtual.atualRevisao})'
			destinatarioEmail = destinatarioEmail
			##Configuraçoes do whats
			flagWhats = flagWhats
			confgs['whatsapp']['destinatario'] = grupoWhats  
		else:
			print("Email e whats não enviado.Dataframe vazio[].")


	elif parametros["produto"] == 'CMO_DC_PRELIMINAR':
		assunto, cmo = rz_deck_dc_preliminar.sumario_cmo(parametros['path'])

		html = '<h1>'+assunto+'</h1>'
		html += '<p>Deck do ONS não comentado.</p>'
		msgWhats = '\`\`\`{}\\nDeck do ONS não comentado\\n{:->15}\\n'.format(assunto,'')
		for submercado in cmo:
			html += '<h3>{}</h3>'.format(submercado)
			html += wx_emailSender.gerarTabela(cmo[submercado][1:], cmo[submercado][0])

			msgWhats += '| {:>2} | {:>6} |\\n'.format(submercado, cmo[submercado][4][1])
		msgWhats += '{:->15}\\n\`\`\`'.format('')


		flagEmail = True
		assuntoEmail = assunto
		corpoEmail = html
		remententeEmail = 'cmo@climenergy.com'
		destinatarioEmail = ['middle@wxe.com.br', 'front@wxe.com.br']

		flagWhats = True
		confgs['whatsapp']['destinatario'] = 'PMO'

	elif parametros["produto"] == 'GERA_DIFCHUVA':

		data = parametros.get("data")
		dados_modelo1 = parametros.get("dados_modelo1")
		dados_modelo2 = parametros.get("dados_modelo2")

		if not data:
			data = datetime.datetime.now()

		html_template = rz_produtos_chuva.produto_tbmaps_chuva_diff(data,dados_modelo1, dados_modelo2)

		date_today_str = data.strftime('%d/%m/%y')
		data_yesterday_str = (data - datetime.timedelta(1)).strftime('%d/%m/%y')

		##Configuraçoes do gmail
		serv_email = wx_emailSender.WxEmail()
		serv_email.username = 'mapas@climenergy.com'
		serv_email.sendEmail(texto=html_template, assunto=f'Dados da diferença das rodadas ({date_today_str} - {data_yesterday_str})')



	elif parametros["produto"] == 'REVISAO_CARGA_NW':
		
		
		path = parametros['path']
		
		zip_name = os.path.basename(path)
		dir_nomeMesAtual = datetime.datetime.strptime(zip_name,"RV0_PMO_%B_%Y_carga_mensal.zip").strftime('%B_%Y').capitalize()
		dataMesAtual = pd.to_datetime(dir_nomeMesAtual,format="%B_%Y")
		nomeMesAtual = dataMesAtual.strftime("%b%Y").capitalize()
		
		archive = zipfile.ZipFile(path, 'r')
		xlfile = archive.open(archive.filelist[0].filename)
  
		try:
			pmo_mes = pd.read_excel(xlfile,sheet_name=1)
		except:
			pmo_mes = pd.read_excel(xlfile)

		datas_list = pmo_mes[pmo_mes.DATE >= dataMesAtual ].DATE.unique()[:2]

		html_completo_sMMGD=""
		html_completo_cMMGD=""
		title = ['', 'DIFF', '%']
		submarket=['SE','S','NE','N','SIN']
  
		msgWhats = f"\`\`\`\\n\\n"
  
		for data in datas_list:
				html_completo_sMMGD += rz_cargaPatamar.gera_tb_pmo_nw(path,data)[0]
				html_completo_cMMGD += rz_cargaPatamar.gera_tb_pmo_nw(path,data)[2]
				lista_sMMGD = rz_cargaPatamar.gera_tb_pmo_nw(path,data)[1][['DIFF','PORCENTAGEM']].values.tolist()
				lista_cMMGD = rz_cargaPatamar.gera_tb_pmo_nw(path,data)[3][['DIFF','PORCENTAGEM']].values.tolist()

				cmo_cMMGD = [title]
				cmo_sMMGD = [title]
    
				for i,values in enumerate(lista_sMMGD):
					cmo_sMMGD.append([submarket[i]] + values)
    
				for i,values in enumerate(lista_cMMGD):
					cmo_cMMGD.append([submarket[i]] + values)
    
				table_sMMGD =  cmo_sMMGD[0], cmo_sMMGD[1], cmo_sMMGD[2], cmo_sMMGD[3], cmo_sMMGD[4], cmo_sMMGD[5]
				table_sMMGD = tabulate.tabulate(table_sMMGD, tablefmt="pretty")
    
				table_cMMGD =  cmo_cMMGD[0], cmo_cMMGD[1], cmo_cMMGD[2], cmo_cMMGD[3], cmo_cMMGD[4], cmo_cMMGD[5]
				table_cMMGD = tabulate.tabulate(table_cMMGD, tablefmt="pretty")
  
				msgWhats  +='\\n{} - carga sem MMGD\\n'.format(pd.to_datetime(data).strftime("%b%Y").capitalize())
				msgWhats += table_sMMGD
    
				msgWhats  +='\\n{} - carga com MMGD\\n'.format(pd.to_datetime(data).strftime("%b%Y").capitalize())
				msgWhats += table_cMMGD
    
    
		msgWhats += "\`\`\`"

		##Configuraçoes do gmail
		flagEmail = True
		remententeEmail = 'rev_carga@climenergy.com'
  
		# fazendo o split pois cada html vem com mes atual e antigo e queremos separar cada mes em sua string
		html_completo_sMMGD_split = html_completo_sMMGD.split('<style type')
		html_completo_sMMGD_parte1 = '<style type' + html_completo_sMMGD_split[1]
		html_completo_sMMGD_parte2 = '<style type' + html_completo_sMMGD_split[2]
  
		html_completo_cMMGD_split = html_completo_cMMGD.split('<style type')
		html_completo_cMMGD_parte1 = '<style type' + html_completo_cMMGD_split[1]
		html_completo_cMMGD_parte2 = '<style type' + html_completo_cMMGD_split[2]

  
		corpoEmail = html_completo_sMMGD_parte1 + html_completo_cMMGD_parte1 + html_completo_sMMGD_parte2 + html_completo_cMMGD_parte2
		assuntoEmail = f'Revisão carga PMO-NW-{nomeMesAtual}'

		##Configuraçoes do whats
		flagWhats = True
		confgs['whatsapp']['destinatario'] = 'PMO'


	elif parametros["produto"] == 'PSATH_DIFF':

		path_arq = parametros['path']
		templates_unificados = rz_produtos_chuva.produto_psath_diff(path_arq,limiar=1)

		if templates_unificados == None:
			flagEmail = False
			print('Não será enviado o e-mail')
		else:
			flagEmail = True
  
  	##Configuraçoes do gmail
		remententeEmail = 'diferenca_cv@climenergy.com'
		passwordEmail = "Clime2@sam"
		corpoEmail = templates_unificados

		data_atual_str =datetime.datetime.today().strftime("%d/%m/%Y")
		data_anterior_str = pd.to_datetime(datetime.datetime.today() - datetime.timedelta(days=1)).strftime("%d/%m/%Y")
		
		assuntoEmail = f'Diferença dos arquivos de histórico de precipitação por satélite ({data_atual_str} - {data_anterior_str})'

	elif parametros["produto"] == 'GRAFICO_MODELOS':

		
		rodada = rz_rodadasModelos.Rodadas(
			modelo=parametros.get('modelo'),
			dt_rodada=parametros.get("data").strftime('%Y-%m-%d'),
			hr_rodada=parametros.get('rodada'),
			flag_pdp=parametros.get('pdp'),
			flag_psat=parametros.get('psat'),
			flag_preliminar=parametros.get('preliminar')
			)
		
		file = rodada.gerar_plot_ena_modelos()

		msgWhats = 'ENA {}_{}z ({})'.format(parametros.get('modelo'),str(parametros.get('rodada')).zfill(2),parametros.get("data").strftime('%d/%m/%Y'))

		fileWhats = file
		confgs['whatsapp']['destinatario'] = 'WX - Meteorologia'
		flagWhats = True


	# # Para testes
	# destinatarioEmail = ['caio.assis@raizen.com', 'joao.filho4@raizen.com']
	# confgs['whatsapp']['destinatario'] = ''

	# Envia o produto via email caso a  flagEmail estiver ativada
	if flagEmail:

		# Objeto ja configurado para enviar email utilizando o climenergy@climenergy.com
		# e possui uma lista default de remetentes
		serv_email = wx_emailSender.WxEmail()
		
		# Produto com remetente diferente do default (climenergy@climenergy.com)
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

		if file != '' and file != []:
			if type(file) == type(''):
				serv_email.sendEmail(texto=corpoEmail, assunto=assuntoEmail, anexos=[file])
			elif type(file) == type([]):
				serv_email.sendEmail(texto=corpoEmail, assunto=assuntoEmail, anexos=file)
		else:
			serv_email.sendEmail(texto=corpoEmail, assunto=assuntoEmail, anexos=[])

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

	print('python {} produto SST data {}'.format(sys.argv[0], data.strftime('%d/%m/%Y')))
	print('python {} produto IPDO data {}'.format(sys.argv[0], data.strftime('%d/%m/%Y')))
	# print('python {} produto ACOMPH data {}'.format(sys.argv[0], data.strftime('%d/%m/%Y')))
	print('python {} produto ACOMPH path {}'.format(sys.argv[0], os.path.join(path_plan_acomph_rdh, 'ACOMPH_{}.xls'.format(data.strftime('%d.%m.%Y')))))
	print('python {} produto RDH data {}'.format(sys.argv[0], data.strftime('%d/%m/%Y')))
	print('python {} produto preciptacao_modelo data {} modelo gefs rodada 0'.format(sys.argv[0], data.strftime('%d/%m/%Y')))
	print('python {} produto RELATORIO_BBCE data {}'.format(sys.argv[0], data.strftime('%d/%m/%Y')))
	# print('python {} produto RESULTADO_CV data {} modelo \'PCONJUNTO\' rodada 0 rev 1 preliminar true pdp true'.format(sys.argv[0], data.strftime('%d/%m/%Y')))
	print('python {} produto RESULTADO_DESSEM data {}'.format(sys.argv[0], (data+datetime.timedelta(days=1)).strftime('%d/%m/%Y')))
	print('python {} produto PREVISAO_VAZAO_DESSEM data {}'.format(sys.argv[0], (data+datetime.timedelta(days=1)).strftime('%d/%m/%Y')))
	print('python {} produto PREVISAO_CARGA_DESSEM data {}'.format(sys.argv[0], (data+datetime.timedelta(days=1)).strftime('%d/%m/%Y')))
	print('python {} produto DIFERENCA_CV data {}'.format(sys.argv[0], data.strftime('%d/%m/%Y')))
	print('python {} produto RESULTADO_PREVS data {} modelo {} rodada {} dtrev {} preliminar {} pdp {} psat {}'.format(sys.argv[0], data.strftime('%d/%m/%Y'), 'PCONJUNTO', 0,dt_proxRv,0, 0, 0))
	print('python {} produto GRAFICO_ACOMPH data {}'.format(sys.argv[0], data.strftime('%d/%m/%Y')))
	print('python {} produto PROCESSOS_ANA data {}'.format(sys.argv[0], data.strftime('%d/%m/%Y')))
	print('python {} produto RESULTADOS_PROSPEC nomeRodadaOriginal \'{}\' path \'{}\' destinatarioEmail \'{}\' assuntoEmail \'{}\' corpoEmail \'{}\' considerarPrevs \'[0]\' fazer_media 0 enviar_whats 0'.format(sys.argv[0], '[\"ecmwf_ens-wx1\",\"ecmwf_ens-wx2\"]', filesProspec,'[\"thiago@wxe.com.br\",\"middle@wxe.com.br\"]', 'Assunto do email', '<a1>Coloque aqui o corpo do email</a1>'))
	print('python {} produto PREVISAO_GEADA data {}'.format(sys.argv[0], data.strftime('%d/%m/%Y')))
	print('python {} produto REVISAO_CARGA path {}'.format(sys.argv[0], os.path.join(path_plan_acomph_rdh,file_revisaoCargas)))
	print('python {} produto CMO_DC_PRELIMINAR path {}'.format(sys.argv[0], os.path.join(deck_decomp,'PMO_deck_preliminar.zip')))
	print('python {} produto GERA_DIFCHUVA data {}'.format(sys.argv[0],data.strftime('%d/%m/%Y')))
	print('python {} produto REVISAO_CARGA_NW path {}'.format(sys.argv[0],os.path.join(path_plan_acomph_rdh,'RV0_PMO_%B_%Y_carga_mensal.zip')))
	print('python {} produto PSATH_DIFF path {}'.format(sys.argv[0],os.path.join(pathArquivosTmp,'psath_%d%m%Y.zip')))
	print('python {} produto GRAFICO_MODELOS modelo {} data {} rodada {} pdp {} psat {} preliminar {}'.format(sys.argv[0],"PCONJUNTO",data.strftime('%d/%m/%Y'),0,1,0,0 ))

	# print('python {} produto RELATORIO_CV url http://wxclima.ddns-intelbras.com.br:5000/ena_diaria_submercado'.format(sys.argv[0]))
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

			elif argumento == "destinatarioemail":
				parametros[argumento] = eval(sys.argv[i+1])

			elif argumento == "assuntoemail":
				parametros[argumento] = sys.argv[i+1]

			elif argumento == "corpoemail":
				parametros[argumento] = sys.argv[i+1]

			elif argumento == "modelo":
				parametros[argumento] = sys.argv[i+1]

			elif argumento == "rodada":
				parametros[argumento] = int(sys.argv[i+1])

			elif argumento == "rev":
				parametros[argumento] = int(sys.argv[i+1])
    	
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

			elif argumento == "nomerodadaoriginal":
				parametros[argumento] = sys.argv[i+1]

			elif argumento == "considerarprevs":
				parametros[argumento] = sys.argv[i+1]
			
			elif argumento == "considerarrv":
				parametros[argumento] = sys.argv[i+1]

			elif argumento == "gerarmatriz":
				parametros[argumento] = int(sys.argv[i+1])

			elif argumento == "fazer_media":
				parametros[argumento] = sys.argv[i+1]
				parametros[argumento] = eval(parametros[argumento][0])
		
			elif argumento == "enviar_whats":
				parametros[argumento] = eval(sys.argv[i+1])

	enviar(parametros)


if __name__ == '__main__':
	geraRelatorioBbce(datetime.datetime.now())	
	runWithParams()

	# url = 'http://wxclima.ddns-intelbras.com.br:5000/ena_diaria_submercado'
	# url = 'http://wxclima.ddns-intelbras.com.br:5000/ena_diaria_bacia'
	# gerarRelatoriosResultCv(url)

	# enviaRelatorioBbce(datetime.datetime(2020,10,26))

	# path = r'C:\Users\thiag\Documents\git\wx_chuvaVazao\arquivos\temporarios\chrome\ACOMPH_07.10.2020.xls'
	# tratarAComph(path)

	# data = datetime.datetime.now()
	# data -= datetime.timedelta(days=1)
	# produto = 'SST'
	# baixarProduto(data, produto)

	# data = datetime.datetime.now()
	# produto = 'preciptacao_modelo'
	# modelo = 'gfs'
	# rodada = '00'
	# gerarProduto(data, produto, modelo, rodada)