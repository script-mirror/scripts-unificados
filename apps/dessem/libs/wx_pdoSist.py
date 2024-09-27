import os
import re
import pdb
import sys
import warnings
import datetime
import numpy as np
import pandas as pd
import sqlalchemy as db
warnings.filterwarnings("ignore")


sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.bibliotecas import wx_dbClass

def getFromFile(path):
	dir_file = os.path.dirname(path)
	fullname_file = os.path.basename(path)
	name_file, extension_file = fullname_file.split('.')

	path = os.path.join(dir_file, f'{name_file.lower()}.{extension_file}')
	if not os.path.exists(path):
		path = os.path.join(dir_file, f'{name_file.upper()}.{extension_file}')
  
	file = open(path, 'r', encoding="latin-1")
	return file.readlines()

def leituraArquivo(filePath):

	arquivo = getFromFile(filePath)

	dados = {'SIST':[]}
	iLine = 0
	while iLine != len(arquivo)-1:
		line = arquivo[iLine]

		if '------;--------;------;------------;'  in line:
			iLine += 4
			line = arquivo[iLine]
			bloco = []
			while iLine != len(arquivo)-1:
				bloco.append(line)
				iLine += 1
				line = arquivo[iLine]
			dados['SIST'] = bloco
			continue
		else:
			iLine += 1

	return dados

def getInfoBlocos():
	blocos = {}

	blocos['SIST'] = {'campos':[
						'iper',
						'pat',
						'sist',
						'cmo',
						'demanda',
						'perdas',
						'gpqusi',
						'gfixbar',
						'grenova',
						'somatgh',
						'somatgt',
						'conseleva',
						'import.',
						'export.',
						'cortcarg.',
						'saldo',
						'recebimento',
						'somagtmin',
						'somatgtmax',
						'earm',
				],
				'regex':'(.{6});(.{8});(.{6});(.{12});(.{12});(.{12});(.{12});(.{12});(.{12});(.{12});(.{12});(.{12});(.{12});(.{12});(.{12});(.{12});(.{12});(.{12});(.{12});(.{12});(.*)',
				'formatacao':'{:>6};{:>8};{:>6};{:>12};{:>12};{:>12};{:>12};{:>12};{:>12};{:>12};{:>12};{:>12};{:>12};{:>12};{:>12};{:>12};{:>12};{:>12};{:>12};{:>12};'}
	return blocos


def extrairInfoBloco(listaLinhas, mnemonico, regex):

	blocos = []
	if mnemonico in listaLinhas:
		for i, linha in enumerate(listaLinhas[mnemonico]):
			infosLinha = re.split(regex, linha)
			blocos.append(infosLinha[1:-2])   # ultimo termo da lista e o que sobra da expressao regex (/n)

	return blocos

# def leituraCmo(data, pathArquivos, comentatio = '', fonte='ccee'):
def leituraSist(pdoSistPath):
	
	infoBlocos = getInfoBlocos()
	pdoSist    = leituraArquivo(pdoSistPath)
	precos     = extrairInfoBloco(pdoSist, 'SIST', infoBlocos['SIST']['regex'])
	df_sist    = pd.DataFrame(precos, columns=infoBlocos['SIST']['campos'])

	return df_sist


def calculoIntercambio(df_sist):
	for i in range(1,49):
		df_aux = df_sist[df_sist['iper'] == i].apply(np.sum, axis = 0)
		ande = df_aux['demanda'] - df_aux['grenova']-df_aux['somatgh'] -df_aux['somatgt'] + df_aux['conseleva']
		df_sist.iat[df_sist[df_sist['sist'] == 'SE'][df_sist['iper'] == i]['somatgh'].index.values[0], 6] = df_sist[df_sist['sist'] == 'SE'][df_sist['iper'] == i]['somatgh'] + ande

	df_sist['intercambio'] = df_sist.apply(lambda  x:x['demanda']-x['grenova']-x['somatgh'] -x['somatgt'] + x['conseleva'], axis=1)

	return df_sist


def insertData(df_sist, dataDeck):
	df_out = pd.DataFrame()
	for subm in df_sist[df_sist['iper'] == 1]['sist']:
		iper = 1
		while iper < 48:
			dict_aux = {'dataHora':str(dataDeck.day)+'/'+str(dataDeck.month)+'/'+str(dataDeck.year)+ ' '+ str(int((int(iper-1)*30)/60)).zfill(2)+':00',
			             'sist': subm,
						 'cmo':str(int((int(df_sist[df_sist['sist'] == subm][df_sist['iper'] == iper]['cmo'])+int(df_sist[df_sist['sist'] == subm][df_sist['iper'] == iper+1]['cmo']))/2)),
						 'demanda':str(int((int(df_sist[df_sist['sist'] == subm][df_sist['iper'] == iper]['demanda'])+int(df_sist[df_sist['sist'] == subm][df_sist['iper'] == iper+1]['demanda'])
						 + int(df_sist[df_sist['sist'] == subm][df_sist['iper'] == iper]['conseleva']) + int(df_sist[df_sist['sist'] == subm][df_sist['iper'] == iper+1]['conseleva']))/2)),
						 'grenova':str(int((int(df_sist[df_sist['sist'] == subm][df_sist['iper'] == iper]['grenova'])+int(df_sist[df_sist['sist'] == subm][df_sist['iper'] == iper+1]['grenova']))/2)),
						 'somatgh':str(int((int(df_sist[df_sist['sist'] == subm][df_sist['iper'] == iper]['somatgh'])+int(df_sist[df_sist['sist'] == subm][df_sist['iper'] == iper+1]['somatgh']))/2)),
						 'somatgt':str(int((int(df_sist[df_sist['sist'] == subm][df_sist['iper'] == iper]['somatgt'])+int(df_sist[df_sist['sist'] == subm][df_sist['iper'] == iper+1]['somatgt']))/2)),
						 'somagtmin':str(int((int(df_sist[df_sist['sist'] == subm][df_sist['iper'] == iper]['somagtmin'])+int(df_sist[df_sist['sist'] == subm][df_sist['iper'] == iper+1]['somagtmin']))/2)),
						 'somagtmax':str(int((int(df_sist[df_sist['sist'] == subm][df_sist['iper'] == iper]['somatgtmax'])+int(df_sist[df_sist['sist'] == subm][df_sist['iper'] == iper+1]['somatgtmax']))/2)),
						 'intercambio':str(int((int(df_sist[df_sist['sist'] == subm][df_sist['iper'] == iper]['intercambio'])+int(df_sist[df_sist['sist'] == subm][df_sist['iper'] == iper+1]['intercambio']))/2)),
						 'pld':str(int((int(df_sist[df_sist['sist'] == subm][df_sist['iper'] == iper]['pld'])+int(df_sist[df_sist['sist'] == subm][df_sist['iper'] == iper+1]['pld']))/2)) }
			df_out = df_out.append(dict_aux,ignore_index=True)
			iper = iper + 2
	return df_out



def calculo_pld(lista_input, PLD_min, PLDmax_h, PLDmax_estr):
    
	# ajusta PLDs para piso e teto
	for i in range(len(lista_input)):
		if float(lista_input[i]) > PLDmax_h:
			lista_input[i] = PLDmax_h
		if float(lista_input[i]) < PLD_min:
			lista_input[i] = PLD_min
    
	# método iterativo para adequação a PLDmax estrutural
	PLD_md0 = sum(lista_input) /len(lista_input)
	PLD_md = PLD_md0
    
	f_est0 = PLDmax_estr / PLD_md0
	f_est = f_est0
	contCalc = 0
	dif = (PLD_md - PLDmax_estr)

	while dif > 0.01:
		#print(f_est)
		
		lista_aux = [round(element * f_est, 2) for element in lista_input]
        
		lista_input = lista_aux
        
		PLD_md = sum(lista_input) / len(lista_input)
        
		f_est = PLDmax_estr / PLD_md

		dif = (PLD_md - PLDmax_estr)

		contCalc = contCalc + 1
		if contCalc > 20:
			break
	lista_output = lista_input
    
	return lista_output

def calculaPLD(df_sist, data):
	db_ons = wx_dbClass.db_mysql_master('db_ons')
	db_ons.connect()
	tb_pld = db_ons.getSchema('tb_pld')
	ano = data.year
	query_get_ano_pld = db.select(tb_pld).where(db.and_(tb_pld.c.str_ano == ano))

	colunas = db_ons.conn.execute(query_get_ano_pld)

	for valor in colunas:
		PLDmax_hora = valor.vl_PLDmax_hora
		PLDmax_estr = valor.vl_PLDmax_estr
		PLDmin = valor.vl_PLDmin

	listPldSE = calculo_pld(df_sist[df_sist['sist'] == 'SE']['cmo'].tolist(), PLDmin, PLDmax_hora, PLDmax_estr) 
	listPldS  = calculo_pld(df_sist[df_sist['sist'] == 'S']['cmo'].tolist(), PLDmin, PLDmax_hora, PLDmax_estr)
	listPldNE = calculo_pld(df_sist[df_sist['sist'] == 'NE']['cmo'].tolist(), PLDmin, PLDmax_hora, PLDmax_estr)
	listPldN  = calculo_pld(df_sist[df_sist['sist'] == 'N']['cmo'].tolist(), PLDmin, PLDmax_hora, PLDmax_estr)

	listPld = []
	for i in range(len(listPldSE)):
		listPld.append(listPldSE[i])
		listPld.append(listPldS[i])
		listPld.append(listPldNE[i])
		listPld.append(listPldN[i])

	df_sist['pld'] = listPld

def readPdoSist(path, data, pathOut ):
	#Leitura PDO Sist
	try:
		df_sist = leituraSist(path + '/pdo_sist.dat')
	except:
		df_sist = leituraSist(path + '/PDO_SIST.DAT')

	df_sist                = df_sist.drop(columns=['perdas', 'gpqusi','gfixbar','import.', 'export.','cortcarg.','saldo','recebimento'])
	df_sist['iper']        = df_sist['iper'].astype(int)
	df_sist                = df_sist.loc[df_sist['iper'] <= 48]
	df_sist['demanda']     = df_sist['demanda'].astype(float)
	df_sist['demanda']     = df_sist['demanda'].astype(int)
	df_sist['grenova']     = df_sist['grenova'].astype(float)
	df_sist['grenova']     = df_sist['grenova'].astype(int)
	df_sist['somatgh']     = df_sist['somatgh'].astype(float)
	df_sist['somatgh']     = df_sist['somatgh'].astype(int)
	df_sist['conseleva']   = df_sist['conseleva'].astype(float)
	df_sist['conseleva']   = df_sist['conseleva'].astype(int)
	df_sist['somatgt']     = df_sist['somatgt'].astype(float)
	df_sist['somatgt']     = df_sist['somatgt'].astype(int)
	df_sist['somagtmin']   = df_sist['somagtmin'].astype(float)
	df_sist['somagtmin']   = df_sist['somagtmin'].astype(int)
	df_sist['somatgtmax']   = df_sist['somatgtmax'].astype(float)
	df_sist['somatgtmax']   = df_sist['somatgtmax'].astype(int)
	df_sist['earm']        = df_sist['earm'].astype(float)
	df_sist['earm']        = df_sist['earm'].astype(int)
	df_sist['cmo']         = df_sist['cmo'].astype(float)
	df_sist['sist']        = df_sist['sist'].str.strip()

	df_sist = calculoIntercambio(df_sist)
	pdoSist = df_sist.loc[df_sist['sist'] != 'FC']

	calculaPLD(pdoSist, data)


	pdoSist = insertData(pdoSist, data)

	db_decks = wx_dbClass.db_mysql_master('db_decks')
	db_decks.connect()
	tb_balanco_dessem = db_decks.getSchema('tb_balanco_dessem')
	balanco_dessem_lista = pdoSist.values.tolist()

	for lista in balanco_dessem_lista:
		data_str = lista[0]
		data = datetime.datetime.strptime(data_str, '%d/%m/%Y %H:%M')
		lista[0] = data
	balanco_dessem_lista = list(balanco_dessem_lista)
	
	dt = data.date()
	dia_inicio = datetime.datetime.combine(dt, datetime.time.min)
	dia_fim = datetime.datetime.combine(data, datetime.time.max)
	delete_balanco_dessem = tb_balanco_dessem.delete().where(tb_balanco_dessem.c.dt_data_hora.between(dia_inicio, dia_fim))
	db_decks.conn.execute(delete_balanco_dessem)
	insert_balanco_dessem = tb_balanco_dessem.insert().values(balanco_dessem_lista).prefix_with('IGNORE')
	db_decks.conn.execute(insert_balanco_dessem)

if '__main__' == __name__:

	diretorioRaiz = os.path.abspath('../../../')
	pathLibUniversal = os.path.join(diretorioRaiz,'bibliotecas')
	sys.path.insert(1, pathLibUniversal)

	import wx_dbLib

	path = os.path.abspath(r'/home/thiago/workspace/wx/alpha/apps/dessem/arquivos/20210217/entrada/ons_entrada_saida/pdo_sist.dat')
	leituraSist(path)

	# data = datetime.datetime.now()
	# data = datetime.datetime(2021,2,10)
	# path = os.path.abspath(r'/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/dessem/arquivos/20210210/saida/resultados/PDO_CMOSIST.DAT')
	# gerarCurvasPrecos(data, path)
	# gerarCurvasPrecos(data, path)
	# leituraCmo(data, path, comentatio = '', fonte='wx')