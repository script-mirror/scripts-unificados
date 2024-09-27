import re
import os
import pdb
import sys
import codecs
import datetime
import unidecode

import pandas as pd


sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.bibliotecas import wx_opweek

def getFromFile(path):
	file = open(path, 'r')
	return file.readlines()

def leituraArquivo(filePath):

	arquivo = getFromFile(filePath)

	dados = {}
	for iLine in range(len(arquivo)):
		line = arquivo[iLine]

		if line[0] == '&'  or line[0] == '\n':
			continue
		else:
			mnemonico = line.split()[0]			
			if mnemonico not in dados:
				dados[mnemonico] = []
			dados[mnemonico].append(line)

	return dados

def getInfoBlocos():
	blocos = {}

	blocos['EOLICA'] = {'campos':[
					'mnemonico',
					'codigo',
					'nome',
					'pmax',
					'fcap',
					'c',
			],
			'regex':'(.{7});(.{5}) ;(.{40}) ;(.{10}) ;(.{3}) ;(.{1});(.*)',
			'formatacao':'{:>7};{:>5} ;{:>40} ;{:>10} ;{:>3} ;{:>1};'}

	blocos['EOLICABARRA'] = {'campos':[
						'mnemonico',
						'codigo',
						'barra',
				],
				'regex':'(.{11}) ;(.{5}) ;(.{5}) ;(.*)',
				'formatacao':'{:>11} ;{:>5} ;{:>5} ;'}

	blocos['EOLICASUBM'] = {'campos':[
						'mnemonico',
						'codigo',
						'sbm',
				],
				'regex':'(.{11});(.{5}) ;(.{2}) ;(.*)',
				'formatacao':'{:>11};{:>5} ;{:>2} ;'}

	blocos['EOLICA-GERACAO'] = {'campos':[
						'mnemonico',
						'codigo',
						'di',
						'hi',
						'mi',
						'df',
						'hf',
						'mf',
						'geracao',
				],
				'regex':'(.{15});(.{5}) ;(.{2}) ;(.{2}) ;(.{1}) ;(.{2}) ;(.{2}) ;(.{1}) ;(.{10}) ;(.*)',
				'formatacao':'{:<15};{:>5} ;{:>2} ;{:>2} ;{:>1} ;{:>2} ;{:>2} ;{:>1} ;{:>10} ;'}

	return blocos


def extrairInfoBloco(listaLinhas, mnemonico, regex):

	blocos = []
	if mnemonico in listaLinhas:
			
		for i, linha in enumerate(listaLinhas[mnemonico]):
			if mnemonico == 'EOLICA':
				infosLinha = re.split(';', linha)
				blocos.append(infosLinha[:-1])   # ultimo termo da lista e o que sobra da expressao regex (/n)
			else:
				infosLinha = re.split(regex, linha)
				blocos.append(infosLinha[1:-2])   # ultimo termo da lista e o que sobra da expressao regex (/n)
			if len(infosLinha) == 0:
				raise Exception('Erro ao extrair informacoes da linha:\n {}\nregex:\n{}'.format(linha, regex))

	return blocos

def getCabecalhos(mnemonico):

	cabecalho = []
	if mnemonico == 'EOLICA':
		cabecalho.append('&XXXXXX;XXXXX ;XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX ;XXXXXXXXXX ;XXX ;X;')
		cabecalho.append('&      ;CODIGO;NOME: Usina, Barra e Tipo de Usina       ;PMAX       ;FCAP;C;')
		cabecalho.append('&XXXXXX;XXXXX ;XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX ;XXXXXXXXXX ;XXX ;X;')

	if mnemonico == 'EOLICABARRA':
		cabecalho.append('&XXXXXXXXXX ;XXXXX ;XXXXX ;')
		cabecalho.append('&           ;CODIGO;BARRA ;')
		cabecalho.append('&XXXXXXXXXX ;XXXXX ;XXXXX ;')

	if mnemonico == 'EOLICASUBM':
		cabecalho.append('&XXXXXXXXXX;XXXXX ;XX ;')
		cabecalho.append('&          ;CODIGO;SBM;')
		cabecalho.append('&XXXXXXXXXX;XXXXX ;XX ;')
	
	if mnemonico == 'EOLICA-GERACAO':
		cabecalho.append('&XXXXXXXXXXXXXX;XXXXX ;XX ;XX ;X ;XX ;XX ;X ;XXXXXXXXXX ;')
		cabecalho.append('&              ;CODIGO;       DATA          ;   GERACAO ;')
		cabecalho.append('&XXXXXXXXXXXXXX;XXXXX ;XX ;XX ;X ;XX ;XX ;X ;XXXXXXXXXX ;')

	return cabecalho

def formatarBloco(df_bloco, formatacao):

	bloco = []
	for index, row in df_bloco.iterrows():
		bloco.append(formatacao.format(*row.values))

	return bloco


def gravarArquivo(pathSaida, valores):
	fileOut = codecs.open(pathSaida, 'a+', 'utf-8')

	for linha in valores:
		fileOut.write('{}\n'.format(linha.strip()))

	fileOut.close()

 
def tratarBlocoGeracao(data, renovaveis_base, pathArqEntrada, debug=False):

	dt_base = data  - datetime.timedelta(days=1)
	dataInicioPrevisao = data  + datetime.timedelta(days=1)
	ultima_rv_date = wx_opweek.getLastSaturday(data.date())
	prox_sexta = ultima_rv_date + datetime.timedelta(days=6)
	prox_sab = ultima_rv_date + datetime.timedelta(days=7)

	infoBlocos = getInfoBlocos()
	baseRenovaveis = leituraArquivo(renovaveis_base)

	pathDeckEolicas = os.path.join(pathArqEntrada, 'eolicas')

	blocoEOLICA = extrairInfoBloco(baseRenovaveis, 'EOLICA', infoBlocos['EOLICA']['regex'])
	df_blocoEolica = pd.DataFrame(blocoEOLICA, columns=infoBlocos['EOLICA']['campos'])

	blocoEOLICABARRA = extrairInfoBloco(baseRenovaveis, 'EOLICABARRA', infoBlocos['EOLICABARRA']['regex'])
	df_blocoEolicabarra = pd.DataFrame(blocoEOLICABARRA, columns=infoBlocos['EOLICABARRA']['campos'])

	blocoEOLICASUBM = extrairInfoBloco(baseRenovaveis, 'EOLICASUBM', infoBlocos['EOLICASUBM']['regex'])
	df_blocoEolicasubm = pd.DataFrame(blocoEOLICASUBM, columns=infoBlocos['EOLICASUBM']['campos'])

	blocoEOLICAGERACAO = extrairInfoBloco(baseRenovaveis, 'EOLICA-GERACAO', infoBlocos['EOLICA-GERACAO']['regex'])
	df_blocoEolicaGeracao = pd.DataFrame(blocoEOLICAGERACAO, columns=infoBlocos['EOLICA-GERACAO']['campos'])

	# usinas que sao sempre zeradas/constantes ou estao no WEOL, mas os valores sempre sao zerados
	# precisamos continuar a  monitorar !!
	zeradas_constantes = [14,21,23,24,25,26,28,29,30,31,33,34,35,36,41,42,52,53,55,56,58,67,68,69,70,71,74,91,92,117,118,119, 278,
						279,280,281,282,283,284,285,286,287,288,289,291,292,293,297,298,301,302,303,304,305,341,342,
						392,393,394,395,396,397,398,399,400,403,404,405,406,407,413,414,415,416,417,418,419,420,421,
						422,423,424,426,427,428,429,430,431,432,433,434,435,436,437,438,452,453,454,476,477,478,479,480,
						495,496,497,498,499,500,501,502,503,504,505,506,507,521,522,523,555,556,557,558,559,560,561,562,
						563,564,565,573,574,575,578,579,580,581, 583,584,585,586,587,588,589,590,591,596,597,598,599]  
	

	# informacoes para usinas do WEOL
	pathUsinaBarraRateio = os.path.join(pathDeckEolicas, 'Arquivos Entrada', 'Usina Barra Rateio.txt')
	pathSaidaEolicas = os.path.join(pathDeckEolicas, 'Previsoes por Usinas', 'Previsao combinada')

	df_usinBarraRateio = pd.read_csv(pathUsinaBarraRateio,delimiter=";")
	nomeColunas=df_usinBarraRateio.columns.tolist()
	df_usinBarraRateio['nomecomp'] = df_usinBarraRateio[nomeColunas[0:2]].agg(' '.join,axis=1)
	df_usinBarraRateio['nomecomp'] = df_usinBarraRateio['nomecomp'].str.strip()
	df_usinBarraRateio['nomecomp'] = df_usinBarraRateio['nomecomp'].str.replace(' ','_')

	df_blocoEolica['barra'] = df_blocoEolicabarra['barra']
	df_blocoEolica['submercado'] = df_blocoEolicasubm['sbm']

	df_blocoEolicaGeracao['di'] = df_blocoEolicaGeracao['di'].astype(int)
	df_blocoEolicaGeracao['df'] = df_blocoEolicaGeracao['df'].astype(int)
	df_blocoEolicaGeracao['codigo'] = df_blocoEolicaGeracao['codigo'].astype(int)

	bloco_GERACAO = []
	for i, usinaInfo in df_blocoEolica.iterrows():
		df_resumo_barra = df_usinBarraRateio[df_usinBarraRateio['BARRA'] == int(usinaInfo['barra'])]
		c1 = df_blocoEolicaGeracao['codigo'] == int(usinaInfo['codigo'])
		# nao e WEOL
		if df_resumo_barra.empty and int(usinaInfo['codigo']) not in zeradas_constantes:
			c2 = df_blocoEolicaGeracao['di'] >= int(dataInicioPrevisao.strftime('%d'))
			# constantes ate prox sexta
			if not df_blocoEolicaGeracao[c1 & c2].empty: 
				if debug:
					print(usinaInfo['codigo'],'ainda nao sei de onde vem, vou pegar o do renovaveis do d-1 (2)')
				df = df_blocoEolicaGeracao[c1 & c2].copy()
				# Geracao nao iniciada na hora 0
				if int(df.iloc[0]['hi']) != 0 or int(df.iloc[0]['mi']) != 0:
					horaZero = df_blocoEolicaGeracao[c1 & ~c2].iloc[-1].copy()
					horaZero['di'] = int(dataInicioPrevisao.strftime('%d'))
					horaZero['hi'] = 0
					horaZero['mi'] = 0
					horaZero['df'] = df.iloc[0]['di']
					horaZero['hf'] = df.iloc[0]['hi']
					horaZero['mf'] = df.iloc[0]['mi']
					df = pd.concat([ horaZero.to_frame().T, df], ignore_index=True)

			elif len(df_blocoEolicaGeracao[c1]) == 1:
				if debug:
					print(usinaInfo['codigo'],'constante na semana')

				df = df_blocoEolicaGeracao[c1].copy()
				df['di'] = int(dataInicioPrevisao.strftime('%d'))
				df['df'] = int(prox_sab.strftime('%d'))
				df[['hf','mf']] = 0
			# df['geracao'] = df['geracao'].astype(float) #?? descomentar p ficar igual o do Edson

		elif int(usinaInfo['codigo']) in zeradas_constantes:
			if debug:
				print(usinaInfo['codigo'],'Da lista zeradas ou constantes')

			df = df_blocoEolicaGeracao[c1].copy()
			df['di'] = int(dataInicioPrevisao.strftime('%d'))
			df['df'] = int(prox_sab.strftime('%d'))
			df[['hf','mf']] = 0
			
			# df['geracao'] = df['geracao'].astype(float) #?? descomentar p ficar igual o do Edson
		
		else:
			if debug:
				print(usinaInfo['codigo'],'WEOL')
			df = pd.DataFrame(columns= infoBlocos['EOLICA-GERACAO']['campos'])

			if not df_resumo_barra.empty:

				dife = int(prox_sexta.strftime('%d'))-int(dt_base.strftime('%d'))
				for idx in range(1,dife):
					dt_prev = data + datetime.timedelta(days=idx)
					
					pathPrevisoesNE = os.path.join(pathSaidaEolicas, 'Previsoes_NE_{}_{}.txt'.format(data.strftime('%Y%m%d'), dt_prev.strftime('%Y%m%d')))
					df_NE = pd.read_csv(pathPrevisoesNE, delimiter=';',encoding='ISO-8859-1',header=None)
					
					pathPrevisoesS = os.path.join(pathSaidaEolicas, 'Previsoes_S_{}_{}.txt'.format(data.strftime('%Y%m%d'), dt_prev.strftime('%Y%m%d')))
					df_S = pd.read_csv(pathPrevisoesS, delimiter=';',encoding='ISO-8859-1',header=None)
					
					df_tot = pd.concat([df_NE,df_S],ignore_index=True)
					# retira acentos da coluna com nomes 
					df_tot.iloc[:,0] = df_tot.iloc[:][0].apply(unidecode.unidecode)
					df_resultado = pd.DataFrame()

					for i, rowResumo in df_resumo_barra.iterrows():

						usi = rowResumo['nomecomp']

						# procurando nome das barras no arquivo de preveolicas 
						cond1 = df_tot[0] == usi[:27]

						resultado = round(df_tot[cond1].iloc[:,1:]*(rowResumo['PERCENTUAL RATEIO']/100))
						resultado.columns = pd.date_range(start=dt_prev, periods=48, freq='30min')
						df_resultado = pd.concat([df_resultado,resultado])
					
					
					# problema com o posto 259
					# if int(usinaInfo['codigo']) == 259:
					# 	pdb.set_trace()
					if len(df_resumo_barra['nomecomp']) > 1: #?? Existe um problema aqui, por exemplo, o codigo 259 para o dia 07/01
						df_resultado = df_resultado.sum().to_frame().T

					df_resultado = df_resultado.T.reset_index()
					df_resultado = df_resultado.rename(columns={"index": "inicio"})
					df_resultado['final'] = df_resultado['inicio'] + datetime.timedelta(minutes=30)

					df_resultado['mnemonico'] = 'EOLICA-GERACAO'
					df_resultado['codigo'] = int(usinaInfo['codigo'])
					df_resultado['di'] = df_resultado['inicio'].dt.strftime('%d').astype(int)
					df_resultado['hi'] = df_resultado['inicio'].dt.strftime('%H').astype(int)
					df_resultado['mi'] = df_resultado['inicio'].dt.strftime('%M')
					df_resultado['mi'] = df_resultado['mi'].replace({'00':0, '30':1})

					df_resultado['df'] = df_resultado['final'].dt.strftime('%d').astype(int)
					df_resultado['hf'] = df_resultado['final'].dt.strftime('%H').astype(int)
					df_resultado['mf'] = df_resultado['mi'].replace({1:0, 0:1})
					
					# ?? Descomente as 2 linhas abaixo para ficar com a saida exatamente igual ao do edson
					# df_resultado['hf'] = df_resultado['hi'] 	# ?? deletar
					# df_resultado.loc[(df_resultado['hf']==23) & (df_resultado['mi'] == 1), 'hf'] = 0 # ??Deletar

					colunaGeracao = df_resultado.columns[1]
					df_resultado['geracao'] = df_resultado[colunaGeracao]
					df = df.append(df_resultado.drop(['inicio', 'final' ,colunaGeracao], axis=1))
					
					# ?? Descomente a linha abaixo abaico para ficar com a saida exatamente igual ao do edson
					# df['geracao'] = df['geracao'].astype(int)
		bloco_GERACAO += formatarBloco(df, infoBlocos['EOLICA-GERACAO']['formatacao'])

	return bloco_GERACAO


def gerarRenovaveis(data, pathArquivos, debug=False):

	pathArqEntrada = os.path.join(pathArquivos, data.strftime('%Y%m%d'), 'entrada')
	if not os.path.exists(pathArqEntrada):
		os.makedirs(pathArqEntrada)

	pathArqSaida = os.path.join(pathArquivos, data.strftime('%Y%m%d'), 'saida')
	if not os.path.exists(pathArqSaida):
		os.makedirs(pathArqSaida)

	path_ultimodeck = os.path.join(pathArqEntrada, 'ccee_entrada')

	# arquivo do renovaveis do d-1
	renovaveis_base = os.path.join(path_ultimodeck, 'renovaveis.dat')
	baseRenovaveis = leituraArquivo(renovaveis_base)

	pathRenovaveisOut = os.path.join(pathArqSaida, 'renovaveis.dat')
	fileOut = open(pathRenovaveisOut, 'w')
	fileOut.close()

	infoBlocos = getInfoBlocos()

	blocoEOLICA = extrairInfoBloco(baseRenovaveis, 'EOLICA', infoBlocos['EOLICA']['regex'])
	df_blocoEolica = pd.DataFrame(blocoEOLICA, columns=infoBlocos['EOLICA']['campos'])
	bloco_EOLICA = getCabecalhos('EOLICA')
	bloco_EOLICA += formatarBloco(df_blocoEolica, infoBlocos['EOLICA']['formatacao'])
	gravarArquivo(pathRenovaveisOut, bloco_EOLICA)

	blocoEOLICABARRA = extrairInfoBloco(baseRenovaveis, 'EOLICABARRA', infoBlocos['EOLICABARRA']['regex'])
	df_blocoEolicabarra = pd.DataFrame(blocoEOLICABARRA, columns=infoBlocos['EOLICABARRA']['campos'])
	bloco_EOLICABARRA = getCabecalhos('EOLICABARRA')
	bloco_EOLICABARRA += formatarBloco(df_blocoEolicabarra, infoBlocos['EOLICABARRA']['formatacao'])
	gravarArquivo(pathRenovaveisOut, bloco_EOLICABARRA)

	blocoEOLICASUBM = extrairInfoBloco(baseRenovaveis, 'EOLICASUBM', infoBlocos['EOLICASUBM']['regex'])
	df_blocoEolicasubm = pd.DataFrame(blocoEOLICASUBM, columns=infoBlocos['EOLICASUBM']['campos'])
	bloco_EOLICASUBM = getCabecalhos('EOLICASUBM')
	bloco_EOLICASUBM += formatarBloco(df_blocoEolicasubm, infoBlocos['EOLICASUBM']['formatacao'])
	gravarArquivo( pathRenovaveisOut, bloco_EOLICASUBM)

	bloco_GERACAO = getCabecalhos('EOLICA-GERACAO')
	bloco_GERACAO += tratarBlocoGeracao(data, renovaveis_base, pathArqEntrada)
	gravarArquivo(pathRenovaveisOut, bloco_GERACAO)

	print('renovaveis.dat: {}'.format(pathRenovaveisOut))

def gerarRenovaveisDef(data, pathArquivos, debug=False):

	dataRodada = data
	data = data + datetime.timedelta(days=1)
	pathArqEntrada = os.path.join(pathArquivos, data.strftime('%Y%m%d'), 'entrada')

	pathArqSaida = os.path.join(pathArquivos, dataRodada.strftime('%Y%m%d'), 'definitiva')
	if not os.path.exists(pathArqSaida):
		os.makedirs(pathArqSaida)

	pathRenovOut = os.path.join(pathArqSaida,'renovaveis.dat')
	fileOut = open(pathRenovOut, 'w')
	fileOut.close()

	pathRenovIn = os.path.join(pathArqEntrada, 'ons_entrada_saida', 'renovaveis.dat')
	renovaveis = leituraArquivo(pathRenovIn)

	infoBlocos = getInfoBlocos()

	blocoEOLICA = extrairInfoBloco(renovaveis, 'EOLICA', infoBlocos['EOLICA']['regex'])
	df_blocoEolica = pd.DataFrame(blocoEOLICA, columns=infoBlocos['EOLICA']['campos'])
	df_blocoEolica['c'] = 0
	bloco_EOLICA = getCabecalhos('EOLICA')
	bloco_EOLICA += formatarBloco(df_blocoEolica, infoBlocos['EOLICA']['formatacao'])
	gravarArquivo(pathRenovOut, bloco_EOLICA)
	
	gravarArquivo(pathRenovOut, getCabecalhos('EOLICABARRA'))
	gravarArquivo(pathRenovOut, renovaveis['EOLICABARRA'])
	gravarArquivo(pathRenovOut, getCabecalhos('EOLICASUBM'))
	gravarArquivo(pathRenovOut, renovaveis['EOLICASUBM'])
	gravarArquivo(pathRenovOut, getCabecalhos('EOLICA-GERACAO'))
	gravarArquivo(pathRenovOut, renovaveis['EOLICA-GERACAO'])

	print('renovaveis.dat: {}'.format(pathRenovOut))
	return pathRenovOut
	


if '__main__' == __name__:

	diretorioRaiz = os.path.abspath('../../../')
	pathLibUniversal = os.path.join(diretorioRaiz,'bibliotecas')
	sys.path.insert(1, pathLibUniversal)

	import wx_opweek

	data = datetime.datetime.now()
	# data = datetime.datetime(2021,1,28)
	path = os.path.abspath(r'C:\Users\thiag\Documents\git\wx_alpha_\apps\dessem\arquivos')
	gerarRenovaveis(data, path)


	# path_ultimodeck = 'C:/Users/Edson Y Barbosa/Documents/rodadas_preco/CCEE/' + dataini.strftime('%Y') + '/DES_' + dataini.strftime('%Y%m') + '/DS_CCEE_' + dataini.strftime('%m%Y') + '_SEMREDE_' + rev +'D' + dt_ontem.strftime('%d') 
	# deck_eol = 'C:/Users/Edson Y Barbosa/Documents/Python Scripts/dessem/Deck_Previsao_' + dt_ontem.strftime('%Y%m%d') + '/Deck_Previsao_' + dt_ontem.strftime('%Y%m%d') 
	# path_out = 'C:/Users/Edson Y Barbosa/Documents/Python Scripts/dessem'
	
	# renovaveis(deck_eol,path_ultimodeck,path_out,dataini)
