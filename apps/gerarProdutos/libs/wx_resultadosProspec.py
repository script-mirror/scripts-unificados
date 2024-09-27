import re
import os
import pdb
import sys
import time
import locale
import shutil
import zipfile
import datetime
import base64
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

try:
	locale.setlocale(locale.LC_ALL, 'pt_BR')
except:
	# locale.setlocale(locale.LC_ALL, 'Portuguese_Brazil')
	locale.setlocale(locale.LC_ALL, '')

sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.bibliotecas import wx_opweek,wx_emailSender

siglas = {'SUDESTE':'SE','SUL':'S','NORDESTE':'NE','NORTE':'N'}

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



def gerarEmailResultadosProspec(pathCompiladoZip, nomeRodadaOriginal='', considerarPrevs='', considerarrv='',gerarMatiz=0, fazer_media_rvs=0):
	
	submercados = ['SUDESTE','SUL','NORDESTE','NORTE']
	pathCompilados = eval(pathCompiladoZip)
	if nomeRodadaOriginal == '':
		nomeRodadaOriginal = ['']*len(pathCompilados)
	else:
		nomeRodadaOriginal = eval(nomeRodadaOriginal)
	pathLocal = os.path.abspath('.')
	dir_saida = os.path.join(pathLocal, 'arquivos', 'tmp')
	if gerarMatiz == 1:
		return  gerarImageMatriz(pathCompilados, nomeRodadaOriginal, considerarrv, dir_saida, fazer_media_rvs)

	else:
		print('Gerando e-mail')

	compila_ena = pd.DataFrame()
	compila_eaInicial = pd.DataFrame()
	compila_cmo_medio = pd.DataFrame()
	compila_preco_medio_nw = pd.DataFrame()
	compila_enaPercentualMensal = pd.DataFrame()
	compila_enaMensal = pd.DataFrame()
	num_estudo = []

	for i, compil in enumerate(pathCompilados):
		pathCompilado = extractFiles(os.path.abspath(compil))

		nomeEstudo = os.path.basename(pathCompilado)
		match = re.match('Estudo_([0-9]{1,})_compilation',nomeEstudo)
		numeroEstudo = match.group(1)
		num_estudo.append(numeroEstudo)

		df = pd.read_csv(os.path.join(pathCompilado, 'compila_cmo_medio.csv'), sep=';')
		df['Sensibilidade'] = df['Sensibilidade'].replace({'Original':nomeRodadaOriginal[i]})
		df_mensal = df[df['MEN=0-SEM=1'] == 0].copy()
		df = df[df['MEN=0-SEM=1'] == 1]
		df['Sensibilidade'] = df['Sensibilidade'] + '_id'+ numeroEstudo


		if fazer_media_rvs:
			df['Deck_'] = df['Deck'].str[:-3]
			for deck_, dados in df.groupby('Deck_').mean().iterrows():
				df.loc[df['Deck'] == '{}_s1'.format(deck_), 'SUDESTE'] = dados['SUDESTE']
				df.loc[df['Deck'] == '{}_s1'.format(deck_), 'SUL'] = dados['SUL']
				df.loc[df['Deck'] == '{}_s1'.format(deck_), 'NORDESTE'] = dados['NORDESTE']
				df.loc[df['Deck'] == '{}_s1'.format(deck_), 'NORTE'] = dados['NORTE']

		compila_cmo_medio = compila_cmo_medio.append(df)
  
		# Colocando o preco medio das 2000 do NW
		df_nw = df.copy()
		df_nw['mes_NW'] = df_nw['Deck'].str[2:8]
		df_nw.set_index('mes_NW', inplace=True)

		df_mensal['mes_NW'] = df_mensal['Deck'].str[2:8]
		df_mensal.set_index('mes_NW', inplace=True)
		df_mensal.drop(['Deck','Sensibilidade', 'MEN=0-SEM=1'],axis=1,inplace=True)

		df_nw.update(df_mensal)
		compila_preco_medio_nw = compila_preco_medio_nw.append(df_nw)

		df = pd.read_csv(os.path.join(pathCompilado, 'compila_ea_inicial.csv'), sep=';')
		df['Sensibilidade'] = df['Sensibilidade'].replace({'Original':nomeRodadaOriginal[i]})
		df = df[df['MEN=0-SEM=1'] == 1]
		df['Sensibilidade'] = df['Sensibilidade'] + '_id'+ numeroEstudo
		compila_eaInicial = compila_eaInicial.append(df)

		df = pd.read_csv(os.path.join(pathCompilado, 'compila_ena.csv'), sep=';')
		df['Sensibilidade'] = df['Sensibilidade'].replace({'Original':nomeRodadaOriginal[i]})
		df = df[df['MEN=0-SEM=1'] == 1]
		df['Sensibilidade'] = df['Sensibilidade'] + '_id'+ numeroEstudo
		compila_ena = compila_ena.append(df)

		df = pd.read_csv(os.path.join(pathCompilado, 'compila_ena_mensal_percentual.csv'), sep=';')
		df['Sensibilidade'] = df['Sensibilidade'].replace({'Original':nomeRodadaOriginal[i]})		
		df = df[df['MEN=0-SEM=1'] == 1]
		df['Sensibilidade'] = df['Sensibilidade'] + '_id'+ numeroEstudo
		compila_enaPercentualMensal = compila_enaPercentualMensal.append(df)

		df = pd.read_csv(os.path.join(pathCompilado, 'compila_ena_mensal.csv'), sep=';')
		df['Sensibilidade'] = df['Sensibilidade'].replace({'Original':nomeRodadaOriginal[i]})		
		df = df[df['MEN=0-SEM=1'] == 1]
		df['Sensibilidade'] = df['Sensibilidade'] + '_id'+ numeroEstudo

		compila_enaMensal = compila_enaMensal.append(df)

	

	if considerarrv !='':
		compila_cmo_medio               = compila_cmo_medio[compila_cmo_medio['Deck'].str.contains(considerarrv)]
		compila_preco_medio_nw			= compila_preco_medio_nw[compila_preco_medio_nw['Deck'].str.contains(considerarrv)]
		compila_eaInicial               = compila_eaInicial[compila_eaInicial['Deck'].str.contains(considerarrv)]
		compila_ena                     = compila_ena[compila_ena['Deck'].str.contains(considerarrv)]
		compila_enaMensal               = compila_enaMensal[compila_enaMensal['Deck'].str.contains(considerarrv)]
		compila_enaPercentualMensal     = compila_enaPercentualMensal[compila_enaPercentualMensal['Deck'].str.contains(considerarrv)]
		if fazer_media_rvs:
			compila_cmo_medio           = compila_cmo_medio[compila_cmo_medio['Deck'].str.contains('s1')]
			compila_preco_medio_nw		= compila_preco_medio_nw[compila_preco_medio_nw['Deck'].str.contains('s1')]
			compila_eaInicial           = compila_eaInicial[compila_eaInicial['Deck'].str.contains('s1')]
			compila_ena                 = compila_ena[compila_ena['Deck'].str.contains('s1')]
			compila_enaMensal           = compila_enaMensal[compila_enaMensal['Deck'].str.contains('s1')]
			compila_enaPercentualMensal = compila_enaPercentualMensal[compila_enaPercentualMensal['Deck'].str.contains('s1')]


	dicionarioDecks = {}
	x_label = []
	for dk in compila_cmo_medio['Deck'].unique():
		mes, sem, encadeamento = re.findall('[0-9]+', dk)
		inicioMesEletrico = wx_opweek.getLastSaturday(datetime.datetime.strptime(mes, '%Y%m').date())
		dt = wx_opweek.ElecData(inicioMesEletrico + datetime.timedelta(days=7*(int(sem)+int(encadeamento)-2)))
		dicionarioDecks[dk] = dt.data
		label = 'DC_{}-rv{}'.format(dt.data.strftime('%Y%m'),dt.atualRevisao)
		if label not in x_label:
			x_label.append(label)

	compila_cmo_medio['Deck'] = compila_cmo_medio['Deck'].replace(dicionarioDecks)
	compila_preco_medio_nw['Deck'] = compila_preco_medio_nw['Deck'].replace(dicionarioDecks)
	compila_eaInicial['Deck'] = compila_eaInicial['Deck'].replace(dicionarioDecks)
	compila_ena['Deck'] = compila_ena['Deck'].replace(dicionarioDecks)
	compila_enaPercentualMensal['Deck'] = compila_enaPercentualMensal['Deck'].replace(dicionarioDecks)

	#print(compila_cmo_medio)
	cmo = {}
	precoNw = {}
	ear = {}
	ena = {}
	enaPercentMensal = {}
	ordem = compila_enaMensal['Sensibilidade'].unique().tolist()

	pathFileOut = os.path.join(dir_saida, 'RODADAS.PNG')
	fig = plt.figure(figsize=(12,6))
	for sub in submercados:

		cmo[sub] = compila_cmo_medio.pivot(index='Sensibilidade', columns='Deck', values=sub).reindex(ordem)
		precoNw[sub] = compila_preco_medio_nw.pivot(index='Sensibilidade', columns='Deck', values=sub).reindex(ordem)
		ear[sub] = compila_eaInicial.pivot(index='Sensibilidade', columns='Deck', values=sub).reindex(ordem)
		ena[sub] = compila_ena.pivot(index='Sensibilidade', columns='Deck', values=sub).reindex(ordem)
		enaPercentMensal[sub] = compila_enaPercentualMensal.pivot(index='Sensibilidade', columns='Deck', values=sub).reindex(ordem)
		
		if sub == 'SUDESTE':
			cmoAux = []
			sens = []
			for key in cmo[sub].T.keys():
				cmoAux.append(cmo[sub].T[key].values.tolist())
				sens.append(key)
			

		cmo[sub] = cmo[sub].applymap(lambda x: "{:.0f}".format(x).replace('.',','))
		precoNw[sub] = precoNw[sub].applymap(lambda x: "{:.0f}".format(x).replace('.',','))
		ear[sub] = ear[sub].applymap(lambda x: "{:.0f}".format(x).replace('.',','))
		ena[sub] = ena[sub].applymap(lambda x: "{:.1f}".format(x/1000))
		enaPercentMensal[sub] = enaPercentMensal[sub].applymap(lambda x: "{:.0f}".format(x).replace('.',','))
	
		# Simulacoes nao encadeadas nao vao apresentar todas as colunas
		enaPercentMensal[sub] = enaPercentMensal[sub].reindex(columns = cmo[sub].columns)    
	
	if considerarPrevs == '':
		dtRvs = cmo['SUDESTE'].columns
	else:
		dtRvs = eval('cmo[\'SUDESTE\'].columns'+considerarPrevs)
		# Caso for apenas um estudo e necessario transformar em lista
		if type(dtRvs) == str:
			dtRvs = [dtRvs]
		if type(dtRvs) == datetime.date:
			dtRvs = [dtRvs]

	msg_whats = ''
	nome_modelo_envio_whats = 'ons_pluvia-preliminar_pluvia'
	if cmo['SUDESTE'].index.str.contains(nome_modelo_envio_whats).any():
		idx = cmo['SUDESTE'].loc[cmo['SUDESTE'].index.str.contains(nome_modelo_envio_whats)].iloc[0].name
		msg_whats = '\`\`\`CMO ({})\\n'.format(datetime.datetime.now().strftime('%d/%m/%Y'))
		msg_whats += '{}\\n'.format(idx)
		for dt in cmo['SUDESTE'].columns:
			dt_op = wx_opweek.ElecData(dt)
			msg_whats += '{} - RV{}\\n{:->16}\\n'.format(dt_op.data.strftime('%b/%y'), dt_op.atualRevisao,'')
			for sub in cmo:
				msg_whats += '| {:>3} | {:>6} |\\n'.format(siglas[sub], cmo[sub].loc[idx,dt])
			
			msg_whats += '{:->16}\\n\\n'.format('')  
		msg_whats += '\`\`\`'

	dts = []
	cmoResumo = pd.DataFrame()
	precoNwResumo = pd.DataFrame()
	earResumo = pd.DataFrame()
	enasResumo = pd.DataFrame()
	enasMensaisResumo = pd.DataFrame()
	legendGrafico = []
	for dt in dtRvs:
		dt = wx_opweek.ElecData(dt)
		dts.append(dt)

		# dataRodadaHtml = '{}<br><small>(RV{})</small>'.format(dt.data.strftime('%d/%m/%Y'), dt.atualRevisao)
		dt_rv = datetime.datetime(dt.anoReferente, dt.mesReferente, 1)
		legendGrafico.append('{} - RV{}'.format(dt_rv.strftime('%b/%y'), dt.atualRevisao))
		dataRodadaHtml = '{} - RV{}'.format(dt_rv.strftime('%b/%y'), dt.atualRevisao) 

		for sub in submercados:

			if sub == 'SUDESTE':
				earResumo[dataRodadaHtml] = ear[sub][dt.data].astype('str')
				cmoResumo[dataRodadaHtml] = cmo[sub][dt.data].astype('str')
				enasResumo[dataRodadaHtml] = ena[sub][dt.data].astype('str')
				precoNwResumo[dataRodadaHtml] = precoNw[sub][dt.data].astype('str')
				enasMensaisResumo[dataRodadaHtml] = enaPercentMensal[sub][dt.data].astype('str')
			else:
				earResumo[dataRodadaHtml] += '-'+ear[sub][dt.data].astype('str')
				cmoResumo[dataRodadaHtml] += '-'+cmo[sub][dt.data].astype('str')
				enasResumo[dataRodadaHtml] += '-'+ena[sub][dt.data].astype('str')
				precoNwResumo[dataRodadaHtml] += '-'+precoNw[sub][dt.data].astype('str')
				enasMensaisResumo[dataRodadaHtml] += '-'+enaPercentMensal[sub][dt.data].astype('str')

	tituloGrafico = 'PLD SE ({})'.format(datetime.datetime.now().strftime('%d/%m/%Y'))
	fig = plt.figure(figsize=(12,6))
	fig.add_axes([0.05, 0.2, 0.75, 0.7]) 
	for list in cmoAux:
		plt.plot(list)
	plt.gca().set_xticks(np.arange(len(cmoAux[0])))
	plt.gca().set_xticklabels(legendGrafico)	
	plt.gca().legend(sens, loc='center left', bbox_to_anchor=(1, 0.5), fontsize=7)			
	plt.title(tituloGrafico)
	plt.xticks(rotation=90)
	plt.ylabel("R$")
	plt.xlabel("RV")
	plt.grid()
	plt.savefig(pathFileOut)

	resumoTabela1 = enasMensaisResumo+'<br>'+earResumo+'<br>'+enasResumo+'<br>'+precoNwResumo+'<br>'+cmoResumo
	resumoTabela1.insert(0, "", ['ENA-Mês(%)<br>EAR(%)<br>ENA-S1(GW)<br>PLD NW(R$)<br>PLD DC(R$)']*resumoTabela1.shape[0])
	resumoTabela1 = resumoTabela1.applymap(lambda x: x.replace('nan-nan-nan-nan','-'))

	titulo = '[Rodada] - Prospec RV{}'.format(dts[0].atualRevisao)
	html = "</div>"
	html += "<div class=\"row\" style=\"display: flex;margin-left:-5px;margin-right:-5px;\">"
	html += "<div class=\"column\" style=\'padding: 5px; text-align: center;\' >"
	header_tabela = resumoTabela1.reset_index().columns.tolist()
	header_tabela_sub = []
	for i_h, h in enumerate(header_tabela):
		if i_h>=2:
			header_tabela_sub.append(f'{h}<br>SE-S-NE-N')
		else:
			header_tabela_sub.append(h)
	corpo_tabela = resumoTabela1.reset_index().values.tolist()
	html += wx_emailSender.gerarTabela(body=corpo_tabela, header=header_tabela_sub, widthColunas=[200,80]+[120]*(resumoTabela1.shape[1]-1))
	html += "<br/>"
	html += "<br/>"
	html += "<br/>"
	html += "<body>"
	html += "<center>"
	html += "<img src=data:image/png;base64,{image}>"
	html += "{caption}"
	html += "<center>"
	html += "<body>"
	html += "<br/>"
	html += "<br/>"
	html += '<p>Compilado utilizado na geração desse email:<br>{}<p>'.format(pathCompiladoZip)

	with open(pathFileOut, 'rb') as image:
		f = image.read()
		image_bytes = bytearray(f)

	image = base64.b64encode(image_bytes).decode('ascii')
	_ = html
	_ = _.format(image = image, caption = '')
	html = html.format(image = base64.b64encode(image_bytes).decode('ascii'), caption = '')


	return html, titulo, pathFileOut, msg_whats

def gerarImageMatriz(pathCompilados, nomeRodadaOriginal, considerarrv, dir_saida, fazer_media_rvs):	
	pathMatriz = []
	for i, compil in enumerate(pathCompilados):

		compila_ena = pd.DataFrame()
		compila_eaInicial = pd.DataFrame()
		compila_cmo_medio = pd.DataFrame()
		compila_enaPercentualMensal = pd.DataFrame()
		compila_enaPercentualMensal1 = pd.DataFrame()
		compila_enaMensal = pd.DataFrame()
		compila_enaMensal1 = pd.DataFrame()

		pathCompilado = extractFiles(os.path.abspath(compil))

		nomeEstudo = os.path.basename(pathCompilado)
		match = re.match('Estudo_([0-9]{1,})_compilation',nomeEstudo)
		numeroEstudo = match.group(1)

		df = pd.read_csv(os.path.join(pathCompilado, 'compila_cmo_medio.csv'), sep=';')
		df['Sensibilidade'] = df['Sensibilidade'].replace({'Original':nomeRodadaOriginal[i]})
		df = df[df['MEN=0-SEM=1'] == 1]
		df['Sensibilidade'] = df['Sensibilidade'] + '_id'+ numeroEstudo

		if fazer_media_rvs:
			considerarrv = 'sem1_s1'
			df['Deck_'] = df['Deck'].str[:-3]
			for deck_, dados in df.groupby(['Deck_','Sensibilidade']).mean().iterrows():
				cond1 = df['Deck'] == '{}_s1'.format(deck_[0])
				cond2 = df['Sensibilidade'] == deck_[1]
				df.loc[cond1&cond2, 'SUDESTE'] = dados['SUDESTE']
				df.loc[cond1&cond2, 'SUL'] = dados['SUL']
				df.loc[cond1&cond2, 'NORDESTE'] = dados['NORDESTE']
				df.loc[cond1&cond2, 'NORTE'] = dados['NORTE']
				df = df.drop('Deck_', axis=1)
		compila_cmo_medio = compila_cmo_medio.append(df)

		df = pd.read_csv(os.path.join(pathCompilado, 'compila_ea_inicial.csv'), sep=';')
		df['Sensibilidade'] = df['Sensibilidade'].replace({'Original':nomeRodadaOriginal[i]})
		df = df[df['MEN=0-SEM=1'] == 1]
		df['Sensibilidade'] = df['Sensibilidade'] + '_id'+ numeroEstudo
		compila_eaInicial = compila_eaInicial.append(df)

		df = pd.read_csv(os.path.join(pathCompilado, 'compila_ena.csv'), sep=';')
		df['Sensibilidade'] = df['Sensibilidade'].replace({'Original':nomeRodadaOriginal[i]})
		df = df[df['MEN=0-SEM=1'] == 1]
		df['Sensibilidade'] = df['Sensibilidade'] + '_id'+ numeroEstudo
		compila_ena = compila_ena.append(df)

		df = pd.read_csv(os.path.join(pathCompilado, 'compila_ena_mensal_percentual.csv'), sep=';')
		df['Sensibilidade'] = df['Sensibilidade'].replace({'Original':nomeRodadaOriginal[i]})		
		df = df[df['MEN=0-SEM=1'] == 1]
		df['Sensibilidade'] = df['Sensibilidade'] + '_id'+ numeroEstudo
		compila_enaPercentualMensal = compila_enaPercentualMensal.append(df)

		df = pd.read_csv(os.path.join(pathCompilado, 'compila_ena_mensal.csv'), sep=';')
		df['Sensibilidade'] = df['Sensibilidade'].replace({'Original':nomeRodadaOriginal[i]})		
		df = df[df['MEN=0-SEM=1'] == 1]
		df['Sensibilidade'] = df['Sensibilidade'] + '_id'+ numeroEstudo
		compila_enaMensal = compila_enaMensal.append(df)

		if considerarrv !='':
			compila_cmo_medio           = compila_cmo_medio[compila_cmo_medio['Deck'].str.contains(considerarrv)]
			compila_eaInicial           = compila_eaInicial[compila_eaInicial['Deck'].str.contains(considerarrv)]			
			compila_enaMensal1          = compila_enaMensal[compila_enaMensal['Deck'].str.contains(considerarrv)]
			compila_enaPercentualMensal1 = compila_enaPercentualMensal[compila_enaPercentualMensal['Deck'].str.contains(considerarrv)]
			compila_ena                 = compila_ena[compila_ena['Deck'].str.contains(considerarrv)]
			if len(compila_enaMensal1)>0:				
				compila_enaMensal = compila_enaMensal1
			else:
				compila_enaMensal['Deck'] =  compila_enaMensal['Deck'].replace(compila_enaMensal['Deck'][0],considerarrv)
				compila_enaPercentualMensal['Deck'] =  compila_enaPercentualMensal['Deck'].replace(compila_enaPercentualMensal['Deck'][0],considerarrv)

		# Gerar matriz
		# Preparando os dados		
		Ea_inicial         = compila_eaInicial.drop(          ['MEN=0-SEM=1', 'Unnamed: 7'], axis=1)
		enaMensal          = compila_enaMensal.drop(          ['MEN=0-SEM=1', 'Unnamed: 7'], axis=1)
		cmo_medio          = compila_cmo_medio.drop(          ['MEN=0-SEM=1', 'Unnamed: 7'], axis=1)
		enaPmes            = compila_enaPercentualMensal.drop(['MEN=0-SEM=1', 'Unnamed: 7'], axis=1)	
		enaMensal.columns  = ['Sensibilidade', 'Deck', 'ENA_MW_SUDESTE', 'ENA_MW_SUL', 'ENA_MW_NORDESTE', 'ENA_MW_NORTE']
		Ea_inicial.columns = ['Sensibilidade', 'Deck',     'EA_SUDESTE',     'EA_SUL',     'EA_NORDESTE',     'EA_NORTE']
		cmo_medio.columns  = ['Sensibilidade', 'Deck',    'PLD_SUDESTE',    'PLD_SUL',    'PLD_NORDESTE',    'PLD_NORTE']
		enaPmes.columns    = ['Sensibilidade', 'Deck',    'ENA_SUDESTE',    'ENA_SUL',    'ENA_NORDESTE',    'ENA_NORTE']
		enaPmes            = enaPmes.round()
		enaMensal          = enaMensal.round()
		cmo_medio          = cmo_medio.round()
		Ea_inicial         = Ea_inicial.round(1)	
		cmo_ena            = pd.merge(cmo_medio,  enaPmes, on=['Sensibilidade', 'Deck'])
		cmo_ena            = pd.merge(cmo_ena,  enaMensal, on=['Sensibilidade', 'Deck'])
		cmo_ena            = pd.merge(cmo_ena, Ea_inicial, on=['Sensibilidade', 'Deck'])
		mes_ano            = cmo_ena['Deck'][0][6:8]+'/'+cmo_ena['Deck'][0][2:6]
		nome_fig           = cmo_ena['Deck'][0][2:6]+'-'+cmo_ena['Deck'][0][6:8]
		cmo_ena            = cmo_ena.drop(['Deck','Sensibilidade'], axis=1).astype(int)
		cenarios_se        = sorted(list(cmo_ena['ENA_SUDESTE'   ].unique()))
		cenarios_s         = sorted(list(cmo_ena['ENA_SUL'       ].unique()))
		cenarios_ne        = sorted(list(cmo_ena['ENA_NORDESTE'  ].unique()))
		cenarios_n         = sorted(list(cmo_ena['ENA_NORTE'     ].unique()))
		cenarios_se_mw     = sorted(list(cmo_ena['ENA_MW_SUDESTE'].unique()))
		cenarios_s_mw      = sorted(list(cmo_ena['ENA_MW_SUL'    ].unique()))
		#del cenarios_s[4]
		
		# Criando dataframe de cada matriz
		dict_matrizes = dict()
		for n in cenarios_n:
			add_dict_n = True		
			for ne in cenarios_ne:

				add_dict_ne = True
				add         = False
				matriz_se   = pd.DataFrame(columns=cenarios_se, index=cenarios_s)
				matriz_s    = pd.DataFrame(columns=cenarios_se, index=cenarios_s)
				matriz_ne   = pd.DataFrame(columns=cenarios_se, index=cenarios_s)
				matriz_n    = pd.DataFrame(columns=cenarios_se, index=cenarios_s)			
				
				for se in cenarios_se:
					for s in cenarios_s:
						value = cmo_ena[(cmo_ena['ENA_SUDESTE' ] == se) & (cmo_ena['ENA_SUL'  ] == s)  &\
										(cmo_ena['ENA_NORDESTE'] == ne) & (cmo_ena['ENA_NORTE'] == n)]

						if len(value) > 0:
							add = True
							if  add_dict_n: dict_matrizes[str(n)]          = dict(); add_dict_n= False
							if add_dict_ne: dict_matrizes[str(n)][str(ne)] = dict({'SUDESTE':{},'SUL':{}, 'NORDESTE':{},'NORTE':{}}); add_dict_ne= False
							
							matriz_se.loc[s, se] = value['PLD_SUDESTE' ].values[0]
							matriz_s.loc[ s, se] = value['PLD_SUL'     ].values[0]	
							matriz_ne.loc[s, se] = value['PLD_NORDESTE'].values[0]	
							matriz_n.loc[ s, se] = value['PLD_NORTE'   ].values[0]
							#print(value)
				if add:
					dict_matrizes[str(n)][str(ne)]['SUDESTE' ] = matriz_se
					dict_matrizes[str(n)][str(ne)]['SUL'     ] = matriz_s
					dict_matrizes[str(n)][str(ne)]['NORDESTE'] = matriz_ne
					dict_matrizes[str(n)][str(ne)]['NORTE'   ] = matriz_n

		# Gerando grafico para cada sensibilidade
		text_title = nomeRodadaOriginal[i] + ', '
		for sens_n in dict_matrizes.keys():
			for sens_ne in dict_matrizes[sens_n].keys():
				pathFileOut = os.path.join(dir_saida,  nome_fig+'_'+datetime.datetime.now().strftime('%Y%m%d') + '_MATRIZ_NE-'+str(sens_ne)+ '_N-'+str(sens_n)+'_Id'+ str(numeroEstudo)+'.PNG')
				tituloGrafico = 'Id:'+ str(numeroEstudo)+'   Data:{}'.format(datetime.datetime.now().strftime('%d/%m/%Y'))
				fig, ax_all = plt.subplots(2, 2, figsize=(11,11), linewidth=5, edgecolor="#04253a")
				fig_posi = 0
				value  = cmo_ena[(cmo_ena['ENA_NORDESTE'] == int(sens_ne)) & (cmo_ena['ENA_NORTE'] == int(sens_n))]
				ena_ne = value['ENA_MW_NORDESTE' ].values[0]
				ena_n  = value['ENA_MW_NORTE' ].values[0]
				ea_ini = str(value['EA_SUDESTE' ].values[0])+'-'+str(value['EA_SUL' ].values[0])+'-'+str(value['EA_NORDESTE' ].values[0])+'-'+str(value['EA_NORTE' ].values[0])
				fig.suptitle(text_title + '  EAini: '+ ea_ini +',  NE: '+ sens_ne + '/' + str(ena_ne) + ',  N: '+ sens_n + '/' + str(ena_n) , fontsize=16)
				
				for sens in dict_matrizes[sens_n][sens_ne].keys():
					matriz_fig = np.array(dict_matrizes[sens_n][sens_ne][sens].astype(int))

					#print(dict_matrizes[sens_n][sens_ne][sens]) 				
					if   fig_posi == 0: ax = ax_all[0,0]
					elif fig_posi == 1: ax = ax_all[1,0]
					elif fig_posi == 2: ax = ax_all[0,1]
					elif fig_posi == 3: ax = ax_all[1,1]
					fig_posi += 1

					ax.imshow(matriz_fig, cmap=plt.cm.summer)
					x_values = list(dict_matrizes[sens_n][sens_ne][sens].keys())
					y_values = list(dict_matrizes[sens_n][sens_ne][sens].index)
					ax.set_xticks(np.arange(len(x_values)))
					ax.set_yticks(np.arange(len(y_values)))
					ax.set_xticklabels(x_values)
					ax.set_yticklabels(y_values)
					#ax.set_xlabel('ENA Sudeste (%)')
					#ax.set_ylabel('ENA Sul (%)')
					ax.xaxis.set_label_position('top') 
					ax.tick_params(top=True, bottom=False, labeltop=True, labelbottom=False)
					secax = ax.secondary_xaxis('bottom')
					secax.set_xticks(np.arange(len(x_values)))
					secax.set_xticklabels(cenarios_se_mw)
					#secax.set_xlabel('ENA Sudeste (MW)')
					secax = ax.secondary_yaxis('right')
					secax.set_yticks(np.arange(len(y_values)))
					secax.set_yticklabels(cenarios_s_mw)
					#secax.set_ylabel('ENA Sul (MW)')

					for i in range(len(y_values)):
						for j in range(len(x_values)):
							text = ax.text(j, i, matriz_fig[i, j],
										ha="center", va="center")

					title_obj = ax.set_title("PLD " + sens + ' '+mes_ano )
					plt.getp(title_obj)                    
					plt.getp(title_obj, 'text')            
					plt.setp(title_obj, color='r') 
					fig.tight_layout()
				ax.text(0.9, -0.05, tituloGrafico,
				horizontalalignment='center',
				verticalalignment='top',
				transform=ax.transAxes, c='r')
				plt.savefig(pathFileOut, dpi = 500, format='png', edgecolor=fig.get_edgecolor())
				pathMatriz.append(pathFileOut)
				#print('Fim')
	#return pathMatriz

	titulo = '[Rodada] - Prospec RV{}'
	html = "</div>"
	html += "<div class=\"row\" style=\"display: flex;margin-left:-5px;margin-right:-5px;\">"
	html += "<div class=\"column\" style=\'padding: 5px; text-align: center;\' >"
	html += "<br/>"
	html += "<br/>"
	html += "<body>"
	html += "<center>"
	for i in range(len(pathMatriz)):
		html += "<img src=data:image/png;base64,{image} >"
		html += "{caption}"
		html += "<center>"
		with open(pathMatriz[i], 'rb') as image:
			f = image.read()
			image_bytes = bytearray(f)
		image = base64.b64encode(image_bytes).decode('ascii')
		_ = html
		_ = _.format(image = image, caption = '')
		html = html.format(image = base64.b64encode(image_bytes).decode('ascii'), caption = '')
		html += "<br/>"
	html += "<body>"
	html += "<br/>"
	html += "<br/>"	
	html += '<p>Compilado utilizado na geração desse email:<br>{}<p>'.format(pathCompilados)
	pathFileOut = ''

	return html, titulo, pathFileOut
	

if __name__ == '__main__':

	diretorioRaiz = os.path.abspath('../../../')
	pathLibUniversal = os.path.join(diretorioRaiz,'bibliotecas')
	sys.path.insert(1, pathLibUniversal)

	#import wx_opweek
	#import wx_emailSender

	pathCompilado = '["C:/Dev/5 - GerarEmailMatriz/arquivos/deck/Estudo_9420_compilation.zip", "C:/Dev/5 - GerarEmailMatriz/arquivos/deck/Estudo_9421_compilation.zip"]'
	gerarEmailResultadosProspec(pathCompilado, considerarrv='sem1_s1')
