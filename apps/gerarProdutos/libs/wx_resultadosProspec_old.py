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
import pandas as pd
import matplotlib.pyplot as plt

# locale.setlocale(locale.LC_TIME, 'pt_BR', 'pt_BR.utf-8', 'pt_BR.utf-8', 'portuguese');
# locale.setlocale(locale.LC_TIME, 'pt_pt.UTF-8');
try:
	locale.setlocale(locale.LC_ALL, 'pt_BR')
except:
	# locale.setlocale(locale.LC_ALL, 'Portuguese_Brazil')
	locale.setlocale(locale.LC_ALL, '')

try:
	import wx_opweek
	import wx_emailSender
except:
	pass

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



def gerarEmailResultadosProspec(pathCompiladoZip, nomeRodadaOriginal='', considerarPrevs='', considerarrv=''):
	
	submercados = ['SUDESTE','SUL','NORDESTE','NORTE']

	pathCompilados = eval(pathCompiladoZip)
	if nomeRodadaOriginal == '':
		nomeRodadaOriginal = [''*len(pathCompilados)]
	else:
		nomeRodadaOriginal = eval(nomeRodadaOriginal)

	compila_ena = pd.DataFrame()
	compila_eaInicial = pd.DataFrame()
	compila_cmo_medio = pd.DataFrame()
	compila_enaPercentualMensal = pd.DataFrame()

	for i, compil in enumerate(pathCompilados):
		pathCompilado = extractFiles(os.path.abspath(compil))

		nomeEstudo = os.path.basename(pathCompilado)
		match = re.match('Estudo_([0-9]{1,})_compilation',nomeEstudo)
		numeroEstudo = match.group(1)

		df = pd.read_csv(os.path.join(pathCompilado, 'compila_cmo_medio.csv'), sep=';')
		df['Sensibilidade'] = df['Sensibilidade'].replace({'Original':nomeRodadaOriginal[i]})
		df = df[df['MEN=0-SEM=1'] == 1]
		df['Sensibilidade'] = df['Sensibilidade'] + '_id'+ numeroEstudo
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

	if considerarrv !='':
		#semRv = "sem" + str(int(considerarrv))+"_s1"
		semRv = considerarrv
		compila_cmo_medio           = compila_cmo_medio[compila_cmo_medio['Deck'].str.contains(semRv)]
		compila_eaInicial           = compila_eaInicial[compila_eaInicial['Deck'].str.contains(semRv)]
		compila_ena                 = compila_ena[compila_ena['Deck'].str.contains(semRv)]
		compila_enaPercentualMensal = compila_enaPercentualMensal[compila_enaPercentualMensal['Deck'].str.contains(semRv)]

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
	compila_eaInicial['Deck'] = compila_eaInicial['Deck'].replace(dicionarioDecks)
	compila_ena['Deck'] = compila_ena['Deck'].replace(dicionarioDecks)
	compila_enaPercentualMensal['Deck'] = compila_enaPercentualMensal['Deck'].replace(dicionarioDecks)

	primeiraRev = min(compila_cmo_medio['Deck'])
	#print(compila_cmo_medio)
	cmo = {}
	ear = {}
	ena = {}
	enaPercentMensal = {}
	ordem = compila_cmo_medio[compila_cmo_medio['Deck'] == primeiraRev]['Sensibilidade']

	pathLocal = os.path.abspath('.')
	dir_saida = os.path.join(pathLocal, 'arquivos', 'tmp')
	pathFileOut = os.path.join(dir_saida, 'RODADAS.PNG')

	for sub in submercados:

		cmo[sub] = compila_cmo_medio.pivot(index='Sensibilidade', columns='Deck', values=sub).reindex(ordem)
		ear[sub] = compila_eaInicial.pivot(index='Sensibilidade', columns='Deck', values=sub).reindex(ordem)
		ena[sub] = compila_ena.pivot(index='Sensibilidade', columns='Deck', values=sub).reindex(ordem)
		enaPercentMensal[sub] = compila_enaPercentualMensal.pivot(index='Sensibilidade', columns='Deck', values=sub).reindex(ordem)
		
		if sub == 'SUDESTE':
			tituloGrafico = 'PLD SE ({})'.format(datetime.datetime.now().strftime('%d/%m/%Y'))
			fig = plt.figure(figsize=(12,6))
			fig.add_axes([0.05, 0.2, 0.75, 0.7]) 
			plt.plot(cmo[sub].T)
			try:
				plt.xticks(ticks=cmo[sub].columns.tolist(),labels=x_label)
			except:
				pass
			#plt.gcf().set_size_inches(10, 4)
			plt.gca().legend(cmo[sub].index, loc='center left', bbox_to_anchor=(1, 0.5), fontsize=7)
			plt.xticks(rotation=90)
			plt.grid()
			plt.title(tituloGrafico)
			plt.ylabel("R$")
			plt.xlabel("RV")
			plt.savefig(pathFileOut)

		cmo[sub] = cmo[sub].applymap(lambda x: "{:.0f}".format(x).replace('.',','))
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

	dts = []
	cmoResumo = pd.DataFrame()
	earResumo = pd.DataFrame()
	enasResumo = pd.DataFrame()
	enasMensaisResumo = pd.DataFrame()
	for dt in dtRvs:
		dt = wx_opweek.ElecData(dt)
		dts.append(dt)

		dataRodadaHtml = '{}<br><small>(RV{})</small>'.format(dt.data.strftime('%d/%m/%Y'), dt.atualRevisao) 

		for sub in submercados:

			if sub == 'SUDESTE':
				earResumo[dataRodadaHtml] = ear[sub][dt.data].astype('str')
				cmoResumo[dataRodadaHtml] = cmo[sub][dt.data].astype('str')
				enasResumo[dataRodadaHtml] = ena[sub][dt.data].astype('str')
				enasMensaisResumo[dataRodadaHtml] = enaPercentMensal[sub][dt.data].astype('str')
			else:
				earResumo[dataRodadaHtml] += '-'+ear[sub][dt.data].astype('str')
				cmoResumo[dataRodadaHtml] += '-'+cmo[sub][dt.data].astype('str')
				enasResumo[dataRodadaHtml] += '-'+ena[sub][dt.data].astype('str')
				enasMensaisResumo[dataRodadaHtml] += '-'+enaPercentMensal[sub][dt.data].astype('str')

	resumoTabela1 = enasMensaisResumo+'<br>'+earResumo+'<br>'+enasResumo+'<br>'+cmoResumo
	resumoTabela1.insert(0, "", ['ENA-Mês(%)<br>EAR(%)<br>ENA-S1(GW)<br>PLD(R$)']*resumoTabela1.shape[0])
	resumoTabela1 = resumoTabela1.applymap(lambda x: x.replace('nan-nan-nan-nan','-'))

	titulo = '[Rodada] - Prospec RV{}'.format(dts[0].atualRevisao)
	html = "</div>"
	html += "<div class=\"row\" style=\"display: flex;margin-left:-5px;margin-right:-5px;\">"
	html += "<div class=\"column\" style=\'padding: 5px; text-align: center;\' >"
	html += wx_emailSender.gerarTabela(body=resumoTabela1.reset_index().values.tolist(), header=resumoTabela1.reset_index().columns.tolist(), widthColunas=[200,80]+[120]*(resumoTabela1.shape[1]-1))
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


	return html, titulo, ''



if __name__ == '__main__':

	diretorioRaiz = os.path.abspath('../../../')
	pathLibUniversal = os.path.join(diretorioRaiz,'bibliotecas')
	sys.path.insert(1, pathLibUniversal)

	import wx_opweek
	import wx_emailSender

	pathCompilado = os.path.abspath(r'C:\Users\thiag\Documents\git\wx_alpha_\apps\gerarProdutos\arquivos\tmp\Compilacao') 
	gerarEmailResultadosProspec(pathCompilado)
