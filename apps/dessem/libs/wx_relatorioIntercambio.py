import os
import pdb
import sys
import glob
import datetime 
import numpy as np
import pandas as pd


sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.bibliotecas import wx_dbClass
from PMO.scripts_unificados.apps.dessem.libs import wx_pdoOperLpp,wx_pdoRestoper,wx_pdoOperRsttab,wx_entdados

# logPath = '/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/dessem/logs/logREs.txt'
# log = open(logPath, 'a')

def resumoRestricoes(restricoes):

	restricoes['iper']  = restricoes['iper'].apply(pd.to_numeric, errors='coerce')
	restricoes['num']   = restricoes['num'].apply(pd.to_numeric, errors='coerce')
	restricoes['fator'] = restricoes['fator'].apply(pd.to_numeric, errors='coerce')
	restricoes['valor'] = restricoes['valor'].apply(pd.to_numeric, errors='coerce')
	restricoes['linf']  = restricoes['linf'].apply(pd.to_numeric, errors='coerce')
	restricoes['lsup']  = restricoes['lsup'].apply(pd.to_numeric, errors='coerce')
	#restricoes          = restricoes.loc[restricoes['iper'] <= 48]
	restricoes_re       = restricoes.loc[restricoes['cod'].str.strip().isin(['RE', 'RI60'])]

	resumoRest = {}
	for grupo, rests in restricoes_re.groupby(['iper', 'num']).__iter__():
		per = grupo[0]
		numRest = grupo[1]

		if per not in resumoRest:
			resumoRest[per] = {}

		resumoRest[per][(numRest,'linf')] = rests['linf'].min()
		resumoRest[per][(numRest,'valor')] = (rests['fator']*rests['valor']).sum()
		resumoRest[per][(numRest,'lsup')] = rests['lsup'].min()

	resumoRest_df = pd.DataFrame(resumoRest)

	#log.write('Restricoes analisadas (RE)\n')
	#log.write('{}\n'.format(resumoRest_df.index.get_level_values(0).unique().tolist()))

	return resumoRest_df

def atualizarLsupLpp(resumoRest, operLpp):

	#print('Atualizando o lsup com valores do Lpp ')
	operLpp['limCalc'] = operLpp['limCalc'].apply(pd.to_numeric, errors='coerce')

	#log.write('\n\nAtualizando o lsup com valores do Lpp\n')
	#log.write('({},({})): {}  ->  {}\n'.format('numero da restricao', 'periodo', 'valor antigo', 'novo valor'))

	for grupo, rests in operLpp.groupby(['numRe', 'iper']).__iter__():
		numeroRest = int(grupo[0])
		per = int(grupo[1])

		if numeroRest in resumoRest.index.get_level_values(0):
			#log.write('({},({})): {}  ->  {}\n'.format(numeroRest, per, resumoRest.loc[(numeroRest, 'lsup'), per], rests['limCalc'].min()))
			resumoRest.loc[(numeroRest, 'lsup'), per] = rests['limCalc'].min()

	return resumoRest


def atualizarLsupRsttab(resumoRest, operRsttab):

	#print('Atualizando o lsup com valores do Rsttab ')
	operRsttab['limiteuti'] = operRsttab['limiteuti'].apply(pd.to_numeric, errors='coerce')

	operRsttab_re = operRsttab.loc[operRsttab['tippar'].str.strip() == 'RE']

	#log.write('\n\nAtualizando o lsup com valores do Rsttab\n')
	#log.write('({},({})): {}  ->  {}\n'.format('numero da restricao', 'periodo', 'valor antigo', 'novo valor'))

	for grupo, rests in operRsttab_re.groupby(['numre', 'iper']).__iter__():
		numeroRest = int(grupo[0])
		per = int(grupo[1])

		limiteuti = rests['limiteuti'].min()

		if pd.isna(resumoRest.loc[(numeroRest, 'lsup'), per]):
			#log.write('({},({})): {}  ->  {}\n'.format(numeroRest, per, resumoRest.loc[(numeroRest, 'lsup'), per], limiteuti))
			resumoRest.loc[(numeroRest, 'lsup'), per] = limiteuti

		elif resumoRest.loc[(numeroRest, 'lsup'), per] > limiteuti:
			#log.write('({},({})): {}  ->  {}\n'.format(numeroRest, per, resumoRest.loc[(numeroRest, 'lsup'), per], limiteuti))
			resumoRest.loc[(numeroRest, 'lsup'), per] = limiteuti

	return resumoRest

def atualizarLsupContratosEntdados(resumoRest, blocoCECI):

	# expostação do bipolo de Belo Monte
	# Xingu-Estreito e Xingu-TerminalRio
	contratos = {950: [601, 602, 701, 702]}

	blocoCECI['lsuperior'] = blocoCECI['lsuperior'].apply(pd.to_numeric, errors='coerce')
	blocoCECI['num'] = blocoCECI['num'].apply(pd.to_numeric, errors='coerce')

	for num in contratos:
		resumoRest.loc[num, 'lsup'] = blocoCECI.loc[blocoCECI['num'].isin(contratos[num])]['lsuperior'].sum()

	return resumoRest


def analisarIntercambios(pathArquivosSaidaDessem, pathArquivosEntradaDessem, dataDeck, nomeREs):
	arquivosEntrada = glob.glob(os.path.join(pathArquivosEntradaDessem, '*'))
	arquivosSaida = glob.glob(os.path.join(pathArquivosSaidaDessem, '*'))
	pathRestOper = list(filter(lambda file: 'pdo_restoper.dat' in file.lower(), arquivosSaida))[0]
	#print("\tLeitura do arquivo: {}".format(pathRestOper))

	restOper = wx_pdoRestoper.leituraRestOper(pathRestOper)
	resumoRest = resumoRestricoes(restOper)

	pathOperLpp = list(filter(lambda file: 'pdo_oper_lpp.dat' in file.lower(), arquivosSaida))[0]
	
	operLpp = wx_pdoOperLpp.leituraOperLpp(pathOperLpp)
	#print("\tLeitura do arquivo: {}".format(pathOperLpp))
	resumoRest = atualizarLsupLpp(resumoRest, operLpp)
	
	pathOpeRrsttab = list(filter(lambda file: 'pdo_oper_rsttab.dat' in file.lower(), arquivosSaida))[0]
	operRsttab = wx_pdoOperRsttab.leituraOperRsttab(pathOpeRrsttab)
	#print("\tLeitura do arquivo: {}".format(pathOpeRrsttab))
	resumoRest = atualizarLsupRsttab(resumoRest, operRsttab)

	pathEntdados = list(filter(lambda file: 'entdados.dat' in file.lower(), arquivosEntrada))[0]
	entdados = wx_entdados.leituraArquivo(pathEntdados)
	#print("\tLeitura do arquivo: {}".format(pathEntdados))
	infoBlocosEntdados = wx_entdados.getInfoBlocos()
	blocoCECI = wx_entdados.extrairInfoBloco(entdados, 'CE_CI', infoBlocosEntdados['CE_CI']['regex'])
	df_blocoCECI = pd.DataFrame(blocoCECI, columns=infoBlocosEntdados['CE_CI']['campos'])
	resumoRest = atualizarLsupContratosEntdados(resumoRest, df_blocoCECI)
	#print('\tResumo finalizado')

	restOpers_re = restOper.loc[restOper['cod'].str.strip() == 'RE'].copy()
	restOpers_re['multipl'] = restOpers_re['multipl'].apply(pd.to_numeric, errors='coerce')
	restricoesAtivasVioladas = restOpers_re[restOpers_re['multipl'] != 0]
	for grupo, rests in restricoesAtivasVioladas.groupby(['num','iper']).__iter__():
		num = grupo[0]
		iper = grupo[1]
		resumoRest.loc[(num, 'lsup'),iper] = resumoRest.loc[(num, 'valor'),iper]	
	df_out = pd.DataFrame()
	resumoRest = resumoRest.replace(np.nan, '99999')
	#print('\tTratando as restricoes ')
	for rest in resumoRest.index.get_level_values(0).unique():
		try :
			nome = nomeREs[str(int(rest))][0]
			iper = 1
			while iper < 48:
				dict_aux = {'Restricao':rest,'Nome': nome, 'dataHora':str(dataDeck.day)+'/'+str(dataDeck.month)+'/'+str(dataDeck.year)+ ' '+ str(int((int(iper-1)*30)/60)).zfill(2)+':00'}
				dict_aux.update({'linf':str(int((int(resumoRest.loc[rest,iper]['linf'])+int(resumoRest.loc[rest,iper+1]['linf']))/2)),
								 'valor':str(int((int(resumoRest.loc[rest,iper]['valor'])+int(resumoRest.loc[rest,iper+1]['valor']))/2)),
								 'lsup':str(int((int(resumoRest.loc[rest,iper]['lsup'])+int(resumoRest.loc[rest,iper+1]['lsup']))/2))} )
				df_out = df_out.append(dict_aux,ignore_index=True)
				iper = iper + 2
		except Exception:
			pass
			#print("Unexpected error:", sys.exc_info()[0])
			#log.write('Restrição '+ str(rest) +' não esta na lista de REs do config' + '\n')
			#print('Restrição ', rest, ' não esta na lista de REs do config')
	df_out = df_out.replace( '99999', ' ')
	df_out = df_out.replace( '-99999', ' ')

	return df_out

def getDataDeck(deck):
	dadvaz = f = open(deck + '/dadvaz.dat', "r")
	countLine = 0
	for line in dadvaz:
		countLine = countLine+1
		if countLine == 10:
			return datetime.date(int(line[12:16]),int(line[8:10]),int(line[4:6]))


def readIntercambios(pathEntrada, patSaida, pathBase, dataDeck, pathOut):
	
	#log.write('\n')
	#log.write('LENDO DECK DE ENTRADA :' + pathEntrada + '\n')
	#log.write('LENDO DECK DE SAIDA :' + patSaida + '\n')

	nomeREs = pd.read_csv(pathBase + '/nomeREs.csv', header=0, sep=';')
	df_out  = analisarIntercambios(patSaida, pathEntrada, dataDeck, nomeREs)
	
	balanco_intercambio_lista = df_out.values.tolist()
	db_decks = wx_dbClass.db_mysql_master('db_decks')
	db_decks.connect()
	tb_intercambio_dessem = db_decks.getSchema('tb_intercambio_dessem')
	
	if balanco_intercambio_lista:
		for lista in balanco_intercambio_lista:
			data_str = lista[2]
			data = datetime.datetime.strptime(data_str, '%d/%m/%Y %H:%M')
			lista[2] = data
		balanco_intercambio_lista = list(balanco_intercambio_lista)
		dt = data.date()
		dia_inicio = datetime.datetime.combine(dt, datetime.time.min)
		dia_fim = datetime.datetime.combine(dt, datetime.time.max)
		delete_balanco_dessem = tb_intercambio_dessem.delete().where(tb_intercambio_dessem.c.dt_data_hora.between(dia_inicio, dia_fim))
		db_decks.conn.execute(delete_balanco_dessem)
		insert_balanco_dessem = tb_intercambio_dessem.insert().values(balanco_intercambio_lista).prefix_with('IGNORE')
		db_decks.conn.execute(insert_balanco_dessem)
	else:
		print("Lista de intercambio vazia!")

