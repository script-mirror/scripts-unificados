import os
import re
import pdb
import sys
import glob
import time
import shutil
import pathlib
import datetime
import threading
import subprocess

import numpy as np
import pandas as pd
import sqlalchemy as db

home = str(pathlib.Path.home())

diretorioApp = os.path.abspath('.')
diretorioArquivos = os.path.join(diretorioApp,'arquivos')
appsDir = os.path.dirname(diretorioApp)
diretorioRaiz = os.path.dirname(appsDir)

libLocal = os.path.join(diretorioApp,'libs')
libUniversal = os.path.join(diretorioRaiz,'bibliotecas')

sys.path.insert(1, libUniversal)
import wx_dbLib
import wx_manipulaArqCpins
import wx_dbClass

sys.path.insert(1, libLocal)

middle = sys.path[0]+"/arquivos/WX Energy Dropbox/WX - Middle/"
middle = os.path.join(home, "Dropbox", "WX - Middle")

dirdados_ent = os.path.join(middle,'NovoSMAP')
dirprog =  os.path.join(middle,'Programas','Aplicativo_SMAP')
dirtxt = os.path.join(dirprog,'arquivos_txt')

bacias = {}
pathEstrutura = os.path.join(diretorioArquivos,'estrutura_base')
for bac in os.listdir(pathEstrutura):
	bacias[bac] = {}
	bacias[bac]['trechos'] = []
	estruturaBacia = os.path.join(pathEstrutura,bac,'ARQ_ENTRADA')
	listaArquivos = os.listdir(estruturaBacia)
	for f in listaArquivos:
		if '_PMEDIA.txt' in f:
			bacias[bac]['trechos'].append(f.replace('_PMEDIA.txt',''))

def cria_arqentrada_smap(path, now, modelos, pathdropbox, run, ndia):
	
	path_smap = os.path.join(dirprog,'4.0.0','Batsmap-desktop')
	pathauxilio = os.path.join(dirprog,'auxilio_ajuste')
	dias_aquec = 32;	 # numero de dias para o aquecimento do modelo
	print('CRIANDO DIRETORIO PARA A RODADA DA DATA ' + now.strftime("%Y%m%d"))

	if 'PRELIMINAR' in str(modelos):
		dtRef = now - datetime.timedelta(days=1)
	else:
		dtRef = now

	for bacia in bacias:

		for nomePasta in ['Arq_Entrada','Arq_Saida','bin','logs']:
			caminhoPasta = os.path.join(path,bacia,nomePasta)
			if not os.path.exists(caminhoPasta):
				os.makedirs(caminhoPasta)

		dst = os.path.join(path,bacia,'Arq_Entrada')
		src = os.path.join(diretorioArquivos,'estrutura_base',bacia,'Arq_Entrada')
		for f in os.listdir(src):
			if '_PMEDIA.txt' in f:
				for modelo in modelos:
					file = os.path.join(src,f)
					nf = f.replace('_PMEDIA.txt','_{}.txt'.format(modelo))
					newFile = os.path.join(dst,nf)
					shutil.copy(file,newFile)
			else:
				file = os.path.join(src,f)
				shutil.copy(file,dst)

		preciptationModel = open(os.path.join(path,bacia,'Arq_Entrada','MODELOS_PRECIPITACAO.txt'), "w")
		preciptationModel.write('{}\n'.format(len(modelos)))
		for modelo in modelos:
			preciptationModel.write(modelo+'\n')
		preciptationModel.close()

		#Copiando excutavel do smap
		src = os.path.join(path_smap,'batsmap-desktop.exe')
		dst = os.path.join(path,bacia)
		shutil.copy(src, dst)

		# Copia dos DLLs necessarios para o smap
		src = os.path.join(path_smap,'bin')
		dst = os.path.join(path,bacia,'bin')
		src_files = os.listdir(src)
		for f in src_files:
			file = os.path.join(src, f)
			shutil.copy(file,dst)
		
		#gera arquivos de chuva
		dst = os.path.join(path,bacia,'Arq_Entrada')
		for modelo in modelos:
			gera_dat_file(modelo,now,run,dst,arqPrev=1)

		src = os.path.join(pathdropbox,'SMAP',bacia,'ARQ_ENTRADA')
		dst = os.path.join(path,bacia,'Arq_Entrada')
		src_files = os.listdir(src)
		for f in src_files:
			file = os.path.join(src, f)
			shutil.copy(file, dst)

		# Arquivo 'CASO.TXT'
		casoFile = open(os.path.join(path, bacia,'Arq_Entrada','CASO.txt'), "w")
		# Escreve o numero de trechos das bacias
		casoFile.write('{}\n'.format(len(bacias[bacia]['trechos'])))

		diaaquecimento = (dtRef - datetime.timedelta(days=dias_aquec)).strftime("%d/%m/%Y")

		for trecho in bacias[bacia]['trechos']:

			# escreve os nomes de cada trecho no 'CASO.TXT'
			casoFile.write(trecho.upper()+'\n')

			# Abre o arquivo de ajustes
			trechoFile = open(os.path.join(pathauxilio,trecho+"_AJUSTE.txt"), "r")
			trechoFile = trechoFile.readlines()

			for i, line in enumerate(trechoFile):
				# procura em cada linha pelo dia de aquecimento
				if diaaquecimento in line:
					lineSplited = line.split()

					# arquivos com as infos para a inicializacao do modelo para cada sub-bacia
					arq_ini = open(os.path.join(path,bacia,'Arq_Entrada',trecho.upper() + "_INICIALIZACAO.txt"), "w")
					arq_ini.write(dtRef.strftime("%d/%m/%Y")+'\n')
					arq_ini.write(str(dias_aquec)+'\n')
					arq_ini.write(str(ndia)+'\n')
					arq_ini.write("{0:<6.2f}\n".format(float(lineSplited[2])))
					arq_ini.write("{0:<6.2f}\n".format(float(lineSplited[3])))
					arq_ini.write("{0:<5.2f}\n".format(float(lineSplited[1])))
					if ((float(lineSplited[1])== 0 and float(lineSplited[2])== 0 and float(lineSplited[3])== 0) ):
						print(f"Valores do dia de aquecimento ({diaaquecimento}) estão zerados! Não será Rodado!")
						print(f"Verificar em {pathauxilio}")
						print("Estes arquivos se encontram no site da ONS Modelos_Chuva_Vazao_%Y%m%d.zip")
						
					# break do for que procura pela data
					break
				if i == len(trechoFile)-1:
					print(trecho)
					print(f"!!! Arquivos de ajustes desatualizados, {diaaquecimento} não foi encontrado no arquivo (.../WX Energy Dropbox/WX - Middle/Programas/Aplicativo_SMAP/auxilio_ajuste/)")
					print("Estes arquivos se encontram no site da ONS Modelos_Chuva_Vazao_%Y%m%d.zip")
					quit()
			arq_ini.close()

		casoFile.close()

def gera_dat_file(modelo,dt_rodada,hr_rodada,dir_destino,arqPrev=1):

	if not type(dt_rodada) == str:
		dt_rodada = datetime.datetime.strftime(dt_rodada, "%d/%m/%Y")


	dt_rodada_date = datetime.datetime.strptime(dt_rodada, "%d/%m/%Y")
	dt_inicial_file = dt_rodada_date.strftime('%d%m%y')

	#instancias das tabelas do banco de dados
	db_rodadas = wx_dbClass.db_mysql_master('db_rodadas')
	db_rodadas.connect()

	tb_cadastro_rodadas = db_rodadas.getSchema('tb_cadastro_rodadas')
	tb_chuva = db_rodadas.getSchema('tb_chuva')
	tb_subbacia = db_rodadas.getSchema('tb_subbacia')
	tb_chuva_obs = db_rodadas.getSchema('tb_chuva_obs')

	modelo_query = modelo
	if "PRELIMINAR" in modelo:
		modelo_query = modelo.replace(".PRELIMINAR","")

	#tb_subbacia
	query_subbac = db.select(tb_subbacia.c.cd_subbacia,tb_subbacia.c.vl_lon,tb_subbacia.c.vl_lat,tb_subbacia.c.txt_nome_subbacia)
	answer_tb_subbac = db_rodadas.conn.execute(query_subbac)
	df_subbac = pd.DataFrame(answer_tb_subbac, columns=['cd_subbacia','vl_lon','vl_lat','txt_nome_subbacia'])

	if "PZERADA" in modelo:
		dt_prev_pzerada = dt_rodada_date + datetime.timedelta(days=1)
		df_subbac['dt_prevista'] = dt_prev_pzerada
		df_subbac['vl_chuva'] = 0
		df_chuva_rodada_subbac = df_subbac

	if "PZERADA" not in modelo:
		#tb_cadastro_rodadas
		query_rodadas_id_chuva = db.select(tb_cadastro_rodadas.c.id_chuva)\
								.where(db.and_(tb_cadastro_rodadas.c.str_modelo == re.sub('\d+p\d+$','', modelo_query),tb_cadastro_rodadas.c.dt_rodada == dt_rodada_date, tb_cadastro_rodadas.c.hr_rodada == hr_rodada ))
		answer_tb_rodadas = db_rodadas.conn.execute(query_rodadas_id_chuva)


		df_rodadas = pd.DataFrame(answer_tb_rodadas, columns=['id_chuva'])
		df_rodadas = df_rodadas.dropna()
		id_chuva = df_rodadas['id_chuva'].unique()

		#tb_chuva
		query_rodadas_tb_chuva = tb_chuva.select().where(tb_chuva.c.id == id_chuva[0])
		answer_tb_chuva = db_rodadas.conn.execute(query_rodadas_tb_chuva)
		df_chuva = pd.DataFrame(answer_tb_chuva,columns=['id_chuva','cd_subbacia','dt_prevista','vl_chuva'])
		df_chuva = df_chuva.rename(columns={'id':'id_chuva'})

		# Verifica o fator multiplicativoesta presente
		padrao_fator_chuva = r"(\d+)p(\d+)$"
		correspondencia = re.search(padrao_fator_chuva, modelo_query)
		if correspondencia:
			fator_multiplicativo = float(f'{correspondencia.group(1)}.{correspondencia.group(2)}')
			df_chuva['vl_chuva'] = df_chuva['vl_chuva']*fator_multiplicativo

		# mergiando dfs
		df_chuva_rodada = pd.merge(df_rodadas, df_chuva, on='id_chuva', how='inner')
		df_chuva_rodada_subbac = pd.merge(df_chuva_rodada, df_subbac, on='cd_subbacia', how='inner')


	df_chuva_rodada_subbac[['vl_lat','vl_lon']] = df_chuva_rodada_subbac[['vl_lat','vl_lon']].applymap(lambda x: f'{x:0>5.2f}')

	values_to_write = df_chuva_rodada_subbac.pivot_table(index=['vl_lon','vl_lat'], columns='dt_prevista', values='vl_chuva')


	if "PRELIMINAR" in modelo:
		dt_inicio_chuva_obs  = dt_rodada_date - datetime.timedelta(days=1)
		dt_inicial_file = dt_inicio_chuva_obs.strftime('%d%m%y')
		
		if 'PZERADA' not in modelo:
			selec_tb_chuva_obs = tb_chuva_obs.select().where(tb_chuva_obs.c.dt_observado == dt_inicio_chuva_obs)
			answer_tb_chuva_obs = db_rodadas.conn.execute(selec_tb_chuva_obs).fetchall()
			if not answer_tb_chuva_obs:
				print("Nao foi encontrada a chuva observada no banco!")
				quit()

			df_chuva_obs = pd.DataFrame(answer_tb_chuva_obs, columns=['cd_subbacia','dt_prevista','vl_chuva'])

			df_chuva_obs_concat = pd.merge(df_chuva_obs,df_subbac, on='cd_subbacia')
			df_chuva_obs_concat[['vl_lat','vl_lon']] = df_chuva_obs_concat[['vl_lat','vl_lon']].applymap(lambda x: f'{x:0>5.2f}')
			df_chuva_obs_pivot = df_chuva_obs_concat.pivot_table(index=['vl_lon','vl_lat'], columns='dt_prevista', values='vl_chuva')

			df_insert_firt_column = pd.concat([df_chuva_obs_pivot, values_to_write], axis=1)
			df_drop_last_column = df_insert_firt_column.iloc[:,:-1]
			values_to_write = df_drop_last_column
		else:
			values_to_write = values_to_write.rename(columns={dt_prev_pzerada:dt_rodada_date})

	#subbacias calculadas
	for date in values_to_write.columns:
		values_to_write.loc[('-64.66','-9.26'),date] = values_to_write.loc[('-64.65','-9.25'),date]*0.13 + values_to_write.loc[('-69.12','-12.60'),date]*0.87
		values_to_write.loc[('-51.77','-3.13'),date] = values_to_write.loc[('-51.91','-3.41'),date]*0.699 + values_to_write.loc[('-52.00','-6.74'),date]*0.264 + values_to_write.loc[('-51.77','-6.75'),date]*0.037

	values_to_write = values_to_write.round(1)

	dt_prevista_final = values_to_write.columns[-1].strftime('%d%m%y')

	if arqPrev == 1:
		nomeSaida = f'{modelo}_p{dt_inicial_file}a{dt_prevista_final}.dat'
		path_arq_destino = os.path.join(dir_destino,nomeSaida)
		values_to_write.to_csv(path_arq_destino,sep=" ", index=True, header=False)
		
	else:
		for date in values_to_write.columns:
			dt_prevista = date.strftime('%d%m%y')
			nomeSaida = f'{modelo}_p{dt_inicial_file}a{dt_prevista}.dat'
			path_arq_destino = os.path.join(dir_destino,nomeSaida)
			values_to_write[date].to_csv(path_arq_destino,sep=" ", index=True, header=False)

	db_rodadas.conn.close()

def runBatsmap(path):
	bacia = os.path.basename(path)
	start_time = time.time()
	print('Smap iniciado para o {}!'.format(bacia))
	
	# Se estiver rodando no Linux
	# p = subprocess.Popen(['wine', 'batsmap-desktop.exe'], cwd=path, stdin=subprocess.PIPE)

	# Se estiver rodando no Windows
	p = subprocess.Popen(['batsmap-desktop.exe'], cwd=path, stdin=subprocess.PIPE, shell=True, stdout=subprocess.PIPE)
	p.communicate(input=b'\n')
	print('{} finalizado! ({:.2f}s)'.format(bacia, time.time() - start_time))

def leituraSaidaSmap(path, modelos):
	vaz_prev = {}
	for modelo in modelos:
		vaz_prev[modelo] = {}
		for bacia in bacias:
			vaz_prev[modelo][bacia] = {}
			for trecho in bacias[bacia]['trechos']:
				vaz_prev[modelo][bacia][trecho] = {}

				full_file_name = os.path.join(path,bacia,'Arq_Saida',trecho+'_'+modelo+'_PREVISAO.txt')
				file_previsao = open(full_file_name, 'r').readlines()
				for i, line in enumerate(file_previsao):
					if i != 0:
						lineSplited = line.split()
						vaz_prev[modelo][bacia][trecho][datetime.datetime.strptime(lineSplited[0], '%d/%m/%Y')] = float(lineSplited[1])
	return vaz_prev


def vazIncremental(path, Q_modelo):

	vazaoVerificado={}
	vazaoVerificado['GRD_PRNB'] = {'PARAGUACU':{}, 'PBUENOS':{}, 'PASSAGEM':{}, 'CAPESCURO':{}, 'RVERDE':{}}
	vazaoVerificado['Sul'] = {'UVITORIA':{}}
	vazaoVerificado['Paranazao'] = {'BALSA':{}, 'FLOR+ESTRA':{}, 'IVINHEMA':{}, 'PTAQUARA':{}, 'ITAIPU':{}, 'FZB':{}}
	vazaoVerificado['Norte'] = {'SMESA':{},'BANDEIRANT':{},'C.ARAGUAIA':{},'PORTO REAL':{},'ESTREITO':{},'LAJEADO':{},'TUCURUI':{}, 'P_DA_BEIRA':{},'GUAJ-MIRIM':{},'JIRAU2':{},'S.ANTONIO':{},'GUAPORE':{},'SAMUEL':{},'RONDONII':{},'DARDANELOS':{}}
	vazaoVerificado['NE'] = {'IRAPE':{},'ITAPEBI':{},'UBESP':{}}

	postosTocantins = ['SMESA','BANDEIRANT','C.ARAGUAIA','PORTO REAL','ESTREITO','LAJEADO','TUCURUI']
	postosMadeira = ['P_DA_BEIRA','GUAJ-MIRIM','JIRAU2','S.ANTONIO','GUAPORE','SAMUEL','RONDONII','DARDANELOS']
	postosJeqParnaiba = ['IRAPE','ITAPEBI','UBESP']

	for bacia in vazaoVerificado:
		for posto in vazaoVerificado[bacia]:
			caminhoArquivo = os.path.join(path,bacia,'Arq_Entrada',posto+'.txt')
			file = open(caminhoArquivo,'r').readlines()
			if posto in postosTocantins+postosMadeira:
				for line in file[-95:]:
					line_splited = line.rstrip().split('|')
					date = datetime.datetime.strptime((line_splited[-2]), '%Y-%m-%d %H:%M:%S')
					vazaoVerificado[bacia][posto][date] = float(line_splited[-1])
			elif posto in postosJeqParnaiba:
				for line in file[-44:]:
					line_splited = line.rstrip().split('|')
					date = datetime.datetime.strptime((line_splited[-2]), '%Y-%m-%d %H:%M:%S')
					vazaoVerificado[bacia][posto][date] = float(line_splited[-1])
			else:
				for line in file[-2:]:
					line_splited = line.rstrip().split('|')
					date = datetime.datetime.strptime((line_splited[-2]), '%Y-%m-%d %H:%M:%S')
					vazaoVerificado[bacia][posto][date] = float(line_splited[-1])

	inicioRdh = min(vazaoVerificado['Norte']['SMESA'].keys()).strftime('%Y/%m/%d')
	fimrdh = max(vazaoVerificado['Norte']['SMESA'].keys()).strftime('%Y/%m/%d')
	wxdb = wx_dbLib.WxDataB()
	action = '''SELECT
					*
				FROM
					VW_ACOMPH_RDH
				WHERE
					DT_REFERENTE BETWEEN '{}' and '{}'
					and CD_POSTO IN (191,253,257)
				order by
					CD_POSTO,
					DT_REFERENTE,
					FONTE_DADOS asc'''.format(inicioRdh,fimrdh)
	
	resposta_db = wxdb.requestServer(action)
	verificado_acomph = {}
	for row in resposta_db:
		if row[2] not in verificado_acomph:
			verificado_acomph[row[2]] = {}

		# essa etapa faz com que caso não exista acomph para o posto, o rdh é utilizado devido a "...order by CD_POSTO,DT_REFERENTE, FONTE_DADOS asc"
		if row[0] not in verificado_acomph[row[2]]:
			verificado_acomph[row[2]][row[0]]=row[3]
		else:
			continue

	vazaoVerificado['Norte']['CANABRAVA']=verificado_acomph[191]
	vazaoVerificado['Norte']['SAOSALVADOR']=verificado_acomph[253]
	vazaoVerificado['Norte']['PEIXEANGICAL']=verificado_acomph[257]
	
	vazoes = calcVazIncremental(Q_modelo, vazaoVerificado)

	return vazoes

	
def calcVazIncremental(Q_modelo, vazaoVerificado):
	# calculo da incremental: 
	#	opc1: Valor do modelo (Q_modelo) ou do verificado (vazaoVerificado), caso o valor no dicionario é menor q 1, é usado como fator multiplicativo, cc, como tempo de viagem 
	#	opc2: tupla, onde o primeiro valor e o fator multiplicativo e o segundo o tempo de viagem
	# 		Ex: 'PCOLOMBIA':(0.5, 15) -> valores de PCOLOMBIA x 0.5 com o delay de 15 horas
	

	# calculo da natural: primeiro array de naturais e o segundo array de incremental
	bacias = {}

	bacias['GRD_PRNB'] = {}
	bacias['GRD_PRNB']['Camargos'] = {'incremental':{'CAMARGOS':{'fator':1}},'natural':[[],['Camargos']]}
	bacias['GRD_PRNB']['Itutinga'] = {'incremental':{},'natural':[[], ['Camargos']]}
	bacias['GRD_PRNB']['Funil'] = {'incremental':{'FUNIL MG':{'fator':1}},'natural':[['Itutinga'],['Funil']]}
	bacias['GRD_PRNB']['Furnas'] = {'incremental':{'FURNAS':{'fator':1}, 'PARAGUACU':{'tv':10}, 'PBUENOS':{'tv': 12}},'natural':[['Funil'], ['Furnas']]}
	bacias['GRD_PRNB']['Moraes'] = {'incremental':{'PCOLOMBIA':{'fator':0.377}},'natural':[['Furnas'], ['Moraes']]}
	bacias['GRD_PRNB']['Estreito'] = {'incremental':{'PCOLOMBIA':{'fator':0.087}},'natural':[['Moraes'], ['Estreito']]}
	bacias['GRD_PRNB']['Jaguara'] = {'incremental':{'PCOLOMBIA':{'fator':0.036}},'natural':[['Estreito'], ['Jaguara']]}
	bacias['GRD_PRNB']['Igarapava'] = {'incremental':{'PCOLOMBIA':{'fator':0.103}},'natural':[['Jaguara'], ['Igarapava']]}
	bacias['GRD_PRNB']['Vgrande'] = {'incremental':{'PCOLOMBIA':{'fator':0.230}},'natural':[['Igarapava'], ['Vgrande']]}
	bacias['GRD_PRNB']['Pcolombia'] = {'incremental':{'PCOLOMBIA':{'fator': 0.167}, 'CAPESCURO':{'tv': 8}},'natural':[['Vgrande'], ['Pcolombia']]}
	bacias['GRD_PRNB']['Caconde'] = {'incremental':{'EDACUNHA':{'fator':0.61}},'natural':[[], ['Caconde']]}
	bacias['GRD_PRNB']['Euclides'] = {'incremental':{'EDACUNHA':{'fator':0.39}},'natural':[['Caconde'], ['Euclides']]}
	bacias['GRD_PRNB']['Limoeiro'] = {'incremental':{'MARIMBONDO':{'fator':0.004}},'natural':[['Euclides'], ['Limoeiro']]}
	bacias['GRD_PRNB']['Marimbondo'] = {'incremental':{'MARIMBONDO':{'fator':0.996}, 'PASSAGEM':{'tv':16}},'natural':[['Pcolombia', 'Limoeiro'], ['Marimbondo']]}
	bacias['GRD_PRNB']['AguaVermelha'] = {'incremental':{'AVERMELHA':{'fator':1}},'natural':[['Marimbondo'], ['AguaVermelha']]}
	bacias['GRD_PRNB']['Corumba4'] = {'incremental':{'CORUMBAIV':{'fator':1}},'natural':[[],['Corumba4']]}
	bacias['GRD_PRNB']['Corumba3'] = {'incremental':{'CORUMBA1':{'fator':0.10}},'natural':[['Corumba4'],['Corumba3']]}
	bacias['GRD_PRNB']['Corumba1'] = {'incremental':{'CORUMBA1':{'fator':0.90}},'natural':[['Corumba3'],['Corumba1']]}
	bacias['GRD_PRNB']['Batalha'] = {'incremental':{'SDOFACAO':{'fator':0.615}},'natural':[[],['Batalha']]}
	bacias['GRD_PRNB']['SerraFacao'] = {'incremental':{'SDOFACAO':{'fator':0.385}},'natural':[[],['Batalha','SerraFacao']]}
	bacias['GRD_PRNB']['Emborcacao'] = {'incremental':{'EMBORCACAO':{'fator':1}},'natural':[['SerraFacao'],['Emborcacao']]}
	bacias['GRD_PRNB']['NovaPonte'] = {'incremental':{'NOVAPONTE':{'fator':1}},'natural':[[],['NovaPonte']]}
	bacias['GRD_PRNB']['Miranda'] = {'incremental':{'ITUMBIARA':{'fator':0.04}},'natural':[['NovaPonte'],['Miranda']]}
	bacias['GRD_PRNB']['Cbranco1'] = {'incremental':{'ITUMBIARA':{'fator':0.005}},'natural':[['Miranda'],['Cbranco1']]}
	bacias['GRD_PRNB']['Cbranco2'] = {'incremental':{'ITUMBIARA':{'fator':0.012}},'natural':[['Cbranco1'],['Cbranco2']]}
	bacias['GRD_PRNB']['Itumbiara'] = {'incremental':{'ITUMBIARA':{'fator':0.943}},'natural':[['Corumba1', 'Emborcacao', 'Cbranco2'], ['Itumbiara']]}
	bacias['GRD_PRNB']['Cdourada'] = {'incremental':{'SSIMAO2':{'fator':0.109}},'natural':[['Itumbiara'],['Cdourada']]}
	bacias['GRD_PRNB']['SaoSimao'] = {'incremental':{'SSIMAO2':{'fator':0.891}, 'RVERDE':{'tv':8}},'natural':[['Cdourada'],['SaoSimao']]}
	bacias['GRD_PRNB']['Espora'] = {'incremental':{'ESPORA':{'fator':1}},'natural':[[],['Espora']]}
	bacias['GRD_PRNB']['Cacu'] = {'incremental':{'FOZCLARO':{'fator':0.894}},'natural':[[],['Cacu']]}
	bacias['GRD_PRNB']['Salto'] = {'incremental':{'SALTOVERDI':{'fator':0.923}},'natural':[[],['Salto']]}
	bacias['GRD_PRNB']['SaltoVerdinho'] = {'incremental':{'SALTOVERDI':{'fator':0.077}},'natural':[['Salto'],['SaltoVerdinho']]}
	bacias['GRD_PRNB']['Coqueiros'] = {'incremental':{'FOZCLARO':{'fator':0.037}},'natural':[['Cacu'],['Coqueiros']]}
	bacias['GRD_PRNB']['RioClaro'] = {'incremental':{'FOZCLARO':{'fator':0.069}},'natural':[['Coqueiros'],['RioClaro']]}

	bacias['Paranazao'] = {}
	bacias['Paranazao']['Jurumim'] = {'incremental':{'JURUMIRIM':{'fator':1}},'natural':[[],['Jurumim']]}
	bacias['Paranazao']['Piraju'] = {'incremental':{'CHAVANTES':{'fator':0.046}},'natural':[[],['Piraju', 'Jurumim']]}
	bacias['Paranazao']['Chavantes'] = {'incremental':{'CHAVANTES':{'fator':0.954}},'natural':[['Piraju'],['Chavantes']]}
	bacias['Paranazao']['Ourinhos'] = {'incremental':{'CANOASI':{'fator':0.031}},'natural':[['Chavantes'],['Ourinhos']]}
	bacias['Paranazao']['SaltoGrande'] = {'incremental':{'CANOASI':{'fator':0.778}},'natural':[['Ourinhos'],['SaltoGrande']]}
	bacias['Paranazao']['Canoas2'] = {'incremental':{'CANOASI':{'fator':0.061}},'natural':[['SaltoGrande'],['Canoas2']]}
	bacias['Paranazao']['Canoas1'] = {'incremental':{'CANOASI':{'fator':0.130}},'natural':[['Canoas2'],['Canoas1']]}
	bacias['Paranazao']['Maua'] = {'incremental':{'MAUA':{'fator':1}},'natural':[[],['Maua']]}
	bacias['Paranazao']['Capivara'] = {'incremental':{'CAPIVARA':{'fator':1}},'natural':[['Canoas1' ,'Maua'],['Capivara']]}
	bacias['Paranazao']['Taquarucu'] = {'incremental':{'ROSANA':{'fator':0.299}},'natural':[['Capivara'],['Taquarucu']]}
	bacias['Paranazao']['Rosana'] = {'incremental':{'ROSANA':{'fator':0.701}},'natural':[['Taquarucu'],['Rosana']]}
	bacias['Paranazao']['BillPedras'] = {'incremental':{'ESOUZA':{'fator':0.183}}, 'natural':[[],['BillPedras']]}
	bacias['Paranazao']['Guarapiranga'] = {'incremental':{'ESOUZA':{'fator':0.120}}, 'natural':[[],['Guarapiranga']]}
	bacias['Paranazao']['PonteNova'] = {'incremental':{'ESOUZA':{'fator':0.073}}, 'natural':[[],['PonteNova']]}
	bacias['Paranazao']['auxEdgardSouza'] = {'incremental':{'ESOUZA':{'fator':0.183*0.8103}}, 'natural':[[],[]]}
	bacias['Paranazao']['EdgardSouza'] = {'incremental':{'ESOUZA':{'fator':0.624}}, 'natural':[['Guarapiranga', 'PonteNova'],['EdgardSouza', 'auxEdgardSouza']]}
	bacias['Paranazao']['BarraBonita'] = {'incremental':{'BBONITA':{'fator':1}}, 'natural':[['EdgardSouza'],['BarraBonita']]}
	bacias['Paranazao']['Bariri'] = {'incremental':{'IBITINGA':{'fator':0.342}}, 'natural':[['BarraBonita'],['Bariri']]}
	bacias['Paranazao']['Ibitinga'] = {'incremental':{'IBITINGA':{'fator':0.658}}, 'natural':[['Bariri'],['Ibitinga']]}
	bacias['Paranazao']['Promissao'] = {'incremental':{'NAVANHANDA':{'fator':0.717}}, 'natural':[['Ibitinga'],['Promissao']]}
	bacias['Paranazao']['Avanhandava'] = {'incremental':{'NAVANHANDA':{'fator':0.283}}, 'natural':[['Promissao'],['Avanhandava']]}
	bacias['Paranazao']['Balsa'] = {'incremental':{'BALSA':{'tv':32}}, 'natural':[[],['Balsa']]}
	bacias['Paranazao']['Flor+Estra'] = {'incremental':{'FLOR+ESTRA':{'tv':33}}, 'natural':[[],['Flor+Estra']]}
	bacias['Paranazao']['Ivinhema'] = {'incremental':{'IVINHEMA':{'tv':45}}, 'natural':[[],['Ivinhema']]}
	bacias['Paranazao']['PTaquara'] = {'incremental':{'PTAQUARA':{'tv':36}}, 'natural':[[],['PTaquara']]}
	bacias['Paranazao']['Itaipu'] = {'incremental':{'ITAIPU':{'fator':1}}, 'natural':[[],['Balsa', 'Flor+Estra', 'Ivinhema', 'PTaquara', 'Itaipu']]}
	bacias['Paranazao']['Ilhasolteira'] = {'incremental':{'ILHAEQUIV':{'fator':0.94}}, 'natural':[[],['Ilhasolteira']]}
	bacias['Paranazao']['SaoDomingos'] = {'incremental':{'SDO':{'fator':1}}, 'natural':[[],['SaoDomingos']]}
	bacias['Paranazao']['TresIrmaos'] = {'incremental':{'ILHAEQUIV':{'fator':0.06}}, 'natural':[[],['TresIrmaos']]}
	bacias['Paranazao']['Jupia'] = {'incremental':{'JUPIA':{'fator':1}}, 'natural':[[],['Jupia']]}
	bacias['Paranazao']['PortoPrimavera'] = {'incremental':{'PPRI':{'fator':1}, 'FZB':{'tv':26}}, 'natural':[[],['PortoPrimavera']]}
	# as naturais dos trechos ilha solteira, tres irmaos, jupia e porto primaveras nao serao calculadas nessa parte

	bacias['Sul'] = {}
	bacias['Sul']['SantaClara'] = {'incremental':{'STACLARA':{'fator':1}},'natural':[[],['SantaClara']]}
	bacias['Sul']['Fundao'] = {'incremental':{'JORDSEG':{'fator':0.039}},'natural':[['SantaClara'],['Fundao']]}
	bacias['Sul']['Jordao'] = {'incremental':{'JORDSEG':{'fator':0.157}},'natural':[['Fundao'],['Jordao']]}
	bacias['Sul']['FozAreia'] = {'incremental':{'FOA':{'fator':1}, 'UVITORIA':{'tv':17.4}},'natural':[[],['FozAreia']]}
	bacias['Sul']['Segredo'] = {'incremental':{'JORDSEG':{'fator':0.804}},'natural':[['FozAreia'],['Segredo']]}
	bacias['Sul']['SaltoSantiago'] = {'incremental':{'BAIXOIG':{'fator':0.205}},'natural':[[],['SaltoSantiago', 'Segredo', 'FozAreia', 'Jordao', 'Fundao', 'SantaClara']]}
	bacias['Sul']['SaltoOsorio'] = {'incremental':{'BAIXOIG':{'fator':0.081}},'natural':[['SaltoSantiago'],['SaltoOsorio']]}
	bacias['Sul']['SaltoCaxias'] = {'incremental':{'BAIXOIG':{'fator':0.51}},'natural':[['SaltoOsorio'],['SaltoCaxias']]}
	bacias['Sul']['Baixoig'] = {'incremental':{'BAIXOIG':{'fator':0.204}},'natural':[['SaltoCaxias'],['Baixoig']]}
	bacias['Sul']['BarraGrande'] = {'incremental':{'BG':{'fator':1}}, 'natural':[[],['BarraGrande']]}
	bacias['Sul']['SaoRoque'] = {'incremental':{'CN':{'fator':0.745}}, 'natural':[[],['SaoRoque']]}
	bacias['Sul']['Garibaldi'] = {'incremental':{'CN':{'fator':0.165}}, 'natural':[['SaoRoque'],['Garibaldi']]}
	bacias['Sul']['CamposNovos'] = {'incremental':{'CN':{'fator':0.09}}, 'natural':[['Garibaldi'],['CamposNovos']]}
	bacias['Sul']['Machadinho'] = {'incremental':{'MACHADINHO':{'fator':1}}, 'natural':[['BarraGrande', 'CamposNovos'],['Machadinho']]}
	bacias['Sul']['Ita'] = {'incremental':{'ITA':{'fator':1}}, 'natural':[['Machadinho'],['Ita']]}
	bacias['Sul']['PassoFundo'] = {'incremental':{'MONJOLINHO':{'fator':0.586}}, 'natural':[[],['PassoFundo']]}
	bacias['Sul']['Monjolinho'] = {'incremental':{'MONJOLINHO':{'fator':0.414}}, 'natural':[['PassoFundo'],['Monjolinho']]}
	bacias['Sul']['FozChapeco'] = {'incremental':{'FOZCHAPECO':{'fator':1}}, 'natural':[['Ita','Monjolinho'],['FozChapeco']]}
	bacias['Sul']['QuebraQueixo'] = {'incremental':{'QQUEIXO':{'fator':1}}, 'natural':[[],['QuebraQueixo']]}
	bacias['Sul']['SaoJose'] = {'incremental':{'SJOAO':{'fator':0.963}}, 'natural':[[],['SaoJose']]}
	bacias['Sul']['SaoJoao'] = {'incremental':{'SJOAO':{'fator':0.037}}, 'natural':[['SaoJose'],['SaoJoao']]}
	bacias['Sul']['Ernestina'] = {'incremental':{'ERNESTINA':{'fator':1}},'natural':[[],['Ernestina']]}
	bacias['Sul']['PassoReal'] = {'incremental':{'PASSOREAL':{'fator':1}},'natural':[['Ernestina'],['PassoReal']]}
	bacias['Sul']['Jacui'] = {'incremental':{'DFRANC':{'fator':0.02}},'natural':[['PassoReal'],['Jacui']]}
	bacias['Sul']['Itauba'] = {'incremental':{'DFRANC':{'fator':0.464}},'natural':[['Jacui'],['Itauba']]}
	bacias['Sul']['DonaFrancisca'] = {'incremental':{'DFRANC':{'fator':0.516}},'natural':[['Itauba'],['DonaFrancisca']]}
	bacias['Sul']['CastroAlves'] = {'incremental':{'CALVES':{'fator':1}},'natural':[[],['CastroAlves']]}
	bacias['Sul']['MonteClaro'] = {'incremental':{'14JULHO':{'fator':0.887}},'natural':[['CastroAlves'],['MonteClaro']]}
	bacias['Sul']['14Julho'] = {'incremental':{'14JULHO':{'fator':0.113}},'natural':[['MonteClaro'],['14Julho']]}
	bacias['Sul']['GPSouza'] = {'incremental':{'GPSOUZA':{'fator':1}},'natural':[[],['GPSouza']]}
	bacias['Sul']['SaltoPilao'] = {'incremental':{'SALTOPILAO':{'fator':1}},'natural':[[],['SaltoPilao']]}


	# Tocantins sera propagada na funcao propagaTocantins
	bacias['Norte'] = {}
	bacias['Norte']['RondonII'] = {'incremental':{'RONDONII':{'fator':1}}, 'natural':[[],['RondonII']]}
	bacias['Norte']['Samuel'] = {'incremental':{'SAMUEL':{'fator':1}}, 'natural':[[],['Samuel']]}
	bacias['Norte']['Dardanelos'] = {'incremental':{'DARDANELOS':{'fator':1}}, 'natural':[[],['Dardanelos']]}
	bacias['Norte']['Guapore'] = {'incremental':{'GUAPORE':{'fator':1}}, 'natural':[[],['Guapore']]}
	bacias['Norte']['Jirau'] = {'incremental':[],'natural':[]}
	bacias['Norte']['SantoAntonio'] = {'incremental':[],'natural':[]}
	bacias['Norte']['Pimental'] = {'incremental':{'PIMENTALT':{'fator':1}}, 'natural':[[],['Pimental']]}
	bacias['Norte']['Sinop'] = {'incremental':{'COLIDER':{'fator':0.915}}, 'natural':[[],['Sinop']]}
	bacias['Norte']['Colider'] = {'incremental':{'COLIDER':{'fator':0.085}}, 'natural':[['Sinop'],['Colider']]}
	bacias['Norte']['TelesPires'] = {'incremental':{'SMANOEL':{'fator':0.991}}, 'natural':[['Colider'],['TelesPires']]}
	bacias['Norte']['SaoMiguel'] = {'incremental':{'SMANOEL':{'fator':0.009}}, 'natural':[['TelesPires'],['SaoMiguel']]}
	bacias['Norte']['CachoeiraCaldeirao']  = {'incremental':{'FGOMES':{'fator':0.989}}, 'natural':[[],['CachoeiraCaldeirao']]}
	bacias['Norte']['CoaracyNunes']  = {'incremental':{'FGOMES':{'fator':0.003}}, 'natural':[['CachoeiraCaldeirao'],['CoaracyNunes']]}
	bacias['Norte']['FerreiraGomes']  = {'incremental':{'FGOMES':{'fator':0.008}}, 'natural':[['CoaracyNunes'],['FerreiraGomes']]}
	bacias['Norte']['Balbina']  = {'incremental':{'BALBINA':{'fator':1}}, 'natural':[[],['Balbina']]}
	bacias['Norte']['CuruaUna']  = {'incremental':{'CURUAUNA':{'fator':1}}, 'natural':[[],['CuruaUna']]}
	bacias['Norte']['StoAntonioJari']  = {'incremental':{'STOANTJARI':{'fator':1}}, 'natural':[[],['StoAntonioJari']]}

	bacias['NE'] = {}
	bacias['NE']['Queimado'] = {'incremental':{'QM':{'fator':1}},'natural':[[],['Queimado']]}
	bacias['NE']['RetiroBaixo'] = {'incremental':{'RB-SMAP':{'fator':1}},'natural':[[],['RetiroBaixo']]}
	bacias['NE']['TresMarias'] = {'incremental':{'TM-SMAP':{'fator':1}},'natural':[['RetiroBaixo'],['TresMarias']]}
	bacias['NE']['Itapebi']  = {'incremental':{'ITAPEBI':{'fator':1}}, 'natural':[[],['Itapebi']]}
	bacias['NE']['Irape']  = {'incremental':{'IRAPE':{'fator':1}}, 'natural':[[],['Irape']]}
	bacias['NE']['BoaEsperanca']  = {'incremental':{'UBESP':{'fator':1}}, 'natural':[[],['BoaEsperanca']]}
	bacias['NE']['PedraCavalo'] = {'incremental':{'PCAVALO':{'fator':1}}, 'natural':[[],['PedraCavalo']]}

	bacias['OSE'] = {}
	bacias['OSE']['Manso'] = {'incremental':{'MANSO':{'fator':1}}, 'natural':[[],['Manso']]}
	bacias['OSE']['ItiquiraI'] = {'incremental':{'ITIQUIRAI':{'fator':1}}, 'natural':[[],['ItiquiraI']]}
	bacias['OSE']['PdePedra'] = {'incremental':{'PDEPEDRA':{'fator':1}}, 'natural':[[],['PdePedra']]}
	bacias['OSE']['Jauru'] = {'incremental':{'JAURU':{'fator':1}}, 'natural':[[],['Jauru']]}
	bacias['OSE']['Candonga'] = {'incremental':{'CANDONGA':{'fator':1}}, 'natural':[[],['Candonga']]}
	bacias['OSE']['GuimanAmorim'] = {'incremental':{'SACARV':{'fator':0.978}}, 'natural':[[],['GuimanAmorim']]}
	bacias['OSE']['SaCarvalho'] = {'incremental':{'SACARV':{'fator':0.022}}, 'natural':[['GuimanAmorim'],['SaCarvalho']]}
	bacias['OSE']['SaltoGrande'] = {'incremental':{'PTOESTRELA':{'fator':1}}, 'natural':[[],['SaltoGrande']]}
	bacias['OSE']['PortoEstrela'] = {'incremental':{'PTOESTRELA':{'fator':0}}, 'natural':[['SaltoGrande'],['PortoEstrela']]}
	bacias['OSE']['Baguari'] = {'incremental':{'MASCARENHA':{'fator':0.456}}, 'natural':[['Candonga','SaCarvalho','PortoEstrela'],['Baguari']]}
	bacias['OSE']['Aimores'] = {'incremental':{'MASCARENHA':{'fator':0.195}}, 'natural':[['Baguari'],['Aimores']]}
	bacias['OSE']['Mascarenhas'] = {'incremental':{'MASCARENHA':{'fator':0.349}}, 'natural':[['Aimores'],['Mascarenhas']]}
	bacias['OSE']['Rosal'] = {'incremental':{'ROSAL':{'fator':1}}, 'natural':[[],['Rosal']]}
	bacias['OSE']['StaClara'] = {'incremental':{'SCLARA':{'fator':1}}, 'natural':[[],['StaClara']]}
 
 
	bacias['OSE']['Paraibuna'] = {'incremental':{'STABRANCA':{'fator':0.873}}, 'natural':[[],['Paraibuna']]}
	bacias['OSE']['Jaguari'] = {'incremental':{'JAGUARI':{'fator':1}}, 'natural':[[],['Jaguari']]}
	bacias['OSE']['StaBranca'] = {'incremental':{'STABRANCA':{'fator':0.127}}, 'natural':[['Paraibuna'],['StaBranca']]}
	bacias['OSE']['Funil2'] = {'incremental':{'FUNIL':{'fator':1}}, 'natural':[['StaBranca','Jaguari'],['Funil2']]}
	bacias['OSE']['StaCecilia'] = {'incremental':{'STACECILIA':{'fator':1}}, 'natural':[['Funil2'],['StaCecilia']]}
	bacias['OSE']['Picada'] = {'incremental':{'PICADA':{'fator':1}}, 'natural':[[],['Picada']]}
	bacias['OSE']['Sobragi'] = {'incremental':{'SOBRAGI':{'fator':1}}, 'natural':[['Picada'],['Sobragi']]}
	bacias['OSE']['Tocos'] = {'incremental':{'LAJESTOCOS':{'fator':0.68}}, 'natural':[[],['Tocos']]}
	bacias['OSE']['Lajes'] = {'incremental':{'LAJESTOCOS':{'fator':0.32}}, 'natural':[[],['Lajes']]}
	bacias['OSE']['Anta'] = {'incremental':{'ILHAP':{'fator':0.711}}, 'natural':[[],['Anta']]}
	bacias['OSE']['IlhaPombo'] = {'incremental':{'ILHAP':{'fator':0.289}}, 'natural':[['Anta'],['IlhaPombo']]}
	bacias['OSE']['barraBrauna'] = {'incremental':{'BBRAUNA':{'fator':1}}, 'natural':[[],['barraBrauna']]}
	
	datas = []
	vazoes = {}
	for modelo in Q_modelo:
		vazoes[modelo] = {}
		for bacia in bacias:
			Qvazoes = {'incremental':{}, 'natural':{}}
			for posto in bacias[bacia]:
				# Caso o dicionario for diferente de vazio
				if bacias[bacia][posto]['incremental'] != {}:
					# Verificação de cada trecho para a incremental
					for trecho in bacias[bacia][posto]['incremental']:

						# Se existir a incremental desejada depender apenas da fracao da incremental de apenas um posto
						if  len(bacias[bacia][posto]['incremental']) == 1 and 'fator' in bacias[bacia][posto]['incremental'][trecho] and 'tv' not in bacias[bacia][posto]['incremental'][trecho]:
							# fator multiplicativo for igual a 1
							if bacias[bacia][posto]['incremental'][trecho]['fator'] == 1:
								Qvazoes['incremental'][posto] = Q_modelo[modelo][bacia][trecho]
									
							# fator multiplicativo for menor que 1
							elif bacias[bacia][posto]['incremental'][trecho]['fator'] < 1:
								Qvazoes['incremental'][posto] = {x: y*float(bacias[bacia][posto]['incremental'][trecho]['fator']) for x, y in Q_modelo[modelo][bacia][trecho].items()}

						# Caso em que exista mais de 1 trecho e/ou tempo de viagem (fator multiplicativo) maior que 1
						else:
							Qvazoes['incremental'][posto] = {}
							if datas == []:
								posto_aux = list(Qvazoes['incremental'].keys())[0]
								datas = [data for data in Qvazoes['incremental'][posto_aux]]

							# calculo de cada dia das incrementais
							for i, data in enumerate(datas):
								Qvazoes['incremental'][posto][data] = 0

								for trecho in bacias[bacia][posto]['incremental']:
									# Caso não exista tempo de viagem
									if 'fator' in bacias[bacia][posto]['incremental'][trecho] and 'tv' not in bacias[bacia][posto]['incremental'][trecho]:
										Qvazoes['incremental'][posto][data] = Qvazoes['incremental'][posto][data] + Q_modelo[modelo][bacia][trecho][data] * bacias[bacia][posto]['incremental'][trecho]['fator']
									
									if 'tv' in bacias[bacia][posto]['incremental'][trecho]:
										fat = 1
										if 'fator' in bacias[bacia][posto]['incremental'][trecho]:
											fat = bacias[bacia][posto]['incremental'][trecho]['fator']
										# Caso o tempo de viagem for maior que 1 e menor que 24
										if bacias[bacia][posto]['incremental'][trecho]['tv'] > 1 and bacias[bacia][posto]['incremental'][trecho]['tv'] < 24:
											if i == 0:
												try:
													Qvazoes['incremental'][posto][data] = Qvazoes['incremental'][posto][data] + fat*(Q_modelo[modelo][bacia][trecho][data]*(24-bacias[bacia][posto]['incremental'][trecho]['tv'])+float(vazaoVerificado[bacia][trecho][data-datetime.timedelta(days=1)])*bacias[bacia][posto]['incremental'][trecho]['tv'])/24
												except:
													Qvazoes['incremental'][posto][data] = Qvazoes['incremental'][posto][data] + fat*(Qvazoes['natural'][trecho][data]*(24-bacias[bacia][posto]['incremental'][trecho]['tv'])+float(vazaoVerificado[bacia][trecho][data-datetime.timedelta(days=1)])*bacias[bacia][posto]['incremental'][trecho]['tv'])/24

											else:
												try:
													Qvazoes['incremental'][posto][data] = Qvazoes['incremental'][posto][data] + fat*(Q_modelo[modelo][bacia][trecho][data]*(24-bacias[bacia][posto]['incremental'][trecho]['tv'])+Q_modelo[modelo][bacia][trecho][data-datetime.timedelta(days=1)]*bacias[bacia][posto]['incremental'][trecho]['tv'])/24		
												except:
													Qvazoes['incremental'][posto][data] = Qvazoes['incremental'][posto][data] + fat*(Qvazoes['natural'][trecho][data]*(24-bacias[bacia][posto]['incremental'][trecho]['tv'])+Qvazoes['natural'][trecho][data-datetime.timedelta(days=1)]*bacias[bacia][posto]['incremental'][trecho]['tv'])/24		

										else:
											tv = bacias[bacia][posto]['incremental'][trecho]['tv']
											if i <= int(tv/24)-1:
												Qvazoes['incremental'][posto][data] = Qvazoes['incremental'][posto][data] + fat*(float(vazaoVerificado[bacia][trecho][data-datetime.timedelta(days=int(tv/24))])*((int(tv/24)+1)*24 -bacias[bacia][posto]['incremental'][trecho]['tv'])+float(vazaoVerificado[bacia][trecho][data-datetime.timedelta(days=(int(tv/24)+1))])*(bacias[bacia][posto]['incremental'][trecho]['tv']-(int(tv/24)*24)))/24
											elif i == int(tv/24):
												try:
													Qvazoes['incremental'][posto][data] = Qvazoes['incremental'][posto][data] + fat*(Q_modelo[modelo][bacia][trecho][data-datetime.timedelta(days=int(tv/24))]*((int(tv/24)+1)*24 -bacias[bacia][posto]['incremental'][trecho]['tv'])+float(vazaoVerificado[bacia][trecho][data-datetime.timedelta(days=(int(tv/24)+1))])*(bacias[bacia][posto]['incremental'][trecho]['tv']-(int(tv/24)*24)))/24
												except Exception as e:
													Qvazoes['incremental'][posto][data] = Qvazoes['incremental'][posto][data] + fat*(Qvazoes['natural'][trecho][data-datetime.timedelta(days=int(tv/24))]*((int(tv/24)+1)*24 -bacias[bacia][posto]['incremental'][trecho]['tv'])+float(vazaoVerificado[bacia][trecho][data-datetime.timedelta(days=(int(tv/24)+1))])*(bacias[bacia][posto]['incremental'][trecho]['tv']-(int(tv/24)*24)))/24
											else:
												try:
													Qvazoes['incremental'][posto][data] = Qvazoes['incremental'][posto][data] + fat*(Q_modelo[modelo][bacia][trecho][data-datetime.timedelta(days=int(tv/24))]*((int(tv/24)+1)*24 -bacias[bacia][posto]['incremental'][trecho]['tv'])+Q_modelo[modelo][bacia][trecho][data-datetime.timedelta(days=(int(tv/24)+1))]*(bacias[bacia][posto]['incremental'][trecho]['tv']-(int(tv/24)*24)))/24
												except Exception as e:
													Qvazoes['incremental'][posto][data] = Qvazoes['incremental'][posto][data] + fat*(Qvazoes['natural'][trecho][data-datetime.timedelta(days=int(tv/24))]*((int(tv/24)+1)*24 -bacias[bacia][posto]['incremental'][trecho]['tv'])+Qvazoes['natural'][trecho][data-datetime.timedelta(days=(int(tv/24)+1))]*(bacias[bacia][posto]['incremental'][trecho]['tv']-(int(tv/24)*24)))/24

				# Caso o dicionario da incremental estiver vazio
				else:
					Qvazoes['incremental'][posto] = {}
					posto_aux = list(Qvazoes['incremental'].keys())[0]
					for i, data in enumerate(Qvazoes['incremental'][posto_aux]):
						Qvazoes['incremental'][posto][data] = 0

				if bacias[bacia][posto]['natural'] != []:
					if bacias[bacia][posto]['natural'][0] == [] and len(bacias[bacia][posto]['natural'][1]) == 1:
						Qvazoes['natural'][posto] = Qvazoes['incremental'][bacias[bacia][posto]['natural'][1][0]]

					else:
						posto_aux = list(Qvazoes['incremental'].keys())[0]
						Qvazoes['natural'][posto] = {x: 0 for x, y in Qvazoes['incremental'][posto_aux].items()}

						for trecho in bacias[bacia][posto]['natural'][0]:
							aux = np.add(list(Qvazoes['natural'][posto].values()), list(Qvazoes['natural'][trecho].values()))
							Qvazoes['natural'][posto] = dict(zip(list(Qvazoes['natural'][posto].keys()), aux))

						for trecho in bacias[bacia][posto]['natural'][1]:
							aux = np.add(list(Qvazoes['natural'][posto].values()), list(Qvazoes['incremental'][trecho].values()))
							Qvazoes['natural'][posto] = dict(zip(list(Qvazoes['natural'][posto].keys()), aux))

						# Somatorio do fator de 0.185 a todo array de Edgard Souza
						if posto == 'EdgardSouza':
							aux = np.add(list(Qvazoes['natural'][posto].values()), 0.185)
							Qvazoes['natural'][posto] = dict(zip(list(Qvazoes['natural'][posto].keys()), aux))

			if bacia == 'Norte':
				QvazoesTocantins = propagaTocantins(Q_modelo[modelo][bacia],Qvazoes['natural'],vazaoVerificado[bacia],datas)
				Qvazoes['incremental'].update(QvazoesTocantins['incremental'])
				Qvazoes['natural'].update(QvazoesTocantins['natural'])
				Qvazoes['natural']['Jirau'],Qvazoes['natural']['SantoAntonio'],Qvazoes['incremental']['Jirau'] ,Qvazoes['incremental']['SantoAntonio']  = propagaMadeira(Q_modelo[modelo][bacia],vazaoVerificado['Norte'],datas)
			elif bacia == 'NE':
				Qvazoes['natural']['Itapebi'] = propagaJequiParnai(Q_modelo[modelo][bacia],vazaoVerificado['NE'],datas)
			elif bacia == 'Paranazao':
				Qvazoes['incremental']['Itaipu'] = Qvazoes['natural']['Itaipu']

			vazoes[modelo][bacia] = Qvazoes

	return vazoes

def propagaJequiParnai(Q_modelo,vaz_verificado,data_out):

	# constantes da Irape
	c1_irape = 0.166666666666667;
	c2_irape = 0.666666666666667;
	c3_irape = 0.166666666666667

	# loop para concatenar observado smap com a saida smap
	vazao={}
	for posto in ['IRAPE', 'ITAPEBI', 'UBESP']:
		vazao[posto]={}
		vazao[posto]={**vaz_verificado[posto],**Q_modelo[posto]}

	vaz_prop={}
	vaz_prop['n1_irape'] = {}
	vaz_prop['n2_irape'] = {}
	vaz_prop['n3_irape'] = {}
	vaz_prop['n4_irape'] = {}
	vaz_prop['n5_irape'] = {}

	vaz_out={'Itapebi':{}}
	# array de datas
	datas = [data for data in vazao['IRAPE']]
	for i, data in enumerate(datas):
		if i == 0:
			vaz_prop['n1_irape'][data] = vazao['IRAPE'][data]
			vaz_prop['n2_irape'][data] = vazao['IRAPE'][data]
			vaz_prop['n3_irape'][data] = vazao['IRAPE'][data]
			vaz_prop['n4_irape'][data] = vazao['IRAPE'][data]
			vaz_prop['n5_irape'][data] = vazao['IRAPE'][data]
		else:
			try:
				vaz_prop['n1_irape'][data] = vazao['IRAPE'][data]*c1_irape + vazao['IRAPE'][data - datetime.timedelta(days=1)]*c2_irape + vaz_prop['n1_irape'][data - datetime.timedelta(days=1)]*c3_irape
			except:
				pdb.set_trace()
			vaz_prop['n2_irape'][data] = vaz_prop['n1_irape'][data]*c1_irape + vaz_prop['n1_irape'][data - datetime.timedelta(days=1)]*c2_irape + vaz_prop['n2_irape'][data - datetime.timedelta(days=1)]*c3_irape
			vaz_prop['n3_irape'][data] = vaz_prop['n2_irape'][data]*c1_irape + vaz_prop['n2_irape'][data - datetime.timedelta(days=1)]*c2_irape + vaz_prop['n3_irape'][data - datetime.timedelta(days=1)]*c3_irape
			vaz_prop['n4_irape'][data] = vaz_prop['n3_irape'][data]*c1_irape + vaz_prop['n3_irape'][data - datetime.timedelta(days=1)]*c2_irape + vaz_prop['n4_irape'][data - datetime.timedelta(days=1)]*c3_irape
			vaz_prop['n5_irape'][data] = vaz_prop['n4_irape'][data]*c1_irape + vaz_prop['n4_irape'][data - datetime.timedelta(days=1)]*c2_irape + vaz_prop['n5_irape'][data - datetime.timedelta(days=1)]*c3_irape

		vaz_out['Itapebi'][data] = vaz_prop['n5_irape'][data] + vazao['ITAPEBI'][data]

	vaz_out['Itapebi'] = { dt: vaz_out['Itapebi'][dt] for dt in data_out }
	return vaz_out['Itapebi']

def propagaMadeira(Q_modelo,vaz_verificado,data_out):

	# constantes da propagacao principe 
	p_c1 = 0.375; p_c2 = 0.625; p_c3 = 0
	# constantes da propagacao guaja
	g_c1 = 0.375; g_c2 = 0.625; g_c3 = 0
	tv_g=14; tv_j = 23
	# loop para concatenar observado smap com a saida smap
	vazao={}
	for posto in ['GUAJ-MIRIM', 'JIRAU2', 'P_DA_BEIRA', 'S.ANTONIO', 'GUAPORE', 'SAMUEL', 'RONDONII', 'DARDANELOS']:
		vazao[posto]={}
		vazao[posto]={**vaz_verificado[posto],**Q_modelo[posto]}
	
	vaz_prop={}
	vaz_prop['GM_tot']= {}
	vaz_prop['GM_tv']= {}
	vaz_prop['jirau']= {} #VNA
	vaz_prop['SantoAntonio']= {} #VNA
	vaz_prop['p_n1']={};vaz_prop['p_n2']={}
	vaz_prop['g_n1']={};vaz_prop['g_n2']={}

	# array de datas
	datas = [data for data in vazao['P_DA_BEIRA']]
	for i, data in enumerate(datas):
		if i == 0:
			# contas para estreito
			vaz_prop['p_n1'][data]  = vazao['P_DA_BEIRA'][data]
			vaz_prop['p_n2'][data]  = vazao['P_DA_BEIRA'][data]
			# contas GM Flu total
			vaz_prop['GM_tot'][data] = vazao['GUAJ-MIRIM'][data] + vaz_prop['p_n2'][data] 
			# Primeira propagacao GM Flu total por TV
			vaz_prop['GM_tv'][data] = vaz_prop['GM_tot'][data]
			# Segunda propagacao GM Flu total
			vaz_prop['g_n1'][data] = vaz_prop['GM_tv'][data]
			vaz_prop['g_n2'][data] = vaz_prop['GM_tv'][data]
			# contas VNA Jirau
			vaz_prop['jirau'][data] = vaz_prop['g_n2'][data] + vazao['JIRAU2'][data]
			# Propagacao JIRAU TV
			vaz_prop['SantoAntonio'][data] = vaz_prop['jirau'][data] + vazao['S.ANTONIO'][data]

		else:
			# contas para estreito
			vaz_prop['p_n1'][data] = vazao['P_DA_BEIRA'][data]*p_c1 + vazao['P_DA_BEIRA'][data-datetime.timedelta(days=1)]*p_c2 + vaz_prop['p_n1'][data-datetime.timedelta(days=1)]*p_c3
			vaz_prop['p_n2'][data] = vaz_prop['p_n1'][data]*p_c1 + vaz_prop['p_n1'][data-datetime.timedelta(days=1)]*p_c2 + vaz_prop['p_n2'][data-datetime.timedelta(days=1)]*p_c3
			# contas GM Flu total
			vaz_prop['GM_tot'][data] = vazao['GUAJ-MIRIM'][data] + vaz_prop['p_n2'][data] 
			# Primeira propagacao GM Flu total por TV			
			vaz_prop['GM_tv'][data] = (vaz_prop['GM_tot'][data]*(24-tv_g) + vaz_prop['GM_tot'][data-datetime.timedelta(days=1)]*tv_g)/24
			# Segunda propagacao GM Flu total
			vaz_prop['g_n1'][data] = vaz_prop['GM_tv'][data]*g_c1 + vaz_prop['GM_tv'][data-datetime.timedelta(days=1)]*g_c2 + vaz_prop['g_n1'][data-datetime.timedelta(days=1)]*g_c3
			vaz_prop['g_n2'][data] = vaz_prop['g_n1'][data]*g_c1 + vaz_prop['g_n1'][data-datetime.timedelta(days=1)]*g_c2 + vaz_prop['g_n2'][data-datetime.timedelta(days=1)]*g_c3
			# contas VNA Jirau
			vaz_prop['jirau'][data] = vaz_prop['g_n2'][data] + vazao['JIRAU2'][data]
			# Propagacao JIRAU TVvazao['S.Antonio'][data]
			vaz_prop['SantoAntonio'][data] = (vaz_prop['jirau'][data]*(24-tv_j) + vaz_prop['jirau'][data-datetime.timedelta(days=1)]*tv_j)/24 + vazao['S.ANTONIO'][data]
	vaz_out={}
	vaz_out['SantoAntonio']={}
	vaz_out['jirau']=vaz_prop['jirau']

	for i in data_out:
		vaz_out['SantoAntonio'][i] = vaz_prop['jirau'][i]+vazao['S.ANTONIO'][i]

	return vaz_out['jirau'],vaz_out['SantoAntonio'],vazao['JIRAU2'],vazao['S.ANTONIO']


def propagaTocantins(Q_modelo,Qvazoes,vaz_verificado,data_out):

	# constantes de BANDEIRANTES - CONCEICAO DO ARAGUAIA
	c1_ban_ca = 0.152775192170181;
	c2_ban_ca = 0.152775192170181;
	c3_ban_ca = 0.694449615659638;

	# constantes de CONCEICAO DO ARAGUAIA - TUCURUI
	c1_ca_tuc = 0.0322580645161291;
	c2_ca_tuc = 0.612903225806452;
	c3_ca_tuc = 0.354838709677419;

	# constantes de LAJEADO - ESTREITO
	c1_laj_est = 0.343207069827643;
	c2_laj_est = 0.366616629265668;
	c3_laj_est = 0.290176300906689;

	# constantes de PORTO REAL - ESTREITO
	c1_pr_est = 0.235961557677904;
	c2_pr_est = 0.622494756568882;
	c3_pr_est = 0.141543685753214;

	# constantes de ESTREITO - TUCURUI
	c1_est_tuc = 0.0502793296089386;
	c2_est_tuc = 0.620111731843575;
	c3_est_tuc = 0.329608938547486;

	vaz_prop = {}
	vaz_prop['bc_n1']={}
	vaz_prop['bc_n2']={}
	vaz_prop['bc_n3']={}
	vaz_prop['bc_n3']={}
	vaz_prop['bc_n4']={}
	vaz_prop['bc_n5']={}
	vaz_prop['bc_n6']={}
	vaz_prop['bc_n7']={}
	vaz_prop['bc_n8']={}
	vaz_prop['bc_n9']={}

	vaz_prop['ca_n1']={}
	vaz_prop['ca_n2']={}
	vaz_prop['ca_n3']={}

	vaz_prop['le_n1']={}
	vaz_prop['le_n2']={}
	vaz_prop['le_n3']={}

	vaz_prop['pe_n1']={}
	vaz_prop['pe_n2']={}
	vaz_prop['pe_n3']={}

	vaz_prop['et_n1']={}
	vaz_prop['et_n2']={}
	vaz_prop['et_n3']={}

	# loop para concatenar observado smap com a saida smap 
	vazao={}
	
	for posto in ['SMESA', 'BANDEIRANT', 'C.ARAGUAIA', 'PORTO REAL', 'ESTREITO', 'LAJEADO', 'TUCURUI']:
		vazao[posto]={}
		vazao[posto]={**vaz_verificado[posto],**Q_modelo[posto]}

	datas = [data for data in vazao['C.ARAGUAIA']]
	vni = {'Estreito':{},'Tucurui':{}}
	vaz_out = {'incremental':{}, 'natural':{}}
	vaz_out['incremental']['SerraMesa'] = {}
	vaz_out['incremental']['CanaBrava'] = {}
	vaz_out['incremental']['SaoSalvador'] = {}
	vaz_out['incremental']['PeixeAngical'] = {}
	vaz_out['incremental']['Lajeado'] = {}
	vaz_out['incremental']['Estreito'] = {}
	vaz_out['incremental']['Tucurui'] = {}

	vaz_out['natural']['SerraMesa'] = {}
	vaz_out['natural']['CanaBrava'] = {}
	vaz_out['natural']['SaoSalvador'] = {}
	vaz_out['natural']['PeixeAngical'] = {}
	vaz_out['natural']['Lajeado'] = {}
	vaz_out['natural']['Estreito'] = {}
	vaz_out['natural']['Tucurui'] = {}

	tv_serraMesa = 10
	tv_canaBrava = 16
	tv_saoSalvador = 16
	tv_peixeAngelical = 64

	fatorVia_canaBrava = 0.056
	fatorVia_saoSalvador = 0.055
	fatorVia_peixeAngical = 0.434
	fatorVia_lajeado = 0.455

	for i, data in enumerate(datas):

		vaz_out['incremental']['SerraMesa'][data] = vazao['SMESA'][data]
		vaz_out['incremental']['CanaBrava'][data] = vazao['LAJEADO'][data] * fatorVia_canaBrava
		vaz_out['incremental']['SaoSalvador'][data] = vazao['LAJEADO'][data] * fatorVia_saoSalvador
		vaz_out['incremental']['PeixeAngical'][data] = vazao['LAJEADO'][data] * fatorVia_peixeAngical
		vaz_out['incremental']['Lajeado'][data] = vazao['LAJEADO'][data] * fatorVia_lajeado
		vaz_out['incremental']['Estreito'][data] = vazao['ESTREITO'][data]
		vaz_out['incremental']['Tucurui'][data] = vazao['TUCURUI'][data]

		vaz_out['natural']['SerraMesa'][data] = vaz_out['incremental']['SerraMesa'][data]

		if i == 0:
			vaz_out['natural']['CanaBrava'][data] = (vazao['SMESA'][data+datetime.timedelta(days=1)]*(24-tv_serraMesa)+vaz_out['natural']['SerraMesa'][data]*tv_serraMesa)/24 + vazao['LAJEADO'][data+datetime.timedelta(days=1)]*fatorVia_canaBrava
			vaz_out['natural']['SaoSalvador'][data] = vaz_out['natural']['CanaBrava'][data] + vazao['LAJEADO'][data+datetime.timedelta(days=1)]*fatorVia_saoSalvador
			vaz_out['natural']['PeixeAngical'][data] = vaz_out['natural']['SaoSalvador'][data] + vazao['LAJEADO'][data+datetime.timedelta(days=1)]*fatorVia_lajeado
		else:
			vaz_out['natural']['CanaBrava'][data] = (vaz_out['natural']['SerraMesa'][data]*(24-tv_serraMesa)+vaz_out['natural']['SerraMesa'][data-datetime.timedelta(days=1)]*tv_serraMesa)/24 + vaz_out['incremental']['CanaBrava'][data]
			vaz_out['natural']['SaoSalvador'][data] = (vaz_out['natural']['CanaBrava'][data]*(24-tv_canaBrava)+vaz_out['natural']['CanaBrava'][data-datetime.timedelta(days=1)]*tv_canaBrava)/24 + vaz_out['incremental']['SaoSalvador'][data]
			vaz_out['natural']['PeixeAngical'][data] = (vaz_out['natural']['SaoSalvador'][data]*(24-tv_saoSalvador)+vaz_out['natural']['SaoSalvador'][data-datetime.timedelta(days=1)]*tv_saoSalvador)/24 + vaz_out['incremental']['PeixeAngical'][data]

	for i, data in enumerate(datas):
		if i < 4:
			vaz_out['natural']['Lajeado'][data] = (vaz_out['natural']['PeixeAngical'][data+datetime.timedelta(days=2-i)]*(72-tv_peixeAngelical)+vaz_out['natural']['PeixeAngical'][data+datetime.timedelta(days=1-i)]*(tv_peixeAngelical-48))/24 + vaz_out['incremental']['Lajeado'][data+datetime.timedelta(days=4-i)]
		else:
			vaz_out['natural']['Lajeado'][data] = (vaz_out['natural']['PeixeAngical'][data-datetime.timedelta(days=2)]*(72-tv_peixeAngelical)+vaz_out['natural']['PeixeAngical'][data-datetime.timedelta(days=3)]*(tv_peixeAngelical-48))/24 + vaz_out['incremental']['Lajeado'][data]

		if i == 0:
			vaz_prop['bc_n1'][data] = vazao['BANDEIRANT'][data]
			vaz_prop['bc_n2'][data] = vaz_prop['bc_n1'][data]
			vaz_prop['bc_n3'][data] = vaz_prop['bc_n2'][data]
			vaz_prop['bc_n4'][data] = vaz_prop['bc_n3'][data]
			vaz_prop['bc_n5'][data] = vaz_prop['bc_n4'][data]
			vaz_prop['bc_n6'][data] = vaz_prop['bc_n5'][data]
			vaz_prop['bc_n7'][data] = vaz_prop['bc_n6'][data]
			vaz_prop['bc_n8'][data] = vaz_prop['bc_n7'][data]
			vaz_prop['bc_n9'][data] = vaz_prop['bc_n8'][data]

			vaz_prop['ca_n1'][data] = vazao['C.ARAGUAIA'][data] + vaz_prop['bc_n9'][data]
			vaz_prop['ca_n2'][data] = vaz_prop['ca_n1'][data]
			vaz_prop['ca_n3'][data] = vaz_prop['ca_n2'][data]

			vaz_prop['le_n1'][data] = vaz_out['natural']['Lajeado'][data]
			vaz_prop['le_n2'][data] = vaz_prop['le_n1'][data]
			vaz_prop['le_n3'][data] = vaz_prop['le_n2'][data]

			vaz_prop['pe_n1'][data] = vazao['PORTO REAL'][data]
			vaz_prop['pe_n2'][data] = vaz_prop['pe_n1'][data]

			vaz_prop['et_n1'][data] = vaz_prop['pe_n2'][data] + vaz_prop['le_n3'][data]
			vaz_prop['et_n2'][data] = vaz_prop['et_n1'][data]
			vaz_prop['et_n3'][data] = vaz_prop['et_n2'][data]

		else:
			vaz_prop['bc_n1'][data] = vazao['BANDEIRANT'][data]*c1_ban_ca + vazao['BANDEIRANT'][data - datetime.timedelta(days=1)]*c2_ban_ca + vaz_prop['bc_n1'][data - datetime.timedelta(days=1)]*c3_ban_ca
			vaz_prop['bc_n2'][data] = vaz_prop['bc_n1'][data]*c1_ban_ca + vaz_prop['bc_n1'][data - datetime.timedelta(days=1)]*c2_ban_ca + vaz_prop['bc_n2'][data - datetime.timedelta(days=1)]*c3_ban_ca
			vaz_prop['bc_n3'][data] = vaz_prop['bc_n2'][data]*c1_ban_ca + vaz_prop['bc_n2'][data - datetime.timedelta(days=1)]*c2_ban_ca + vaz_prop['bc_n3'][data - datetime.timedelta(days=1)]*c3_ban_ca
			vaz_prop['bc_n4'][data] = vaz_prop['bc_n3'][data]*c1_ban_ca + vaz_prop['bc_n3'][data - datetime.timedelta(days=1)]*c2_ban_ca + vaz_prop['bc_n4'][data - datetime.timedelta(days=1)]*c3_ban_ca
			vaz_prop['bc_n5'][data] = vaz_prop['bc_n4'][data]*c1_ban_ca + vaz_prop['bc_n4'][data - datetime.timedelta(days=1)]*c2_ban_ca + vaz_prop['bc_n5'][data - datetime.timedelta(days=1)]*c3_ban_ca
			vaz_prop['bc_n6'][data] = vaz_prop['bc_n5'][data]*c1_ban_ca + vaz_prop['bc_n5'][data - datetime.timedelta(days=1)]*c2_ban_ca + vaz_prop['bc_n6'][data - datetime.timedelta(days=1)]*c3_ban_ca
			vaz_prop['bc_n7'][data] = vaz_prop['bc_n6'][data]*c1_ban_ca + vaz_prop['bc_n6'][data - datetime.timedelta(days=1)]*c2_ban_ca + vaz_prop['bc_n7'][data - datetime.timedelta(days=1)]*c3_ban_ca
			vaz_prop['bc_n8'][data] = vaz_prop['bc_n7'][data]*c1_ban_ca + vaz_prop['bc_n7'][data - datetime.timedelta(days=1)]*c2_ban_ca + vaz_prop['bc_n8'][data - datetime.timedelta(days=1)]*c3_ban_ca
			vaz_prop['bc_n9'][data] = vaz_prop['bc_n8'][data]*c1_ban_ca + vaz_prop['bc_n8'][data - datetime.timedelta(days=1)]*c2_ban_ca + vaz_prop['bc_n9'][data - datetime.timedelta(days=1)]*c3_ban_ca

			vaz_prop['ca_n1'][data] = (vazao['C.ARAGUAIA'][data] + vaz_prop['bc_n9'][data])*c1_ca_tuc + (vazao['C.ARAGUAIA'][data - datetime.timedelta(days=1)] + vaz_prop['bc_n9'][data - datetime.timedelta(days=1)])*c2_ca_tuc + vaz_prop['ca_n1'][data - datetime.timedelta(days=1)]*c3_ca_tuc 
			vaz_prop['ca_n2'][data] = vaz_prop['ca_n1'][data]*c1_ca_tuc + vaz_prop['ca_n1'][data - datetime.timedelta(days=1)]*c2_ca_tuc + vaz_prop['ca_n2'][data - datetime.timedelta(days=1)]*c3_ca_tuc 
			vaz_prop['ca_n3'][data] = vaz_prop['ca_n2'][data]*c1_ca_tuc + vaz_prop['ca_n2'][data - datetime.timedelta(days=1)]*c2_ca_tuc + vaz_prop['ca_n3'][data - datetime.timedelta(days=1)]*c3_ca_tuc

			vaz_prop['le_n1'][data] = vaz_out['natural']['Lajeado'][data]*c1_laj_est + vaz_out['natural']['Lajeado'][data - datetime.timedelta(days=1)]*c2_laj_est + vaz_prop['le_n1'][data - datetime.timedelta(days=1)]*c3_laj_est
			vaz_prop['le_n2'][data] = vaz_prop['le_n1'][data]*c1_laj_est + vaz_prop['le_n1'][data - datetime.timedelta(days=1)]*c2_laj_est + vaz_prop['le_n2'][data - datetime.timedelta(days=1)]*c3_laj_est
			vaz_prop['le_n3'][data] = vaz_prop['le_n2'][data]*c1_laj_est + vaz_prop['le_n2'][data - datetime.timedelta(days=1)]*c2_laj_est + vaz_prop['le_n3'][data - datetime.timedelta(days=1)]*c3_laj_est

			vaz_prop['pe_n1'][data] = vazao['PORTO REAL'][data]*c1_pr_est + vazao['PORTO REAL'][data - datetime.timedelta(days=1)]*c2_pr_est + vaz_prop['pe_n1'][data - datetime.timedelta(days=1)]*c3_pr_est
			vaz_prop['pe_n2'][data] = vaz_prop['pe_n1'][data]*c1_pr_est + vaz_prop['pe_n1'][data - datetime.timedelta(days=1)]*c2_pr_est + vaz_prop['pe_n2'][data - datetime.timedelta(days=1)]*c3_pr_est

			vaz_prop['et_n1'][data] = (vaz_prop['pe_n2'][data] + vaz_prop['le_n3'][data] + vaz_out['incremental']['Estreito'][data])*c1_est_tuc + (vaz_prop['pe_n2'][data - datetime.timedelta(days=1)] + vaz_prop['le_n3'][data - datetime.timedelta(days=1)] + vaz_out['incremental']['Estreito'][data - datetime.timedelta(days=1)])*c2_est_tuc + vaz_prop['et_n1'][data - datetime.timedelta(days=1)]*c3_est_tuc
			vaz_prop['et_n2'][data] = vaz_prop['et_n1'][data]*c1_est_tuc + vaz_prop['et_n1'][data - datetime.timedelta(days=1)]*c2_est_tuc + vaz_prop['et_n2'][data - datetime.timedelta(days=1)]*c3_est_tuc
			vaz_prop['et_n3'][data] = vaz_prop['et_n2'][data]*c1_est_tuc + vaz_prop['et_n2'][data - datetime.timedelta(days=1)]*c2_est_tuc + vaz_prop['et_n3'][data - datetime.timedelta(days=1)]*c3_est_tuc

		vaz_out['natural']['Estreito'][data] = vaz_prop['pe_n2'][data] + vaz_prop['le_n3'][data] + vaz_out['incremental']['Estreito'][data]
		vaz_out['natural']['Tucurui'][data] = vaz_prop['et_n3'][data] +vaz_prop['ca_n3'][data] + vaz_out['incremental']['Tucurui'][data]
		vni['Estreito'][data] = vaz_prop['pe_n2'][data] + vaz_out['incremental']['Estreito'][data]
		vni['Tucurui'][data] = vaz_prop['ca_n3'][data] + vaz_out['incremental']['Tucurui'][data]

	# colocando a VNI 
	vaz_out['incremental']['Estreito'] = vni['Estreito']
	vaz_out['incremental']['Tucurui'] = vni['Tucurui']

	return vaz_out

def get_vazao_nat(dtInicio, dtFinal):

	wxdb = wx_dbLib.WxDataB()
	query = '''SELECT
					DT_REFERENTE,
					CD_POSTO,
					VL_VAZOES_DIARIAS
				FROM
					dbo.TB_VAZOES_DIARIAS
				WHERE
					DT_REFERENTE >= \'{}\' AND
					DT_REFERENTE <= \'{}\''''.format(dtInicio,dtFinal)

	resposta_db = wxdb.requestServer(query)
	
	vazNat = {}
	for row in resposta_db:
		if row[1] not in vazNat:
			vazNat[row[1]] = {}
		vazNat[row[1]][row[0]]=row[2]

	return vazNat

def get_vazao_inc(dtInicio,dtFinal):
	wxdb = wx_dbLib.WxDataB()
	query = '''SELECT
					DT_REFERENTE,
					CD_POSTO,
					VL_VAZAO_INC
				FROM
					TB_VAZAO_DFL_INC
				WHERE
					DT_REFERENTE >= \'{}\' AND
					DT_REFERENTE <= \'{}\''''.format(dtInicio,dtFinal)
	resposta_db = wxdb.requestServer(query)

	vazInc = {}
	for row in resposta_db:
		if row[1] not in vazInc:
			vazInc[row[1]] = {}
		vazInc[row[1]][row[0]]=row[2]

	return vazInc	

def smap_paralelo(data, modelos, run, ndia='', flag_pdp=False):

	# Configura as variáveis
	diropera = os.path.join(diretorioApp,'arquivos','opera-smap')

	if ndia == '':
		ndia=30

	dataExc = datetime.datetime.strptime(data, "%d/%m/%Y")

	# Data de execucao somente vai ser diferente da data referente caso for rodadas preliminares
	if 'PRELIMINAR' in str(modelos):
		dataRef = dataExc - datetime.timedelta(days=1)
	else:
		dataRef = dataExc

	dataExc_str = dataExc.strftime("%Y%m%d")
	dataRef_str = dataRef.strftime("%Y%m%d")

	primeirodiasemana = dataRef.date()
	nDiasDesdeInicioSemana = ndia
	while primeirodiasemana.weekday() != 5:
		primeirodiasemana = primeirodiasemana - datetime.timedelta(days=1)
		nDiasDesdeInicioSemana += 1

	ultimodiardh = dataRef.date() - datetime.timedelta(days=1)

	iniciordh = primeirodiasemana.strftime("%Y/%m/%d")		#primeiro dia da sem. eletrica
	fimrdh = ultimodiardh.strftime("%Y/%m/%d")				#ultimo dia para puxar do rdh
	hoje_rdh = dataRef.strftime("%Y/%m/%d")

	# Sabado
	if iniciordh > fimrdh:
		iniciordh = fimrdh

	dirCV = os.path.join(middle,'Chuva-vazão')

	# bacias={'GRD_PRNB':{},'NE':{},'Norte':{},'OSE':{},'Paranazao':{},'Sul':{}}

	# bacias['GRD_PRNB']['trechos'] = ['AVERMELHA', 'CAMARGOS', 'CAPESCURO', 'CORUMBA1', 'CORUMBAIV', 'EDACUNHA', 'EMBORCACAO', 'ESPORA', 'FOZCLARO', 'FUNIL MG', 'FURNAS', 'ITUMBIARA', 'MARIMBONDO', 'NOVAPONTE', 'PARAGUACU', 'PASSAGEM', 'PBUENOS', 'PCOLOMBIA', 'RVERDE', 'SALTOVERDI', 'SDOFACAO', 'SSIMAO2']
	# bacias['NE']['trechos'] = ['BOQ', 'IRAPE', 'ITAPEBI', 'PCAVALO', 'QM', 'RB-SMAP', 'SFR2', 'SRM2', 'TM-SMAP', 'UBESP']
	# bacias['Norte']['trechos'] = ['BALBINA', 'BANDEIRANT', 'C.ARAGUAIA', 'COLIDER', 'CURUAUNA', 'DARDANELOS', 'ESTREITO', 'FGOMES', 'GUAJ-MIRIM', 'GUAPORE', 'JIRAU2', 'LAJEADO', 'PIMENTALT', 'PORTO REAL', 'P_DA_BEIRA', 'RONDONII', 'S.ANTONIO', 'SAMUEL', 'SMANOEL', 'SMESA', 'STOANTJARI', 'TUCURUI']
	# bacias['OSE']['trechos'] = ['CANDONGA', 'ITIQUIRAI', 'JAURU', 'MANSO', 'MASCARENHA', 'PDEPEDRA', 'PTOESTRELA', 'ROSAL', 'SACARV', 'SCLARA']
	# bacias['Paranazao']['trechos'] = ['BALSA', 'BBONITA', 'CANOASI', 'CAPIVARA', 'CHAVANTES', 'ESOUZA', 'FLOR+ESTRA', 'FZB', 'IBITINGA', 'ILHAEQUIV', 'ITAIPU', 'IVINHEMA', 'JUPIA', 'JURUMIRIM', 'MAUA', 'NAVANHANDA', 'PPRI', 'PTAQUARA', 'ROSANA', 'SDO']
	# bacias['Sul']['trechos'] = ['14JULHO', 'BG', 'CALVES', 'CN', 'DFRANC', 'ERNESTINA', 'FOA', 'FOZCHAPECO', 'GPSOUZA', 'ITA', 'JORDSEG', 'MACHADINHO', 'MONJOLINHO', 'PASSOREAL', 'QQUEIXO', 'SALTOPILAO', 'SCAXIAS', 'SJOAO', 'STACLARA', 'UVITORIA']

	bacias['GRD_PRNB']['postos'] = {1:'Camargos', 2:'Itutinga', 211:'Funil', 6:'Furnas', 7:'Moraes', 8:'Estreito', 9:'Jaguara', 10:'Igarapava', 11:'Vgrande', 12:'Pcolombia', 14:'Caconde', 15:'Euclides', 16:'Limoeiro', 17:'Marimbondo', 18:'AguaVermelha',205:'Corumba4', 23:'Corumba3', 209:'Corumba1', 22:'Batalha', 251:'SerraFacao', 24:'Emborcacao', 25:'NovaPonte', 206:'Miranda', 207:'Cbranco1', 28:'Cbranco2', 31:'Itumbiara', 32:'Cdourada', 33:'SaoSimao', 99:'Espora', 247:'Cacu', 294:'Salto', 241:'SaltoVerdinho', 248:'Coqueiros', 261:'RioClaro'}
	bacias['NE']['postos'] = {188:'Itapebi',190:'BoaEsperanca',255:'Irape',254:'PedraCavalo',155:'RetiroBaixo', 156:'TresMarias', 158:'Queimado'}
	bacias['Norte']['postos'] = {285:'Jirau', 287: 'SantoAntonio',145: 'RondonII', 279: 'Samuel', 291: 'Dardanelos', 296: 'Guapore',204:'CachoeiraCaldeirao', 269:'Balbina', 277:'CuruaUna', 280:'CoaracyNunes', 290:'StoAntonioJari', 297:'FerreiraGomes',270:'SerraMesa', 257:'PeixeAngical', 191:'CanaBrava', 253:'SaoSalvador', 273:'Lajeado', 271:'Estreito', 275:'Tucurui',227:'Sinop', 228:'Colider', 288:'Pimental', 229:'TelesPires', 230:'SaoMiguel'}
	bacias['OSE']['postos'] = {149:'Candonga', 262:'GuimanAmorim', 134:'SaltoGrande', 263:'PortoEstrela', 141:'Baguari', 148:'Aimores', 144:'Mascarenhas',259:'ItiquiraI', 278:'Manso', 281:'PdePedra', 295:'Jauru',196:'Rosal', 283:'StaClara', 121: 'Paraibuna', 120: 'Jaguari', 122: 'StaBranca', 123: 'Funil2', 125: 'StaCecilia', 197: 'Picada', 198: 'Sobragi', 201: 'Tocos', 202: 'Lajes', 129: 'Anta', 130: 'IlhaPombo', 135:'barraBrauna'}
	bacias['Paranazao']['postos'] = {34:'Ilhasolteira', 154: 'SaoDomingos', 243:'TresIrmaos', 245:'Jupia', 246:'PortoPrimavera', 266:'Itaipu',47:'Jurumim', 48:'Piraju', 49:'Chavantes', 249:'Ourinhos', 50:'SaltoGrande', 51:'Canoas2', 52:'Canoas1', 57:'Maua', 61:'Capivara', 62:'Taquarucu', 63:'Rosana',119:'BillPedras', 117:'Guarapiranga', 160:'PonteNova', 161:'EdgardSouza', 237:'BarraBonita', 238:'Bariri', 239:'Ibitinga', 240:'Promissao', 242:'Avanhandava'}
	bacias['Sul']['postos'] = {71:'SantaClara', 72:'Fundao', 73:'Jordao', 74:'FozAreia', 76:'Segredo', 77:'SaltoSantiago', 78:'SaltoOsorio', 222:'SaltoCaxias',110:'Ernestina', 111: 'PassoReal', 112:'Jacui', 113:'Itauba', 114:'DonaFrancisca', 98:'CastroAlves', 97:'MonteClaro', 284:'14Julho', 115:'GPSouza', 101:'SaltoPilao',215:'BarraGrande', 88:'SaoRoque', 89:'Garibaldi', 216:'CamposNovos', 217:'Machadinho', 92:'Ita', 93:'PassoFundo', 220:'Monjolinho', 94:'FozChapeco', 286:'QuebraQueixo', 102:'SaoJose', 103:'SaoJoao', 81:'Baixoig'}


	# TESTA SE O RDH ESTA ATUALIZADO
	wxdb = wx_dbLib.WxDataB()
	action = '''SELECT
					*
				FROM
					VW_ACOMPH_RDH
				WHERE
					DT_REFERENTE BETWEEN \'{}\' and \'{}\'
					and CD_POSTO = 266
					AND FONTE_DADOS like \'ACOMPH\''''.format(fimrdh,hoje_rdh)

	while 1:
		print('VERIFICANDO SE O RDH ESTA ATUALIZADO NO BANCO')
		resposta_db = wxdb.requestServer(action)
		if resposta_db != []:
			print("RDH da data {} - OK".format(resposta_db[0][0].date()))
			break
		else:
			print("RDH AINDA NAO ATUALIZADO PARA A DATA " +fimrdh+ ", nova tentativa em 5 minutos...")
			time.sleep(5*60)


	path = os.path.join(diropera, dataExc_str)

	if 'PRELIMINAR' in str(modelos):
		pathdropbox = os.path.join(dirdados_ent,dataRef_str)
		dirSaida = os.path.join(dirCV,dataExc_str,'preliminar')
	else:
		pathdropbox = os.path.join(dirdados_ent,dataExc_str)
		dirSaida = os.path.join(dirCV,dataExc_str,'teste')

	if not os.path.exists(dirSaida):
		os.makedirs(dirSaida)

	dirEntrada = os.path.join(pathdropbox,'SMAP')
	if not os.path.exists(dirEntrada):
		print("Verificar o diretorio:\n\t{}".format(dirEntrada))
		quit()

	print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
	print("%%% Criando pastas e arquivos entrada %%%")
	print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")

	cria_arqentrada_smap(path, dataExc, modelos, pathdropbox, run, ndia)

	print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
	print("%%% Rodando modelos SMAP em paralelo  %%%")
	print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")

	start_time_total = time.time()
	threads = []
	for bacia in bacias:
		pathBacia = os.path.join(path,bacia)
		t = threading.Thread(target=runBatsmap, args=[pathBacia])
		threads.append(t)
		t.start()

	for p in threads:
		p.join()

	print('\nFinalizado para todas as bacias em {:.2f} segundos!'.format(time.time() - start_time_total))

	print('SMAP RODADO')
	print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
	print("%% Convertendo incremental para natural %%")
	print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")

	vaz_prev = leituraSaidaSmap(path, modelos)

	vazoes = vazIncremental(path, vaz_prev)

	vazNatRdh = get_vazao_nat(dtInicio=iniciordh, dtFinal=fimrdh)  # Tentar tirar a funcao get_acomph()
	vazIncRdh = get_vazao_inc(dtInicio=iniciordh, dtFinal=fimrdh)

	vazNatRdh[119] = {dt: (vaz-0.185)/0.8103 for dt, vaz in vazNatRdh[118].items()}
	vazIncRdh[160] = {dt: (vaz*0.073) for dt, vaz in vazNatRdh[161].items()}

	print(">>Gerando arquivos de saida")

	# postos 273 e 275 são VNI
	subBaciasIncrementais = [160, 239, 242] + [34, 154, 243, 245, 246, 266] + [191, 253, 257, 271, 273, 275] + [89]

	dtInicial = datetime.datetime.combine(primeirodiasemana, datetime.datetime.min.time())
	dtFinal = datetime.datetime.combine(primeirodiasemana+datetime.timedelta(days=nDiasDesdeInicioSemana), datetime.datetime.min.time())
	saidaCpins = wx_manipulaArqCpins.getCpins(os.path.join(dirdados_ent,'PlanilhaUSB.xls'), primeiroDia=dtInicial, ultimoDia=dtFinal)

	vazaoSaida = {}
	for modelo in vazoes:
		vazaoSaida[modelo] = {}
		for bacia in vazoes[modelo]:
			for posto in bacias[bacia]['postos']:
				nomePosto = bacias[bacia]['postos'][posto]
				if posto not in subBaciasIncrementais:
					vazoes[modelo][bacia]['natural'][nomePosto].update(vazNatRdh[posto])
					vazaoSaida[modelo][posto] = vazoes[modelo][bacia]['natural'][nomePosto]
				else:
					vazoes[modelo][bacia]['incremental'][nomePosto].update(vazIncRdh[posto])
					vazaoSaida[modelo][posto] = vazoes[modelo][bacia]['incremental'][nomePosto]

		vazaoSaida[modelo][168] = saidaCpins[168].to_dict()
		vazaoSaida[modelo][169] = saidaCpins[169].to_dict()

	# No caso a rodada utilizar os dados do pdp, sera adicionada ao nome dos arquivos a seguinte flag
	if flag_pdp:
		str_flag_pdp = '.PDP'
	else:
		str_flag_pdp = ''

	for modelo in vazaoSaida:

		if 'PRELIMINAR' in modelo:
			smap_nat_path = os.path.join(dirCV,dataExc_str,'preliminar','smap_nat_{}{}_r{}z.txt'.format(modelo, str_flag_pdp, run))
			smap_nat_semanal_path = os.path.join(dirCV,dataExc_str,'preliminar','smap_nat_semanal_{}{}_r{}z.txt'.format(modelo, str_flag_pdp, run))
		else:
			smap_nat_path = os.path.join(dirCV,dataExc_str,'teste','smap_nat_{}{}_r{}z.txt'.format(modelo, str_flag_pdp, run))
			smap_nat_semanal_path = os.path.join(dirCV,dataExc_str,'teste','smap_nat_semanal_{}{}_r{}z.txt'.format(modelo, str_flag_pdp, run))
			
		smap_nat_file = open(smap_nat_path, 'w+')
		smap_nat_semanal_file = open(smap_nat_semanal_path, 'w+')

		line = "Nposto"
		for i in range(nDiasDesdeInicioSemana//7):
			line = line + " {}".format((primeirodiasemana+datetime.timedelta(days=7*i)).strftime('%d/%m/%Y'))
		line = line + "\n"	
		smap_nat_semanal_file.write(line)	
	
		for posto in vazaoSaida[modelo]:

			line = "{:>3}".format(posto)

			for dt in pd.date_range(dataRef, periods=ndia):
				try:
					line = line + "{:>15.2f}".format(vazaoSaida[modelo][posto][dt])
				except:
					line = line + "{:>15.2f}".format(-999)

			line = line + "\n"
			smap_nat_file.write(line)

			line = "{:>3}   ".format(posto)				
			for i, dt in enumerate(pd.date_range(primeirodiasemana, periods=nDiasDesdeInicioSemana)):
				try:
					vaz = vazaoSaida[modelo][posto][dt]
				except:
					vaz = -999
					mediaSemana =  [-999]

				if i % 7 == 0:
					if i != 0:
						line = line + "{:>11.1f}".format(int(round(sum(mediaSemana)/len(mediaSemana))))
					mediaSemana = [vaz]
				else:
					mediaSemana.append(vaz)
			line = line + "\n"
			smap_nat_semanal_file.write(line)

		smap_nat_file.close()
		smap_nat_semanal_file.close()

		print(os.path.abspath(smap_nat_path))
		print(os.path.abspath(smap_nat_semanal_path))

		for bacia in bacias:
			dst = os.path.join(path,bacia,'Arq_Entrada')
			arquivos_dat = glob.glob(os.path.join(dst, f"{modelo}*.DAT"))
			for arquivo in arquivos_dat:
				os.remove(arquivo)
				

	quit()


def printHelper():
	hoje = datetime.datetime.now()
	ultimoSabado = hoje
	while ultimoSabado.weekday() != 5:
		ultimoSabado -= datetime.timedelta(days=1)

	print("python {} data {} modelos ['PCONJUNTO'] run 00 flag_pdp True".format(sys.argv[0],datetime.datetime.now().strftime('%d/%m/%Y')))
	print("python {} data {} modelos ['PCONJUNTO0p5'] run 00 flag_pdp True".format(sys.argv[0],datetime.datetime.now().strftime('%d/%m/%Y')))
	print("python {} data {} modelos ['PMEDIA','PCONJUNTO'] run 00 flag_pdp True".format(sys.argv[0],datetime.datetime.now().strftime('%d/%m/%Y')))
	print("python {} data {} modelos ['GERADOR-CHUVA.estudo'] run 00 flag_pdp True ndia 60".format(sys.argv[0],datetime.datetime.now().strftime('%d/%m/%Y')))
	quit()


def runWithParams():

	if len(sys.argv) > 1:
		# passar o print do help pra quando nao entrar com nenhum parametro
		if 'help' in sys.argv or '--h' in sys.argv or '-help' in sys.argv:
			printHelper()
			quit()

		data = ''
		ndia = ''
		run = ''
		flag_pdp = False

		for i in range(1, len(sys.argv[1:])):
			argumento = sys.argv[i].lower()

			if argumento == 'data':
				try:
					data = sys.argv[i+1] # "%d/%m/%Y"
					datetime.datetime.strptime(data, "%d/%m/%Y")
				except:
					print('Erro ao tentar converter '+data+' em data!')
					print( 'Utilze o padrão dd/mm/YYYY')
					quit()

			elif argumento == 'modelos':
				try:
					modelos = eval(sys.argv[i+1])
				except:
					print("Modelos não reconhecidos, tente passar os modelos como um array sem espaço entre eles:")
					print("Ex: python " + sys.argv[0] + "modelos ['PRECMEDIA_ETA40.GEFS','GEFS','GFS']")
					quit()

			elif argumento in 'ndia':
				try:
					ndia = int(sys.argv[i+1])
				except:
					print("Erro ao tentar pegar ndia, valor encontrado:" + sys.argv[i+1])
					quit()

			elif argumento =='run':
				try:
					run = sys.argv[i+1]
				except:
					print("Erro ao tentar pegar run, valor encontrado:" + sys.argv[i+1])
					quit()

			elif argumento == 'flag_pdp':
				try:
					flag_pdp = sys.argv[i+1]
				except:
					print("Erro ao tentar pegar flag_pdp, valor encontrado:" + sys.argv[i+1])
					quit()
			
		smap_paralelo(data = data, modelos = modelos, run=run, ndia=ndia, flag_pdp=flag_pdp)

	else:
		printHelper()

if __name__ == '__main__':
	# from wx_dbLib import WxDataB
	# from directories_management import pwd, formatPath, delete_file, create_file_copy, create_folder
	
	runWithParams()

	# Rodar smap automatico
	# smap_paralelo(data = '17-03-2020', modelos = ['PRECMEDIA_ETA40.GEFS', 'GEFS', 'GFS'])
	# smap_paralelo(modelos = ['PRECMEDIA_ETA40.GEFS', 'GEFS', 'GFS'])

	# Roda smap com uma data especifica 
	# smap_paralelo(data = '26-02-2020')
