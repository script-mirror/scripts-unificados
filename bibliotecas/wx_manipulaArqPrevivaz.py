import os
import pdb
import glob
import shutil
import datetime
import sys
import pandas as pd
import numpy as np


sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.bibliotecas import wx_dbLib,wx_opweek


def getListaPostosPrevivaz(arquivosPrev):
	arquivosEntPrev = os.path.join(arquivosPrev, 'Arq_Entrada', '*.inp')
	postosPrevivaz = [int(os.path.basename(f).split(".")[0]) for f in glob.glob(arquivosEntPrev)]
	postosPrevivaz.sort()
	return postosPrevivaz


def leituraStrs(pathFiles):
	padraoNome = os.path.join(pathFiles, 'Arq_Entrada', '*_str.DAT')
	arquivosStr = [f for f in glob.glob(padraoNome)]

	compiladoStr = {}
	for arq in arquivosStr:
		posto = int(os.path.basename(arq).replace('_str.DAT','').replace('_str.dat',''))

		compiladoStr[posto] = {}
		f = open(arq, 'r', encoding="ISO-8859-1")		
		for i, linha in enumerate(f.readlines()):
			
			if i < 2:
				continue

			linha = linha.strip().split()
			if len(linha) == 10:
				anoCorrente = int(linha[-1])
				if anoCorrente not in compiladoStr[posto]:
					compiladoStr[posto][anoCorrente] = []
				compiladoStr[posto][anoCorrente] += linha[:-1]
			else:
				compiladoStr[posto][anoCorrente] += linha

	return compiladoStr


def atualizarStrs(compiladoStr, mediasSemanais):
	for posto in compiladoStr:
		for sem in mediasSemanais.loc[posto].dropna().index:
			semEletrica = wx_opweek.ElecData(sem.to_pydatetime().date())
			ano = semEletrica.anoReferente
			numSemana = semEletrica.numSemanas-1
			# if compiladoStr[posto][ano][numSemana] != '0.':
				# continue
			maxValue = max(1,int(round(mediasSemanais.loc[posto][sem])))
			if ano not in compiladoStr[posto]:
				compiladoStr[posto][ano] = []
			if numSemana < len(compiladoStr[posto][ano]):
				compiladoStr[posto][ano][numSemana] = '{}.'.format(maxValue)
			else:
				compiladoStr[posto][ano].append('{}.'.format(maxValue))

	return compiladoStr


def gerarArqEntradaPrevivaz(path, compiladoStr, dataRevInteresse, postosSmap, numeroSemanasPrevisaoSmap=2):

	for posto in compiladoStr:

		dir_base = os.path.join(path, "Caso{}".format(posto))
		if os.path.exists(dir_base):
			shutil.rmtree(dir_base)
		os.makedirs(dir_base)

		# CASO.DAT
		f = open(os.path.join(dir_base, "CASO.DAT"), 'w')
		f.write(str(posto) + ".inp\n")
		f.close()

		# ENCAD.DAT
		f = open(os.path.join(dir_base, "ENCAD.DAT"), 'w')
		f.write("ALGHAO234PGJAGAENCAD")
		f.close()

		# INP
		f = open(os.path.join(path, "Arq_Entrada", "{}.inp".format(posto)), 'r')
		arquivo_inp = f.readlines()
		f.close()


		inicioPrevisao = wx_opweek.ElecData(dataRevInteresse)

		if posto in postosSmap:
			inicioPrevisao = wx_opweek.ElecData(dataRevInteresse + datetime.timedelta(days=numeroSemanasPrevisaoSmap*7)) # (GT-SMAP)

		# Deletar essa parte depois q o GT entrar em operação
		if posto in [168,169]:
			inicioPrevisao = wx_opweek.ElecData(dataRevInteresse + datetime.timedelta(days=2*7))

		anoRef = inicioPrevisao.anoReferente
		semRef = inicioPrevisao.numSemanas

		f = open(os.path.join(dir_base, "{}.inp".format(posto)), 'w')
		for i, linha in enumerate(arquivo_inp):
			if i == 8:
				f.write('{}\n'.format(semRef))
			elif i == 9:
				f.write('{0:>2}\n'.format(anoRef))
			else:
				f.write(linha)
		f.close()

		# STR
		f = open(os.path.join(path, "Arq_Entrada", "{}_str.DAT".format(posto)), 'r')
		ultimoStr = f.readlines()
		f.close()

		f = open(os.path.join(dir_base, "{}_str.DAT".format(posto)), 'w')
		f.write(ultimoStr[0]) # numero e nome do posto
		f.write('{:>5}{:>5}{:>9}\n'.format(min(compiladoStr[posto]),anoRef,ultimoStr[1].split()[-1])) # ano inicial e final
		for ano in compiladoStr[posto]:
			line = ''
			for i, sem in enumerate(compiladoStr[posto][ano]):
				line += '{:>8}'.format(compiladoStr[posto][ano][i])
				# if i%9==0 and i!=0:
				if (i+1)%9==0:
					line += '{:>8}\n'.format(ano)
					f.write(line)
					line = ''
			
			if len(compiladoStr[posto][ano]) < 45:
				for i in range(len(compiladoStr[posto][ano])%9,9):
					line += '{:>8}'.format('0.')
					if (i+1)%9==0:
						line += '{:>8}\n'.format(ano)
						f.write(line+'\n')
			else:
				f.write(line+'\n')
		f.close()

		# .LIM
		shutil.copy(os.path.join(path, "Arq_Entrada", "{}.LIM".format(posto)), os.path.join(dir_base, "{}.LIM".format(posto)))


def criarBatFiles(path, previvazPath, postos):

	num_processadores = int(os.popen('echo %NUMBER_OF_PROCESSORS%').read().split('\n')[0])-1
	lista_execucao=[[] for i in range(num_processadores)]

	for i, posto in enumerate(postos):
		lista_execucao[i%num_processadores].append(posto)

	for i, lista in enumerate(lista_execucao):
		Executa_bat = open(os.path.join(path, "Executa_{}.bat".format(str(i+1))), "w" )
		Executa_bat.write("set atual=%cd%\n")
		Executa_bat.write("for %%G in (" )
		for posto in lista:
			if posto != lista[-1]:
				Executa_bat.write(" {},".format(posto))
			else:
				Executa_bat.write(" {}".format(posto))

		Executa_bat.write(' ) do (cd %atual%\\caso%%G) && (call "{}")\n'.format(previvazPath))
		Executa_bat.write("cd %atual%\n")
		Executa_bat.write("exit\n")
		Executa_bat.close()


		roda_previvaz_bat = open(os.path.join(path, "roda_previvaz.bat"), "w")
		roda_previvaz_bat.write("@echo off\n")
		roda_previvaz_bat.write("(\n")

	for j in range(1,num_processadores+1):
		roda_previvaz_bat.write('\t start cmd /k "cd "{}" && Executa_{}.bat"\n'.format(path, j))
	roda_previvaz_bat.write(") | set /P \"=\" \n")
	roda_previvaz_bat.write("del Executa_*\n")
	roda_previvaz_bat.close()

def executarBatFiles(path):
	batFile = os.path.join(path, "roda_previvaz.bat")
	os.system(batFile)
	os.remove(batFile)


def carregaFutFiles(path):

	padraoNome = os.path.join(path, "Caso*")
	pastasCaso = [f for f in glob.glob(padraoNome)]

	futCompilado = pd.DataFrame()

	for pathCaso in pastasCaso:
		posto = int(os.path.basename(pathCaso).replace('Caso', ''))

		path_arquivo_fut = os.path.join(pathCaso, "{}_fut.DAT".format(posto))
		f = open(path_arquivo_fut, 'r')
		arq_fut = f.readlines()

		try:
			anoRef = int(arq_fut[-3].split()[1])
			semanaInic = int(arq_fut[-3].split()[3])
			dtInicial = wx_opweek.getLastSaturday(datetime.date(anoRef,1,1)) + datetime.timedelta(days=(semanaInic-1)*7)
		except Exception as e:
			raise Exception('Problema na leitura do .fut do posto %s\n%s' %(posto, path_arquivo_fut))
			quit()

		for i in range(6):
			futCompilado.loc[posto, dtInicial+datetime.timedelta(days=i*7)] = float(arq_fut[-3].split()[4+i])

	return futCompilado


def calcularRegressao(vazoes, datasInteresse=[]):

	db_clime = wx_dbLib.DadosClime()
	respostaDb = db_clime.getCoeficienteRegres()
	dic_coefic = {}
	for linha in respostaDb:
		if linha[0] not in dic_coefic:
			dic_coefic[linha[0]] = {'PostoBase':linha[1], 'coefic':{}}
		mes = linha[2]
		dic_coefic[linha[0]]['coefic'][mes] = (linha[3],linha[4])

	for posto in dic_coefic:

		postoBase = dic_coefic[posto]['PostoBase']
		# Caso nao for necessario fazer o calculo ou a nao exista dados do posto base
		if postoBase not in vazoes.index:
			continue

		if posto not in vazoes.index:
			vazoes = vazoes.append(pd.Series(name=posto))

		if datasInteresse == []:
			datas_postoBase = vazoes.loc[posto][vazoes.loc[posto].isna()].index
		else:
			# Forca a substiruir os valores dessa datas
			datas_postoBase = datasInteresse

		for data in datas_postoBase:

			mes_semana = data.month
			A0,A1 = dic_coefic[posto]['coefic'][mes_semana]

			vaz = A0 + A1 * vazoes.loc[postoBase, data]
			if vaz <=0:
				vazoes.loc[posto, data] = 1
				print('Valor de vazao para o posto {} regredido de {} na data {} com valor negativo'.format(posto,postoBase,data.strftime('%d/%m/%Y')))
			else:
				vazoes.loc[posto, data] = A0 + A1 * vazoes.loc[postoBase, data]

	return vazoes


def formulasPrevisao(vazao, datas=[]):
	relacoes_postos = {}
	relacoes_postos[104] = {117: 1, 118: 1}
	relacoes_postos[244] = {34: 1, 243: 1}
	relacoes_postos[116] = {119: 1, 301: -1}
	relacoes_postos[166] = {266: 1, 61: -1, 244: -1}
	relacoes_postos[109] = {118: 1}
	relacoes_postos[172] = {169: 1}
	relacoes_postos[173] = {169: 1}
	relacoes_postos[175] = {173: 1}
	relacoes_postos[176] = {173: 1}
	relacoes_postos[178] = {169: 1}
	relacoes_postos[252] = {259: 1}
	relacoes_postos[301] = {118: 1}
	relacoes_postos[320] = {119: 1}
	relacoes_postos[230] = {229: 1}
	relacoes_postos[228] = {0: 0}
	relacoes_postos[171] = {0: 0}

	ordem_relacoes = [104, 109, 116, 171, 172, 173, 175, 176, 178, 244, 252, 301, 320, 166, 228, 230]

	for posto in ordem_relacoes:

		if posto not in vazao.index:
			vazao = vazao.append(pd.Series(name=posto))

		if datas == []:
			dts = vazao.loc[posto].isna()
			datasLoop = vazao.loc[posto, dts].index
		else:
			datasLoop = datas
		
		for data in datasLoop:

			for i, postoBase in enumerate(relacoes_postos[posto]):

				if postoBase not in vazao.index:
					print('{}: {}'.format(posto,postoBase))
					continue

				if postoBase == 0:
					vazao.loc[posto, data] = 0
				elif pd.isna(vazao.loc[posto, data]):
					vazao.loc[posto, data] = relacoes_postos[posto][postoBase] * vazao.loc[postoBase,data]
				else:
					vazao.loc[posto, data] = vazao.loc[posto, data] + relacoes_postos[posto][postoBase] * vazao.loc[postoBase,data]
	return vazao
