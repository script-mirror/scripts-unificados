# -*- coding: utf-8 -*-
import os
import sys
import pdb
import glob
import shutil
import codecs
import datetime

import numpy as np
import pandas as pd

try:
	import wx_smap
	import wx_dbLib
	import wx_relato
	import wx_opweek
except:
	pass


def getInfoBlocos():
	blocos = {}

	blocos['DEFANT'] = {'campos':[
						'Mont',
						'Jus',
						'TpJ',
						'di',
						'hi',
						'm',
						'df',
						'hf',
						'm',
						'defluencia',
				],
				'regex':'(.{2})       (.{3})  (.{3})  (.{1})    (.{2}) (.{2}) (.{1}) (.{2}) (.{2}) (.{1})     (.{10})(.*)',
				'formatacao':'{:>2}       {:>3}  {:>3}  {:>1}    {:>2} {:>2} {:>1} {:>2} {:>2} {:>1}     {:>10}'}

	return blocos


def getCabecalhos(mnemonico):
	cabecalho = []

	if mnemonico == 'DADVAZ':

		cabecalho.append('NUMERO DE USINAS')
		cabecalho.append('XXX')
		cabecalho.append('162')
		cabecalho.append('NUMERO DAS USINAS NO CADASTRO')
		cabecalho.append('1   2   3   4   5   6   7   8   9   10  11  12  13  14  15  16  17  18  19  20  21  22  23  24  25  26  27  28  29  30  31  32  33  34  35  36  37  38  39  40  41  42  43  44  45  46  47  48  49  50  51  52  53  54  55  56  57  58  59  60  61  62  63  64  65  66  67  68  69  70  71  72  73  74  75  76  77  78  79  80  81  82  83  84  85  86  87  88  89  90  91  92  93  94  95  96  97  98  99  100 101 102 103 104 105 106 107 108 109 110 111 112 113 114 115 116 117 118 119 120 121 122 123 124 125 126 127 128 129 130 131 132 133 134 135 136 137 138 139 140 141 142 143 144 145 146 147 148 149 150 151 152 153 154 155 156 157 158 159 160 161 162 ')
		cabecalho.append('XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX ')
		cabecalho.append('1   2   4   24  27  28  25  33  156 134 10  143 162 148 217 193 141 14  18  37  40  42  15  16  38  39  62  63  52  51  47  49  61  50  45  43  34  153 46  120 121 6   7   8   12  17  31  30  251 278 123 129 20  287 257 122 124 130 131 133 125 180 181 182 183 184 107 108 109 117 118 119 32  11  135 139 262 89  144 66  110 111 112 113 114 74  76  82  83  115 73  71  72  57  77  78  93  91  92  252 253 267 281 9   26  86  90  97  98  99  103 169 172 173 174 175 178 190 189 275 272 279 280 204 127 126 283 261 48  249 241 304 305 196 195 95  203 154 310 29  94  215 155 311 312 315 21  290 101 276 102 277 285 286 284 227 228 229 192 230 288 314 ')
		cabecalho.append('Hr  Dd  Mm  Ano')
		cabecalho.append('XX  XX  XX  XXXX')
		cabecalho.append('{}  {}  {}  {}')
		cabecalho.append('Dia inic(1-SAB...7-SEX); sem da FCF; n. semanas; pre-interesse')
		cabecalho.append('X X X X')
		cabecalho.append('{} {} {} {}')
		cabecalho.append('VAZOES DIARIAS PARA CADA USINA (m3/s)')
		cabecalho.append('NUM     NOME      itp   DI HI M DF HF M      VAZAO')
		cabecalho.append('XXX XXXXXXXXXXXX   X    XXxXXxXxXXxXXxX     XXXXXXXXX')

	return cabecalho

def gravarArquivo(pathSaida, valores):
	fileOut = codecs.open(pathSaida, 'w+', 'utf-8')

	for linha in valores:
		fileOut.write('{}\n'.format(linha.strip()))
	fileOut.close()

def gerarDadvaz(data, pathArquivos):

	if data.date() < datetime.datetime.now().date() + datetime.timedelta(days=2):
		pathFile = os.path.abspath('/WX2TB/Documentos/fontes/PMO/arquivos/diario/{}/dessem'.format((data+datetime.timedelta(1)).strftime('%Y%m%d')))
		fileName = 'dadvaz_{}_para_{}.DAT'.format((data-datetime.timedelta(1)).strftime('%d_%m_%Y'), (data+datetime.timedelta(1)).strftime('%d_%m_%Y'))
		dadvazFilePath = os.path.join(pathFile, fileName)
		pathDestino = os.path.join(pathArquivos, '{}'.format(data.strftime('%Y%m%d')), 'saida', 'dadvaz.dat')
		shutil.copyfile(dadvazFilePath, pathDestino)
		print('dadvaz.dat: {}'.format(pathDestino))
		return pathDestino

	bacias={'Grande':{}, 'Paranapanema':{}, 'Paranaiba':{}, 'Iguacu':{}, 'Tocantins':{}, 'Saofrancisco':{}, 'Uruguai':{}, 'Tiete':{}, 'Parana':{}, 'OSUL':{}, 'Madeira':{}}
	bacias['Grande']['trechos'] = ['Camargos', 'FUNIL MG', 'PARAGUACU', 'FURNAS', 'PBUENOS', 'CAPESCURO', 'PCOLOMBIA', 'PASSAGEM', 'EDACUNHA', 'MARIMBONDO', 'AVERMELHA']
	bacias['Paranapanema']['trechos'] = ['Jurumirim', 'Chavantes', 'CanoasI', 'Maua', 'Capivara', 'Rosana']
	bacias['Paranaiba']['trechos'] = ['SDOFACAO', 'EMBORCACAO', 'NOVAPONTE', 'CORUMBAIV', 'CORUMBA1', 'ITUMBIARA', 'RVERDE', 'SSIMAO2', 'SaltoVerdi', 'Espora', 'FozClaro']
	bacias['Iguacu']['trechos'] =['UVitoria', 'FOA', 'StaClara', 'JordSeg', 'SCaxias']
	bacias['Tocantins']['trechos'] = ['SMesa', 'Bandeirant', 'C.Araguaia', 'Porto Real', 'Estreito', 'Lajeado', 'Tucurui']
	bacias['Saofrancisco']['trechos'] = ['RB-SMAP', 'TM-SMAP']
	bacias['Uruguai']['trechos'] = ['BG', 'CN', 'Machadinho', 'Ita', 'Monjolinho', 'FozChapeco', 'QQueixo', 'SJoao']
	bacias['Tiete']['trechos'] = ['ESouza', 'BBonita', 'Ibitinga', 'NAvanhanda']
	bacias['Parana']['trechos'] = ['FZB', 'ILHAEQUIV', 'JUPIA', 'PPRI', 'SDO', 'BALSA', 'FLOR+ESTRA', 'IVINHEMA', 'PTAQUARA', 'ITAIPU']
	bacias['OSUL']['trechos'] = ['Ernestina', 'PassoReal', 'DFRANC', 'CALVES', '14JULHO', 'GPSouza', 'SaltoPilao']
	bacias['Madeira']['trechos'] = ['Guaj-mirim', 'Jirau2', 'P_da_Beira', 'S.Antonio','Guapore','Samuel','RondonII','Dardanelos']

	# datas ultima e proxima rev
	dataEletrica = wx_opweek.ElecData(data.date())

	ultima_rv_date = dataEletrica.inicioSemana
	prox_rv_date = ultima_rv_date + datetime.timedelta(days=7)

	dataReferente = data + datetime.timedelta(days=2)


	pathArqEntrada = os.path.join(pathArquivos, data.strftime('%Y%m%d'), 'entrada')
	if not os.path.exists(pathArqEntrada):
		os.makedirs(pathArqEntrada)

	pathArqSaida = os.path.join(pathArquivos, data.strftime('%Y%m%d'), 'saida')
	if not os.path.exists(pathArqSaida):
		os.makedirs(pathArqSaida)

	pathArqEstatico = os.path.join(pathArquivos, 'estaticos')
	pathArqPmo = os.path.join(pathArqEstatico, 'PMO')
	pathResultsSmap = os.path.join(pathArqPmo, '{}{:0>2}_REV{}'.format(dataEletrica.anoRefente, dataEletrica.mesRefente, int(dataEletrica.atualRevisao)), 'Modelos_Chuva_Vazao', 'SMAP')

	bloco_dadavaz = getCabecalhos('DADVAZ')

	if data.weekday() <= 4:
		dsemana = dataReferente.weekday() + 3
	else:
		dsemana = dataReferente.weekday() - 4

	semanaFcf = 1
	numSemanas = 1
	flagPeriodoSimulacao = 1

	bloco_dadavaz[9] = bloco_dadavaz[9].format(dataReferente.strftime('%H'), dataReferente.strftime('%d'), dataReferente.strftime('%m'), dataReferente.strftime('%Y'))
	bloco_dadavaz[12] = bloco_dadavaz[12].format(dsemana, semanaFcf, numSemanas, flagPeriodoSimulacao)

	relatoPaterName = os.path.join(pathArqEntrada, 'relato.rv*')
	arq_relato = glob.glob(relatoPaterName)
	if len(arq_relato) == 0:
		print('Inserir o arquivo RELATO.RV* no diretorio: ')
		print(pathArqEntrada)
		quit()

	relatoPath = os.path.abspath(arq_relato[-1])

	infoBlocosRelato = wx_relato.getInfoBlocos()
	relatoFile = wx_relato.leituraArquivo(relatoPath)

	indiceSemanasRelato = [1,2]
	balancoHidraulico = {}
	for semana in indiceSemanasRelato:
		balancoHidraulico = wx_relato.extrairInfoBloco(relatoFile['balancoHidraulico'][semana], infoBlocosRelato['balancoHidraulico']['regex'])
		df_balancoI = pd.DataFrame(balancoHidraulico, columns=infoBlocosRelato['balancoHidraulico']['campos'])
		df_balancoI.drop(['desprezar1', 'desprezar2'], axis=1, inplace=True)
		balancoHidraulico[semana] = df_balancoI


	# lendo informacoes base para escrita dadvaz
	pathArqInfoPostos = os.path.join(pathArqEstatico, 'info_postos_dadvaz.txt')
	df_infosPostos = pd.read_csv(pathArqInfoPostos, delimiter=";")

	resultadoSmap = wx_smap.smap_bacia_novo(pathResultsSmap)
	vazoes = wx_smap.vazIncremental(pathResultsSmap+'/', resultadoSmap)
	# leituraSaidaSmap(pathResultsSmap)


	pdb.set_trace()

def gerarDadvazDef(data, pathArquivos):
	dataRodada = data
	data = data + datetime.timedelta(days=1)

	pathArqEntrada = os.path.join(pathArquivos, data.strftime('%Y%m%d'), 'entrada')

	pathArqSaida = os.path.join(pathArquivos, dataRodada.strftime('%Y%m%d'), 'definitiva')
	if not os.path.exists(pathArqSaida):
		os.makedirs(pathArqSaida)

	src = os.path.join(pathArqEntrada, 'ons_entrada_saida', 'dadvaz.dat')
	dst = os.path.join(pathArqSaida,'dadvaz.dat')

	shutil.copy(src, dst)
	print('dadvaz.dat: {}'.format(dst))


if __name__ == '__main__':

	diretorioRaiz = os.path.abspath('../../../')
	diretorioApps = os.path.abspath('../../')
	sys.path.insert(1, os.path.join(diretorioApps,'smap'))
	sys.path.insert(1, os.path.join(diretorioRaiz,'bibliotecas'))

	import wx_smap
	import wx_relato
	import wx_opweek
	import wx_dbLib

	data = datetime.datetime.now()
	data = datetime.datetime(2021,1,18)
	pathArquivos = os.path.abspath(r'C:\Users\thiag\Documents\git\wx_alpha_\apps\dessem\arquivos')

	gerarDadvaz(data, pathArquivos)
