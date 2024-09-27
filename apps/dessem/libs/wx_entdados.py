# -*- coding: utf-8 -*-
import os
import re
import sys
import pdb
import codecs
import datetime
import pandas as pd


sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.bibliotecas import wx_opweek, wx_dbLib
from PMO.scripts_unificados.apps.dessem.libs import wx_pdoSist


def getNomesDiferentesUsinas():
	nomesDiferentes = {'FUNIL-MG':'FUNIL-GRAND',
						'M. MORAES':'M. DE MORAE',
						'VOLTA GRANDE':'VOLTA GRAND',
						'A. S. OLIVEIRA':'A.S.OLIVEIR',
						'SERRA DO FACAO':'SERRA FACAO',
						'C. BRANCO 1':'CAPIM BRANC',
						'C. BRANCO 2':'CAPIM BRANC',
						'CORUMBA 4':'CORUMBA IV',
						'CORUMBA':'CORUMBA I',
						'C. DOURADA':'CACH.DOURAD',
						'ILHA SOLTEIRA':'I. SOLTEIRA',
						'B. BONITA':'BARRA BONIT',
						'BARIRI':'A.S. LIMA',
						'N. AVANHANDAVA':'NAVANHANDAV',
						'T. IRMAOS':'TRES IRMAOS',
						'P. PRIMAVERA':'P. PRIMAVER',
						'JURUMIRIM':'A.A. LAYDNE',
						'L. N. GARCEZ':'L.N. GARCEZ',
						'CANOAS 2':'CANOAS II',
						'CANOAS 1':'CANOAS I',
						'STA. CLARA PR':'STA CLARA P',
						'F. AREIA':'G.B. MUNHOZ',
						'S. SANTIAGO':'SLT.SANTIAG',
						'S. OSORIO':'SALTO OSORI',
						'S. CAXIAS':'SALTO CAXIA',
						'BAIXO IGUACU':'BAIXO IGUAC',
						'B. GRANDE':'BARRA GRAND',
						'C. NOVOS':'CAMPOS NOVO',
						'P. FUNDO':'PASSO FUNDO',
						'Q. QUEIXO':'QUEBRA QUEI',
						'C. ALVES':'CASTRO ALVE',
						'M. CLARO':'MONTE CLARO',
						'S. JOSE':'SAO JOSE',
						'P. S. JOAO':'PASSO S JOA',
						'F. CHAPECO':'FOZ CHAPECO',
						'P. REAL':'PASSO REAL',
						'D. FRANCISCA':'D. FRANCISC',
						'CAP. CACHOEIRA':'G.P. SOUZA',
						'SANTA BRANCA':'SANTA BRANC',
						'SANTA CECILIA':'STA CECILIA',
						'SALTO GRANDE':'SALTO GRAND',
						'PORTO ESTRELA':'P. ESTRELA',
						'SAO DOMINGOS':'SAO DOMINGO',
						'RETIRO BAIXO':'RETIRO BAIX',
						'P. AFONSO 1,2,3':'P.AFONSO 12',
						'P. AFONSO 4':'P.AFONSO 4',
						'XINGA':'XINGO',
						'PEDRA DO CAVALO':'P. CAVALO',
						'BOA ESPERANAA':'B. ESPERANC',
						'GUILMAN AMORIM':'GUILMAN-AMO',
						'CORUMBA 3':'CORUMBA III',
						'CACHOEIRA CALDEIRAO':'CACH.CALDEI',
						'SALTO VERDINHO':'SLT VERDINH',
						'S. DA MESA':'SERRA MESA',
						'SAO SALVADOR':'SAO SALVADO',
						'PEIXE ANGICAL':'PEIXE ANGIC',
						'CURUA UNA':'CURUA-UNA',
						'COARACY NUNES':'COARACY NUN',
						'PONTE DE PEDRA':'PONTE PEDRA',
						'STA CLARA - MG':'STA CLARA M',
						'FERREIRA GOMES':'FERREIRA GO',
						'STO ANTONIO DO JARI':'STO ANT JAR',
						'SANTO ANTONIO':'STO ANTONIO',
						'B. COQUEIROS':'B. COQUEIRO',
						'FOZ RIO CLARO':'FOZ R. CLAR'}

	return nomesDiferentes

def getInfoBlocos():
	blocos = {}

	# ENTDADOS

	blocos['TM'] = {'campos':[
						'mnemonico',
						'dd',
						'hr',
						'mh',
						'durac',
						'rede',
						'Patamar',
				],
				'regex':'(.{2})  (.{2})   (.{2})   (.{1})    (.{5})     (.{1})   (.{6})(.*)',
				'formatacao':'{:>2}  {:>2}   {:>2}   {:>1}    {:>5}     {:>1}   {:>6}'}

	blocos['DP'] = {'campos':[
						'mnemonico',
						'ss',
						'di',
						'hi',
						'm',
						'df',
						'hf',
						'm',
						'Demanda',
				],
				'regex':'(.{2})  (.{2})  (.{2}) (.{2}) (.{1}) (.{2}) (.{2}) (.{1}) (.{10})(.*)',
				'formatacao':'{:>2}  {:>2}  {:0>2} {:0>2} {:>1} {:>2} {:>2} {:>1} {:>10}'}
	blocos['UT'] = {	'campos':[
							'mnemonico', 
							'numeroUsina', 
							'nomeUsina', 
							'subssistema', 
							'tipoRestricao', 
							'diaInicial', 
							'horaInicial', 
							'meiaHoraInicial', 
							'diaFinal', 
							'horaFinal', 
							'meiaHoraFinal', 
							'unidadeRestricao', 
							'restricaoMinima', 
							'restricaoMaxima', 
							'geracaoMeiaHoraAnterior'],
						'regex':'(.{2})  (.{3})  (.{12}) (.{2}) (.{1}) (.{2}) (.{2}) (.{1}) (.{2}) (.{2}) (.{1})    (.{1})(.{10})(.{10})(.{0,10})(.*)',
						'formatacao':'{:>2}  {:>3}  {:>12} {:>2} {:>1} {:>2} {:>2} {:>1} {:>2} {:>2} {:>1}    {:>1}{:>10}{:>10}{:>10}'}

	blocos['&UT'] = {	'campos':[
							'mnemonico', 
							'numeroUsina', 
							'nomeUsina', 
							'subssistema', 
							'tipoRestricao', 
							'diaInicial', 
							'horaInicial', 
							'meiaHoraInicial', 
							'diaFinal', 
							'horaFinal', 
							'meiaHoraFinal', 
							'unidadeRestricao', 
							'restricaoMinima', 
							'restricaoMaxima', 
							'geracaoMeiaHoraAnterior'],
						'regex':'(.{3})  (.{3})  (.{12}) (.{2}) (.{1}) (.{2}) (.{2}) (.{1}) (.{2}) (.{2}) (.{1})    (.{1})(.{10})(.{10})(.{10})(.*)',
						'formatacao':'{:>3}  {:>3}  {:>12} {:>2} {:>1} {:>2} {:>2} {:>1} {:>2} {:>2} {:>1}    {:>1}{:>10}{:>10}{:>10}'}


	blocos['RD'] = {	'campos':[
							'mnemonico', 
							'flagFolgaRestricLimeFluxo', 
							'maxCircuitosVioladosPeriodo',
							'flagRede',
							'flagLimFluxosTransfElev',
							'flagRestriLimFluxo',
							'flagPerdaRede',
						],
						'regex':'(.{2})  (.{1})    (.{3})  (.{1}) (.{1}) (.{1}) (.{1})(.*)',
						'formatacao':'{:>2}  {:>1}    {:>3}  {:>1} {:>1} {:>1} {:>1}'}
	

	blocos['RIVAR'] = {	'campos':[
							'mnemonico', 
							'entidadesCadastroDefinidas', 
							'paraCampo4', 
							'tipo', 
						],
						'regex':'(.{5})  (.{3}) (.{2})  (.{1,2})(.*)',
						'formatacao':'{:>5}  {:>3} {:>2} {:>2}'}
						# 'regex':'(.{5})  (.{3}) (.{3}) (.{1,2})(.{0,12})(.*)',
						# 'formatacao':'{:>5}  {:>3} {:>3} {:>2}  {:>10}'}

	blocos['SIST'] = {	'campos':[
							'mnemonico',
							'idSubsistema',
							'mnemonicoSubsistema',
							'flagFicticio',
							'nomeSubsistema',
						],
						'regex':'(.{4})   (.{2}) (.{2}) (.{2}) (.{4,10})(.*)',
						'formatacao':'{:>4}   {:>2} {:>2} {:>2} {:<10}'}

	blocos['REE'] = {	'campos':[
							'mnemonico',
							'idREE',
							'IdSubsistema',
							'nomeREE',
						],
					'regex':'(.{3})   (.{2}) (.{2}) (.{4,10})(.*)',
					'formatacao':'{:>3}   {:>2} {:>2} {:<10}'}

	blocos['UH'] = {'campos':[
						'mnemonico',
						'ind',
						'nome',
						'ss',
						'vinic',
						'evap',
						'di',
						'hi',
						'm',
						'vmorinic',
				],
				'regex':'(.{2})  (.{3})  (.{12})   (.{2})   (.{10})(.{1}) (.{0,2}).{0,}(.{0,2}).{0,}(.{0,1}).{0,}(.{0,10})(.*)',
				'formatacao':'{:>2}  {:>3}  {:>12}   {:>2}   {:>10}{:>1} {:<2} {:>2} {:>1} {:>10}'}

	blocos['TVIAG'] = {	'campos':[
							'mnemonico',
							'usinaMontante',
							'usinaJusante',
							'tipoElementoJusante',
							'tempoViagem',
							'tipoCurvaTv',
						],
					'regex':'(.{6})(.{3}) (.{3}) (.{1})    (.{3})  (.{1})(.*)',
					'formatacao':'{:>6}{:>3} {:>3} {:>1}    {:>3}  {:>1}'}

	blocos['USIE'] = {	'campos':[
							'mnemonico',
							'usinaElevatoria',
							'idSubsistema',
							'nomeUsinaElevatoria',
							'numeroUsinaMontante',
							'numeroUsinaJusante',
							'vazaoMin',
							'vazaoMax',
							'taxaConsumo',
						],
					'regex':'(.{4}) (.{3}) (.{2})   (.{12})   (.{3})  (.{3})  (.{10})(.{10})(.{10})(.*)',
					'formatacao':'{:>4} {:>3} {:>2}   {:>12}   {:>3}  {:>3}  {:>10}{:>10}{:>10}'}
	
	blocos['DE'] = {	'campos':[
							'mnemonico',
							'idDemandaEsp',
							'diaInicial',
							'horaInicial',
							'meiaHoraInicial',
							'diaFinal',
							'horaFinal',
							'meiaHoraFinal',
							'demanda',
							'justificativa',
							],
						'regex':'(.{2})  (.{3}) (.{2}) (.{2}) (.{1}) (.{2}) (.{2}) (.{1}) (.{10}).{0,1}(.{0,10})(.*)',
						'formatacao':'{:>2}  {:>3} {:>2} {:>2} {:>1} {:>2} {:>2} {:>1} {:>10} {:>10}',
					}
	
	blocos['CD'] = {	'campos':[
							'mnemonico',
							'idSubsistema',
							'seguimentoCurvaDeficit',
							'diaInicial', 
							'horaInicial', 
							'meiaHoraInicial',
							'diaFinal', 
							'horaFinal', 
							'meiaHoraFinal', 
							'custo',
							'profundidade',
						],
					'regex':'(.{2}) (.{2}) (.{2}) (.{2}) (.{2}) (.{1}) (.{2}) (.{2}) (.{1}) (.{10})(.{10})(.*)',
					'formatacao':'{:>2} {:>2} {:>2} {:>2} {:>2} {:>1} {:>2} {:>2} {:>1} {:>10}{:>10}'}


	blocos['PQ'] = {	'campos':[
							'mnemonico',
							'numeroUsina',
							'nomeUsina',
							'idSubsistema',
							'diaInicial',
							'horaInicial',
							'meiaHoraInicial',
							'diaFinal',
							'horaFinal',
							'meiaHoraFinal', 
							'geracao',
						],
					'regex':'(.{2})  (.{3})  (.{10})(.{5})(.{2}) (.{2}) (.{1}) (.{2}) (.{2}) (.{1}) (.{10})(.*)',
					'formatacao':'{:>2}  {:>3}  {:>10}{:>5}{:>2} {:>2} {:>1} {:>2} {:>2} {:>1} {:>10}'}

	blocos['IT'] = {	'campos':[
							'mnemonico',
							'ree',
							'coeficientesPolinomio',
						],
					'regex':'(.{2})  (.{2})  (.{70,75})(.*)',
					'formatacao':'{:>2}  {:>2}  {:<75}'}

	blocos['RI'] = {	'campos':[
							'mnemonico',
							'ind',
							'diaInicial',
							'horaInicial',
							'meiaHoraInicial',
							'diaFinal',
							'horaFinal',
							'meiaHoraFinal',
							'limInferior50Hz',
							'limSuperior50Hz',
							'limInferior60Hz',
							'limSuperior60Hz',
							'cargaANDE',
						],
					'regex':'(.{2})  (.{3}) (.{2}) (.{2}) (.{1}) (.{2}) (.{2}) (.{1})   (.{10})(.{10})(.{10})(.{10})(.{10})(.*)',
					'formatacao':'{:>2}  {:>3} {:>2} {:>2} {:>1} {:>2} {:>2} {:>1}   {:>10}{:>10}{:>10}{:>10}{:>10}'}


	blocos['IA'] = {	'campos':[
							'mnemonico',
							'subsistemaOrigem',
							'subsistemaDestino',
							'diaInicial',
							'horaInicial',
							'meiaHoraInicial',
							'diaFinal',
							'horaFinal',
							'meiaHoraFinal',
							'capacidadeDePara',
							'capacidadeParaDe',
						],
					'regex':'(.{2})  (.{2})   (.{2})  (.{2}) (.{2}) (.{1}) (.{2}) (.{2}) (.{1}) (.{10})(.{10})(.*)',
					'formatacao':'{:>2}  {:>2}   {:>2}  {:>2} {:>2} {:>1} {:>2} {:>2} {:>1} {:>10}{:>10}'}



	blocos['GP'] = {	'campos':[
							'mnemonico',
							'toleranciaConvergencia',
							'toleranciaConvergenciaProbInteriro',
						],
					'regex':'(.{2})  (.{10}) (.{10})(.*)',
					'formatacao':'{:>2}  {:>10} {:>10}'}

	blocos['NI'] = {	'campos':[
							'mnemonico',
							'flagMaxIteracoes',
							'numeroMaximosIteracoes',
						],
					'regex':'(.{2})  (.{1})    (.{3})(.*)',
					'formatacao':'{:>2}  {:>1}    {:>3}'}

	blocos['VE'] = {	'campos':[
							'mnemonico',
							'numeroUsina',
							'diaInicial',
							'horaInicial',
							'meiaHoraInicial',
							'diaFinal',
							'horaFinal',
							'meiaHoraFinal',
							'volumeEsperado',
						],
					'regex':'(.{2})  (.{3}) (.{2}) (.{2}) (.{1}) (.{2}) (.{2}) (.{1}) (.{10})(.*)',
					'formatacao':'{:>2}  {:>3} {:>2} {:>2} {:>1} {:>2} {:>2} {:>1} {:>10}'}

	blocos['CE_CI'] = {	'campos':[
							'mnemonico',
							'num',
							'nome',
							'ss',
							'busf',
							'diaInicial',
							'horaInicial',
							'meiaHoraInicial',
							'diaFinal',
							'horaFinal',
							'meiaHoraFinal',
							'F',
							'linferior',
							'lsuperior',
							'custo',
							'inicial'
						],
					'regex':'(.{2}) (.{3}) (.{10}) (.{5})(.{1}) (.{2}) (.{2}) (.{1}) (.{2}) (.{2}) (.{1}) (.{1}) (.{10})(.{10})(.{10})(.{0,10})(.*)',
					'formatacao':'{:>2} {:>3} {:<10} {:>5}{:>1} {:>2} {:>2} {:>1} {:>2} {:>2} {:>1} {:>1} {:>10}{:>10}{:>10}{:>10}'}

	# ############################################
	blocos['DA'] = {	'campos':[
							'mnemonico',
							'usina',
							'diaInicial',
							'horaInicial',
							'meiaHoraInicial',
							'diaFinal',
							'horaFinal',
							'meiaHoraFinal',
							'retiradaAgua',
					],
				'regex':'(.{2})  (.{3}) (.{2}) (.{2}) (.{1}) (.{2}) (.{2}) (.{1}) (.{10})(.*)',
				'formatacao':'{:>2}  {:>3} {:>2} {:>2} {:>1} {:>2} {:>2} {:>1} {:>10}'}

	blocos['FP'] = {	'campos':[
							'mnemonico',
							'usina',
							'tipo',
							'pontosDiscretizadosQ',
							'pontosDiscretizadosV',
							'flagConcavCurvaFuncaoProd',
							'flagAjustMinimosQuadrad',
							'comprimentoJanela',
							# 'tolerancia',
					],
				'regex':'(.{2}) (.{3}) (.{1})  (.{3})  (.{3})  (.{1})   (.{1})    (.{1,10})(.{0,10})',
				# 'formatacao':'{:>2} {:>3} {:>1}  {:>3}  {:>3}  {:>1}   {:>1}    {:>10}{:>10}'}
				'formatacao':'{:>2} {:>3} {:>1}  {:>3}  {:>3}  {:>1}   {:>1}    {:<10}'}

	blocos['TX'] = {'campos':[
							'mnemonico',
							'taxaJurosAnual'
						],
					'regex':'(.{2})  (.{1,10})(.*)',
					'formatacao':'{:>2}  {:<9}'}

	blocos['EZ'] = {	'campos':[
						'mnemonico',
						'usina',
						'volumeUtil',
					],
				'regex':'(.{2})  (.{3})  (.{5})(.*)',
				'formatacao':'{:>2}  {:>3}  {:>5}'}

	blocos['AG'] = {'campos':[
						'mnemonico',
						'numeroEstagios',
					],
				'regex':'(.{2})  (.{1,3})(.*)',
				'formatacao':'{:>2}  {:<3}'}

	blocos['SECR'] = {'campos':[
						'mnemonico',
						'numero',
						'nome',
						'usina1',
						'fator1',
						'usina2',
						'fator2',
					],
				'regex':'(.{4}) (.{3}) (.{12})   (.{3}) (.{5}) (.{3}) (.{5})(.*)',
				'formatacao':'{:>4} {:>3} {:>12}   {:>3} {:>5} {:>3} {:>5}'}


	blocos['MH'] = {'campos':[
						'mnemonico',
						'usina',
						'indiceGrupo',
						'indiceUnidade',
						'diaInicial',
						'horaInicial',
						'meiaHoraInicial',
						'diaFinal',
						'horaFinal',
						'meiaHoraFinal',
						'flagDisponibilidade'
					],
				'regex':'(.{2})  (.{3})  (.{2}) (.{2})(.{2}) (.{2}) (.{1}) (.{2}) (.{2}) (.{1}) (.{1})(.*)',
				'formatacao':'{:>2}  {:>3}  {:>2} {:>2}{:>2} {:>2} {:>1} {:>2} {:>2} {:>1} {:>1}'}

	blocos['CR'] = {	'campos':[
					'mnemonico',
					'idSecaoRio',
					'nomeSecaoRio',
					'grau',
					'termoIndep',
					'coef1',
					'coef2',
					'coef3',
					'coef4',
					'coef5',
					'coef6',
				],
			'regex':'(.{2})  (.{3})  (.{12})   (.{2}) (.{15}) (.{15}) (.{15}) (.{15}) (.{15}) (.{15})  (.{14})(.*)',
			'formatacao':'{:>2}  {:>3}  {:>12}   {:>2} {:>15} {:>15} {:>15} {:>15} {:>15} {:>15}  {:>14}'}

	blocos['R11'] = {'campos':[
						'mnemonico',
						'diaInicial',
						'horaInicial',
						'meiaHoraInicial',
						'diaFinal',
						'horaFinal',
						'meiaHoraFinal',
						'nivelMeiaHoraAntesEstudo',
						'VariacaoMaxHora',
						'VariacaoMaxDia',
					],
				'regex':'(.{3}) (.{2}) (.{2}) (.{1}) (.{2}) (.{2}) (.{1}) (.{0,10})(.{0,10})(.{0,10})(.*)',
				'formatacao':'{:>3} {:>2} {:>2} {:>1} {:>2} {:>2} {:>1} {:>10}{:>10}{:>10}'}

	blocos['RE'] = {'campos':[
						'mnemonico',
						'ind',
						'di',
						'hi',
						'm',
						'df',
						'hf',
						'm',
				],
				# 'regex':'(.{2})  (.{3})  (.{2}) (.{2}) (.{1}) (.{2}) (.{2}) (.{1})(.*)',
				'regex':'(.{2})..(.{3})..(.{2}).(.{2}).(.{1}).(.{0,2}).{0,1}(.{0,2}).{0,1}(.{0,1})(.*)',
				'formatacao':'{:>2}  {:>3}  {:>2} {:>2} {:>1} {:>2} {:>2} {:>1}'}

	blocos['LU'] = {'campos':[
						'mnemonico',
						'ind',
						'di',
						'hi',
						'm',
						'df',
						'hf',
						'm',
						'Linf',
						'Lsup',
				],
				'regex':'(.{2})  (.{3}) (.{2}) (.{2}) (.{1}) (.{2}) (.{2}) (.{1}) (.{10})(.{0,10})(.*)',
				'formatacao':'{:>2}  {:>3} {:>2} {:>2} {:>1} {:>2} {:>2} {:>1} {:>10}{:>10}'}
	
	blocos['FH'] = {'campos':[
						'mnemonico',
						'ind',
						'di',
						'hi',
						'm',
						'df',
						'hf',
						'm',
						'ush',
						'nmaq',
						'Fator',
				],
				'regex':'(.{2})  (.{3}) (.{2}) (.{2}) (.{1}) (.{2}) (.{2}) (.{1}) (.{3})(.{2})     (.{10})(.*)',
				'formatacao':'{:>2}  {:>3} {:>2} {:>2} {:>1} {:>2} {:>2} {:>1} {:>3}{:>2}     {:>10}'}

	blocos['FT'] = {'campos':[
						'mnemonico',
						'ind',
						'di',
						'hi',
						'm',
						'df',
						'hf',
						'm',
						'ust',
						'Fator',
				],
				'regex':'(.{2})  (.{3}) (.{2}) (.{2}) (.{1}) (.{2}) (.{2}) (.{1}) (.{3})       (.{10})(.*)',
				'formatacao':'{:>2}  {:>3} {:>2} {:>2} {:>1} {:>2} {:>2} {:>1} {:>3}       {:>10}'}

	blocos['FI'] = {'campos':[
						'mnemonico',
						'ind',
						'di',
						'hi',
						'm',
						'df',
						'hf',
						'm',
						'ss1',
						'ss2',
						'Fator',
				],
				'regex':'(.{2})  (.{3}) (.{2}) (.{2}) (.{1}) (.{2}) (.{2}) (.{1}) (.{2})   (.{2})   (.{0,10})(.*)',
				'formatacao':'{:>2}  {:>3} {:>2} {:>2} {:>1} {:>2} {:>2} {:>1} {:>2}   {:>2}   {:>10}'}
	
	blocos['FE'] = {'campos':[
						'mnemonico',
						'ind',
						'di',
						'hi',
						'm',
						'df',
						'hf',
						'm',
						'ust',
						'Fator',
				],
				'regex':'(.{2})  (.{3}) (.{2}) (.{2}) (.{1}) (.{2}) (.{2}) (.{1}) (.{3})       (.{10})(.*)',
				'formatacao':'{:>2}  {:>3} {:>2} {:>2} {:>1} {:>2} {:>2} {:>1} {:>3}       {:>10}'}
	
	blocos['FC'] = {'campos':[
						'mnemonico',
						'ind',
						'di',
						'hi',
						'm',
						'df',
						'hf',
						'm',
						'nde',
						'Fator',
				],
				'regex':'(.{2})  (.{3})   (.{2}) (.{2}) (.{1}) (.{2}) (.{2}) (.{1}) (.{3})     (.{10})(.*)',
				'formatacao':'{:>2}  {:>3}   {:>2} {:>2} {:>1} {:>2} {:>2} {:>1} {:>3}     {:>10}'}

	
	blocos['FR'] = {'campos':[
						'mnemonicoind',
						'ind',
						'di',
						'hi',
						'm',
						'df',
						'hf',
						'm',
						'neo',
						'Fator',
				],
				'regex':'(.{2})  (.{3})   (.{2}) (.{2}) (.{1}) (.{2}) (.{2}) (.{1}) (.{4})    (.{10})(.*)',
				'formatacao':'{:>2}  {:>3}   {:>2} {:>2} {:>1} {:>2} {:>2} {:>1} {:>4}    {:>10}'}

	blocos['MT'] = {'campos':[
						'mnemonico',
						'ute',
						'ug',
						'di',
						'hi',
						'm',
						'df',
						'hf',
						'm',
						'F',
				],
				'regex':'(.{2})  (.{3}) (.{3})  (.{2}) (.{2}) (.{1}) (.{2}) (.{2}) (.{1}) (.{1})(.*)',
				'formatacao':'{:>2}  {:>3} {:>3}  {:0>2} {:>2} {:>1} {:0>2} {:>2} {:>1} {:>1}'}

	return blocos

def getInfoBlocosDadger():
	blocos = {}

	blocos['IA'] = {	'campos':[
							'mnemonico',
							'estagio',
							'subsistemaOrigem',
							'subsistemaDestino',
							'capacidadeDeParaPat1',
							'capacidadeParaDePat1',
							'capacidadeDeParaPat2',
							'capacidadeParaDePat2',
							'capacidadeDeParaPat3',
							'capacidadeParaDePat3',
						],
					# 'regex':'(.{2})   (.{2})   (.{2})   (.{2})   (.{10})(.{10})(.{10})(.{10})(.{10})(.{10})(.*)',
					'regex':'(.{2})   (.{2})  (.{2})   (.{2})   (.{10})(.{10})(.{10})(.{10})(.{10})(.{10})(.*)',
					'formatacao':'{:>2}   {:>2}  {:>2}   {:>2}   {:>10}{:>10}{:>10}{:>10}{:>10}{:>10}'}



	return blocos



def ultimoDiaMes(data):
    proximoMes = data.replace(day=28) + datetime.timedelta(days=4)
    return proximoMes - datetime.timedelta(days=proximoMes.day)

def getFromFile(path):
	file = open(path, 'r')
	return file.readlines()

def leituraArquivo(filePath):

	arquivo = getFromFile(filePath)

	dados = {}
	for iLine in range(len(arquivo)):
		line = arquivo[iLine]

		if line[0] == '&':
			if line[0:3] == '&UT':
				pass
			else:
				continue
		elif line[0] == '\n':
			continue

		mnemonico = line.split()[0]
		if mnemonico in ['CE', 'CI']:
			mnemonico = 'CE_CI'
		
		if mnemonico not in dados:
			dados[mnemonico] = []
		dados[mnemonico].append(line)

	return dados

def extrairInfoBloco(listaLinhas, mnemonico, regex):
	blocos = []
	if mnemonico in listaLinhas:
		for i, linha in enumerate(listaLinhas[mnemonico]):
			infosLinha = re.split(regex, linha)
			blocos.append(infosLinha[1:-2])   # ultimo termo da lista e o que sobra da expressao regex (/n)

	return blocos

def extrairInfoBlocoRestricoes(listaLinhas):
	blocos = []
	infoBlocos = getInfoBlocos()
	for linha in listaLinhas:
		mnemonico = linha.split()[0]
		regex = infoBlocos[mnemonico]['regex']
		infosLinha = re.split(regex, linha)
		blocos.append(infosLinha[1:-2])   # ultimo termo da lista e o que sobra da expressao regex (/n)
	return blocos


def getCabecalhos(mnemonico):

	cabecalho = []
	if mnemonico == 'RD':
		cabecalho.append('&')
		cabecalho.append('& FLAGs PARA INSERIR AS FOLGAS NAS RESTRICOES DE REDE                                                                   ')
		cabecalho.append('&1  2    3    4 5 6 7                                                                                                   ')
		cabecalho.append('&X  X    XXX  X X X X                                                                                                   ')

	if mnemonico == 'RIVAR':
		cabecalho.append('&')
		cabecalho.append('&')

	elif mnemonico == 'TM':
		cabecalho.append('&')
		cabecalho.append('&   DISCRETIZACAO DO ESTUDO')
		cabecalho.append('&   ')
		cabecalho.append('&X  dd   hr   mh   durac    rede Patamar')
		cabecalho.append('&   XX   XX   X    XXXXX     X   XXXXXX')

	elif mnemonico == 'SIST':
		cabecalho.append('&')
		cabecalho.append('&')
		cabecalho.append('&   DEFINICAO DOS SUBSISTEMAS                                                                                           ')
		cabecalho.append('&')
		cabecalho.append('&IST   XX XX XX XXXXXXXXXX ')

	elif mnemonico == 'REE':
		cabecalho.append('&')
		cabecalho.append('&')
		cabecalho.append('&   DEFINICAO DOS RESERVATORIOS EQUIVALENTES - REE')
		cabecalho.append('&')
		cabecalho.append('&REE  XX XX XXXXXXXXXX ')

	elif mnemonico == 'UH':
		cabecalho.append('&')
		cabecalho.append('&')
		cabecalho.append('&   USINAS HIDRAULICAS')
		cabecalho.append('&')
		cabecalho.append('&   ind      nome       ss   Vinic  Evap di hi m     VmorInic                Pdconst')
		cabecalho.append('&X  XXX  XXXXXXXXXXXX   XX   XXXXXXXXXXx XX XX X XXXXXXXXXX               X')
		cabecalho.append('&')

	elif mnemonico == 'TVIAG':
		cabecalho.append('&')
		cabecalho.append('&')
		cabecalho.append('&  TEMPO DE VIAGEM')
		cabecalho.append('&')
		cabecalho.append('&     Mon Jus tp  hora tpTVIAG')
		cabecalho.append('&xxxxxXXX XXX X    XXX  X')

	elif mnemonico == 'UT':
		cabecalho.append('&------------------------------------------------------------------------------------------')
		cabecalho.append('& TERMICAS                             ')
		cabecalho.append('&   No     Nome       ss F di hi midf hf mf   F   Gmin      Gmax   ')
		cabecalho.append('&X  XXX  XXXXXXXXXXXX XX X XX XX X XX XX X    xXXXXXXXXXXxxxxxxxxxx')

	elif mnemonico == 'UT2':
		cabecalho.append('&------------------------------------------------------------------------------------------')
		cabecalho.append('&Restricoes de rampa das UTEs                                                              ')
		cabecalho.append('&   No     Nome       ss F di hi midf hf mf   F   Decr     Acresc    Ginic   ')
		cabecalho.append('&X  XXX  XXXXXXXXXXXX XX X XX XX X XX XX X    xXXXXXXXXXXxxxxxxxxxxXXXXXXXXXX')

	elif mnemonico == 'USIE':
		cabecalho.append('&------------------------------------------------------------------------------------------')
		cabecalho.append('&')
		cabecalho.append('&')
		cabecalho.append('&   USINAS ELEVATORIAS                                                                                                  ')
		cabecalho.append('&')
		cabecalho.append('&     No   ss   Nome           Mont Jus  Qmin      Qmax      Taxa consumo                                               ')
		cabecalho.append('&SIE XXX  XX   XXXXXXXXXXXX   XXX  XXX  XXXXXXXXXXxxxxxxxxxxXXXXXXXXXX ')

	elif mnemonico ==  'DP':
		cabecalho.append('&')
		cabecalho.append('&')
		cabecalho.append('&   CARGA')
		cabecalho.append('&   ')
		cabecalho.append('&   ss  di hi m df hf m Demanda')
		cabecalho.append('&X  XX  XX XX X XX XX X XXXXXXXXXX')

	elif mnemonico ==  'DE':
		cabecalho.append('&   ')
		cabecalho.append('&   ')
		cabecalho.append('&   DEMANDAS/CARGAS ESPECIAIS')
		cabecalho.append('&   ')
		cabecalho.append('&   nde di hi m df hf m    Demanda Justific')
		cabecalho.append('&X  XXX XX XX X XX XX X XXXXXXXXXX XXXXXXXXXX')


	elif mnemonico == 'CD':
		cabecalho.append('&')
		cabecalho.append('&')
		cabecalho.append('&   CUSTO DE DEFICIT')
		cabecalho.append('&')
		cabecalho.append('&  is ic di hi m df hf m Custo     LimSup')
		cabecalho.append('&X XX XX XX XX X XX XX X XXXXXXXXXXxxxxxxxxxx')

	elif mnemonico == 'PQ':
		cabecalho.append('&')
		cabecalho.append('&')
		cabecalho.append('&   GERACOES DAS PEQUENAS USINAS')
		cabecalho.append('&')
		cabecalho.append('&   ind   Nome     ss/b di hi m df hf m  Geracao  ')
		cabecalho.append('&X  XXX  XXXXXXXXXXxxxxxXX XX X XX XX X XXXXXXXXXX')

	elif mnemonico == 'IT':
		cabecalho.append('&')
		cabecalho.append('&   Coeficientes do Canal de Fuga de Itaipu - Regua 11                                                                  ')
		cabecalho.append('&')
		cabecalho.append('&   ss   Coefic. da Regua 11 (1 .. 5)   ')
		cabecalho.append('&   XX   XXXXXXXXXXXXXXXxxxxxxxxxxxxxxxXXXXXXXXXXXXXXXxxxxxxxxxxxxxxxXXXXXXXXXXXXXXX  ')

	elif mnemonico == 'RI':
		cabecalho.append('&')
		cabecalho.append('&')
		cabecalho.append('&   RESTRICAO DE ITAIPU 50HZ E 60HZ E PARCELA DA ANDE                                                                   ')
		cabecalho.append('&')
		cabecalho.append('&   ind di hi m df hf m    Gh50 min  GH50 max  GH60 min  GH60 max    ANDE                                               ')
		cabecalho.append('&   XXX XX XX X XX XX X   XXXXXXXXXXxxxxxxxxxxXXXXXXXXXXxxxxxxxxxxXXXXXXXXXX ')

	elif mnemonico == 'IA':
		cabecalho.append('&')
		cabecalho.append('&')
		cabecalho.append('&   INTERCAMBIOS DE ENERGIA ENTRE SUBSISTEMAS                                                                           ')
		cabecalho.append('&')
		cabecalho.append('&   ss1  ss2 di hi m df hf m ss1->ss2  ss2->ss1                                                                         ')
		cabecalho.append('&X  XX   XX  XX XX X XX XX X XXXXXXXXXXxxxxxxxxxx')

	elif mnemonico == 'GP':
		cabecalho.append('&')
		cabecalho.append('&')
		cabecalho.append('&   GAP PARA CONVERGENCIA                                                                                               ')
		cabecalho.append('&')
		cabecalho.append('&X  XXXXXXXXXX XXXXXXXXXX ')

	elif mnemonico == 'NI':
		cabecalho.append('&')
		cabecalho.append('&')
		cabecalho.append('&  flg   NmaxIter                                                                                                       ')
		cabecalho.append('&')
		cabecalho.append('&X  X    XXX  ')

	elif mnemonico == 'VE':
		cabecalho.append('&')
		cabecalho.append('&  VOLUME DE ESPERA')
		cabecalho.append('&')
		cabecalho.append('&   ind di hi m df hf m  Vol(%)')
		cabecalho.append('&X  XXX XX XX X XX XX X XXXXXXXXXX')

	elif mnemonico == 'CE_CI':
		cabecalho.append('&')
		cabecalho.append('&')
		cabecalho.append('&BLOCO A SER ADICIONADO NO ARQUIVO ENTDADOS PARA AS SEGUINTES USINAS TERMICAS E ELOS DE CC:')
		cabecalho.append('&')
		cabecalho.append('&      - NORTE FLUMINENSE')
		cabecalho.append('&      - ATLANTICO')
		cabecalho.append('&      ')
		cabecalho.append('&      - 2 BIPOLOS DO MADEIRA (Obs.: A usina de Sto Antonio possui um montante que Ã© escoado pelo 230kV, portanto a geraÃ§Ã£o das unidades 45 a 50 devem ser expurgadas na representaÃ§Ã£o por parcela.)')
		cabecalho.append('&      - 2 BK-TO-BK')
		cabecalho.append('&      - 1 BIPOLO XINGU-ESTREITO')
		cabecalho.append('&      ')
		cabecalho.append('&ObservaÃ§Ãµes: As restriÃ§Ãµes elÃ©tricas 807 a 809 forÃ§am a distribuiÃ§Ã£o igual entre os Bipolos e Bk-to-Bk, porÃ©m quando um componente estiver sob intervenÃ§Ã£o a restriÃ§Ã£o nÃ£o serÃ¡ atendida')
		cabecalho.append('&')
		cabecalho.append('&=================================================================================')

	elif mnemonico == 'AC':
		cabecalho.append('&   ALTERACOES DE CADASTRO                                                                                              ')
		cabecalho.append('&')
		cabecalho.append('&   USI  MNEMONICO VALOR(ES)                                                                                            ')
		cabecalho.append('&X  XXX  XXXXXX    XXXXXxxxxx                                                                                           ')
		cabecalho.append('&X  XXX  XXXXXX    XXXXXxxxxxxxxxx                                                                                      ')
		cabecalho.append('&X  XXX  XXXXXX    XXXXXxxxxxxxxxxxxxxx                                                                                 ')
		cabecalho.append('&X  XXX  XXXXXX    XXXXXXXXXX')

	elif mnemonico == 'DA':
		cabecalho.append('&  TAXA DE DESVIO DE AGUA                                                                                               ')
		cabecalho.append('&')
		cabecalho.append('&   ind di hi m df hf m Taxa(m3/s)                                                                                      ')
		cabecalho.append('&X  XXX XX XX X XX XX X XXXXXXXXXX   ')
	
	elif mnemonico == 'FP':
		cabecalho.append('&   DADOS PARA A DISCRETIZACAO DA FUNCAO DE PRODUCAO                                                                    ')
		cabecalho.append('&')
		cabecalho.append('&  usi F nptQ nptV           DELTAV               TR                                                                    ')
		cabecalho.append('&X XXX X  XXX  XXX  X   X    XXXXXXXXXXxxxxxxxxxx X    ')

	elif mnemonico == 'TX':
		cabecalho.append('&   taxa de juros                                                                                                       ')
		cabecalho.append('&')
		cabecalho.append('&x  xxxxxxxxxx  ')

	elif mnemonico == 'EZ':
		cabecalho.append('&   Usi  Energia armazenada                                                                                             ')
		cabecalho.append('&')
		cabecalho.append('&z  xxx  xxxxx                                                                                                          ')

	elif mnemonico == 'AG':
		cabecalho.append('&   Nest                                                                                                                ')
		cabecalho.append('&')
		cabecalho.append('&G  xxx                                                                                                                 ')

	elif mnemonico == 'SECR':
		cabecalho.append('&   Define a secao do rio em R11                                                                                                                                                                                       ')
		cabecalho.append('&    num         nome   usi fator usi fator                                                                                                                                                                            ')
		cabecalho.append('&XXX XXX XXXXXXXXXXXX   XXX XXXXX XXX XXXXX                                                                                                                                                                            ')

	elif mnemonico == 'MH':
		cabecalho.append('&   MANUTENCAO HIDRAULICA')
		cabecalho.append('&')
		cabecalho.append('&   No   gr iddi hi midf hf mfF')
		cabecalho.append('&X  XXX  XX xxXX xx X XX XX X x')

	elif mnemonico == 'CR':
		cabecalho.append('&   Polinomio Cota x Vazao em R11                                                                                                                                                                                      ')
		cabecalho.append('&   num          nome   gr             A0              A1              A2               A3              A4              A5              A6                                                                              ')
		cabecalho.append('&X  XXX  XXXXXXXXXXXX   XX XXXXXXXXXXXXXXX XXXXXXXXXXXXXXX XXXXXXXXXXXXXXX XXXXXXXXXXXXXXX XXXXXXXXXXXXXXX XXXXXXXXXXXXXXX  XXXXXXXXXXXXXX                                                                              ')

	elif mnemonico == 'R11':
		cabecalho.append('&   Registro R11                                                                                                                                                                                                       ')
		cabecalho.append('&   di hi m df hf m    cotaIni  var.hora   var.dia                                                                                                                                                                     ')
		cabecalho.append('&X  XX XX X XX XX X XXXXXXXXXXxxxxxxxxxxXXXXXXXXXX                                                                                                                                                                     ')
	
	elif mnemonico == 'MT':
		cabecalho.append('&')
		cabecalho.append('&')
		cabecalho.append('&   MANUTENCAO TERMELETRICA')
		cabecalho.append('&')
		cabecalho.append('&   ute  ug  di hi m df hf m F')
		cabecalho.append('&X  XXX XXX  xx xx x xx xx x x')

	return cabecalho

def getRodapes(mnemonico):
	rodape = []
	if mnemonico == 'RD':
		rodape.append('&')
		rodape.append('&1 - Mnemonico RD')
		rodape.append('&2 - Folga para as restricoes de limite de fluxos nas linhas')
		rodape.append('&3 - Numero maximo de folgas para as restricoes de limite de fluxo nas linhas')
		rodape.append('&4 - Utiliza a carga da rede mas sem calcular os fluxos e verificar os limites')
		rodape.append('&5 - Libera os limites de fluxo dos trafos elevadores')
		rodape.append('&6 - Libera todos os circuitos (mas nao as inequações)')
		rodape.append('&7 - Considera as perdas de acordo com osregistros DLIN e DGBT')

	return rodape

def formatarBloco(df_bloco, formatacao):

	bloco = []
	for index, row in df_bloco.iterrows():
		bloco.append(formatacao.format(*row.values))

	return bloco


def getValoresIniciaisUH(data):

	datab = wx_dbLib.WxDataB()
	dataFormat = datab.getDateFormat()
	sql = '''SELECT
					TB_NIVEIS_DESSEM.CD_UHE,
					TB_POSTO_UHE.STR_USINA,
					TB2_PRODUTIBILIDADE.CD_REE,
					TB_NIVEIS_DESSEM.VL_PERC_VAZ_DEFL
				FROM
					TB_NIVEIS_DESSEM
				LEFT JOIN TB_POSTO_UHE ON
					TB_POSTO_UHE.CD_UHE = TB_NIVEIS_DESSEM.CD_UHE
				LEFT JOIN TB2_PRODUTIBILIDADE ON
					TB2_PRODUTIBILIDADE.CD_POSTO = TB_POSTO_UHE.CD_POSTO
				WHERE
					DT_REFERENTE = '{}'
				ORDER BY
					CD_UHE'''.format((data-datetime.timedelta(days=0)).strftime(dataFormat))


	answer = datab.requestServer(sql)
	valoresIniciais = pd.DataFrame(answer, columns=['cd_uhe', 'nomeUsina', 'cd_ree', 'vazaoDefluente'])

	valoresIniciais.loc[valoresIniciais['cd_uhe'] == 107, 'cd_ree'] = 10

	# Inserir valores iniciais para os postos de bombeamento (valor default igual a 60%)
	valoresIniciais = valoresIniciais.append({'cd_uhe':108, 'nomeUsina':'TRAICAO', 'cd_ree':1 ,'vazaoDefluente':60}, ignore_index=True)
	valoresIniciais = valoresIniciais.append({'cd_uhe':109, 'nomeUsina':'PEDREIRA', 'cd_ree':1 ,'vazaoDefluente':60}, ignore_index=True)

	valoresIniciais = valoresIniciais.append({'cd_uhe': 117, 'nomeUsina':'GUARAPIRANG', 'cd_ree':1, 'vazaoDefluente':60}, ignore_index=True)
	valoresIniciais = valoresIniciais.append({'cd_uhe': 118, 'nomeUsina':'BILLINGS', 'cd_ree':1, 'vazaoDefluente':60}, ignore_index=True)
	valoresIniciais = valoresIniciais.append({'cd_uhe': 119, 'nomeUsina':'HENRY BORDE', 'cd_ree':1, 'vazaoDefluente':60}, ignore_index=True)

	valoresIniciais = valoresIniciais.append({'cd_uhe': 124, 'nomeUsina':'LAJES', 'cd_ree':1, 'vazaoDefluente':60}, ignore_index=True)

	valoresIniciais = valoresIniciais.append({'cd_uhe': 131,  'nomeUsina':'NILO PECANH', 'cd_ree':1, 'vazaoDefluente':60}, ignore_index=True)
	valoresIniciais = valoresIniciais.append({'cd_uhe': 133,  'nomeUsina':'P. PASSOS', 'cd_ree':1, 'vazaoDefluente':60}, ignore_index=True)
	
	valoresIniciais = valoresIniciais.append({'cd_uhe': 146,  'nomeUsina':'FONTES C', 'cd_ree':1, 'vazaoDefluente':60}, ignore_index=True)
	valoresIniciais = valoresIniciais.append({'cd_uhe': 147,  'nomeUsina':'FONTES AB', 'cd_ree':1, 'vazaoDefluente':60}, ignore_index=True)

	valoresIniciais = valoresIniciais.append({'cd_uhe': 180,  'nomeUsina':'TOCOS', 'cd_ree':1, 'vazaoDefluente':60}, ignore_index=True)
	valoresIniciais = valoresIniciais.append({'cd_uhe': 181,  'nomeUsina':'SANTANA', 'cd_ree':1, 'vazaoDefluente':60}, ignore_index=True)
	valoresIniciais = valoresIniciais.append({'cd_uhe': 182,  'nomeUsina':'VIGARIO', 'cd_ree':1, 'vazaoDefluente':60}, ignore_index=True)


	# Deixar em ordem dos cd_uhe
	valoresIniciais = valoresIniciais.sort_values(by=['cd_uhe'])

	# Removendo o posto 128 (ANTA)
	valoresIniciais.drop(valoresIniciais[valoresIniciais['cd_uhe'] == 128].index, inplace=True)

	nomesDiferentesUsinas = getNomesDiferentesUsinas()
	valoresIniciais['nomeUsina'] = valoresIniciais['nomeUsina'].replace(getNomesDiferentesUsinas())

	valoresIniciais['flagEvaporacao'] = 1
	valoresIniciais['di'] = 'I'
	valoresIniciais['hi'] = ''
	valoresIniciais['m'] = ''
	valoresIniciais['mortoInicial'] = ''
	valoresIniciais['flagProdutividadeConst'] = ''
	valoresIniciais['flagInclusaoRestricao'] = ''

	bloco_uh = []
	for index, row in valoresIniciais.iterrows():
		bloco_uh.append('UH  {:>3}  {:<12}   {:>2}   {:>6}    {:>1} {:<2} {:<2} {:<1} {:>10}     {:>1}    {:>1}'.format(row['cd_uhe'], row['nomeUsina'][0:12], int(row['cd_ree']), float(row['vazaoDefluente']), row['flagEvaporacao'], row['di'], row['hi'], row['m'], row['mortoInicial'], row['flagProdutividadeConst'], row['flagInclusaoRestricao']))

	return bloco_uh


def gravarArquivo(pathSaida, valores):
	fileOut = codecs.open(pathSaida, 'a+', 'utf-8')

	for linha in valores:
		fileOut.write('{}\n'.format(linha.strip()))

	fileOut.close()

def gerarEntdados(data, pathArquivos):

	pathArqEntrada = os.path.join(pathArquivos, data.strftime('%Y%m%d'), 'entrada')

	pathArqSaida = os.path.join(pathArquivos, data.strftime('%Y%m%d'), 'saida')
	if not os.path.exists(pathArqSaida):
		os.makedirs(pathArqSaida)

	diaReferente = (data + datetime.timedelta(days=1)).day
	dataEletrica = wx_opweek.getLastSaturday(data + datetime.timedelta(days=1))
	dataEletrica = wx_opweek.ElecData(dataEletrica)

	diasSemana = []
	diasSemanaMaisUm = []
	diasSemanaPassado = []
	diasSemanaFuturo = []
	primeiroDiaSemanaEletrica = dataEletrica.inicioSemana
	calendarioAux = {}
	for i in range(7):
		dia = primeiroDiaSemanaEletrica + datetime.timedelta(days=i)
		diasSemana.append(dia.day)
		diasSemanaMaisUm.append((dia + datetime.timedelta(days=1)).day)
		if dia <= data.date():
			diasSemanaPassado.append(dia.day)
		else:
			diasSemanaFuturo.append(dia.day)
	calendarioAux = dict(zip(diasSemana,diasSemanaMaisUm))

	pathEntdadosOut = os.path.join(pathArqSaida,'entdados.dat')
	fileOut = open(pathEntdadosOut, 'w')
	fileOut.close()

	entdados = leituraArquivo(os.path.join(pathArqEntrada, 'ccee_entrada', 'entdados.dat'))

	entdadosOns = leituraArquivo(os.path.join(pathArqEntrada, 'ons_entrada_saida', 'entdados.dat'))
	
	infoBlocos = getInfoBlocos()

	# Bloco RD
	blocoRD = extrairInfoBloco(entdados, 'RD', infoBlocos['RD']['regex'])
	df_blocoRD = pd.DataFrame(blocoRD, columns=infoBlocos['RD']['campos'])
	bloco_rd = getCabecalhos('RD')
	bloco_rd += formatarBloco(df_blocoRD, infoBlocos['RD']['formatacao'])
	bloco_rd += getRodapes('RD')
	gravarArquivo(pathEntdadosOut, bloco_rd)

	# Bloco RIVAR
	blocoRIVAR = extrairInfoBloco(entdados, 'RIVAR', infoBlocos['RIVAR']['regex'])
	df_blocoRIVAR = pd.DataFrame(blocoRIVAR, columns=infoBlocos['RIVAR']['campos'])
	bloco_rivar = getCabecalhos('RIVAR')
	bloco_rivar += formatarBloco(df_blocoRIVAR, infoBlocos['RIVAR']['formatacao'])
	gravarArquivo(pathEntdadosOut, bloco_rivar)

	# Bloco TM
	blocoTm_txt = leituraArquivo(os.path.join(pathArqEntrada, 'blocos', 'Bloco_TM.txt'))
	bloco_TM = extrairInfoBloco(blocoTm_txt, 'TM', infoBlocos['TM']['regex'])
	df_blocoTM = pd.DataFrame(bloco_TM, columns=infoBlocos['TM']['campos'])
	# Desabilitar a rede
	df_blocoTM['rede'] = 0
	bloco_tm = getCabecalhos('TM')
	bloco_tm += formatarBloco(df_blocoTM, infoBlocos['TM']['formatacao'])
	gravarArquivo(pathEntdadosOut, bloco_tm)

	# Bloco SIST
	blocoSIST = extrairInfoBloco(entdados, 'SIST', infoBlocos['SIST']['regex'])
	df_blocoSIST = pd.DataFrame(blocoSIST, columns=infoBlocos['SIST']['campos'])
	bloco_sis = getCabecalhos('SIST')
	bloco_sis += formatarBloco(df_blocoSIST, infoBlocos['SIST']['formatacao'])
	gravarArquivo(pathEntdadosOut, bloco_sis)

	# Bloco REE
	blocoREE = extrairInfoBloco(entdados, 'REE', infoBlocos['REE']['regex'])
	df_blocoREE = pd.DataFrame(blocoREE, columns=infoBlocos['REE']['campos'])
	bloco_ree = getCabecalhos('REE')
	bloco_ree += formatarBloco(df_blocoREE, infoBlocos['REE']['formatacao'])
	gravarArquivo(pathEntdadosOut, bloco_ree)

	# Bloco UH
	bloco_uh = getCabecalhos('UH')
	bloco_uh += getValoresIniciaisUH(data)
	gravarArquivo(pathEntdadosOut, bloco_uh)

	# Bloco TVIAG
	blocoTVIAG = extrairInfoBloco(entdados, 'TVIAG', infoBlocos['TVIAG']['regex'])
	df_blocoTVIAG = pd.DataFrame(blocoTVIAG, columns=infoBlocos['TVIAG']['campos'])
	bloco_tviag = getCabecalhos('TVIAG')
	bloco_tviag += formatarBloco(df_blocoTVIAG, infoBlocos['TVIAG']['formatacao'])
	gravarArquivo(pathEntdadosOut, bloco_tviag)

	# Bloco UT (geracao)
	blocoUT = extrairInfoBloco(entdados, 'UT', infoBlocos['UT']['regex'])
	df_blocoUT = pd.DataFrame(blocoUT, columns=infoBlocos['UT']['campos'])
	df_blocoUtGeracao = df_blocoUT[df_blocoUT['unidadeRestricao'] == ' '].copy()
	df_blocoUtGeracao['diaInicial'] = df_blocoUtGeracao['diaInicial'].astype(int).replace(calendarioAux)
	bloco_ut = getCabecalhos('UT')
	bloco_ut += formatarBloco(df_blocoUtGeracao, infoBlocos['UT']['formatacao'])
	gravarArquivo(pathEntdadosOut, bloco_ut)
	
	# Bloco UT (restricao)
	blocoUTR = extrairInfoBloco(entdados, '&UT', infoBlocos['&UT']['regex'])
	df_blocoUTR = pd.DataFrame(blocoUTR, columns=infoBlocos['&UT']['campos'])
	df_blocoUTR['diaInicial'] = df_blocoUTR['diaInicial'].astype(int).replace(calendarioAux)
	bloco_utr = getCabecalhos('UT2')
	bloco_utr += formatarBloco(df_blocoUTR, infoBlocos['&UT']['formatacao'])
	gravarArquivo(pathEntdadosOut, bloco_utr)

	# Bloco USIE
	blocoUSIE = extrairInfoBloco(entdados, 'USIE', infoBlocos['USIE']['regex'])
	df_blocoUSIE = pd.DataFrame(blocoUSIE, columns=infoBlocos['USIE']['campos'])
	bloco_usie = getCabecalhos('USIE')
	bloco_usie += formatarBloco(df_blocoUSIE, infoBlocos['USIE']['formatacao'])
	gravarArquivo(pathEntdadosOut, bloco_usie)

	# Bloco DP
	blocodp_ccee = extrairInfoBloco(entdados, 'DP', infoBlocos['DP']['regex'])
	blocoDP_ccee = pd.DataFrame(blocodp_ccee, columns=infoBlocos['DP']['campos'])
	blocoDP_ccee['Demanda'] = blocoDP_ccee['Demanda'].apply(pd.to_numeric, errors='coerce')
	blocodp_ons = extrairInfoBloco(entdadosOns, 'DP', infoBlocos['DP']['regex'])
	blocoDP_ons = pd.DataFrame(blocodp_ons, columns=infoBlocos['DP']['campos'])
	blocoDP_ons['Demanda'] = blocoDP_ons['Demanda'].apply(pd.to_numeric, errors='coerce')
	blocoDP_ons['diferencaRede'] = blocoDP_ons['Demanda'] - blocoDP_ccee['Demanda']
	blocoDP_ons['ss'] = blocoDP_ons['ss'].apply(pd.to_numeric, errors='coerce')

	blocodp = leituraArquivo(os.path.join(pathArqEntrada, 'blocos', 'DP.txt'))
	blocoDP = extrairInfoBloco(blocodp, 'DP', infoBlocos['DP']['regex'])
	df_blocoDP = pd.DataFrame(blocoDP, columns=infoBlocos['DP']['campos'])
	df_blocoDP['Demanda'] = df_blocoDP['Demanda'].apply(pd.to_numeric, errors='coerce')
	df_blocoDP['ss'] = df_blocoDP['ss'].apply(pd.to_numeric, errors='coerce')
	# Retirar a diferenca da rede
	for submercado in df_blocoDP['ss'].unique():
		numPeriodos = (df_blocoDP[df_blocoDP['ss'] == submercado]).shape[0]
		diff = blocoDP_ons[blocoDP_ons['ss'] == submercado].iloc[:numPeriodos]['diferencaRede']
		df_blocoDP.loc[df_blocoDP['ss'] == submercado, 'Demanda'] = df_blocoDP.loc[df_blocoDP['ss'] == submercado, 'Demanda'].values - diff.values

	df_blocoDP['Demanda'] = df_blocoDP['Demanda'].round(decimals=2)
	bloco_dp = getCabecalhos('DP')
	bloco_dp += formatarBloco(df_blocoDP, infoBlocos['DP']['formatacao'])
	gravarArquivo(pathEntdadosOut, bloco_dp)

	# Bloco DE
	bloco_de = getFromFile(os.path.join(pathArqEntrada, 'blocos', 'DE.txt'))
	blocoDE = extrairInfoBloco(entdados, 'DE', infoBlocos['DE']['regex'])
	df_blocoDE = pd.DataFrame(blocoDE, columns=infoBlocos['DE']['campos'])
	df_blocoDE['justificativa'] = df_blocoDE['justificativa'].str.strip()
	df_blocoDE = df_blocoDE[df_blocoDE['justificativa'].isin(['ANDE','BKTBK'])]
	bktbkIndex = df_blocoDE[df_blocoDE['justificativa'] == 'BKTBK'].index
	df_blocoDE.loc[bktbkIndex, 'diaInicial'] = df_blocoDE.loc[bktbkIndex, 'diaInicial'].astype(int).replace(calendarioAux)
	bloco_de += formatarBloco(df_blocoDE, infoBlocos['DE']['formatacao'])
	gravarArquivo(pathEntdadosOut, bloco_de)

	# Bloco CD
	blocoCD = extrairInfoBloco(entdados, 'CD', infoBlocos['CD']['regex'])
	df_blocoCD = pd.DataFrame(blocoCD, columns=infoBlocos['CD']['campos'])
	bloco_cd = getCabecalhos('CD')
	bloco_cd += formatarBloco(df_blocoCD, infoBlocos['CD']['formatacao'])
	gravarArquivo(pathEntdadosOut, bloco_cd)

	# Bloco PQ
	blocoPQ = extrairInfoBloco(entdados, 'PQ', infoBlocos['PQ']['regex'])
	df_blocoPQ = pd.DataFrame(blocoPQ, columns=infoBlocos['PQ']['campos'])
	df_blocoPQ['diaInicial'] = df_blocoPQ['diaInicial'].astype(int).replace(calendarioAux)
	df_blocoPQ['diaFinal'] = df_blocoPQ['diaFinal'].astype(int).replace(calendarioAux)
	bloco_pq = getCabecalhos('PQ')
	bloco_pq += formatarBloco(df_blocoPQ, infoBlocos['PQ']['formatacao'])
	gravarArquivo(pathEntdadosOut, bloco_pq)

	# Bloco IT
	blocoIT = extrairInfoBloco(entdados, 'IT', infoBlocos['IT']['regex'])
	df_blocoIT = pd.DataFrame(blocoIT, columns=infoBlocos['IT']['campos'])
	bloco_it = getCabecalhos('IT')
	bloco_it += formatarBloco(df_blocoIT, infoBlocos['IT']['formatacao'])
	gravarArquivo(pathEntdadosOut, bloco_it)

	# Bloco RI
	blocoRI = extrairInfoBloco(entdados, 'RI', infoBlocos['RI']['regex'])
	df_blocoRI = pd.DataFrame(blocoRI, columns=infoBlocos['RI']['campos'])
	bloco_ri = getCabecalhos('RI')
	bloco_ri += formatarBloco(df_blocoRI, infoBlocos['RI']['formatacao'])
	gravarArquivo(pathEntdadosOut, bloco_ri)

	# Bloco IA
	blocoIA = extrairInfoBloco(entdados, 'IA', infoBlocos['IA']['regex'])
	df_blocoIA = pd.DataFrame(blocoIA, columns=infoBlocos['IA']['campos'])
	# intercambios = df_blocoIA['diaInicial'].astype(int) == primeiroDiaSemanaEletrica.day
	# intercambioDestinoFc = df_blocoIA['subsistemaDestino'] == 'FC'
	# intercambioFuturos = df_blocoIA['diaInicial'].astype(int) >= data.day
	# df_blocoIA.loc[intercambioDestinoFc & intercambioFuturos, 'diaInicial'] = df_blocoIA[intercambioDestinoFc & intercambioFuturos]['diaInicial'].astype(int).replace(calendarioAux)
	# indexFuturo = df_blocoIA[~df_blocoIA['diaInicial'].astype(int).isin(diasSemana)].index
	# df_blocoIA.drop(indexFuturo, inplace=True)
	bloco_ia = getCabecalhos('IA')
	bloco_ia += formatarBloco(df_blocoIA, infoBlocos['IA']['formatacao'])
	gravarArquivo(pathEntdadosOut, bloco_ia)

	# Bloco GP
	blocoGP = extrairInfoBloco(entdados, 'GP', infoBlocos['GP']['regex'])
	df_blocoGP = pd.DataFrame(blocoGP, columns=infoBlocos['GP']['campos'])
	bloco_gp = getCabecalhos('GP')
	bloco_gp += formatarBloco(df_blocoGP, infoBlocos['GP']['formatacao'])
	gravarArquivo(pathEntdadosOut, bloco_gp)

	# Bloco NI
	blocoNI = extrairInfoBloco(entdados, 'NI', infoBlocos['NI']['regex'])
	df_blocoNI = pd.DataFrame(blocoNI, columns=infoBlocos['NI']['campos'])
	bloco_ni = getCabecalhos('NI')
	bloco_ni += formatarBloco(df_blocoNI, infoBlocos['NI']['formatacao'])
	gravarArquivo(pathEntdadosOut, bloco_ni)

	# Bloco VE
	blocoVE = extrairInfoBloco(entdados, 'VE', infoBlocos['VE']['regex'])
	df_blocoVE = pd.DataFrame(blocoVE, columns=infoBlocos['VE']['campos'])
	df_blocoVE['diaInicial'] = df_blocoVE['diaInicial'].astype(int).replace(calendarioAux)
	df_blocoVE['diaFinal'] = df_blocoVE['diaFinal'].astype(int).replace(calendarioAux)
	bloco_ve = getCabecalhos('VE')
	bloco_ve += formatarBloco(df_blocoVE, infoBlocos['VE']['formatacao'])
	gravarArquivo(pathEntdadosOut, bloco_ve)


	# Bloco CE e CI
	blocoCECI = extrairInfoBloco(entdados, 'CE_CI', infoBlocos['CE_CI']['regex'])
	df_blocoCECI = pd.DataFrame(blocoCECI, columns=infoBlocos['CE_CI']['campos'])	
	indexDiaInicial = df_blocoCECI[df_blocoCECI['diaInicial'].str.strip() != 'I'].index
	df_blocoCECI.loc[indexDiaInicial, 'diaInicial'] = df_blocoCECI.loc[indexDiaInicial, 'diaInicial'].astype(int).replace(calendarioAux)
	indexPassado = df_blocoCECI[df_blocoCECI['diaFinal'].str.strip() != 'F']['diaFinal'].astype(int).isin(diasSemanaPassado).index
	df_blocoCECI.drop(indexPassado, inplace=True)
	bloco_ceci = getCabecalhos('CE_CI')
	bloco_ceci += formatarBloco(df_blocoCECI, infoBlocos['CE_CI']['formatacao'])
	gravarArquivo(pathEntdadosOut, bloco_ceci)

	# Bloco RESTRICOES ESPECIAIS
	# 'RE', 'LU', 'FH', 'FT', 'FI', 'FE', 'FR', 'FC'
	blocoRE = extrairInfoBloco(entdados, 'RE', infoBlocos['RE']['regex'])
	df_blocoRE = pd.DataFrame(blocoRE, columns=infoBlocos['RE']['campos'])
	blocoLU = extrairInfoBloco(entdados, 'LU', infoBlocos['LU']['regex'])
	df_blocoLU = pd.DataFrame(blocoLU, columns=infoBlocos['LU']['campos'])
	blocoFH = extrairInfoBloco(entdados, 'FH', infoBlocos['FH']['regex'])
	df_blocoFH = pd.DataFrame(blocoFH, columns=infoBlocos['FH']['campos'])
	blocoFT = extrairInfoBloco(entdados, 'FT', infoBlocos['FT']['regex'])
	df_blocoFT = pd.DataFrame(blocoFT, columns=infoBlocos['FT']['campos'])
	blocoFI = extrairInfoBloco(entdados, 'FI', infoBlocos['FI']['regex'])
	df_blocoFI = pd.DataFrame(blocoFI, columns=infoBlocos['FI']['campos'])
	blocoFE = extrairInfoBloco(entdados, 'FE', infoBlocos['FE']['regex'])
	df_blocoFE = pd.DataFrame(blocoFE, columns=infoBlocos['FE']['campos'])
	blocoFR = extrairInfoBloco(entdados, 'FR', infoBlocos['FR']['regex'])
	df_blocoFR = pd.DataFrame(blocoFR, columns=infoBlocos['FR']['campos'])
	blocoFC = extrairInfoBloco(entdados, 'FC', infoBlocos['FC']['regex'])
	df_blocoFC = pd.DataFrame(blocoFC, columns=infoBlocos['FC']['campos'])

	if diaReferente in calendarioAux:
		dictReplace = {'{:>2}'.format(diaReferente):calendarioAux[diaReferente]}
		df_blocoRE['di'] = df_blocoRE['di'].replace(dictReplace)
		df_blocoFH['di'] = df_blocoFH['di'].replace(dictReplace)
		df_blocoFT['di'] = df_blocoFT['di'].replace(dictReplace)
		df_blocoFI['di'] = df_blocoFI['di'].replace(dictReplace)
		df_blocoFE['di'] = df_blocoFE['di'].replace(dictReplace)
		df_blocoFR['di'] = df_blocoFR['di'].replace(dictReplace)
		df_blocoFC['di'] = df_blocoFC['di'].replace(dictReplace)

		# Condicao especial para 147
		df_blocoLU.loc[df_blocoLU[df_blocoLU['ind'] !=' 147'].index, 'di'] = df_blocoLU.loc[df_blocoLU[df_blocoLU['ind'] !=' 147'].index, 'di'].replace(dictReplace)


	# Restricao especial para o 45
	index_id45 = df_blocoLU[(df_blocoLU['ind']==' 45') & (df_blocoLU['di']==str(data.day))].index
	df_blocoLU.loc[index_id45, 'di'] = (data+datetime.timedelta(days=1)).day

	# indexPassado = df_blocoLU.loc[df_blocoLU['di'].str.strip() != 'I', 'di'].astype(int).isin(diasSemanaPassado).index
	# df_blocoLU.drop(indexPassado, inplace=True)

	bloco_restricoes = []
	for index, row in df_blocoRE.iterrows():
		ind = row['ind']
		bloco_restricoes += formatarBloco(df_blocoRE[df_blocoRE['ind'] == ind], infoBlocos['RE']['formatacao'])
		bloco_restricoes += formatarBloco(df_blocoLU[df_blocoLU['ind'] == ind], infoBlocos['LU']['formatacao'])
		bloco_restricoes += formatarBloco(df_blocoFH[df_blocoFH['ind'] == ind], infoBlocos['FH']['formatacao'])
		bloco_restricoes += formatarBloco(df_blocoFE[df_blocoFE['ind'] == ind], infoBlocos['FE']['formatacao'])
		bloco_restricoes += formatarBloco(df_blocoFT[df_blocoFT['ind'] == ind], infoBlocos['FT']['formatacao'])
		bloco_restricoes += formatarBloco(df_blocoFR[df_blocoFR['ind'] == ind], infoBlocos['FR']['formatacao'])
		bloco_restricoes += formatarBloco(df_blocoFC[df_blocoFC['ind'] == ind], infoBlocos['FC']['formatacao'])
		bloco_restricoes += formatarBloco(df_blocoFI[df_blocoFI['ind'] == ind], infoBlocos['FI']['formatacao'])


	gravarArquivo(pathEntdadosOut, bloco_restricoes)


	# Bloco AC
	bloco_AC = getCabecalhos('AC')
	bloco_AC += entdados['AC']
	gravarArquivo(pathEntdadosOut, bloco_AC)

	# Bloco DA
	blocoDA = extrairInfoBloco(entdados, 'DA', infoBlocos['DA']['regex'])
	df_blocoDA = pd.DataFrame(blocoDA, columns=infoBlocos['DA']['campos'])
	bloco_DA = getCabecalhos('DA')
	bloco_DA += formatarBloco(df_blocoDA, infoBlocos['DA']['formatacao'])
	bloco_DA += getRodapes('DA')
	gravarArquivo(pathEntdadosOut, bloco_DA)

	# Bloco FP
	blocoFP = extrairInfoBloco(entdados, 'FP', infoBlocos['FP']['regex'])
	df_blocoFP = pd.DataFrame(blocoFP, columns=infoBlocos['FP']['campos'])
	bloco_FP = getCabecalhos('FP')
	bloco_FP += formatarBloco(df_blocoFP, infoBlocos['FP']['formatacao'])
	bloco_FP += getRodapes('FP')
	gravarArquivo(pathEntdadosOut, bloco_FP)

	# Bloco TX
	blocoTX = extrairInfoBloco(entdados, 'TX', infoBlocos['TX']['regex'])
	df_blocoTX = pd.DataFrame(blocoTX, columns=infoBlocos['TX']['campos'])
	bloco_TX = getCabecalhos('TX')
	bloco_TX += formatarBloco(df_blocoTX, infoBlocos['TX']['formatacao'])
	bloco_TX += getRodapes('TX')
	gravarArquivo(pathEntdadosOut, bloco_TX)

	# Bloco EZ
	blocoEZ = extrairInfoBloco(entdados, 'EZ', infoBlocos['EZ']['regex'])
	df_blocoEZ = pd.DataFrame(blocoEZ, columns=infoBlocos['EZ']['campos'])
	bloco_EZ = getCabecalhos('EZ')
	bloco_EZ += formatarBloco(df_blocoEZ, infoBlocos['EZ']['formatacao'])
	bloco_EZ += getRodapes('EZ')
	gravarArquivo(pathEntdadosOut, bloco_EZ)

	# Bloco AG
	blocoAG = extrairInfoBloco(entdados, 'AG', infoBlocos['AG']['regex'])
	df_blocoAG = pd.DataFrame(blocoAG, columns=infoBlocos['AG']['campos'])
	bloco_AG = getCabecalhos('AG')
	bloco_AG += formatarBloco(df_blocoAG, infoBlocos['AG']['formatacao'])
	bloco_AG += getRodapes('AG')
	gravarArquivo(pathEntdadosOut, bloco_AG)

	# Bloco SECR
	blocoSECR = extrairInfoBloco(entdados, 'SECR', infoBlocos['SECR']['regex'])
	df_blocoSECR = pd.DataFrame(blocoSECR, columns=infoBlocos['SECR']['campos'])
	bloco_SECR = getCabecalhos('SECR')
	bloco_SECR += formatarBloco(df_blocoSECR, infoBlocos['SECR']['formatacao'])
	bloco_SECR += getRodapes('SECR')
	gravarArquivo(pathEntdadosOut, bloco_SECR)

	# Bloco MH
	blocoMH = extrairInfoBloco(entdados, 'MH', infoBlocos['MH']['regex'])
	df_blocoMH = pd.DataFrame(blocoMH, columns=infoBlocos['MH']['campos'])
	# df_blocoMH['diaInicial'] = df_blocoMH['diaInicial'].astype(int)
	# df_blocoMH['diaFinal'] = df_blocoMH['diaFinal'].astype(int)
	# df_blocoMH.drop(df_blocoMH[df_blocoMH['diaFinal'].isin(diasSemanaPassado)].index, inplace=True)
	# df_blocoMH['diaInicial'] = df_blocoMH['diaInicial'].replace({data.day:calendarioAux[data.day]})
	bloco_MH = getCabecalhos('MH')
	bloco_MH += formatarBloco(df_blocoMH, infoBlocos['MH']['formatacao'])
	bloco_MH += getRodapes('MH')
	gravarArquivo(pathEntdadosOut, bloco_MH)

	# Bloco CR
	blocoCR = extrairInfoBloco(entdados, 'CR', infoBlocos['CR']['regex'])
	df_blocoCR = pd.DataFrame(blocoCR, columns=infoBlocos['CR']['campos'])
	bloco_CR = getCabecalhos('CR')
	bloco_CR += formatarBloco(df_blocoCR, infoBlocos['CR']['formatacao'])
	bloco_CR += getRodapes('CR')
	gravarArquivo(pathEntdadosOut, bloco_CR)

	# Bloco R11
	blocoR11 = extrairInfoBloco(entdados, 'R11', infoBlocos['R11']['regex'])
	df_blocoR11 = pd.DataFrame(blocoR11, columns=infoBlocos['R11']['campos'])
	# df_blocoR11['nivelMeiaHoraAntesEstudo'] = cotasr11[-1].split()[-1]
	df_blocoR11['diaInicial'] = df_blocoR11['diaInicial'].astype(int).replace({data.day:calendarioAux[data.day]})
	bloco_R11 = getCabecalhos('R11')
	bloco_R11 += formatarBloco(df_blocoR11, infoBlocos['R11']['formatacao'])
	bloco_R11 += getRodapes('R11')
	gravarArquivo(pathEntdadosOut, bloco_R11)

	# Bloco MT
	blocoMT = extrairInfoBloco(entdados, 'MT', infoBlocos['MT']['regex'])
	df_blocoMT = pd.DataFrame(blocoMT, columns=infoBlocos['MT']['campos'])
	df_blocoMT['di'] = df_blocoMT['di'].astype(int).replace(calendarioAux)
	df_blocoMT['df'] = df_blocoMT['df'].astype(int).replace(calendarioAux)
	bloco_MT = getCabecalhos('MT')
	bloco_MT += formatarBloco(df_blocoMT, infoBlocos['MT']['formatacao'])
	bloco_MT += getRodapes('MT')
	gravarArquivo(pathEntdadosOut, bloco_MT)

	print('entdados.dat: {}'.format(pathEntdadosOut))


def gerarEntdadosDef(data, pathArquivos):

	try:
		arquivoMudancasOnsCcee = os.path.join(os.path.abspath('.'), 'arquivos', 'diferencasOnsCcee.xlsx')
		df_mudancasCcee = pd.ExcelFile(arquivoMudancasOnsCcee)
		mudancasCcee_CE_CE = df_mudancasCcee.parse('bloco_CE_CI', header=0).convert_dtypes()
	except:
		mudancasCcee_CE_CE = pd.DataFrame()
		print('Não foi encontrado nenhum arquivo com as configurações que se aplicam apenas na CCEE\n{}'.format(arquivoMudancasOnsCcee))


	dataRodada = data
	diaAnterior = data - datetime.timedelta(days=1) 
	data = data + datetime.timedelta(days=1)

	dataEletrica = wx_opweek.getLastSaturday(data)
	dataEletrica = wx_opweek.ElecData(dataEletrica)

	diasSemana = []
	diasSemanaMaisUm = []
	diasSemanaPassado = []
	diasSemanaFuturo = []
	primeiroDiaSemanaEletrica = dataEletrica.inicioSemana
	calendarioAux = {}
	for i in range(7):
		dia = primeiroDiaSemanaEletrica + datetime.timedelta(days=i)
		diasSemana.append(dia.day)
		diasSemanaMaisUm.append((dia + datetime.timedelta(days=1)).day)
		if dia < data.date():
			diasSemanaPassado.append(dia.day)
		else:
			diasSemanaFuturo.append(dia.day)
	calendarioAux = dict(zip(diasSemana,diasSemanaMaisUm))

	pathArqEntrada = os.path.join(pathArquivos, data.strftime('%Y%m%d'), 'entrada')
	pathArqEntradaDiaAnter = os.path.join(pathArquivos, dataRodada.strftime('%Y%m%d'), 'entrada')

	pathArqSaida = os.path.join(pathArquivos, dataRodada.strftime('%Y%m%d'), 'definitiva')
	if not os.path.exists(pathArqSaida):
		os.makedirs(pathArqSaida)

	pathEntdadosOut = os.path.join(pathArqSaida,'entdados.dat')
	fileOut = open(pathEntdadosOut, 'w')
	fileOut.close()

	# entdados do ONS que sera adaptado para rodar sem rede da data referente
	pathEntdadosIn = os.path.join(pathArqEntrada, 'ons_entrada_saida', 'entdados.dat')
	entdados = leituraArquivo(pathEntdadosIn)

	# entdados da CCEE do dia anterior a data referente
	pathEntdadosCceeIn = os.path.join(pathArqEntradaDiaAnter, 'ccee_entrada', 'entdados.dat')
	entdadosCceeAnt = leituraArquivo(pathEntdadosCceeIn)

	infoBlocos = getInfoBlocos()

	# Bloco RD
	bloco_rd = getCabecalhos('RD')
	bloco_rd += getRodapes('RD')
	gravarArquivo(pathEntdadosOut, bloco_rd)

	# Bloco RIVAR
	blocoRIVAR = extrairInfoBloco(entdados, 'RIVAR', infoBlocos['RIVAR']['regex'])
	df_blocoRIVAR = pd.DataFrame(blocoRIVAR, columns=infoBlocos['RIVAR']['campos'])
	bloco_rivar = getCabecalhos('RIVAR')
	bloco_rivar += formatarBloco(df_blocoRIVAR, infoBlocos['RIVAR']['formatacao'])
	gravarArquivo(pathEntdadosOut, bloco_rivar)

	# Bloco TM
	bloco_TM = extrairInfoBloco(entdados, 'TM', infoBlocos['TM']['regex'])
	df_blocoTM = pd.DataFrame(bloco_TM, columns=infoBlocos['TM']['campos'])
	# Desabilitar a rede
	df_blocoTM['rede'] = 0
	bloco_tm = getCabecalhos('TM')
	bloco_tm += formatarBloco(df_blocoTM, infoBlocos['TM']['formatacao'])
	gravarArquivo(pathEntdadosOut, bloco_tm)

	# Bloco SIST
	blocoSIST = extrairInfoBloco(entdados, 'SIST', infoBlocos['SIST']['regex'])
	df_blocoSIST = pd.DataFrame(blocoSIST, columns=infoBlocos['SIST']['campos'])
	bloco_sis = getCabecalhos('SIST')
	bloco_sis += formatarBloco(df_blocoSIST, infoBlocos['SIST']['formatacao'])
	gravarArquivo(pathEntdadosOut, bloco_sis)


	# Bloco REE
	blocoREE = extrairInfoBloco(entdados, 'REE', infoBlocos['REE']['regex'])
	df_blocoREE = pd.DataFrame(blocoREE, columns=infoBlocos['REE']['campos'])
	bloco_ree = getCabecalhos('REE')
	bloco_ree += formatarBloco(df_blocoREE, infoBlocos['REE']['formatacao'])
	gravarArquivo(pathEntdadosOut, bloco_ree)

	# Bloco UH
	blocoUH = extrairInfoBloco(entdados, 'UH', infoBlocos['UH']['regex'])
	df_blocoUH = pd.DataFrame(blocoUH, columns=infoBlocos['UH']['campos'])
	bloco_uh = getCabecalhos('UH')
	bloco_uh += formatarBloco(df_blocoUH, infoBlocos['UH']['formatacao'])
	gravarArquivo(pathEntdadosOut, bloco_uh)

	# Bloco TVIAG
	blocoTVIAG = extrairInfoBloco(entdados, 'TVIAG', infoBlocos['TVIAG']['regex'])
	df_blocoTVIAG = pd.DataFrame(blocoTVIAG, columns=infoBlocos['TVIAG']['campos'])
	bloco_tviag = getCabecalhos('TVIAG')
	bloco_tviag += formatarBloco(df_blocoTVIAG, infoBlocos['TVIAG']['formatacao'])
	gravarArquivo(pathEntdadosOut, bloco_tviag)

	# Bloco UT (geracao)
	blocoUT = extrairInfoBloco(entdados, 'UT', infoBlocos['UT']['regex'])
	df_blocoUT = pd.DataFrame(blocoUT, columns=infoBlocos['UT']['campos'])
	bloco_ut = getCabecalhos('UT')
	bloco_ut += formatarBloco(df_blocoUT, infoBlocos['UT']['formatacao'])
	gravarArquivo(pathEntdadosOut, bloco_ut)
	
	# Bloco UT (restricao)
	blocoUTR = extrairInfoBloco(entdados, '&UT', infoBlocos['&UT']['regex'])
	df_blocoUTR = pd.DataFrame(blocoUTR, columns=infoBlocos['&UT']['campos'])
	bloco_utr = getCabecalhos('UT2')
	bloco_utr += formatarBloco(df_blocoUTR, infoBlocos['&UT']['formatacao'])
	gravarArquivo(pathEntdadosOut, bloco_utr)

	# Bloco USIE
	blocoUSIE = extrairInfoBloco(entdados, 'USIE', infoBlocos['USIE']['regex'])
	df_blocoUSIE = pd.DataFrame(blocoUSIE, columns=infoBlocos['USIE']['campos'])
	bloco_usie = getCabecalhos('USIE')
	bloco_usie += formatarBloco(df_blocoUSIE, infoBlocos['USIE']['formatacao'])
	gravarArquivo(pathEntdadosOut, bloco_usie)

	# Bloco DP
	blocoDP = extrairInfoBloco(entdados, 'DP', infoBlocos['DP']['regex'])
	df_blocoDP = pd.DataFrame(blocoDP, columns=infoBlocos['DP']['campos'])
	# Atualizacao da demanda com o resultado do pdo_sist
	pdoSistPath = os.path.join(pathArqEntrada, 'ons_entrada_saida', 'pdo_sist.dat')
	pmoSist = wx_pdoSist.leituraSist(pdoSistPath)
	pmoSist['sist'] = pmoSist['sist'].str.strip()
	pmoSist['demanda'] = pmoSist['demanda'].astype(float)
	submercados = ['SE', 'S', 'NE', 'N']

	# Demanda do primeiro dia
	demanda = []
	for subm in submercados:
		demanda += list(pmoSist[pmoSist['sist'] == subm]['demanda'].iloc[0:48].values)

	# Demanda dos dias restantes
	for subm in submercados:
		iloc_inicio = 48
		for i, dd in enumerate(df_blocoTM['dd'].unique()):
			if i == 0:
				continue
			iloc_fim = iloc_inicio + df_blocoTM[df_blocoTM['dd'] == dd].shape[0]
			demanda += list(pmoSist[pmoSist['sist'] == subm]['demanda'].iloc[iloc_inicio:iloc_fim].values)
			iloc_inicio = iloc_fim

	demanda += ['0.0']
	df_blocoDP['Demanda'] = demanda
	bloco_dp = getCabecalhos('DP')
	bloco_dp += formatarBloco(df_blocoDP, infoBlocos['DP']['formatacao'])
	gravarArquivo(pathEntdadosOut, bloco_dp)

	# Bloco DE
	blocoDE = extrairInfoBloco(entdados, 'DE', infoBlocos['DE']['regex'])
	df_blocoDE = pd.DataFrame(blocoDE, columns=infoBlocos['DE']['campos'])
	bloco_DE = getCabecalhos('DE')
	bloco_DE += formatarBloco(df_blocoDE, infoBlocos['DE']['formatacao'])
	gravarArquivo(pathEntdadosOut, bloco_DE)

	# Bloco CD
	blocoCD = extrairInfoBloco(entdados, 'CD', infoBlocos['CD']['regex'])
	df_blocoCD = pd.DataFrame(blocoCD, columns=infoBlocos['CD']['campos'])
	bloco_cd = getCabecalhos('CD')
	bloco_cd += formatarBloco(df_blocoCD, infoBlocos['CD']['formatacao'])
	gravarArquivo(pathEntdadosOut, bloco_cd)

	# Bloco PQ
	blocoPQ = extrairInfoBloco(entdados, 'PQ', infoBlocos['PQ']['regex'])
	df_blocoPQ = pd.DataFrame(blocoPQ, columns=infoBlocos['PQ']['campos'])
	bloco_pq = getCabecalhos('PQ')
	bloco_pq += formatarBloco(df_blocoPQ, infoBlocos['PQ']['formatacao'])
	gravarArquivo(pathEntdadosOut, bloco_pq)

	# Bloco IT
	blocoIT = extrairInfoBloco(entdados, 'IT', infoBlocos['IT']['regex'])
	df_blocoIT = pd.DataFrame(blocoIT, columns=infoBlocos['IT']['campos'])
	bloco_it = getCabecalhos('IT')
	bloco_it += formatarBloco(df_blocoIT, infoBlocos['IT']['formatacao'])
	gravarArquivo(pathEntdadosOut, bloco_it)

	# Bloco RI
	blocoRI = extrairInfoBloco(entdados, 'RI', infoBlocos['RI']['regex'])
	df_blocoRI = pd.DataFrame(blocoRI, columns=infoBlocos['RI']['campos'])
	bloco_ri = getCabecalhos('RI')
	bloco_ri += formatarBloco(df_blocoRI, infoBlocos['RI']['formatacao'])
	gravarArquivo(pathEntdadosOut, bloco_ri)

	# Bloco IA
	blocoIA = extrairInfoBloco(entdadosCceeAnt, 'IA', infoBlocos['IA']['regex'])
	df_blocoIA = pd.DataFrame(blocoIA, columns=infoBlocos['IA']['campos'])
	bloco_ia = getCabecalhos('IA')
	bloco_ia += formatarBloco(df_blocoIA, infoBlocos['IA']['formatacao'])
	gravarArquivo(pathEntdadosOut, bloco_ia)

	# Bloco GP
	blocoGP = extrairInfoBloco(entdados, 'GP', infoBlocos['GP']['regex'])
	df_blocoGP = pd.DataFrame(blocoGP, columns=infoBlocos['GP']['campos'])
	bloco_gp = getCabecalhos('GP')
	bloco_gp += formatarBloco(df_blocoGP, infoBlocos['GP']['formatacao'])
	gravarArquivo(pathEntdadosOut, bloco_gp)

	# Bloco NI
	blocoNI = extrairInfoBloco(entdados, 'NI', infoBlocos['NI']['regex'])
	df_blocoNI = pd.DataFrame(blocoNI, columns=infoBlocos['NI']['campos'])
	bloco_ni = getCabecalhos('NI')
	bloco_ni += formatarBloco(df_blocoNI, infoBlocos['NI']['formatacao'])
	gravarArquivo(pathEntdadosOut, bloco_ni)

	# Bloco VE
	blocoVE = extrairInfoBloco(entdados, 'VE', infoBlocos['VE']['regex'])
	df_blocoVE = pd.DataFrame(blocoVE, columns=infoBlocos['VE']['campos'])
	bloco_ve = getCabecalhos('VE')
	bloco_ve += formatarBloco(df_blocoVE, infoBlocos['VE']['formatacao'])
	gravarArquivo(pathEntdadosOut, bloco_ve)

	# Bloco CE e CI
	blocoCECI_ons = extrairInfoBloco(entdados, 'CE_CI', infoBlocos['CE_CI']['regex'])
	df_blocoCECI_ons = pd.DataFrame(blocoCECI_ons, columns=infoBlocos['CE_CI']['campos'])	
	df_blocoCECI_ons['num'] = df_blocoCECI_ons['num'].apply(pd.to_numeric, errors='coerce')

	blocoCECI = extrairInfoBloco(entdadosCceeAnt, 'CE_CI', infoBlocos['CE_CI']['regex'])
	df_blocoCECI = pd.DataFrame(blocoCECI, columns=infoBlocos['CE_CI']['campos'])
	df_blocoCECI['num'] = df_blocoCECI['num'].apply(pd.to_numeric, errors='coerce')

	# Substituicao dos valores dos contratos do dia anterior do deck da ccee com os valores do deck do ONS
	variaveisAtualizar = ['busf', 'diaInicial', 'horaInicial', 'meiaHoraInicial', 'diaFinal', 'horaFinal', 'meiaHoraFinal', 'F', 'linferior', 'lsuperior', 'custo', 'inicial']
	for index, row in df_blocoCECI.iterrows():
		valoresOns = df_blocoCECI_ons[df_blocoCECI_ons['num'] == row['num']]
		for var in variaveisAtualizar:
			df_blocoCECI.loc[df_blocoCECI['num'] == row['num'], var] = valoresOns.iloc[0][var]

	if not mudancasCcee_CE_CE.empty:
		df_blocoCECI['nome'] = df_blocoCECI['nome'].str.strip()
		for index, mudanca in mudancasCcee_CE_CE[mudancasCcee_CE_CE['Ação'] == 'Comentar'].iterrows():
			if mudanca.iloc[1] < data:
				continue
			filtro1 = df_blocoCECI['mnemonico'] == mudanca['mnemonico']
			filtro2 = df_blocoCECI['num'] == mudanca['num']
			filtro3 = df_blocoCECI['nome'] == mudanca['nome']

			df_blocoCECI.drop(df_blocoCECI.loc[filtro1 & filtro2 & filtro3].index.tolist(), inplace=True)
		
		for index, mudanca in mudancasCcee_CE_CE[mudancasCcee_CE_CE['Ação'] == 'Adicionar'].iterrows():
			if mudanca.iloc[1] < data:
				continue
			df_blocoCECI = df_blocoCECI.append(mudanca[2:].fillna(''))

	indexDiaInicial = df_blocoCECI[df_blocoCECI['diaInicial'].str.strip() != 'I'].index
	df_blocoCECI.loc[indexDiaInicial, 'diaInicial'] = df_blocoCECI.loc[indexDiaInicial, 'diaInicial'].astype(int).replace(calendarioAux)
	indexPassado = df_blocoCECI[df_blocoCECI['diaFinal'].str.strip() != 'F']['diaFinal'].astype(int).isin(diasSemanaPassado).index
	
	df_blocoCECI.drop(indexPassado, inplace=True)
	bloco_ceci = getCabecalhos('CE_CI')
	bloco_ceci += formatarBloco(df_blocoCECI, infoBlocos['CE_CI']['formatacao'])
	gravarArquivo(pathEntdadosOut, bloco_ceci)


	# Bloco RESTRICOES ESPECIAIS
	# 'RE', 'LU', 'FH', 'FT', 'FI', 'FE', 'FR', 'FC'
	blocoRE = extrairInfoBloco(entdadosCceeAnt, 'RE', infoBlocos['RE']['regex'])
	df_blocoRE = pd.DataFrame(blocoRE, columns=infoBlocos['RE']['campos'])
	blocoLU = extrairInfoBloco(entdadosCceeAnt, 'LU', infoBlocos['LU']['regex'])
	df_blocoLU = pd.DataFrame(blocoLU, columns=infoBlocos['LU']['campos'])
	blocoFH = extrairInfoBloco(entdadosCceeAnt, 'FH', infoBlocos['FH']['regex'])
	df_blocoFH = pd.DataFrame(blocoFH, columns=infoBlocos['FH']['campos'])
	blocoFT = extrairInfoBloco(entdadosCceeAnt, 'FT', infoBlocos['FT']['regex'])
	df_blocoFT = pd.DataFrame(blocoFT, columns=infoBlocos['FT']['campos'])
	blocoFI = extrairInfoBloco(entdadosCceeAnt, 'FI', infoBlocos['FI']['regex'])
	df_blocoFI = pd.DataFrame(blocoFI, columns=infoBlocos['FI']['campos'])
	blocoFE = extrairInfoBloco(entdadosCceeAnt, 'FE', infoBlocos['FE']['regex'])
	df_blocoFE = pd.DataFrame(blocoFE, columns=infoBlocos['FE']['campos'])
	blocoFR = extrairInfoBloco(entdadosCceeAnt, 'FR', infoBlocos['FR']['regex'])
	df_blocoFR = pd.DataFrame(blocoFR, columns=infoBlocos['FR']['campos'])
	blocoFC = extrairInfoBloco(entdadosCceeAnt, 'FC', infoBlocos['FC']['regex'])
	df_blocoFC = pd.DataFrame(blocoFC, columns=infoBlocos['FC']['campos'])

	if diaAnterior.day in calendarioAux:
		dictReplace = {'{:>2}'.format(diaAnterior.day):calendarioAux[dataRodada.day]}
		df_blocoRE['di'] = df_blocoRE['di'].replace(dictReplace)
		df_blocoFH['di'] = df_blocoFH['di'].replace(dictReplace)
		df_blocoFT['di'] = df_blocoFT['di'].replace(dictReplace)
		df_blocoFI['di'] = df_blocoFI['di'].replace(dictReplace)
		df_blocoFE['di'] = df_blocoFE['di'].replace(dictReplace)
		df_blocoFR['di'] = df_blocoFR['di'].replace(dictReplace)
		df_blocoFC['di'] = df_blocoFC['di'].replace(dictReplace)

		# Condicao especial para 147
		df_blocoLU.loc[df_blocoLU[df_blocoLU['ind'] !=' 147'].index, 'di'] = df_blocoLU.loc[df_blocoLU[df_blocoLU['ind'] !=' 147'].index, 'di'].replace(dictReplace)


	# Restricao especial para o 45
	index_id45 = df_blocoLU[(df_blocoLU['ind']==' 45') & (df_blocoLU['di']==str(dataRodada.day))].index
	df_blocoLU.loc[index_id45, 'di'] = (dataRodada+datetime.timedelta(days=1)).day

	bloco_restricoes = []
	for index, row in df_blocoRE.iterrows():
		ind = row['ind']
		bloco_restricoes += formatarBloco(df_blocoRE[df_blocoRE['ind'] == ind], infoBlocos['RE']['formatacao'])
		bloco_restricoes += formatarBloco(df_blocoLU[df_blocoLU['ind'] == ind], infoBlocos['LU']['formatacao'])
		bloco_restricoes += formatarBloco(df_blocoFH[df_blocoFH['ind'] == ind], infoBlocos['FH']['formatacao'])
		bloco_restricoes += formatarBloco(df_blocoFE[df_blocoFE['ind'] == ind], infoBlocos['FE']['formatacao'])
		bloco_restricoes += formatarBloco(df_blocoFT[df_blocoFT['ind'] == ind], infoBlocos['FT']['formatacao'])
		bloco_restricoes += formatarBloco(df_blocoFR[df_blocoFR['ind'] == ind], infoBlocos['FR']['formatacao'])
		bloco_restricoes += formatarBloco(df_blocoFC[df_blocoFC['ind'] == ind], infoBlocos['FC']['formatacao'])
		bloco_restricoes += formatarBloco(df_blocoFI[df_blocoFI['ind'] == ind], infoBlocos['FI']['formatacao'])


	gravarArquivo(pathEntdadosOut, bloco_restricoes)


	# Bloco AC
	bloco_AC = getCabecalhos('AC')
	bloco_AC += entdados['AC']
	gravarArquivo(pathEntdadosOut, bloco_AC)

	# Bloco DA
	blocoDA = extrairInfoBloco(entdados, 'DA', infoBlocos['DA']['regex'])
	df_blocoDA = pd.DataFrame(blocoDA, columns=infoBlocos['DA']['campos'])
	bloco_DA = getCabecalhos('DA')
	bloco_DA += formatarBloco(df_blocoDA, infoBlocos['DA']['formatacao'])
	bloco_DA += getRodapes('DA')
	gravarArquivo(pathEntdadosOut, bloco_DA)

	# Bloco FP
	blocoFP = extrairInfoBloco(entdados, 'FP', infoBlocos['FP']['regex'])
	df_blocoFP = pd.DataFrame(blocoFP, columns=infoBlocos['FP']['campos'])
	bloco_FP = getCabecalhos('FP')
	bloco_FP += formatarBloco(df_blocoFP, infoBlocos['FP']['formatacao'])
	bloco_FP += getRodapes('FP')
	gravarArquivo(pathEntdadosOut, bloco_FP)

	# Bloco TX
	blocoTX = extrairInfoBloco(entdados, 'TX', infoBlocos['TX']['regex'])
	df_blocoTX = pd.DataFrame(blocoTX, columns=infoBlocos['TX']['campos'])
	bloco_TX = getCabecalhos('TX')
	bloco_TX += formatarBloco(df_blocoTX, infoBlocos['TX']['formatacao'])
	bloco_TX += getRodapes('TX')
	gravarArquivo(pathEntdadosOut, bloco_TX)

	# Bloco EZ
	blocoEZ = extrairInfoBloco(entdados, 'EZ', infoBlocos['EZ']['regex'])
	df_blocoEZ = pd.DataFrame(blocoEZ, columns=infoBlocos['EZ']['campos'])
	bloco_EZ = getCabecalhos('EZ')
	bloco_EZ += formatarBloco(df_blocoEZ, infoBlocos['EZ']['formatacao'])
	bloco_EZ += getRodapes('EZ')
	gravarArquivo(pathEntdadosOut, bloco_EZ)

	# Bloco AG
	blocoAG = extrairInfoBloco(entdados, 'AG', infoBlocos['AG']['regex'])
	df_blocoAG = pd.DataFrame(blocoAG, columns=infoBlocos['AG']['campos'])
	bloco_AG = getCabecalhos('AG')
	bloco_AG += formatarBloco(df_blocoAG, infoBlocos['AG']['formatacao'])
	bloco_AG += getRodapes('AG')
	gravarArquivo(pathEntdadosOut, bloco_AG)

	# Bloco SECR
	blocoSECR = extrairInfoBloco(entdados, 'SECR', infoBlocos['SECR']['regex'])
	df_blocoSECR = pd.DataFrame(blocoSECR, columns=infoBlocos['SECR']['campos'])
	bloco_SECR = getCabecalhos('SECR')
	bloco_SECR += formatarBloco(df_blocoSECR, infoBlocos['SECR']['formatacao'])
	bloco_SECR += getRodapes('SECR')
	gravarArquivo(pathEntdadosOut, bloco_SECR)

	# Bloco MH
	blocoMH = extrairInfoBloco(entdados, 'MH', infoBlocos['MH']['regex'])
	df_blocoMH = pd.DataFrame(blocoMH, columns=infoBlocos['MH']['campos'])
	bloco_MH = getCabecalhos('MH')
	bloco_MH += formatarBloco(df_blocoMH, infoBlocos['MH']['formatacao'])
	bloco_MH += getRodapes('MH')
	gravarArquivo(pathEntdadosOut, bloco_MH)

	# Bloco CR
	blocoCR = extrairInfoBloco(entdados, 'CR', infoBlocos['CR']['regex'])
	df_blocoCR = pd.DataFrame(blocoCR, columns=infoBlocos['CR']['campos'])
	bloco_CR = getCabecalhos('CR')
	bloco_CR += formatarBloco(df_blocoCR, infoBlocos['CR']['formatacao'])
	bloco_CR += getRodapes('CR')
	gravarArquivo(pathEntdadosOut, bloco_CR)

	# Bloco R11
	blocoR11 = extrairInfoBloco(entdados, 'R11', infoBlocos['R11']['regex'])
	df_blocoR11 = pd.DataFrame(blocoR11, columns=infoBlocos['R11']['campos'])
	bloco_R11 = getCabecalhos('R11')
	bloco_R11 += formatarBloco(df_blocoR11, infoBlocos['R11']['formatacao'])
	bloco_R11 += getRodapes('R11')
	gravarArquivo(pathEntdadosOut, bloco_R11)

	# Bloco MT
	# blocoMT = extrairInfoBloco(entdadosCceeAnt, 'MT', infoBlocos['MT']['regex'])
	# df_blocoMT = pd.DataFrame(blocoMT, columns=infoBlocos['MT']['campos'])
	# df_blocoMT['di'] = df_blocoMT['di'].astype(int).replace(calendarioAux)
	# df_blocoMT['df'] = df_blocoMT['df'].astype(int).replace(calendarioAux)
	# bloco_MT = getCabecalhos('MT')
	# bloco_MT += formatarBloco(df_blocoMT, infoBlocos['MT']['formatacao'])
	# bloco_MT += getRodapes('MT')
	# gravarArquivo(pathEntdadosOut, bloco_MT)

	print('entdados.dat: {}'.format(pathEntdadosOut))


def gerarEntdadosAux(data, pathArquivos):

	infoBlocos = getInfoBlocos()
	dataAnterior = data - datetime.timedelta(days=1)

	dataEletrica = wx_opweek.getLastSaturday(data)
	dataEletrica = wx_opweek.ElecData(dataEletrica)

	diasSemana = []
	diasSemanaMenosUm = []
	primeiroDiaSemanaEletrica = dataEletrica.inicioSemana
	for i in range(7):
		dia = primeiroDiaSemanaEletrica + datetime.timedelta(days=i)
		diasSemana.append(dia.day)
		diasSemanaMenosUm.append((dia - datetime.timedelta(days=1)).day)
	calendarioAux = dict(zip(diasSemana,diasSemanaMenosUm))

	pathArqEntradaAnt = os.path.join(pathArquivos, dataAnterior.strftime('%Y%m%d'), 'entrada')
	pathArqEntrada = os.path.join(pathArquivos, data.strftime('%Y%m%d'), 'entrada')

	pathEntdadosIn = os.path.join(pathArqEntradaAnt, 'ccee_entrada', 'entdados.dat')
	pathEntdadosOut = os.path.join(pathArqEntrada, 'ccee_entrada', 'entdados_aux_wx.dat')

	entdadosIn = leituraArquivo(pathEntdadosIn)
	blocoDP_in = extrairInfoBloco(entdadosIn, 'DP', infoBlocos['DP']['regex'])
	df_blocoDP_in = pd.DataFrame(blocoDP_in, columns=infoBlocos['DP']['campos'])
	df_blocoDP_in['di'] = df_blocoDP_in['di'].astype(int)
	indexUltimoDia = df_blocoDP_in[df_blocoDP_in['di'] == diasSemana[-1]].index

	dpFile_aux = leituraArquivo(os.path.join(pathArqEntradaAnt, 'blocos', 'DP.txt'))
	blocoDP_aux = extrairInfoBloco(dpFile_aux, 'DP', infoBlocos['DP']['regex'])
	df_blocoDP_aux = pd.DataFrame(blocoDP_aux, columns=infoBlocos['DP']['campos'])
	df_blocoDP_aux['di'] = df_blocoDP_aux['di'].astype(int)

	df_blocoDP_aux['di'] = df_blocoDP_aux['di'].replace(calendarioAux)
	df_blocoDP_aux = df_blocoDP_aux.append(df_blocoDP_in.loc[indexUltimoDia])

	df_blocoDP_aux = df_blocoDP_aux.sort_values(by=['ss','di','hi'])

	# bloco_DP = getCabecalhos('DP')
	# bloco_DP += formatarBloco(df_blocoDP_aux, infoBlocos['DP']['formatacao'])

	entdadosIn['DP'] = formatarBloco(df_blocoDP_aux, infoBlocos['DP']['formatacao'])

	entdadosOut = []
	for mnemonico in entdadosIn:
		entdadosOut += getCabecalhos(mnemonico)
		entdadosOut += entdadosIn[mnemonico]
		entdadosOut += getRodapes(mnemonico)
		
	gravarArquivo(pathEntdadosOut, entdadosOut)
	print('entdados_aux_wx.dat: {}'.format(pathEntdadosOut))

if __name__ == '__main__':

	diretorioRaiz = os.path.abspath('../../../')
	pathLibUniversal = os.path.join(diretorioRaiz,'bibliotecas')
	sys.path.insert(1, pathLibUniversal)

	import wx_opweek
	import wx_dbLib


	data = datetime.datetime.now()
	data = datetime.datetime(2021,1,29)
	path = os.path.abspath(r'C:\Users\thiag\Documents\git\wx_alpha_\apps\dessem\arquivos')
	# gerarEntdados(data, path)

	gerarEntdadosAux(data-datetime.timedelta(days=1), path)

	# getValoresIniciais(data)