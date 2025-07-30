# -*- coding: utf-8 -*-
import calendar
import time
from datetime import datetime
import json
import pickle
import os.path
from os import path

import locale
# locale.setlocale(locale.LC_ALL, '')

#date is a list with the format [day, month, year]
def timeStampGmt(date):
	d = datetime(date[2], date[1], date[0])
	return calendar.timegm(d.timetuple())

#date is a list with the format [day, month, year]
def timeStampLocal(date):
	d = datetime(date[2], date[1], date[0])
	return int(time.mktime(d.timetuple()))

# from datetime import datetime
def readableDateFromTimeS(tStamp):
	# return datetime.utcfromtimestamp(tStamp).strftime('%Y-%m-%d')
	return datetime.utcfromtimestamp(tStamp).strftime('%m-%d-%Y')

def getYearFromTimeS(tStamp):
	time = datetime.utcfromtimestamp(tStamp)
	return time.year

def getMonthFromTimeS(tStamp):
	time = datetime.utcfromtimestamp(tStamp)
	return time.month

def getDayFromTimeS(tStamp):
	time = datetime.utcfromtimestamp(tStamp)
	return time.day

def convertToDayMonth(tStamp):
	return datetime.utcfromtimestamp(tStamp).strftime('%d/%m')

def getColorModels():
	colorsModel={}
	colorsModel['mlt_complete']='102, 0, 102'	#roxo
	colorsModel['ena_complete']='0, 102, 0'	#verde
	colorsModel['ena_media_complete']='204, 0, 0'	#vermelho
	colorsModel['ena_revisions_complete']='255, 204, 0'	#amarelo
	colorsModel['ena_round_complete']='0, 51, 204'	#azul
	colorsModel['ena_estimations_complete']='184, 115, 51'#marrom

	return colorsModel



def storeInCache(roundN):
	fileName = 'cache'
	cacheStored = loadCache()
	cacheStored.update(roundN)
	
	with open( fileName + '.pkl', 'wb') as f:
		pickle.dump(cacheStored, f, pickle.HIGHEST_PROTOCOL)

def loadCache():
	fileName = 'cache'
	cacheStored = {}
	if path.exists(fileName+".pkl"):
		with open(fileName + '.pkl', 'rb') as f:
			return pickle.load(f)
	else:
		dicEmpty={}
		return dicEmpty


def checkCache(revisionName, rounds):
	cache = loadCache()
	if revisionName in list(cache.keys()):
		roundsInCache = (list(cache[revisionName]['SUDESTE'].keys()))
		if roundsInCache == rounds:
			return cache[revisionName]
		else:
			return 0


def organizar(tableResults):
	dic1 = {}
	for roundX in tableResults.keys():
		enaNorte = round(float(tableResults[roundX]['NORTE']['ena']),1)

		# ENAs com ate 1% de diferenca serao agrupadas (somente no NORTE)
		if dic1:
			if enaNorte not in dic1:
				flag_insert = 1
				for ii in list(dic1.keys()).copy():
					if (ii-1 <= enaNorte  and enaNorte <= ii+1):
						enaNorte = ii
						flag_insert = 0
						
				if flag_insert:
					dic1[enaNorte]={}
		else:
			dic1[enaNorte]={}

		enaNordeste = round(float(tableResults[roundX]['NORDESTE']['ena']),1)
		if enaNordeste not in dic1[enaNorte]:
			dic1[enaNorte][enaNordeste]={}

		enaSul = round(float(tableResults[roundX]['SUL']['ena']),1)
		if enaSul not in dic1[enaNorte][enaNordeste]:
			dic1[enaNorte][enaNordeste][enaSul]={}

		enaSudeste = round(float(tableResults[roundX]['SUDESTE']['ena']),1)
		dic1[enaNorte][enaNordeste][enaSul][enaSudeste]=[roundX, tableResults[roundX]['SUDESTE']['cmo'], tableResults[roundX]['SUDESTE']['first_week']['ena'][0], tableResults[roundX]['SUL']['first_week']['ena'][0]]

	matriz = {}
	enaSudesteIndex={}
	for enaNorte in sorted(dic1.keys()):
		matriz[enaNorte] = {}
		for enaNordeste in sorted(dic1[enaNorte].keys()):
			matriz[enaNorte][enaNordeste] = {'aux':{'enaPrimeiraSemanaSul':[], 'enaPrimeiraSemanaSudeste':[]} }
			for enaSul in sorted(dic1[enaNorte][enaNordeste].keys()):
				matriz[enaNorte][enaNordeste][enaSul] = {}
				for enaSudeste in sorted(dic1[enaNorte][enaNordeste][enaSul].keys()):
					matriz[enaNorte][enaNordeste][enaSul][enaSudeste] = dic1[enaNorte][enaNordeste][enaSul][enaSudeste]

					if format(dic1[enaNorte][enaNordeste][enaSul][enaSudeste][2], ',d').replace(",", ".") not in matriz[enaNorte][enaNordeste]['aux']['enaPrimeiraSemanaSudeste']:
						matriz[enaNorte][enaNordeste]['aux']['enaPrimeiraSemanaSudeste'].append(format(dic1[enaNorte][enaNordeste][enaSul][enaSudeste][2], ',d').replace(",", "."))

					if format(dic1[enaNorte][enaNordeste][enaSul][enaSudeste][3], ',d').replace(",", ".") not in matriz[enaNorte][enaNordeste]['aux']['enaPrimeiraSemanaSul']:
						matriz[enaNorte][enaNordeste]['aux']['enaPrimeiraSemanaSul'].append(format(dic1[enaNorte][enaNordeste][enaSul][enaSudeste][3], ',d').replace(",", "."))
				matriz[enaNorte][enaNordeste]['aux']['enaPercentSudeste'] = list(matriz[enaNorte][enaNordeste][enaSul].keys())
	return matriz

def formatar_data(data: str):
    data = data.replace('/', '-')
    formatos = ['%d-%m-%Y', '%Y-%m-%d', '%Y-%m-%dT%H:%M']
    
    for formato in formatos:
        try:
            return datetime.strptime(data, formato)
        except ValueError:
            pass
    
    raise ValueError(f'Formato de data {data} inválido')

def filtrar_dict_por_mes(dicionario:dict, mes_inicio:int, mes_fim:int):
	filtrado:dict = dict()
	for key in dicionario:
		if datetime.strptime(key, "%Y-%m-%d").month >= mes_inicio and datetime.strptime(key, "%Y-%m-%d").month <= mes_fim:
			filtrado.update({key: dicionario[key]})
	return filtrado

def subsistema_para_sigla(subsistema:str):
	subsistemas = {'norte': 'n', 'nordeste': 'ne', 'sul': 's', 'sudeste': 'se'}
	if subsistemas[subsistema.lower()]:
		return subsistemas[subsistema.lower()]
	return None

def str_to_bool(s:str):
    return s in ['True', 'true', 1, '1']

def list_param(s:str):
    return s.split(',')

def date_list_param(s:str):
    params = list_param(s)
    dates = []
    for date in params:
        dates.append(formatar_data(date))
    return dates
if __name__ == "__main__":
	# time1 = timeStampGmt([16, 7, 2019])
	# time2 = timeStampLocal([16, 7, 2019])

	# print(readableDateFromTimeS(time1))
	# print(readableDateFromTimeS(time2))
	# data = {'Python' : '.py', 'C++' : '.cpp', 'Java' : '.java'}
	# writeInCache(data)

	# print(checkInCache('Python'))
	pass




