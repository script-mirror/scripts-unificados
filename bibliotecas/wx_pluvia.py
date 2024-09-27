# -*- coding: utf-8 -*-
"""
Created on Jun 2020 by Norus
"""

import os
import time

import json
import requests

# -----------------------------------------------------------------------------
# Global variables | Variáveis globais
# -----------------------------------------------------------------------------
global token

basicURL ='https://pluvia.app'

verifyCertificate = True
username = ''
password = ''

# -----------------------------------------------------------------------------
# Get token | Obter token
# -----------------------------------------------------------------------------

def getToken(username_temp, password_temp):
    url = basicURL + '/api/token'

    global username
    username = username_temp
    global password
    password = password_temp

    headers = {
        'content-type': 'application/x-www-form-urlencoded'
    }

    data = {
        'grant_type': 'password',
        'username': username,
        'password': password
    }

    tokenResponse = requests.post(url, headers=headers, data=data,
                                  verify=verifyCertificate)
    token_json = tokenResponse.json()
    token = token_json["access_token"]

    return token

# -----------------------------------------------------------------------------
# Get JSON from REST API | Obter JSON via REST API
# -----------------------------------------------------------------------------


def getInfoFromAPI(*args):

    if len(args) < 2:
        print('Sao necessarios ao menos dois argumentos: token e apiFunction')
        return ''
    elif len(args) == 2:
        token = args[0]
        apiFunction = args[1]
    elif len(args) == 3:
        token = args[0]
        apiFunction = args[1]
        params = args[2]
    else:
        print('Sao necessarios ao menos dois argumentos: token, apiFunction e'
              ' parametros')
        return ''

    # Specify URL | Especificar URL
    url = basicURL + apiFunction

    headers = {
        'Authorization': 'Bearer ' + token,
        "Content-Type": "application/json"
    }

    # Call REST API | Chamar Rest API
    if 'params' in locals():
        response = requests.get(url, headers=headers, params=params,
                                verify=verifyCertificate)
    else:
        response = requests.get(url, headers=headers, verify=verifyCertificate)

    #print(response.status_code)

    if (response.status_code == 401):
        token = getToken(username, password)

        headers = {
            'Authorization': 'Bearer ' + token,
            "Content-Type": "application/json"
        }

        if 'params' in locals():
            response = requests.get(url, headers=headers, params=params,
                                    verify=verifyCertificate)
        else:
            response = requests.get(url, headers=headers,
                                    verify=verifyCertificate)

        #print(response.status_code)

    if (response.status_code == 200):
        return response.json()
    return ''

# -----------------------------------------------------------------------------
# Get file from REST API | Obter arquivo via REST API
# -----------------------------------------------------------------------------


def getFileFromAPI(*args):

    if len(args) < 3:
        print('Sao necessarios ao menos tres argumentos: token e apiFunction')
        return ''
    elif len(args) == 3:
        token = args[0]
        apiFunction = args[1]
        fileName = args[2]
        pathToDownload = ''
    elif len(args) == 4:
        token = args[0]
        apiFunction = args[1]
        fileName = args[2]
        pathToDownload = args[3]
    else:
        print('Sao aceitos no máximo quatro argumentos: token, apiFunction e'
              ' parametros')
        return ''

    # Specify URL | Especificar URL
    url = basicURL + apiFunction

    headers = {
        'Authorization': 'Bearer ' + token,
        "Content-Type": "application/json"
    }

    # Call REST API | Chamar REST API
    response = requests.get(url, headers=headers, stream=True,
                            verify=verifyCertificate)

    print(response.status_code)
    #print(response.text)

    if (response.status_code == 401):
        token = getToken(username, password)

        headers = {
            'Authorization': 'Bearer ' + token,
            "Content-Type": "application/json"
        }

        response = requests.get(url, headers=headers, stream=True,
                                verify=verifyCertificate)

        print(response.status_code)

    if (response.status_code == 200):
        try:
            with open(os.path.join(pathToDownload, fileName), 'wb') as file:
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:  # filter out keep-alive new chunks
                        file.write(chunk)
        except:
            print('Não foi possível salvar o arquivo', os.path.join(pathToDownload, fileName))

    return ''


# -----------------------------------------------------------------------------
# Get token | Obter token
# -----------------------------------------------------------------------------


def authenticatePluvia(username, password):
    global token
    token = getToken(username, password)

# -----------------------------------------------------------------------------
# Get list of Precitation Data Source | Obter lista de Fonte de Dados de Precipitação
# -----------------------------------------------------------------------------


def getIdsOfPrecipitationsDataSource():
    return getInfoFromAPI(token, '/api/valoresParametros/mapas')


def getIdOfPrecipitationDataSource(precipitationDataSource):
    "Possible values: MERGE, ETA, GEFS, CFS, ONS NT0156, Usuário, Prec. Zero, ONS, ONS SOMBRA, ECMWF_ENS or ECMWF_ENS_EXT"
    return next(item for item in getIdsOfPrecipitationsDataSource() if item["descricao"] == precipitationDataSource)['id']


# -----------------------------------------------------------------------------
# Get list of Forecast Modelos | Obter lista dos Modelos de Previsão
# -----------------------------------------------------------------------------


def getIdsOfForecastModels():
    return getInfoFromAPI(token, '/api/valoresParametros/modelos')

def getIdOfForecastModel(forecastModel):
    "Possible values: IA, IA+SMAP or SMAP "
    return next(item for item in getIdsOfForecastModels() if item["descricao"] == forecastModel)['id']

# -----------------------------------------------------------------------------
# Get list of Precitation Data Source | Obter lista de Fonte de Dados de Precipitação
# -----------------------------------------------------------------------------


def getForecasts(forecastDate, forecastSources, forecastModels, bias, preliminary, years, members):
    "forecastDate mandatory"

    if forecastDate == '':
        print('Data de Previsão não pode ser nula')
        return []

    params = "dataPrevisao=" + forecastDate
   
    for forecastSource in forecastSources:
        params += "&mapas=" + str(forecastSource)
    
    for forecastModel in forecastModels:
        params += "&modelos=" + str(forecastModel)
    
    if bias != '':
        params += "&semVies=" + str.lower(bias)
    
    if preliminary != '':
        params += "&preliminar=" + str.lower(preliminary)
    
    for year in years:
        params += "&anos=" + str(year)

    for member in members:
        params += "&membros=" + str(member)

    return getInfoFromAPI(token, '/api/previsoes?' + params)

def downloadForecast(idForecast, pathToDownload, fileName):
    response = getFileFromAPI(token, '/api/resultados/' + str(idForecast), fileName, pathToDownload)