# -*- coding: utf-8 -*-
"""
Created on Jun 2020 by Norus
"""

import sys
sys.path.insert(1,"/WX2TB/Documentos/fontes")
from PMO.scripts_unificados.apps.pluvia.libs.requestsPluviaAPI import getInfoFromAPI, getFileFromAPI


# -----------------------------------------------------------------------------
# Get list of Modes | Obter lista de Fonte de Dados sobre o Modo dos modelos
# -----------------------------------------------------------------------------

def getIdsOfModes():
    return getInfoFromAPI('/v2/valoresParametros/modos')

def getIdOfModes(modes):
    "Possible values: Diário, Mensal"
    return next(item for item in getIdsOfModes() if item["descricao"] == modes)['id']

# -----------------------------------------------------------------------------
# Get list of Precitation Data Source | Obter lista de Fonte de Dados de Precipitação
# -----------------------------------------------------------------------------

def getIdsOfPrecipitationsDataSource():
    return getInfoFromAPI('/v2/valoresParametros/mapas')


def getIdOfPrecipitationDataSource(precipitationDataSource):
    "Possible values: MERGE,ETA,GEFS,CFS,Usuário,Prec. Zero,ECMWF_ENS,ECMWF_ENS_EXT,ONS,ONS_Pluvia,ONS_ETAd_1_Pluvia,GEFS_EXT"
    return next(item for item in getIdsOfPrecipitationsDataSource() if item["descricao"] == precipitationDataSource)['id']

# -----------------------------------------------------------------------------
# Get list of Forecast Modelos | Obter lista dos Modelos de Previsão
# -----------------------------------------------------------------------------

def getIdsOfForecastModels():
    return getInfoFromAPI('/v2/valoresParametros/modelos')

def getIdOfForecastModel(forecastModel):
    "Possible values: IA, IA+SMAP or SMAP "
    return next(item for item in getIdsOfForecastModels() if item["descricao"] == forecastModel)['id']

# -----------------------------------------------------------------------------
# Get list of Precitation Data Source | Obter lista de Fonte de Dados de Precipitação
# -----------------------------------------------------------------------------

def getForecasts(forecastDate, forecastSources, forecastModels, bias, preliminary, modes, years, members):
    "forecastDate mandatory"

    if forecastDate == '':
        print('Data de Previsão não pode ser nula')
        return []

    params = "dataPrevisao=" + forecastDate
   
    for forecastSource in forecastSources:
        params += "&Mapas=" + str(forecastSource)
    
    for forecastModel in forecastModels:
        params += "&Modelos=" + str(forecastModel)
    
    for mode in modes:
        params += "&Modos=" + str(mode)
        
    if bias != '':
        params += "&Vies=" + str.lower(bias)
    
    if preliminary != '':
        params += "&Rodada=" + str.lower(preliminary)
    
    for year in years:
        params += "&Anos=" + str(year)
        
    for member in members:
        params += "&Membros=" + str(member)
    
    return getInfoFromAPI('/v2/previsoes?' + params)

def downloadForecast(idForecast, pathToDownload, fileName):
    getFileFromAPI('/v2/resultados/' + str(idForecast), fileName, pathToDownload)
