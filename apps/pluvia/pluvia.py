# -*- coding: utf-8 -*-
import os
import sys
import glob
import time
import shutil
import zipfile
import datetime
import pdb

import pandas as pd
from unidecode import unidecode

sys.path.insert(1,"/WX2TB/Documentos/fontes")
from PMO.scripts_unificados.apps.pluvia.libs import functionsPluviaAPI,requestsPluviaAPI
from PMO.scripts_unificados.bibliotecas import wx_dbLib
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")),'.env'))

USER_PLUVIA = os.getenv('USER_PLUVIA') 
PASSWORD_PLUVIA = os.getenv('PASSWORD_PLUVIA')

pathResultados = os.path.abspath('./arquivos/')
pathPrevisaoDia = ''

precipitationDataSources = {
                                "MERGE": 0,
                                "ETA": 1,
                                "GEFS": 2,
                                "CFS": 3,
                                "suário": 5,
                                "Prec. Zero": 7,
                                "ECMWF_ENS": 9,
                                "ECMWF_ENS_EXT": 10,
                                "ONS": 11,
                                "ONS_Pluvia": 14,
                                "ONS_ETAd_1_Pluvia": 15,
                                "GEFS_EXT": 17
                            }
forecastModels = {
                    "IA":1,
                    "IA+SMAP":2,
                    "SMAP":3
                }



def baixarArquivos(produtos, dataPrevisao, mapas, modelos, flagVies, flagPreliminar, anosInteresse=[]):
    global pathResultados, pathPrevisaoDia

    # -----------------------------------------------------------------------------
    # Criação                
    # -----------------------------------------------------------------------------
    requestsPluviaAPI.authenticatePluvia(USER_PLUVIA, PASSWORD_PLUVIA)
    if anosInteresse == []:
        anosInteresse = [dataPrevisao.year]
    membros = [] # ENSEMBLE / 01 / 02 / etc
    idModo = []

    if not os.path.exists(pathResultados):
        try:
            os.makedirs(pathResultados)
        except:
            print('Falha em criar pasta de resultados')
            time.sleep(1)
            try:
                os.makedirs(pathResultados)
            except:
                print('Nova falha em criar pasta de resultados')
                return(1)

    idPreciptacaoMapa = []
    idPrevisao = []
    
    for preciptacaoMapa in mapas:
        idPreciptacaoMapa.append(precipitationDataSources[preciptacaoMapa])

    for modeloPrevisao in modelos:    
        idPrevisao.append(forecastModels[modeloPrevisao])
    
    for i in range(15):
        print('Tentativa #{} ({})'.format(i+1, datetime.datetime.now().strftime('%H:%M')))
        print('idPreciptacaoMapa: {}'.format(idPreciptacaoMapa))
        print('idPrevisao: {}'.format(idPrevisao))
        previsoes = functionsPluviaAPI.getForecasts(dataPrevisao.strftime('%d/%m/%Y'), idPreciptacaoMapa, idPrevisao, flagVies, flagPreliminar, idModo, anosInteresse, membros)
        if len(previsoes):
            break
        else:
            print('Nenhuma rodada encontrada, nova tentativa em 15 minutos\n')
            time.sleep(15*60)
            requestsPluviaAPI.authenticatePluvia(USER_PLUVIA, PASSWORD_PLUVIA)


    for forecast in previsoes:
        
        if not os.path.exists(pathPrevisaoDia):
            try:
                os.makedirs(pathPrevisaoDia)
            except:
                print('Falha em criar a pasta para salvar as previsões do dia')
                time.sleep(1)
                try:
                    os.makedirs(pathPrevisaoDia)
                except:
                    print('Falha em criar a pasta para salvar as previsões do dia')
                    return(1)

        if 'ENA' in produtos:
            functionsPluviaAPI.downloadForecast(forecast['enaId'], pathPrevisaoDia, forecast['nome'] + ' - ' + forecast['membro'] + '- ENA.zip')
        if 'PREVS' in produtos:
            functionsPluviaAPI.downloadForecast(forecast['prevsId'], pathPrevisaoDia, forecast['nome'] + ' - ' + forecast['membro'] + ' - Prevs.zip')
        if 'VNA' in produtos:
            functionsPluviaAPI.downloadForecast(forecast['vnaId'], pathPrevisaoDia, forecast['nome'] + ' - ' + forecast['membro'] + '- VNA.csv')
        if 'STR' in produtos:
            functionsPluviaAPI.downloadForecast(forecast['strId'], pathPrevisaoDia, forecast['nome'] + ' - ' + forecast['membro'] + '- STR.zip')
    
    print('Ok')
    return previsoes

def atualizacaoDiaria(produtos, dataPrevisao, flagVies, flagPreliminar, anosInteresse = []):
    global pathResultados, pathPrevisaoDia

    requestsPluviaAPI.authenticatePluvia(USER_PLUVIA, PASSWORD_PLUVIA)

    if not os.path.exists(pathResultados):
        try:
            os.makedirs(pathResultados)
        except:
            print('Falha em criar pasta de resultados')
            time.sleep(1)
            try:
                os.makedirs(pathResultados)
            except:
                print('Nova falha em criar pasta de resultados')
                return(1)

    if not os.path.exists(pathPrevisaoDia):
        try:
            os.makedirs(pathPrevisaoDia)
        except:
            print('Falha em criar a pasta para salvar as previsões do dia')
            time.sleep(1)
            try:
                os.makedirs(pathPrevisaoDia)
            except:
                print('Falha em criar a pasta para salvar as previsões do dia')
                return(1)

    mapasInteresse = ['ONS_Pluvia', 'Prec. Zero', 'GEFS', 'ECMWF_ENS']
    mapasOnlyIa = []

    loop = 0
    while loop <= 15:
        loop += 1
        print('\nTentativa #{} ({})'.format(loop, datetime.datetime.now().strftime('%H:%M')))
        print('Mapas: {}'.format(mapasInteresse))
        previsoes = functionsPluviaAPI.getForecasts(dataPrevisao.strftime('%d/%m/%Y'), [], [], '', '', [], '', '')
        
        mapasImportadosNoLoop = 0
        if len(previsoes) > 0:

            df_previsoes = pd.DataFrame(previsoes, dtype='object')

            if flagPreliminar != '':
                if flagPreliminar == 'True':
                    df_previsoes = df_previsoes[df_previsoes['rodada'] == 'preliminar']
                else:
                    df_previsoes = df_previsoes[df_previsoes['rodada'] == 'definitiva']

            mapasInteresseAux = mapasInteresse.copy()
            for mapa in mapasInteresseAux:

                previsoesInteresse = df_previsoes[(df_previsoes['mapa']  == mapa)]

                numeroRodadasIaSmap = previsoesInteresse[(previsoesInteresse['modelo']  == 'SMAP')].shape[0]
                if numeroRodadasIaSmap > 0:
                    previsoesInteresse = previsoesInteresse[(previsoesInteresse['modelo']  == 'SMAP')]
                elif mapa not in mapasOnlyIa:
                    previsoesInteresse = previsoesInteresse[(previsoesInteresse['modelo']  == 'IA')]
                    mapasOnlyIa.append(mapa)
                else:
                    continue
                
                # Condicoes especificas para os modelo
                if mapa in ['Prec. Zero', 'GEFS']:
                    previsoesInteresse = previsoesInteresse[(previsoesInteresse['vies']  == 'original')]
                else:
                    previsoesInteresse = previsoesInteresse[(previsoesInteresse['vies']  == 'comRemocaoVies')]
                
                if mapa == 'Prec. Zero':
                    previsoesInteresse = previsoesInteresse[previsoesInteresse['nome'].str.contains('PrecZero_60')]

                if len(previsoesInteresse) > 0 :
                    forecast = previsoesInteresse.iloc[-1].to_dict()
                    if forecast['modelo'] == 'SMAP':
                        mapasInteresse.remove(mapa)

                    # Apos baixar o 'ONS_Pluvia' e 'Prec. Zero' encerra o loop
                    if mapa == 'ONS_Pluvia' and 'Prec. Zero' not in mapasInteresse:
                        mapasInteresse = []
                    elif mapa == 'Prec. Zero' and 'ONS_Pluvia' not in mapasInteresse:
                        mapasInteresse = []

                    if forecast != {}:
                        
                        nome_arquivo = forecast['mapa']
                        
                        if forecast['rodada'] == 'preliminar':
                            nome_arquivo = f"{nome_arquivo}-Preliminar"
                        
                        if forecast['mapa'] == 'ECMWF_ENS':
                            if forecast['vies'] == 'comRemocaoVies':
                                nome_arquivo = f"{nome_arquivo}-SemVies"
                        
                        if forecast['modelo'] == 'IA':
                            nome_arquivo = f"{nome_arquivo}-IA"
                        else:
                            nome_arquivo = f"{nome_arquivo}-IA+SMAP"
                            
                        if forecast['membro'] == '':
                            nome_arquivo = f"{nome_arquivo} - NULO"
                        else:
                            nome_arquivo = f"{nome_arquivo} - {forecast['membro']}"
                            
                        for result in forecast['resultados']:
                            if result['nome'] in ['ENA', 'VNA']:
                                nome_arquivo_zip = f"{nome_arquivo}- {result['nome']}.zip"
                                caminho_completo_arquivo_zip = os.path.join(pathPrevisaoDia, nome_arquivo_zip)
                                
                                functionsPluviaAPI.downloadForecast(result['id'], pathPrevisaoDia, nome_arquivo_zip)

                                organizarArquivos(caminho_completo_arquivo_zip)
                                
                                if result['nome'] == 'ENA':
                                    importarPluviaEna(caminho_completo_arquivo_zip.replace('.zip',''), forecast)
                                elif result['nome'] == 'VNA':
                                    importarPluviaVna(caminho_completo_arquivo_zip.replace('.zip',''), forecast)
                                            
                                            
                                # importarArquivos(caminho_completo_arquivo_zip.replace('.zip','') ,forecast)
                                mapasImportadosNoLoop += 1

                    if mapasInteresse == []:
                        loop = 16
                        break

            if mapasImportadosNoLoop == 0:
                print('Nenhuma rodada encontrada, nova tentativa em 15 minutos\n')
                time.sleep(15*60)
                requestsPluviaAPI.authenticatePluvia(USER_PLUVIA, PASSWORD_PLUVIA)

    print('Ok')
    return previsoes


def organizarArquivos(zipf):

    pathDst = os.path.splitext(zipf)[0]

    if os.path.exists(pathDst):
        shutil.rmtree(pathDst)

    with zipfile.ZipFile(zipf, 'r') as zip_ref:
        zip_ref.extractall(pathDst)

    os.remove(zipf)

def importarPluviaVna(folderPath, descricao):

    filePath = glob.glob(os.path.join(folderPath, '*-VNA.csv'))
    open(filePath[0])
    with open(filePath[0], 'r', encoding="utf8") as file:
        vna = file.readlines()
        for i_linha, linha in enumerate(vna):
            if linha == '### Resultados em Formato de Base de Dados\n':
                inicio_dados = i_linha + 1
                break

    df_vna = pd.read_csv(filePath[0], skiprows=inicio_dados, delimiter=';', decimal=',', engine='python')
    df_vna['Data'] = pd.to_datetime(df_vna['Data'], format="%d/%m/%Y")

    for result in  descricao['resultados']:
        if result['nome'] == 'VNA':
            idPrevisao = result['id']
            break
        
    strdata = datetime.datetime.strptime(descricao['dataPrevisao'], '%d/%m/%Y')
    strMapa = descricao['mapa']
    if descricao['membro'] == '':
        vlMembro = 'NULO'
    else:
        vlMembro = descricao['membro']
        
    if descricao['modelo'] == 'IA':
        strModelo = "IA"
    else:
        strModelo = "IA+SMAP"

    flagVies = 0
    if descricao['vies'] == 'comRemocaoVies':
        flagVies = 1

    flagPreliminar = 0
    if descricao['rodada'] == 'preliminar':
        flagPreliminar = 1

    dbase = wx_dbLib.WxDataB(DBM='mysqlWx') #mysqlWx
    dbase.changeDatabase('db_pluvia')
    dataFormat = dbase.getDateFormat()

    print("\nImportando VNC do pluvia.\nID: {}\nModelo: {}:\nMembro: {}".format(idPrevisao, strMapa, vlMembro))
    sql = "DELETE FROM tb_pluvia_vna WHERE ID_RODADA={}".format(idPrevisao)
    numRows = dbase.requestServer(sql)
    print('{} linhas deletadas!'.format(numRows, idPrevisao))


    sql = 'INSERT INTO tb_pluvia_vna (ID_RODADA, CD_POSTO, VL_VAZAO, DT_REFERENTE, STR_MAPA, DT_RODADA, VL_MEMBRO, FL_VIES, FL_PRELIMINAR, STR_MODELO) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
    val = []

    for index, row in df_vna.iterrows():
        try:
            val.append((idPrevisao, int(row['Posto']), float(row['VNA_m3/s']), row['Data'].strftime(dataFormat), strMapa, strdata, vlMembro, flagVies, flagPreliminar, strModelo))
        except:
            # Erro para tratar valores em notacao cientifica com virgula (EX '3,56666E-3') que nao foram convertida
            val.append((idPrevisao, int(row['Posto']), float(row['VNA_m3/s'].replace(',', '.')), row['Data'].strftime(dataFormat), strMapa, strdata, vlMembro, flagVies, flagPreliminar, strModelo))

    numRows = dbase.executemany(sql, val)
    print('{} linhas inseridas!'.format(numRows))


def getCodBacias():

    dbase = wx_dbLib.WxDataB(DBM='mysqlWx')

    sql = '''SELECT
                cd_bacia,
                str_bacia,
                cd_submercado
            FROM
                db_ons.tb_bacias_segmentadas;'''

    answer = dbase.requestServer(sql)
    df_bacias = pd.DataFrame(answer)
    df_bacias.columns = ['id', 'nome', 'cd_submercado']
    df_bacias['nome'] = df_bacias['nome'].apply(unidecode)

    return dict(zip(df_bacias['nome'], df_bacias['id']))

def importarPluviaEna(folderFiles, descricao):

    codBacias = getCodBacias()
    
    for result in  descricao['resultados']:
        if result['nome'] == 'ENA':
            idPrevisao = result['id']
            break
    
    strdata = datetime.datetime.strptime(descricao['dataPrevisao'], '%d/%m/%Y')
    strMapa = descricao['mapa']
    
    if descricao['membro'] == '':
        vlMembro = 'NULO'
    else:
        vlMembro = descricao['membro']
        
    if descricao['modelo'] == 'IA':
        strModelo = "IA"
    else:
        strModelo = "IA+SMAP"
        
    flagVies = 0
    if descricao['vies'] == 'comRemocaoVies':
        flagVies = 1

    flagPreliminar = 0
    if descricao['rodada'] == 'preliminar':
        flagPreliminar = 1

    filePath = glob.glob(os.path.join(folderFiles, '*-ENA.csv'))
    df_ena = pd.read_csv(filePath[0], skiprows=60, sep = '[;]', decimal=',',engine='python')

    indexTipoBacia = df_ena[df_ena['Tipo'] != 'Bacia'].index
    df_ena.drop(indexTipoBacia ,inplace=True)

    indexDataMedia = df_ena[df_ena['Data'] == 'MEDIA'].index
    df_ena.drop(indexDataMedia ,inplace=True)

    indexSin = df_ena[df_ena['Nome'] == 'SIN'].index
    df_ena.drop(indexSin ,inplace=True)

    indexSantaMariaVitoria = df_ena[df_ena['Nome'] == 'SANTA MARIA DA VITORIA'].index
    df_ena.drop(indexSantaMariaVitoria ,inplace=True)

    df_ena['Data'] = pd.to_datetime(df_ena['Data'], format="%d/%m/%Y")

    # deixando no padrao do banco com a data do inicio da vazao
    df_ena['Data'] = df_ena['Data'] - datetime.timedelta(days=1)

    df_ena['Nome'] = df_ena['Nome'].replace('PARANAPANEMA (SE)', 'PARANAPANEMA')
    df_ena['Nome'] = df_ena['Nome'].map(codBacias)

    dbase = wx_dbLib.WxDataB(DBM='mysqlWx') #mysqlWx
    dbase.changeDatabase('db_pluvia')
    dataFormat = dbase.getDateFormat()
    strdata = strdata.strftime(dataFormat)

    print("\nImportando ENA do pluvia.\nID: {}\nModelo: {}:\nMembro: {}".format(idPrevisao, strMapa, vlMembro))
    sql = "DELETE FROM tb_pluvia_ena WHERE ID_RODADA={}".format(idPrevisao)
    numRows = dbase.requestServer(sql)
    print('{} linhas deletadas!'.format(numRows, idPrevisao))

    sql = "INSERT INTO tb_pluvia_ena (ID_RODADA, CD_BACIA, VL_ENA, VL_ENA_PERC_MLT, DT_REFERENTE, STR_MAPA, DT_RODADA, VL_MEMBRO, FL_VIES, FL_PRELIMINAR, STR_MODELO) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
    val = []

    for index, row in df_ena.iterrows():
        val.append((idPrevisao, row['Nome'], row['ENA_MWmes'], row['ENA_Percentual_MLT'], row['Data'].strftime(dataFormat), strMapa, strdata, vlMembro, flagVies, flagPreliminar, strModelo))

    numRows = dbase.executemany(sql, val)
    print('{} linhas inseridas!'.format(numRows))


def parametroBoolean(param):
    if param in ['0', 0]:
        param = 'False'
    elif param in ['1', 1]:
        param = 'True'
    return param.lower().capitalize()


def printHelper():
    """ Imprime na tela o helper da funcao 
    :param None: 
    :return None: 
    """
    dt_hoje = datetime.datetime.now()
    helper = "Como executar o script:\n"
    helper += "python {} baixarImportarModelos data {} mapas \"['ONS','ONS_Pluvia','ECMWF_ENS','GEFS']\" modelos \"['IA+SMAP','IA']\" preliminar 0 vies 1\n".format(sys.argv[0], dt_hoje.strftime('%d/%m/%Y'))
    helper += "python {} atualizacaoDiaria data {} preliminar 0 vies 1\n".format(sys.argv[0], dt_hoje.strftime('%d/%m/%Y'))

    print(helper)

def runWithParams():
    global pathResultados, pathPrevisaoDia
    # global pathPrevisaoDia

    parametros = {}
    if len(sys.argv) > 1:
        # passar o print do help pra quando nao entrar com nenhum parametro
        if 'help' in sys.argv or '--h' in sys.argv or '-help' in sys.argv:
            printHelper()
            quit()

        produto = ['ENA']
        dataPrevisao = datetime.datetime.now()
        mapas = []
        modelos = []
        flagPreliminar = ''
        flagVies = ''

        # Verificacao e substituicao dos valores defaults com 
        # os valores passados por parametro 
        for i in range(1, len(sys.argv[1:])):
            argumento = sys.argv[i].lower()
            if argumento == 'data':
                try:
                    dataPrevisao = datetime.datetime.strptime(sys.argv[i+1], '%d/%m/%Y')
                except:
                    print("Erro ao tentar converter '{}' para data.\nFavor entrar com o formato 'dd/mm/yyyy'".format(sys.argv[i+1]))
            if argumento == 'mapas':
                try:
                    mapas = eval(sys.argv[i+1])
                except:
                    print("Erro ao identificar o parametro '{}', valor inserido: '{}'.\nO parametro {} deve obdecer o seguinte formato: ['ONS','ONS_Pluvia',...,'GEFS']".format(argumento, sys.argv[i+1], argumento))
                    quit()
            if argumento == 'modelos':
                try:
                    modelos = eval(sys.argv[i+1])
                except:
                    print("Erro ao identificar o parametro '{}', valor inserido: '{}'.\nO parametro {} deve obdecer o seguinte formato: ['ONS','ONS_Pluvia',...,'GEFS']".format(argumento, sys.argv[i+1], argumento))
                    quit()
            if argumento == 'preliminar':
                flagPreliminar = parametroBoolean(sys.argv[i+1])
                if flagPreliminar not in ['True', 'False']:
                    print("Erro ao identificar o parametro '{}'. Possiveis valors: 0 ou 1\nEncontrado {}".format(argumento, parametros[argumento]))
                    quit()
            if argumento == 'vies':
                flagVies = parametroBoolean(sys.argv[i+1])
                if flagVies not in ['True', 'False']:
                    print("Erro ao identificar o parametro '{}'. Possiveis valors: 0 ou 1\nEncontrado {}".format(argumento, parametros[argumento]))
                    quit()

        print('*********************************************')
        print('Execução iniciada em: {}'.format(datetime.datetime.now().strftime('%d/%m/%Y %H:%M')))
        print('Produto   : {}'.format(produto))
        print('Data      : {}'.format(dataPrevisao.strftime('%d/%m/%Y')))
        print('Mapas     : {}'.format(mapas))
        print('Modelos   : {}'.format(modelos))
        print('Vies      : {}'.format(flagVies))
        print('Preliminar: {}'.format(flagPreliminar))
        print('*********************************************')

        pathPrevisaoDia = os.path.join(pathResultados, dataPrevisao.strftime('%Y-%m-%d'))

        if sys.argv[1] == 'baixarImportarModelos':
            infoRodadas = baixarArquivos(produto, dataPrevisao, mapas, modelos, flagVies, flagPreliminar)
            organizarArquivos()
            importarArquivos(infoRodadas)

        elif sys.argv[1] == 'atualizacaoDiaria':
            infoRodadas = atualizacaoDiaria(produto, dataPrevisao, flagVies, flagPreliminar)

    else:
        printHelper()
        exit()

if __name__ == '__main__':
    runWithParams()