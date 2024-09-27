import os
import re
import sys
import pdb
import copy
import codecs
import datetime

from dateutil.relativedelta import relativedelta
import pandas as pd



# path_home =  os.path.expanduser("~")
path_modulo = os.path.dirname(os.path.abspath(__file__))
path_app = os.path.dirname(path_modulo)
path_apps = os.path.dirname(path_app)
path_libs = os.path.join(os.path.dirname(path_apps), 'bibliotecas')


sys.path.insert(1, path_libs)
import wx_opweek

info_blocos = {}
info_blocos['CADH-VOL-REF-PER'] = {'campos':[
                                        'mnemonico',
                                        'cod_posto',
                                        'PerIni',
                                        'PerFin',
                                        'vol',
                        ],
                        'regex':'(.{1,});(.{1,});(.{1,});(.{1,});(.{1,})(.*)',
                        'formatacao':'{:};{:};{:};{:};{:}'}

def leitura_arquivo(filePath:str):
    file = open(filePath, 'r', encoding='utf-8')
    
    arquivo = file.readlines()
    file.close()
    
    blocos = {}
    comentarios = {}
    
    coment = []
    for iLine in range(len(arquivo)):
        line = arquivo[iLine]
        if line[0:6] == 'VOLUME':
            coment.append(line)
            continue
        
        mnemonico = line.split(';')[0].strip()
        if mnemonico not in info_blocos:
            print(mnemonico)
            print(line)
            pdb.set_trace()
            
        infosLinha = re.split(info_blocos[mnemonico]['regex'], line)
        if len(infosLinha) < 2:
            pdb.set_trace()
        
        if mnemonico not in blocos:
            blocos[mnemonico] = []
            comentarios[mnemonico] = {}
            
        if len(coment) > 0:
            comentarios[mnemonico][len(blocos[mnemonico])] = coment
            coment = []
        
        blocos[mnemonico].append(infosLinha[1:-2])   # ultimo termo da lista e o que sobra da expressao regex (/n)
        
    if len(coment) > 0:
        comentarios[mnemonico][len(blocos[mnemonico])] = coment
        
    df_file = {}
    for mnemonico in blocos:
        df_file[mnemonico] = pd.DataFrame(blocos[mnemonico], columns=info_blocos[mnemonico]['campos'])
            
    return df_file, comentarios


def escrever_arquivo(df_file, comentarios, path_saida):
    fileOut = codecs.open(path_saida, 'w', 'utf-8')
    for mnemonico in df_file:
        
        for index, row in df_file[mnemonico].iterrows():
            if index in comentarios[mnemonico]:
                for coment in comentarios[mnemonico][index]:
                    fileOut.write(coment)
            fileOut.write('{}\n'.format(info_blocos[mnemonico]['formatacao'].format(*row.values).strip()))

    fileOut.close()
    print(path_saida)
    return path_saida
        
        
def gerarvolumeReferencia(arquivoBase:str, dtInicial:datetime.datetime, pathSaida:str):
    
    df_volumeReferencia, comentarios = leitura_arquivo(arquivoBase)
    df_volumeReferencia['CADH-VOL-REF-PER']['PerIni'] = pd.to_datetime(df_volumeReferencia['CADH-VOL-REF-PER']['PerIni'], format='%Y/%m')
    df_volumeReferencia['CADH-VOL-REF-PER']['PerFin'] = pd.to_datetime(df_volumeReferencia['CADH-VOL-REF-PER']['PerFin'], format='%Y/%m')
    
    filtro1 = df_volumeReferencia['CADH-VOL-REF-PER']['PerIni'] > dtInicial
    df_volumeReferencia['CADH-VOL-REF-PER'] = df_volumeReferencia['CADH-VOL-REF-PER'][filtro1]
    
    filtro2 = df_volumeReferencia['CADH-VOL-REF-PER']['PerIni'] >= dtInicial + relativedelta(years=8)
    df_novoAno = df_volumeReferencia['CADH-VOL-REF-PER'][filtro2].copy()
    
    df_novoAno['PerIni'] = df_novoAno['PerIni'].apply(lambda x: x + relativedelta(months=12)).dt.strftime('%Y/%m')
    df_novoAno['PerFin'] = df_novoAno['PerFin'].apply(lambda x: x + relativedelta(months=12)).dt.strftime('%Y/%m')
    
    df_volumeReferencia['CADH-VOL-REF-PER']['PerIni'] = df_volumeReferencia['CADH-VOL-REF-PER']['PerIni'].dt.strftime('%Y/%m')
    df_volumeReferencia['CADH-VOL-REF-PER']['PerFin'] = df_volumeReferencia['CADH-VOL-REF-PER']['PerFin'].dt.strftime('%Y/%m')
    
    df_volumeReferencia['CADH-VOL-REF-PER'] = pd.concat([df_volumeReferencia['CADH-VOL-REF-PER'], df_novoAno])
    
    escrever_arquivo(df_volumeReferencia, comentarios, pathSaida)
    
    return pathSaida

if __name__ == '__main__':
    
    filePath = os.path.abspath(r'C:\Users\cs341052\Downloads\NEW_CCEE_082024-SOMBRA_HIB_2publicacao\volumes-referencia.csv')
    # df_columeReferencia, comentarios = leitura_arquivo(filePath)
    # escrever_arquivo(df_columeReferencia, comentarios, filePath.replace('volumes-referencia.csv','volumes-referencia_thiago.csv'))
    
    dtInicial = datetime.datetime(2025,1,1)
    pathSaida = filePath.replace('volumes-referencia.csv','volumes-referencia_thiago.csv')
    gerarvolumeReferencia(filePath, dtInicial, pathSaida)