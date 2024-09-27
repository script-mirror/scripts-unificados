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
info_blocos['RE'] = {'campos':[
                                        'mnemonico',
                                        'cod_rest',
                                        'formula',
                        ],
                        'regex':'(.{2})  ; (.{4}); (.{1,})(.*)',
                        'formatacao':'{:>2}  ; {:>4}; {:<}'}
info_blocos['RE-HORIZ-PER'] = {'campos':[
                                        'mnemonico',
                                        'cod_rest',
                                        'PerIni',
                                        'PerFin',
                        ],
                        'regex':'(.{12}) ; (.{8});(.{7});(.{7})(.*)',
                        'formatacao':'{:>12} ; {:>8};{:>7};{:>7}'} 

info_blocos['RE-LIM-FORM-PER-PAT'] = {'campos':[
                                        'mnemonico',
                                        'cod_rest',
                                        'PerIni',
                                        'PerFin',
                                        'Pat',
                                        'LimInf',
                                        'LimSup',
                        ],
                        'regex':'(.{19}) ; (.{8}); (.{7}); (.{7}); (.{5}); (.{9}); (.{7})(.*)',
                        'formatacao':'{:>19} ; {:>8}; {:>7}; {:>7}; {:>5}; {:>9}; {:>7}'}

def leitura_arquivo(filePath:str):
    file = open(filePath, 'r', encoding='utf-8')
    
    arquivo = file.readlines()
    file.close()
    
    blocos = {}
    comentarios = {}
    
    coment = []
    for iLine in range(len(arquivo)):
        line = arquivo[iLine]
        if line[0] == '&' or line.strip() == '':
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
        
    df_restricao_eletrica = {}
    for mnemonico in blocos:
        df_restricao_eletrica[mnemonico] = pd.DataFrame(blocos[mnemonico], columns=info_blocos[mnemonico]['campos'])
            
    return df_restricao_eletrica, comentarios


def escrever_dadgnl(df_restricao_eletrica, comentarios, path_saida):
    fileOut = codecs.open(path_saida, 'w', 'utf-8')
    for mnemonico in df_restricao_eletrica:
        
        for index, row in df_restricao_eletrica[mnemonico].iterrows():
            if index in comentarios[mnemonico]:
                for coment in comentarios[mnemonico][index]:
                    fileOut.write(coment)
            fileOut.write('{}\n'.format(info_blocos[mnemonico]['formatacao'].format(*row.values).strip()))

    fileOut.close()
    print(path_saida)
    return path_saida
        
        
def gerarProximosMeses(filePath:str, dtInicio:datetime.datetime,
                       dtFim:datetime.datetime, path_restricaoEletrica_saida:str):

    df_rest, comentarios = leitura_arquivo(filePath)

    datasIncrementar = {
        'RE-HORIZ-PER': ['PerIni', 'PerFin'],
        'RE-LIM-FORM-PER-PAT': ['PerIni', 'PerFin']
    }

    dtRef = datetime.datetime.strptime(df_rest['RE-HORIZ-PER'].loc[0, 'PerIni'], '%Y/%m')
    while dtRef <= dtFim:
        for mnemon in datasIncrementar:
            for col in datasIncrementar[mnemon]:
                df_rest[mnemon][col] = pd.to_datetime(df_rest[mnemon][col], format='%Y/%m')
                df_rest[mnemon][col] = df_rest[mnemon][col].apply(lambda x: x + relativedelta(months=1))
                df_rest[mnemon][col] = df_rest[mnemon][col].dt.strftime('%Y/%m')

        if dtRef >= dtInicio:
            escrever_dadgnl(df_rest, comentarios, filePath.replace('.csv',f'{dtRef.strftime("%Y%m")}.csv'))
        
        dtRef = dtRef + relativedelta(months=1)

if __name__ == '__main__':
    
    filePath = os.path.abspath(r'C:\Users\cs341052\Downloads\NEW_CCEE_082024-SOMBRA_HIB_2publicacao\restricao-eletrica.csv')
    # df_restricao_eletrica, comentarios = leitura_arquivo(filePath)
    # escrever_dadgnl(df_restricao_eletrica, comentarios, filePath.replace('restricao-eletrica.csv','restricao-eletrica_thiago.csv'))
    
    dtInicio = datetime.datetime(2025, 1, 1)
    dtFim = datetime.datetime(2025, 12, 1)
    path_restricaoEletrica_saida = filePath.replace('restricao-eletrica.csv', 'restricao-eletrica_thiago.csv')
    gerarProximosMeses(filePath, dtInicio, dtFim, path_restricaoEletrica_saida)