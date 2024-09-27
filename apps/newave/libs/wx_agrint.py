import os
import re
import sys
import pdb
import codecs
import datetime

import pandas as pd



# path_home =  os.path.expanduser("~")
path_modulo = os.path.dirname(os.path.abspath(__file__))
path_app = os.path.dirname(path_modulo)
path_apps = os.path.dirname(path_app)
path_libs = os.path.join(os.path.dirname(path_apps), 'bibliotecas')


sys.path.insert(1, path_libs)
import wx_opweek

info_bloco = {}
info_bloco['identificacao_agrupamento'] = {'campos':[
                                        'ag',
                                        'sub_origem',
                                        'sub_destino',
                                        'coef',
                        ],
                        'regex':' (.{3}) (.{3}) (.{3}) (.{7})(.*)',    
                        'formatacao':' {:>3} {:>3} {:>3} {:>7}'}

info_bloco['limites_agrupamento'] = {'campos':[
                                        'ag',    
                                        'mi',    
                                        'anoi',  
                                        'mf',    
                                        'anof',  
                                        'lim_p1',
                                        'lim_p2',
                                        'lim_p3',
                                        'nome_agrupamento',
                        ],
                        'regex':' (.{3})  (.{2}) (.{4}) (.{2}) (.{4}) (.{6})\. (.{6})\. (.{6})\.    (.{1,})(.*)',
                        'formatacao':' {:>3}  {:>2} {:>4} {:>2} {:>4} {:>6}. {:>6}. {:>6}.    {:<}'}

def leituraArquivo(filePath):

    file = open(filePath, 'r', encoding='latin-1')
    
    arquivo = file.readlines()
    file.close()
    
    num_linhas = len(arquivo)
    comentarios = {}
    bloco = {}
    
    flag_comentario = True

    for iLine in range(num_linhas):
        
        line = arquivo[iLine]
        if line.strip() == "AGRUPAMENTOS DE INTERCÂMBIO":
            nome_bloco = 'identificacao_agrupamento'
            comentarios[nome_bloco] = {}
            bloco[nome_bloco] = []
        elif line.strip() == "LIMITES POR GRUPO":
            nome_bloco = 'limites_agrupamento'
            comentarios[nome_bloco] = {}
            bloco[nome_bloco] = []
        
        if flag_comentario:
            iLinhaInfo = len(bloco[nome_bloco])
            if iLinhaInfo not in comentarios[nome_bloco]:
                comentarios[nome_bloco][iLinhaInfo] = [line]
            else:
                comentarios[nome_bloco][iLinhaInfo].append(line)
            
            if line[:4] == ' XXX':
                flag_comentario = False

        elif line.strip() == '':
            continue
        
        elif line.strip() == '999':
            flag_comentario = True
            
        else:
            infosLinha = re.split(info_bloco[nome_bloco]['regex'], line)
            if len(infosLinha) < 2:
                print(info_bloco[nome_bloco]['regex'])
                print(line)
                pdb.set_trace()
                
            bloco[nome_bloco].append(infosLinha[1:-2])

    df_agrint = {}
    for nome_bloco in bloco:
        df_agrint[nome_bloco] = pd.DataFrame(bloco[nome_bloco], columns=info_bloco[nome_bloco]['campos'])
            
    return comentarios, df_agrint
            
def escrever_arquivo(df_agrint, comentarios, fileSaida):

    fileOut = codecs.open(fileSaida, 'w', 'utf-8')
    for nome_bloco in df_agrint:

        for index, row in df_agrint[nome_bloco].iterrows():
            if index in comentarios[nome_bloco]:
                for coment in comentarios[nome_bloco][index]:
                    fileOut.write(coment)
            fileOut.write('{}\n'.format(info_bloco[nome_bloco]['formatacao'].format(*row.values)))

        fileOut.write(' 999\n')
    

    fileOut.close()
    print(fileSaida)
    return fileSaida


if __name__ == '__main__':
    
    filePath = os.path.abspath(r'C:\Users\cs341052\Downloads\NW202306_base\AGRINT.DAT')
    fileSaida = os.path.abspath(r'C:\WX2TB\Documentos\fontes\PMO\scripts_unificados\apps\newave\arquivos\saida\AGRINT.DAT')
    comentarios, df_agrint = leituraArquivo(filePath)

    if os.path.exists(fileSaida):
        os.remove(fileSaida)
    
    escrever_arquivo(df_agrint, comentarios, fileSaida)
