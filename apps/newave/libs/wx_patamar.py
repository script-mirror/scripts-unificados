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

info_bloco['patamares'] = {'campos':[
                                        'ano',
                                        'jan',
                                        'fev',
                                        'mar',
                                        'abr',
                                        'mai',
                                        'jun',
                                        'jul',
                                        'ago',
                                        'set',
                                        'out',
                                        'nov',
                                        'dez',
                        ],
                        'regex':'(.{4})  (.{6})  (.{6})  (.{6})  (.{6})  (.{6})  (.{6})  (.{6})  (.{6})  (.{6})  (.{6})  (.{6})  (.{6})(.*)',
                        'formatacao':'{:>4}  {:>6}  {:>6}  {:>6}  {:>6}  {:>6}  {:>6}  {:>6}  {:>6}  {:>6}  {:>6}  {:>6}  {:>6}'}

info_bloco['carga'] = {'campos':[
                                        'ano',
                                        'jan',
                                        'fev',
                                        'mar',
                                        'abr',
                                        'mai',
                                        'jun',
                                        'jul',
                                        'ago',
                                        'set',
                                        'out',
                                        'nov',
                                        'dez',
                        ],
                        'regex':'   (.{4}) (.{6}) (.{6}) (.{6}) (.{6}) (.{6}) (.{6}) (.{6}) (.{6}) (.{6}) (.{6}) (.{6}) (.{6})(.*)',
                        'formatacao':'   {:>4} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6}'}

info_bloco['intercambio'] = {'campos':[
                                        'ano',
                                        'jan',
                                        'fev',
                                        'mar',
                                        'abr',
                                        'mai',
                                        'jun',
                                        'jul',
                                        'ago',
                                        'set',
                                        'out',
                                        'nov',
                                        'dez',
                        ],
                        'regex':'   (.{4}) (.{6}) (.{6}) (.{6}) (.{6}) (.{6}) (.{6}) (.{6}) (.{6}) (.{6}) (.{6}) (.{6}) (.{6})(.*)',
                        'formatacao':'   {:>4} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6}'}

info_bloco['nao_simuladas'] = {'campos':[
                                        'ano',
                                        'jan',
                                        'fev',
                                        'mar',
                                        'abr',
                                        'mai',
                                        'jun',
                                        'jul',
                                        'ago',
                                        'set',
                                        'out',
                                        'nov',
                                        'dez',
                        ],
                        'regex':'   (.{4}) (.{6}) (.{6}) (.{6}) (.{6}) (.{6}) (.{6}) (.{6}) (.{6}) (.{6}) (.{6}) (.{6}) (.{6})(.*)',
                        'formatacao':'   {:>4} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6} {:>6}'}

def leituraArquivo(filePath):

    file = open(filePath, 'r', encoding='latin-1')
    
    arquivo = file.readlines()
    file.close()
    
    num_linhas = len(arquivo)
    comentarios = {}
    blocos = {}
    
    flag_comentario = True
    flag_ultimo_comentario = False
    for iLine in range(num_linhas):
        line = arquivo[iLine]

        if re.search("NUMERO DE PATAMARES", line):
            nome_bloco = 'patamares'
            comentarios[nome_bloco] = {}
            blocos[nome_bloco] = []

        elif re.search("SUBSISTEMA", line):
            if 'carga' not in blocos:
                flag_comentario = True
                nome_bloco = 'carga'
                comentarios[nome_bloco] = {}
                blocos[nome_bloco] = []


        elif re.search("^9999", line):
            if 'intercambio' not in blocos:
                flag_comentario = True
                nome_bloco = 'intercambio'
                comentarios[nome_bloco] = {}
                blocos[nome_bloco] = []
            else:
                flag_comentario = True
                nome_bloco = 'nao_simuladas'
                comentarios[nome_bloco] = {}
                blocos[nome_bloco] = []

        elif re.search("X.XXXX", line):
            flag_ultimo_comentario = True

        elif re.search("^\d$", line.strip()) and nome_bloco == 'carga':
            flag_comentario = True
            flag_ultimo_comentario = True

        elif re.search("^\d{1,2} {1,5}\d{1,2}$", line.strip()) and nome_bloco == 'intercambio':
            flag_comentario = True
            flag_ultimo_comentario = True

        elif re.search("^\d{1,2} {1,5}\d{1,2}$", line.strip()) and nome_bloco == 'nao_simuladas':
            flag_comentario = True
            flag_ultimo_comentario = True

        elif line.strip() == '':
            continue

        if flag_comentario:
            iLinhaInfo = len(blocos[nome_bloco])
            if iLinhaInfo not in comentarios[nome_bloco]:
                comentarios[nome_bloco][iLinhaInfo] = [line]
            else:
                comentarios[nome_bloco][iLinhaInfo].append(line)

        else:
            infosLinha = re.split(info_bloco[nome_bloco]['regex'], line)
            if len(infosLinha) > 1:
                blocos[nome_bloco].append(infosLinha[1:-2])
            else:
                pdb.set_trace()

        if flag_ultimo_comentario:
            flag_ultimo_comentario = False
            flag_comentario = False

    df_patamar = {}
    for nome_bloco in blocos:
        df_patamar[nome_bloco] = pd.DataFrame(blocos[nome_bloco], columns=info_bloco[nome_bloco]['campos'])

    return comentarios, df_patamar
            
def escrever_arquivo(df_blocos, comentarios, filePath):
    fileOut = codecs.open(filePath, 'w', 'utf-8')
    
    for nome_bloco in df_blocos:
        for index, row in df_blocos[nome_bloco].iterrows():
            if index in comentarios[nome_bloco]:
                for coment in comentarios[nome_bloco][index]:
                    fileOut.write(coment)
            fileOut.write('{}\n'.format(info_bloco[nome_bloco]['formatacao'].format(*row.values)))

    fileOut.close()
    print(filePath)
    return filePath



if __name__ == '__main__':
    
    filePath = os.path.abspath(r'C:\Users\cs341052\Downloads\NW202306_base\PATAMAR.DAT')
    fileSaida = os.path.abspath(r'C:\WX2TB\Documentos\fontes\PMO\scripts_unificados\apps\newave\arquivos\saida\PATAMAR.DAT')
    comentarios, blocos = leituraArquivo(filePath)        
    escrever_arquivo(blocos, comentarios, fileSaida)
