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
info_bloco['config_hidro'] = {'campos':[
                                        'num',
                                        'nome',
                                        'posto',
                                        'jus',
                                        'ree',
                                        'vaz_inic',
                                        'usin_exis',
                                        'modif',
                                        'inic_hist',
                                        'fim_hist',
                        ],
                        'regex':' (.{4}) (.{12}) (.{4})  (.{4}) (.{4}) (.{6}) (.{4})   (.{4})     (.{4})     (.{4})(.*)',
                        'formatacao':' {:>4} {:>12} {:>4}  {:>4} {:>4} {:>6} {:>4}   {:>4}     {:>4}     {:>4}'}



def leituraArquivo(filePath):
    file = open(filePath, 'r', encoding='latin-1')
    
    arquivo = file.readlines()
    file.close()
    
    num_linhas = len(arquivo)
    comentarios = {}
    blocos = {}
    
    flag_comentario = True

    nome_bloco = 'config_hidro'
    blocos[nome_bloco] = []
    comentarios[nome_bloco] = {}
    for iLine in range(num_linhas):
        
        line = arquivo[iLine]
        if flag_comentario:
            idx_line = len(blocos[nome_bloco])
            if idx_line not in comentarios[nome_bloco]:
                comentarios[nome_bloco][idx_line] = [line]
            else:
                comentarios[nome_bloco][idx_line].append(line)
            if re.search("XXXX", line):
                flag_comentario = False

        elif line.strip() == '':
            continue
        
        elif line.strip() == '999':
            flag_comentario = True

        else:
            infosLinha = re.split(info_bloco['config_hidro']['regex'], line)
            
            if len(infosLinha) > 1:
                blocos['config_hidro'].append(infosLinha[1:-2])
            else:
                print(info_bloco['id_subsistema']['regex'])
                print(line)
                pdb.set_trace()

    df_confhd = {}
    for nome_bloco in blocos:
        df_confhd[nome_bloco] = pd.DataFrame(blocos[nome_bloco], columns=info_bloco[nome_bloco]['campos'])

    return comentarios, df_confhd
            
def escrever_arquivo(df_blocos, comentarios, filePath):
    
    fileOut = codecs.open(filePath, 'w', 'utf-8')
    for nome_bloco in df_blocos:
        for index, row in df_blocos[nome_bloco].iterrows():
            if index in comentarios[nome_bloco]:
                for coment in comentarios[nome_bloco][index]:
                    fileOut.write(coment)
            fileOut.write('{}\n'.format(info_bloco[nome_bloco]['formatacao'].format(*row.values)))
        fileOut.write(' 999\n')

    fileOut.close()
    print(filePath)
    return filePath


if __name__ == '__main__':
    
    filePath = os.path.abspath(r'C:\Users\cs341052\Downloads\NW202306_base\CONFHD.DAT')
    fileSaida = os.path.abspath(r'C:\WX2TB\Documentos\fontes\PMO\scripts_unificados\apps\newave\arquivos\saida\CONFHD.DAT')
    
    comentarios, blocos = leituraArquivo(filePath)    
    escrever_arquivo(blocos, comentarios, fileSaida)