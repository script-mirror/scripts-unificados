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


info_bloco['devios'] = {'campos':[
                                        'ano',
                                        'usin',
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
                                        'flag_desv',
                                        'coment',
                        ],
                        'regex':'(.{4}) (.{4})(.{7})(.{7})(.{7})(.{7})(.{7})(.{7})(.{7})(.{7})(.{7})(.{7})(.{7})(.{7})    (.{1})     (.{0,})(\\n)',
                        'formatacao':'{:>4} {:>4}{:>7}{:>7}{:>7}{:>7}{:>7}{:>7}{:>7}{:>7}{:>7}{:>7}{:>7}{:>7}    {:>1}     {:}'}



def leituraArquivo(filePath):

    file = open(filePath, 'r', encoding='latin-1')
    
    arquivo = file.readlines()
    file.close()
    
    num_linhas = len(arquivo)
    comentarios = {}
    blocos = {}
    
    nome_bloco = 'devios'
    comentarios[nome_bloco] = {}
    blocos[nome_bloco] = []
    flag_comentario = True
    flag_ultimo_comentario = False
    
    for iLine in range(num_linhas):
        line = arquivo[iLine]

        if re.search("XXXX", line):
            flag_ultimo_comentario = True

        elif line.strip() == '':
            continue

        elif line.strip() == '9999':
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

    df_curva = {}
    for nome_bloco in blocos:
        df_curva[nome_bloco] = pd.DataFrame(blocos[nome_bloco], columns=info_bloco[nome_bloco]['campos'])

    return comentarios, df_curva
            
def escrever_arquivo(df_blocos, comentarios, filePath):
    fileOut = codecs.open(filePath, 'w', 'utf-8')
    
    for nome_bloco in df_blocos:
        for index, row in df_blocos[nome_bloco].iterrows():
            if index in comentarios[nome_bloco]:
                for coment in comentarios[nome_bloco][index]:
                    fileOut.write(coment)
            fileOut.write('{}\n'.format(info_bloco[nome_bloco]['formatacao'].format(*row.values)))
        
        fileOut.write('9999\n')

    fileOut.close()
    print(filePath)
    return filePath


if __name__ == '__main__':
    
    filePath = os.path.abspath(r'C:\Users\cs341052\Downloads\NW202306_base\DSVAGUA.DAT')
    fileSaida = os.path.abspath(r'C:\WX2TB\Documentos\fontes\PMO\scripts_unificados\apps\newave\arquivos\saida\DSVAGUA.DAT')
    comentarios, blocos = leituraArquivo(filePath)        
    escrever_arquivo(blocos, comentarios, fileSaida)
