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

info_bloco['valor_penalizacao'] = {'campos':[
                                        'cod_ree',
                                        'penalidade',
                        ],
                        'regex':' (.{3})       (.{7})(.*)',
                        'formatacao':' {:0>3}       {:>4.2f}'}

info_bloco['curva_seguranca'] = {'campos':[
                                        'ano',
                                        'ener_max_jan',
                                        'ener_max_fev',
                                        'ener_max_mar',
                                        'ener_max_abr',
                                        'ener_max_mai',
                                        'ener_max_jun',
                                        'ener_max_jul',
                                        'ener_max_ago',
                                        'ener_max_set',
                                        'ener_max_out',
                                        'ener_max_nov',
                                        'ener_max_dez',
                        ],
                        'regex':'(.{5}) (.{5}) (.{5}) (.{5}) (.{5}) (.{5}) (.{5}) (.{5}) (.{5}) (.{5}) (.{5}) (.{5}) (.{5})(.*)',
                        'formatacao':'{:<5} {:>5} {:>5} {:>5} {:>5} {:>5} {:>5} {:>5} {:>5} {:>5} {:>5} {:>5} {:>5}'}

info_bloco['rodape'] = {'campos':[
                                        'registro',
                                        'valor',
                        ],
                        'regex':'(.{29})(.{5})(.*)',
                        'formatacao':'{:<29}{:>5}'}

def leituraArquivo(filePath):

    file = open(filePath, 'r', encoding='latin-1')
    
    arquivo = file.readlines()
    file.close()
    
    num_linhas = len(arquivo)
    comentarios = {}
    blocos = {}
    
    nome_bloco = 'valor_penalizacao'
    comentarios[nome_bloco] = {}
    blocos[nome_bloco] = []
    flag_comentario = True
    flag_ultimo_comentario = False
    
    for iLine in range(num_linhas):
        line = arquivo[iLine]

        if re.search("CURVA DE SEGURANCA", line):
            nome_bloco = 'curva_seguranca'
            comentarios[nome_bloco] = {}
            blocos[nome_bloco] = []

        elif line.strip() == '9999':
            nome_bloco = 'rodape'
            comentarios[nome_bloco] = {}
            blocos[nome_bloco] = []
            flag_comentario = True
            flag_ultimo_comentario = True
            continue

        elif re.search(" XXX       XXXX.XX", line) and nome_bloco == 'valor_penalizacao':
            flag_ultimo_comentario = True

        elif re.search("JAN.X", line) and nome_bloco == 'valor_penalizacao':
            flag_ultimo_comentario = True

        elif line.strip() == '':
            continue

        elif line.strip() == '999':
            flag_comentario = True
            continue

        if nome_bloco == 'curva_seguranca' and re.search("^\d{1,2}$", line.strip()):
            flag_comentario = True
            flag_ultimo_comentario = True

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
        
    df_curva['valor_penalizacao']['penalidade'] = df_curva['valor_penalizacao']['penalidade'].astype('float')

    return comentarios, df_curva
            
def escrever_arquivo(df_blocos, comentarios, filePath):
    fileOut = codecs.open(filePath, 'w', 'utf-8')
    
    for nome_bloco in df_blocos:
        for index, row in df_blocos[nome_bloco].iterrows():
            if index in comentarios[nome_bloco]:
                for coment in comentarios[nome_bloco][index]:
                    fileOut.write(coment)
            fileOut.write('{}\n'.format(info_bloco[nome_bloco]['formatacao'].format(*row.values)))
        
        if nome_bloco == 'curva_seguranca':
            fileOut.write('9999\n')
        elif nome_bloco == 'rodape':
            pass
        else:
            fileOut.write(' 999\n')

    fileOut.close()
    print(filePath)
    return filePath


if __name__ == '__main__':
    
    filePath = os.path.abspath(r'C:\Users\cs341052\Downloads\NW202306_base\CURVA.DAT')
    fileSaida = os.path.abspath(r'C:\WX2TB\Documentos\fontes\PMO\scripts_unificados\apps\newave\arquivos\saida\CURVA.DAT')
    comentarios, blocos = leituraArquivo(filePath)        
    escrever_arquivo(blocos, comentarios, fileSaida)
