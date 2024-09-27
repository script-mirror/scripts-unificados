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
info_bloco['usina'] = {'campos':[
                                        'mnemonico',
                                        'valores',
                                        'comentario',
                        ],
                        'regex':' (.{8}) (.{20})(.*)(\\n)',
                        'formatacao':' {:>8} {:>20}{}'}


info_bloco['generico'] = {'campos':[
                                        'mnemonico',
                                        'val',
                        ],
                        'regex':' (.{8}) (.{1,60})(.*)',
                        'formatacao':' {:>7}  {:>7}'}

info_bloco['vazmin'] = {'campos':[
                                        'mnemonico',
                                        'val',
                        ],
                        'regex':' (.{8}) (.{1,6})(.*)',
                        'formatacao':' {:>8} {:>6}'}

info_bloco['volmin'] = {'campos':[
                                        'mnemonico',
                                        'val',
                                        'unidade',
                        ],
                        'regex':' (.{8}) (.{15}) (.{3})(.*)',
                        'formatacao':' {:>8} {:>15} {:>3}'}

info_bloco['volmax'] = {'campos':[
                                        'mnemonico',
                                        'val',
                                        'unidade',
                        ],
                        'regex':' (.{7})  (.{6}) (.{3})(.*)',
                        'formatacao':' {:>7}  {:>6} {:>3}'}

info_bloco['vmaxt'] = {'campos':[
                                        'mnemonico',
                                        'mes',
                                        'ano',
                                        'val',
                                        'unidade',
                        ],
                        'regex':' (.{8}) (.{2}) (.{4}) (.{1,7}) (.{3})(.*)',
                        'formatacao':' {:>8} {:>2} {:>4} {:>7} {:>3}'}


info_bloco['vmint'] = {'campos':[
                                        'mnemonico',
                                        'mes',
                                        'ano',
                                        'val',
                                        'unidade',
                        ],
                        'regex':' (.{8}) (.{2}) (.{4}) (.{1,7}) (.{3})(.*)',
                        'formatacao':' {:>8} {:>2} {:>4} {:>7} {:>3}'}

info_bloco['vazmint'] = {'campos':[
                                        'mnemonico',
                                        'mes',
                                        'ano',
                                        'val',
                        ],
                        'regex':' (.{8}) (.{2}) (.{4}) (.{1,7})(.*)',
                        'formatacao':' {:>8} {:>2} {:>4} {:>7}'}

info_bloco['cfuga'] = {'campos':[
                                        'mnemonico',
                                        'mes',
                                        'ano',
                                        'val',
                        ],
                        'regex':' (.{8}) (.{2}) (.{4}) (.{1,7})(.*)',
                        'formatacao':' {:>8} {:>2} {:>4} {:>7}'}

info_bloco['cmont'] = {'campos':[
                                        'mnemonico',
                                        'mes',
                                        'ano',
                                        'val',
                        ],
                        'regex':' (.{8}) (.{2}) (.{4}) (.{1,7})(.*)',
                        'formatacao':' {:>8} {:>2} {:>4} {:>7}'}

info_bloco['turbmint'] = {'campos':[
                                        'mnemonico',
                                        'mes',
                                        'ano',
                                        'val',
                        ],
                        'regex':' (.{8}) (.{2}) (.{4}) (.{7})(.*)',
                        'formatacao':' {:>8} {:>2} {:>4} {:>7}'} 

info_bloco['turbmaxt'] = {'campos':[
                                        'mnemonico',
                                        'mes',
                                        'ano',
                                        'val',
                        ],
                        'regex':' (.{8}) (.{2}) (.{4}) (.{7})(.*)',
                        'formatacao':' {:>8} {:>2} {:>4} {:>7}'} 

info_bloco['numcnj'] = {'campos':[
                                        'mnemonico', 
                                        'val',       
                        ],
                        'regex':' (.{8}) (.{1,3})(.*)',
                        'formatacao':' {:>8} {:>3}'} 

info_bloco['nummaq'] = {'campos':[
                                        'mnemonico',
                                        'numMaq',
                                        'conj',
                        ],
                        'regex':' (.{8})   (.{1,2})  (.{1,2})(.*)',
                        'formatacao':' {:>8}   {:>2} {:>2}'} 

info_bloco['cotarea'] = {'campos':[
                                        'mnemonico',
                                        'coe1',
                                        'coe2',
                                        'coe3',
                                        'coe4',
                                        'coe5',
                        ],
                        'regex':' (.{8}) (.{4}) (.{4}) (.{4}) (.{4}) (.{4})(.*)',
                        'formatacao':' {:<8} {:>4} {:>4} {:>4} {:>4} {:>4}'}



def leituraArquivo(filePath):

    file = open(filePath, 'r', encoding='latin-1')
    
    arquivo = file.readlines()
    file.close()
    
    num_linhas = len(arquivo)
    comentarios = {}
    blocos = {}
    
    flag_comentario = True
    usina = None
    comentario_inicial = []
    for iLine in range(num_linhas):
        line = arquivo[iLine]

        if re.search("^ USINA", line):
                    infosLinha = re.split(info_bloco['usina']['regex'], line)
                    usina = int(infosLinha[2])
                    blocos[usina] = {'usina':[infosLinha[1:-2]]}
                    if len(comentario_inicial) >= 1:
                        comentarios[usina] = {'usina': {0:comentario_inicial}}
                        comentario_inicial = []
                    flag_comentario = False

        elif flag_comentario:
            if usina == None:
                comentario_inicial.append(line)

        elif line.strip() == '':
            continue

        else:
            infosLinhaGenerica = re.split(info_bloco['generico']['regex'], line)
            mnemonico = infosLinhaGenerica[1].strip().lower()
            infosLinha = re.split(info_bloco[mnemonico]['regex'], line)
            if mnemonico not in blocos[usina]:
                blocos[usina][mnemonico] = []
            if len(infosLinha) <= 1:
                pdb.set_trace()
            else:
                blocos[usina][mnemonico].append(infosLinha[1:-2])

    df_modif = {}
    for usina in blocos:
        df_modif[usina] = {}
        for nome_bloco in blocos[usina]:
            df_modif[usina][nome_bloco] = pd.DataFrame(blocos[usina][nome_bloco], columns=info_bloco[nome_bloco]['campos'])

    return comentarios, df_modif
            
def escrever_arquivo(df_blocos, comentarios, filePath):
    fileOut = codecs.open(filePath, 'w', 'utf-8')
    
    for usina in df_blocos:
        for nome_bloco in df_blocos[usina]:
            for index, row in df_blocos[usina][nome_bloco].iterrows():
                if usina in comentarios:
                    if nome_bloco in comentarios[usina]:
                        if index in comentarios[usina][nome_bloco]:
                            for coment in comentarios[usina][nome_bloco][index]:
                                fileOut.write(coment)
                try:
                    fileOut.write('{}\n'.format(info_bloco[nome_bloco]['formatacao'].format(*row.values)))
                except:
                    pdb.set_trace()
        
    fileOut.close()
    print(filePath)
    return filePath


if __name__ == '__main__':
    
    filePath = os.path.abspath(r'C:\Users\cs341052\Downloads\NW202306_base\MODIF.DAT')
    fileSaida = os.path.abspath(r'C:\WX2TB\Documentos\fontes\PMO\scripts_unificados\apps\newave\arquivos\saida\MODIF.DAT')
    comentarios, blocos = leituraArquivo(filePath)        
    escrever_arquivo(blocos, comentarios, fileSaida)
