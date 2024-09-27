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

info_bloco = {'campos':[
                            'cod_usina',
                            'tipo',
                            'modif',
                            'mi',
                            'anoi',
                            'mf',
                            'anof',
                            'usina',
                        ],
                        'regex':'(.{4}) (.{5}) (.{8}) (.{2}) (.{4}) {0,1}(.{0,2}) {0,1}(.{0,4}) {0,1}(.{0,})(.*)',
                        'formatacao':'{:>4} {:>5} {:>8} {:>2} {:>4} {:>2} {:>4} {:<}'}

def leituraArquivo(filePath):

    file = open(filePath, 'r', encoding='utf-8')
    
    arquivo = file.readlines()
    file.close()
    
    num_linhas = len(arquivo)
    comentarios = {0:[]}
    bloco = []
    
    flag_comentario = True
    for iLine in range(num_linhas):
        line = arquivo[iLine]
        
        if flag_comentario:
            
            # if iLine not in comentarios:
            #     comentarios[iLine] = []
                
            comentarios[0].append(line)
            
            if line[:4] == 'XXXX':
                flag_comentario = False

        elif line.strip() == '':
            continue
            
        else:
            infosLinha = re.split(info_bloco['regex'], line)
            if len(infosLinha) < 2:
                print(info_bloco['regex'])
                print(line)
                pdb.set_trace()
                
            bloco.append(infosLinha[1:-2])
            
    df_expt = pd.DataFrame(bloco, columns=info_bloco['campos'])

    return comentarios, df_expt
            
def escrever_arquivo(df_dadger, comentarios, filePath):
    
    fileOut = codecs.open(filePath, 'w', 'utf-8')
    
    for index, row in df_dadger.iterrows():
        if index in comentarios:
            for coment in comentarios[index]:
                fileOut.write(coment)
        fileOut.write('{}\n'.format(info_bloco['formatacao'].format(*row.values)))

    fileOut.close()
    print(filePath)
    return filePath


if __name__ == '__main__':
    
    filePath = os.path.abspath(r'C:\Users\cs341052\Desktop\deck\newave\NW202301\EXPT.DAT')
    fileSaida = os.path.abspath(r'C:\WX2TB\Documentos\fontes\PMO\scripts_unificados\apps\newave\arquivos\saida\EXPT.DAT')
    comentarios, bloco = leituraArquivo(filePath)
    pdb.set_trace()
    arq_expt = pd.DataFrame(bloco, columns=info_bloco['campos'])
    arq_expt[['modif','mi','anoi','mf']] = arq_expt[['modif','mi','anoi','mf']].apply(pd.to_numeric)
    escrever_arquivo(arq_expt, comentarios, fileSaida)