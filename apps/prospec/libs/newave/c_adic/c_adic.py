import os
import re
import pdb
import codecs
import pandas as pd


info_bloco = {}
info_bloco['id_subsistema'] = {'campos':[
                                        'id_sub',
                                        'nome_submercado',
                                        'obs',
                        ],
                        'regex':'  (.{2}) (.{8}) (.{18})(.*)',
                        'formatacao':'  {:>2} {:>8} {:>18}'}

info_bloco['carga_adicional_mensal'] = {'campos':[
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
                        'regex':'(.{7})(.{7}) (.{7}) (.{7}) (.{7}) (.{7}) (.{7}) (.{7}) (.{7}) (.{7}) (.{7}) (.{7}) (.{7})(.*)',
                        'formatacao':'{:<7}{:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7}'}

def leituraArquivo(filePath):

    file = open(filePath, 'r', encoding='latin-1')
    
    arquivo = file.readlines()
    file.close()
    
    num_linhas = len(arquivo)
    comentarios = {}
    bloco = {'carga_adicional_mensal':[]}
    
    flag_comentario = True

    for iLine in range(num_linhas):
        
        line = arquivo[iLine]
        
        if flag_comentario:
            iAno = len(bloco['carga_adicional_mensal'])
            if iAno not in comentarios:
                comentarios[iAno] = [line]
            else:
                comentarios[iAno].append(line)
            if line[:14] == '       XXXJAN.':
                flag_comentario = False

        elif line.strip() == '':
            continue
        
        elif line.strip() == '999':
            flag_comentario = True

        else:
            infosLinha = re.split(info_bloco['carga_adicional_mensal']['regex'], line)
            
            if len(infosLinha) > 1:
                bloco['carga_adicional_mensal'].append(infosLinha[1:-2])
            
            else:
                infosLinha = re.split(info_bloco['id_subsistema']['regex'], line)
                if len(infosLinha) > 1:
                    iAno = len(bloco['carga_adicional_mensal'])
                    if iAno not in comentarios:
                        comentarios[iAno] = [line]
                    else:
                        comentarios[iAno].append(line)
                    
                else:
                    print(info_bloco['id_subsistema']['regex'])
                    print(line)
                    pdb.set_trace()

    df_c_adic = {}
    for nome_bloco in bloco:
        df_c_adic[nome_bloco] = pd.DataFrame(bloco[nome_bloco], columns=info_bloco[nome_bloco]['campos'])

    return comentarios, df_c_adic
            
def escrever_arquivo(df_blocos, comentarios, filePath):
    fileOut = codecs.open(filePath, 'w', 'utf-8')
    for nome_bloco in df_blocos:
        for index, row in df_blocos[nome_bloco].iterrows():
            if index in comentarios:
                for coment in comentarios[index]:
                    fileOut.write(coment)
            fileOut.write('{}\n'.format(info_bloco[nome_bloco]['formatacao'].format(*row.values)))
        fileOut.write(' 999\n')

    fileOut.close()
    print(filePath)
    return filePath


if __name__ == '__main__':
    
    filePath = os.path.abspath(r'C:\WX2TB\Documentos\fontes\PMO\scripts_unificados\apps\prospec\Estudo_22152\NW202412\c_adic.dat')
    comentarios, bloco = leituraArquivo(filePath)
    pdb.set_trace()