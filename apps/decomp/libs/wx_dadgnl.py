
import os
import re
import pdb
import codecs

import pandas as pd

info_blocos = {}
info_blocos['TG'] = {'campos':[
				'mnemonico',
				'cod',
				'ss',
				'nome',
				'ip',
				'infl',
				'disp',
				'cvu',
				'infl',
				'disp',
				'cvu',
				'infl',
				'disp',
				'cvu',
			],
			'regex':'(.{2})  (.{3})  (.{2})   (.{10})(.{2})   (.{5})(.{5})(.{10})(.{5})(.{5})(.{10})(.{5})(.{5})(.{10})(.*)',
			'formatacao':'{:>2}  {:>3}  {:>2}   {:>10}{:>2}   {:>5}{:>5}{:>10}{:>5}{:>5}{:>10}{:>5}{:>5}{:>10}'}


info_blocos['GS'] = {'campos':[
				'mnemonico',
				'mes',
				'semanas',
			],
			'regex':'(.{2})  (.{2})   (.{1})(.*)',
			'formatacao':'{:>2}  {:>2}   {:>1}'}

info_blocos['NL'] = {'campos':[
				'mnemonico',
				'cod',
				'ss',
				'lag',
			],
			'regex':'(.{2})  (.{3})  (.{2})   (.{1})(.*)',
			'formatacao':'{:>2}  {:>3}  {:>2}   {:>1}'}


info_blocos['GL'] = {'campos':[
				'mnemonico',
				'cod',
				'ss',
				'sem',
				'geracao_p1',
				'dur_p1',
				'geracao_p2',
				'dur_p2',
				'geracao_p3',
				'dur_p3',
				'dia',
				'mes',
				'ano',
			],
			'regex':'(.{2})  (.{3})  (.{2})   (.{2})   (.{10})(.{5})(.{10})(.{5})(.{10})(.{5}) (.{2})(.{2})(.{4})(.*)',
			'formatacao':'{:>2}  {:>3}  {:>2}   {:>2}   {:>10}{:>5}{:>10}{:>5}{:>10}{:>5} {:>2}{:>2}{:>4}'}


def leituraArquivo(filePath):

    file = open(filePath, 'r', encoding='utf-8')
    arquivo = file.readlines()
    file.close()
    
    coment = []
    comentarios = {}
    blocos = {}
    for iLine in range(len(arquivo)):
        line = arquivo[iLine]
        if line[0] == '&':
            coment.append(line)
        elif line[0].strip() == '':
            continue
        else:
            mnemonico = line.split()[0]
            if mnemonico not in info_blocos:
                print(mnemonico)
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
    
    df_dadgnl = {}
    for mnemonico in blocos:
        df_dadgnl[mnemonico] = pd.DataFrame(blocos[mnemonico], columns=info_blocos[mnemonico]['campos'])
            
    return df_dadgnl, comentarios


def escrever_dadgnl(df_dadgnl, comentarios, path_saida):
    fileOut = codecs.open(path_saida, 'w', 'utf-8')
    for mnemonico in df_dadgnl:
        
        for index, row in df_dadgnl[mnemonico].iterrows():
            if index in comentarios[mnemonico]:
                for coment in comentarios[mnemonico][index]:
                    fileOut.write(coment)
            fileOut.write('{}\n'.format(info_blocos[mnemonico]['formatacao'].format(*row.values).strip()))

    fileOut.close()
    print(path_saida)
    return path_saida

def comentar_dadgnl_ons_ccee(df_dadgnl, comentarios):
    
    zerar_gl = []
    idx_inicio_gl = None
    for idx in comentarios['GL']:
        for coment in comentarios['GL'][idx]:
            if 'eletrica' in coment:
                idx_inicio_gl = idx
            elif 'merito'in coment and idx_inicio_gl != None:
                zerar_gl.append((idx_inicio_gl, idx-1))
                idx_inicio_gl = None
    
    if idx_inicio_gl != None:
        zerar_gl.append((idx_inicio_gl, df_dadgnl['GL'].shape[0]))
                
    for idx_i, idx_f in zerar_gl:
        df_dadgnl['GL'].loc[idx_i:idx_f, 'geracao_p1'] = '0.0'
        df_dadgnl['GL'].loc[idx_i:idx_f, 'geracao_p2'] = '0.0'
        df_dadgnl['GL'].loc[idx_i:idx_f, 'geracao_p3'] = '0.0'
        
    return df_dadgnl, comentarios



if __name__ == '__main__':
    
    path_gadgnl = os.path.abspath(r'C:\Users\cs341052\Downloads\deck_202207_r3\DC202207\DC202207-sem3\dadgnl.rv2')
    path_novo_dadgnl = path_gadgnl.replace('dadgnl.rv', 'dadgnl_rz2.rv')
    
    df_dadgnl, comentarios = leituraArquivo(path_gadgnl)       
    
    df_dadgnl, comentarios = comentar_dadgnl_ons_ccee(df_dadgnl, comentarios)
        
    escrever_dadgnl(df_dadgnl, comentarios, path_novo_dadgnl)