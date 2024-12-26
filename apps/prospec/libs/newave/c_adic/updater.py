import os
import re
import sys
import glob
import pdb
import pandas as pd
pd.options.mode.chained_assignment = None

import datetime
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.apps.prospec.libs import utils
from PMO.scripts_unificados.apps.prospec.libs.newave.c_adic import c_adic


def atualizar_carga_c_adic_NW(info_cargas_nw,paths_to_modify):
    
    paths_modified = []

    for path_c_adic in paths_to_modify:
        print(f"\n\n\nModificando arquivo {path_c_adic}")

        headers,extracted_carga = c_adic.leituraArquivo(path_c_adic)
        extracted_blocos=extracted_carga['carga_adicional_mensal']

        #LEITURA DO DECK
        folder_name = os.path.basename(os.path.dirname(path_c_adic))
        padrao_folder = r"NW(\d{4})(\d{2})"

        match = re.match(padrao_folder, folder_name)
        if match:
            ano = match.group(1)
            mes = match.group(2)
            dt_referente = f"{ano}{mes}"
        else:
            quit(f"Erro ao tentar extrair ano e mes do nome da pasta {folder_name}")
        
        if info_cargas_nw.get(dt_referente,pd.DataFrame()).empty:
            continue
        
        deck_date = datetime.datetime.strptime(dt_referente, "%Y%m")
        filtered_keys = [key for key in info_cargas_nw.keys() if datetime.datetime.strptime(key, "%Y%m") >= deck_date]

        #posicoes iniciais dos blocos de submercado
        index_initial_position = extracted_blocos[extracted_blocos['ano'].str.startswith('POS')].index +1
        submercado_sequence= ['ITAIPU','ANDE','SUDESTE','SUL','NORDESTE','BOA VISTA','NORTE']
        tamanho_bloco_submercado = 6

        for i,submercado in enumerate(submercado_sequence):
            if not submercado in info_cargas_nw[dt_referente]['SOURCE'].values: continue

            print(f"\nAtualizando {submercado}:")

            #blocos 'SUDESTE','SUL','NORDESTE','NORTE' serão alterados
            bloco_submercado = extracted_blocos.loc[(index_initial_position[i] - tamanho_bloco_submercado):index_initial_position[i]-1]
            for dt in filtered_keys:

                print(f"\t-Atualizando carga de {dt}")

                values = info_cargas_nw[dt].set_index('SOURCE')[['Base_CGH','Base_EOL','Base_UFV','Base_UTE']].sum(axis=1).to_dict()
                ano, mes = dt[:4],dt[4:]
                try:
                    column_mes = datetime.datetime.strptime(mes,'%m').strftime('%b')
                    bloco_submercado.loc[bloco_submercado['ano'].str.startswith(ano),column_mes] = f"{round(values[submercado]):>6}."
                except:
                    print(f"Erro ao tentar atualizar carga do submercado {submercado} no mes {mes} do ano {ano}")

            #copia ultimo (index -2) ano para o index "POS" (index -1)
            bloco_submercado.loc[index_initial_position[i]-1,'jan':'dez'] = bloco_submercado.loc[index_initial_position[i]-2,'jan':'dez']

        c_adic.escrever_arquivo(extracted_carga,headers,path_c_adic)
        paths_modified.append(path_c_adic)

    return paths_modified
    