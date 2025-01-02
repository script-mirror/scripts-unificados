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
from PMO.scripts_unificados.apps.prospec.libs.newave.sistema import sistema



# LOAD_sMMGD	MERCADO DE ENERGIA TOTAL (SISTEMA.DAT)

# Exp_CGH	PCH MMGD (SISTEMA.DAT)
# Exp_EOL	EOL MMGD (SISTEMA.DAT)
# Exp_UFV	UFV MMGD (SISTEMA.DAT)
# Exp_UTE	PCT MMGD (SISTEMA.DAT)



def atualizar_carga_sistema_NW(info_cargas_nw,paths_to_modify):

    paths_modified=[]
    for path_sistema in paths_to_modify:
        print(f"\n\n\nModificando arquivo {path_sistema}")

        extracted_carga = sistema.leituraArquivo(path_sistema)

        #LEITURA DO DECK
        folder_name = os.path.basename(os.path.dirname(path_sistema))
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


        bloco_total = 'MERCADO DE ENERGIA TOTAL'

        for cd_submercado in sistema.SUBMERCADOS_MAPPING.keys():
            print(f"\nAtualizando {sistema.SUBMERCADOS_MAPPING[cd_submercado]}:")

            df_energia = extracted_carga[bloco_total] 
            
            initial_position_submercado = df_energia.loc[df_energia[0]==str(cd_submercado)].index[0]
            tamanho_bloco_submercado = 6
            final_position_submercado = initial_position_submercado + tamanho_bloco_submercado

            bloco_submercado = df_energia.loc[initial_position_submercado : final_position_submercado ]
            for dt in filtered_keys:
                print(f"\t-Atualizando carga de {dt}")

                values = info_cargas_nw[dt].set_index('SOURCE')['LOAD_sMMGD'].to_dict()
                ano, mes = dt[:4],dt[4:]
                try:
                    bloco_submercado.loc[bloco_submercado[0]==ano,int(mes)] = f"{round(values[sistema.SUBMERCADOS_MAPPING[cd_submercado]]):>6}."
                except:
                    print(f"Erro ao tentar atualizar carga do submercado {sistema.SUBMERCADOS_MAPPING[cd_submercado]} no mes {mes} do ano {ano}")

            bloco_submercado.loc[final_position_submercado,1:12] = bloco_submercado.loc[final_position_submercado -1 ,1:12]
        

        df_energia.loc[df_energia[0].str.contains('POS'),0] = 'POS '
        values = df_energia.fillna('').apply(
                    lambda row: sistema.INFO_BLOCOS[bloco_total]['formatacao'].format(*row), 
                    axis=1
                    ).tolist()

        sistema.sobrescreve_bloco(
            path_to_modify = path_sistema,
            bloco=bloco_total,
            title=values[0],
            values=values[1:],
            skip_lines=len(values[1:])
            )
        paths_modified.append(path_sistema)

    return paths_modified


def atualizar_geracao_sistema_NW(info_cargas_nw,paths_to_modify):

    paths_modified = []
    for path_sistema in paths_to_modify:
        print(f"\n\n\nModificando arquivo {path_sistema}")

        extracted_carga = sistema.leituraArquivo(path_sistema)

        #LEITURA DO DECK
        folder_name = os.path.basename(os.path.dirname(path_sistema))
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

        bloco_total = 'GERACAO DE USINAS NAO SIMULADAS'
        for cd_submercado in sistema.SUBMERCADOS_MAPPING.keys():
            print(f"\nAtualizando {sistema.SUBMERCADOS_MAPPING[cd_submercado]}:")

            df_geracao = extracted_carga[bloco_total] 

            for geracao in sistema.GERACAO_MAPPING.keys():
                print(f"\t-Geracao {geracao} MMGD:")


                initial_position_submercado = df_geracao.loc[
                    (df_geracao[0]==str(cd_submercado)) & 
                    (df_geracao[3].str.contains('MMGD')) & 
                    (df_geracao[2].str.contains(geracao))
                    ].index[0]

                tamanho_bloco_submercado = 5
                final_position_submercado = initial_position_submercado + tamanho_bloco_submercado

                bloco_submercado = df_geracao.loc[initial_position_submercado : final_position_submercado ]
                for dt in filtered_keys:
                    print(f"\t\t-Atualizando {dt}")

                    values = info_cargas_nw[dt].set_index('SOURCE')[sistema.GERACAO_MAPPING[geracao]].to_dict()
                    ano, mes = dt[:4],dt[4:]

                    try:
                        bloco_submercado.loc[bloco_submercado[0]==ano,int(mes)] = f"{round(values[sistema.SUBMERCADOS_MAPPING[cd_submercado]],2):>7.2f}"
                    except:
                        print(f"Erro ao tentar atualizar carga do submercado {sistema.SUBMERCADOS_MAPPING[cd_submercado]} no mes {mes} do ano {ano}")

        def formatar_linha(row, index):
            formatacao_descricao = '{:>4}   {:>2}  {:>3} {:<4}'
            row = row.str.strip()
            if index % 6 == 0:
                return formatacao_descricao.format(*row)
            else:
                return sistema.INFO_BLOCOS[bloco_total]['formatacao'].format(*row)

        values = df_geracao.loc[1:].reset_index(drop=True).fillna('').apply(
            lambda row: formatar_linha(row, row.name), axis=1
        ).tolist()
        values.insert(0, sistema.INFO_BLOCOS[bloco_total]['formatacao'].format(*df_geracao.loc[0].str.strip()))

        sistema.sobrescreve_bloco(
            path_to_modify = path_sistema,
            bloco=bloco_total,
            title=values[0],
            values=values[1:],
            skip_lines=len(values[1:])
            )

        paths_modified.append(path_sistema)

    return paths_modified
    