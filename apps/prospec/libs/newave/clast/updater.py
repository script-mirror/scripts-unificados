import os
import re
import sys
import pdb
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.apps.prospec.libs.newave.clast import clast



def atualizar_cvu_NW(info_cvu,paths_to_modify, tipos_cvu):

    paths_modified = []
    for path_clast in paths_to_modify:

        #LEITURA DO DECK
        folder_name = os.path.basename(os.path.dirname(path_clast))
        padrao_folder = r"NW(\d{4})(\d{2})"

        match = re.match(padrao_folder, folder_name)
        if match:
            ano = match.group(1)
            mes = match.group(2)
            dt_referente = f"{ano}{mes}"
        else:
            quit(f"Erro ao tentar extrair ano e mes do nome da pasta {folder_name}")

        print(f"\n\n\nModificando arquivo {path_clast}")

        if dt_referente not in info_cvu['mes_referencia'].unique():
           dt_referente = max(info_cvu['mes_referencia'])


        #SEPARANDO BLOCOS DE CLAST
        df_clast_completo = pd.read_fwf(path_clast,sep=';',encoding='latin1')
        df_clast_completo['NUM'] = df_clast_completo['NUM'].astype(str)
        index_separacao_blocos = df_clast_completo[df_clast_completo['NUM']== '9999'].index[0]

        #COMPLETANDO BLOCO DE CLAST ESTRUTURAL
        df_clast_estrutural = df_clast_completo.iloc[:index_separacao_blocos].copy()

        info_cvu['cd_usina'] = info_cvu['cd_usina'].astype(str)

        if "estrutural" in tipos_cvu:
        
            df_cvu_map = info_cvu.set_index(['mes_referencia','tipo_cvu']).loc[(dt_referente,'estrutural')]
            df_cvu_map = df_cvu_map.pivot_table(index=['mes_referencia', 'tipo_cvu', 'cd_usina', 'dt_atualizacao'], 
                                columns='ano_horizonte', 
                                values='vl_cvu').reset_index()

            for i,col in enumerate(["CUSTO", "CUSTO.1", "CUSTO.2", "CUSTO.3", "CUSTO.4"]):
                cvu_map = df_cvu_map.set_index('cd_usina')[df_cvu_map.columns[-5:][i]].round(2).to_dict()
                df_clast_estrutural[col].update(df_clast_estrutural["NUM"].map(cvu_map))

        #COMPLETANDO BLOCO DE CLAST CONJUNTURAL
        df_clast_conjuntural = pd.read_fwf(path_clast, skiprows = index_separacao_blocos + 2)
        
        if "conjuntural" in tipos_cvu:
            cvu_map = info_cvu.set_index(['mes_referencia','tipo_cvu']).loc[(dt_referente,'conjuntural')].set_index("cd_usina")["vl_cvu"].round(2).to_dict()
            for col in ["CUSTO"]:
                df_clast_conjuntural[col].update(df_clast_conjuntural["NUM"].map(cvu_map))

        clast.sobrescreve_clast_file(
            output_path = path_clast,
            df_estrutural = df_clast_estrutural.fillna(''),
            df_conjuntural = df_clast_conjuntural.fillna('')
            )
        
        paths_modified.append(path_clast)
    return paths_modified

if __name__ == "__main__":
    from PMO.scripts_unificados.apps.prospec.libs import utils
    from PMO.scripts_unificados.apps.prospec.libs.info_arquivos_externos import info_external_files


    import glob
    path_saida = "C:/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/prospec/libs/info_arquivos_externos/tmp"

    #EXTRAINDO ZIP
    # extracted_zip_estudo = utils.extract_file_estudo(
    #     file_estudo,
    #     path_saida,
    #     )

    # extracted_zip_estudo = "/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/prospec/libs/info_arquivos_externos/tmp/Estudo_22971"
    # ORGANIZA INFORMACOES DE CVU
    info_cvu = info_external_files.organizar_info_cvu(
        titles_cvu_ccee=[
            'custo_variavel_unitario_conjuntural_revisado',
            'custo_variavel_unitario_estrutural',
            'custo_variavel_unitario_conjuntural',
            'custo_variavel_unitario_merchant'
            ],
        )

    extracted_zip_estudo="C:/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/prospec/libs/info_arquivos_externos/tmp/Estudo_23188_Entrada"

    #ALTERAR CVU EM DECKS DC
    paths_to_modify = glob.glob(os.path.join(extracted_zip_estudo,"**",f"*clast*"),recursive=True)
    atualizar_cvu_NW(
        info_cvu,
        paths_to_modify
        )