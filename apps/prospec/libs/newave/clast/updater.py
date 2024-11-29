import os
import re
import sys
import pdb
import pandas as pd

sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.apps.prospec.libs.newave.clast import clast



def atualizar_cvu_NW(info_cvu,paths_to_modify):


    for path_clast in paths_to_modify:
        print(f"\n\n\nModificando arquivo {path_clast}")

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

        if not info_cvu.get(dt_referente):
            continue

        #SEPARANDO BLOCOS DE CLAST
        df_clast_completo = pd.read_fwf(path_clast,sep=';',encoding='latin1')
        df_clast_completo['NUM'] = df_clast_completo['NUM'].astype(str)
        index_separacao_blocos = df_clast_completo[df_clast_completo['NUM']== '9999'].index[0]

        #COMPLETANDO BLOCO DE CLAST ESTRUTURAL
        df_clast_estrutural = df_clast_completo.iloc[:index_separacao_blocos].copy()
        cvu_map = info_cvu[dt_referente]['estrutural'].set_index("CÓDIGO")["CVU ESTRUTURAL"].round(2).to_dict()
        for col in ["CUSTO", "CUSTO.1", "CUSTO.2", "CUSTO.3", "CUSTO.4"]:
            df_clast_estrutural[col].update(df_clast_estrutural["NUM"].map(cvu_map))

        #COMPLETANDO BLOCO DE CLAST CONJUNTURAL
        df_clast_conjuntural = pd.read_fwf(path_clast, skiprows = index_separacao_blocos + 2)
        cvu_map = info_cvu[dt_referente]['conjuntural'].set_index("CÓDIGO")["CVU CONJUNTURAL"].round(2).to_dict()
        for col in ["CUSTO"]:
            df_clast_conjuntural[col].update(df_clast_conjuntural["NUM"].map(cvu_map))

        clast.sobrescreve_clast_file(
            output_path = path_clast,
            df_estrutural = df_clast_estrutural.fillna(''),
            df_conjuntural = df_clast_conjuntural.fillna('')
            )

if __name__ == "__main__":
    from PMO.scripts_unificados.apps.prospec.libs import utils
    from PMO.scripts_unificados.apps.prospec.libs.info_arquivos_externos import info_external_files


    import glob
    path_saida = "C:/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/prospec/libs/info_arquivos_externos/tmp"

    #EXTRAINDO ZIP
    file_estudo = "/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/verificadores/ccee/Estudo_21904_Entrada.zip"
    extracted_zip_estudo = utils.extract_file_estudo(
        file_estudo,
        path_saida,
        )

    # ORGANIZA INFORMACOES DE CVU
    info_cvu = info_external_files.organizar_info_cvu(
        ano_referencia=2024,    
        mes_referencia=11,
        path_saida=path_saida
        )

    #ALTERAR CVU EM DECKS DC
    paths_to_modify = glob.glob(os.path.join(extracted_zip_estudo,"**",f"*clast*"),recursive=True)
    atualizar_cvu_NW(
        info_cvu,
        paths_to_modify
        )