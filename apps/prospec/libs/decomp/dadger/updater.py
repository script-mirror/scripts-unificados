import os
import re
import sys
import glob
import pdb
import pandas as pd

sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.apps.prospec.libs import utils
from PMO.scripts_unificados.apps.prospec.libs.decomp.dadger import dadger
from PMO.scripts_unificados.apps.prospec.libs.info_arquivos_externos import info_external_files



def atualizar_cvu_DC(info_cvu,paths_to_modify):
    for path_dadger in paths_to_modify:
        print(f"\n\n\nModificando arquivo {path_dadger}")

        extracted_blocos_dadger, headers = dadger.leituraArquivo(path_dadger)

        #LEITURA DO DECK 
        folder_name = os.path.basename(os.path.dirname(path_dadger))
        padrao_folder = r"DC(\d{4})(\d{2})-sem(\d{1})"

        match = re.match(padrao_folder, folder_name)
        if match:
            ano = match.group(1)
            mes = match.group(2)
            semana = match.group(3)
            dt_referente = f"{ano}{mes}"

        else:
            quit(f"Erro ao tentar extrair ano e mes do nome da pasta {folder_name}")

        cvu_map = info_cvu[dt_referente]['conjuntural'].set_index("CÓDIGO")["CVU CONJUNTURAL"].to_dict()
        extracted_blocos_dadger['CT'] = utils.trim_df(extracted_blocos_dadger['CT'])

        for col in ["cvu_p1", "cvu_p2", "cvu_pat3"]:
            extracted_blocos_dadger['CT'][col].update(extracted_blocos_dadger['CT']["cod"].map(cvu_map))
        
        formatacao = '{:>2}  {:>3}  {:>2}   {:<10}{:>2}   {:>5}{:>5}{:>10}{:>5}{:>5}{:>10}{:>5}{:>5}{:>10}'
        linhas_formatadas = extracted_blocos_dadger['CT'].apply(
            lambda row: formatacao.format(*row), 
            axis=1
            ).tolist()

        dadger.sobrescreve_bloco(
            path_to_modify=path_dadger,
            mnemonico_bloco='CT',
            values=linhas_formatadas,
            skip_lines=len(extracted_blocos_dadger['CT'])
            )

def atualizar_carga_DC(info_cargas,paths_to_modify):
    
    for path_dadger in paths_to_modify:
        print(f"\n\n\nModificando arquivo {path_dadger}")

        #LEITURA DO DECK 
        folder_name = os.path.basename(os.path.dirname(path_dadger))
        padrao_folder = r"DC(\d{4})(\d{2})-sem(\d{1})"

        match = re.match(padrao_folder, folder_name)
        if match:
            ano = match.group(1)
            mes = match.group(2)
            semana = match.group(3)
            dt_referente = f"{ano}{mes}"

        else:
            quit(f"Erro ao tentar extrair ano e mes do nome da pasta {folder_name}")
        
        formatacao = '{:>2}  {:>2}   {:>2}  {:>3}   {:>10}{:>10}{:>10}{:>10}{:>10}{:>10}'
        linhas_formatadas = info_cargas[dt_referente][int(semana) - 1].apply(
            lambda row: formatacao.format(*row), 
            axis=1
            ).tolist()

        linhas_formatadas = utils.adicionar_simbolo(
            linhas_formatadas,
            simbolos_sequenciais=['&'],
            intervalo_linhas=5
        )
        dadger.sobrescreve_bloco(
            path_to_modify=path_dadger,
            mnemonico_bloco='DP',
            values=linhas_formatadas,
            skip_lines=len(linhas_formatadas)
            )
            


if __name__ == "__main__":

    
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
    paths_to_modify = glob.glob(os.path.join(extracted_zip_estudo,"**",f"*dadger*"),recursive=True)
    atualizar_cvu_DC(
        info_cvu,
        paths_to_modify
        )

    #ORGANIZA INFORMACOES DE CARGA
    path_carga_zip= "/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/verificadores/ccee/RV3_PMO_Novembro_2024_carga_semanal.zip"
    info_cargas = info_external_files.organizar_info_carga(
        path_carga_zip,
        extracted_zip_estudo,
        path_saida
        )
    #ALTERAR CARGA EM DECKS DC
    paths_to_modify = glob.glob(os.path.join(extracted_zip_estudo,"**",f"*dadger*"),recursive=True)
    atualizar_carga_DC(
        info_cargas,
        paths_to_modify
    )