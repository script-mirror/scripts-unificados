import os
import re
import sys
import pdb
import glob
import logging
import datetime
import warnings
import pandas as pd
from typing import List

warnings.simplefilter(action='ignore', category=FutureWarning)
sys.path.insert(1,"/WX2TB/Documentos/fontes/")

from PMO.scripts_unificados.apps.prospec.libs import utils
from PMO.scripts_unificados.apps.prospec.libs.decomp.dadger import dadger
from PMO.scripts_unificados.apps.prospec.libs.info_arquivos_externos import info_external_files


logging.basicConfig(level=logging.DEBUG,
                    format='%(levelname)s:\t%(asctime)s\t %(name)s.py:%(lineno)d\t %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers=[
                        logging.StreamHandler()
                    ])
logger = logging.getLogger(__name__)


def atualizar_cvu_DC(info_cvu,paths_to_modify):

    paths_modified=[]
    for path_dadger in paths_to_modify:

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

        if not info_cvu.get(dt_referente):
            continue

        logger.info(f"\n\n\nModificando arquivo {path_dadger}")

        cvu_map = info_cvu[dt_referente]['conjuntural'].set_index("CÓDIGO")["CVU CONJUNTURAL"].to_dict()
        cvu_map = {key : round(value,2) for key, value in cvu_map.items()}
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
        paths_modified.append(path_dadger)

    return paths_modified
        

def atualizar_carga_DC(info_cargas,paths_to_modify):
    
    paths_modified=[]

    for path_dadger in paths_to_modify:

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
        
        if not info_cargas.get(dt_referente):
            continue
        
        if info_cargas[dt_referente].get(int(semana)- 1,pd.DataFrame()).empty:
            continue

        logger.info(f"\n\n\nModificando arquivo {path_dadger}")

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

        paths_modified.append(path_dadger)

    return paths_modified
            
def update_eolica_DC(paths_to_modify:List[str], data_produto:datetime.date):
    info_eolica = info_external_files.organizar_info_eolica(
        paths_to_modify,
        data_produto
        )
    for path_dadger in info_eolica:
        bloco = info_eolica[path_dadger].split('\n')
        try:
            dadger.sobrescreve_bloco(
                path_to_modify=path_dadger,
                mnemonico_bloco='PQ',
                values=bloco,
                skip_lines=len(bloco)
            )
            logger.info(f"Bloco {path_dadger} sobrescrito com sucesso")
        except Exception as e:
            logger.error(f"Erro ao tentar sobrescrever bloco {path_dadger}: {str(e)}")
            continue    

if __name__ == "__main__":

    update_eolica_DC([
        '/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/prospec/libs/info_arquivos_externos/tmp/Estudo_21904_Entrada/DC202411-sem4/dadger.rv3',
        '/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/prospec/libs/info_arquivos_externos/tmp/Estudo_21904_Entrada/DC202411-sem5/dadger.rv4',
        '/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/prospec/libs/info_arquivos_externos/tmp/Estudo_21904_Entrada/DC202412-sem1/dadger.rv0'
        ], datetime.date(2025,11,16))
    path_saida = "/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/prospec/libs/info_arquivos_externos/tmp"

    #EXTRAINDO ZIP
    file_estudo = "/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/verificadores/ccee/Estudo_21904_Entrada.zip"
    extracted_zip_estudo = utils.extract_file_estudo(
        file_estudo,
        path_saida,
        )

    # ORGANIZA INFORMACOES DE CVU
    # info_cvu = info_external_files.organizar_info_cvu(
    #     ano_referencia=2024,    
    #     mes_referencia=11,
    #     path_saida=path_saida
    #     )

    # #ALTERAR CVU EM DECKS DC
    # paths_to_modify = glob.glob(os.path.join(extracted_zip_estudo,"**",f"*dadger*"),recursive=True)
    # atualizar_cvu_DC(
    #     info_cvu,
    #     paths_to_modify
    #     )

    # #ORGANIZA INFORMACOES DE CARGA
    # path_carga_zip= "/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/verificadores/ccee/RV3_PMO_Novembro_2024_carga_semanal.zip"
    # info_cargas = info_external_files.organizar_info_carga(
    #     path_carga_zip,
    #     extracted_zip_estudo,
    #     path_saida
    #     )
    # #ALTERAR CARGA EM DECKS DC
    # paths_to_modify = glob.glob(os.path.join(extracted_zip_estudo,"**",f"*dadger*"),recursive=True)
    # atualizar_carga_DC(
    #     info_cargas,
    #     paths_to_modify
    # )
    
    path_eolica_zip= "/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/verificadores/ccee/RV3_PMO_Novembro_2024_carga_semanal.zip"
    paths_dadgers = glob.glob(os.path.join(extracted_zip_estudo,"**",f"*dadger*"),recursive=True)
    pdb.set_trace()
    info_eolica = info_external_files.organizar_info_eolica(
        paths_dadgers,
        path_saida
        )
    
    # atualizar_eolica_DC(
    #     info_eolica,
    #     paths_to_modify
    # )