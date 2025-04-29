import os
import re
import pdb
import sys
import glob
import shutil
import logging
import datetime
import pandas as pd
from typing import List
from io import StringIO

sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.apps.prospec.libs import utils
from PMO.scripts_unificados.apps.prospec.prospec import RzProspec
from PMO.scripts_unificados.apps.prospec.libs.newave.clast import updater as clast_updater
from PMO.scripts_unificados.apps.prospec.libs.decomp.dadger import updater as dadger_updater
from PMO.scripts_unificados.apps.prospec.libs.newave.c_adic import updater as c_adic_updater
from PMO.scripts_unificados.apps.prospec.libs.newave.sistema import updater as sistema_updater
from PMO.scripts_unificados.apps.prospec.libs.info_arquivos_externos import info_external_files

logging.basicConfig(level=logging.INFO,
                    format='%(levelname)s:\t%(asctime)s\t %(name)s.py:%(lineno)d\t %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers=[
                        logging.StreamHandler()
                    ])

logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger(__name__)

api = RzProspec()

def get_ids_to_modify():

    path = "/WX2TB/Documentos/fontes/PMO/API_Prospec/ConfigProspecAPI/ConfigRodadaDiaria.csv"
    with open(path, 'r') as file:
        csv_text = file.read()
    
    csv_text = csv_text[:csv_text.find('prospecStudyIdToAssociate')]
    df = pd.read_csv(StringIO(csv_text), sep=';')
    df.columns = df.columns.str.strip()
    df.set_index('username', inplace=True)
    ids_mask = df.index[df.index.str.startswith('prospecStudyIdToDuplicate')]
    ids_to_modify = df.loc[ids_mask,df.columns[0]].values.tolist()
    return list(set(ids_to_modify))


def send_files_to_api(id_estudo:int, paths_modified:List[str], tag:str):

    info_estudo = api.getInfoRodadaPorId(id_estudo)
    df_estudo = pd.DataFrame(info_estudo['Decks'])

    patterns = [r"NW(\d{6})", r"DC\d{6}-sem\d"]

    api.update_tags(id_estudo, tag, "#FFF", "#44F")
    for path in paths_modified:
        
        match = re.search(patterns[0], path)
        if not match: match = re.search(patterns[1], path)
            
        nome_estudo = match.group() + ".zip"
        id_deck = int(df_estudo['Id'][df_estudo['FileName'] == nome_estudo].values[0])

        endpoint = f'/api/prospectiveStudies/{id_estudo}/UploadFiles?deckId={id_deck}'
        arquivo_enviado = api.sendFile(endpoint, path)

        if 'filesUploaded' in arquivo_enviado:
            logger.info(f'{arquivo_enviado["filesUploaded"][0]} - OK')
        else:
            logger.info(f'Falha ao enviar estudo {id_estudo}')

#DECOMP
def update_cvu_dadger_dc_estudo(
    fontes_to_search:List[str],
    dt_atualizacao:datetime.datetime,
    ids_to_modify:List[int]=None
    ):

    tag = [f'CVU {datetime.datetime.now().strftime("%d/%m %H:%M")}']
    if not ids_to_modify:
        ids_to_modify = get_ids_to_modify()

    # ORGANIZA INFORMACOES DE CVU
    info_cvu = info_external_files.organizar_info_cvu(
        fontes_to_search=fontes_to_search,
        dt_atualizacao=dt_atualizacao
        )

    for id_estudo in ids_to_modify:

        logger.info("\n\n")
        logger.info(f"Modificando estudo {id_estudo}")

        path_to_modify = api.downloadEstudoPorId(id_estudo)


        extracted_zip_estudo = utils.extract_file_estudo(
            path_to_modify,
            ) 

        #ALTERAR CVU EM DECKS DC
        dadgers_to_modify = glob.glob(os.path.join(extracted_zip_estudo,"**",f"*dadger*"),recursive=True)
        arquivos_filtrados = [arquivo for arquivo in dadgers_to_modify if not re.search(r'\.0+$', arquivo)]
        if arquivos_filtrados == []:
            raise Exception(f"Não foi encontrado nenhum arquivo dadger no estudo {id_estudo}")
        paths_modified = dadger_updater.atualizar_cvu_DC(
            info_cvu,
            arquivos_filtrados
            )

        send_files_to_api(id_estudo, paths_modified, tag)

        logger.info(f"============================================")

def update_carga_dadger_dc_estudo(file_path:str,ids_to_modify:List[int]=None):

    tag = [f'CARGA-DC {datetime.datetime.now().strftime("%d/%m %H:%M")}']
    if not ids_to_modify:
        ids_to_modify = get_ids_to_modify()

    for id_estudo in ids_to_modify:
        logger.info("\n\n")
        logger.info(f"Modificando estudo {id_estudo}")

        path_to_modify = api.downloadEstudoPorId(id_estudo)

        extracted_zip_estudo = utils.extract_file_estudo(
            path_to_modify,
            ) 

        info_cargas = info_external_files.organizar_info_carga(
            file_path,
            extracted_zip_estudo,
            )

        # #ALTERAR CARGA EM DECKS DC
        dadgers_to_modify = glob.glob(os.path.join(extracted_zip_estudo,"**",f"*dadger*"),recursive=True)
        arquivos_filtrados = [arquivo for arquivo in dadgers_to_modify if not re.search(r'\.0+$', arquivo)]
        if arquivos_filtrados == []:
            raise Exception(f"Não foi encontrado nenhum arquivo dadger no estudo {id_estudo}")
        paths_modified = dadger_updater.atualizar_carga_DC(
            info_cargas,
            arquivos_filtrados
            )
        print(f"nao enviando arquivos {paths_modified}")
        # send_files_to_api(id_estudo, paths_modified, tag)

        logger.info(f"============================================")
        
def update_weol_dadger_dc_estudo(data_produto:datetime.date, ids_to_modify:List[int] = None):
    logger.info(f"UPDATE DADGER DECOMP")
    tag = [f'WEOL {datetime.datetime.now().strftime("%d/%m %H:%M")}']
    if not ids_to_modify:
        ids_to_modify = get_ids_to_modify()

    for id_estudo in ids_to_modify:
        logger.info("\n\n")
        logger.info(f"Modificando estudo {id_estudo}")

        path_estudo = api.downloadEstudoPorId(id_estudo)

        extracted_zip_estudo = utils.extract_file_estudo(
            path_estudo,
            )

        dadgers_to_modify = glob.glob(os.path.join(extracted_zip_estudo,"**",f"*dadger*"),recursive=True)
        arquivos_filtrados = [arquivo for arquivo in dadgers_to_modify if not re.search(r'\.0+$', arquivo)]
        if arquivos_filtrados == []:
            raise Exception(f"Não foi encontrado nenhum arquivo dadger no estudo {id_estudo}")
        dadger_updater.update_eolica_DC(
            arquivos_filtrados,
            data_produto
            )

        send_files_to_api(id_estudo, arquivos_filtrados, tag)

        logger.info(f"============================================")
        
def update_carga_pq_dadger_dc_estudo(data_produto:datetime.date, ids_to_modify:List[int] = None):
    logger.info(f"UPDATE DADGER DECOMP")
    tag = [f'GD-DC {datetime.datetime.now().strftime("%d/%m %H:%M")}']
    if not ids_to_modify:
        ids_to_modify = get_ids_to_modify()

    for id_estudo in ids_to_modify:
        logger.info("\n\n")
        logger.info(f"Modificando estudo {id_estudo}")

        path_estudo = api.downloadEstudoPorId(id_estudo)

        extracted_zip_estudo = utils.extract_file_estudo(
            path_estudo,
            )

        dadgers_to_modify = glob.glob(os.path.join(extracted_zip_estudo,"**",f"*dadger*"),recursive=True)
        arquivos_filtrados = [arquivo for arquivo in dadgers_to_modify if not re.search(r'\.0+$', arquivo)]
        if arquivos_filtrados == []:
            raise Exception(f"Não foi encontrado nenhum arquivo dadger no estudo {id_estudo}")
        dadger_updater.update_carga_pq_dc(
            arquivos_filtrados,
            data_produto
            )
        pdb.set_trace()
        send_files_to_api(id_estudo, arquivos_filtrados, tag)

        logger.info(f"============================================")

#NEWAVE
def update_cvu_clast_nw_estudo(
    fontes_to_search:List[str],
    dt_atualizacao:datetime.datetime,
    ids_to_modify:List[int]=None
    ):
    apenas_estrutural = False
    if apenas_estrutural:
        fontes_to_search = [x for x in fontes_to_search if "estrutural" in x.lower()]
    tag = [f'CVU {datetime.datetime.now().strftime("%d/%m %H:%M")}']
    if not ids_to_modify:
        ids_to_modify = get_ids_to_modify()

    # ORGANIZA INFORMACOES DE CVU
    info_cvu = info_external_files.organizar_info_cvu(
        fontes_to_search=fontes_to_search,
        dt_atualizacao=dt_atualizacao
        )

    for id_estudo in ids_to_modify:

        logger.info("\n\n")
        logger.info(f"Modificando estudo {id_estudo}")

        path_to_modify = api.downloadEstudoPorId(id_estudo)


        extracted_zip_estudo = utils.extract_file_estudo(
            path_to_modify,
            )
        pasta_nw = [
        nome for nome in os.listdir(extracted_zip_estudo)
        if os.path.isdir(os.path.join(extracted_zip_estudo, nome)) and nome.startswith("NW")
        ]
        if not pasta_nw:
            logger.info(f"Estudo {id_estudo} nao possui NW")
            continue


        clast_to_modify = glob.glob(os.path.join(extracted_zip_estudo,"**",f"*clast*"),recursive=True)
        arquivos_filtrados = [arquivo for arquivo in clast_to_modify if not re.search(r'\.0+$', arquivo)]
        if arquivos_filtrados == []:
            raise Exception(f"Não foi encontrado nenhum arquivo clas no estudo {id_estudo}")
        paths_modified = clast_updater.atualizar_cvu_NW(
            info_cvu,
            clast_to_modify,
            apenas_estrutural
            )
        send_files_to_api(id_estudo, paths_modified, tag)

        logger.info(f"============================================")

def update_carga_c_adic_nw_estudo(file_path:str,ids_to_modify:List[int]=None):

    tag = [f'CARGA-NW {datetime.datetime.now().strftime("%d/%m %H:%M")}']
    # if not ids_to_modify:
    #     ids_to_modify = get_ids_to_modify()

    info_cargas_nw = info_external_files.organizar_info_carga_nw(
            file_path,
            )
    initial_info_carga_date = sorted(info_cargas_nw.keys())[0]

    for id_estudo in ids_to_modify:

        logger.info(f"\n\nModificando estudo {id_estudo}...")

        path_to_modify = api.downloadEstudoPorId(id_estudo)

        extracted_zip_estudo = utils.extract_file_estudo(
            path_to_modify,
            ) 
        pasta_nw = [
        nome for nome in os.listdir(extracted_zip_estudo)
        if os.path.isdir(os.path.join(extracted_zip_estudo, nome)) and nome.startswith("NW")
        ]
        if not pasta_nw:
            logger.info(f"Estudo {id_estudo} nao possui NW")
            continue


        c_adic_to_modify = glob.glob(os.path.join(extracted_zip_estudo,"**",f"*c_adic*"),recursive=True)
        if c_adic_to_modify == []:
            raise Exception(f"Não foi encontrado nenhum arquivo cadic no estudo {id_estudo}")
        if 'carga_mensal' in os.path.basename(file_path).lower():

            intial_deck_date = sorted([os.path.basename(os.path.dirname(dir))[2:] for dir in c_adic_to_modify])[0] 
            
            if not intial_deck_date == initial_info_carga_date:
                logger.info(f'''
                    A data referente do arquivo de carga {os.path.basename(file_path)} 
                    Não é compativel com a data do Deck inicial NW{intial_deck_date}.
                ''')
                continue
        
        paths_modified = c_adic_updater.atualizar_carga_c_adic_NW(
            info_cargas_nw,
            c_adic_to_modify
        )

        send_files_to_api(id_estudo, paths_modified, tag)

        logger.info(f"============================================")

def update_carga_sistema_nw_estudo(file_path:str,ids_to_modify:List[int]=None):

    tag = [f'CARGA-NW {datetime.datetime.now().strftime("%d/%m %H:%M")}']
    # if not ids_to_modify:
    #     ids_to_modify = get_ids_to_modify()

    info_cargas_nw = info_external_files.organizar_info_carga_nw(
            file_path,
            )
    initial_info_carga_date = sorted(info_cargas_nw.keys())[0]

    for id_estudo in ids_to_modify:

        logger.info(f"\n\nModificando estudo {id_estudo}...")

        path_to_modify = api.downloadEstudoPorId(id_estudo)

        extracted_zip_estudo = utils.extract_file_estudo(
            path_to_modify,
            ) 
        pasta_nw = [
        nome for nome in os.listdir(extracted_zip_estudo)
        if os.path.isdir(os.path.join(extracted_zip_estudo, nome)) and nome.startswith("NW")
        ]
        if not pasta_nw:
            logger.info(f"Estudo {id_estudo} nao possui NW")
            continue

        paths_sistema_to_modify = glob.glob(os.path.join(extracted_zip_estudo,"**",f"*sistema*"),recursive=True)
        if paths_sistema_to_modify == []:
            raise Exception(f"Não foi encontrado nenhum arquivo sistema no estudo {id_estudo}")
        if 'carga_mensal' in os.path.basename(file_path).lower():

            intial_deck_date = sorted([os.path.basename(os.path.dirname(dir))[2:] for dir in paths_sistema_to_modify])[0] 
            
            if not intial_deck_date == initial_info_carga_date:
                logger.info(f'''
                    A data referente do arquivo de carga {os.path.basename(file_path)} 
                    Não é compativel com a data do Deck inicial NW{intial_deck_date}.
                ''')
                continue

        paths_modified = sistema_updater.atualizar_carga_sistema_NW(
            info_cargas_nw,
            paths_sistema_to_modify
        )
        paths_modified = sistema_updater.atualizar_geracao_sistema_NW(
            info_cargas_nw,
            paths_sistema_to_modify
        )

        send_files_to_api(id_estudo, paths_modified, tag)

        logger.info(f"============================================")
        
def update_weol_sistema_nw_estudo(data_produto:datetime.date, ids_to_modify:List[int] = None):
    logger.info(f"UPDATE SISTEMA NEWAVE")
    tag = [f'WEOL {datetime.datetime.now().strftime("%d/%m %H:%M")}']
    if not ids_to_modify:
        ids_to_modify = get_ids_to_modify()

    for id_estudo in ids_to_modify:
        logger.info("\n\n")
        logger.info(f"Modificando estudo {id_estudo}")

        path_estudo = api.downloadEstudoPorId(id_estudo)

        extracted_zip_estudo = utils.extract_file_estudo(
            path_estudo,
            )
        pasta_nw = [
        nome for nome in os.listdir(extracted_zip_estudo)
        if os.path.isdir(os.path.join(extracted_zip_estudo, nome)) and nome.startswith("NW")
        ]
        if not pasta_nw:
            logger.info(f"Estudo {id_estudo} nao possui NW")
            continue

        paths_sistema_to_modify = glob.glob(os.path.join(extracted_zip_estudo,"**",f"*sistema*"),recursive=True)
        if paths_sistema_to_modify == []:
            raise Exception(f"Não foi encontrado nenhum arquivo sistema no estudo {id_estudo}")
        sistema_updater.update_weol_sistema(
            data_produto,
            paths_sistema_to_modify
            )
        send_files_to_api(id_estudo, paths_sistema_to_modify, tag)

        logger.info(f"============================================")
        
def update_restricoes_dadger_dc_estudo(file_path:str, ids_to_modify:List[int] = None):
    
    info_restricoes = info_external_files.organizar_info_restricoes_eletricas_dc(file_path)
    logger.info(f"UPDATE DADGER RESTRICOES (bloco LU)")
    tag = [f'RE {datetime.datetime.now().strftime("%d/%m %H:%M")}']

    if not ids_to_modify:
        ids_to_modify = get_ids_to_modify()

    for id_estudo in ids_to_modify:
        logger.info("\n\n")
        logger.info(f"Modificando estudo {id_estudo}")

        path_estudo = api.downloadEstudoPorId(id_estudo)

        extracted_zip_estudo = utils.extract_file_estudo(
            path_estudo,
            )

        dadgers_to_modify = glob.glob(os.path.join(extracted_zip_estudo,"**",f"*dadger*"),recursive=True)
        arquivos_filtrados = [arquivo for arquivo in dadgers_to_modify if not re.search(r'\.0+$', arquivo)]
        if arquivos_filtrados == []:
            raise Exception(f"Não foi encontrado nenhum arquivo dadger no estudo {id_estudo}")

        dadger_updater.update_restricoes_eletricas_DC(
            info_restricoes,
            arquivos_filtrados
        )

        send_files_to_api(id_estudo, arquivos_filtrados, tag)

        logger.info(f"============================================")
        


if __name__ == "__main__":
    # update_weol_sistema_nw_estudo(
    #     datetime.date(2025, 1, 22),
    #     [22800]
    #     )
    # ids_to_modify = get_ids_to_modify()

    # for id_estudo in ids_to_modify:
    #     logger.info("\n\n")
    #     logger.info(f"Modificando estudo {id_estudo}")

    #     path_to_modify = api.downloadEstudoPorId(id_estudo)
    # update_weol_dadger_dc_estudo(datetime.date(2024, 12, 19))
    # ids_to_modify = [22152]
    # file_path=r"C:\Users\CS399274\Downloads\RV0_PMO_Dezembro_2024_carga_semanal.zip"

    # ano_referencia_cvu=2024
    # mes_referencia_cvu=11

    # update_cvu_estudo(ids_to_modify,ano_referencia_cvu,mes_referencia_cvu)
    # update_carga_estudo(ids_to_modify,file_path)

    update_carga_pq_dadger_dc_estudo(datetime.date(2025, 4, 26))
    