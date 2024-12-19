
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
from PMO.scripts_unificados.apps.prospec.libs.info_arquivos_externos import info_external_files

logging.basicConfig(level=logging.DEBUG,
                    format='%(levelname)s:\t%(asctime)s\t %(name)s.py:%(lineno)d\t %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers=[
                        logging.StreamHandler()
                    ])

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


def send_files_to_api(idEstudo, pathEstudo, deckId):

    endpoint = f'/api/prospectiveStudies/{idEstudo}/UploadFiles?deckId={deckId}'
    arquivo_enviado = api.sendFile(endpoint, pathEstudo)

    if 'filesUploaded' in arquivo_enviado:
        logger.info(f'{arquivo_enviado["filesUploaded"][0]} - OK')
    else:
        logger.info(f'Falha ao enviar estudo {idEstudo}')


def update_cvu_estudo(ids_to_modify,ano_referencia_cvu,mes_referencia_cvu):

    # ORGANIZA INFORMACOES DE CVU
    info_cvu = info_external_files.organizar_info_cvu(
        ano_referencia=ano_referencia_cvu,    
        mes_referencia=mes_referencia_cvu,
        )

    for id_estudo in ids_to_modify:

        print("\n\n")
        logger.info(f"Modificando estudo {id_estudo}")

        path_to_modify = api.downloadEstudoPorId(id_estudo)


        extracted_zip_estudo = utils.extract_file_estudo(
            path_to_modify,
            ) 

        #ALTERAR CVU EM DECKS DC
        dadgers_to_modify = glob.glob(os.path.join(extracted_zip_estudo,"**",f"*dadger*"),recursive=True)
        dadger_updater.atualizar_cvu_DC(
            info_cvu,
            dadgers_to_modify
            )

        clast_to_modify = glob.glob(os.path.join(extracted_zip_estudo,"**",f"*clast*"),recursive=True)
        clast_updater.atualizar_cvu_NW(
            info_cvu,
            clast_to_modify
            )

        file = shutil.make_archive(
            extracted_zip_estudo,
            'zip',
            extracted_zip_estudo
            )

        send_files_to_api(id_estudo, file)
        logger.info(f"============================================")


def update_carga_estudo(ids_to_modify,path_carga_zip):

    # #ORGANIZA INFORMACOES DE CARGA

    for id_estudo in ids_to_modify:
        print("\n\n")
        logger.info(f"Modificando estudo {id_estudo}")

        path_to_modify = api.downloadEstudoPorId(id_estudo)

        extracted_zip_estudo = utils.extract_file_estudo(
            path_to_modify,
            ) 

        info_cargas = info_external_files.organizar_info_carga(
            path_carga_zip,
            extracted_zip_estudo,
            )

        # #ALTERAR CARGA EM DECKS DC
        dadgers_to_modify = glob.glob(os.path.join(extracted_zip_estudo,"**",f"*dadger*"),recursive=True)
        dadger_updater.atualizar_carga_DC(
            info_cargas,
            dadgers_to_modify
        )

        file = shutil.make_archive(
            extracted_zip_estudo,
            'zip',
            extracted_zip_estudo
            )

        send_files_to_api(id_estudo, file)
        logger.info(f"============================================")
        
def update_weol_estudo(data_produto:datetime.date, ids_to_modify:List[int] = None):
    tag = [f'WEOL {datetime.datetime.now().strftime("%d/%m %H:%M")}']
    if ids_to_modify == None:
        ids_to_modify = get_ids_to_modify()
    for id_estudo in ids_to_modify:
        info_estudo = api.getInfoRodadaPorId(id_estudo)
        df_estudo = pd.DataFrame(info_estudo['Decks'])
        print("\n\n")
        logger.info(f"Modificando estudo {id_estudo}")

        path_estudo = api.downloadEstudoPorId(id_estudo)

        extracted_zip_estudo = utils.extract_file_estudo(
            path_estudo,
            )
        dadgers_to_modify = glob.glob(os.path.join(extracted_zip_estudo,"**",f"*dadger*"),recursive=True)
        dadger_updater.update_eolica_DC(dadgers_to_modify, data_produto)
        api.update_tags(id_estudo, tag, "#FFF", "#44F")
        pattern = r'DC\d{6}-sem\d'
        
        for dadger in dadgers_to_modify:
            match = re.search(pattern, dadger)
            nome_estudo = match.group() + ".zip"
            id_deck = int(df_estudo['Id'][df_estudo['FileName'] == nome_estudo].values[0])
            send_files_to_api(id_estudo, dadger, id_deck)

        logger.info(f"============================================")
        
if __name__ == "__main__":

    # ids_to_modify = get_ids_to_modify()
    update_weol_estudo(datetime.date(2024, 12, 17))
    # ids_to_modify = [22152]
    # path_carga_zip=r"C:\Users\CS399274\Downloads\RV0_PMO_Dezembro_2024_carga_semanal.zip"

    # ano_referencia_cvu=2024
    # mes_referencia_cvu=11

    # update_cvu_estudo(ids_to_modify,ano_referencia_cvu,mes_referencia_cvu)
    # update_carga_estudo(ids_to_modify,path_carga_zip)