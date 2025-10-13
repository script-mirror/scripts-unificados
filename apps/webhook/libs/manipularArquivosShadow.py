import os
import sys
import pdb
import glob
import base64
import datetime
import io
import csv
import logging
import zipfile
import pandas as pd
import requests as req
import hashlib
import re
import shutil
import subprocess
from time import sleep
from dotenv import load_dotenv
from middle.utils import get_auth_header, Constants

logging.basicConfig(level=logging.INFO,
                    format='%(levelname)s:\t%(asctime)s\t %(name)s.py:%(lineno)d\t %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers=[
                        logging.StreamHandler()
                    ])

logger = logging.getLogger(__name__)

load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")),'.env'))
HOST_SERVIDOR = os.getenv('HOST_SERVIDOR')
URL_COGNITO = os.getenv('URL_COGNITO')
CONFIG_COGNITO = os.getenv('CONFIG_COGNITO')
PATH_PROJETO = os.getenv("PATH_PROJETO", "/WX2TB/Documentos/fontes/PMO")

path_libs = os.path.dirname(os.path.abspath(__file__))
path_webhook = os.path.dirname(path_libs)

path_fontes = "/WX2TB/Documentos/fontes/"
constants = Constants()

sys.path.insert(1,"/WX2TB/Documentos/fontes/")
sys.path.insert(1, f"{PATH_PROJETO}/scripts_unificados")
from apps.airflow import airflow_tools
from apps.webhook.libs import wx_libs_preco
from apps.smap.libs import SmapTools
from apps.dessem import dessem
from apps.gerarProdutos import gerarProdutos2
from bibliotecas import wx_opweek,rz_dir_tools
from apps.dbUpdater.libs import carga_ons,chuva,deck_ds,deck_dc,deck_nw,geracao,revisao,temperatura,vazao


#constantes path
PATH_CV = os.path.abspath("/WX2TB/Documentos/chuva-vazao")
PATH_PCONJUNTO = os.path.join(path_fontes, "PMO", "scripts_unificados","apps","pconjunto") 
PATH_ROTINA_CONJUNTO = os.path.join(PATH_PCONJUNTO,"pconjunto-ONS")
PATH_WEBHOOK_TMP = os.path.join(path_webhook, "arquivos","tmp")
PATH_PLAN_ACOMPH_RDH = os.path.join(path_fontes,"PMO","monitora_ONS","plan_acomph_rdh")

#constantes Classes
GERAR_PRODUTO = gerarProdutos2.GeradorProdutos()
DIR_TOOLS = rz_dir_tools.DirTools()


def hex_hash(s):
    h = hashlib.new('sha512')
    h.update(s.encode())
    hx = h.hexdigest()
    return hx


def get_auth():
    response = req.post(
        URL_COGNITO,
        data=CONFIG_COGNITO,
        headers={'Content-Type': 'application/x-www-form-urlencoded'}
    )
    headers = {
        'Authorization': f"Bearer {response.json()['access_token']}"
    }
    
    return headers

def get_access_token() -> str:
    response = req.post(
        URL_COGNITO,
        data=CONFIG_COGNITO,
        headers={'Content-Type': 'application/x-www-form-urlencoded'}
    )
    return response.json()['access_token']

def remover_acentos_e_caracteres_especiais(texto):
    import re
    import unicodedata
    texto_norm = unicodedata.normalize('NFD', texto)
    texto_limpo = re.sub(r'[^\w\s]', '', texto_norm)
    texto_limpo = re.sub(r'\W+', '_', texto_limpo)
    
    return texto_limpo
def get_filename(dadosProduto:dict):
    path_download = os.path.join(PATH_WEBHOOK_TMP, dadosProduto['nome'])
    
    os.makedirs(path_download, exist_ok=True)

    if dadosProduto.get('origem') == "botSintegre":
        filename = _handle_bot_sintegre_file(dadosProduto, path_download)
    elif dadosProduto.get('origem') == "sqsSintegre":
        filename = _handle_sqs_sintegre_file(dadosProduto, path_download)
    elif dadosProduto.get('webhookId'):
        filename = _handle_webhook_file(dadosProduto, path_download)
    else:
        filename = DIR_TOOLS.downloadFile(dadosProduto['url'], path_download)

    # if dadosProduto.get('origem') != "botSintegre":
    #     _verify_file_is_new(filename, dadosProduto['nome'])

    return filename

def _handle_bot_sintegre_file(dadosProduto: dict, path_download: str) -> str:
    source_file = dadosProduto['base64']
    dest_file = os.path.join(path_download, dadosProduto['filename'])
    os.system(f"cp -r {source_file} {path_download}")
    return dest_file

def _handle_sqs_sintegre_file(dadosProduto: dict, path_download: str) -> str:
    filename = os.path.join(path_download, dadosProduto['filename'])
    file_bytes = base64.b64decode(dadosProduto['base64'])
    with open(filename, 'wb') as file:
        file.write(file_bytes)
    return filename

def _handle_webhook_file(payload_webhook: dict, path_download: str) -> str:
    filename = os.path.join(path_download, payload_webhook['filename'])
    auth = get_auth()
    
    res = req.get(
        f"https://tradingenergiarz.com/webhook/api/webhooks/{payload_webhook['webhookId']}/download", 
        headers=auth
    )
    if res.status_code != 200:
        raise Exception(f"Erro ao baixar arquivo do S3: {res.text}")
        
    file_content = req.get(res.json()['url'])
    with open(filename, 'wb') as f:
        f.write(file_content.content)
    return filename

def _verify_file_is_new(filename: str, product_name: str) -> None:
    try:
        with open(filename, "rb") as f:
            data = f.read()
    except:
        print(f"erro ao escrever arquivo no manipularArquivosShadow ")
        raise

    file_hash = hex_hash(base64.b64encode(data).decode('utf-8'))
    is_new = req.post(
        "https://tradingenergiarz.com/api/v2/bot-sintegre/verify",
        json={"nome": product_name, "fileHash": file_hash},
        headers={
                'Authorization': f'Bearer {get_access_token()}'
            }
    ).json()

    if not is_new:
        raise Exception("Produto ja inserido")

def resultados_preliminares_consistidos(dadosProduto: dict):

    filename = get_filename(dadosProduto)
    logger.info(filename)

    dst = os.path.join(PATH_WEBHOOK_TMP, os.path.basename(filename)[:-4])
    DIR_TOOLS.extract(filename, dst, False)

    file = glob.glob(os.path.join(dst, 'Consistido', '*-preliminar.xls'))

    revisao.importar_rev_consistido(filename)
    
    params = dadosProduto.copy()
    
    airflow_tools.trigger_airflow_dag(
        dag_id="1.12-PROSPEC_CONSISTIDO",
        json_produtos=params,
        url_airflow="https://tradingenergiarz.com/airflow-middle/api/v2"
    )
    

def entrada_saida_previvaz(dadosProduto: dict):
    filename = get_filename(dadosProduto)
    logger.info(filename)

    dtInicial, dtFinal = dadosProduto["dataProduto"].split(' - ')



def arquivos_modelo_pdp(dadosProduto: dict):
    filename = get_filename(dadosProduto)
    logger.info(filename)

    dtRef = datetime.datetime.strptime(
          dadosProduto["dataProduto"], "%d/%m/%Y"
      )
    
    #rodar smap
    SmapTools.organizar_chuva_vazao_files(pdp_file_zip=filename,files_to_copy=['AJUSTE','PlanilhaUSB'], flag_db=True)
    # SmapTools.trigger_dag_SMAP(dtRef)
    # SmapTools.resultado_cv_obs(
    #     dtRef.date(),
    #     fonte_referencia='pdp',
    #     dst_path= os.path.join(PATH_CV,dtRef.strftime("%Y%m%d"),'fontes')
    #     )

    #gerar Produto
    
def arquivo_acomph(dadosProduto: dict):
    filename = get_filename(dadosProduto)
    logger.info(filename)

    path_copia_tmp = DIR_TOOLS.copy_src(filename, PATH_PLAN_ACOMPH_RDH)
    logger.info(path_copia_tmp)

    
    vazao.importAcomph(filename)

    dtRef = datetime.datetime.strptime(
            dadosProduto["dataProduto"], "%d/%m/%Y"
        )
    #gerar Produto

    if dadosProduto.get('enviar', True):
        GERAR_PRODUTO.enviar({
        "produto":"ACOMPH",
        "data" : dtRef,
        "path":filename,
        "destinatarioWhats": ["Condicao Hidrica"]

    })
    if dadosProduto.get('enviar', True):
        GERAR_PRODUTO.enviar({
        "produto":"ACOMPH_TABELA",
        "data" : dtRef,
        "path":filename,
        "destinatarioWhats": ["Condicao Hidrica"]

    })
    if dadosProduto.get('enviar', True):
        airflow_tools.trigger_airflow_dag(
            dag_id="1.08-PROSPEC_GRUPOS-ONS",
            json_produtos={},
            url_airflow="https://tradingenergiarz.com/airflow-middle/api/v2"
            )
        
        airflow_tools.trigger_airflow_dag(
            dag_id="1.01-PROSPEC_PCONJUNTO_DEFINITIVO",
            json_produtos={
                'dt_ref':dadosProduto['dataProduto']
                },
            url_airflow="https://tradingenergiarz.com/airflow-middle/api/v2"
            )

def arquivo_rdh(dadosProduto: dict):
    filename = get_filename(dadosProduto)
    logger.info(filename)

    path_copia_tmp = DIR_TOOLS.copy_src(filename, PATH_PLAN_ACOMPH_RDH)
    logger.info(path_copia_tmp)

    dtRef = datetime.datetime.strptime(
          dadosProduto["dataProduto"], "%d/%m/%Y"
      )
    vazao.importRdh(filename)

    #gerar Produto
    if dadosProduto.get('enviar', True):
        GERAR_PRODUTO.enviar({
        "produto":"RDH",
        "data":dtRef,
        "path":filename
    })

def historico_preciptacao(dadosProduto: dict):
    
    filename = get_filename(dadosProduto)
    logger.info(filename)

    dtRef = datetime.datetime.strptime(
        dadosProduto["dataProduto"], "%d/%m/%Y"
    )
    
    #temporario: salvando os arquivos psat para o pconjunto da data inicial para frente
    files_extracted = DIR_TOOLS.extract_specific_files_from_zip(
    path=filename,
    files_name_template= ["psat_{}.txt"],
    date_format='%d%m%Y',
    data_ini = datetime.datetime.strptime("01012024", "%d%m%Y"),
    dst=os.path.join(PATH_ROTINA_CONJUNTO, 'Arq_Entrada', 'Observado')
    )

    chuva.importar_chuva_psath(filename)

    #rodar smap
    # SmapTools.trigger_dag_SMAP(dtRef)

    # SmapTools.resultado_cv_obs(
    #     dtRef.date(),
    #     fonte_referencia='psat',
    #     dst_path= os.path.join(PATH_CV,dtRef.strftime("%Y%m%d"),'fontes')
    #     )

    #gerar Produto
    airflow_tools.trigger_airflow_dag(
            dag_id="Mapas_PSAT")

    
    #gerar Produto

        


def modelo_eta(dadosProduto: dict):
    filename = get_filename(dadosProduto)
    logger.info(filename)

    dst = os.path.join(PATH_ROTINA_CONJUNTO, 'Arq_Entrada', 'ETA40')
    os.makedirs(dst, exist_ok=True)
    extracted_files = DIR_TOOLS.extract_specific_files_from_zip(
        path=filename,
        files_name_template=['ETA40_m_*.dat'],
        date_format='%d%m%y',
        dst=dst,
        extracted_files=[])
    logger.info(extracted_files)

    dtRef = datetime.datetime.strptime(dadosProduto["dataProduto"],'%d/%m/%Y')
    dst = os.path.join(PATH_CV,dtRef.strftime('%Y%m%d'),'ETA40','ONS')
    os.makedirs(dst, exist_ok=True)
    extracted_files = DIR_TOOLS.extract_specific_files_from_zip(
        path=filename,
        files_name_template= ["ETA40_p*a*.dat"],
        date_format='%d%m%y',
        dst=dst)
    logger.info(extracted_files)
    if dadosProduto.get('enviar', True):
        airflow_tools.trigger_airflow_dag(
            dag_id="PCONJUNTO",
            json_produtos={
                'dt_ref':dadosProduto['dataProduto']
                })


def deck_preliminar_decomp(dadosProduto: dict):

    path_download = os.path.join(PATH_WEBHOOK_TMP,dadosProduto['nome'])

    filename = get_filename(dadosProduto)
    logger.info(filename)

    # dst_copia = os.path.join('/WX2TB/Documentos/fontes/PMO/converte_dc/input', os.path.basename(filename))
    DIR_TOOLS.extract(filename, path_download)

    dst_copia = "/WX2TB/Documentos/fontes/PMO/converte_dc/input/"
    path_copia_tmp = DIR_TOOLS.copy_src(filename, dst_copia)[0]
    logger.info(path_copia_tmp)

    #deck values
    deck_dc.importar_dc_bloco_pq(filename)
    deck_dc.importar_dc_bloco_dp(filename)

    # decomp.importar_deck_entrada(path_copia_tmp, True)

    #gerar Produto
    if dadosProduto.get('enviar', True):
        GERAR_PRODUTO.enviar({
        "produto":"CMO_DC_PRELIMINAR",
        "path":filename,
    })
    if dadosProduto.get('enviar', True): 
        dadosProduto['dt_ref'] = dadosProduto['dataProduto']
        airflow_tools.trigger_airflow_dag(
            dag_id="1.16-DECOMP_ONS-TO-CCEE",
            json_produtos=dadosProduto,
            url_airflow="https://tradingenergiarz.com/airflow-middle/api/v2"
            )

    
def deck_entrada_saida_dessem(dadosProduto: dict):

    filename = get_filename(dadosProduto)
    rename_filename = filename.replace("_2° nível de contingência", "")
    print(f'filename: {filename}')
    print(f'rename_filename: {rename_filename}')
    os.popen(f'mv {filename} {rename_filename}')
    filename = rename_filename
    logger.info(filename)

    dtRef = datetime.datetime.strptime(dadosProduto["dataProduto"], '%d/%m/%Y')
    path_dessem_diario = '/WX2TB/Documentos/fontes/PMO/arquivos/diario/{}/dessem'.format(dtRef.strftime('%Y%m%d'))
    path_copia_tmp = DIR_TOOLS.copy_src(filename, path_dessem_diario)
    logger.info(path_copia_tmp)
    
    path_arquivos_ds ="/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/dessem/arquivos".format(
            dtRef.strftime("%Y%m%d")
        )

    deck_ds.importar_ds_bloco_dp(filename,dtRef,str_fonte='ons')
    deck_ds.importar_pdo_cmosist_ds(path_file=filename, dt_ref=dtRef, str_fonte='ons')

    if dadosProduto.get('enviar', True):
        GERAR_PRODUTO.enviar({
        "produto":"RESULTADO_DESSEM",
        "data":dtRef,
    })
    
    dessem.organizarArquivosOns(dtRef, path_arquivos_ds, importarDb=False, enviarEmail=True)
    logger.info(f"Triggering DESSEM_convertido, dt_ref: {dadosProduto['dataProduto']}")
    if dadosProduto.get('enviar', True):
        airflow_tools.trigger_airflow_dag(
            dag_id="DESSEM_convertido",
            json_produtos={
                'dt_ref':dadosProduto['dataProduto']
                })
    
    deck_ds.importar_renovaveis_ds(path_file=filename, dt_ref=dtRef, str_fonte='ons')
    
    if dadosProduto.get('enviar', True):
        GERAR_PRODUTO.enviar({
        "produto":"PREVISAO_CARGA_DESSEM",
        "data":dtRef,
    })
    

    
def previsao_carga_dessem(dadosProduto: dict):
    
    filename = get_filename(dadosProduto)
    logger.info(filename)

    nome_arquivo = os.path.basename(filename)
    dtRef = datetime.datetime.strptime(nome_arquivo.lower(), "blocos_%Y-%m-%d.zip")

    extract_dst = "/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/dessem/arquivos/{}/entrada/blocos".format(
        dtRef.strftime("%Y%m%d")
    )
    
    DIR_TOOLS.extract(filename,extract_dst)

    blocoDp = os.path.join(extract_dst, "DP.txt")
    blocoTm = os.path.join(extract_dst, "Bloco_TM.txt")

    #tb_carga_ds
    deck_ds.importar_ds_bloco_dp(filename,dtRef)

    if dtRef.weekday() == 5:
        deck_ds.importar_ds_bloco_tm(filename,dtRef)

    #gerar Produto
    if dadosProduto.get('enviar', True):
        GERAR_PRODUTO.enviar({
        "produto":"PREVISAO_CARGA_DESSEM",
        "data":dtRef,
    })

 
def prevCarga_dessem(dadosProduto: dict):

    filename = get_filename(dadosProduto)

    logger.info(filename)

    dtRef = datetime.datetime.strptime(dadosProduto["dataProduto"], "%d/%m/%Y")
    path_deck_ds = "/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/dessem/arquivos/{}/entrada/decks".format(
            dtRef.strftime("%Y%m%d")
        )
    path_copia_tmp = DIR_TOOLS.copy_src(filename, path_deck_ds)
    logger.info(path_copia_tmp)

    temperatura.importar_temperatura_obs(filename)
    temperatura.importar_temperatura_prev_dessem(filename)

    data_atual = datetime.date.today()
    anoAtual = data_atual.year
    data_string = data_atual.strftime("%Y-%m-%d")

    if data_string == f"{anoAtual}-01-01":
        revisao.atualizar_patamar_ano_atual(filename,dtRef )


def carga_patamar_nw(dadosProduto: dict):
    

    filename = get_filename(dadosProduto)
    
    airflow_tools.trigger_airflow_dag(
        dag_id="webhook-sintegre",
        json_produtos=dadosProduto,
        url_airflow="https://tradingenergiarz.com/airflow-middle/api/v2"
    )
    
    logger.info(filename)

    path_copia_tmp = DIR_TOOLS.copy_src(filename, PATH_PLAN_ACOMPH_RDH)
    logger.info(path_copia_tmp)

    if "ons" in dadosProduto["url"]:
        id_fonte = "ons"
    else:
        id_fonte = "ccee" 
        
    dtRef = datetime.datetime.strptime(dadosProduto["dataProduto"], "%m/%Y")
    dtRef_last_saturday = wx_opweek.getLastSaturday(dtRef)

    deck_nw.importar_carga_nw(
        pathZip = filename, 
        dataProduto = dtRef, 
        dataReferente = dtRef_last_saturday, 
        str_fonte = id_fonte)

    #gerar Produto
    # if dadosProduto.get('enviar', True):
    #     GERAR_PRODUTO.enviar({
    #     "produto":"REVISAO_CARGA_NW",
    #     "path":filename,
    #     "data_ref": dtRef
    # })
        
    if dadosProduto.get('enviar', True): 
        dadosProduto['dt_ref'] = dadosProduto['dataProduto']
        airflow_tools.trigger_airflow_dag(
            dag_id="1.17-NEWAVE_ONS-TO-CCEE",
            json_produtos=dadosProduto,
            url_airflow="https://tradingenergiarz.com/airflow-middle/api/v2"
        )

    return {
        "file_path": filename,
        "trigger_dag_id":"PROSPEC_UPDATER",
        "task_to_execute": "revisao_carga_nw"
    }


def carga_IPDO(dadosProduto: dict):
    

    filename = get_filename(dadosProduto)
    logger.info(filename)

    path_copia_tmp = DIR_TOOLS.copy_src(filename, PATH_PLAN_ACOMPH_RDH)
    logger.info(path_copia_tmp)

    dtRef = datetime.datetime.strptime(
          dadosProduto["dataProduto"], "%d/%m/%Y"
      )

    carga_ons.importar_carga_ipdo(filename,dtRef)

    #gerar Produto
    if dadosProduto.get('enviar', True):
        GERAR_PRODUTO.enviar({
        "produto":"IPDO",
        "data":dtRef,
        "path": filename,
        "destinatarioWhats": ["Condicao Hidrica"]
    })
    
    
def modelo_ECMWF(dadosProduto: dict):
    filename = get_filename(dadosProduto)
    logger.info(filename)
    
    dst = os.path.join(PATH_ROTINA_CONJUNTO, 'Arq_Entrada', 'ECMWF')
    os.makedirs(dst, exist_ok=True)
    extracted_files = DIR_TOOLS.extract_specific_files_from_zip(
        path=filename,
        files_name_template=['ECMWF_m_*.dat'],
        date_format='%d%m%y',
        dst=dst,
        extracted_files=[])
    logger.info(extracted_files)
    if dadosProduto.get('enviar', True):
        airflow_tools.trigger_airflow_dag(
            dag_id="PCONJUNTO",
            json_produtos={
                'dt_ref':dadosProduto['dataProduto']
                })

            
def dados_geracaoEolica(dadosProduto: dict):

    filename = get_filename(dadosProduto)
    logger.info(filename)
    
    #arquivo copia
    dtReferente = datetime.datetime.strptime(dadosProduto["dataProduto"], '%d/%m/%Y')
    path_dessem_diario = '/WX2TB/Documentos/fontes/PMO/arquivos/diario/{}/dessem'.format(dtReferente.strftime('%Y%m%d'))
    path_copia_tmp = DIR_TOOLS.copy_src(filename, path_dessem_diario)
    logger.info(path_copia_tmp)

    geracao.importar_eolica_files(path_zip = filename)
    
    
def prevCarga_dessem_saida(dadosProduto: dict):

    filename = get_filename(dadosProduto)
    logger.info(filename)

    dtReferente = datetime.datetime.strptime(dadosProduto["dataProduto"], '%d/%m/%Y')

    path_prev_carga_dessem = '/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/dessem/arquivos/{}/entrada/PrevCargaDessem'.format(dtReferente.strftime('%Y%m%d'))
    path_copia_tmp = DIR_TOOLS.copy_src(filename, path_prev_carga_dessem)
    logger.info(path_copia_tmp)

    carga_ons.importar_prev_carga_dessem_saida(path_zip = filename)
    

def modelo_gefs(dadosProduto: dict):

    filename = get_filename(dadosProduto)
    logger.info(filename)
    
    dst = os.path.join(PATH_ROTINA_CONJUNTO, 'Arq_Entrada', 'GEFS')
    os.makedirs(dst, exist_ok=True)
    extracted_files = DIR_TOOLS.extract_specific_files_from_zip(
        path=filename,
        files_name_template=['GEFS_m_*.dat'],
        date_format='%d%m%y',
        dst=dst,
        extracted_files=[])
    logger.info(extracted_files)
    if dadosProduto.get('enviar', True):
        airflow_tools.trigger_airflow_dag(
            dag_id="PCONJUNTO",
            json_produtos={
                'dt_ref':dadosProduto['dataProduto']
                })


# def resultados_nao_consistidos_semanal(dadosProduto: dict):

#     filename = get_filename(dadosProduto)
#     logger.info(filename)
    
#     # titulo, html = wx_libs_preco.nao_consistido_rv(filename)
    
#     # data_produto = datetime.datetime.strptime(dadosProduto.get('dataProduto')[:10], "%d/%m/%Y")
    
#     # if dadosProduto.get('enviar', True):
#     #     GERAR_PRODUTO.enviar({
#     #         "produto":"PREV_ENA_CONSISTIDO",
#     #         "data":data_produto,
#     #         "titulo":titulo,
#     #         "html":html
#     #     })
    
#     temp_dir = os.path.join(PATH_WEBHOOK_TMP, 'temp_extract_' + datetime.datetime.now().strftime('%Y%m%d_%H%M%S'))
#     os.makedirs(temp_dir, exist_ok=True)
    
#     DIR_TOOLS.extract(filename, temp_dir)
    
#     zip_filename = os.path.basename(filename)
#     pdb.set_trace()
#     rev_match = re.search(r'REV(\d+)', zip_filename, re.IGNORECASE)
#     month_match = re.search(r'_(\d{6})_', zip_filename)
    
#     if not rev_match:
#         rev_match = re.search(r'REV(\d+)', zip_filename, re.IGNORECASE)
    
#     if not month_match:
#         month_match = re.search(r'(\d{6})', zip_filename)
        
#     prevs_files = glob.glob(os.path.join(temp_dir, '**/Prevs_VE.prv'), recursive=True)
    
#     prevs_file = prevs_files[0]
#     logger.info(f"Arquivo de previsão encontrado em: {prevs_file}")
    
#     if month_match:
        
#         params = dadosProduto.copy()
#         params["sensibilidade"] = "NÃO CONSISTIDO"
        
#         airflow_tools.trigger_airflow_dag(
#             dag_id="1.12-PROSPEC_CONSISTIDO",
#             json_produtos=params,
#             url_airflow="https://tradingenergiarz.com/airflow-middle/api/v2"
#         )
#     else:
#         raise Exception(f"Não foi possível extrair mês do nome do arquivo: {zip_filename}")
    
    

# def relatorio_resutados_finais_consistidos(dadosProduto: dict):

#     filename = get_filename(dadosProduto)
#     logger.info(filename)

#     # path_copia_tmp = DIR_TOOLS.copy_src(filename, PATH_PLAN_ACOMPH_RDH)
#     # logger.info(path_copia_tmp)

#     wx_libs_preco.dadvaz_pdp(filename,data["dataProduto"])
#     filename_splited = os.path.basename(filename).split('_')
#     diaPrevisao = int(filename_splited[3])
#     mesPrevisao = int(filename_splited[4])
#     anoPrevisao = int(filename_splited[5])
#     dtPrevisao = datetime.datetime(anoPrevisao, mesPrevisao, diaPrevisao)

#     # revisao.importar_prev_ena_consistido(filename,dtPrevisao)
#     if dadosProduto.get('enviar', True):
#         GERAR_PRODUTO.enviar({
#     "produto":"PREVISAO_ENA_SUBMERCADO",
#     "data":dtPrevisao,
    
#     })

# def niveis_partida_dessem(dadosProduto: dict):
#     filename = get_filename(dadosProduto)
#     logger.info(filename)

#     path_copia_tmp = DIR_TOOLS.copy_src(filename, PATH_PLAN_ACOMPH_RDH)
#     logger.info(path_copia_tmp)

#     vazao.importNiveisDessem(filename)


# def dadvaz_vaz_prev(dadosProduto: dict):

#     filename = get_filename(dadosProduto)
#     logger.info(filename)

#     dtReferente = datetime.datetime.strptime(dadosProduto["dataProduto"], '%d/%m/%Y')
#     path_dessem_diario = '/WX2TB/Documentos/fontes/PMO/arquivos/diario/{}/dessem'.format(dtReferente.strftime('%Y%m%d'))
#     path_copia_tmp = DIR_TOOLS.copy_src(filename, path_dessem_diario)
#     logger.info(path_copia_tmp)
    
def deck_resultados_decomp(dadosProduto: dict):

    filename = get_filename(dadosProduto)
    logger.info(filename)

    dtInicial, dtFinal = dadosProduto["dataProduto"].split(' - ')
    dtInicial = datetime.datetime.strptime(dtInicial, '%d/%m/%Y')
    dataEletrica =  wx_opweek.ElecData(dtInicial.date())
    # /WX2TB/Documentos/fontes/PMO/arquivos/decks/202101_RV1
    path_decks = '/WX2TB/Documentos/fontes/PMO/arquivos/decks/{}{:0>2}_RV{}'.format(dataEletrica.anoReferente, dataEletrica.mesReferente, dataEletrica.atualRevisao)
    if dadosProduto.get('enviar', True): 
        dadosProduto['dt_ref'] = dadosProduto['dataProduto']
        airflow_tools.trigger_airflow_dag(
            dag_id="1.16-DECOMP_ONS-TO-CCEE",
            json_produtos=dadosProduto,
            url_airflow="https://tradingenergiarz.com/airflow-middle/api/v2"
            )
    path_copia_tmp = DIR_TOOLS.copy_src(filename, path_decks)
    logger.info(path_copia_tmp)
    

def resultados_finais_consistidos(dadosProduto: dict):

    filename = get_filename(dadosProduto)
    logger.info(filename)

    dtInicial = dadosProduto["dataProduto"]
    dtInicial = datetime.datetime.strptime(dtInicial, '%d/%m/%Y')
    dataEletrica =  wx_opweek.ElecData(dtInicial.date())
    path_decks = os.path.abspath('/WX2TB/Documentos/fontes/PMO/arquivos/decks/{}{:0>2}_RV{}'.format(dataEletrica.anoReferente, dataEletrica.mesReferente, dataEletrica.atualRevisao))
    path_copia_tmp = DIR_TOOLS.copy_src(filename, path_decks)
    logger.info(path_copia_tmp)


def ler_csv_prev_weol_para_dicionario(file):
    reader = csv.reader(file, delimiter=';')
    headers = next(reader)[1:]
    data_dict = {}
    for row in reader:
        regiao = row[0]
        valores = row[1:]
        for i in range(0, len(headers), 3):
            data_key = f"{headers[i]} {headers[i+2]}"
            if data_key not in data_dict:
                data_dict[data_key] = {}
            if regiao != "Patamares":
                data_dict[data_key][regiao] = {
                    "pesado": valores[i],
                    "medio": valores[i+1],
                    "leve": valores[i+2]
                }
    return data_dict
    
    
    
def deck_prev_eolica_semanal_patamares(dadosProduto: dict):
    try:
        res  = req.get(dadosProduto["url"])
        zip_file = zipfile.ZipFile(io.BytesIO(res.content))
    except:
        zip_file = zipfile.ZipFile(dadosProduto["base64"])
        
    
    patamates_csv_path = [x for x in zip_file.namelist() if "Arquivos Entrada/Dados Cadastrais/Patamares_" in x and ".csv" in x]
    patamates_csv_path = patamates_csv_path[0]
    
    
    with zip_file.open(patamates_csv_path) as file:
        content = file.read()
        df_patamares = pd.read_csv(io.StringIO(content.decode("latin-1")), sep=";")
        df_patamares.columns = [x[0].lower() + x[1:] for x in df_patamares.columns]
    df_patamares.columns = ['inicio','patamar','cod_patamar','dia_semana','dia_tipico','tipo_dia','intervalo','dia','semana','mes']
    post_patamates = req.post(f"https://tradingenergiarz.com/api/v2/decks/patamares", json=df_patamares.to_dict("records"),
        headers={
        'Authorization': f'Bearer {get_access_token()}'
    })
    if post_patamates.status_code == 200:
        logger.info("Patamares inseridos com sucesso")
    else:
        logger.error(f"Erro ao inserir patamares. status code: {post_patamates.status_code}")

def deck_prev_eolica_semanal_previsao_final(dadosProduto: dict):
    res  = req.get(dadosProduto["url"])
    try:
        res  = req.get(dadosProduto["url"])
        zip_file = zipfile.ZipFile(io.BytesIO(res.content))
    except:
        zip_file = zipfile.ZipFile(dadosProduto["base64"])
    
    ## puxando arquivo baixado locakmente ##
    # zip_file = zipfile.ZipFile("/home/arthur-moraes/Downloads/Deck_PrevMes_20241116.zip")
    
    prev_eol_csv_path = [x for x in zip_file.namelist() if "Arquivos Saida/Previsoes Subsistemas Finais/Total/Prev_" in x and ".csv" in x]
    prev_eol_csv_path = prev_eol_csv_path[0]
    
    body_weol = []
    with zip_file.open(prev_eol_csv_path) as file:
        content = file.read()
        info_weol = ler_csv_prev_weol_para_dicionario(io.StringIO(content.decode("latin-1")))
        for data in info_weol.keys():
            data_inicio:datetime.datetime = datetime.datetime.strptime(data.split(" ")[0], "%d/%m/%Y")
            data_fim:datetime.datetime = datetime.datetime.strptime(data.split(" ")[1], "%d/%m/%Y")
            for submercado in info_weol[data].keys():                
                for patamar in info_weol[data][submercado].keys():
                    body_weol.append({
                        "inicio_semana":str(data_inicio.date()),
                        "final_semana":str(data_fim.date()),
                        "rv_atual": wx_opweek.ElecData(data_inicio.date()).atualRevisao,
                        "mes_eletrico": wx_opweek.ElecData(data_inicio.date()).mesReferente,
                        "submercado":submercado,
                        "patamar":patamar,
                        "valor":info_weol[data][submercado][patamar],
                        "data_produto":str(datetime.datetime.strptime(dadosProduto['dataProduto'], "%d/%m/%Y").date())})
        
    post_decks_weol = req.post(f"https://tradingenergiarz.com/api/v2/decks/weol",
                               json=body_weol,
                               headers={
                'Authorization': f'Bearer {get_access_token()}'
            })
    if post_decks_weol.status_code == 200:
        logger.info("WEOL inserido com sucesso")
    else:
        logger.error(f"Erro ao inserir WEOL. status code: {post_decks_weol.status_code}")
        
def deck_prev_eolica_semanal_weol(dadosProduto:dict):
    filename = get_filename(dadosProduto)
    airflow_tools.trigger_airflow_dag(
        dag_id="webhook_deck_prev_eolica_semanal_weol",
        json_produtos=dadosProduto
        )
    
def enviar_tabela_comparacao_weol_whatsapp_email(dadosProduto:dict):
    data_produto = datetime.datetime.strptime(dadosProduto.get('dataProduto'), "%d/%m/%Y")
    if dadosProduto.get('enviar', True):
        GERAR_PRODUTO.enviar({
        "produto":"TABELA_WEOL_MENSAL",
        "data":data_produto.date(),
    })
    if dadosProduto.get('enviar', True):
        GERAR_PRODUTO.enviar({
        "produto":"TABELA_WEOL_SEMANAL",
        "data":data_produto.date(),
    })
    if dadosProduto.get('enviar', True):
        GERAR_PRODUTO.enviar({
        "produto":"TABELA_WEOL_DIFF",
        "data":data_produto.date(),
    })





# def notas_tecnicas_medio_prazo(dadosProduto: dict):
    
#     arquivo_zip = get_filename(dadosProduto)
#     logger.info(arquivo_zip)
    
#     path_arquivo = os.path.join(PATH_WEBHOOK_TMP, os.path.basename(arquivo_zip)[:-4])
#     os.makedirs(path_arquivo, exist_ok=True)
    
#     with zipfile.ZipFile(arquivo_zip, 'r') as zip_ref:
#         zip_contents = zip_ref.namelist()
#         logger.info(f"Conteúdo do zip: {zip_contents}")
        
#         excel_files = [f for f in zip_contents if f.endswith('.xlsx') or f.endswith('.xls')]
#         for excel_file in excel_files:
#             zip_ref.extract(excel_file, path_arquivo)
#             logger.info(f"Arquivo extraído: {excel_file}")
    
#     arquivo_xls = os.path.join(path_arquivo, os.path.basename(excel_file))
    
#     logger.info(f"Arquivos Excel encontrados: {arquivo_xls}")
    
#     if not arquivo_xls:
#         logger.error(f"Nenhum arquivo Excel encontrado em {path_arquivo}")
#         return
#     GERAR_PRODUTO.enviar({
#         "produto":"NOTAS_TECNICAS",
#         "data": datetime.datetime.strptime(dadosProduto.get('dataProduto'), "%m/%Y"),
#         "arquivo": arquivo_xls
#     })
#     if f"GTMIN_CCEE_{(datetime.date.today().replace(day=1, month=datetime.date.today().month+1)).strftime('%m%Y')}" in arquivo_xls.upper():
#         airflow_tools.trigger_airflow_dag(
#             dag_id="1.17-NEWAVE_ONS-TO-CCEE",
#             json_produtos=dadosProduto,
#         )
    
    
    
    

if __name__ == '__main__':
    
    dadosProduto = {
  "dataProduto": "30/09/2025",
  "filename": "Relatorio_previsao_diaria_28_09_2025_para_30_09_2025.xls",
  "macroProcesso": "Programação da Operação",
  "nome": "Relatório dos resultados finais consistidos da previsão diária (PDP)",
  "periodicidade": "2025-09-30T00:00:00",
  "periodicidadeFinal": "2025-09-30T23:59:59",
  "processo": "Previsão de Vazões Diárias - PDP",
  "s3Key": "webhooks/Relatório dos resultados finais consistidos da previsão diária (PDP)/68d98616995e02e90c3fc514_Relatorio_previsao_diaria_28_09_2025_para_30_09_2025.xls",
  "url": "https://apps08.ons.org.br/ONS.Sintegre.Proxy/webhook?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJVUkwiOiJodHRwczovL3NpbnRlZ3JlLm9ucy5vcmcuYnIvc2l0ZXMvOS8xMy84Mi9Qcm9kdXRvcy81NDgvUmVsYXRvcmlvX3ByZXZpc2FvX2RpYXJpYV8yOF8wOV8yMDI1X3BhcmFfMzBfMDlfMjAyNS54bHMiLCJ1c2VybmFtZSI6ImdpbHNldS5tdWhsZW5AcmFpemVuLmNvbSIsIm5vbWVQcm9kdXRvIjoiUmVsYXTDs3JpbyBkb3MgcmVzdWx0YWRvcyBmaW5haXMgY29uc2lzdGlkb3MgZGEgcHJldmlzw6NvIGRpw6FyaWEgKFBEUCkiLCJJc0ZpbGUiOiJUcnVlIiwiaXNzIjoiaHR0cDovL2xvY2FsLm9ucy5vcmcuYnIiLCJhdWQiOiJodHRwOi8vbG9jYWwub25zLm9yZy5iciIsImV4cCI6MTc1OTE3Mjc0MiwibmJmIjoxNzU5MDg2MTAyfQ.sQMIZ57iv4NHLxfcWGTMA9Mnh_8yiiRxtbXF64qIc6Q",
  "webhookId": "68d98616995e02e90c3fc514"
}
        
    

    relatorio_resutados_finais_consistidos(dadosProduto)
    

