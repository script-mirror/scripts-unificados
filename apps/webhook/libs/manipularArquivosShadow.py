import os
import sys
import pdb
import base64
import datetime
import logging
import requests as req
import hashlib
from dotenv import load_dotenv
from middle.utils import  Constants

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

path_libs = os.path.dirname(os.path.abspath(__file__))
path_webhook = os.path.dirname(path_libs)

path_fontes = "/WX2TB/Documentos/fontes/"
constants = Constants()

sys.path.insert(1,"/WX2TB/Documentos/fontes/PMO/scripts_unificados/")
from server_configuration.containers.airflow_mysql import airflow_tools
from apps.smap.libs import SmapTools
from apps.dessem import dessem
from apps.gerarProdutos import gerarProdutos2
from bibliotecas import rz_dir_tools
from apps.dbUpdater.libs import carga_ons,chuva,deck_ds,geracao,revisao,temperatura,vazao


#constantes path
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


def arquivos_modelo_pdp(dadosProduto: dict):
    filename = get_filename(dadosProduto)
    logger.info(filename)

    dtRef = datetime.datetime.strptime(
          dadosProduto["dataProduto"], "%d/%m/%Y"
      )
    SmapTools.organizar_chuva_vazao_files(pdp_file_zip=filename,files_to_copy=['AJUSTE','PlanilhaUSB'], flag_db=True)

    
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

    airflow_tools.trigger_airflow_dag(
            dag_id="Mapas_PSAT")

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
    




if __name__ == '__main__':
    
    dadosProduto = {
  "dataProduto": "03/11/2025",
  "filename": "ACOMPH_03.11.2025.xls",
  "macroProcesso": "Programação da Operação",
  "nome": "Acomph",
  "periodicidade": "2025-11-03T00:00:00",
  "periodicidadeFinal": "2025-11-03T23:59:59",
  "processo": "Acompanhamento das Condições Hidroenergéticas",
  "s3Key": "webhooks/Acomph/228275a3-7443-4a24-8c03-d5d0edabf9ee_ACOMPH_03.11.2025.xls",
  "url": "https://apps08.ons.org.br/ONS.Sintegre.Proxy/webhook?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJVUkwiOiJodHRwczovL3NpbnRlZ3JlLm9ucy5vcmcuYnIvc2l0ZXMvOS8xMy81Ni9Qcm9kdXRvcy8yMzAvQUNPTVBIXzAzLjExLjIwMjUueGxzIiwidXNlcm5hbWUiOiJnaWxzZXUubXVobGVuQHJhaXplbi5jb20iLCJub21lUHJvZHV0byI6IkFjb21waCIsIklzRmlsZSI6IlRydWUiLCJpc3MiOiJodHRwOi8vbG9jYWwub25zLm9yZy5iciIsImF1ZCI6Imh0dHA6Ly9sb2NhbC5vbnMub3JnLmJyIiwiZXhwIjoxNzYyMjY4MTQzLCJuYmYiOjE3NjIxODE1MDN9.5AvwJxcvQ1KAJn2R-0DAApNrawWrM9iPqwr7vFt_HIo",
  "webhookId": "228275a3-7443-4a24-8c03-d5d0edabf9ee"
}
        
    

    arquivo_acomph(dadosProduto)
    

