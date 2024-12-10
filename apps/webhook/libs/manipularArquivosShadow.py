import os
import sys
import pdb
import glob
import datetime
import io
import csv
import logging
import zipfile
import pandas as pd
import requests as r
from dotenv import load_dotenv

logging.basicConfig(level=logging.DEBUG,
                    format='%(levelname)s:\t%(asctime)s\t %(name)s.py:%(lineno)d\t %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers=[
                        logging.StreamHandler()
                    ])

logger = logging.getLogger(__name__)

load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")),'.env'))
__HOST_SERVIDOR = os.getenv('HOST_SERVIDOR')

path_libs = os.path.dirname(os.path.abspath(__file__))
path_webhook = os.path.dirname(path_libs)

path_fontes = "/WX2TB/Documentos/fontes/"


sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.apps.airflow import airflow_tools
from PMO.scripts_unificados.apps.webhook.libs import wx_libs_preco
from PMO.scripts_unificados.apps.tempo.libs import plot
from PMO.scripts_unificados.apps.smap.libs import SmapTools
from PMO.scripts_unificados.apps.dessem import dessem
from PMO.scripts_unificados.apps.pconjunto import wx_plota_pconjunto
from PMO.scripts_unificados.apps.gerarProdutos import gerarProdutos2 
from PMO.scripts_unificados.bibliotecas import wx_opweek,rz_dir_tools
from PMO.scripts_unificados.apps.dbUpdater.libs import carga_ons,chuva,deck_ds,deck_dc,deck_nw,geracao,revisao,temperatura,vazao
from PMO.scripts_unificados.apps.prospec.libs import update_estudo

#constantes path
PATH_CV = os.path.abspath("/WX2TB/Documentos/chuva-vazao")
PATH_PCONJUNTO = os.path.join(path_fontes, "PMO", "scripts_unificados","apps","pconjunto") 
PATH_ROTINA_CONJUNTO = os.path.join(PATH_PCONJUNTO,"pconjunto-ONS")
PATH_WEBHOOK_TMP = os.path.join(path_webhook, "arquivos","tmp")
PATH_PLAN_ACOMPH_RDH = os.path.join(path_fontes,"PMO","monitora_ONS","plan_acomph_rdh")

#constantes Classes
GERAR_PRODUTO = gerarProdutos2.GerardorProdutos()
DIR_TOOLS = rz_dir_tools.DirTools()


def resultados_preliminares_consistidos(dadosProduto):

    path_download = os.path.join(PATH_WEBHOOK_TMP,dadosProduto['nome'])

    filename = DIR_TOOLS.downloadFile(dadosProduto['url'], path_download)
    logger.info(filename)

    dst = os.path.join(PATH_WEBHOOK_TMP, os.path.basename(filename)[:-4])
    DIR_TOOLS.extract(filename, dst, False)

    file = glob.glob(os.path.join(dst, 'Consistido', '*-preliminar.xls'))

    revisao.importar_rev_consistido(filename)
    

def entrada_saida_previvaz(dadosProduto):
    path_download = os.path.join(PATH_WEBHOOK_TMP,dadosProduto['nome'])
    filename = DIR_TOOLS.downloadFile(dadosProduto['url'], path_download)
    logger.info(filename)

    dtInicial, dtFinal = dadosProduto["dataProduto"].split(' - ')



def arquivos_modelo_pdp(dadosProduto):
    path_download = os.path.join(PATH_WEBHOOK_TMP,dadosProduto['nome'])
    filename = DIR_TOOLS.downloadFile(dadosProduto['url'], path_download)
    logger.info(filename)

    dtRef = datetime.datetime.strptime(
          dadosProduto["dataProduto"], "%d/%m/%Y"
      )
    
    #rodar smap
    SmapTools.organizar_chuva_vazao_files(pdp_file_zip=filename,files_to_copy=['AJUSTE','PlanilhaUSB'], flag_db=True)
    SmapTools.trigger_dag_SMAP(dtRef)
    SmapTools.resultado_cv_obs(
        dtRef.date(),
        fonte_referencia='pdp',
        dst_path= os.path.join(PATH_CV,dtRef.strftime("%Y%m%d"),'fontes')
        )

    #gerar Produto
    GERAR_PRODUTO.enviar({
        "produto":"DIFERENCA_CV",
        "data":dtRef,
    })


def arquivo_acomph(dadosProduto):
    path_download = os.path.join(PATH_WEBHOOK_TMP,dadosProduto['nome'])
    filename = DIR_TOOLS.downloadFile(dadosProduto['url'], path_download)
    logger.info(filename)

    path_copia_tmp = DIR_TOOLS.copy_src(filename, PATH_PLAN_ACOMPH_RDH)
    logger.info(path_copia_tmp)

    airflow_tools.trigger_airflow_dag(
        dag_id="1.8-PROSPEC_GRUPOS-ONS",
        json_produtos={}
        )
    
    airflow_tools.trigger_airflow_dag(
        dag_id="1.1-PROSPEC_PCONJUNTO_DEFINITIVO",
        json_produtos={
            'dt_ref':dadosProduto['dataProduto']
            })
    
    vazao.importAcomph(filename)

    dtRef = datetime.datetime.strptime(
            dadosProduto["dataProduto"], "%d/%m/%Y"
        )
    #gerar Produto
    GERAR_PRODUTO.enviar({
        "produto":"ACOMPH",
        "data" : dtRef,
        "path":filename,
    })


def arquivo_rdh(dadosProduto):
    path_download = os.path.join(PATH_WEBHOOK_TMP,dadosProduto['nome'])
    filename = DIR_TOOLS.downloadFile(dadosProduto['url'], path_download)
    logger.info(filename)

    path_copia_tmp = DIR_TOOLS.copy_src(filename, PATH_PLAN_ACOMPH_RDH)
    logger.info(path_copia_tmp)

    dtRef = datetime.datetime.strptime(
          dadosProduto["dataProduto"], "%d/%m/%Y"
      )
    vazao.importRdh(filename)

    #gerar Produto
    GERAR_PRODUTO.enviar({
        "produto":"RDH",
        "data":dtRef,
        "path":filename
    })

def historico_preciptacao(dadosProduto):
    
    path_download = os.path.join(PATH_WEBHOOK_TMP,dadosProduto['nome'])
    filename = DIR_TOOLS.downloadFile(dadosProduto['url'], path_download)
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
    SmapTools.trigger_dag_SMAP(dtRef)
    SmapTools.resultado_cv_obs(
        dtRef.date(),
        fonte_referencia='psat',
        dst_path= os.path.join(PATH_CV,dtRef.strftime("%Y%m%d"),'fontes')
        )

    #gerar Produto
    GERAR_PRODUTO.enviar({
        "produto":"PSATH_DIFF",
        "path":filename,
    })
    #gerar Produto
    GERAR_PRODUTO.enviar({
        "produto":"DIFERENCA_CV",
        "data":dtRef,
    })


def modelo_eta(dadosProduto):
    path_download = os.path.join(PATH_WEBHOOK_TMP,dadosProduto['nome'])
    filename = DIR_TOOLS.downloadFile(dadosProduto['url'], path_download)
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

    airflow_tools.trigger_airflow_dag(
        dag_id="PCONJUNTO",
        json_produtos={
            'dt_ref':dadosProduto['dataProduto']
            })


def carga_patamar(dadosProduto):
    path_download = os.path.join(PATH_WEBHOOK_TMP,dadosProduto['nome'])
    filename = DIR_TOOLS.downloadFile(dadosProduto['url'], path_download)
    logger.info(filename)

    ids_to_modify = update_estudo.get_ids_to_modify()
    logger.info(ids_to_modify)
    ids_to_modify = [22152]
    update_estudo.update_carga_estudo(ids_to_modify,filename)

    # gerar Produto
    GERAR_PRODUTO.enviar({
        "produto":"REVISAO_CARGA",
        "path":filename,
    })


def deck_preliminar_decomp(dadosProduto):

    path_download = os.path.join(PATH_WEBHOOK_TMP,dadosProduto['nome'])
    filename = DIR_TOOLS.downloadFile(dadosProduto['url'], path_download)
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
    GERAR_PRODUTO.enviar({
        "produto":"CMO_DC_PRELIMINAR",
        "path":filename,
    })

    
def deck_entrada_saida_dessem(dadosProduto):

    path_download = os.path.join(PATH_WEBHOOK_TMP,dadosProduto['nome'])
    filename = DIR_TOOLS.downloadFile(dadosProduto['url'], path_download)
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

    GERAR_PRODUTO.enviar({
        "produto":"RESULTADO_DESSEM",
        "data":dtRef,
    })
    
    dessem.organizarArquivosOns(dtRef, path_arquivos_ds, importarDb=False, enviarEmail=True)
    logger.info(f"Triggering DESSEM_convertido, dt_ref: {dadosProduto['dataProduto']}")
    airflow_tools.trigger_airflow_dag(
        dag_id="DESSEM_convertido",
        json_produtos={
            'dt_ref':dadosProduto['dataProduto']
            })
    
    deck_ds.importar_renovaveis_ds(path_file=filename, dt_ref=dtRef, str_fonte='ons')
    
    GERAR_PRODUTO.enviar({
        "produto":"PREVISAO_CARGA_DESSEM",
        "data":dtRef,
    })
    

    
def previsao_carga_dessem(dadosProduto):
    
    path_download = os.path.join(PATH_WEBHOOK_TMP,dadosProduto['nome'])
    filename = DIR_TOOLS.downloadFile(dadosProduto['url'], path_download)
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
    GERAR_PRODUTO.enviar({
        "produto":"PREVISAO_CARGA_DESSEM",
        "data":dtRef,
    })

 
def prevCarga_dessem(dadosProduto):

    path_download = os.path.join(PATH_WEBHOOK_TMP,dadosProduto['nome'])
    filename = DIR_TOOLS.downloadFile(dadosProduto['url'], path_download)
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


def carga_patamar_nw(dadosProduto):
    

    path_download = os.path.join(PATH_WEBHOOK_TMP,dadosProduto['nome'])
    filename = DIR_TOOLS.downloadFile(dadosProduto['url'], path_download)
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
        dataReferente = dtRef_last_saturday, 
        str_fonte = id_fonte)

    #gerar Produto
    GERAR_PRODUTO.enviar({
        "produto":"REVISAO_CARGA_NW",
        "path":filename,
    })


def carga_IPDO(dadosProduto):
    

    path_download = os.path.join(PATH_WEBHOOK_TMP,dadosProduto['nome'])
    filename = DIR_TOOLS.downloadFile(dadosProduto['url'], path_download)
    logger.info(filename)

    path_copia_tmp = DIR_TOOLS.copy_src(filename, PATH_PLAN_ACOMPH_RDH)
    logger.info(path_copia_tmp)

    dtRef = datetime.datetime.strptime(
          dadosProduto["dataProduto"], "%d/%m/%Y"
      )

    carga_ons.importar_carga_ipdo(filename,dtRef)

    #gerar Produto
    GERAR_PRODUTO.enviar({
        "produto":"IPDO",
        "data":dtRef,
        "path": filename
    })
    
    
def modelo_ECMWF(dadosProduto):
    path_download = os.path.join(PATH_WEBHOOK_TMP,dadosProduto['nome'])
    filename = DIR_TOOLS.downloadFile(dadosProduto['url'], path_download)
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

    airflow_tools.trigger_airflow_dag(
        dag_id="PCONJUNTO",
        json_produtos={
            'dt_ref':dadosProduto['dataProduto']
            })

    # dtRef = datetime.datetime.strptime(dadosProduto["dataProduto"],'%d/%m/%Y')
    # dst = os.path.join(PATH_CV,dtRef.strftime('%Y%m%d'),'ECMWF','ONS')
    # os.makedirs(dst, exist_ok=True)
    # extracted_files = DIR_TOOLS.extract_specific_files_from_zip(
    #     path=filename,
    #     files_name_template= ["ECMWF_p*a*.dat"],
    #     date_format='%d%m%y',
    #     dst=dst)
    # logger.info(extracted_files)

            
def dados_geracaoEolica(dadosProduto):

    path_download = os.path.join(PATH_WEBHOOK_TMP,dadosProduto['nome'])
    filename = DIR_TOOLS.downloadFile(dadosProduto['url'], path_download)
    logger.info(filename)
    
    #arquivo copia
    dtReferente = datetime.datetime.strptime(dadosProduto["dataProduto"], '%d/%m/%Y')
    path_dessem_diario = '/WX2TB/Documentos/fontes/PMO/arquivos/diario/{}/dessem'.format(dtReferente.strftime('%Y%m%d'))
    path_copia_tmp = DIR_TOOLS.copy_src(filename, path_dessem_diario)
    logger.info(path_copia_tmp)

    geracao.importar_eolica_files(path_zip = filename)
    
    
def prevCarga_dessem_saida(dadosProduto):

    path_download = os.path.join(PATH_WEBHOOK_TMP,dadosProduto['nome'])
    filename = DIR_TOOLS.downloadFile(dadosProduto['url'], path_download)
    logger.info(filename)

    dtReferente = datetime.datetime.strptime(dadosProduto["dataProduto"], '%d/%m/%Y')

    path_prev_carga_dessem = '/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/dessem/arquivos/{}/entrada/PrevCargaDessem'.format(dtReferente.strftime('%Y%m%d'))
    path_copia_tmp = DIR_TOOLS.copy_src(filename, path_prev_carga_dessem)
    logger.info(path_copia_tmp)

    carga_ons.importar_prev_carga_dessem_saida(path_zip = filename)
    

def modelo_gefs(dadosProduto):

    path_download = os.path.join(PATH_WEBHOOK_TMP,dadosProduto['nome'])
    filename = DIR_TOOLS.downloadFile(dadosProduto['url'], path_download)
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

    airflow_tools.trigger_airflow_dag(
        dag_id="PCONJUNTO",
        json_produtos={
            'dt_ref':dadosProduto['dataProduto']
            })

    # dtRef = datetime.datetime.strptime(dadosProduto["dataProduto"],'%d/%m/%Y')
    # dst = os.path.join(PATH_CV,dtRef.strftime('%Y%m%d'),'ECMWF','ONS')
    # os.makedirs(dst, exist_ok=True)
    # extracted_files = DIR_TOOLS.extract_specific_files_from_zip(
    #     path=filename,
    #     files_name_template= ["ECMWF_p*a*.dat"],
    #     date_format='%d%m%y',
    #     dst=dst)
    # logger.info(extracted_files)


def vazoes_observadas(dadosProduto):

    path_download = os.path.join(PATH_WEBHOOK_TMP,dadosProduto['nome'])
    filename = DIR_TOOLS.downloadFile(dadosProduto['url'], path_download)
    logger.info(filename)

    # Data dp relatorio hidrologico vem sempre com a data do dia anterior, portanto e necessario adicionar um dia
    dtRef = datetime.datetime.strptime(
        dadosProduto["dataProduto"], "%d/%m/%Y"
    ) + datetime.timedelta(days=1)

    #novo smap
    vazao.process_planilha_vazoes_obs(filename)
    SmapTools.trigger_dag_SMAP(dtRef)
    SmapTools.resultado_cv_obs(
        dtRef.date(),
        fonte_referencia='psat',
        dst_path= os.path.join(PATH_CV,dtRef.strftime("%Y%m%d"),'fontes')
        )
    


def psat_file(dadosProduto):

    path_download = os.path.join(PATH_WEBHOOK_TMP,dadosProduto['nome'])
    filename = DIR_TOOLS.downloadFile(dadosProduto['url'], path_download)
    logger.info(filename)

    path_pconjunto_psat = os.path.join(PATH_ROTINA_CONJUNTO,"Arq_Entrada","Observado")
    path_copia_tmp = DIR_TOOLS.copy_src(filename, path_pconjunto_psat)
    logger.info(path_copia_tmp)


    dtRef = datetime.datetime.strptime(
        dadosProduto["dataProduto"], "%d/%m/%Y"
    )

    #cria figuras
    plot.plotPrevDiaria(modelo="psat",dataPrevisao=dtRef,rodada=0)
    wx_plota_pconjunto.plota_psat(dtRef)


def resultados_nao_consistidos_semanal(dadosProduto):

    path_download = os.path.join(PATH_WEBHOOK_TMP,dadosProduto['nome'])
    filename = DIR_TOOLS.downloadFile(dadosProduto['url'], path_download)
    logger.info(filename)

    path_decomp_downloads = '/WX2TB/Documentos/fontes/PMO/monitora_ONS/DECOMP/downloads'
    path_copia_tmp = DIR_TOOLS.copy_src(filename, path_decomp_downloads)
    logger.info(path_copia_tmp)

    wx_libs_preco.nao_consistido_rv(filename)
    # manda arquivo .zip para maquina newave'
    cmd = f"cp {filename} /WX/SERVER_NW/WX4TB/Documentos/fontes/PMO/decomp/entradas/DC_preliminar/;"
    os.system(cmd)

    airflow_tools.trigger_airflow_dag(
        dag_id="DECOMP",
        json_produtos={
            'dt_ref':dadosProduto['dataProduto']
            })

    
    

def relatorio_resutados_finais_consistidos(dadosProduto):

    path_download = os.path.join(PATH_WEBHOOK_TMP,dadosProduto['nome'])
    filename = DIR_TOOLS.downloadFile(dadosProduto['url'], path_download)
    logger.info(filename)

    path_copia_tmp = DIR_TOOLS.copy_src(filename, PATH_PLAN_ACOMPH_RDH)
    logger.info(path_copia_tmp)

    # wx_libs_preco.dadvaz_pdp(filename,data["dataProduto"])
    filename_splited = os.path.basename(filename).split('_')
    diaPrevisao = int(filename_splited[3])
    mesPrevisao = int(filename_splited[4])
    anoPrevisao = int(filename_splited[5])
    dtPrevisao = datetime.datetime(anoPrevisao, mesPrevisao, diaPrevisao)

    revisao.importar_prev_ena_consistido(filename)
    GERAR_PRODUTO.enviar({
    "produto":"PREVISAO_ENA_SUBMERCADO",
    "data":dtPrevisao,
    })

def niveis_partida_dessem(dadosProduto):
    path_download = os.path.join(PATH_WEBHOOK_TMP,dadosProduto['nome'])
    filename = DIR_TOOLS.downloadFile(dadosProduto['url'], path_download)
    logger.info(filename)

    path_copia_tmp = DIR_TOOLS.copy_src(filename, PATH_PLAN_ACOMPH_RDH)
    logger.info(path_copia_tmp)

    vazao.importNiveisDessem(filename)

def dadvaz_vaz_prev(dadosProduto):

    path_download = os.path.join(PATH_WEBHOOK_TMP,dadosProduto['nome'])
    filename = DIR_TOOLS.downloadFile(dadosProduto['url'], path_download)
    logger.info(filename)

    dtReferente = datetime.datetime.strptime(dadosProduto["dataProduto"], '%d/%m/%Y')
    path_dessem_diario = '/WX2TB/Documentos/fontes/PMO/arquivos/diario/{}/dessem'.format(dtReferente.strftime('%Y%m%d'))
    path_copia_tmp = DIR_TOOLS.copy_src(filename, path_dessem_diario)
    logger.info(path_copia_tmp)
    
def deck_resultados_decomp(dadosProduto):

    path_download = os.path.join(PATH_WEBHOOK_TMP,dadosProduto['nome'])
    filename = DIR_TOOLS.downloadFile(dadosProduto['url'], path_download)
    logger.info(filename)

    dtInicial, dtFinal = dadosProduto["dataProduto"].split(' - ')
    dtInicial = datetime.datetime.strptime(dtInicial, '%d/%m/%Y')
    dataEletrica =  wx_opweek.ElecData(dtInicial.date())
    # /WX2TB/Documentos/fontes/PMO/arquivos/decks/202101_RV1
    path_decks = '/WX2TB/Documentos/fontes/PMO/arquivos/decks/{}{:0>2}_RV{}'.format(dataEletrica.anoReferente, dataEletrica.mesReferente, dataEletrica.atualRevisao)
    path_copia_tmp = DIR_TOOLS.copy_src(filename, path_decks)
    logger.info(path_copia_tmp)
    

def resultados_finais_consistidos(dadosProduto):

    path_download = os.path.join(PATH_WEBHOOK_TMP,dadosProduto['nome'])
    filename = DIR_TOOLS.downloadFile(dadosProduto['url'], path_download)
    logger.info(filename)

    dtInicial = dadosProduto["dataProduto"]
    dtInicial = datetime.datetime.strptime(dtInicial, '%d/%m/%Y')
    dataEletrica =  wx_opweek.ElecData(dtInicial.date())
    path_decks = os.path.abspath('/WX2TB/Documentos/fontes/PMO/arquivos/decks/{}{:0>2}_RV{}'.format(dataEletrica.anoReferente, dataEletrica.mesReferente, dataEletrica.atualRevisao))
    path_copia_tmp = DIR_TOOLS.copy_src(filename, path_decks)
    logger.info(path_copia_tmp)


def carga_newave_preliminar(dadosProduto):
    path_download = os.path.join(PATH_WEBHOOK_TMP,dadosProduto['nome'])
    filename = DIR_TOOLS.downloadFile(dadosProduto['url'], path_download)
    logger.info(filename)
    logger.info(dadosProduto)
    
    DIR_TOOLS.extract(filename,path_download)
        
    path_decks = "/WX2TB/Documentos/fontes/PMO/decks/ons/nw"
    
    path_copia_tmp = DIR_TOOLS.extract(filename,path_decks,True)
    
    logger.info(path_copia_tmp)
    
    mes_ano = dadosProduto["dataProduto"]
    mes, ano = mes_ano.split('/')
    dtRef = datetime.datetime.strptime(f"01/{mes}/{ano}", "%d/%m/%Y")
    
    #gerar Produto
    GERAR_PRODUTO.enviar({
        "produto":"REVISAO_CARGA_NW_PRELIMINAR",
        "path":filename,
        "data":dtRef,
    }) 
    


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
    
    
    
def deck_prev_eolica_semanal_patamares(dadosProduto):
    res  = r.get(dadosProduto["url"])
    zip_file = zipfile.ZipFile(io.BytesIO(res.content))
    
    patamates_csv_path = [x for x in zip_file.namelist() if "Arquivos Entrada/Dados Cadastrais/Patamares_" in x and ".csv" in x]
    patamates_csv_path = patamates_csv_path[0]
    
    
    with zip_file.open(patamates_csv_path) as file:
        content = file.read()
        df_patamares = pd.read_csv(io.StringIO(content.decode("latin-1")), sep=";")
        df_patamares.columns = [x[0].lower() + x[1:] for x in df_patamares.columns]
    df_patamares.columns = ['inicio','patamar','cod_patamar','dia_semana','dia_tipico','tipo_dia','intervalo','dia','semana','mes']
    post_patamates = r.post(f"http://{__HOST_SERVIDOR}:8000/api/v2/decks/patamares", json=df_patamares.to_dict("records"))
    if post_patamates.status_code == 200:
        logger.info("Patamares inseridos com sucesso")
    else:
        logger.error(f"Erro ao inserir patamares. status code: {post_patamates.status_code}")

def deck_prev_eolica_semanal_previsao_final(dadosProduto):
    res  = r.get(dadosProduto["url"])
    zip_file = zipfile.ZipFile(io.BytesIO(res.content))
    
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
        
    post_decks_weol = r.post(f"http://{__HOST_SERVIDOR}:8000/api/v2/decks/weol", json=body_weol)
    if post_decks_weol.status_code == 200:
        logger.info("WEOL inserido com sucesso")
    else:
        logger.error(f"Erro ao inserir WEOL. status code: {post_decks_weol.status_code}")
        
def deck_prev_eolica_semanal_weol(dadosProduto:dict):
    airflow_tools.trigger_airflow_dag(
        dag_id="webhook_deck_prev_eolica_semanal_weol",
        json_produtos=dadosProduto
        )


if __name__ == '__main__':
    # deck_prev_eolica_semanal_patamares({"dataProduto":"28/11/2024"})
    deck_prev_eolica_semanal_previsao_final({"dataProduto":"16/11/2024"})
    # Copiar os parametros no airflow e colar aqui para testar
    # params = {"product_details": {"nome": "Arquivos dos modelos de previs\u00e3o de vaz\u00f5es di\u00e1rias - PDP", "processo": "Previs\u00e3o de Vaz\u00f5es Di\u00e1rias - PDP", "dataProduto": "22/07/2024", "macroProcesso": "Programa\u00e7\u00e3o da Opera\u00e7\u00e3o", "periodicidade": "2024-07-22T00:00:00", "periodicidadeFinal": "2024-07-22T23:59:59", "url": "https://apps08.ons.org.br/ONS.Sintegre.Proxy/webhook?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJVUkwiOiJodHRwczovL3NpbnRlZ3JlLm9ucy5vcmcuYnIvc2l0ZXMvOS8xMy84Mi9Qcm9kdXRvcy8yMzgvTW9kZWxvc19DaHV2YV9WYXphb18yMDI0MDcyMi56aXAiLCJ1c2VybmFtZSI6InRoaWFnby5zY2hlckByYWl6ZW4uY29tIiwibm9tZVByb2R1dG8iOiJBcnF1aXZvcyBkb3MgbW9kZWxvcyBkZSBwcmV2aXPDo28gZGUgdmF6w7VlcyBkacOhcmlhcyAtIFBEUCIsIklzRmlsZSI6IlRydWUiLCJpc3MiOiJodHRwOi8vbG9jYWwub25zLm9yZy5iciIsImF1ZCI6Imh0dHA6Ly9sb2NhbC5vbnMub3JnLmJyIiwiZXhwIjoxNzIxODIzNjgxLCJuYmYiOjE3MjE3MzcwNDF9.jvsi4HOIZsme-EqFHlgocqTZe4W2DDuL13E3NCBqoGY"}, "function_name": "arquivos_modelo_pdp"}
    # function_name = params.get('function_name')
    # product_details = params.get('product_details')
    # arquivos_modelo_pdp(product_details)