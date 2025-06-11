import os
import sys
import pdb
import glob
import datetime
import pandas as pd
from typing import Optional
from time import sleep
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")),'.env'))
__API_URL_AIRFLOW = os.getenv('API_URL_AIRFLOW') 

diretorioApp = os.path.dirname(os.path.abspath(__file__))
PATH_ARQUIVOS_LOCAL = os.path.join(os.path.dirname(diretorioApp),"arquivos")


sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.apps.dbUpdater.libs import vazao
from PMO.scripts_unificados.bibliotecas import rz_dir_tools
from PMO.scripts_unificados.apps.airflow import airflow_tools
from PMO.scripts_unificados.apps.smap.libs import BuildStructure


DIR_TOOLS = rz_dir_tools.DirTools()
PATH_ARQUIVOS_SMAP = "/WX2TB/Documentos/fontes/PMO/raizen-power-trading-previsao-hidrologia/smap/arquivos"

__RODADAS =  {
        0:[
            "GEFS","GFS","EC","PCONJUNTO",
            "PCONJUNTO2","PMEDIA","EC-ensremvies",
            "ETA40remvies","GEFSremvies","PCONJUNTO-EXT",
            "PZERADA","ETA40"
            ],
        6: ["GEFS","GFS"],
        12:["GEFS","GFS","EC"],
        18:["GEFS","GFS"]
    }   

def copy_pdp_files(path_src:str,dst:str,file_to_copy:str,upper=False):

    padraoNomeAjuste = os.path.join(path_src,'**',file_to_copy)
    copy_files = DIR_TOOLS.copy_src(padraoNomeAjuste,dst,upper=upper)
    print(f"Arquivos salvos: {copy_files}" )
    return copy_files

def organizar_chuva_vazao_files(pdp_file_zip:str,files_to_copy:list=[], flag_db=True):
    
    file_zip_exists= DIR_TOOLS.get_name_insentive_name(pdp_file_zip)
    if not file_zip_exists: raise Exception(f'Arquivo não encontrado: {pdp_file_zip}')

    dt_now = datetime.datetime.now().strftime("%d%m%Y %H%M%S")
    path_extraction = os.path.join(PATH_ARQUIVOS_LOCAL,"tmp",dt_now)
    DIR_TOOLS.extract(zipFile=pdp_file_zip,path=path_extraction)

    if 'AJUSTE' in files_to_copy: 
        copied_files = copy_pdp_files(path_src=path_extraction,dst=os.path.join(PATH_ARQUIVOS_SMAP,'auxilio_ajuste_inicializacao'),file_to_copy='*AJUSTE.txt',upper=True)
        if not copied_files: copy_pdp_files(path_src=path_extraction,dst=os.path.join(PATH_ARQUIVOS_SMAP,'auxilio_ajuste_inicializacao'),file_to_copy='*ajuste.txt',upper=True)
        
    if 'PlanilhaUSB' in files_to_copy:
        copy_pdp_files(path_src=path_extraction,dst=os.path.join(PATH_ARQUIVOS_SMAP,'cpins'),file_to_copy='PlanilhaUSB*')

    if flag_db: 
        process_vazao_obs_pdp(path_src=path_extraction)

    DIR_TOOLS.remove_src(path_extraction)


def process_vazao_obs_pdp(path_src:str):
    PATH_LISTA_VAZOES= "/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/smap/arquivos/opera-smap/smap_novo/info_vazao_obs.json"
    
    df_lista_vazoes = pd.read_json(PATH_LISTA_VAZOES)

    values = []
    for subbacia in df_lista_vazoes.keys():
        if subbacia == 'GOV. JAYME CANET':
            subbacia = 'MAUA'
        try:
            file_subbacia = glob.glob(os.path.join(path_src,'**',f'{subbacia}.txt'),recursive=True)[0]
        except:
            print(f"WARNING    -    Arquivo não encontrado: {subbacia}.txt")
        print(file_subbacia)
        df_subbacia = pd.read_csv(file_subbacia,sep='|',header=None)
        df_subbacia['subbacia'] = subbacia
        values += df_subbacia[['subbacia',0,3,4,5]].values.tolist()

    vazao.importar_vazoes_obs_smap(values)

    return values

def resultado_cv_obs(dt_rodada:datetime.date, fonte_referencia:str,dst_path:str):

    dt_ini= dt_rodada - datetime.timedelta(days=1)

    StructureSMAP = BuildStructure.StructureSMAP(modelos=[])

    df_vazao_obs,_ = StructureSMAP.historico_vazao(dt_ini=dt_ini,dt_fim=dt_rodada)
    df_vazao_obs = df_vazao_obs.loc[dt_ini.strftime("%Y-%m-%d")].reset_index() 

    df_chuva_obs,ultima_dt_chuva = StructureSMAP.historico_psat(dt_ini=dt_ini,dt_fim=dt_rodada)
    df_chuva_obs = df_chuva_obs.loc[dt_rodada].reset_index()

    StructureSMAP.info_bacias()
    bacias = glob.glob(BuildStructure.PATH_ESTRUTURA_BASE_SMAP+"/**")

    for bacia in bacias:
        bacia = os.path.basename(bacia)
        tt = glob.glob(BuildStructure.PATH_ESTRUTURA_BASE_SMAP+f"/{bacia}/ARQ_ENTRADA/*PSAT*")
        psat_sem_extensao = [os.path.splitext(os.path.basename(name))[0].split('_')[0] for name in tt] 
        df_chuva_obs['txt_nome_subbacia'] = df_chuva_obs['txt_nome_subbacia'].str.zfill(8).str.upper()

        df_chuva_obs.loc[df_chuva_obs['txt_nome_subbacia'].isin(psat_sem_extensao), 'bacia'] = bacia  

        df_vazao_obs['txt_nome_subbacia'] = df_vazao_obs['txt_nome_subbacia'].str.upper()
        df_vazao_obs.loc[df_vazao_obs['txt_nome_subbacia'].isin(StructureSMAP.bacias[bacia]), 'bacia'] = bacia  

    if dt_rodada > ultima_dt_chuva: 
        fonte= "GPM"
    else: 
        fonte = fonte_referencia
    
    print("Escrevendo fonte de dados:",fonte)
    os.makedirs(dst_path,exist_ok=True)

    df_vazao_obs.to_csv(os.path.join(dst_path,f"vazao_{fonte}.txt"), sep=" ", index=None, header=None)
    df_chuva_obs.to_csv(os.path.join(dst_path,f"chuva_{fonte}.txt"), sep=" ", index=None, header=None)
    
    print(os.path.join(dst_path,f"obs_vazao_{fonte}.txt"))
    print(os.path.join(dst_path,f"obs_chuva{fonte}.txt"))


def convert_modelos_to_json(dt_rodada:datetime.datetime,modelos_names:list=[], rodada:int=None):

    if (rodada!=None and modelos_names!=[]):
        rodadas = {rodada:modelos_names}
    else:
        rodadas = __RODADAS
        
    modelos = []
    for hr in rodadas:
        for modelo in rodadas[hr]:
            modelos += (modelo,hr,dt_rodada.strftime("%d/%m/%Y")),
    json_produtos = {"modelos": modelos}
    return json_produtos


def trigger_dag_SMAP(dt_rodada:datetime.date, modelos_names:list=[], rodada:int=None, momento_req:Optional[datetime.datetime]=None):

    json_produtos = convert_modelos_to_json(
        dt_rodada=dt_rodada,
        modelos_names=modelos_names,
        rodada=rodada)
    print(json_produtos)
    airflow_tools.trigger_airflow_dag(dag_id='SSH_SMAP',json_produtos=json_produtos, momento_req=momento_req)

def get_dag_smap_run_status(nome_modelo:str, momento_req:datetime.datetime) -> str:
    dag_id:str = 'SSH_SMAP'
    dag_run_id:str = f"{nome_modelo}{momento_req.strftime('_%d_%m_%Y_%H_%M_%S')}"
    task_url = __API_URL_AIRFLOW.removesuffix('api/v1')
    task_url = f"{task_url}/dags/{dag_id}/grid?dag_run_id={dag_run_id}"
    sleep(10)
    
    while True:
        res:dict = airflow_tools.get_dag_run_state(dag_id=dag_id, dag_run_id=dag_run_id)
        if not(res["state"] == 'success' or res["state"] == 'failed'):
            sleep(10)
            print('executando...')
        else:
            return {"state":res["state"], "url":task_url, "end_datetime":res["end_datetime"]}


if __name__ == '__main__':
    dt = datetime.datetime.now()
    hr=0
    # modelos = ['PCONJUNTO']
    # trigger_dag_SMAP(dt_rodada=dt,modelos_names=modelos,rodada=hr)

    # path = r"C:\Users\CS399274\Downloads\Modelos_Chuva_Vazao_20240708.zip"
    # organizar_chuva_vazao_files(path, files_to_copy=["AJUSTE","PlanilhaUSB"])
