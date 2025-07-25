# -*- coding: utf-8 -*-
import os
import re
import sys
import pdb
import glob
import time
import shutil
import logging
import datetime
import subprocess
import pandas as pd
import requests as req
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO,
                    format='%(levelname)s:\t%(asctime)s\t %(name)s.py:%(lineno)d\t %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers=[
                        logging.StreamHandler()
                    ])
logger = logging.getLogger(__name__)

load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")),'.env'))

sys.path.insert(1,"/WX2TB/Documentos/fontes/")
# from PMO.scripts_unificados.apps.dbUpdater.libs import rodadas
# from PMO.scripts_unificados.bibliotecas import wx_dbClass
# from PMO.scripts_unificados.apps.tempo.libs import conversorArquivos
from PMO.scripts_unificados.apps.smap.libs import SmapTools
# from PMO.scripts_unificados.apps.pconjunto import wx_plota_pconjunto

PATH_HOME = os.path.expanduser('~')
PATH_CV = os.path.abspath("/WX2TB/Documentos/chuva-vazao")
PATH_DROPBOX_MIDDLE = os.path.join(PATH_HOME, "Dropbox", "WX - Middle")

PATH_PCONJUNTO = os.path.dirname(os.path.abspath(__file__))
PATH_ROTINA_CONJUNTO = os.path.join(PATH_PCONJUNTO, "pconjunto-ONS")
PATH_ROTINA_CONJUNTO_TMP = os.path.join(PATH_PCONJUNTO, "pconjunto-tmp")
PATH_PREVISAO_MODELOS_RZ = os.path.abspath('/WX4TB/Documentos/saidas-modelos')


MODELOS_COMPOSICAO_PCONJUNTO = ['ETA','GEFS', 'ECMWF']
MODELOS_COMPOSICAO_PCONJUNTO_RZ = {
    "gefs": "GEFS",
    "ecmwf-ens": "ECMWF",
    "eta": "ETA40"
    }

MODELOS_DERIVADOS = ['pconjunto','pconjunto2']
URL_COGNITO = os.getenv('URL_COGNITO')
CONFIG_COGNITO = os.getenv('CONFIG_COGNITO')
URL_HTML_TO_IMAGE = os.getenv('URL_HTML_TO_IMAGE')
def get_access_token() -> str:
    response = req.post(
        URL_COGNITO,
        data=CONFIG_COGNITO,
        headers={'Content-Type': 'application/x-www-form-urlencoded'}
    )
    return response.json()['access_token']


def previsao_conjunto_ONS(param):

    data = param['data']
    f_forcarRodar = param['forcarRodar']

    path_dropbox_working_date = os.path.join(PATH_DROPBOX_MIDDLE, "NovoSMAP", data.strftime('%Y%m%d'), "rodada00z")
    path_cv_working_date = os.path.join(PATH_CV, data.strftime('%Y%m%d'))

    print("\nRODANDO PCONJUNTO -",data.strftime('%d/%m/%Y'))

    cria_diretorio(path_dropbox_working_date)

    task_conjunto_rz(data, f_forcarRodar)
    task_conjunto_ons(data, f_forcarRodar)

    print("Rotina Finalizada!")

    return True


def task_conjunto_rz(dt_rodada_exec, forcar_rodar=False):
    if forcar_rodar: exists = False
    else:
        exists = verifica_rodada_R_existente(PATH_ROTINA_CONJUNTO_TMP, dt_rodada_exec)
    if (not exists) :

        crirPconjuntoTmp()
        get_modelos_conjunto_rz(dt_rodada_exec)

        files_exists = verifica_arquivos_entrada(PATH_ROTINA_CONJUNTO_TMP, dt_rodada_exec)

        if files_exists:
            print(f'\n### CONJUNTO: PCONJUNTO-RZ')
            executarScriptR(PATH_ROTINA_CONJUNTO_TMP, dt_rodada_exec)
            sufixo='-rz'
            df_prev_chuva = organizarArquivosSaida(data=dt_rodada_exec, path_rotina=PATH_ROTINA_CONJUNTO_TMP, sufixo=sufixo, )
            post_prev_chuva(df_prev_chuva)
        else:
            print("Arquivos Faltantes!")
    else:
        print("Task Conjunto ONS ja foi executada!")

def task_conjunto_ons(dt_rodada_exec, forcar_rodar=False):

    if forcar_rodar: exists = False
    else:
        exists = verifica_rodada_R_existente(PATH_ROTINA_CONJUNTO, dt_rodada_exec)
    
    if (not exists) :

        files_exists = verifica_arquivos_entrada(PATH_ROTINA_CONJUNTO, dt_rodada_exec)

        if files_exists:
            print(f'\n### CONJUNTO: PCONJUNTO-ONS')
            executarScriptR(PATH_ROTINA_CONJUNTO, dt_rodada_exec)
            derivados = ['PMEDIA', 'PCONJUNTO', 'PCONJUNTO2', 'PCONJUNTO-EXT'] + MODELOS_COMPOSICAO_PCONJUNTO
            sufixo='-ons'
            numdias=14
            df_prev_chuva = organizarArquivosSaida(data=dt_rodada_exec, path_rotina=PATH_ROTINA_CONJUNTO, sufixo=sufixo)
            post_prev_chuva(df_prev_chuva)

        else:
            print("Arquivos Faltantes!")
    else:
        print("Task Conjunto ONS ja foi executada!")


def get_modelo_chuva_by_id(id_chuva:int,dt_ini:str=None,dt_fim:str=None):

        response_rodadas = req.get('https://tradingenergiarz.com/api/v2/rodadas/chuva/previsao',
                    params = {
                        "id_chuva":id_chuva,
                        "granularidade":"subbacia",
                        "dt_inicio_previsao": dt_ini,
                        "dt_fim_previsao": dt_ini,
                    },
                    headers={
                'Authorization': f'Bearer {get_access_token()}'
            }
                )
        return response_rodadas.json()

def get_rodadas_do_dia(dt_rodada):
    response_rodadas = req.get('https://tradingenergiarz.com/api/v2/rodadas',
        params = {
            "dt": dt_rodada,
            "no_cache": "true",
            "atualizar": "false"
        },
        headers={
                'Authorization': f'Bearer {get_access_token()}'
            }
    )
    return response_rodadas.json()

def gerar_modelos_derivados(df_previsao_modelos:pd.DataFrame,sufixo='-ons') -> pd.DataFrame:

    df_previsao_modelos['dt_rodada'] = pd.to_datetime(df_previsao_modelos['dt_rodada'])
    df_previsao_modelos['dt_prevista'] = pd.to_datetime(df_previsao_modelos['dt_prevista'])

    dias_ate_quinta = (3-df_previsao_modelos['dt_rodada'].dt.dayofweek +7) % 7
    quinta_feira = df_previsao_modelos['dt_rodada'] + datetime.timedelta(days=int(dias_ate_quinta.unique()[0]))
    mask = (df_previsao_modelos['dt_prevista'] <= quinta_feira)

    for modelo in MODELOS_DERIVADOS:


        df_pmedia_to_modify = df_previsao_modelos[df_previsao_modelos['modelo'] == 'PMEDIA'].copy()
        
        if modelo =='pconjunto':
            #completar até quinta feira com o gefs-ens , se for sexta ou sabado completar até a quinta da semana que vem
            df_composicao = df_previsao_modelos[df_previsao_modelos['modelo'] == f'gefs{sufixo}'].copy()

        elif modelo =='pconjunto2':
            #completar até quinta feira com o ecmwf-ens , se for sexta ou sabado completar até a quinta da semana que vem
            df_composicao = df_previsao_modelos[df_previsao_modelos['modelo'] == f'ecmwf-ens{sufixo}'].copy()

        df_pmedia_to_modify.loc[mask, df_composicao.columns] = df_composicao.loc[mask,df_composicao.columns].values
        df_pmedia_to_modify.loc[:,'modelo'] = modelo.lower() + sufixo
        df_previsao_modelos = pd.concat([df_previsao_modelos, df_pmedia_to_modify], ignore_index=True)

        df_previsao_modelos['dt_rodada'] = pd.to_datetime(df_previsao_modelos['dt_rodada']).apply(lambda x: x.isoformat())
        df_previsao_modelos['dt_prevista'] = pd.to_datetime(df_previsao_modelos['dt_prevista']).dt.strftime("%Y-%m-%d")


    return df_previsao_modelos


def escreve_saida_modelos_derivados(df_previsao_modelos, path_dropbox_working_date, sufixo):

    df_previsao_modelos['dt_prevista'] = pd.to_datetime(df_previsao_modelos['dt_prevista']).dt.date
    df_previsao_modelos['dt_rodada'] = pd.to_datetime(df_previsao_modelos['dt_rodada']).dt.date
    
    dt_rodada = df_previsao_modelos['dt_rodada'].unique()[0]

    df_pconjunto_out = df_previsao_modelos[df_previsao_modelos['modelo']==f'pconjunto{sufixo}']
    df_to_write = df_pconjunto_out[["dt_prevista","Longitude","Latitude","vl_chuva"]].set_index('dt_prevista')
    for dt_prevista in df_to_write.index.unique():
        file = os.path.join(path_dropbox_working_date, f'PCONJUNTO_p{dt_rodada.strftime("%d%m%y")}a{dt_prevista.strftime("%d%m%y")}.dat')
        print(file)
        df_to_write.loc[dt_prevista].to_csv(file, sep = '\t',header=False,index=False)

            
def read_configs_file()-> pd.DataFrame:
    path_configs =os.path.join(PATH_ROTINA_CONJUNTO,'Arq_Entrada',"configuracao.xlsx")
    df_configs = pd.read_excel(path_configs)
    return df_configs

def get_subbacias() -> pd.DataFrame:
    res = req.get("https://tradingenergiarz.com/api/v2/rodadas/subbacias",
                  headers={
                'Authorization': f'Bearer {get_access_token()}'
            })
    df_response = pd.DataFrame(res.json()).rename(columns={"vl_lat":"Latitude","vl_lon":"Longitude",'id':"cd_subbacia","nome":"Codigo ANA"})
    df_configs = read_configs_file()
    df_subbacias_completo = pd.merge(df_configs[['Latitude','Longitude','Codigo ANA']],df_response[['Codigo ANA','cd_subbacia']])
    return df_subbacias_completo


def get_modelos_conjunto_rz(dt_rodada):

    response_rodadas = get_rodadas_do_dia(dt_rodada.strftime('%Y-%m-%d'))
    df_rodadas_do_dia = pd.DataFrame(response_rodadas)
    df_subbacias_config = get_subbacias()

    modelos_rz = df_rodadas_do_dia[
        (df_rodadas_do_dia['str_modelo'].str.lower().isin(MODELOS_COMPOSICAO_PCONJUNTO_RZ.keys())) & 
        (df_rodadas_do_dia['hr_rodada'] == 0)
        ]
    ids_chuva = modelos_rz['id_chuva'].unique()
    
    for id_chuva in ids_chuva:
        response_prev_chuva = get_modelo_chuva_by_id(
            id_chuva=id_chuva,
        )
        df_modelo_rz = pd.DataFrame(response_prev_chuva)
        df_modelo_rz['modelo'] = df_modelo_rz['modelo'].str.lower()
        df_modelo_rz_completo = pd.merge(df_modelo_rz,df_subbacias_config, on='cd_subbacia')

        modelo = MODELOS_COMPOSICAO_PCONJUNTO_RZ[df_modelo_rz['modelo'].unique()[0]]
        file = os.path.join(PATH_ROTINA_CONJUNTO_TMP,'Arq_Entrada',modelo,f'{modelo}_m_{dt_rodada.strftime("%d%m%y")}.dat')
        print(file)

        teste = df_modelo_rz_completo.pivot_table(
            index=["Codigo ANA", "Latitude", "Longitude"],
            columns="dt_prevista",
            values="vl_chuva",
        ).reset_index()

        if not modelo == 'ECMWF':
            teste = teste.iloc[:,:-1]

        teste.to_csv(file, sep = ' ',header=False,index=False)


def gerar_arq_modelos_saida(dt_rodada,path_rotina,sufixo='-ons'):

        path_conjunto_entrada = os.path.join(path_rotina,'Arq_Entrada')
        path_conjunto_saida = os.path.join(path_rotina,'Arq_Saida')

        path_ecmwf = os.path.join(path_conjunto_entrada,'ECMWF',f'ECMWF_m_{dt_rodada.strftime("%d%m%y")}.dat')
        path_gefs = os.path.join(path_conjunto_entrada,'GEFS',f'GEFS_m_{dt_rodada.strftime("%d%m%y")}.dat')
        path_eta = os.path.join(path_conjunto_entrada,'ETA40',f'ETA40_m_{dt_rodada.strftime("%d%m%y")}.dat')
        
        path_ecmwf_vies = os.path.join(path_conjunto_saida, 'ECMWF_rem_vies.dat')
        path_gefs_vies = os.path.join(path_conjunto_saida,'GEFS_rem_vies.dat')
        path_eta_vies = os.path.join(path_conjunto_saida, 'ETA40_rem_vies.dat')

        config_modelos= [
            #essemble 0.1
            {"modelo":f"ecmwf-ens{sufixo}", "ndias": 14, "path":path_ecmwf },
            #essemble
            {"modelo":f"gefs{sufixo}", "ndias": 14, "path":path_gefs },
            {"modelo":f"eta40{sufixo}", "ndias": 9, "path":path_eta },

            {"modelo":f"ecmwf-ens-remvies{sufixo}", "ndias": 14, "path":path_ecmwf_vies },
            {"modelo":f"gefs-remvies{sufixo}", "ndias": 14, "path":path_gefs_vies },
            {"modelo":f"eta40-remvies{sufixo}", "ndias": 9, "path":path_eta_vies },
            ]

        for config in config_modelos:
            print(f"Escrevendo modelo: {config['modelo']}")

            escreve_arquivos_chuva(
                dt_rodada,
                config['path'],
                config['modelo'],
                path_conjunto_saida,
                config['ndias'],
                sufixo
                )


def organizarArquivosSaida(data, path_rotina, sufixo='-ons'):
    
    print("GERANDO ARQUIVOS DE SAIDA")
    path_conjunto_saida = os.path.join(path_rotina,'Arq_Saida')
    path_dropbox_working_date = os.path.join(PATH_DROPBOX_MIDDLE, "NovoSMAP", data.strftime('%Y%m%d'), "rodada00z")

    
    def extract_info_name(path_file):

        file_name =  os.path.basename(path_file)
        match = re.match(r'(.+?)_p(\d{6})a(\d{6})\.dat', file_name)
        modelo, dt_rodada,dt_prevista = None,None,None
        if match:
            modelo = match.group(1)
            dt_rodada = datetime.datetime.strptime(match.group(2),"%d%m%y").isoformat()
            dt_prevista = datetime.datetime.strptime(match.group(3),"%d%m%y").strftime("%Y-%m-%d")
        
        return modelo, dt_rodada,dt_prevista

    gerar_arq_modelos_saida(data,path_rotina,sufixo)

    #leitura Arquivos de saida
    df_concatenated = pd.DataFrame()
    file_paths = os.path.join(path_conjunto_saida,f"*_p{data.strftime('%d%m%y')}*.dat")
    forecast_files = glob.glob(file_paths)

    for file_path in forecast_files:

        modelo, dt_rodada,dt_prevista = extract_info_name(file_path)
        if modelo in ["ECMWF","GEFS","ETA40"]:
            continue

        df_temp = pd.read_csv(file_path, delimiter='\s+', header=None, names=['Longitude','Latitude','vl_chuva'])
        df_temp['modelo'] = modelo
        df_temp['dt_rodada'] = dt_rodada
        df_temp['dt_prevista'] = dt_prevista

        df_concatenated = pd.concat([df_concatenated, df_temp], ignore_index=True)

    df_sub_bacias_db = get_subbacias()
    df_previsao_modelos = pd.merge(df_concatenated,df_sub_bacias_db, on=['Longitude','Latitude'])
    df_previsao_modelos = df_previsao_modelos.dropna()
    df_previsao_modelos = gerar_modelos_derivados(df_previsao_modelos,sufixo)
    escreve_saida_modelos_derivados(df_previsao_modelos, path_dropbox_working_date, sufixo)
    df_previsao_modelos.loc[df_previsao_modelos['modelo']=='PMEDIA','modelo'] = f'pmedia{sufixo}'

    if 'rz' in sufixo:
        #nao insere os modelos pois ja estão no banco
        df_previsao_modelos = df_previsao_modelos[~df_previsao_modelos['modelo'].isin(['gefs-rz','ecmwf-ens-rz','eta40-rz'])]
        
    return df_previsao_modelos


def post_prev_chuva(df_previsao_modelos):

    df_previsao_modelos['dt_prevista'] = pd.to_datetime(df_previsao_modelos['dt_prevista']).dt.strftime("%Y-%m-%d")
    df_previsao_modelos['dt_rodada'] = pd.to_datetime(df_previsao_modelos['dt_rodada']).dt.strftime("%Y-%m-%d")

    for modelo in df_previsao_modelos['modelo'].unique():
        previsao_modelos = df_previsao_modelos[df_previsao_modelos['modelo']==modelo][['cd_subbacia','dt_prevista','vl_chuva','modelo','dt_rodada']]
        previsao_modelos = previsao_modelos.dropna()
        previsao_modelos['modelo'] = previsao_modelos['modelo'].str.replace('eta40-ons', 'eta-ons') #mudança no nome do modelo para padronizar com os outros
        response = req.post(
                'https://tradingenergiarz.com/api/v2/rodadas/chuva/previsao/modelos?prev_estendida=true',
                json=previsao_modelos.to_dict('records'),
                headers={
                'Authorization': f'Bearer {get_access_token()}'
            }
            )
        print(f'{modelo} - > Código POST: {response.status_code}')


#===================UTILS=========================

def cria_diretorio(path):
    try:
        os.makedirs(path, exist_ok=True)
        logger.info(f"Directory created successfully: {path}")
    except PermissionError:
        logger.error(f"Permission denied when creating directory: {path}")
        raise
    except OSError as e:
        logger.error(f"OS error when creating directory {path}: {e}")
        try:
            path_parts = path.split(os.sep)
            current_path = ""
            
            for part in path_parts:
                if not part:
                    current_path = os.sep
                    continue
                
                current_path = os.path.join(current_path, part)
                if not os.path.exists(current_path):
                    try:
                        os.mkdir(current_path)
                        logger.info(f"Created intermediate directory: {current_path}")
                    except OSError as mkdir_error:
                        logger.error(f"Failed to create directory {current_path}: {mkdir_error}")
                        raise
        except Exception as fallback_error:
            logger.error(f"Fallback directory creation failed for {path}: {fallback_error}")
            raise

def copia_arquivos(arquivos_copia,path_dst):


    cria_diretorio(path_dst)

    for src in (glob.glob(arquivos_copia)):
        try:
            dst = os.path.join(path_dst, os.path.basename(src))
            shutil.copyfile(src, dst)
        except Exception as e:
            print("Nao foi possivel copiar arquivo " + src)
            print(e)

def crirPconjuntoTmp():
    
    if os.path.exists(PATH_ROTINA_CONJUNTO_TMP):
        shutil.rmtree(PATH_ROTINA_CONJUNTO_TMP)

    pconjuntoTmp_saida = os.path.join(PATH_ROTINA_CONJUNTO_TMP,'Arq_Saida')
    os.makedirs(pconjuntoTmp_saida)

    src = os.path.join(PATH_ROTINA_CONJUNTO,'Arq_Entrada')
    pconjuntoTmp_entrada = os.path.join(PATH_ROTINA_CONJUNTO_TMP,'Arq_Entrada')
    shutil.copytree(src, pconjuntoTmp_entrada)

    src = os.path.join(PATH_ROTINA_CONJUNTO,'Codigos_R')
    dst = os.path.join(PATH_ROTINA_CONJUNTO_TMP,'Codigos_R')
    shutil.copytree(src, dst)

    return True


def executarScriptR(path_rotina, data):
    
    # Data para a entrada do script R (CONJUNTO-ONS)
    pathDataFilePconjunto = os.path.join(path_rotina, 'Arq_Entrada', 'data.txt')
    dataFilePconjunto = open(pathDataFilePconjunto, 'w')
    dataFilePconjunto.write(data.strftime("%d/%m/%Y\n"))
    dataFilePconjunto.close()
    time.sleep(2)
    
    print("\n### INICIANDO ROTINAS EM R")
    pathScriptR = os.path.join(path_rotina, 'Codigos_R', 'Roda_Conjunto_V3.4.R')
    os.chdir(path_rotina)
    try:
        subprocess.run(["Rscript", "--vanilla", pathScriptR], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Ocorreu um erro ao executar o script R: {e}")
        quit()

    print("\n### CODIGOS EM R FINALIZADOS")
    print("")

def verifica_rodada_R_existente(path_rotina,dt_rodada_exec):
    data_seguinte = dt_rodada_exec + datetime.timedelta(days=1)
    arqons = f"PMEDIA_p{dt_rodada_exec.strftime('%d%m%y')}a{data_seguinte.strftime('%d%m%y')}.dat"

    pmediaPath = os.path.join(path_rotina,'Arq_Saida', arqons)

    if os.path.exists(pmediaPath):
        return True
    else:
        return False
    
def verifica_arquivos_entrada(path_rotina,dt_rodada_exec):

    files_exists = True

    for modelo in MODELOS_COMPOSICAO_PCONJUNTO:
        if modelo.lower() == 'eta': modelo = 'ETA40' 

        file_to_verify = f"{modelo.upper()}_m_{dt_rodada_exec.strftime('%d%m%y')}.dat"
        verify_path = os.path.join(path_rotina,'Arq_Entrada', modelo, file_to_verify)
        exists = glob.glob(verify_path)
        if not exists:
            print(f"Faltando o arquivo: {verify_path}")
            files_exists = False
    
    return files_exists

    
def escreve_arquivos_chuva(data,arqin,modelout,pathout,dias,sufixo):
    
    colunas_fixas = ['PSAT', 'Lon', 'Lat']
    colunas_dinamicas = [f'D{i+1}' for i in range(dias)]

    if modelout.lower() in [f'eta40{sufixo}',f'gefs{sufixo}',f'ecmwf-ens{sufixo}']:
        #Arquivos de entrada tem a sequencia lat e depois lon
        colunas_fixas = ['PSAT', 'Lat', 'Lon']
        colunas_dinamicas = [f'D{i+1}' for i in range(dias)]
    
    cols = colunas_fixas + colunas_dinamicas
    
    df =  pd.read_csv(arqin,delimiter=r'\s+',header=None,names=cols)

    subBaciasCalculadas = {}

    subBaciasCalculadas[(-64.66, -09.26)] = [{'Lon':-64.65, 'Lat':-9.25, 'fator':0.13},{'Lon':-69.12, 'Lat':-12.60, 'fator':0.87}]
    subBaciasCalculadas[(-51.77, -03.13)] = [{'Lon':-51.91, 'Lat':-3.41, 'fator':0.699},{'Lon':-52.00, 'Lat':-6.74, 'fator':0.264},{'Lon':-51.77, 'Lat':-6.75, 'fator':0.037}]

    for dd in range(0,dias):
        datapos = data + datetime.timedelta(days=dd+1)
        arq_out = pathout + '/' + modelout + '_p' + data.strftime('%d%m%y') + 'a' + datapos.strftime('%d%m%y') + '.dat'
        df_out = df[['Lon','Lat','D' +str(dd+1)]].copy()

        # Contas feitas dentro dos scripts R para subbacias especiais
        # for coord in subBaciasCalculadas:
        #     subBac_calc = subBaciasCalculadas[coord]

        #     precipCalculado = 0
        #     for sbac in subBac_calc:
        #         precipCalculado += df_out[(df_out['Lon'] == sbac['Lon']) & (df_out['Lat'] == sbac['Lat'])].iloc[0,2]*sbac['fator']

        #     df_out = df_out.append({'Lon':coord[0], 'Lat':coord[1], 'D'+str(dd+1):round(precipCalculado,1)}, ignore_index=True)

        df_out.to_csv(arq_out, index=False, sep = '\t', header = False)


def printHelper():
    hoje = datetime.datetime.now()
    print("python {} data {} forcarRodar True".format(sys.argv[0], hoje.strftime("%d/%m/%Y")))
    quit()


if __name__ == '__main__':

    if 'help' in sys.argv or '--h' in sys.argv or '-help' in sys.argv:
        printHelper()

    parametros = {}
    parametros['data'] = datetime.datetime.now()
    parametros['forcarRodar'] = False

    for i in range(len(sys.argv)):
        argumento = sys.argv[i].lower()

        if argumento == 'data':
            p_dataReferente = sys.argv[i+1]
            parametros['data'] = datetime.datetime.strptime(p_dataReferente, "%d/%m/%Y")

        if argumento == 'forcarrodar':
            parametros['forcarRodar'] = True
            
    previsao_conjunto_ONS(parametros)