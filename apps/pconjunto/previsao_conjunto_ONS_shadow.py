# -*- coding: utf-8 -*-
import os
import re
import sys
import pdb
import glob
import time
import shutil
import datetime
import requests
import subprocess
import pandas as pd
# import sqlalchemy as db

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
MODELOS_DERIVADOS = ['PCONJUNTO','PCONJUNTO2']

def previsao_conjunto_ONS(param):

    data = param['data']
    f_forcarRodar = param['forcarRodar']

    path_dropbox_working_date = os.path.join(PATH_DROPBOX_MIDDLE, "NovoSMAP", data.strftime('%Y%m%d'), "rodada00z")
    path_cv_working_date = os.path.join(PATH_CV, data.strftime('%Y%m%d'))

    print("\nRODANDO PCONJUNTO -",data.strftime('%d/%m/%Y'))

    # cria_diretorio(path_dropbox_working_date)
    # cria_diretorio(path_cv_working_date + '/CONJUNTO_ONS_ORIG')
    # cria_diretorio(path_cv_working_date + '/CONJUNTO_ONS_WX')
    # cria_diretorio(path_cv_working_date + '/ETA40')

    task_conjunto_ons(data, f_forcarRodar)
    
    #precisa ser refatorado pois nao tem mais os netcdf
    # if not run_conjunto_RZ(data, f_forcarRodar):
    #     raise

    print("Rotina Finalizada!")

    return True

# def run_conjunto_RZ(data, forcar_rodar=False):
    
#     models_structure = []
#     dt_anterior = data - datetime.timedelta(days=1)

    
#     if 'GEFS' in MODELOS_COMPOSICAO_PCONJUNTO:
#         path_ncGefs = os.path.join(PATH_PREVISAO_MODELOS_RZ, "gefs",f"{data.strftime('%Y%m%d')}00","data","gefs.acumul12z-12z.pgrb2a.0p50f15.nc")
#         models_structure+= ("GEFS", path_ncGefs),
    
#     if 'ECMWF' in MODELOS_COMPOSICAO_PCONJUNTO:
#         path_ncEcmwf = os.path.join(PATH_PREVISAO_MODELOS_RZ, "ecmwf-ens-orig",f"{dt_anterior.strftime('%Y%m%d')}00","data","A1F*0000_101.nc")
#         models_structure+= ("ECMWF", path_ncEcmwf),
    
#     if 'ETA' in MODELOS_COMPOSICAO_PCONJUNTO:
#         path_ncEta = os.path.join(PATH_PREVISAO_MODELOS_RZ, "ons-eta",f"{data.strftime('%Y%m%d')}00","data","pp*_0252.ctl.nc")
#         models_structure+= ("ETA40", path_ncEta),
    
#     modelos_disponiveis = []
#     for modelo, path_modelo in models_structure:
#         if not glob.glob(path_modelo):
#             print(f'Faltando:\n{path_modelo}')
#         else:
#             modelos_disponiveis += modelo,
    
#     #se tiver pelo menos 2 de 3 modelos do pconjunto, ou 1 de 2 modelos 
#     if len(modelos_disponiveis) >= len(MODELOS_COMPOSICAO_PCONJUNTO)-1 : 

#         if len(modelos_disponiveis) == len(MODELOS_COMPOSICAO_PCONJUNTO):
#             task_conjunto_rz(
#                 dt_rodada_exec=data,
#                 forcar_rodar=forcar_rodar   
#                 )
#         else:
#             task_conjunto_rz_d1(
#                 dt_rodada_exec=data,
#                 modelos_disponiveis=modelos_disponiveis,
#                 forcar_rodar=forcar_rodar
#                 )
#         return True
#     else:
#         print("Não há modelos suficientes para executar a task PCONJUNTO_RZ!")
#         return False
        
# def task_conjunto_rz(dt_rodada_exec,path_rotina=PATH_ROTINA_CONJUNTO_TMP,forcar_rodar=False):

#     exists = verifica_rodada_R_existente(path_rotina, dt_rodada_exec)
#     if not exists or forcar_rodar:
#         print(f'\n### CONJUNTO: PCONJUNTO-RZ')
        
#         crirPconjuntoTmp()

#         pconjuntoTmp_entrada = os.path.join(path_rotina,'Arq_Entrada')
#         if 'GEFS' in MODELOS_COMPOSICAO_PCONJUNTO:
#             dst = os.path.join(pconjuntoTmp_entrada, 'GEFS')
#             conversorArquivos.converterFormatoPconjunto(modelo='gefs',dataRodada=dt_rodada_exec,rodada=0,destino=dst)
#         if 'ETA' in MODELOS_COMPOSICAO_PCONJUNTO:
#             dst = os.path.join(pconjuntoTmp_entrada, 'ETA40')
#             conversorArquivos.converterFormatoPconjunto(modelo='eta',dataRodada=dt_rodada_exec,rodada=0,destino=dst)
#         if 'ECMWF' in MODELOS_COMPOSICAO_PCONJUNTO:
#             dst = os.path.join(pconjuntoTmp_entrada, 'ECMWF')
#             conversorArquivos.converterFormatoPconjunto(modelo='ecmwf',dataRodada=dt_rodada_exec,rodada=0,destino=dst)

#         numdias=14
#         sufixo='-RZ'
#         derivados = ['PCONJUNTO']
        
#         executarScriptR(path_rotina, dt_rodada_exec)
        
#         organizarArquivosSaida(data=dt_rodada_exec, path_rotina=path_rotina, derivados=derivados, sufixo=sufixo, numdias=numdias)
#         #smap novo
#         SmapTools.trigger_dag_SMAP(dt_rodada=dt_rodada_exec,modelos_names=['PCONJUNTO-RZ'],rodada=0)
                
#     else:
#         print("Task Conjunto RZ ja foi executada!")

# def task_conjunto_rz_d1(dt_rodada_exec,modelos_disponiveis,forcar_rodar=False):

#     pconjuntoTmp_entrada = os.path.join(PATH_ROTINA_CONJUNTO_TMP,'Arq_Entrada')
#     dt_anterior = dt_rodada_exec - datetime.timedelta(days=1)
    
#     exists = verifica_rodada_R_existente(PATH_ROTINA_CONJUNTO_TMP, dt_anterior)
#     if not exists or forcar_rodar:
        
#         crirPconjuntoTmp()
#         modelo_faltante = list(set(MODELOS_COMPOSICAO_PCONJUNTO) - set(modelos_disponiveis))[0]
#         print(f'\n### CONJUNTO: PCONJUNTO-RZ-{modelo_faltante}D1')

#         for modelo in modelos_disponiveis:
#             print(f"Convertendo {modelo}")
#             dst = os.path.join(pconjuntoTmp_entrada, modelo)
#             conversorArquivos.converterFormatoPconjunto(modelo=modelo.lower().replace("eta40","eta"),dataRodada=dt_rodada_exec,rodada=0,destino=dst)
#             pathArqPrevisto = os.path.join(dst,f'{modelo}_m_{dt_rodada_exec.strftime("%d%m%y")}.dat')
#             convertArqConjuD1(pathFile=pathArqPrevisto)
        
#         dst = os.path.join(pconjuntoTmp_entrada, modelo_faltante)
#         conversorArquivos.converterFormatoPconjunto(modelo=modelo_faltante.lower().replace("eta40","eta"),dataRodada=dt_anterior,rodada=0,destino=dst)

#         executarScriptR(PATH_ROTINA_CONJUNTO_TMP, dt_anterior)

#         numdias=13
#         sufixo=f'-RZ-{modelo_faltante}D1'
#         derivados = ['PCONJUNTO']
        
#         organizarArquivosSaida(data=dt_rodada_exec, path_rotina=PATH_ROTINA_CONJUNTO_TMP, derivados=derivados, sufixo=sufixo, numdias=numdias)
#         #smap novo
#         SmapTools.trigger_dag_SMAP(dt_rodada=dt_rodada_exec,modelos_names=[f"PCONJUNTO-RZ-{modelo_faltante}D1"],rodada=0)
#     else:
#         print("Task Conjunto RZ-D1 ja foi executada!")

def task_conjunto_ons(dt_rodada_exec, forcar_rodar=False):

    if forcar_rodar: exists = False
    else:
        exists = verifica_rodada_R_existente(PATH_ROTINA_CONJUNTO, dt_rodada_exec)
    
    files_exists = verifica_arquivos_entrada(PATH_ROTINA_CONJUNTO, dt_rodada_exec)
    
    if (not exists) :

        if files_exists:
            print(f'\n### CONJUNTO: PCONJUNTO-ONS')
            executarScriptR(PATH_ROTINA_CONJUNTO, dt_rodada_exec)
            derivados = ['PMEDIA', 'PCONJUNTO', 'PCONJUNTO2', 'PCONJUNTO-EXT'] + MODELOS_COMPOSICAO_PCONJUNTO
            sufixo=''
            numdias=14
            modelos_inserir_fila = organizarArquivosSaida(data=dt_rodada_exec, path_rotina=PATH_ROTINA_CONJUNTO, derivados=derivados, sufixo=sufixo, numdias=numdias)
            
            #smap novo
            SmapTools.trigger_dag_SMAP(dt_rodada=dt_rodada_exec)
                
            # path_fig = os.path.abspath('/WX2TB/Documentos/saidas-modelos/gefs-eta')
            # wx_plota_pconjunto.plota_saida_pconjunto(dt_rodada_exec,PATH_ROTINA_CONJUNTO,path_fig)

            cmd = "cd /WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/tempo"
            cmd + f"python tempo.py plotarMapasPrevisao data {dt_rodada_exec.strftime('%d/%m/%Y')} modelo 'pconjunto' rodada 0"
            os.system(cmd)
        else:
            print("Arquivos Faltantes!")
    else:
        print("Task Conjunto ONS ja foi executada!")

def organizarArquivosSaida(data, path_rotina, derivados, sufixo, numdias=14):
    modelos_inserir_fila = []
    print("AJUSTANDO .DAT PARA FORMATO SMAP ONS")

    # path_cv_working_date = os.path.join(PATH_CV, data.strftime('%Y%m%d'))
    # path_dropbox_working_date = os.path.join(PATH_DROPBOX_MIDDLE, "NovoSMAP", data.strftime('%Y%m%d'), "rodada00z")
    path_conjunto_saida = os.path.join(path_rotina,'Arq_Saida')
    path_conjunto_entrada = os.path.join(path_rotina,'Arq_Entrada')
    
    # dirPrevisoesModelosRaizenDiario = os.path.join(PATH_PREVISAO_MODELOS_RZ, 'gefs-eta', data.strftime('%Y%m%d') + '00', 'data')
    # if not os.path.exists(dirPrevisoesModelosRaizenDiario):
    #     os.makedirs(dirPrevisoesModelosRaizenDiario)

    def read_configs_file()-> pd.DataFrame:
        path_configs =os.path.join(path_conjunto_entrada,"configuracao.xlsx")
        df_configs = pd.read_excel(path_configs)
        return df_configs

    def get_subbacias() -> pd.DataFrame:
        res = requests.get("https://tradingenergiarz.com/api/v2/rodadas/subbacias", verify=False)
        df_response = pd.DataFrame(res.json()).rename(columns={"vl_lat":"Latitude","vl_lon":"Longitude",'id':"cd_subbacia","nome":"Codigo ANA"})
        df_configs = read_configs_file()
        df_subbacias_completo = pd.merge(df_configs[['Latitude','Longitude','Codigo ANA']],df_response[['Codigo ANA','cd_subbacia']])
        return df_subbacias_completo

    def extract_info_name(path_file):

        file_name =  os.path.basename(path_file)
        match = re.match(r'(.+?)_p(\d{6})a(\d{6})\.dat', file_name)
        modelo, dt_rodada,dt_prevista = None,None,None
        if match:
            modelo = match.group(1)
            dt_rodada = datetime.datetime.strptime(match.group(2),"%d%m%y").isoformat()
            dt_prevista = datetime.datetime.strptime(match.group(3),"%d%m%y").strftime("%Y-%m-%d")
        
        return modelo, dt_rodada,dt_prevista

    def gerar_arq_modelos_saida():

        path_ecmwf = os.path.join(path_conjunto_entrada,'ECMWF',f'ECMWF_m_{data.strftime("%d%m%y")}.dat')
        path_gefs = os.path.join(path_conjunto_entrada,'GEFS',f'GEFS_m_{data.strftime("%d%m%y")}.dat')
        path_eta = os.path.join(path_conjunto_entrada,'ETA40',f'ETA40_m_{data.strftime("%d%m%y")}.dat')
        
        path_ecmwf_vies = os.path.join(path_conjunto_saida, 'ECMWF_rem_vies.dat')
        path_gefs_vies = os.path.join(path_conjunto_saida,'GEFS_rem_vies.dat')
        path_eta_vies = os.path.join(path_conjunto_saida, 'ETA40_rem_vies.dat')

        config_modelos = [
            
            #essemble 
            {"modelo":"ECMWF", "ndias": 14, "path":path_ecmwf },

            #essemble
            {"modelo":"GEFS", "ndias": 14, "path":path_gefs },
            
            {"modelo":"ETA40", "ndias": 9, "path":path_eta },

            {"modelo":"ECMWFremvies", "ndias": 14, "path":path_ecmwf_vies },
            {"modelo":"GEFSremvies", "ndias": 14, "path":path_gefs_vies },
            {"modelo":"ETA40remvies", "ndias": 9, "path":path_eta_vies },
        ]

        for config in config_modelos:
            print(f"Escrevendo modelo: {config['modelo']}")

            escreve_arquivos_chuva(
                data,
                config['path'],
                config['modelo'],
                path_conjunto_saida,
                config['ndias']
                )
    
    def get_modelo_chuva_by_id(id_chuva:int,dt_ini:str,dt_fim):

        response_rodadas = requests.get('https://tradingenergiarz.com/api/v2/rodadas/chuva/previsao/subbacia',
                    params = {
                        "id":id_chuva,
                        "dt_inicio": dt_ini,
                        "dt_fim": dt_fim,
                        "no_cache": "false",
                        "atualizar": "false"
                    },
                    verify=False
                )
        return response_rodadas.json()

    def get_rodadas_do_dia(dt_rodada):
        response_rodadas = requests.get('https://tradingenergiarz.com/api/v2/rodadas',
            params = {
                "dt": dt_rodada,
                "no_cache": "true",
                "atualizar": "false"
            },
            verify=False
        )
        return response_rodadas.json()
    

    def gerar_modelos_derivados(df_previsao_modelos:pd.DataFrame):

        df_previsao_modelos['dt_rodada'] = pd.to_datetime(df_previsao_modelos['dt_rodada'])
        df_previsao_modelos['dt_prevista'] = pd.to_datetime(df_previsao_modelos['dt_prevista'])
        dt_rodada = df_previsao_modelos['dt_rodada'].dt.strftime('%Y-%m-%d').unique()[0]

        response_rodadas = get_rodadas_do_dia(dt_rodada)
        df_rodadas_do_dia = pd.DataFrame(response_rodadas)


        for modelo in MODELOS_DERIVADOS:

            dias_ate_quinta = (3-df_previsao_modelos['dt_rodada'].dt.dayofweek +7) % 7
            quinta_feira = df_previsao_modelos['dt_rodada'] + datetime.timedelta(days=int(dias_ate_quinta.unique()[0]))
            mask = (df_previsao_modelos['dt_prevista'] <= quinta_feira)

            df_pmedia = df_previsao_modelos[df_previsao_modelos['modelo'] == 'PMEDIA'].copy()
            if modelo =='PCONJUNTO':
                #completar até quinta feira com o gefs-ens , se for sexta ou sabado completar até a quinta da semana que vem
                df_pconjunto = df_pmedia.copy()
                df_gefs = df_previsao_modelos[df_previsao_modelos['modelo'] == 'GEFS'].copy()
                df_pconjunto.loc[mask, df_gefs.columns] = df_gefs.loc[mask,df_gefs.columns].values
                df_pconjunto.loc[:,'modelo'] = 'PCONJUNTO'
                df_previsao_modelos = pd.concat([df_previsao_modelos, df_pconjunto], ignore_index=True)

            if modelo =='PCONJUNTO2':
                #completar até quinta feira com o ecmwf-ens , se for sexta ou sabado completar até a quinta da semana que vem
                df_pconjunto2 = df_pmedia.copy()
                df_ecmwf = df_previsao_modelos[df_previsao_modelos['modelo'] == 'ECMWF'].copy()
                df_pconjunto2.loc[mask, df_ecmwf.columns] = df_ecmwf.loc[mask,df_ecmwf.columns].values
                df_pconjunto2.loc[:,'modelo'] = 'PCONJUNTO2'
                df_previsao_modelos = pd.concat([df_previsao_modelos, df_pconjunto2], ignore_index=True)

                # try:
                #     df_rodada_ecmwf_ens = df_rodadas_do_dia[df_rodadas_do_dia['str_modelo'].str.lower() == 'ecmwf-ens']
                #     id_chuva = df_rodada_ecmwf_ens['id_chuva'].values[0]

                #     response_prev_chuva = get_modelo_chuva_by_id(
                #         id_chuva=id_chuva,
                #         dt_ini=df_previsao_modelos['dt_rodada'].dt.strftime('%Y-%m-%d').unique()[0],
                #         dt_fim=quinta_feira.dt.strftime('%Y-%m-%d').unique()[0]
                #         )

                #     df_pconjunto2 = df_previsao_modelos[df_previsao_modelos['modelo'] == 'PMEDIA'].copy()
                    
                #     if response_prev_chuva:

                #         df_ecmwf_ens = pd.DataFrame(response_prev_chuva).rename(columns={'id':"cd_subbacia"})
                #         df_ecmwf_ens = pd.merge(df_ecmwf_ens, df_pconjunto2[['Longitude', 'Latitude', 'Codigo ANA', 'cd_subbacia']].drop_duplicates(), on='cd_subbacia', how='left')
                        
                #         df_ecmwf_ens['dt_rodada'] = pd.to_datetime(df_ecmwf_ens['dt_rodada'])
                #         df_ecmwf_ens['dt_prevista'] = pd.to_datetime(df_ecmwf_ens['dt_prevista'])

                #         quinta_feira = df_ecmwf_ens['dt_rodada'] + datetime.timedelta(days=int(dias_ate_quinta.unique()[0]))
                #         mask_ec = (df_ecmwf_ens['dt_prevista'] <= quinta_feira)

                #         df_pconjunto2.loc[mask, df_pconjunto2.columns] = df_ecmwf_ens.loc[mask_ec,df_pconjunto2.columns].values

                #     df_pconjunto2.loc[:,'modelo'] = 'PCONJUNTO2'
                #     df_previsao_modelos = pd.concat([df_previsao_modelos, df_pconjunto2], ignore_index=True)
                # except:
                #     print("Não foi possivel gerar o PCONJUNTO2")

        df_previsao_modelos['dt_rodada'] = pd.to_datetime(df_previsao_modelos['dt_rodada']).apply(lambda x: x.isoformat())
        df_previsao_modelos['dt_prevista'] = pd.to_datetime(df_previsao_modelos['dt_prevista']).dt.strftime("%Y-%m-%d")

        return df_previsao_modelos

    gerar_arq_modelos_saida()

    #leitura Arquivos de saida
    df_concatenated = pd.DataFrame()
    file_paths = os.path.join(path_conjunto_saida,f"*_p{data.strftime('%d%m%y')}*.dat")
    forecast_files = glob.glob(file_paths)

    for file_path in forecast_files:
        modelo, dt_rodada,dt_prevista = extract_info_name(file_path)
        df_temp = pd.read_csv(file_path, delimiter='\s+', header=None, names=['Longitude','Latitude','vl_chuva'])
        df_temp['modelo'] = modelo
        df_temp['dt_rodada'] = dt_rodada
        df_temp['dt_prevista'] = dt_prevista

        df_concatenated = pd.concat([df_concatenated, df_temp], ignore_index=True)

    df_sub_bacias_db = get_subbacias()
    df_previsao_modelos = pd.merge(df_concatenated,df_sub_bacias_db, on=['Longitude','Latitude'])
    df_previsao_modelos = df_previsao_modelos.dropna()

    df_previsao_modelos = gerar_modelos_derivados(df_previsao_modelos)

    for modelo in df_previsao_modelos['modelo'].unique():
        previsao_modelos = df_previsao_modelos[df_previsao_modelos['modelo']==modelo][['cd_subbacia','dt_prevista','vl_chuva','modelo','dt_rodada']]
        previsao_modelos = previsao_modelos.dropna()
        response = requests.post('https://tradingenergiarz.com/api/v2/rodadas/chuva/previsao/modelos', verify=False, json=previsao_modelos.to_dict('records'))
        print(f'{modelo} - > Código POST: {response.status_code}')


        

    # flag_primeirraSemana = 1
    # for nDia in range(1, numdias+1):
    #     data_arquivo = data + datetime.timedelta(days=nDia)

    #     if data_arquivo.weekday() in [4,5] and flag_primeirraSemana == 1:
    #         flag_primeirraSemana = 0

    #     if 'D1' in sufixo:
    #         pathArquivoPmedia = os.path.join(path_conjunto_saida, 'PMEDIA_p{}a{}.dat'.format((data-datetime.timedelta(days=1)).strftime('%d%m%y'), data_arquivo.strftime('%d%m%y')))
        
    #     else:
    #         pathArquivoPmedia = os.path.join(path_conjunto_saida, 'PMEDIA_p{}a{}.dat'.format(data.strftime('%d%m%y'), data_arquivo.strftime('%d%m%y')))

    #     if 'PMEDIA' in derivados:

    #         dst_dropbox = os.path.join(path_dropbox_working_date, 'PMEDIA{}_p{}a{}.dat'.format(sufixo, data.strftime('%d%m%y'), data_arquivo.strftime('%d%m%y')))
    #         shutil.copyfile(pathArquivoPmedia, dst_dropbox)

    #     if 'PCONJUNTO' in derivados:

    #         pathArquivoPconjunto = os.path.join(path_dropbox_working_date, 'PCONJUNTO{}_p{}a{}.dat'.format(sufixo, data.strftime('%d%m%y'), data_arquivo.strftime('%d%m%y')))
    #         print(pathArquivoPconjunto)
    #         shutil.copyfile(pathArquivoPmedia,pathArquivoPconjunto)
            
    #         # Completa a primeira semana (ate quinta) com o GEFS para o PCONJUNTO
    #         if data_arquivo.weekday() in [0,1,2,3,6] and flag_primeirraSemana == 1:
    #             src = os.path.join(path_cv_working_date, "GEFS", "GEFS_p" + data.strftime('%d%m%y') + "a" + data_arquivo.strftime('%d%m%y') + ".dat")                
    #             dst = pathArquivoPconjunto
    #             try:
    #                 shutil.copyfile(src,dst)
    #             except Exception as e:
    #                 print("Nao foi possivel copiar arquivo GEFS")
    #                 print(e)

    #     if 'PCONJUNTO2' in derivados:

    #         pathArquivoPconjunto2 = os.path.join(path_dropbox_working_date, 'PCONJUNTO2{}_p{}a{}.dat'.format(sufixo, data.strftime('%d%m%y'), data_arquivo.strftime('%d%m%y')))
    #         shutil.copyfile(pathArquivoPmedia,pathArquivoPconjunto2)

    #         # Completa a primeira semana (ate quinta) com o EC-ens  para o PCONJUNTO2
    #         if data_arquivo.weekday() in [0,1,2,3,6] and flag_primeirraSemana == 1:
    #             src = os.path.join(path_cv_working_date, "ECMWF-ens", "EC-ens101_p" + data.strftime('%d%m%y') + "a" + data_arquivo.strftime('%d%m%y') + ".dat")                
    #             dst = pathArquivoPconjunto2
    #             try:
    #                 shutil.copyfile(src,dst)
    #             except Exception as e:
    #                 print("Nao foi possivel copiar arquivo EC")
    #                 print(e)

    # if 'PCONJUNTO' in derivados:
    #     try:
    #         arquivos_copia = os.path.join(path_dropbox_working_date, 'PCONJUNTO{}_p{}a*'.format(sufixo,data.strftime('%d%m%y')))            
    #         copia_arquivos(arquivos_copia,os.path.join(path_cv_working_date, 'CONJUNTO_ONS_WX'))
    #         rodadas.importar_chuva(data=data,rodada=0,modelo=f"PCONJUNTO{sufixo}",flag_estudo=False)
    #         modelos_inserir_fila += f'PCONJUNTO{sufixo}',
    #     except Exception as e:
    #         print("Não foi possivel completar o PCONJUNTO")
    #         print(e)

    # if 'PCONJUNTO2' in derivados:
    #     try:
    #         arquivos_copia = os.path.join(path_dropbox_working_date, 'PCONJUNTO2{}_p{}a*'.format(sufixo,data.strftime('%d%m%y')))            
    #         copia_arquivos(arquivos_copia,os.path.join(path_cv_working_date, 'CONJUNTO_ONS_WX'))
    #         rodadas.importar_chuva(data=data,rodada=0,modelo="PCONJUNTO2",flag_estudo=False)
    #         modelos_inserir_fila += 'PCONJUNTO2',
    #     except Exception as e:
    #         print("Não foi possivel completar o PCONJUNTO2")
    #         print(e)

    # if 'PMEDIA' in derivados:
    #     try:
    #         arquivos_copia = os.path.join(path_dropbox_working_date, 'PMEDIA{}_p{}a*'.format(sufixo,data.strftime('%d%m%y')))
    #         copia_arquivos(arquivos_copia,os.path.join(path_cv_working_date,'CONJUNTO_ONS_ORIG'))
    #         rodadas.importar_chuva(data=data,rodada=0,modelo="PMEDIA",flag_estudo=False)
    #         modelos_inserir_fila += 'PMEDIA',
    #     except Exception as e:
    #         print("Não foi possivel completar o PMEDIA")
    #         print(e)

    


    # if 'ECMWF' in derivados:
    #     try:
    #         arquivo = os.path.join(path_conjunto_saida, 'ECMWF_rem_vies.dat')
    #         copia_arquivos(arquivo,dirPrevisoesModelosRaizenDiario)
    #         model = 'EC-ensremvies'; dias = 14
    #         escreve_arquivos_chuva(data,arquivo,model,path_conjunto_saida,dias)  

    #         arquivos_copia = os.path.join(path_conjunto_saida, model + '_p' + data.strftime('%d%m%y') + 'a*')
    #         copia_arquivos(arquivos_copia,path_cv_working_date + '/CONJUNTO_ONS_ORIG')   
    #         copia_arquivos(arquivos_copia,path_dropbox_working_date) 
    #         rodadas.importar_chuva(data=data,rodada=0,modelo="EC-ensremvies",flag_estudo=False)
    #         modelos_inserir_fila += 'EC-ensremvies',
    #     except Exception as e:
    #         print("Não foi possivel completar o EC-ensremvies")
    #         print(e)

    # if 'ETA' in derivados:
    #     try:
    #         arquivo = os.path.join(path_conjunto_saida, 'ETA40_rem_vies.dat')
    #         copia_arquivos(arquivo,dirPrevisoesModelosRaizenDiario)
    #         model = 'ETA40remvies'; dias = 9
    #         escreve_arquivos_chuva(data,arquivo,model,path_conjunto_saida,dias)
    #         arquivos_copia = os.path.join(path_conjunto_saida, model + '_p' + data.strftime('%d%m%y') + 'a*')
    #         copia_arquivos(arquivos_copia,path_cv_working_date + '/CONJUNTO_ONS_ORIG')   
    #         copia_arquivos(arquivos_copia,path_dropbox_working_date)
    #         rodadas.importar_chuva(data=data,rodada=0,modelo="ETA40remvies",flag_estudo=False)
    #         modelos_inserir_fila += 'ETA40remvies',
    #         rodadas.importar_chuva(data=data,rodada=0,modelo="ETA40",flag_estudo=False)
    #         modelos_inserir_fila += 'ETA40',
    #     except Exception as e:
    #         print("Não foi possivel completar o ETA40remvies")
    #         print(e)

    # if 'GEFS' in derivados:
    #     try:
    #         arquivo = os.path.join(path_conjunto_saida, 'GEFS_rem_vies.dat')
    #         copia_arquivos(arquivo,dirPrevisoesModelosRaizenDiario)

    #         model = 'GEFSremvies'; dias = 14
    #         escreve_arquivos_chuva(data,arquivo,model,path_conjunto_saida,dias)
    #         arquivos_copia = os.path.join(path_conjunto_saida, model + '_p' + data.strftime('%d%m%y') + 'a*')
    #         copia_arquivos(arquivos_copia,path_cv_working_date + '/CONJUNTO_ONS_ORIG')   
    #         copia_arquivos(arquivos_copia,path_dropbox_working_date)
    #         rodadas.importar_chuva(data=data,rodada=0,modelo="GEFSremvies",flag_estudo=False)
    #         modelos_inserir_fila += 'GEFSremvies',
    #     except Exception as e:
    #         print("Não foi possivel completar o GEFSremvies")
    #         print(e)

    # if 'PCONJUNTO-EXT' in derivados:
    #     try:
    #         dt =  data + datetime.timedelta(days=1)
    #         dtFinalPconjunto = data + datetime.timedelta(days=nDia)

    #         while dt <= dtFinalPconjunto:
    #             nomeArq = 'PCONJUNTO_p{}a{}.dat'.format(data.strftime('%d%m%y'),dt.strftime('%d%m%y'))
    #             src = os.path.join(path_dropbox_working_date, nomeArq)
    #             novoNomeArq = nomeArq.replace('PCONJUNTO_','PCONJUNTO-EXT_')
    #             dst = os.path.join(path_dropbox_working_date, novoNomeArq)
    #             shutil.copyfile(src,dst)
    #             print(dst)

    #             dt = dt + datetime.timedelta(days=1)

    #         dtDiaAnterior = data - datetime.timedelta(days=1)
    #         cvGefsAnterior = os.path.join(PATH_CV, dtDiaAnterior.strftime('%Y%m%d'), 'GEFS')

    #         # Completa a terceira semana do PCONJUNTO com a previsao do GEFS-estendido do dia anterior (gefs estendido demora muito)
    #         while dt.weekday() != 5:
    #             nomeArq = 'GEFS-ext_p{}a{}.dat'.format(dtDiaAnterior.strftime('%d%m%y'),dt.strftime('%d%m%y'))
    #             src = os.path.join(cvGefsAnterior,nomeArq)
    #             novoNomeArq = 'PCONJUNTO-EXT_p{}a{}.dat'.format(data.strftime('%d%m%y'),dt.strftime('%d%m%y'))
    #             dst = os.path.join(path_dropbox_working_date,novoNomeArq)
    #             shutil.copyfile(src,dst)
    #             print(dst)

    #             dt = dt + datetime.timedelta(days=1)

    #         rodadas.importar_chuva(data=data,rodada=0,modelo="PCONJUNTO-EXT",flag_estudo=False)
    #         modelos_inserir_fila += 'PCONJUNTO-EXT',
    #     except Exception as e:
    #         print("Não foi possivel completar o PCONJUNTO-EXT")
    #         print(e)

    return modelos_inserir_fila
    


#===================UTILS=========================

def cria_diretorio(path):
    if not os.path.exists(path):
        os.makedirs(path)

def copia_arquivos(arquivos_copia,path_dst):


    cria_diretorio(path_dst)

    for src in (glob.glob(arquivos_copia)):
        try:
            dst = os.path.join(path_dst, os.path.basename(src))
            shutil.copyfile(src, dst)
        except Exception as e:
            print("Nao foi possivel copiar arquivo " + src)
            print(e)

# def get_psat(dt_ini_obs):
#     db_rodadas = wx_dbClass.db_mysql_master('db_rodadas')
#     db_rodadas.connect()

#     tb_subbacia = db_rodadas.getSchema('tb_subbacia')
#     tb_chuva_psat = db_rodadas.getSchema('tb_chuva_psat')

#     select_psat = db.select(
#         tb_subbacia.c.txt_nome_subbacia,
#         tb_subbacia.c.vl_lat,
#         tb_subbacia.c.vl_lon,
#         tb_chuva_psat.c.vl_chuva
#         )\
#         .join(
#             tb_subbacia, 
#             tb_subbacia.c.cd_subbacia==tb_chuva_psat.c.cd_subbacia
#         )\
#         .where(
#             tb_chuva_psat.c.dt_ini_observado == dt_ini_obs.strftime("%Y-%m-%d")
#             )
#     psat_values = db_rodadas.conn.execute(select_psat).fetchall()
#     return psat_values


# def get_gpm(dt_ini_obs):
#     db_rodadas = wx_dbClass.db_mysql_master('db_rodadas')
#     db_rodadas.connect()

#     tb_subbacia = db_rodadas.getSchema('tb_subbacia')
#     tb_chuva_obs = db_rodadas.getSchema('tb_chuva_obs')

#     select_gpm = db.select(tb_subbacia.c.txt_nome_subbacia,tb_subbacia.c.vl_lat,tb_subbacia.c.vl_lon,tb_chuva_obs.c.vl_chuva)\
#                     .join(tb_subbacia, tb_subbacia.c.cd_subbacia==tb_chuva_obs.c.cd_subbacia)\
#                     .where(tb_chuva_obs.c.dt_observado == dt_ini_obs.strftime("%Y-%m-%d"))
#     gpm_values = db_rodadas.conn.execute(select_gpm).fetchall()

#     return gpm_values


# def get_df_chuva_obserervada(data):

    #a chuva do dia alvo inicia no dia anterior 
    # por isso no banco a chuva da data alvo esta cadastrada como a data de inicio
    dt_ini_obs = data - datetime.timedelta(days=1)
    observado_values = get_psat(dt_ini_obs=dt_ini_obs)
    if not observado_values:
        print(f'PSAT {data.strftime("%Y-%m-%d")} não disponivel no banco!')
        print(f'GPM sera utilizado no lugar!')
        observado_values = get_gpm(dt_ini_obs=dt_ini_obs)
        if not observado_values:
            print(f'GPM {data.strftime("%Y-%m-%d")} não disponivel!')

    df_observado = pd.DataFrame(observado_values,columns=["subbacia","lat","lon","vl_chuva"])
    return df_observado.set_index("subbacia")

# def cria_arq_psat(data, destino):

#     arq_psat = "psat_{}.txt".format(data.strftime('%d%m%Y'))
#     path_out = os.path.join(destino, arq_psat)
    
#     df_observado = get_df_chuva_obserervada(data)
#     df_observado.to_csv(path_out, sep=' ', index=None,header = False)
#     print(path_out)

#     return path_out

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


def convertArqConjuD1(pathFile):

    nomeFile = os.path.basename(pathFile)
    match = re.match('([A-Za-z0-9]+)_m_([0-9]+).dat', nomeFile)
    modelo = match[1]
    dataStr = match[2]
    dt = datetime.datetime.strptime(dataStr,'%d%m%y')
    dtDiaAnterior = dt - datetime.timedelta(days=1)
    #quermos a chuva alvo do dia anterior
    df_observado = get_df_chuva_obserervada(dtDiaAnterior)
    modelo_to_d1 = pd.read_csv(pathFile,header=None,sep=' ',skipinitialspace=True,index_col=0)

    #inserindo chuva obs ['lat','lon','obs',....] na posicao 2 do dataframe
    modelo_to_d1.insert(loc=2, column='observado', value=df_observado['vl_chuva'])

    # colunas = [1,2,'observado',3,4,5,6,7,8,9,10,11,12,13,14,15]
    # modelo_to_d1 = modelo_to_d1[colunas]

    #retirando o ultimo dia pois é d-1
    modelo_to_d1 = modelo_to_d1.iloc[:,:-1]

    pathArqSaida = pathFile.replace(f'{modelo}_m_{dataStr}.dat',f'{modelo}_m_{dtDiaAnterior.strftime("%d%m%y")}.dat')
    modelo_to_d1.to_csv(pathArqSaida, sep=' ', index=True,header = False)
    print(pathArqSaida)

    return pathArqSaida


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
        raise

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

    
def escreve_arquivos_chuva(data,arqin,modelout,pathout,dias):

    
    colunas_fixas = ['PSAT', 'Lon', 'Lat']
    colunas_dinamicas = [f'D{i+1}' for i in range(dias)]

    if modelout.upper() in ['ETA40','GEFS','ECMWF']:
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