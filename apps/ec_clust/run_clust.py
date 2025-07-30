import os
import sys
import time
import pandas as pd
import datetime
import subprocess

sys.path.insert(1,"/WX2TB/Documentos/fontes")
from PMO.scripts_unificados.bibliotecas import rz_dir_tools

DIR_TOOLS = rz_dir_tools.DirTools()


segunda = 0
quinta = 3

def copy_reforcast_files(
        dt_forecast:datetime.datetime,
        src_dir:str="/home/admin/s3-drive/saidas/data",
        dst_dir:str='./Arq_Entrada/ECMWF/Reforecast'
        ):
    
    #======considerando a semana começando no domingo=====

    dt_week_day = dt_forecast.weekday()

    #domingo
    if (dt_week_day < segunda):
        #pega o hindi dq quinta semana passada
        dt_hindicast = dt_forecast - datetime.timedelta(days=4)

    #segunda
    elif (dt_week_day == segunda):
        dt_hindicast = dt_forecast

    #terça, quarta
    elif (dt_week_day > segunda) and (dt_week_day < quinta):
        #pega o hindi de segunda semana atual
        dt_hindicast = dt_forecast - datetime.timedelta(days=dt_week_day)
    #quinta
    elif (dt_week_day == quinta):
        dt_hindicast = dt_forecast
    
    #sexta, sabado
    elif (dt_week_day > quinta):
        #pega o hindi de quinta semana atual
        dt_hindicast = dt_forecast - datetime.timedelta(days=(dt_week_day - quinta ))

    print(f"Será utilizando a data de hindicast {dt_hindicast.strftime( '%Y-%m-%d')}")
    # #mudando ano inicial para ano final
    dt_real_hindicast = (dt_hindicast.replace(year=(dt_hindicast.year - 20))).strftime("%d%m%y")
    dt_utilizada_hindicast = dt_forecast.strftime("%d%m%y")

    src = os.path.join(src_dir,"**",f"ECMWFh_m_{dt_real_hindicast}*.dat")
    print(src)
    print("Arquivos Hindicast:")

    print(DIR_TOOLS.copy_src(src,dst=dst_dir,replace_text=[dt_real_hindicast,dt_utilizada_hindicast]))

    return dt_hindicast

def copy_prev_files(
        dt_forecast:datetime.datetime,
        src_dir:str="/home/admin/s3-drive/saidas/data",
        dst_dir:str='./Arq_Entrada/ECMWF/Prev'
        ):
    
    dt_forecast_str= dt_forecast.strftime("%d%m%y")
    src = os.path.join(src_dir,"**",f"ECMWFf_m_{dt_forecast_str}*.dat")

    exists = DIR_TOOLS.copy_src(src,dst=dst_dir)
    print("Arquivos Forecast:")
    print(exists)

def set_namelist_file_date(dt_hindicast:datetime.datetime, src_file:str="./namelist_ec.txt"):
    teste = pd.read_csv(src_file)
    teste.loc[1] = f"ic.date.char={dt_hindicast.strftime('%Y%m%d')}"
    teste.to_csv(src_file, index=None)

def set_ecmwf_clust_file_date(dt_forecast:datetime.datetime, src_file:str="./EC_Clus.txt"):
    teste = pd.read_csv(src_file)
    teste.loc[len(teste) -1] = f"{dt_forecast.strftime('%d/%m/%Y')}  !DATA"
    teste.to_csv(src_file, index=None)


def build(dt_forecast:datetime.datetime):

    print(f"Iniciando rotina para data do forecast: {dt_forecast.strftime( '%Y-%m-%d')}")

    copy_prev_files(dt_forecast = dt_forecast)
    dt_hindicast = copy_reforcast_files(dt_forecast=dt_forecast)

    #set namelist variables
    set_namelist_file_date(dt_hindicast=dt_hindicast)
    set_ecmwf_clust_file_date(dt_forecast=dt_forecast)


def run_cluster_script(dt_forecast:datetime.datetime, src_file = "./main.R"):

    # build(dt_forecast=dt_forecast)

    print(f"Iniciado Clusterização! data de forecast {dt_forecast.strftime('%d/%m/%Y')}")
    src_file = os.path.join(os.path.dirname(os.path.abspath(__file__)),"main.R" )
    comando = ["Rscript", src_file]

    import pdb
    pdb.set_trace()
    
    start_time_total = time.time()
    process = subprocess.run(comando, capture_output=True, text=True)
    print(process.stdout)
    print('\nClusterização finalizada em {:.2f} segundos!'.format(time.time() - start_time_total))
    
    if process.returncode != 0:
        print("Erro no script R:")
        print(process.stderr)


if __name__ == '__main__':

    dt_forecast = datetime.datetime(2024,7,15)
    run_cluster_script(dt_forecast=dt_forecast)








