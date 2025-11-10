import os
import sys
import pdb
import glob
import numpy as np
import pandas as pd
import xarray as xr 
import sqlalchemy as db
from datetime import datetime, timedelta
import requests
sys.path.insert("/WX2TB/Documentos/fontes/PMO/scripts_unificados/")
from bibliotecas import wx_dbClass
from apps.web_modelos.server.libs import rz_chuva

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")),'.env'))

dirArqCoordenadasPsat = os.path.abspath('/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/tempo/arquivos/auxiliares/CV/')
dirPrevisoesModelosRaizen = os.path.abspath('/WX4TB/Documentos/saidas-modelos/')
dirpreciptacaoObservada = os.path.abspath('/WX2TB/Documentos/dados/gpm-1h')
dirPreciptacaoImerge = os.path.abspath('/WX2TB/Documentos/dados/imerg_diario')
dirPreciptacaoMerge = os.path.abspath('/WX2TB/Documentos/dados/merge')


def get_padraoNome_netcdf(modelo,dt_rodada,rodada,membro=''):
    
        
    dt_rodada = datetime.strptime(dt_rodada,'%d/%m/%Y')
    dt_rodada_formated = dt_rodada.strftime('%Y%m%d')

    if modelo == 'gefs':
        padraoNome = os.path.join(dirPrevisoesModelosRaizen, 'gefs', '{}{:0>2}'.format(dt_rodada_formated, rodada), 'data', '{}.acumul12z-12z.pgrb2a.0p50f*.nc'.format(modelo))
    elif modelo == 'gefs-ext':
        padraoNome = os.path.join(dirPrevisoesModelosRaizen, 'gefs', '{}{:0>2}'.format(dt_rodada_formated, rodada), 'data', 'gefs.acumul12z-12z.pgrb2af_estendido.nc')
    elif modelo == 'gefs_membro':
        padraoNome = os.path.join(dirPrevisoesModelosRaizen, 'gefs', '{}{:0>2}'.format(dt_rodada_formated, rodada), 'data','gefs_membro{:0>2}.acumul12z-12z.pgrb2af.nc'.format(membro))
    elif modelo == 'gefs_membro-ext':
        padraoNome = os.path.join(dirPrevisoesModelosRaizen, 'gefs', '{}{:0>2}'.format(dt_rodada_formated, rodada), 'data','gefs_membro{:0>2}.acumul12z-12z.pgrb2af_estendido.nc'.format(membro))
    elif modelo == 'gfs':
        padraoNome = os.path.join(dirPrevisoesModelosRaizen, 'gfs', '{}{:0>2}'.format(dt_rodada_formated, rodada), 'gfs-regrid.acumul12z-12z.pgrb2f*_regrided.nc')	
    elif modelo == 'ecmwf':
        padraoNome = os.path.join(dirPrevisoesModelosRaizen, 'ecmwf', '{}{:0>2}'.format(dt_rodada_formated, rodada), 'data','ecm-br_{}{:0>2}_12z-12z.nc'.format(dt_rodada_formated, rodada))	
    elif modelo == 'ecmwf-ens': # ec usado no pconjunto
        padraoNome = os.path.join(dirPrevisoesModelosRaizen, 'ecmwf-ens', '{}{:0>2}'.format(dt_rodada_formated, rodada), 'data','ec-ens-br_{}{:0>2}_ens_AVG-12z-12z.nc'.format(dt_rodada_formated, rodada))	
    elif modelo == 'ecmwf-ens_membro':
        padraoNome = os.path.join(dirPrevisoesModelosRaizen, 'ecmwf-ens-orig', '{}{:0>2}'.format(dt_rodada_formated, rodada), 'data','A1E{}0000_{:0>2}.nc'.format(dt_rodada.strftime('%m%d'), membro))	
    elif modelo == 'ecmwf-ens-ext':
        padraoNome = os.path.join(dirPrevisoesModelosRaizen, 'ecmwf-ens-orig', '{}{:0>2}'.format(dt_rodada_formated, rodada), 'data','A1EF_all_{}0000_{:0>2}_regrid.nc'.format(dt_rodada.strftime('%m%d'), membro))	
    elif modelo == 'ecmwf-orig':
        padraoNome = os.path.join(dirPrevisoesModelosRaizen, 'ecmwf-orig', '{}{:0>2}'.format(dt_rodada_formated, rodada),'{}{:0>2}0000-d*-oper-fc_12z-12z.nc'.format(dt_rodada_formated, rodada))	
    elif modelo == 'eta':
        padraoNome = os.path.join(dirPrevisoesModelosRaizen, 'ons-eta', '{}{:0>2}'.format(dt_rodada_formated, rodada), 'data','pp{}_*.ctl.nc'.format(dt_rodada_formated))
    elif modelo == 'pobs':
        padraoNome = os.path.join(dirpreciptacaoObservada, 'MERGE_CPTEC_{}*.acumul12z.nc'.format(dt_rodada_formated))
    elif modelo == 'imerg':
        padraoNome = os.path.join(dirPreciptacaoImerge, 'imerg_daily_{}.nc'.format(dt_rodada_formated))
    elif modelo == 'merge':
        padraoNome = os.path.join(dirPreciptacaoMerge, 'prec_{}.nc'.format(dt_rodada.strftime('%Y.%m.%d')))

    return padraoNome


def nc_create_padrao(arq_netcdf):
    
    if len(arq_netcdf) == 1:
        arq = xr.open_dataarray(arq_netcdf[0], decode_times=False)
        acumulado_nc_padrao_list=[arq]
    
    else:
        acumulado_nc_padrao_list= [xr.open_mfdataset(arq_netcdf, concat_dim='time',combine='nested',engine='netcdf4',decode_times=False)]

    dims_names = acumulado_nc_padrao_list[0].dims
    
    for dim in dims_names:
        if "lat" in dim:
            lat_name = dim
        elif "lon" in dim:
            lon_name = dim
        elif "time" in dim:
            time_name = dim
    try:
        xr_concatenado = xr.concat(acumulado_nc_padrao_list, dim=time_name)
        xr_concatenado = xr_concatenado.rename({time_name: 'time'})
    except:
        xr_concatenado = xr.concat(acumulado_nc_padrao_list, dim='time')
        
    xr_concatenado = xr_concatenado.rename({lat_name: 'lat', lon_name: 'lon'})
    ds = xr_concatenado.assign_coords(lon=(((xr_concatenado.lon + 180) % 360) - 180)).sortby('lon')
    
    ds_transposed = ds.transpose('time', "lon", "lat")

    if ds_transposed["lat"][0] > ds_transposed["lat"][-1]:
        ds_transposed = np.flip(ds_transposed,2)
    try:
        var_name =  list(ds_transposed.data_vars)[0]
        ds_transposed = ds_transposed.rename({var_name:"acumulados24"})
    except:
        ds_transposed = ds_transposed.rename("acumulados24")

    return ds_transposed


def leitura_chuva_netcdf(modelo,dt_rodada,rodada=""):

    arq_coords_modelo = os.path.join(dirArqCoordenadasPsat,'coordenadas_{}.txt'.format(modelo))

    padraoNome = get_padraoNome_netcdf(modelo,dt_rodada,rodada)
    arqs_netcdf = glob.glob(padraoNome)

    if len(arqs_netcdf) == 0:
            raise Exception('Nenhum arquivo encontrado com o padrao de nome:\n%s' %(padraoNome))

    datas_previsao = []
    dt_rodada_time = datetime.strptime(dt_rodada,'%d/%m/%Y')
    for i, arq in enumerate(arqs_netcdf):
        ndia = i
        dtReferente = dt_rodada_time + timedelta(days=ndia)
        datas_previsao += dtReferente.strftime('%d/%m/%Y'),


    contornos_subbac={}
    with open(arq_coords_modelo, 'r') as arquivo:
        contornos = arquivo.readlines()
    for line in contornos:
        values = line.split(';')
        subbac = values[0]
        coords_list = []
        for value in values[1:-1]:
            coords_list += eval(value),
        contornos_subbac[subbac] = coords_list


    #nome da bacias
    subbacias = contornos_subbac.keys()

    arq_padrao = nc_create_padrao(arqs_netcdf)
    brasil = arq_padrao.sel(lat=slice(-37.0,15.0),lon=slice(-80, -30))
    brasil['time']= datas_previsao
    bac_prev={}

    for i,time in enumerate(np.array(brasil.time)):
        try:
            br_netcdf = brasil['acumulados24'][i]
        except:
            br_netcdf = brasil[i]   
        for subbac in subbacias:
            values_list = []
            for lon,lat in contornos_subbac[subbac]:
                p = float(br_netcdf.sel(lon=lon,lat=lat,method="nearest").values)
                if str(p) == 'nan': p = 0
                values_list += p,
            bac_prev[(time,subbac)] = [round(np.mean(values_list),2)]
    
    df_chuva = pd.DataFrame(bac_prev).T
    df_chuva = df_chuva.reset_index()
    df_chuva = df_chuva.rename({'level_1':'txt_nome_subbacia'},axis = 1)
    df_chuva = df_chuva.rename({'level_0':'dt_referente'},axis = 1)
    df_chuva = df_chuva.rename({0:'vl_chuva_media'},axis = 1)


    return df_chuva

def get_access_token() -> str:
    URL_COGNITO = os.getenv('URL_COGNITO')
    CONFIG_COGNITO = os.getenv('CONFIG_COGNITO')
    
    response = requests.post(
        URL_COGNITO,
        data=CONFIG_COGNITO,
        headers={'Content-Type': 'application/x-www-form-urlencoded'}
    )
    return response.json()['access_token']

def importar_chuva_observada(modelo,dt_final_observado):

    if type(dt_final_observado) == datetime:
        dt_final_observado = dt_final_observado.strftime('%d/%m/%Y')

    df_chuva = leitura_chuva_netcdf(modelo,dt_final_observado)

    #instancias das tabelas do banco de dados
    db_rodadas = wx_dbClass.db_mysql_master('db_rodadas')
    db_rodadas.connect()
    
    tb_subbacia = db_rodadas.getSchema('tb_subbacia')
    tb_chuva_obs = db_rodadas.getSchema('tb_chuva_obs')

    select_subbac = db.select(tb_subbacia.c.cd_subbacia,tb_subbacia.c.txt_nome_subbacia)
    tb_subbac_values = db_rodadas.conn.execute(select_subbac).fetchall()
    df_subbac_values = pd.DataFrame(tb_subbac_values,columns=["cd_subbacia","txt_nome_subbacia"])

    df_merged_values = pd.merge(df_subbac_values,df_chuva, on=['txt_nome_subbacia'])
    df_merged_values = df_merged_values.drop('txt_nome_subbacia', axis=1)

    dt_observado_time = datetime.strptime(dt_final_observado,'%d/%m/%Y')
    dt_inicio_observado_formated = (dt_observado_time - timedelta(days=1)).strftime('%Y-%m-%d')
    df_merged_values['dt_observado'] = dt_inicio_observado_formated

    values_to_insert = df_merged_values[['cd_subbacia','dt_observado','vl_chuva_media']].values.tolist()

    delete_chuva = tb_chuva_obs.delete().where(tb_chuva_obs.c.dt_observado == dt_inicio_observado_formated)
    num_linhas_deletadas = db_rodadas.conn.execute(delete_chuva).rowcount
    print(f"{num_linhas_deletadas} Linhas deletadas na tb_chuva_obs")

    insert_chuva = tb_chuva_obs.insert().values(values_to_insert)
    num_linhas_inseridas = db_rodadas.conn.execute(insert_chuva).rowcount
    print(f"{num_linhas_inseridas} Linhas inseridas na tb_chuva_obs")

    print(f"Dados do modelo {modelo} inseridos!")
    WHATSAPP_API = os.getenv('WHATSAPP_API')
    NUMERO_TESTE = os.getenv('NUMERO_TESTE')
    fields={
            "destinatario": NUMERO_TESTE,
            "mensagem": f"Dados do modelo {modelo} inseridos!",
        }
    
    response = requests.post(
        WHATSAPP_API,
        data=fields,
        headers={
            'Authorization': f'Bearer {get_access_token()}'
            }
        )
    
    print("Status Code:", response.status_code)

    return values_to_insert

def verificar_chuva_observada(dt_final_observado):


    if type(dt_final_observado) == str:
        dt_final_observado = datetime.strptime(dt_final_observado,'%d/%m/%Y')

    db_rodadas = wx_dbClass.db_mysql_master('db_rodadas')
    db_rodadas.connect()
    tb_chuva_obs = db_rodadas.getSchema('tb_chuva_obs')

    dt_inicio_observado_formated = (dt_final_observado - timedelta(days=1)).strftime('%Y-%m-%d')

    print(f"Verificando a chuva observada da data {dt_inicio_observado_formated} no banco!")

    selec_tb_chuva_obs = tb_chuva_obs.select().where(tb_chuva_obs.c.dt_observado == dt_inicio_observado_formated)
    answer_tb_chuva_obs = db_rodadas.conn.execute(selec_tb_chuva_obs).fetchall()

    if not answer_tb_chuva_obs:

        try:
            importar_chuva_observada("pobs",dt_final_observado)

        except:
            importar_chuva_observada("imerg",dt_final_observado)
    else:
        print("Chuva observada presente no banco!")



def importar_chuva_psath(path_psath):

    print(f'Fazendo leitura do arquivo {path_psath}')

    db_rodadas = wx_dbClass.db_mysql_master('db_rodadas')
    db_rodadas.connect()

    tb_subbacia = db_rodadas.getSchema('tb_subbacia')
    tb_chuva_psat = db_rodadas.getSchema('tb_chuva_psat')

    select_subbac = db.select(tb_subbacia.c.cd_subbacia,tb_subbacia.c.txt_nome_subbacia)
    tb_subbac_values = db_rodadas.conn.execute(select_subbac).fetchall()
    df_subbac_values = pd.DataFrame(tb_subbac_values,columns=["cd_subbacia","subbacia"])

    df_chuva = rz_chuva.get_zip_psath_df(path_psath)

    df_merged_values = pd.merge(df_subbac_values,df_chuva, on=['subbacia'])
    df_merged_values = df_merged_values.drop('subbacia', axis=1)

    zip_name = os.path.basename(path_psath)
    dt_psath = datetime.strptime(zip_name,'psath_%d%m%Y.zip').strftime('%Y-%m-%d')

    values = []
    dates_to_update = []
    df_merged_values = df_merged_values.fillna(0)
    for dt_final_obs in df_merged_values.columns[1:]:

        dt_ini_obs = dt_final_obs - timedelta(days=1)

        dates_to_update += dt_ini_obs.strftime('%Y-%m-%d'),
        
        df_aux = df_merged_values[['cd_subbacia', dt_final_obs]].copy()
        df_aux.loc[:,'dt_ini_obs'] = dt_ini_obs.strftime('%Y-%m-%d')

        #dt_final_obs : coluna que tem os valores de chuva
        values += df_aux[['cd_subbacia','dt_ini_obs',dt_final_obs]].values.tolist()

    sql_psat_delete = tb_chuva_psat.delete().where(tb_chuva_psat.c.dt_ini_observado.in_(dates_to_update))
    num_deletadas = db_rodadas.conn.execute(sql_psat_delete).rowcount
    print(f"{num_deletadas} Linhas deletadas da tabela tb_chuva_psat!")

    sql_psat_insert = tb_chuva_psat.insert().values(values)
    num_inseridas = db_rodadas.conn.execute(sql_psat_insert).rowcount
    print(f"{num_inseridas} Linhas inseridas na tabela tb_chuva_psat!")


if __name__ == '__main__':

    pass
