import pdb
import pandas as pd
import datetime
from sys import path
import pytz
import warnings
from typing import Optional

warnings.filterwarnings("ignore")


path.append('/WX2TB/Documentos/fontes/')
from PMO.scripts_unificados.bibliotecas.wx_dbClass import db_mysql_master
from PMO.scripts_unificados.apps.dadosAbertosOns.libs import utils
from PMO.scripts_unificados.apps.dadosAbertosOns.libs import carga
 

def inserir(
    data:datetime.datetime = datetime.datetime.now(),
    data_fim:Optional[datetime.datetime] = None
):
    data = data.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if data_fim == None:
        data_fim = utils.last_day_of_month(data)
        
    subsistemas = ['SECO', 'S', 'NE', 'N']
    db:db_mysql_master = db_mysql_master('db_ons_dados_abertos')
    tabela = 'tb_carga_programada'
    colunas_df = ['cod_areacarga','din_referenciautc','val_cargaglobalprogramada']
    colunas_renamed = ['id_subsistema','dt_data_hora','vl_carga_global_programada']
    utils.delete(
        db,tabela,'dt_data_hora',data,data_fim + datetime.timedelta(days=1)
    )
    df_tratado = pd.DataFrame()
    for subsistema in subsistemas:
        df_raw = utils.request_api('carga_global_programada',data,data_fim,subsistema)
        df_tratado = pd.concat([df_tratado, carga.tratar_dados(df_raw,colunas_df,colunas_renamed)])
    utils.inserir(
        db,tabela,df_tratado
    )
    
    return None 


if __name__ == '__main__':
    inserir()
    # dinicio = datetime.datetime(2019,2,18) + datetime.timedelta(days=0)
    # while dinicio < datetime.datetime.now():
    #     dinicio = dinicio + datetime.timedelta(days=80)
    #     dfim = dinicio + datetime.timedelta(days=80)
    #     print(f'{dinicio} {dfim}')
    #     inserir(dinicio, dfim)
    pass
