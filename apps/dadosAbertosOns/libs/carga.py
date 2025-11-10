import pandas as pd
import datetime
import sys
import pytz
import warnings
from typing import Optional
warnings.filterwarnings("ignore")
sys.path.insert(1,"/WX2TB/Documentos/fontes/PMO/scripts_unificados/")
from bibliotecas.wx_dbClass import db_mysql_master
from apps.dadosAbertosOns.libs import utils


def tratar_dados(
    df: pd.DataFrame,
    colunas_df: str,
    colunas_renamed: str) -> pd.DataFrame:
    df = utils.tratamento_df(df, colunas_df, colunas_renamed)
    tz_brasil = pytz.timezone('America/Sao_Paulo')
    df['dt_data_hora'] = pd.to_datetime(df['dt_data_hora']).dt.tz_localize(None)
    df = df.set_index(['dt_data_hora'])
    
    df = df.resample('h').mean()
    df.index = df.index.tz_localize(pytz.utc).tz_convert(tz_brasil)
    df.reset_index(inplace=True)
    df['dt_data_hora'] = pd.to_datetime(df['dt_data_hora']).dt.tz_localize(None)
    df = df[df['dt_data_hora'] < datetime.datetime.now()]
    return df

def inserir(
    data:datetime.datetime = datetime.datetime.now(),
    data_fim:Optional[datetime.datetime] = None
):
    data = data.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if data_fim == None:
        data_fim = utils.last_day_of_month(data).replace(hour=23, minute=59, second=59)
        
    subsistemas = ['SECO', 'S', 'NE', 'N']
    db:db_mysql_master = db_mysql_master('db_ons_dados_abertos')
    tabela = 'tb_carga'
    colunas_df = ['cod_areacarga','din_referenciautc','val_cargaglobal','val_cargaglobalcons','val_cargaglobalsmmgd','val_cargasupervisionada','val_carganaosupervisionada','val_cargammgd','val_consistencia']
    colunas_renamed = ['id_subsistema','dt_data_hora','vl_carga_global','vl_carga_global_cons','vl_carga_global_smmgd','vl_carga_supervisionada','vl_carga_nao_supervisionada','vl_carga_mmgd','vl_consistencia']
    utils.delete(
        db,tabela,'dt_data_hora',data,data_fim + datetime.timedelta(days=1)
    )
    df_tratado = pd.DataFrame()
    for subsistema in subsistemas:
        df_raw = utils.request_api('carga_global',data,data_fim,subsistema)
        df_tratado = pd.concat([df_tratado, tratar_dados(df_raw,colunas_df,colunas_renamed)])
    utils.inserir(
        db,tabela,df_tratado
    )
    
    return None


if __name__ == '__main__':
    inserir(datetime.datetime.now().replace(day=1, month=datetime.datetime.now().month-1))
    # dinicio = datetime.datetime(2019,2,18) + datetime.timedelta(days=0)
    # while dinicio < datetime.datetime.now():
    #     dinicio = dinicio + datetime.timedelta(days=80)
    #     dfim = dinicio + datetime.timedelta(days=80)
    #     print(f'{dinicio} {dfim}')
    #     inserir(dinicio, dfim)
    pass
