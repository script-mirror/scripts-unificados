import pandas as pd
import datetime
import sqlalchemy as sa
import sys

sys.path.insert(1,"/WX2TB/Documentos/fontes/PMO/scripts_unificados/")
from bibliotecas.wx_dbClass import db_mysql_master
from apps.dadosAbertosOns.libs import utils



def tratar_horario(
    df:pd.DataFrame,
) -> pd.DataFrame:
    
        df['dt_data_hora'] = pd.to_datetime(df['dt_data_hora']);
        
        df.set_index('dt_data_hora', inplace=True);
        
        df_resample = df.groupby('id_subsistema').resample('1H').agg({
            'vl_cmo': 'mean'
        })
        
        df = df_resample.reset_index();
        
        df.dropna(inplace=True)
        
        return df
    
    
    
def inserir_cmo_semanal(
    tabela:str,
    data:datetime.datetime = datetime.datetime.now()
):
    
    if data.weekday() != 5:
        return 
    
   
    df_cmo_semanal = utils.request_csv(data, 'din_instante', 'ons_cmo_semanal', 'anual');
    
    
    df_cmo_semanal_tratado = utils.tratamento_df(df_cmo_semanal, ['id_subsistema', 'din_instante', 'val_cmomediasemanal', 'val_cmoleve', 'val_cmomedia', 'val_cmopesada'], ['id_subsistema', 'dt', 'vl_media_semanal', 'vl_cmo_leve', 'vl_cmo_media', 'vl_cmo_pesada']);
    
    
    
    db = db_mysql_master('db_ons_dados_abertos');
    
    cmo_year = data.year;
    
    
    utils.delete(
        db, tabela, 'dt', datetime.datetime(cmo_year, 1, 1), datetime.datetime(cmo_year, 12, 31)
    )
        
    utils.inserir(
        db, tabela, df_cmo_semanal_tratado
    )
        
        
    
        

def inserir_cmo_horario(
    tabela:str,
    data:datetime.datetime = datetime.datetime.now()
):
    
    
    df_cmo_horario = utils.request_csv(data, 'din_instante', 'ons_cmo_horario', 'anual');
    
    
    df_cmo_horario_tratado = tratar_horario(utils.tratamento_df(df_cmo_horario, ['id_subsistema', 'din_instante', 'val_cmo'], ['id_subsistema', 'dt_data_hora', 'vl_cmo']));
    
    
    db = db_mysql_master('db_ons_dados_abertos');
    
    cmo_year = data.year;
    
    
    utils.delete(
        db, tabela, 'dt_data_hora', datetime.datetime(cmo_year, 1, 1), datetime.datetime(cmo_year, 12, 31)
    )
        
    utils.inserir(
        db, tabela, df_cmo_horario_tratado
    )



if __name__ == '__main__':
    # inserir_cmo_semanal(tabela='tb_cmo_semanal');
    # inserir_cmo_horario(tabela='tb_cmo_horario');
    pass;