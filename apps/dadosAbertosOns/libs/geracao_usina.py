import pdb
import pandas as pd
import datetime
import sys
from numpy import nan
import warnings
warnings.filterwarnings("ignore")



sys.path.insert(1,"/WX2TB/Documentos/fontes/PMO/scripts_unificados/")
from bibliotecas.wx_dbClass import db_mysql_master
from apps.dadosAbertosOns.libs import utils

def tratar_dados(
        df: pd.DataFrame,
        colunas_df: str,
        colunas_renamed: str) -> dict:
    
    df = utils.tratamento_df(df, colunas_df, colunas_renamed)
    df = df.set_index(['dt_data_hora'])
    
    gb = df.groupby('tipo_usina')
    dfs_por_tipo_usina:dict = dict()

    for group in gb.groups:
        
        df_agrupado = gb.get_group(group)
        tipo_usina:str = df_agrupado['tipo_usina'].unique().all()
        
        df_agrupado['id_estado'] = df_agrupado['id_estado'].astype(int)
        df_agrupado = df_agrupado.groupby(['id_estado', pd.Grouper(freq='h')]).agg({'vl_geracao':'sum'})
        dfs_por_tipo_usina[tipo_usina] = df_agrupado
        
    return dfs_por_tipo_usina

def inserir(data:datetime.datetime = datetime.datetime.now()):
    tabelas = {'FOTOVOLTAICA':'tb_geracao_usina_solar', 'HIDROELÉTRICA':'tb_geracao_usina_hidraulica', 'TÉRMICA':'tb_geracao_usina_termica', 'EOLIELÉTRICA':'tb_geracao_usina_eolica',
       'NUCLEAR':'tb_geracao_usina_NUCLEAR'}
    colunas_df:list = ['din_instante', 'id_estado', 'nom_tipousina','val_geracao']
    colunas_renamed:list = ['dt_data_hora','id_estado', 'tipo_usina', 'vl_geracao']
    db:db_mysql_master = db_mysql_master('db_ons_dados_abertos')
    
    df_raw:pd.DataFrame = utils.request_csv(data, 'din_instante', 'geracao_usina_2')
    dfs_tratados = tratar_dados(df_raw, colunas_df, colunas_renamed)
    for index in dfs_tratados:
        utils.delete(
            db, tabelas[index], 'dt_data_hora', data
        )
        utils.inserir(
            db, tabelas[index], dfs_tratados[index].reset_index()
        )
    
    return None

if __name__ == '__main__':
    inserir(datetime.datetime(2024,6,1))