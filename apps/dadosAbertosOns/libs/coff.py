import pdb
import pandas as pd
import datetime
from io import StringIO
from sys import path
import sqlalchemy
from numpy import nan
import warnings
warnings.filterwarnings("ignore")


path.append('/WX2TB/Documentos/fontes/')
from PMO.scripts_unificados.bibliotecas.wx_dbClass import db_mysql_master
from PMO.scripts_unificados.apps.dadosAbertosOns.libs import utils


def tratar_dados(
        df: pd.DataFrame,
        colunas_df: str,
        colunas_renamed: str) -> pd.DataFrame:
    df = utils.tratamento_df(df, colunas_df, colunas_renamed)
    df = df.set_index(['dt_data_hora'])
    df['vl_geracao_limitada'] = df['vl_geracao_limitada'].astype(float)
    df['vl_geracao_referencia_final'] = df['vl_geracao_referencia_final'].astype(float)
    df['id_estado'] = df['id_estado'].astype(str)
    # df = df.groupby('id_estado').resample('h', include_groups=False).sum().replace(nan, 0.0).div(2)
    df['cd_razao_restricao'] = df['cd_razao_restricao'].fillna('none')
    df = df.groupby(['id_estado', 'cd_razao_restricao', pd.Grouper(freq='h')]).sum().replace(nan, 0.0).div(2)
    df.reset_index(inplace=True)
    return df

def inserir(tabela:str, data:datetime.datetime = datetime.datetime.now()):
    colunas_df:list = ['id_estado', 'din_instante', 'val_geracao','val_geracaolimitada', 'val_disponibilidade', 'val_geracaoreferencia', 'val_geracaoreferenciafinal', 'cod_razaorestricao']
    colunas_renamed:list = ['id_estado', 'dt_data_hora', 'vl_geracao', 'vl_geracao_limitada', 'vl_disponibilidade', 'vl_geracao_referencia', 'vl_geracao_referencia_final', 'cd_razao_restricao']
    db:db_mysql_master = db_mysql_master('db_ons_dados_abertos')
    
    
    df_raw:pd.DataFrame = utils.request_csv(data, 'din_instante', tabela.replace('tb_', ''))
    df_tratado:pd.DataFrame = tratar_dados(df_raw, colunas_df, colunas_renamed)

    utils.delete(
        db, tabela, 'dt_data_hora', data
        )
    
    utils.inserir(
        db, tabela, df_tratado
        )
    
if __name__ == '__main__':
    inserir('tb_restricoes_coff_eolica', datetime.datetime(2024, 6, 1))
    pass