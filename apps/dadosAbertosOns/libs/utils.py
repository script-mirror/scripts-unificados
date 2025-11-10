import pdb
import requests
from io import StringIO
import sqlalchemy
from sys import path
from numpy import nan
import datetime
import pandas as pd
from typing import Optional

path.insert("/WX2TB/Documentos/fontes/PMO/scripts_unificados/")
from bibliotecas.wx_dbClass import db_mysql_master

formatos_data = {'mensal':'%Y_%m', 'diario':'%Y_%m_%d', 'anual':'%Y'}
link_base = {'restricoes_coff_eolica':'https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/restricao_coff_eolica_tm/RESTRICAO_COFF_EOLICA_',
             'restricoes_coff_solar':'https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/restricao_coff_fotovoltaica_tm/RESTRICAO_COFF_FOTOVOLTAICA_',
             'geracao_usina_2':'https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/geracao_usina_2_ho/GERACAO_USINA-2_',
             'geracao_usina':'https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/geracao_usina_2_ho/GERACAO_USINA_',
             'carga_global':'https://apicarga.ons.org.br/prd/cargaverificada',
             'carga_global_programada':'https://apicarga.ons.org.br/prd/cargaprogramada',
             'ons_cmo_semanal':'https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/cmo_se/CMO_SEMANAL_', 
             'ons_cmo_horario':'https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/cmo_tm/CMO_SEMIHORARIO_'}

def request_csv(
        data:datetime.datetime,
        coluna_data:str,
        link_key:str,
        date_format: str = 'mensal',
        extensao:str = '.csv'
        ) -> pd.DataFrame:
    
    data_formatada = data.strftime(formatos_data[date_format])
    res = requests.get(f'{link_base[link_key]}{data_formatada}{extensao}')
    res.raise_for_status()
    # if res.status_code == 404:
    #     raise Exception(f'Link incorreto ou data informada nao disponivel\nVerificar link manualmente: {link_base[link_key]}{data_formatada}{extensao}')
    str_res = res.text

    df = pd.read_csv(StringIO(str_res),
                     sep=";",
                     parse_dates=[coluna_data],
                     na_values=None)
    return df

def request_api(
    link_key:str,
    data_inicio:datetime.datetime,
    data_fim:datetime.datetime,
    subsistema:str
)->pd.DataFrame:
    res = requests.get(f'{link_base[link_key]}', params={'dat_inicio':data_inicio.strftime('%Y-%m-%d'), 'dat_fim':data_fim.strftime('%Y-%m-%d'), 'cod_areacarga':subsistema})
    res.raise_for_status()
    df = pd.DataFrame(res.json())
    return df

def inserir(
        db: db_mysql_master,
        tabela:str,
        df: pd.DataFrame
        ) -> None:
    
    tabela = db.getSchema(tabela)
    db.connect()

    insert = sqlalchemy.insert(tabela).values(
        df.to_dict('records')
    )
    result = db.db_execute(insert)
    print(f'{result.rowcount} linha(s) inserida(s)')
    return None

def delete(
        db: db_mysql_master,
        tabela:str,
        coluna_data:str,
        data:datetime.datetime,
        data_fim: Optional[datetime.datetime] = None,
        corrigir_horario:bool = False
        ) -> None:
    """_summary_

    Args:
        db (db_mysql_master): instacia de db_mysql_master nao conectada
        tabela (str): nome da tabela
        coluna_data (str): nome da coluna de data usada para deletar corretamente os dados
        data (datetime.datetime): data para deletar
        data_fim (datetime.datetime | None, optional): data final da delecao. 
            caso seja informado, sera deletado todos os dados entre data e data_fim.
                caso contrario, apenas os dados do mes e ano de data.
        corrigir_horario(bool) = False: caso true, data sera ajustada para 00h e data fim sera ajustada para 23h
    Returns:
        None: None
    """
    tabela = db.getSchema(tabela)
    db.connect()
    if data_fim:
            if corrigir_horario:
                data = data.replace(hour=0, minute=0, second=0)
                data_fim = data_fim.replace(hour=23, minute=59, second=59)
            delete = sqlalchemy.delete(tabela).where(
            tabela.c[coluna_data].between(data, data_fim)
    )
    else:
        delete = sqlalchemy.delete(tabela).where(sqlalchemy.and_(
                sqlalchemy.func.year(tabela.c[coluna_data]) == sqlalchemy.func.year(data),
                sqlalchemy.func.month(tabela.c[coluna_data]) == sqlalchemy.func.month(data)
            )
        )
    result = db.db_execute(delete)
    print(f'{result.rowcount} linha(s) deletada(s)')
    return None
    
def tratamento_df(
        df: pd.DataFrame,
        colunas_df: str,
        colunas_renamed: str) -> pd.DataFrame:
    
    df.replace(nan, None, inplace=True)
    rename = dict(zip(colunas_df, colunas_renamed))
    df = df[colunas_df]
    df.rename(columns=rename, inplace=True)
    if 'id_estado' in df:
        dict_estados = estados_to_dict()
        df['id_estado'] = df['id_estado'].map(dict_estados)
    elif 'id_subsistema' in df:
        dict_subsistemas = subsistemas_to_dict()
        dict_subsistemas['SECO'] = dict_subsistemas['SE']
        df['id_subsistema'] = df['id_subsistema'].map(dict_subsistemas)
    return df

def estados_to_dict() -> dict:
    db = db_mysql_master('db_ons_dados_abertos')
    db.connect()
    
    tb_estado = db.getSchema('tb_estado')
    query = sqlalchemy.select(
        tb_estado.c['id_estado'],
        tb_estado.c['str_sigla']
    )
    result = db.db_execute(query)
    result = result.all()
    df = pd.DataFrame(result, columns=['id_estado','str_sigla'])
    return dict(zip(df['str_sigla'], df['id_estado']))

def subsistemas_to_dict() -> dict:
    db = db_mysql_master('db_ons')
    db.connect()
    
    tb_estado = db.getSchema('tb_submercado')
    query = sqlalchemy.select(
        tb_estado.c['cd_submercado'],
        tb_estado.c['str_sigla']
    )
    result = db.db_execute(query)
    result = result.all()
    df = pd.DataFrame(result, columns=['cd_submercado','str_sigla'])
    return dict(zip(df['str_sigla'], df['cd_submercado']))

def buscar_dados_armazenados_por_data(
        db: db_mysql_master,
        tabela:str,
        coluna_data:str,
        colunas:list,
        data:datetime.datetime = datetime.datetime.now(),
        ):
    tabela:sqlalchemy.Table = db.getSchema(tabela)
    db.connect()

    select = sqlalchemy.select(tabela).where(sqlalchemy.and_(
        sqlalchemy.func.month(tabela.c[coluna_data]) == sqlalchemy.func.month(data),
        sqlalchemy.func.year(tabela.c[coluna_data]) == sqlalchemy.func.year(data),
        )
    )
    result = db.db_execute(select).all()
    return pd.DataFrame(result, columns=colunas)

def filtrar_dados_para_update(df_dados_atuais:pd.DataFrame, df_dados_novos:pd.DataFrame) -> pd.DataFrame:
    concat_df = pd.concat([df_dados_novos, df_dados_atuais])
    return concat_df

def filtrar_dados_para_insert(df_dados_atuais:pd.DataFrame, df_dados_novos:pd.DataFrame, coluna_data:str) -> pd.DataFrame:
    merged_df = df_dados_novos.merge(df_dados_atuais[[coluna_data]], on=coluna_data, how='left', indicator=True)
    df_result = merged_df[merged_df['_merge'] == 'left_only']
    return df_result.drop(columns=['_merge'])

def last_day_of_month(any_day) -> datetime.datetime:
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
    return next_month - datetime.timedelta(days=next_month.day)

if __name__ == '__main__':
    # request_api('carga_global', datetime.datetime.now() - datetime.timedelta(days=2), datetime.datetime.now(), 'SECO')
    print(last_day_of_month(datetime.datetime.now()))
    pass