import pymysql
import numpy as np
import pandas as pd
import datetime
import sqlalchemy as db
import sys
import os
import calendar

from sqlalchemy.sql.expression import func
from sqlalchemy import between, desc, and_
from datetime import date
from unidecode import unidecode


sys.path.insert(1,"/WX2TB/Documentos/fontes/PMO/scripts_unificados/")
from bibliotecas import wx_dbClass,wx_opweek,wx_dbLib

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")),'.env'))


# Variaveis globais
dateFormat = "%Y/%m/%d"

class WxDataB:

    def __init__(self, dbm='mssql'):
        #DB MSSQL CONFIGURATION
        __HOST_MSSQL = os.getenv('HOST_MSSQL') 
        __PORT_DB_MSSQL = os.getenv('PORT_DB_MSSQL') 

        __USER_DB_MSSQL = os.getenv('USER_DB_MSSQL') 
        __PASSWORD_DB_MSSQL = os.getenv('PASSWORD_DB_MSSQL')


        #DB MYSQL CONFIGURATION
        __HOST_MYSQL = os.getenv('HOST_MYSQL')
        __PORT_DB_MYSQL = os.getenv('PORT_DB_MYSQL')

        __USER_DB_MYSQL = os.getenv('USER_DB_MYSQL')
        __PASSWORD_DB_MYSQL = os.getenv('PASSWORD_DB_MYSQL')
        
        if dbm == 'mssql':
            self.dbHost = __HOST_MSSQL
            self.dbUser = __USER_DB_MSSQL
            self.dbPassword = __PASSWORD_DB_MSSQL
            self.dbDatabase = 'climenergy'
            self.port = int(__PORT_DB_MSSQL)

        elif dbm == 'mysqlWx':
            self.dbHost = __HOST_MYSQL
            self.dbUser = __USER_DB_MYSQL
            self.dbPassword = __PASSWORD_DB_MYSQL
            self.dbDatabase = 'bbce'
            self.port = int(__PORT_DB_MYSQL)

        elif dbm == 'mysql':
            self.dbHost = '127.0.0.1'
            self.dbUser = 'root'
            self.dbPassword = ''
            self.dbDatabase = 'climenergy'
            self.port = 3306

        elif dbm == 'aws':
            self.dbHost = 'climenergy.cnvxdohkhlix.sa-east-1.rds.amazonaws.com'
            self.dbUser = 'climenergy'
            self.dbPassword = 'climeserver'
            self.dbDatabase = 'climenergy'
            self.port = "1433"

        self.dbm = dbm

    def connectHost(self):

        if self.dbm in ['mysql', 'mysqlWx']:
            conn = pymysql.connect(host=self.dbHost, user=self.dbUser, password=self.dbPassword, database=self.dbDatabase, port=self.port)

        return conn

    def requestServer(self, actionDb):
        try:
            conn = self.connectHost()
            cursor = conn.cursor()
            cursor.execute(actionDb)
            answerDb = ''
            # Retirada a parte de escrita pois o site so ira fazer leitura
            # if actionDb.split()[0]=="INSERT" or actionDb.split()[0]=="UPDATE":
            if 0:
                conn.commit()
                answerDb = cursor.rowcount
            # elif actionDb.split()[0]=="SELECT" or actionDb.split()[0]=="DECLARE":
            else:
                answerDb = cursor.fetchall()


            conn.close()
            return answerDb
        except Exception as e:
            # print('Sever out!')
            print(e)
            return 0

    def execute(self, actionDb, values):
        if actionDb.lower().split()[0] == "select":
            # Values deve ser uma tuplas com os valores na mesma ordem da query
            conn = self.connectHost()
            cursor = conn.cursor()
            cursor.execute(actionDb, values)
            return cursor.fetchall()
        else:
            # Values deve ser um array de tuplas
            conn = self.connectHost()
            cursor = conn.cursor()
            cursor.execute(actionDb, values)
            numLinhasInseridas = cursor.rowcount
            conn.commit()
            conn.close()
            return numLinhasInseridas



def getLstRev():

    db_ons = wx_dbClass.db_mysql_master("db_ons", connect=True)
    tb_ve = db_ons.db_schemas['tb_ve']

    submercados = {4:'NORTE', 3: 'NORDESTE', 1: 'SUDESTE', 2: 'SUL'}

    subquery_max_ano = db.select([db.func.max(tb_ve.c.vl_ano)]).scalar_subquery()

    subquery_max_mes = db.select([db.func.max(tb_ve.c.vl_mes)]).where(tb_ve.c.vl_ano == subquery_max_ano).scalar_subquery()

    subquery_max_revisao = db.select([db.func.max(tb_ve.c.cd_revisao)]).where(
        db.and_(
            tb_ve.c.vl_ano == subquery_max_ano,
            tb_ve.c.vl_mes == subquery_max_mes
        )
    ).scalar_subquery()

    query = db.select([
        tb_ve.c.dt_inicio_semana,
        tb_ve.c.vl_ena.label('ena'),
        tb_ve.c.cd_revisao.label('rev'),
        tb_ve.c.cd_submercado.label('submercado')
    ]).where(
        db.and_(
            tb_ve.c.vl_ano == subquery_max_ano,
            tb_ve.c.vl_mes == subquery_max_mes,
            tb_ve.c.cd_revisao == subquery_max_revisao
        )
    ).order_by(
        tb_ve.c.cd_submercado,
        tb_ve.c.dt_inicio_semana
        )

    # Executando a query
    answer = db_ons.db_execute(query).fetchall()
    db_ons.db_dispose()

    ena = {}
    for row in answer:
        if submercados[row[3]] not in ena:
            ena[submercados[row[3]]] = {}
        ena[submercados[row[3]]][row[0]] = row[1]

    # numero da revisao
    revisao = answer[0][-2]
    return ena, revisao

def getAllRvs():
    """ Pega todas as revisao inserida no banco do mes
    :param : 
    :return ena: dicionario com os valores de ENA
    """

    db_ons = wx_dbClass.db_mysql_master("db_ons", connect=True)
    tb_ve = db_ons.db_schemas['tb_ve']

    subquery_max_ano = db.select(db.func.max(tb_ve.c.vl_ano)).scalar_subquery()

    subquery_max_mes = db.select(db.func.max(tb_ve.c.vl_mes)).where(tb_ve.c.vl_ano == subquery_max_ano).scalar_subquery()

    # Consulta principal
    query = db.select(
        tb_ve.c.dt_inicio_semana,
        tb_ve.c.vl_ena,
        tb_ve.c.cd_revisao,
        tb_ve.c.cd_submercado
    ).where(
        db.and_(
            tb_ve.c.vl_ano == subquery_max_ano,
            tb_ve.c.vl_mes == subquery_max_mes
        )
    )
    answer = db_ons.db_execute(query).fetchall()
    db_ons.db_dispose()
    return answer

def getRevBacias(dataInicial):
    """ Pega os dados da ultima revisao inserida no banco separados por bacias
    :param : 
    :return ena: dicionario com os valores de ENA
    :return revisao: Numero da ultima revisao 
    """
    dateFormat = "%Y-%m-%d"

    db_ons = wx_dbClass.db_mysql_master("db_ons", connect=True)
    tb_ve_bacias = db_ons.db_schemas['tb_ve_bacias']
    tb_bacias_segmentadas = db_ons.db_schemas['tb_bacias_segmentadas']

    row_number_column = db.func.row_number().over(
        partition_by=[tb_ve_bacias.c.cd_bacia, tb_ve_bacias.c.dt_inicio_semana],
        order_by=[tb_ve_bacias.c.vl_ano.desc(), tb_ve_bacias.c.vl_mes.desc(), tb_ve_bacias.c.cd_revisao.desc(), tb_ve_bacias.c.dt_inicio_semana.desc()]
    ).label('row_number')

    groups_subquery = db.select(
        tb_ve_bacias.c.vl_ano,
        tb_ve_bacias.c.vl_mes,
        tb_ve_bacias.c.cd_revisao,
        tb_ve_bacias.c.dt_inicio_semana,
        tb_ve_bacias.c.vl_ena,
        tb_ve_bacias.c.cd_bacia,
        tb_ve_bacias.c.vl_perc_mlt,
        row_number_column
    ).where(
        tb_ve_bacias.c.dt_inicio_semana >= dataInicial.strftime(dateFormat)
    ).subquery('groups')

    query = db.select(
        groups_subquery.c.vl_ano,
        groups_subquery.c.vl_mes,
        groups_subquery.c.cd_revisao,
        groups_subquery.c.dt_inicio_semana,
        groups_subquery.c.vl_ena,
        tb_bacias_segmentadas.c.str_bacia,
        groups_subquery.c.vl_perc_mlt
    ).join(
        tb_bacias_segmentadas,
        groups_subquery.c.cd_bacia == tb_bacias_segmentadas.c.cd_bacia
    ).where(
        groups_subquery.c.row_number == 1
    ).order_by(
        tb_bacias_segmentadas.c.str_bacia,
        groups_subquery.c.dt_inicio_semana
    )
    answer = db_ons.db_execute(query).fetchall()
    db_ons.db_dispose()
    return answer


def get_geracao_eolica_verificada(data_inicial, data_final):
    datab = WxDataB("mysqlWx")
    datab.dbDatabase = 'db_ons'
    query_get_geracao_eolica = '''SELECT STR_SUBMERCADO, DT_REFERENTE, VL_CARGA FROM tb_geracao_horaria where DT_REFERENTE BETWEEN %s and %s and CD_GERACAO = %s'''
    answer = datab.execute(query_get_geracao_eolica, (data_inicial, data_final, 1))
    return answer

#armazenamento % do submercado e do sin

def get_Earm_submercado(data_inicial):

    db_ons = wx_dbClass.db_mysql_master('db_ons')
    db_ons.connect()

    tb_earm_max = db_ons.getSchema('tb_earm_max')
    tb_rdh_submercado = db_ons.getSchema('tb_rdh_submercado')
    tb_submercado = db_ons.getSchema('tb_submercado')

    select_tb_earm_max = tb_earm_max.select()
    answer = db_ons.conn.execute(select_tb_earm_max).fetchall()

    df_arm_max = pd.DataFrame(answer, columns=['str_submercado', 'vl_earm_max'])
    total_sin = df_arm_max['vl_earm_max'].sum()

    select_tb = db.select(
        tb_submercado.c.str_submercado,
        tb_rdh_submercado.c.cd_submercado,
        tb_rdh_submercado.c.dt_referente,
        tb_rdh_submercado.c.vl_vol_arm_perc
        ).join(
            tb_rdh_submercado, tb_rdh_submercado.c.cd_submercado == tb_submercado.c.cd_submercado
        ).where(
            tb_rdh_submercado.c.dt_referente >= data_inicial
            )

    result = db_ons.conn.execute(select_tb).fetchall()
    
    df_armazenamento = pd.DataFrame(result, columns = ['str_submercado','cd_submercado','dt_referente','vl_armazenamento'])
    
    df_armazenamento['dt_referente'] = df_armazenamento['dt_referente'].dt.strftime('%Y-%m-%d')

    df_arm_submerc = df_armazenamento.pivot_table(index='dt_referente', columns='str_submercado', values='vl_armazenamento')

    df_arm_submerc_aux = df_arm_submerc.T
    df_arm_submerc_aux = pd.concat([df_arm_submerc_aux, df_arm_max.set_index('str_submercado')], axis=1)
    df_arm_submerc_aux = df_arm_submerc_aux.iloc[:, :-1].mul(df_arm_submerc_aux['vl_earm_max'], axis=0)

    row_sin = pd.DataFrame(df_arm_submerc_aux.sum()/total_sin)
    row_sin = row_sin.rename({0:'SIN'}, axis=1)

    df_arm_submerc_sin = pd.concat([df_arm_submerc,row_sin], axis=1).round(2)


    values = df_arm_submerc_sin.to_dict()
    
    return values


def get_deck_geracao_dessem_eolica(dt_ini,dt_fim,dt_deck):
    
    db_ons = wx_dbClass.db_mysql_master('db_ons')
    db_ons.connect()
    tb_weol_eolica = db_ons.getSchema('tb_weol_eolica')

    sql_select = db.select(
        tb_weol_eolica.c.cd_submercado,
        tb_weol_eolica.c.vl_geracao_eol,
        tb_weol_eolica.c.dt_referente,
        tb_weol_eolica.c.dt_deck
    ).where(
        db.and_(
            tb_weol_eolica.c.dt_referente >= dt_ini,
            tb_weol_eolica.c.dt_referente < dt_fim,
            tb_weol_eolica.c.dt_deck == dt_deck

        )
        )
    
    resultados = db_ons.conn.execute(sql_select).fetchall()
    df_eol_prev = pd.DataFrame(resultados, columns=['cd_submercado','vl_geracao_eol','dt_referente','dt_deck'])
    
    submercados = {3:'NORDESTE', 2:'SUL'}
    df_eol_prev['submercado'] = df_eol_prev['cd_submercado'].map(submercados)
    df_eol_prev = df_eol_prev.drop('cd_submercado',axis=1)

    return df_eol_prev

def get_df_ultima_geracao_termica_min_semana(dt_prev):

    dt_anterior = dt_prev - datetime.timedelta(days=1)
    dt_prev_aux = dt_prev
    ultimo_sabado = wx_opweek.getLastSaturday(dt_prev)
    prox_sabado = ultimo_sabado + datetime.timedelta(days=7)

    #GERAÇÃO TERMICA MINIMA
    balanco = wx_dbLib.get_Balanco_Dessem(dt_anterior)
    columns = ['dt_data_hora', 'str_subsistema', 'vl_cmo', 'vl_demanda',
       'vl_geracao_renovaveis', 'vl_geracao_hidreletrica',
       'vl_geracao_termica', 'vl_gtmin', 'vl_gtmax', 'vl_intercambio',
       'vl_pld']

    df_balanco = pd.DataFrame(balanco, columns = columns)

    df_geracao_termica_min = pd.DataFrame(columns = ['dt_data_hora','str_subsistema','vl_gtmin'])
    
    df_balanco_do_dia = df_balanco[(df_balanco['dt_data_hora']>= dt_prev) & (df_balanco['dt_data_hora']< dt_prev+datetime.timedelta(days=1))][['dt_data_hora','str_subsistema','vl_gtmin']]

    count_days = 0
    if df_balanco_do_dia.empty:
            df_balanco_do_dia = df_balanco[df_balanco['dt_data_hora']< dt_prev][['dt_data_hora','str_subsistema','vl_gtmin']]
            count_days = 1
    
    while dt_prev_aux < prox_sabado :

        df_geracao_termica_min_aux = df_balanco_do_dia
        
        df_geracao_termica_min_aux['dt_referente'] = df_geracao_termica_min_aux['dt_data_hora'] + datetime.timedelta(days=count_days)
        if df_geracao_termica_min_aux.empty:
            df_geracao_termica_min = df_geracao_termica_min_aux
        else:
            df_geracao_termica_min = pd.concat([df_geracao_termica_min, df_geracao_termica_min_aux], axis=0)
        
        count_days += 1
        dt_prev_aux += datetime.timedelta(days=1)
        
    df_geracao_termica_min['cd_submercado'] = df_geracao_termica_min['str_subsistema'].map(lambda x: 1 if x == 'SE' else 2 if x == 'S' else 3 if x == 'NE' else 4)

    return df_geracao_termica_min


def get_df_prev_geracao_renovaveis_semana(dt_prev):

    ##RENOVAVEIS
    previsao = wx_dbLib.get_deck_dessem_renovaveis(dt_prev)
    dt_prev_aux = dt_prev

    colunas = ['vl_periodo', 'cd_submercado', 'vl_geracao_uhe', 'vl_geracao_ute',
                         'vl_geracao_cgh', 'vl_geracao_pch', 'vl_geracao_ufv', 'vl_geracao_uee','vl_geracao_mgd']
    df_previsao = pd.DataFrame(previsao, columns=colunas)

    if df_previsao.empty:
        return df_previsao

    else:
        df_previsao['dt_referente'] = dt_prev_aux + pd.to_timedelta((df_previsao['vl_periodo']-1)*30, unit='m')

        df_prev_renovaveis = df_previsao.set_index(['dt_referente','cd_submercado'])\
                                                    [['vl_geracao_uhe', 'vl_geracao_ute','vl_geracao_cgh', 'vl_geracao_pch', 'vl_geracao_ufv', 'vl_geracao_uee','vl_geracao_mgd']]\
                                                    .sum(axis=1).reset_index().rename(columns={0:'total geracao'})
        df_prev_renovaveis_resample = df_prev_renovaveis.set_index('dt_referente').groupby('cd_submercado')['total geracao'].resample('1H').mean().ffill().reset_index()
        return df_prev_renovaveis_resample

#Carga do PrevCargaDessem
def get_df_prev_carga_ds(dt_prev):

    df_prev_carga_db = get_PrevCargaDessem_saida(dt_prev - datetime.timedelta(days=1))

    columns = ['cd_submercado', 'dt_referente', 'vl_carga', 'dt_ini']
    df_prev_carga_db.columns = columns

    if df_prev_carga_db.empty:
        msg = "Não há valores do PrevCargaDessem({})".format((dt_prev - datetime.timedelta(days=1)).strftime("%d/%m/%Y"))
        print(msg)
        return df_prev_carga_db
    
    else:
        df_carga_complete = df_prev_carga_db[df_prev_carga_db['dt_referente'] >= dt_prev]
        df_carga_resample = df_carga_complete.set_index('dt_referente').groupby('cd_submercado')['vl_carga'].resample('1h').mean().ffill().reset_index()

        return df_carga_resample


#Carga do bloco DP
def get_df_prev_carga_ds_DP(dt_prev):

    carga_db = wx_dbLib.getPrevisaoCargaDs(dt_prev)

    columns = ['cd_submercado', 'dt_referente', 'vl_carga', 'dt_inicio','id_fonte']
    df_carga_db = pd.DataFrame(carga_db, columns= columns)

    if df_carga_db.empty:
        msg = 'Não Há valores para a previsão de carga do bloco DP'
        return df_carga_db

    else:
        df_carga_bloco = df_carga_db[df_carga_db['dt_referente'] < dt_prev + datetime.timedelta(days=1)]
        df_carga_resample = df_carga_bloco.set_index('dt_referente').groupby('cd_submercado')['vl_carga'].resample('1h').mean().ffill().reset_index()

        return df_carga_resample



#Carga liquida pro PrevCargaDessem 
def prev_carga_liquida_ds(dt_prev , fonte_carga = 'bloco_dp'):

    dt_anterior = dt_prev - datetime.timedelta(days=1)

    #GERAÇÃO TERMICA MINIMA
    df_geracao_termica_min = get_df_ultima_geracao_termica_min_semana(dt_prev)

    ##RENOVAVEIS
    df_prev_renovaveis_resample =get_df_prev_geracao_renovaveis_semana(dt_prev)
    if df_prev_renovaveis_resample.empty:
        
        df_prev_renovaveis_resample = get_df_prev_geracao_renovaveis_semana(dt_anterior)
        if df_prev_renovaveis_resample.empty:
            return df_prev_renovaveis_resample


    #PREVISÃO DE CARGA
    if fonte_carga == 'bloco_dp':
        df_carga_resample = get_df_prev_carga_ds_DP(dt_prev)
    else:
        df_carga_resample = get_df_prev_carga_ds(dt_prev)
    if df_carga_resample.empty:
        return df_carga_resample

    #CARGA LIQUIDA
    df_ger_carga = pd.merge(df_prev_renovaveis_resample,df_carga_resample, on=['cd_submercado','dt_referente'])
    df_final = pd.merge(df_ger_carga,df_geracao_termica_min, on=['cd_submercado','dt_referente'])

    df_final['vl_carga_liquida'] = df_final['vl_carga'] - df_final['total geracao'] - df_final['vl_gtmin']

    df_ger_carga_submerc = df_final.groupby(['str_subsistema','dt_referente'])['vl_carga_liquida'].sum().reset_index()
    
    df_ger_carga_sin = df_final.groupby(['dt_referente'])['vl_carga_liquida'].sum().reset_index()
    
    df_ger_carga_sin['str_subsistema'] = 'SIN'

    df_ger_carga_completo  = pd.concat([df_ger_carga_submerc, df_ger_carga_sin]).round(2)
    df_ger_carga_completo['hr'] = pd.to_datetime(df_ger_carga_completo['dt_referente']).dt.strftime('%H:%M')
    df_ger_carga_completo['horario'] = pd.to_datetime(df_ger_carga_completo['dt_referente']).dt.strftime('%d/%m/%Y')

    return df_ger_carga_completo


def get_PrevCargaDessem_saida(dt_ini):

  db_ons = wx_dbClass.db_mysql_master('db_ons')
  db_ons.connect()
  tb_prev_carga = db_ons.getSchema('tb_prev_carga')

  sql_select = tb_prev_carga.select().where(tb_prev_carga.c.dt_inicio == dt_ini)
  carga_values = db_ons.conn.execute(sql_select).fetchall()

  columns = ['cd_submercado', 'dt_referente', 'vl_carga', 'dt_inicio']
  df_carga = pd.DataFrame(carga_values, columns= columns)
  
  return df_carga


def get_df_info_rdh(ano:str, tipo:str, mes_inicio:int=1, mes_fim:int=12):

    tipos= {
        'ear':'vl_vol_arm_perc',
        'vaz_turbinada':'vl_vaz_turb',
        'vaz_afluente': 'vl_vaz_afl',
        'vaz_defluente': 'vl_vaz_dfl',
        'vaz_incremental': 'vl_vaz_inc',
        'vaz_vertida' : 'vl_vaz_vert'
        }
    
    coluna = tipos[tipo]
    db_ons = wx_dbClass.db_mysql_master('db_ons')
    db_ons.connect()
    tb_rdh = db_ons.getSchema('tb_rdh')

    tb_posto = db_ons.getSchema('tb_postos_completo')
    sql_select = db.select(
        tb_posto.c.str_posto,
        tb_rdh.c[coluna],
        tb_rdh.c.dt_referente)\
    .join(tb_rdh, tb_rdh.c.cd_posto == tb_posto.c.cd_posto)\
    .where(
         db.and_(
            db.extract('year', tb_rdh.c.dt_referente) == ano,
            db.extract('month', tb_rdh.c.dt_referente).between(mes_inicio, mes_fim),
          )
        ).order_by(tb_rdh.c.dt_referente)
    values = db_ons.conn.execute(sql_select).fetchall()
    columns = ['str_posto',coluna,'dt_referente']
    df_rdh = pd.DataFrame(values, columns=columns)
    
    if df_rdh.empty:
        print(f'0 Valores encontrados para o ano {ano}')
        return df_rdh

    df_rdh['dt_referente'] = pd.to_datetime(df_rdh['dt_referente']).dt.strftime('%Y-%m-%d')
    df_rdh['str_posto'] = df_rdh['str_posto'].apply(lambda x: unidecode(x))

    dict_master = {}
    dict_master['Ano'] = ano
    dict_master['valores'] = {}
    if df_rdh.empty:
        return {}
    df_pivot = df_rdh.pivot_table(index='str_posto', columns='dt_referente', values=coluna, aggfunc='mean')
    df_pivot = df_pivot.dropna()
    dict_values = df_pivot.to_dict('index')

    dict_master['valores'][tipo] = dict_values

    return dict_master

def get_info_rdh_por_posto(ano:str, tipo:str, posto:str):

    tipos= {
        'ear':'vl_vol_arm_perc',
        'vaz_turbinada':'vl_vaz_turb',
        'vaz_afluente': 'vl_vaz_afl',
        'vaz_defluente': 'vl_vaz_dfl',
        'vaz_incremental': 'vl_vaz_inc',
        'vaz_vertida' : 'vl_vaz_vert'
        }
    
    coluna = tipos[tipo]
    db_ons = wx_dbClass.db_mysql_master('db_ons')
    db_ons.connect()
    tb_rdh = db_ons.getSchema('tb_rdh')

    tb_posto = db_ons.getSchema('tb_postos_completo')

    sql_select = db.select(
        tb_posto.c.str_posto,
        tb_rdh.c[coluna],
        tb_rdh.c.dt_referente)\
    .join(tb_rdh, tb_rdh.c.cd_posto == tb_posto.c.cd_posto)\
    .where(
         db.and_(
            db.extract('year', tb_rdh.c.dt_referente) == ano),
            tb_posto.c.str_posto == posto
         ).order_by(tb_rdh.c.dt_referente)
    values = db_ons.conn.execute(sql_select).fetchall()
    columns = ['str_posto',coluna,'dt_referente']
    df_rdh = pd.DataFrame(values, columns=columns)
    
    if df_rdh.empty:
        print(f'0 Valores encontrados para o ano {ano}')
        return df_rdh

    df_rdh['dt_referente'] = pd.to_datetime(df_rdh['dt_referente']).dt.strftime('%Y-%m-%d')
    df_rdh['str_posto'] = df_rdh['str_posto'].apply(lambda x: unidecode(x))

    dict_master = {}
    dict_master['Ano'] = ano
    dict_master['valores'] = {}
    if df_rdh.empty:
        return {}
    df_pivot = df_rdh.pivot_table(index='str_posto', columns='dt_referente', values=coluna, aggfunc='mean')
    df_pivot = df_pivot.dropna()
    dict_values = df_pivot.to_dict('index')

    dict_master['valores'][tipo] = dict_values

    return dict_master

def get_postos_rdh():
    db_ons = wx_dbClass.db_mysql_master('db_ons')
    db_ons.connect()

    tb_posto = db_ons.getSchema('tb_postos_completo')

    sql_select = db.select(
        tb_posto.c.str_posto
            ).order_by(
                db.case(
                     (tb_posto.c.str_posto == 'ITAIPU', 1),
                     else_= 2
                     ), tb_posto.c.str_posto
                )
    
    values = db_ons.conn.execute(sql_select).fetchall()
    columns = ['str_posto']
    df_rdh = pd.DataFrame(values, columns=columns)

    df_rdh['str_posto'] = df_rdh['str_posto'].apply(lambda x: unidecode(x))

    dict_master = {}
    if df_rdh.empty:
        return {}
    dict_master['postos'] = df_rdh['str_posto'].to_list() 
    return dict_master

def get_mlt_submercado(data_inicial:datetime = datetime.datetime(2021,1,1), data_final:datetime = datetime.datetime(2022,1,1)):
    
    cod_submercados = {1:'SE',2:'S',3:'NE',4:'N'}
    dataBase = wx_dbClass.db_mysql_master('db_ons')
    dataBase.connect()
    tb_ena_submercado = dataBase.getSchema("tb_ena_submercado")
    
    if data_final == None:
        s = db.select(tb_ena_submercado.c.id_submercado,
                                     tb_ena_submercado.c.dt_ref,
                                     tb_ena_submercado.c.vl_mwmed,
                                     tb_ena_submercado.c.vl_percent).where(
            tb_ena_submercado.c.dt_ref>=data_inicial)
    else:
        s = db.select(tb_ena_submercado.c.id_submercado,
                                     tb_ena_submercado.c.dt_ref,
                                     tb_ena_submercado.c.vl_mwmed,
                                     tb_ena_submercado.c.vl_percent).where(
            and_(tb_ena_submercado.c.dt_ref>=data_inicial,
                 tb_ena_submercado.c.dt_ref<data_final))
                                  
    resposta = dataBase.conn.execute(s)
    ena = pd.DataFrame(resposta, columns=['submercado','dt_ref','vl_mwmed','vl_percent'])
    ena['mlt'] = (ena['vl_mwmed'] * 100) / ena['vl_percent']
    ena['mlt'] = ena['mlt'].apply(lambda x: round(x / 1000, 2))
    
    ena['dt_ref'] = ena['dt_ref'].apply(lambda x: x.strftime('%m/%Y'))

    ena = ena[['submercado', 'dt_ref', 'mlt']]
    ena['submercado'] = ena['submercado'].replace(cod_submercados)
    return ena
    
    
def get_mlt_bacia(data_inicial:datetime = datetime.datetime(2021,1,1), data_final:datetime = datetime.datetime(2022,1,1)):
    dataBase = wx_dbClass.db_mysql_master('db_ons')
    dataBase.connect()
    tb_ena_bacia = dataBase.getSchema("tb_ena_bacia")

    if data_final == None:
        
        s = db.select(tb_ena_bacia.c.id_bacia,
                                tb_ena_bacia.c.dt_ref,
                                tb_ena_bacia.c.vl_mwmed,
                                tb_ena_bacia.c.vl_percent).where(
                            tb_ena_bacia.c.dt_ref>=data_inicial)
    else:
        s = db.select(tb_ena_bacia.c.id_bacia,
                                tb_ena_bacia.c.dt_ref,
                                tb_ena_bacia.c.vl_mwmed,
                                tb_ena_bacia.c.vl_percent).where(
            and_(tb_ena_bacia.c.dt_ref>=data_inicial,
                 tb_ena_bacia.c.dt_ref<data_final))
                                     
    resposta = dataBase.conn.execute(s)
    ena = pd.DataFrame(resposta, columns=['bacia','dt_ref','vl_mwmed','vl_percent'])
    ena['mlt'] = (ena['vl_mwmed'] * 100) / ena['vl_percent']
    ena['mlt'] = ena['mlt'].apply(lambda x: round(x / 1000, 2))
    
    ena['dt_ref'] = ena['dt_ref'].apply(lambda x: x.strftime('%m/%Y'))
    
    ena = ena[['bacia', 'dt_ref', 'mlt']]
    
    tb_bacia = dataBase.getSchema('tb_bacias')
    s = tb_bacia.select()
    resposta = dataBase.conn.execute(s)
    
    cod_bacias = {}
    for r in resposta:
        cod_bacias[r[0]] = r[1]
        
    ena['bacia'] = ena['bacia'].replace(cod_bacias)
    return ena

def get_nw_eolica():

    db_decks = wx_dbClass.db_mysql_master('db_decks')
    db_decks.connect()

    table_energia_nw = db_decks.getSchema('tb_nw_sist_energia')
    subquery = db.select(
        table_energia_nw.c.vl_ano,
        table_energia_nw.c.vl_mes,
        table_energia_nw.c.cd_submercado,
        db.func.max(table_energia_nw.c.dt_deck).label('max_dt_deck')
        ).where(
            table_energia_nw.c.cd_submercado.in_([2,3])
        ).group_by(
            table_energia_nw.c.cd_submercado,
            table_energia_nw.c.vl_ano,
            table_energia_nw.c.vl_mes,
        ).alias('subquery')

    select_query = db.select(
        table_energia_nw.c.cd_submercado,
        table_energia_nw.c.vl_ano,
        table_energia_nw.c.vl_mes,
        table_energia_nw.c.vl_geracao_eol,
        table_energia_nw.c.dt_deck
    ).join(
        subquery,
        (table_energia_nw.c.cd_submercado == subquery.c.cd_submercado) &
        (table_energia_nw.c.vl_ano == subquery.c.vl_ano) &
        (table_energia_nw.c.vl_mes == subquery.c.vl_mes) &
        (table_energia_nw.c.dt_deck == subquery.c.max_dt_deck)
    )
    teste = db_decks.conn.execute(select_query).fetchall()
    return teste
    
def get_eol_mlt_nw(dt_ref:datetime, num_semanas:int=None, data_type:str="diario"):

    if not data_type: data_type = 'diario'

    submercados_eol= {3:'NORDESTE', 2:'SUL'}  

    teste = get_nw_eolica()  

    df_eol_nw = pd.DataFrame(teste, columns=['cd_submercado','vl_ano','vl_mes','vl_geracao_eol','dt_deck'])

    df_eol_nw[['vl_mes','vl_ano']] = df_eol_nw[['vl_mes','vl_ano']].astype('str')
    df_eol_nw['str_submercado'] =  df_eol_nw['cd_submercado'].replace(submercados_eol)
    df_eol_nw['vl_mes'] = df_eol_nw['vl_mes'].apply(lambda x: x.zfill(2))  
    df_eol_nw['date'] = pd.to_datetime(df_eol_nw['vl_mes']+"/"+df_eol_nw['vl_ano'],format = "%m/%Y")
    df_eol_nw = df_eol_nw.set_index('date')

    dt_ini_semana = wx_opweek.getLastSaturday(dt_ref)

    df_eol_nw = df_eol_nw[['str_submercado','vl_geracao_eol']]
    values_eol_nw ={}

    for str_submercado in df_eol_nw['str_submercado'].unique():

        eol_submercado_diario = df_eol_nw[df_eol_nw['str_submercado']==str_submercado][['vl_geracao_eol']]
        eol_submercado_diario =  eol_submercado_diario.resample('1D').mean().ffill()

        if num_semanas:
            eol_submercado_diario = eol_submercado_diario.reset_index()
            if num_semanas ==  0: num_semanas = 1
            dt_fim_semana = dt_ini_semana + datetime.timedelta(days= num_semanas*7)
            eol_submercado_diario = eol_submercado_diario[(eol_submercado_diario['date']>=dt_ini_semana) & (eol_submercado_diario['date']<=dt_fim_semana)]
            eol_submercado_diario = eol_submercado_diario.set_index('date')

        if data_type == 'hr':
            eol_submercado_diario = eol_submercado_diario.resample("1H").mean().ffill()[:-1]
            
        if data_type == 'diario':
            eol_submercado_diario.loc[eol_submercado_diario.index[-1]] =  eol_submercado_diario.loc[eol_submercado_diario.index[-2]]  
            eol_submercado_diario.rename(index={eol_submercado_diario.index[-1]: eol_submercado_diario.index[-1] - datetime.timedelta(hours=1)}, inplace=True)
        
        eol_submercado_diario.index = pd.to_datetime(eol_submercado_diario.index).strftime('%Y-%m-%d %H:%M:%S')
        values_eol_nw[str_submercado] = eol_submercado_diario.to_dict()
        

    return values_eol_nw

def get_anos_disponiveis_rdh():
    db_ons = wx_dbClass.db_mysql_master('db_ons')
    db_ons.connect()
    tb_rdh = db_ons.getSchema('tb_rdh')
    query = db.select(
        db.distinct(db.sql.func.year(tb_rdh.c['dt_referente']))
    ).order_by(db.text('YEAR(dt_referente)'))

    result = db_ons.conn.execute(query)
    result = result.all()
    df = pd.DataFrame(result, columns=['ano'])
    return {'anos':df['ano'].to_list()}

if __name__ == '__main__':

