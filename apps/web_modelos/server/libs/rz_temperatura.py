import sys
import pdb
import datetime
import pandas as pd
import sqlalchemy as db


sys.path.insert(1,"/WX2TB/Documentos/fontes")
from PMO.scripts_unificados.bibliotecas import wx_dbClass

def get_temperatura_obs(dt_ini, dt_fim):
    db_ons = wx_dbClass.db_mysql_master('db_ons')
    db_ons.connect()
    tb_submercado = db_ons.getSchema('tb_submercado')
    tb_temperatura_obs = db_ons.getSchema('tb_temperatura_obs')

    sql_select = db.select(
        tb_submercado.c.str_submercado,
        tb_temperatura_obs.c.dt_referente,
        tb_temperatura_obs.c.vl_temperatura
        ).join( 
                tb_temperatura_obs, 
                tb_temperatura_obs.c.cd_submercado == tb_submercado.c.cd_submercado,
    ).where(
        db.and_(
                tb_temperatura_obs.c.dt_referente >= dt_ini,
                tb_temperatura_obs.c.dt_referente < dt_fim
                )
        )
    values = db_ons.conn.execute(sql_select).fetchall()
    
    columns = ['str_submercado','dt_referente','vl_temperatura']
    df_values = pd.DataFrame(values, columns=columns)
    
    if values:
        df_values['horario'] = pd.to_datetime(df_values['dt_referente']).dt.strftime('%H:%M')
        df_values['data'] = pd.to_datetime(df_values['dt_referente']).dt.strftime('%Y-%m-%d')

    return df_values 

def get_temperatura_prev(dt_deck):
    db_ons = wx_dbClass.db_mysql_master('db_ons')
    db_ons.connect()
    tb_submercado = db_ons.getSchema('tb_submercado')
    tb_prev_carga_temperatura = db_ons.getSchema('tb_prev_carga_temperatura')

    sql_select = db.select(
        tb_submercado.c.str_submercado,
        tb_prev_carga_temperatura.c.dt_referente,
        tb_prev_carga_temperatura.c.vl_temperatura,
        tb_prev_carga_temperatura.c.dt_inicio,
        ).join( 
                tb_prev_carga_temperatura, 
                tb_prev_carga_temperatura.c.cd_submercado == tb_submercado.c.cd_submercado,
    ).where(
        db.and_(
                tb_prev_carga_temperatura.c.dt_inicio == dt_deck,
                )
        )
    values = db_ons.conn.execute(sql_select).fetchall()
    
    columns = ['str_submercado','dt_referente','vl_temperatura','dt_ini']
    df_values = pd.DataFrame(values, columns=columns)
    
    if values:
        df_values['horario'] = pd.to_datetime(df_values['dt_referente']).dt.strftime('%H:%M')
        df_values['data'] = pd.to_datetime(df_values['dt_referente']).dt.strftime('%Y-%m-%d')

    return df_values 

