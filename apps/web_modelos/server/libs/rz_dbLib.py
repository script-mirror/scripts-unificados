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
from middle.message import send_whatsapp_message


sys.path.insert(1,"/WX2TB/Documentos/fontes")
from PMO.scripts_unificados.bibliotecas import wx_dbClass,wx_opweek,wx_dbLib

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")),'.env'))


# Variaveis globais
dateFormat = "%Y/%m/%d"

class WxDataB:

    def __init__(self, dbm='mssql'):
        send_whatsapp_message("debug", "Iniciando conexão com banco de dados " + dbm, None)
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

def get_deck_dessem_renovaveis_eolica(data_deck):
    query_get_rodada = '''SELECT tdr.vl_periodo, tdr.id_submercado, tdr.vl_geracao_uhe, tdr.vl_geracao_ute, 
                        tdr.vl_geracao_cgh, tdr.vl_geracao_pch, tdr.vl_geracao_ufv, tdr.vl_geracao_uee FROM 
                          tb_cadastro_dessem as tcd right join tb_ds_renovaveis as tdr on tcd.id=tdr.id_deck where tcd.dt_referente = %s and tcd.id_fonte = 1'''
    datab = WxDataB('mysqlWx')
    datab.dbDatabase = 'db_decks'
    answer = datab.execute(query_get_rodada, (data_deck))
    return answer

def get_deck_decomp_bloco_pq(dt_inicio_deck):
    datab = WxDataB('mysqlWx')
    datab.dbDatabase = 'db_decks'
    query_get_rodada_id = '''SELECT vl_estagio, vl_patamar, cd_submercado, vl_geracao_eol FROM
            tb_dc_dadger_pq tddp right join tb_cadastro_decomp tcd on tcd.id = tddp.id_deck where tcd.dt_inicio_rv = %s;'''
    answer = datab.execute(query_get_rodada_id, (dt_inicio_deck,))
    return answer

def get_discretizacao_tempo(data_inicial, data_final):
    datab = WxDataB('mysqlWx')
    datab.dbDatabase = 'db_decks'
    query_get_id_rodada = "SELECT dt_inicio_patamar, vl_patamar FROM db_decks.tb_ds_bloco_tm where dt_inicio_patamar >= %s and dt_inicio_patamar <= %s order by dt_inicio_patamar"
    answer = datab.execute(query_get_id_rodada, (data_inicial, data_final))
    return answer



def query_rodadas_do_dia(dt_rodada):
    dataBase = wx_dbClass.db_mysql_master('db_rodadas')
    dataBase.connect()
    tb_cadastro_rodadas = dataBase.getSchema('tb_cadastro_rodadas')
    
    query = db.select(tb_cadastro_rodadas.c.id,tb_cadastro_rodadas.c.str_modelo,tb_cadastro_rodadas.c.hr_rodada,tb_cadastro_rodadas.c.fl_psat,tb_cadastro_rodadas.c.fl_pdp,tb_cadastro_rodadas.c.fl_preliminar)\
            .where(db.and_(tb_cadastro_rodadas.c.dt_rodada == dt_rodada)) 
    answer = dataBase.conn.execute(query)
    return answer.all()



def get_valores_IPDO(dtref):
    
    global dateFormat

    db_ons = wx_dbClass.db_mysql_master('db_ons')
    db_ons.connect()
    tb_carga_ipdo = db_ons.getSchema('tb_carga_ipdo')
 
    if dtref.weekday() == 5:
        data_inicial = dtref
    else:
        while dtref.weekday() != 5:  # 5 representa sábado (0 é segunda-feira e 6 é domingo)
                dtref += datetime.timedelta(days=1)
        data_inicial = dtref - datetime.timedelta(weeks=1)
        while data_inicial.weekday() != 5:
                data_inicial += datetime.timedelta(days=1)

    lista_datas = []
    data_atual = data_inicial
    data_final = datetime.datetime.today()

    while data_atual <= data_final:
            lista_datas.append(data_atual.strftime("%Y-%m-%d"))
            data_atual += datetime.timedelta(days=1)

    dataFormatada = [datetime.datetime.strptime(date, '%Y-%m-%d').date() for date in lista_datas]

    query_get_ipdo_carga = db.select(tb_carga_ipdo.c.dt_referente, tb_carga_ipdo.c.carga_se, tb_carga_ipdo.c.carga_s,tb_carga_ipdo.c.carga_ne,tb_carga_ipdo.c.carga_n).where(db.and_(tb_carga_ipdo.c.dt_referente.in_ (dataFormatada)))
    dt_referente = db_ons.conn.execute(query_get_ipdo_carga).all()
    # criando data frame com a data pesquisada.
    df_carga_ipdo = pd.DataFrame(dt_referente)
    # renomeando colunas dataframe
    df_carga_ipdo = df_carga_ipdo.rename(columns={0:'dt_referente',1:'carga_se',2:'carga_s',3:'carga_ne',4:'carga_n'})
    df_carga_ipdo = df_carga_ipdo.sort_values('dt_referente')
    df_carga_ipdo['dt_referente'] = pd.to_datetime(df_carga_ipdo['dt_referente']).dt.strftime('%Y%m%d')

    # Criando um dicionário vazio para armazenar os resultados referente a cada submercado, colocando o código do submercado como chave
    dicionario_carga_ipdo = {
                    'SE': {},
                    'S': {},
                    'NE': {},
                    'N': {}
    }

    # Percorrendo cada linha do dataframe
    for index, row in df_carga_ipdo.iterrows():  
                    # Adicionando cada valor da coluna do dataframe carga_ipdo dentro da chave da data da carga de cada submercado
                    data = row['dt_referente']
                    dicionario_carga_ipdo['SE'][data] = row['carga_se']
                    dicionario_carga_ipdo['S'][data] = row['carga_s']
                    dicionario_carga_ipdo['NE'][data] = row['carga_ne']
                    dicionario_carga_ipdo['N'][data] = row['carga_n']

    # Criando um dicionário vazio para armazenar chaves das datas formatadas e todos os valores.
    carga_ipdo_dataFormatada = {}
    for chave, valor in dicionario_carga_ipdo.items():
                    chaves_formatadas = {}
                    for data, carga in valor.items():
                                    chaves_formatadas[data] = carga
                    chave_formatada = chave  # Mantém a chave original como string
                    carga_ipdo_dataFormatada[chave_formatada] = chaves_formatadas
 
    ipdo_Verificado= {}
    ipdo_Verificado['ipdo_verificado'] = carga_ipdo_dataFormatada
    return ipdo_Verificado

def calculate_weighted_daily_mean(group):
    """
    Calculate weighted mean for a group of time series data based on actual time intervals.
    Each record is weighted by the time period it represents (time until next record).
    """
    if len(group) <= 1:
        return group['vl_carga'].iloc[0] if len(group) == 1 else 0
    
    # Sort by datetime to ensure correct order
    group = group.sort_values('dataHora').copy()
    
    # Calculate time differences between consecutive records
    # Shift time differences up so each record gets the time until the NEXT record
    group['time_diff'] = group['dataHora'].shift(-1) - group['dataHora']
    
    # For the last record, calculate time until start of next day (00:00:00)
    if len(group) > 0:
        last_datetime = group['dataHora'].iloc[-1]
        # Calculate time until next day at 00:00:00
        next_day = last_datetime.normalize() + pd.Timedelta(days=1)
        time_to_next_day = next_day - last_datetime
        group.iloc[-1, group.columns.get_loc('time_diff')] = time_to_next_day
    
    # Convert time differences to hours (weights)
    group['hours_weight'] = group['time_diff'].dt.total_seconds() / 3600
    
    # Handle edge case: if all time differences are NaN or 0
    if group['hours_weight'].isna().all() or group['hours_weight'].sum() == 0:
        return group['vl_carga'].mean()
    
    # Calculate weighted mean
    weighted_sum = (group['vl_carga'] * group['hours_weight']).sum()
    total_weight = group['hours_weight'].sum()
    
    return weighted_sum / total_weight

def get_previsao_dessem():
    """
    Pega previsão IPDO:
    - deck mais recente completo
    - se faltarem registros de hoje, busca somente hoje no deck anterior
    Agrega por dia e sigla usando média ponderada baseada nos intervalos de tempo.
    """
    # --- setup DB ---
    db_decks = wx_dbClass.db_mysql_master('db_decks')
    db_decks.connect()
    tb_ds_carga   = db_decks.getSchema('tb_ds_carga')
    tb_submercado = db_decks.getSchema('tb_submercado')

    hoje = datetime.date.today()

    # 1) deck mais recente
    max_deck_id = db.select(func.max(tb_ds_carga.c.id_deck)).scalar_subquery()

    # 2) deck anterior ao mais recente
    prev_deck_id = (
        db.select(func.max(tb_ds_carga.c.id_deck))
          .where(tb_ds_carga.c.id_deck < max_deck_id)
          .scalar_subquery()
    )

    # helper para montar query base
    def make_query(deck_id_subq, only_today=False):
        q = (
            db.select(
                tb_submercado.c.str_sigla.label('sigla'),
                tb_ds_carga.c.dataHora,
                tb_ds_carga.c.vl_carga
            )
            .select_from(
                tb_ds_carga.join(
                    tb_submercado,
                    tb_ds_carga.c.cd_submercado == tb_submercado.c.cd_submercado
                )
            )
            .where(tb_ds_carga.c.id_deck == deck_id_subq)
        )
        if only_today:
            # filtra somente registros de hoje
            q = q.where(func.date(tb_ds_carga.c.dataHora) == hoje)
        return q

    # 3) busca todos os dados do deck mais recente
    rows_max   = db_decks.conn.execute(make_query(max_deck_id)).all()
    df_max     = pd.DataFrame(rows_max, columns=['sigla','dataHora','vl_carga'])
    df_max['day'] = df_max['dataHora'].dt.date.astype(str)

    # 4) checa se há registros para hoje no deck mais recente
    existe_hoje = (df_max['day'] == str(hoje)).any()

    # 5) se não existir, busca só hoje no deck anterior e anexa
    if not existe_hoje:
        rows_prev = db_decks.conn.execute(make_query(prev_deck_id, only_today=True)).all()
        df_prev   = pd.DataFrame(rows_prev, columns=['sigla','dataHora','vl_carga'])
        if not df_prev.empty:
            df_prev['day'] = df_prev['dataHora'].dt.date.astype(str)
            # concatena mantendo ambos
            df_max = pd.concat([df_max, df_prev], ignore_index=True)

    # 6) agrupa e calcula média diária ponderada por sigla
    daily_avg = (
        df_max
        .groupby(['sigla', 'day'])
        .apply(calculate_weighted_daily_mean)
        .reset_index()
        .rename(columns={0: 'avg_vl_carga'})
    )

    # 7) monta dict aninhado por sigla
    nested = (
        daily_avg
        .groupby("sigla")
        .apply(lambda grp: grp[['day','avg_vl_carga']].to_dict('records'))
        .to_dict()
    )

    return nested

def get_valores_IPDO_previsao(dtref):
    
    global dateFormat

    db_decks = wx_dbClass.db_mysql_master('db_decks')
    db_decks.connect()
    tb_ds_carga = db_decks.getSchema('tb_ds_carga')
    tb_cadastro_dessem = db_decks.getSchema('tb_cadastro_dessem')
    db_ons = wx_dbClass.db_mysql_master('db_ons')
    db_ons.connect()
    vw_perfil_semanal = db_ons.getSchema('vw_perfil_semanal')
    tb_carga_ipdo = db_ons.getSchema('tb_carga_ipdo')

    data_ontem = datetime.datetime.today() - datetime.timedelta(days=1)
    dataOntemFormatada = data_ontem.date()
    
    query_get_ipdo_carga = db.select(tb_carga_ipdo.c.dt_referente, tb_carga_ipdo.c.carga_se, tb_carga_ipdo.c.carga_s,tb_carga_ipdo.c.carga_ne,tb_carga_ipdo.c.carga_n).where(tb_carga_ipdo.c.dt_referente == dataOntemFormatada)
    dt_referente = db_ons.conn.execute(query_get_ipdo_carga).all()
    
    if dt_referente == []:
        #  query para pegar id cargaDessem conforme ultimo dado inserido na cargaIPDO
        query_get_id_dessem_carga = db.select(tb_cadastro_dessem.c.id).where(tb_cadastro_dessem.c.dt_referente == dataOntemFormatada, tb_cadastro_dessem.c.id_fonte == 2)
        id_cargaDessem = db_decks.conn.execute(query_get_id_dessem_carga).all()
        id_deck = id_cargaDessem[0][0]  
    else:
        #  pegando o ultimo id_deck que esta no banco de dados
        id_deck = db.select([func.max(tb_ds_carga.c.id_deck)])

    #  query para pegar os valores da carga dessem que recebemos da CCEE
    query_get_dessem_carga = db.select(tb_ds_carga.c.id_deck, tb_ds_carga.c.cd_submercado, tb_ds_carga.c.dataHora,tb_ds_carga.c.vl_carga).where(tb_ds_carga.c.id_deck == id_deck)
    valores_dessem = db_decks.conn.execute(query_get_dessem_carga).all()

    # Criando dataframe com nomes das colunas referente ao resultado dos perfis semanais do mes atual e do proximo mes.
    colunas = ["id_deck", "cd_submercado", "dataHora", "vl_carga"]
    dfvalores_dessem = pd.DataFrame(valores_dessem, columns=colunas)
    df_dessem = pd.DataFrame(dfvalores_dessem)

    # Mapeando os números para os códigos de submercado
    submercado_mapping = {
            1: "SE",
            2: "S",
            3: "NE",
            4: "N"
    }

    df_dessem['cd_submercado'] = df_dessem['cd_submercado'].map(submercado_mapping)
    df_dessem['dataHora'] = pd.to_datetime(df_dessem['dataHora'])
    df_dessem['dataHora'] = df_dessem['dataHora'].dt.date


    # Calcular a média da coluna 'vl_carga' com base nas colunas 'cd_submercado' e 'dataHora'
    media_por_submercado = df_dessem.groupby(['cd_submercado', 'dataHora'])['vl_carga'].mean()

    # Reformatar o resultado em um novo DataFrame
    df_media_por_submercado_dessem = media_por_submercado.reset_index()
 
    # criando DataFrame com a coluna dataHora em string
    df_media_por_submercado_dessem['dataHora'] = pd.to_datetime(df_media_por_submercado_dessem['dataHora'])
    df_media_por_submercado_dessem['dataHora'] = df_media_por_submercado_dessem['dataHora'].dt.strftime("%Y%m%d")

    # Criando um dicionário vazio para armazenar os resultados referente a cada submercado, colocando o código do submercado como chave
    dicionario_carga_dessem = {
                    'SE': {},
                    'S': {},
                    'NE': {},
                    'N': {}
    }

    # adicionando valores ao dicionario novo, considerando cada linha e seu index sem repetir.
    for index, row in df_media_por_submercado_dessem.iterrows():
        submercado = row['cd_submercado']
        data = row['dataHora']
        carga = int(row['vl_carga'])
  
        if data not in dicionario_carga_dessem[submercado]:
            dicionario_carga_dessem[submercado][data] = carga

            

    # Pegando a ultima data dos ultimos dados que recebemos da cargaDessem
    ultimaData = list(dicionario_carga_dessem['SE'].keys())[-1]
    ultimaData = datetime.datetime.strptime(ultimaData, '%Y%m%d').date()

    # Com a ultima data, estou pegando o numero do mes atual.
    ultimoMes = ultimaData.month
 
    # Pegando dos proximos 3 meses 
    proximosMesesDesejado = ultimoMes + 3
 
    # Fazendo uma query para pegar os resultados baseado no mesmo atual da data pesquisada e do proximo mes.
    query_get_perfil_semanal = db.select(vw_perfil_semanal.c.mes,vw_perfil_semanal.c.nome_dia_semana,vw_perfil_semanal.c.resultado_carga_se,vw_perfil_semanal.c.resultado_carga_s,vw_perfil_semanal.c.resultado_carga_ne,vw_perfil_semanal.c.resultado_carga_n).where(between(vw_perfil_semanal.c.mes, ultimoMes, proximosMesesDesejado))

    resultadosPerfilSemanal = db_ons.conn.execute(query_get_perfil_semanal).all()
    # Criando dataframe com nomes das colunas referente ao resultado dos perfis semanais do mes atual e do proximo mes.
    colunas = ["mes", "nome_dia_semana", "resultado_carga_se", "resultado_carga_s", "resultado_carga_ne", "resultado_carga_n"]
    dfResultadosPerfilSemanal = pd.DataFrame(resultadosPerfilSemanal, columns=colunas)

    # pegando os ultimos valores referente a cada submercado, considerando a ultima dada do dicionario
    ultima_data = {chave: {max(dicionario, key=int): dicionario[max(dicionario, key=int)]} for chave, dicionario in dicionario_carga_dessem.items()}

    # criando DataFrame com a ultima linha dos valores do dessem
    df_dessem_ultimaData = pd.DataFrame(ultima_data)
    df_dessem_ultimaData = df_dessem_ultimaData.reset_index()
    df_dessem_ultimaData.rename(columns = {"index":"dt_referente", "SE":"carga_se","S":"carga_s","NE":"carga_ne","N":"carga_n"}, inplace = True)
    df_dessem_ultimaData['dt_referente'] = pd.to_datetime(df_dessem_ultimaData['dt_referente'])

    # Criar as colunas mes e nome_dia_semana
    df_dessem_ultimaData['mes'] = df_dessem_ultimaData['dt_referente'].dt.month
    df_dessem_ultimaData['dt_referente'] = pd.to_datetime(df_dessem_ultimaData['dt_referente'], format='%Y%m%d')
    df_dessem_ultimaData['nome_dia_semana'] = df_dessem_ultimaData['dt_referente'].dt.weekday

    # Selecionar as colunas desejadas no novo dataframe
    novo_dfcargaIpdo = df_dessem_ultimaData[['mes', 'nome_dia_semana', 'carga_se', 'carga_s', 'carga_ne', 'carga_n']].copy()

    # capturando a data de inicio
    dt_ref = df_dessem_ultimaData['dt_referente']
    dt_referente = pd.Series(pd.to_datetime(dt_ref))
    dt_referente_str = dt_referente.dt.strftime("%Y-%m-%d").values[0]
    start_date = dt_referente_str


    # pegando a linha correta do perfil semanal, baseado no dia da semana e mes.
    indices_dfResultadosPerfilSemanal = dfResultadosPerfilSemanal[(dfResultadosPerfilSemanal['mes'] == df_dessem_ultimaData['mes'].iloc[0]) & (dfResultadosPerfilSemanal['nome_dia_semana'] == df_dessem_ultimaData['nome_dia_semana'].iloc[0])].index

    # removendo a linha do indice que é anterior ao pesquisado.
    indice_resultado = indices_dfResultadosPerfilSemanal[0]
    dfResultadosPerfilSemanal_filtrado = dfResultadosPerfilSemanal.loc[indice_resultado:]

    # Criar o terceiro dataframe
    df_ipdo_Previsao = pd.DataFrame(columns=['dt_referente', 'carga_se', 'carga_s', 'carga_ne', 'carga_n'])

    start_date = pd.to_datetime(start_date)  # Data inicial (ajuste conforme sua necessidade)
    # Criando dicionario vazio para recever cargas de previsao do IPDO:
    ipdo_Previsao = {}

    
    # Fazendo a médida baseado nos valores da semana e dia do mes, considerando o perfil semanal para o calculo.
    for i in range(len(dfResultadosPerfilSemanal_filtrado) - 1):	
            row = novo_dfcargaIpdo.copy()
            carga_se = row['carga_se'].iloc[0] / dfResultadosPerfilSemanal_filtrado['resultado_carga_se'].iloc[i]
            carga_se *= dfResultadosPerfilSemanal_filtrado['resultado_carga_se'].iloc[i + 1]

            carga_s = row['carga_s'].iloc[0] / dfResultadosPerfilSemanal_filtrado['resultado_carga_s'].iloc[i]
            carga_s *= dfResultadosPerfilSemanal_filtrado['resultado_carga_s'].iloc[i + 1]

            carga_ne = row['carga_ne'].iloc[0] / dfResultadosPerfilSemanal_filtrado['resultado_carga_ne'].iloc[i]
            carga_ne *= dfResultadosPerfilSemanal_filtrado['resultado_carga_ne'].iloc[i + 1]

            carga_n = row['carga_n'].iloc[0] / dfResultadosPerfilSemanal_filtrado['resultado_carga_n'].iloc[i]
            carga_n *= dfResultadosPerfilSemanal_filtrado['resultado_carga_n'].iloc[i + 1]

            dt_referente = start_date + pd.DateOffset(days=i+1)  # Calcula a data referente

            row['carga_se'] = carga_se
            row['carga_s'] = carga_s
            row['carga_ne'] = carga_ne
            row['carga_n'] = carga_n
            row['dt_referente'] = dt_referente

            novo_dfcargaIpdo = row.copy()

            df_ipdo_Previsao = df_ipdo_Previsao.append(row, ignore_index=True)
   
    # Exibir o DataFrame final
    df_ipdo_Previsao = df_ipdo_Previsao.drop(['mes', 'nome_dia_semana'], axis=1)
    df_ipdo_Previsao['dt_referente'] = pd.to_datetime(df_ipdo_Previsao['dt_referente']).dt.strftime("%Y%m%d")
    df_ipdo_Previsao = df_ipdo_Previsao.rename(columns={'carga_se': 'SE', 'carga_s': 'S', 'carga_ne': 'NE', 'carga_n': 'N'})
    df_ipdo_Previsao_arredondado = df_ipdo_Previsao.round(0).astype(int)
    dia_da_semana = start_date.weekday()  # Obtém o dia da semana da data fornecida (0 = segunda-feira, 6 = domingo)
    dias_para_proximo_sabado = (5 - dia_da_semana) % 7  # Calcula a quantidade de dias até o próximo sábado (5 = sábado)
    prox_sabado = start_date + datetime.timedelta(days=dias_para_proximo_sabado)  # Adiciona os dias necessários para chegar ao próximo sábado
    # Adiciona mais 3 semanas para chegar ao próximo 4º sábado
    prox_4o_sabado = prox_sabado + datetime.timedelta(weeks=3)
    # Transforma a data proximo 4° sabado em um numero inteiro para poder buscar na dataframe.
    data_final_filtro = prox_4o_sabado.year * 10000 + prox_4o_sabado.month * 100 + prox_4o_sabado.day
    # Removendo as datas que passam do 4 sabado eletrico de previsao.

    df_ipdo_Previsao_4semanasEletricas = df_ipdo_Previsao_arredondado[df_ipdo_Previsao_arredondado['dt_referente'] <= data_final_filtro]
    # convertendo data que estava em INT para string "yyyymmdd"
    df_ipdo_Previsao_4semanasEletricas['dt_referente'] = df_ipdo_Previsao_4semanasEletricas['dt_referente'].astype(str).str.zfill(8)

    #Criando dicionario que sera fornecido para javascript com as previsoes das cargas.
    ipdo_Previsao = {
            'ipdo_Previsao': {
                    'SE': dict(zip(df_ipdo_Previsao_4semanasEletricas['dt_referente'], df_ipdo_Previsao_4semanasEletricas['SE'])),
                    'S': dict(zip(df_ipdo_Previsao_4semanasEletricas['dt_referente'], df_ipdo_Previsao_4semanasEletricas['S'])),
                    'NE': dict(zip(df_ipdo_Previsao_4semanasEletricas['dt_referente'], df_ipdo_Previsao_4semanasEletricas['NE'])),
                    'N': dict(zip(df_ipdo_Previsao_4semanasEletricas['dt_referente'], df_ipdo_Previsao_4semanasEletricas['N']))
            }
    }
 
    dfdicionario_carga_dessem= pd.DataFrame(dicionario_carga_dessem)
    dfdicionario_carga_dessem['dt_referente'] = dfdicionario_carga_dessem.index
    dfdicionario_carga_dessem.reset_index(drop=True, inplace=True)
    dfdicionario_carga_dessem['dt_referente'] = pd.to_datetime(dfdicionario_carga_dessem['dt_referente'], format='%Y%m%d')

    dfipdo_Previsao = pd.DataFrame(ipdo_Previsao['ipdo_Previsao'])
    dfipdo_Previsao['dt_referente'] = dfipdo_Previsao.index
    dfipdo_Previsao.reset_index(drop=True, inplace=True)
    dfipdo_Previsao['dt_referente'] = pd.to_datetime(dfipdo_Previsao['dt_referente'], format='%Y%m%d')

    # Juntar os dois dataframes usando concat e ordenar pela coluna "dt_referente"
    combined_df = pd.concat([dfdicionario_carga_dessem, dfipdo_Previsao]).sort_values(by="dt_referente").reset_index(drop=True)
    combined_df['dt_referente'] = pd.to_datetime(combined_df['dt_referente']).dt.strftime('%Y%m%d')

    #Criando dicionario que sera fornecido para javascript com as previsoes das cargas.
    ipdo_Previsao_final = {
            'ipdo_Previsao': {
                    'SE': dict(zip(combined_df['dt_referente'], combined_df['SE'])),
                    'S': dict(zip(combined_df['dt_referente'], combined_df['S'])),
                    'NE': dict(zip(combined_df['dt_referente'], combined_df['NE'])),
                    'N': dict(zip(combined_df['dt_referente'], combined_df['N']))
            }
    }
    return ipdo_Previsao_final

def juntaTabela_IPDO(ipdo_Verificado,ipdo_Previsao):
  
        # Leitura do dicionario e criacao do dataframe
        dfipdo_verificado = pd.DataFrame(ipdo_Verificado['ipdo_verificado'])
        dfipdo_Previsao = pd.DataFrame(ipdo_Previsao['ipdo_Previsao'])
        dfipdo_verificado['dt_referente'] = dfipdo_verificado.index
        dfipdo_Previsao['dt_referente'] = dfipdo_Previsao.index
        dfipdo_verificado.reset_index(drop=True, inplace=True)
        dfipdo_Previsao.reset_index(drop=True, inplace=True)
        dfipdo_verificado['dt_referente'] = pd.to_datetime(dfipdo_verificado['dt_referente'], format='%Y%m%d')
        dfipdo_Previsao['dt_referente'] = pd.to_datetime(dfipdo_Previsao['dt_referente'], format='%Y%m%d')
        # Juntar os dois dataframes usando concat e ordenar pela coluna "dt_referente"
        combined_df = pd.concat([dfipdo_verificado, dfipdo_Previsao]).sort_values(by="dt_referente").reset_index(drop=True)
        combined_df['dt_referente'] = pd.to_datetime(combined_df['dt_referente']).dt.strftime('%Y%m%d')

        # Criando dicionario que sera fornecido para javascript com as previsoes das cargas.
        ipdo_verificado_previsao = {
                'ipdo_verificado_previsao': {
                        'N': dict(zip(combined_df['dt_referente'], combined_df['N'])),
                  'NE': dict(zip(combined_df['dt_referente'], combined_df['NE'])),
                 'S': dict(zip(combined_df['dt_referente'], combined_df['S'])),
               'SE': dict(zip(combined_df['dt_referente'], combined_df['SE']))
                }
        }
  
        return ipdo_verificado_previsao

def get_valores_DECOMP(dtref):
    
    global dateFormat
    dtFutura = dtref + datetime.timedelta(days = 720)

    db_decks = wx_dbClass.db_mysql_master('db_decks')
    db_decks.connect()
    tb_cadastro_decomp = db_decks.getSchema('tb_cadastro_decomp')
    tb_dc_dadger_dp = db_decks.getSchema('tb_dc_dadger_dp')
    tb_dc_patamar = db_decks.getSchema('tb_dc_patamar')

    data_inicial = dtref - datetime.timedelta(weeks=16)
    while data_inicial.weekday() != 5:
            data_inicial += datetime.timedelta(days=1)

    lista_datas = []
    data_atual = data_inicial

    while data_atual <= dtFutura:
            lista_datas.append(data_atual.strftime("%Y-%m-%d"))
            data_atual += datetime.timedelta(days=1)

    dataFormatada = [datetime.datetime.strptime(date, '%Y-%m-%d').date() for date in lista_datas]
    dataFormatadaPrimeiraPosicao = dataFormatada[0]
    query_get_cadastro_decomp = db.select(tb_cadastro_decomp.c.id, tb_cadastro_decomp.c.dt_inicio_rv).where(db.and_(tb_cadastro_decomp.c.dt_inicio_rv.in_ (dataFormatada)))
    id_cadastro_decomp = db_decks.conn.execute(query_get_cadastro_decomp).all()

    id_cadastro_decomp = sorted(id_cadastro_decomp, key=lambda x: x[1])
    df_id_cadastro_decomp = pd.DataFrame(id_cadastro_decomp)
    linhasDataFrame = len(df_id_cadastro_decomp)
    if linhasDataFrame >= 6:
        df_id_cadastro_decomp = df_id_cadastro_decomp.drop(0)
    else:
        df_id_cadastro_decomp
    # Renomeando dataframe que estava com numeros nas colunas.
    df_id_cadastro_decomp = df_id_cadastro_decomp.rename(columns={0:'id',1:'dt_inicio_rv'})
    listaId = list(df_id_cadastro_decomp['id'])
    # Query para pegar valors do patamar referente ao cadastro decomp pesquisado.
    query_get_patamar = db.select(tb_dc_patamar.c.id_deck,tb_dc_patamar.c.ip,tb_dc_patamar.c.hr_p1,tb_dc_patamar.c.hr_p2,tb_dc_patamar.c.hr_p3).where(db.and_(tb_dc_patamar.c.id_deck.in_(listaId),tb_dc_patamar.c.ip == 1))
    patamar_decomp = db_decks.conn.execute(query_get_patamar).all()
 
    # Query para pegar valors do patamar referente ao cadastro decomp pesquisado.
    query_get_patamar_Mes_Atual = db.select(tb_dc_patamar.c.id_deck,tb_dc_patamar.c.ip,tb_dc_patamar.c.hr_p1,tb_dc_patamar.c.hr_p2,tb_dc_patamar.c.hr_p3).where(tb_dc_patamar.c.id_deck == listaId[-1])
    patamar_decomp_Mes_Atual = db_decks.conn.execute(query_get_patamar_Mes_Atual).all()
 
    # Query para pegar carga decomp.
    query_get_carga_decomp = db.select(tb_dc_dadger_dp.c.id_deck,tb_dc_dadger_dp.c.ip,tb_dc_dadger_dp.c.vl_carga_pesada,tb_dc_dadger_dp.c.vl_carga_media,tb_dc_dadger_dp.c.vl_carga_leve,tb_dc_dadger_dp.c.cd_submercado).where(db.and_(tb_dc_dadger_dp.c.id_deck.in_(listaId[:-1]),tb_dc_dadger_dp.c.ip == 1))
    carga_decomp = db_decks.conn.execute(query_get_carga_decomp).all()
    
    # Query para pegar ultimos valores da cargaDecomp do mes atual.
    query_get_carga_decomp_Mes_Atual = db.select(tb_dc_dadger_dp.c.id_deck,tb_dc_dadger_dp.c.ip,tb_dc_dadger_dp.c.vl_carga_pesada,tb_dc_dadger_dp.c.vl_carga_media,tb_dc_dadger_dp.c.vl_carga_leve,tb_dc_dadger_dp.c.cd_submercado).where(tb_dc_dadger_dp.c.id_deck == listaId[-1])
    carga_decomp_Mes_Atual = db_decks.conn.execute(query_get_carga_decomp_Mes_Atual).all()
  
    
  
    # transformando os valores de carga decomp em dataframe.
    df_carga_decomp = pd.DataFrame(carga_decomp)
    # transformando os valores de carga decomp em dataframe.
    df_patamar_decomp = pd.DataFrame(patamar_decomp)
 
    # transformando os valores de carga decomp em dataframe.
    df_carga_decomp_Mes_Atual = pd.DataFrame(carga_decomp_Mes_Atual)
    # transformando os valores de carga decomp em dataframe.
    df_patamar_decomp_Mes_Atual = pd.DataFrame(patamar_decomp_Mes_Atual)


    # Renomeando dataframe que estava com numeros nas colunas.
    df_carga_decomp = df_carga_decomp.rename(columns={0:'id_deck',1:'ip',2:'vl_carga_pesada',3:'vl_carga_media',4:'vl_carga_leve',5:'cd_submercado'})
    # Renomeando dataframe que estava com numeros nas colunas.
    df_patamar_decomp = df_patamar_decomp.rename(columns={0:'id_deck',1:'ip',2:'hr_p1',3:'hr_p2',4:'hr_p3'})
    # Combinando os dataframes
    df_final_cargaDecomp = pd.merge(df_carga_decomp, df_patamar_decomp, on=['id_deck', 'ip'])
    # Mapeando os valores do submercado
    submercado_mapping = {1: "SE", 2: "S", 3: "NE", 4: "N"}
    df_final_cargaDecomp['cd_submercado'] = df_final_cargaDecomp['cd_submercado'].map(submercado_mapping)
 
    
    # Renomeando dataframe que estava com numeros nas colunas.
    df_carga_decomp_Mes_Atual = df_carga_decomp_Mes_Atual.rename(columns={0:'id_deck',1:'ip',2:'vl_carga_pesada',3:'vl_carga_media',4:'vl_carga_leve',5:'cd_submercado'})
    # Renomeando dataframe que estava com numeros nas colunas.
    df_patamar_decomp_Mes_Atual = df_patamar_decomp_Mes_Atual.rename(columns={0:'id_deck',1:'ip',2:'hr_p1',3:'hr_p2',4:'hr_p3'})
    # Combinando os dataframes
    df_final_carga_decomp_Mes_Atual = pd.merge(df_carga_decomp_Mes_Atual, df_patamar_decomp_Mes_Atual, on=['id_deck', 'ip'])
    # Mapeando os valores do submercado
    submercado_mapping = {1: "SE", 2: "S", 3: "NE", 4: "N"}
    df_final_carga_decomp_Mes_Atual['cd_submercado'] = df_final_carga_decomp_Mes_Atual['cd_submercado'].map(submercado_mapping)
    
    # Calculando os valores para cada linha
    df_final_cargaDecomp['valor_calculado'] = (
            (df_final_cargaDecomp['vl_carga_pesada'] * df_final_cargaDecomp['hr_p1']) +
            (df_final_cargaDecomp['vl_carga_media'] * df_final_cargaDecomp['hr_p2']) +
            (df_final_cargaDecomp['vl_carga_leve'] * df_final_cargaDecomp['hr_p3'])
    ) / 168
    df_final_cargaDecomp = df_final_cargaDecomp[['id_deck','cd_submercado','valor_calculado']]
 
        # Calculando os valores para cada linha
    df_final_carga_decomp_Mes_Atual['valor_calculado'] = (
            (df_final_carga_decomp_Mes_Atual['vl_carga_pesada'] * df_final_carga_decomp_Mes_Atual['hr_p1']) +
            (df_final_carga_decomp_Mes_Atual['vl_carga_media'] * df_final_carga_decomp_Mes_Atual['hr_p2']) +
            (df_final_carga_decomp_Mes_Atual['vl_carga_leve'] * df_final_carga_decomp_Mes_Atual['hr_p3'])
    ) / 168
    df_final_carga_decomp_Mes_Atual = df_final_carga_decomp_Mes_Atual[['id_deck','cd_submercado','valor_calculado']]

    # Mesclando os DataFrames
    df_resultado_decomp = df_final_cargaDecomp.merge(df_id_cadastro_decomp, left_on='id_deck', right_on='id', how='left')
    # Removendo a coluna 'id' desnecessária
    df_resultado_decomp = df_resultado_decomp.drop(['id_deck','id'], axis=1)
    # alteranfo formato da data para yyyymmdd.
    df_resultado_decomp['dt_inicio_rv'] = pd.to_datetime(df_resultado_decomp['dt_inicio_rv']).dt.strftime('%Y%m%d')
    df_resultado_decomp['valor_calculado'] = df_resultado_decomp['valor_calculado'].round(0).astype(int)
 
    # Mesclando os DataFrames
    df_resultado_decomp_Mes_Atual = df_final_carga_decomp_Mes_Atual.merge(df_id_cadastro_decomp, left_on='id_deck', right_on='id', how='left')
    # Removendo a coluna 'id' desnecessária
    df_resultado_decomp_Mes_Atual = df_resultado_decomp_Mes_Atual.drop(['id_deck','id'], axis=1)
    # alteranfo formato da data para yyyymmdd.
    df_resultado_decomp_Mes_Atual['dt_inicio_rv'] = pd.to_datetime(df_resultado_decomp_Mes_Atual['dt_inicio_rv']).dt.strftime('%Y%m%d')
    df_resultado_decomp_Mes_Atual['valor_calculado'] = df_resultado_decomp_Mes_Atual['valor_calculado'].round(0).astype(int)
    # removendo as ultimas 4 linhas do dataframe para remover o valor da carga horaria do mes
    df_resultado_decomp_Mes_Atual = df_resultado_decomp_Mes_Atual[:-4]

    df_resultado_decomp_Mes_Atual['dt_inicio_rv'] = pd.to_datetime(df_resultado_decomp_Mes_Atual['dt_inicio_rv'],format='%Y%m%d')

    # Converter a coluna 'dt_inicio_rv' para o tipo datetime
    df_resultado_decomp_Mes_Atual['dt_inicio_rv'] = pd.to_datetime(df_resultado_decomp_Mes_Atual['dt_inicio_rv'], format='%Y%m%d')

    # Atualizar as datas usando uma função lambda, pegando a cada 4 linhas e trocando pela nova data.
    df_resultado_decomp_Mes_Atual['dt_inicio_rv'] = df_resultado_decomp_Mes_Atual.groupby(df_resultado_decomp_Mes_Atual.index // 4)['dt_inicio_rv'].apply(lambda group: group + pd.DateOffset(days=7 * group.name))

    # Converter a coluna 'dt_inicio_rv' de volta para strings no formato 'YYYYMMDD'
    df_resultado_decomp_Mes_Atual['dt_inicio_rv'] = df_resultado_decomp_Mes_Atual['dt_inicio_rv'].dt.strftime('%Y%m%d')
 
    # concatenando os dois dataframes para resultar em apenas um com todas as datas.
    df_resultadoFinal = pd.concat([df_resultado_decomp, df_resultado_decomp_Mes_Atual],ignore_index=True)

    decomp = {
            'decomp': {
                    'N': df_resultadoFinal.groupby('cd_submercado').get_group('N').set_index('dt_inicio_rv')['valor_calculado'].to_dict(),
                    'NE': df_resultadoFinal.groupby('cd_submercado').get_group('NE').set_index('dt_inicio_rv')['valor_calculado'].to_dict(),
                    'S': df_resultadoFinal.groupby('cd_submercado').get_group('S').set_index('dt_inicio_rv')['valor_calculado'].to_dict(),
                    'SE': df_resultadoFinal.groupby('cd_submercado').get_group('SE').set_index('dt_inicio_rv')['valor_calculado'].to_dict()
            }
    }

    return decomp,dataFormatadaPrimeiraPosicao

def get_valores_Newave(dtref):
    global dateFormat
    db_decks = wx_dbClass.db_mysql_master('db_decks')
    db_decks.connect()
    tb_cadastro_newave = db_decks.getSchema('tb_cadastro_newave')
    tb_nw_carga = db_decks.getSchema('newave_previsoes_cargas')	

    # query para pegar o ultimo ID da tabela cadastro newave para poder pegar as ultimas cargas da tabela carga newave.
    query_get_ultimo_id_cadastro_newave = (db.select([tb_cadastro_newave.c.id, tb_cadastro_newave.c.dt_inicio_rv]).where(db.func.extract('year', tb_cadastro_newave.c.dt_inicio_rv) == dtref.year).order_by(desc(tb_cadastro_newave.c.dt_inicio_rv)).limit(1))
    ultimo_id_cadastro_carga_newave = db_decks.conn.execute(query_get_ultimo_id_cadastro_newave).scalar()

    # capturando o mes da data que foi passada como parametro.
    data_referencia = pd.to_datetime(dtref)
    data_referencia_mes = data_referencia.month
    data_Anterior_quatroSemanas = data_referencia - datetime.timedelta(weeks=4)
    data_referencia_quatroSemanas = data_Anterior_quatroSemanas.month
     # comparando data selecionada com data 4 semanas anteriores, para ver se é do mesmo mes.
    if data_referencia_quatroSemanas == data_referencia_mes:
        data_referencia_mes = data_referencia_mes
    else:
        data_referencia_mes = data_referencia_quatroSemanas
  # criando uma variavel id para fazer a query e buscar o ultimo id cadastrado na tabela carga newave.
    id = ultimo_id_cadastro_carga_newave
    query_get_newave_carga = db.select(tb_nw_carga.c.dt_referente,tb_nw_carga.c.vl_energia_total,tb_nw_carga.c.cd_submercado).where(db.and_(tb_nw_carga.c.id_deck == id))
    carga_newave = db_decks.conn.execute(query_get_newave_carga).all()
    # criando data frame com a data pesquisada.
    carga_newaveDF = pd.DataFrame(carga_newave)
    # renomeando colunas do dataframe 
    carga_newaveDF = carga_newaveDF.rename(columns={0:'dt_referente',1:'vl_energia_total',2:'cd_submercado'})
    # Convertendo a coluna 'dt_referente' para o tipo datetime
    carga_newaveDF['dt_referente'] = pd.to_datetime(carga_newaveDF['dt_referente'])
    # Extrair o mês da coluna 'dt_referente'
    carga_newaveDF['mes'] = carga_newaveDF['dt_referente'].dt.month
    carga_newaveDF_filtrado = carga_newaveDF[carga_newaveDF['dt_referente'] >= dtref]
    # alteranfo formato da data para yyyymmdd.
    carga_newaveDF_filtrado['dt_referente'] = pd.to_datetime(carga_newaveDF_filtrado['dt_referente']).dt.strftime('%Y%m%d')
    # Crie o dicionário vazio para receber os valores das cargas e datas.
    dicionario_carga_Newave = {}
    # Percorra o dataframe e adicione os valores ao dicionário criado
    for index, row in carga_newaveDF_filtrado.iterrows():
            submercado = None
            if row['cd_submercado'] == 1:
                    submercado = 'SE'
            elif row['cd_submercado'] == 2:
                    submercado = 'S'
            elif row['cd_submercado'] == 3:
                    submercado = 'NE'
            elif row['cd_submercado'] == 4:
                    submercado = 'N'
            data_referencia = row['dt_referente']
            valor_carga = row['vl_energia_total']
            if submercado not in dicionario_carga_Newave:
                    dicionario_carga_Newave[submercado] = {}  # Se não existir, crie um dicionário interno
            dicionario_carga_Newave[submercado][data_referencia] = valor_carga
            
    # Criando um dicionário vazio para armazenar chaves das datas formatadas e todos os valores.
    carga_Newave_dataFormatada = {}
    for chave, valor in dicionario_carga_Newave.items():
            chaves_formatadas = {}
            for data, carga in valor.items():
                    # data_str = data.strftime("%d/%m/%Y")  # Converte o objeto datetime em string
                    # chaves_formatadas[data_str] = carga
                    chaves_formatadas[data] = carga
            chave_formatada = chave  # Mantém a chave original como string
            carga_Newave_dataFormatada[chave_formatada] = chaves_formatadas
    # Criando um dicionário vazio para poder criar uma key com o nome do modelo newave .
    newave = {}
    newave['newave'] = carga_Newave_dataFormatada
    return newave

def calc_expectativa_realizado(dtref,dicionario_resultado_decomp,dataFormatadaPrimeiraPosicao,tabela_completa_IPDO):
    global dateFormat
    dtref = dtref + datetime.timedelta(days=720)
    
    # Criar listas vazias para armazenar os valores
    cd_submercado_list = []
    valor_calculado_list = []
    dt_inicio_rv_list = []

    # Iterar sobre o dicionário para extrair os valores
    for submercado, data in dicionario_resultado_decomp['decomp'].items():
            for dt_inicio_rv, valor_calculado in data.items():
                    cd_submercado_list.append(submercado)
                    valor_calculado_list.append(valor_calculado)
                    dt_inicio_rv_list.append(dt_inicio_rv)
    # Criar o DataFrame com as listas
    df_resultado_decomp = pd.DataFrame({'cd_submercado': cd_submercado_list,
                                        'valor_calculado': valor_calculado_list,
                                        'dt_inicio_rv': dt_inicio_rv_list})
    # Mapear os valores de cd_submercado para as colunas desejadas
    chaves = {
            'NE': 'carga_ne',
            'N': 'carga_n',
            'S': 'carga_s',
            'SE': 'carga_se'
    }

    # Criando index para trocar chaves dos submercados
    df_resultado_decomp['index'] = df_resultado_decomp['cd_submercado'].replace(chaves)

    # Pivotar a tabela
    df_resultado_decomp = df_resultado_decomp.pivot_table(values='valor_calculado', index='dt_inicio_rv', columns='index', aggfunc='sum').reset_index()
    # Renomear columns para None

    df_resultado_decomp = df_resultado_decomp.rename_axis(None, axis='columns')

    # Reordenar as colunas
    colunas = ['dt_inicio_rv', 'carga_se','carga_s', 'carga_ne', 'carga_n']
    df_resultado_decomp = df_resultado_decomp[colunas]

    db_ons = wx_dbClass.db_mysql_master('db_ons')
    db_ons.connect()
    tb_carga_ipdo = db_ons.getSchema('tb_carga_ipdo')

    query_get_ipdo_carga = db.select(tb_carga_ipdo.c.dt_referente,tb_carga_ipdo.c.carga_se,tb_carga_ipdo.c.carga_s,tb_carga_ipdo.c.carga_ne,tb_carga_ipdo.c.carga_n).where(between(tb_carga_ipdo.c.dt_referente, dataFormatadaPrimeiraPosicao, dtref))
    resultadosIpdo_carga = db_ons.conn.execute(query_get_ipdo_carga).all()

    # Reordenar resultados em ordem crescente
    resultadosIpdo_carga_ordenada = sorted(resultadosIpdo_carga)

    # Criando dataframe cargaIpdo
    colunas = ["dt_referente", "carga_se", "carga_s", "carga_ne", "carga_n"]
    dfResultados_cargaIpdo = pd.DataFrame(resultadosIpdo_carga_ordenada, columns=colunas)



    tabela_completa_IPDO = tabela_completa_IPDO['ipdo_verificado_previsao']
    dfResultados_cargaIpdo_Previsao = pd.DataFrame(tabela_completa_IPDO)
    dfResultados_cargaIpdo_Previsao['dt_referente'] = dfResultados_cargaIpdo_Previsao.index
    dfResultados_cargaIpdo_Previsao.reset_index(drop=True, inplace=True)
    dfResultados_cargaIpdo_Previsao = dfResultados_cargaIpdo_Previsao.rename(columns={'dt_referente': 'dt_referente', 'N': 'carga_n','NE': 'carga_ne', 'S': 'carga_s', 'SE': 'carga_se',})
    dfResultados_cargaIpdo_Previsao = dfResultados_cargaIpdo_Previsao[colunas]


    # Aplicar a função na coluna do DataFrame, trasnformando date em string
    dfResultados_cargaIpdo['dt_referente'] = dfResultados_cargaIpdo['dt_referente'].apply(lambda x: x.strftime('%Y%m%d'))

    # Definir o tamanho do intervalo de dias para fazer média
    tamanho_intervalo = 7

    # Calcular a média para cada intervalo
    resultados = []
    datas = []
    for i in range(0, len(dfResultados_cargaIpdo), tamanho_intervalo):
            intervalo = dfResultados_cargaIpdo.iloc[i:i+tamanho_intervalo]
            media_intervalo = intervalo.mean()
            data_referente = intervalo['dt_referente'].iloc[0]  # Obter a data do primeiro dia do intervalo
            resultados.append(media_intervalo)
            datas.append(data_referente)

    # Converter os resultados e datas em um DataFrame
    resultado_final = pd.DataFrame(resultados)
    resultado_final['dt_referente'] = datas
    resultado_final_media_IPDO = resultado_final.astype(int)

    # Calcular a média para cada intervalo
    resultados_previsao = []
    datas = []
    for i in range(0, len(dfResultados_cargaIpdo_Previsao), tamanho_intervalo):
            intervalo = dfResultados_cargaIpdo_Previsao.iloc[i:i+tamanho_intervalo]
            media_intervalo = intervalo.mean()
            data_referente = intervalo['dt_referente'].iloc[0]  # Obter a data do primeiro dia do intervalo
            resultados_previsao.append(media_intervalo)
            datas.append(data_referente)

    # Converter os resultados e datas em um DataFrame
    resultado_final_previsao = pd.DataFrame(resultados_previsao)
    resultado_final_previsao['dt_referente'] = datas
    resultado_final_media_IPDO_Previsao = resultado_final_previsao.astype(int)

    # Transformar a coluna 'dt_referente' em formato de data
    resultado_final_media_IPDO['dt_referente'] = pd.to_datetime(resultado_final_media_IPDO['dt_referente'], format='%Y%m%d').dt.date

    # Transformar a coluna 'dt_referente' em formato de data
    resultado_final_media_IPDO_Previsao['dt_referente'] = pd.to_datetime(resultado_final_media_IPDO_Previsao['dt_referente'], format='%Y%m%d').dt.date

    # Filtrando dataframe para pegar data inicial e final
    inicio = resultado_final_media_IPDO['dt_referente'][0]
    fim = resultado_final_media_IPDO['dt_referente'].iloc[-1]

    # Transformar a coluna 'dt_referente' em formato de data
    df_resultado_decomp['dt_inicio_rv'] = pd.to_datetime(df_resultado_decomp['dt_inicio_rv'], format='%Y%m%d').dt.date
    # Filtrar as linhas com base no intervalo de datas
    #df_resultado_decomp_filtrado = df_resultado_decomp.loc[(df_resultado_decomp['dt_inicio_rv'] >= inicio) & (df_resultado_decomp['dt_inicio_rv'] <= fim)]

    # Converter a coluna 'dt_referente' para o tipo de dados datetime
    resultado_final_media_IPDO['dt_referente'] = pd.to_datetime(resultado_final_media_IPDO['dt_referente'])
    # Formatar a coluna 'dt_referente' para o formato '%Y%m%d'
    resultado_final_media_IPDO['dt_referente'] = resultado_final_media_IPDO['dt_referente'].dt.strftime('%Y%m%d')

    # Converter a coluna 'dt_referente' para o tipo de dados datetime
    resultado_final_media_IPDO_Previsao['dt_referente'] = pd.to_datetime(resultado_final_media_IPDO_Previsao['dt_referente'])
    # Formatar a coluna 'dt_referente' para o formato '%Y%m%d'
    resultado_final_media_IPDO_Previsao['dt_referente'] = resultado_final_media_IPDO_Previsao['dt_referente'].dt.strftime('%Y%m%d')

    # Converter a coluna 'dt_inicio_rv' para o tipo de dados datetime
    df_resultado_decomp['dt_inicio_rv'] = pd.to_datetime(df_resultado_decomp['dt_inicio_rv'])
    # Formatar a coluna 'dt_referente' para o formato '%Y%m%d'
    df_resultado_decomp['dt_inicio_rv'] = df_resultado_decomp['dt_inicio_rv'].dt.strftime('%Y%m%d')

    # renomeando coluna do dataframe decomp de dt_inicio_rv para dt_referente
    df_resultado_decomp = df_resultado_decomp.rename(columns={'dt_inicio_rv': 'dt_referente'})



    resultado_final_media_IPDO['dt_referente'] = pd.to_datetime(resultado_final_media_IPDO['dt_referente'])
    resultado_final_media_IPDO_Previsao['dt_referente'] = pd.to_datetime(resultado_final_media_IPDO_Previsao['dt_referente'])
    df3 = pd.concat([resultado_final_media_IPDO,resultado_final_media_IPDO_Previsao])

    df_media_IPDO_valoresUnicos = df3.drop_duplicates(subset='dt_referente', keep='last')

    df_media_IPDO_valoresUnicos['dt_referente'] = df_media_IPDO_valoresUnicos['dt_referente'].dt.strftime('%Y%m%d')

    # juntando os valores dos dois dataframes usando coluna dt_referente como chave, para poder calcular medias.
    df_merged = pd.merge(df_media_IPDO_valoresUnicos, df_resultado_decomp, on='dt_referente')
 
 # Calcular a diferença entre as colunas correspondentes
    df_merged['carga_se_diff'] = df_merged['carga_se_x'] - df_merged['carga_se_y']
    df_merged['carga_s_diff'] = df_merged['carga_s_x'] - df_merged['carga_s_y']
    df_merged['carga_ne_diff'] = df_merged['carga_ne_x'] - df_merged['carga_ne_y']
    df_merged['carga_n_diff'] = df_merged['carga_n_x'] - df_merged['carga_n_y']

    # Calcular a variação percentual
    df_merged['carga_se_var'] = (df_merged['carga_se_diff'] / df_merged['carga_se_y']) * 100
    df_merged['carga_s_var'] = (df_merged['carga_s_diff'] / df_merged['carga_s_y']) * 100
    df_merged['carga_ne_var'] = (df_merged['carga_ne_diff'] / df_merged['carga_ne_y']) * 100
    df_merged['carga_n_var'] = (df_merged['carga_n_diff'] / df_merged['carga_n_y']) * 100

    # Renomeando os valores das chaves para padronizar igual os outros dicionarios
    novas_chaves = {
            'N': 'carga_n',
            'NE': 'carga_ne',
            'SE': 'carga_se',
            'S': 'carga_s'
    }

    # Criando dicionario com primeira chave para receber os valores
    ipdo_media_valores = {"ipdo_media_valores": {}}

    # Define 'dt_referente' como o índice
    resultado_media_IPDO = df_media_IPDO_valoresUnicos.set_index('dt_referente')  

    for nova_chave, chave_antiga in novas_chaves.items():
            ipdo_media_valores["ipdo_media_valores"][nova_chave] = resultado_media_IPDO[chave_antiga].to_dict()

    
    # Criar o novo dataframe com as colunas de variação percentual
    resultado_final_percentual = df_merged[['dt_referente', 'carga_se_var', 'carga_s_var', 'carga_ne_var', 'carga_n_var']]
    resultado_final_percentual_arredondado = resultado_final_percentual.round(decimals=2)

    # Converter a coluna 'dt_referente' para o tipo de dados datetime
    resultado_final_percentual_arredondado['dt_referente'] = pd.to_datetime(resultado_final_percentual_arredondado['dt_referente'])
    # Formatar a coluna 'dt_referente' para o formato '%Y%m%d'
    resultado_final_percentual_arredondado['dt_referente'] = resultado_final_percentual_arredondado['dt_referente'].dt.strftime('%Y%m%d')

    # Renomeando os valores das chaves para padronizar igual os outros dicionarios
    novas_chaves = {
            'N': 'carga_n_var',
            'NE': 'carga_ne_var',
            'SE': 'carga_se_var',
            'S': 'carga_s_var'
    }

    # Criar dicionário vazio para armazenar os resultados
    variacao_percentual_arredondado = {"percentual_arredondado": {}}

    resultado_percentual_arredondado = resultado_final_percentual_arredondado.set_index('dt_referente')  

    for nova_chave, chave_antiga in novas_chaves.items():
                    variacao_percentual_arredondado["percentual_arredondado"][nova_chave] = resultado_percentual_arredondado[chave_antiga].to_dict()

    # Dataframe com a diferenca dos valores
    resultado_final_diff = df_merged[['dt_referente', 'carga_se_diff', 'carga_s_diff', 'carga_ne_diff', 'carga_n_diff']]

    # Converter a coluna 'dt_referente' para o tipo de dados datetime
    resultado_final_diff['dt_referente'] = pd.to_datetime(resultado_final_diff['dt_referente'])

    # Formatar a coluna 'dt_referente' para o formato '%Y%m%d'
    resultado_final_diff['dt_referente'] = resultado_final_diff['dt_referente'].dt.strftime('%Y%m%d')

    # Renomeando os valores das chaves para padronizar igual os outros dicionarios
    novas_chaves = {
            'N': 'carga_n_diff',
            'NE': 'carga_ne_diff',
            'SE': 'carga_se_diff',
            'S': 'carga_s_diff'
    }

    # Criar dicionário vazio para armazenar os resultados
    diferenca_valores = {"diferenca_valores": {}}

    resultado_diff = resultado_final_diff.set_index('dt_referente')  

    for nova_chave, chave_antiga in novas_chaves.items():
                    diferenca_valores["diferenca_valores"][nova_chave] = resultado_diff[chave_antiga].to_dict()
    
    return ipdo_media_valores,variacao_percentual_arredondado,diferenca_valores



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


def get_newave_submercado(data_deck):
  
    global dateFormat
    data_deck = datetime.datetime.strptime(data_deck, "%Y%m%d")

    db_decks = wx_dbClass.db_mysql_master('db_decks')
    db_decks.connect()
    tb_cadastro_newave = db_decks.getSchema('tb_cadastro_newave')
    tb_nw_carga = db_decks.getSchema('tb_nw_carga')

    dt_deck_Selecionado = data_deck

    query_id_Selecionado = db.select(tb_cadastro_newave.c.id, tb_cadastro_newave.c.dt_inicio_rv).where(tb_cadastro_newave.c.dt_inicio_rv == dt_deck_Selecionado).with_only_columns([tb_cadastro_newave.c.id])
    id_escolhido = db_decks.conn.execute(query_id_Selecionado)

    for linha in id_escolhido:
            id = linha.id

    query_carga_newave = db.select(tb_nw_carga.c.id_deck, tb_nw_carga.c.dt_referente, tb_nw_carga.c.cd_submercado, tb_nw_carga.c.vl_energia_total).where(tb_nw_carga.c.id_deck == id)
    cargas_newave = db_decks.conn.execute(query_carga_newave).all()

    colunas = ["id_deck", "dt_referente", "cd_submercado", "vl_carga"]
    df_cargasNewave = pd.DataFrame(cargas_newave, columns=colunas)

    # Crie o dicionário vazio para receber os valores das cargas e datas.
    dicionario_carga_Newave = {}

    # Percorra o dataframe e adicione os valores ao dicionário criado
    for index, row in df_cargasNewave.iterrows():
            submercado = None
            if row['cd_submercado'] == 1:
                    submercado = 'SE'
            elif row['cd_submercado'] == 2:
                    submercado = 'S'
            elif row['cd_submercado'] == 3:
                    submercado = 'NE'
            elif row['cd_submercado'] == 4:
                    submercado = 'N'
    
            data_referencia = datetime.datetime.strftime(row['dt_referente'],'%Y%m%d')  # Certifique-se de que a data seja uma string
            valor_carga = row['vl_carga']

            # Verifique se o submercado já existe no dicionário
            if submercado not in dicionario_carga_Newave:
                    dicionario_carga_Newave[submercado] = {}  # Se não existir, crie um dicionário interno

            # Adicione o valor da carga ao dicionário interno usando a data_referencia como chave
            dicionario_carga_Newave[submercado][data_referencia] = valor_carga

    return dicionario_carga_Newave

def get_dessem_submercado(data_deck):
  
    global dateFormat
    data_deck = datetime.datetime.strptime(data_deck, "%Y%m%d")

    db_decks = wx_dbClass.db_mysql_master('db_decks')
    db_decks.connect()
    tb_cadastro_dessem = db_decks.getSchema('tb_cadastro_dessem')
    tb_ds_carga = db_decks.getSchema('tb_ds_carga')

    
    query_id_Selecionado = db.select(tb_cadastro_dessem.c.id, tb_cadastro_dessem.c.dt_referente, tb_cadastro_dessem.c.id_fonte).where((tb_cadastro_dessem.c.dt_referente == data_deck) & (tb_cadastro_dessem.c.id_fonte == '2')).with_only_columns([tb_cadastro_dessem.c.id])
    id_escolhido = db_decks.conn.execute(query_id_Selecionado).fetchall()
    
    if not id_escolhido:
        # Se não encontrou com id_fonte == '2' == 'CCEE', busca com id_fonte == '1' == 'ONS' 
        query_id_Selecionado = db.select(tb_cadastro_dessem.c.id, tb_cadastro_dessem.c.dt_referente, tb_cadastro_dessem.c.id_fonte).where((tb_cadastro_dessem.c.dt_referente == data_deck) & (tb_cadastro_dessem.c.id_fonte == '1')).with_only_columns([tb_cadastro_dessem.c.id])
        id_escolhido = db_decks.conn.execute(query_id_Selecionado).fetchall()
    
    for linha in id_escolhido:
            id_cadastroDessem = linha.id


    query_carga_dessem = db.select(tb_ds_carga.c.id_deck, tb_ds_carga.c.cd_submercado, tb_ds_carga.c.dataHora,tb_ds_carga.c.vl_carga).where(tb_ds_carga.c.id_deck == id_cadastroDessem)
    dessem_carga = db_decks.conn.execute(query_carga_dessem).all()

    colunas = ["id_deck", "cd_submercado","dataHora","vl_carga"]
    df_cargasDessem = pd.DataFrame(dessem_carga, columns=colunas)
    # pdb.set_trace()
    df_cargasDessem['dataHora'] = df_cargasDessem['dataHora'].dt.strftime('%Y%m%d%H:%M')

    # Crie o dicionário vazio para receber os valores das cargas e datas.
    dicionario_carga_Dessem = {}

    # Percorra o dataframe e adicione os valores ao dicionário criado
    for index, row in df_cargasDessem.iterrows():
            submercado = None
            if row['cd_submercado'] == 1:
                    submercado = 'SE'
            elif row['cd_submercado'] == 2:
                    submercado = 'S'
            elif row['cd_submercado'] == 3:
                    submercado = 'NE'
            elif row['cd_submercado'] == 4:
                    submercado = 'N'
    
            # data_referencia = datetime.strftime(row['dataHora'],'%Y%m%d') 
            data_referencia = row['dataHora']
            data_referencia = row['dataHora']  
            valor_carga = row['vl_carga']

            # Verifique se o submercado já existe no dicionário
            if submercado not in dicionario_carga_Dessem:
                    dicionario_carga_Dessem[submercado] = {}  # Se não existir, crie um dicionário interno

            # Adicione o valor da carga ao dicionário interno usando a data_referencia como chave
            dicionario_carga_Dessem[submercado][data_referencia] = valor_carga


    return dicionario_carga_Dessem

def get_decomp_submercado(data_deck):
  
    global dateFormat
    data_deck = datetime.datetime.strptime(data_deck, "%Y%m%d")
    db_decks = wx_dbClass.db_mysql_master('db_decks')
    db_decks.connect()
    tb_ds_bloco_tm = db_decks.getSchema('tb_ds_bloco_tm')
    tb_cadastro_decomp = db_decks.getSchema('tb_cadastro_decomp')
    tb_dc_dadger_dp = db_decks.getSchema('tb_dc_dadger_dp')
    

    
    query_id_decomp = db.select(tb_cadastro_decomp.c.id, tb_cadastro_decomp.c.dt_inicio_rv).where(tb_cadastro_decomp.c.dt_inicio_rv == data_deck).with_only_columns([tb_cadastro_decomp.c.id])
    id_escolhido = db_decks.conn.execute(query_id_decomp)
    for linha in id_escolhido:
            id_cadastroDecomp = linha.id

    query_carga_decompDadger = db.select(tb_dc_dadger_dp.c.id_deck,tb_dc_dadger_dp.c.ip,tb_dc_dadger_dp.c.vl_carga_pesada, tb_dc_dadger_dp.c.vl_carga_media, tb_dc_dadger_dp.c.vl_carga_leve, tb_dc_dadger_dp.c.cd_submercado).where((tb_dc_dadger_dp.c.id_deck == id_cadastroDecomp))
    carga_decomp_dadger = db_decks.conn.execute(query_carga_decompDadger).all()

    colunas_decomp_dadger = ["id_deck", "ip","vl_carga_pesada","vl_carga_media","vl_carga_leve","cd_submercado"]
    df_cargasDecompDadger = pd.DataFrame(carga_decomp_dadger, columns=colunas_decomp_dadger)

    listaIP = df_cargasDecompDadger['ip'].unique()
    ip_to_date = {}

    for item, ip in enumerate(listaIP):
            semanas = item  
            quantidadeSemana = datetime.timedelta(weeks=semanas)
            nova_data = data_deck + quantidadeSemana
            ip_to_date[ip] = nova_data

    # Aplicar a lógica de substituição com base no mapeamento
    df_cargasDecompDadger['ip'] = df_cargasDecompDadger['ip'].map(ip_to_date)

    # Criando uma nova coluna 'data' a partir de 'ip' para facilitar o processamento
    df_cargasDecompDadger['ip'] = pd.to_datetime(df_cargasDecompDadger['ip'])

    start_date = df_cargasDecompDadger['ip'].min()
    end_date = df_cargasDecompDadger['ip'].max()

    # Criando uma lista de datas em sequência
    date_range = pd.date_range(start=start_date, end=end_date)
    # pdb.set_trace()
    df_cargasDecompDadger.columns = ['id_deck', 'data', 'vl_carga_pesada', 'vl_carga_media', 'vl_carga_leve', 'cd_submercado']

    # Convertendo a coluna 'data' para o formato de data
    df_cargasDecompDadger['data'] = pd.to_datetime(df_cargasDecompDadger['data'])

    # Encontrando as datas únicas no DataFrame original
    unique_dates = df_cargasDecompDadger['data'].unique()

    # Criando um DataFrame com o range de datas
    date_range = pd.date_range(start=min(unique_dates), end=max(unique_dates), freq='H')

    # Criando um DataFrame vazio para receber os dados
    novo_df_cargasDecompDadger = pd.DataFrame(columns=df_cargasDecompDadger.columns)

    # Iterando pelas datas do range
    for date in date_range:
            # Iterando pelos submercados
            for submercado in df_cargasDecompDadger['cd_submercado'].unique():
                    # Filtrando os dados do DataFrame original para a data e submercado atuais
                    filtered_data = df_cargasDecompDadger[(df_cargasDecompDadger['data'] <= date) & (df_cargasDecompDadger['cd_submercado'] == submercado)]
        
                    # Copiando o último registro da data filtrada (o mais recente)
                    last_data = filtered_data.tail(1).copy()
        
                    # Atualizando a data do último registro com a data atual
                    last_data['data'] = date
        
                    # Concatenando o registro no novo DataFrame
                    novo_df_cargasDecompDadger = pd.concat([novo_df_cargasDecompDadger, last_data])

    # Resetando o índice do novo DataFrame
    novo_df_cargasDecompDadger.reset_index(drop=True, inplace=True)

    query_discretizacao_patamar = db.select(tb_ds_bloco_tm.c.dt_inicio_patamar, tb_ds_bloco_tm.c.vl_patamar).where((tb_ds_bloco_tm.c.dt_inicio_patamar >= start_date) & (tb_ds_bloco_tm.c.dt_inicio_patamar <= end_date))
    discretizacao_patamar = db_decks.conn.execute(query_discretizacao_patamar).all()

    colunas_discretizacao_patamar = ["data", "vl_patamar"]
    df_discretizacao_patamar = pd.DataFrame(discretizacao_patamar, columns=colunas_discretizacao_patamar)
    

    mapeamentoCargas = {1: 'vl_carga_leve', 2: 'vl_carga_media', 3: 'vl_carga_pesada'}
    df_discretizacao_patamar['vl_patamar'] = df_discretizacao_patamar['vl_patamar'].replace(mapeamentoCargas)

    df_final_cargaDecomp = pd.merge(novo_df_cargasDecompDadger, df_discretizacao_patamar, on=['data'])

    # Função para manter apenas a coluna correspondente a vl_patamar
    def manter_coluna(row):
            return row[row['vl_patamar']]

    # Aplicar a função em cada linha do DataFrame
    df_final_cargaDecomp['valor_final'] = df_final_cargaDecomp.apply(manter_coluna, axis=1)

    colunas_a_remover = ['vl_carga_pesada', 'vl_carga_media', 'vl_carga_leve']
    df_final_cargaDecomp = df_final_cargaDecomp.drop(columns=colunas_a_remover)
    df_final_cargaDecomp['valor_final'] = df_final_cargaDecomp['valor_final'].round(0).astype(int)

    # Crie o dicionário vazio para receber os valores das cargas e datas.
    dicionario_carga_Decomp = {}

    # Percorra o dataframe e adicione os valores ao dicionário criado
    for index, row in df_final_cargaDecomp.iterrows():
            submercado = None
            if row['cd_submercado'] == 1:
                    submercado = 'SE'
            elif row['cd_submercado'] == 2:
                    submercado = 'S'
            elif row['cd_submercado'] == 3:
                    submercado = 'NE'
            elif row['cd_submercado'] == 4:
                    submercado = 'N'
    
            data_referencia = datetime.datetime.strftime(row['data'],'%Y%m%d%H:%M')  
            valor_carga = row['valor_final']

            # Verifique se o submercado já existe no dicionário
            if submercado not in dicionario_carga_Decomp:
                    dicionario_carga_Decomp[submercado] = {}  # Se não existir, crie um dicionário interno

            # Adicione o valor da carga ao dicionário interno usando a data_referencia como chave
            dicionario_carga_Decomp[submercado][data_referencia] = valor_carga

    
    return dicionario_carga_Decomp


def get_mediaMensal_PLD(data_inicial):
  
    db_decks = wx_dbClass.db_mysql_master('db_decks', connect=True)
    tb_cadastro_dessem = db_decks.db_schemas.get("tb_cadastro_dessem")
    tb_ds_pdo_cmosist = db_decks.db_schemas.get("tb_ds_pdo_cmosist")

    db_ons = wx_dbClass.db_mysql_master('db_ons', connect=True)
    tb_pld = db_ons.db_schemas.get("tb_pld")

    idSubmercados = {1:'SE', 2:'S', 3:'NE', 4:'N'}
    
    dataHojeCompleta = datetime.datetime.strptime(data_inicial,'%Y%m%d')
    mes_Atual = dataHojeCompleta.month
    ano_Atual = dataHojeCompleta.year
    primeiro_dia_do_mes = dataHojeCompleta.replace(day=1)
    dataPrimeiroDiaMes_String = primeiro_dia_do_mes.strftime("%Y-%m-%d")

    ultimo_dia_mes = calendar.monthrange(ano_Atual, mes_Atual)[1]
    ultimoDiaMesAtual = date(ano_Atual, mes_Atual, ultimo_dia_mes)
    ultimoDiaMesAtual_string = ultimoDiaMesAtual.strftime("%Y-%m-%d")


    select_MesAtualDessem = db.select(
            tb_cadastro_dessem.c.id, 
            tb_cadastro_dessem.c.dt_referente, 
            tb_cadastro_dessem.c.id_fonte,

            tb_ds_pdo_cmosist.c.cd_submercado,
            tb_ds_pdo_cmosist.c.vl_horario,
            tb_ds_pdo_cmosist.c.vl_preco
         ).join(
              tb_cadastro_dessem,tb_cadastro_dessem.c.id == tb_ds_pdo_cmosist.c.id_deck
         ).where(
            (tb_cadastro_dessem.c.dt_referente.between(dataPrimeiroDiaMes_String, ultimoDiaMesAtual_string)) &
            (tb_cadastro_dessem.c.id_fonte == 2) &
            (tb_ds_pdo_cmosist.c.cd_submercado == 1)
            )
    VL_Preço_dessem = db_decks.db_execute(select_MesAtualDessem).fetchall()

    select_pldMinMax = db.select(
        tb_pld.c.str_ano,
        tb_pld.c.vl_PLDmax_hora,
        tb_pld.c.vl_PLDmin,
        tb_pld.c.vl_PLDmax_estr,
        ).where(
            tb_pld.c.str_ano == ano_Atual
        )

    vl_pldMinMax = db_ons.db_execute(select_pldMinMax).fetchall()
    
    pld_max = vl_pldMinMax[0][1]
    pld_min = vl_pldMinMax[0][2]
    pld_diario_max = vl_pldMinMax[0][3]
    
    precos = pd.DataFrame(VL_Preço_dessem, columns=['ID_RODADA','DATA','FONTE', 'SUBMERCADO', 'HORARIO', 'CMO'])
    precos['SUBMERCADO'] = precos['SUBMERCADO'].replace(idSubmercados)
    precos['HORARIO'] = precos['HORARIO'].astype(str).str[0:5]
    precos['PLD'] = precos['CMO'] 
    precos.loc[precos['PLD'] < pld_min, 'PLD'] = pld_min
    precos.loc[precos['PLD'] > pld_max, 'PLD'] = pld_max

    mmm = precos.groupby(['FONTE', 'SUBMERCADO']).agg({'PLD': ['mean', 'min', 'max']})


    for fonte in precos['FONTE'].unique():
        for sub in precos['SUBMERCADO'].unique():
            media = mmm.loc[fonte, 'PLD'].loc[sub, 'mean']
            if media > pld_diario_max:
                fator = media/pld_diario_max
                precos.loc[(precos['SUBMERCADO']==sub) & (precos['FONTE']==fonte), 'PLD'] = precos.loc[(precos['SUBMERCADO']==sub) & (precos['FONTE']==fonte),'PLD']/fator

    media_pld_por_id_rodada = precos.groupby('ID_RODADA')[['PLD']].mean().reset_index()
    media_pld_por_id_rodada = media_pld_por_id_rodada.merge(precos[['ID_RODADA', 'DATA']], on='ID_RODADA', how='left')


    resultado_final_valoresUnicos = media_pld_por_id_rodada.drop_duplicates()
    resultado_final_valoresUnicos['PLD'] = resultado_final_valoresUnicos['PLD'].round(2)
    resultado_final_valoresUnicos['DATA'] = pd.to_datetime(resultado_final_valoresUnicos['DATA'], format='%Y-%m-%d').dt.strftime('%Y%m%d')

    resultado_final_valoresUnicos = resultado_final_valoresUnicos.drop('ID_RODADA',axis=1)
    dicionario_PLD = {}

    # Percorra as linhas do DataFrame
    for _, row in resultado_final_valoresUnicos.iterrows():
        pld = row['PLD']
        data = row['DATA']
        
        if data not in dicionario_PLD:
            dicionario_PLD[data] = pld
    
    return dicionario_PLD , pld_min


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

def query_rodadas_ndias_atras(dt_rodada: datetime, ndias: int, hr_rodada: str,modelo: str = None):
    data = {}
    if type(hr_rodada != int):
        hr_rodada = int(hr_rodada)
    dt_inicial = dt_rodada - datetime.timedelta(days=ndias)
    dataBase = wx_dbClass.db_mysql_master('db_rodadas')
    dataBase.connect()
    tb_cadastro_rodadas = dataBase.getSchema('tb_cadastro_rodadas')
    query = db.select(tb_cadastro_rodadas.c.id,tb_cadastro_rodadas.c.str_modelo, tb_cadastro_rodadas.c.dt_rodada, tb_cadastro_rodadas.c.fl_preliminar,  tb_cadastro_rodadas.c.fl_pdp, tb_cadastro_rodadas.c.fl_psat)\
            .where(db.and_(
                tb_cadastro_rodadas.c.id_smap.isnot(None),
				tb_cadastro_rodadas.c.dt_rodada.between(dt_inicial, dt_rodada),
				tb_cadastro_rodadas.c.str_modelo == modelo,
				tb_cadastro_rodadas.c.hr_rodada == hr_rodada
				)).order_by(tb_cadastro_rodadas.c.dt_rodada, tb_cadastro_rodadas.c.fl_preliminar, tb_cadastro_rodadas.c.fl_pdp.desc(),tb_cadastro_rodadas.c.fl_psat.desc())

    answer = dataBase.conn.execute(query)
    answer = answer.all()
    df = pd.DataFrame(answer, columns=['id', 'str_modelo' , 'dt_rodada', 'fl_preliminar', 'fl_pdp', 'fl_psat'])
    # df['fl_preliminar'] = df['fl_preliminar'].apply(lambda x: "PRE" if x else "")
    # df['fl_pdp'] = df['fl_pdp'].apply(lambda x: "PDP" if x else "")
    # df['fl_psat'] = df['fl_psat'].apply(lambda x: "PSAT" if x else "")
    # df['str_modelo'] = df['str_modelo'] + df['fl_preliminar']
    df = df.groupby("dt_rodada").first()
    df['flag'] = df.apply(lambda x: f'.Psat{hr_rodada:02d}' if x['fl_psat'] else f'.Pdp{hr_rodada:02d}' if x['fl_pdp'] else f'.PRE{hr_rodada:02d}', axis = 1)
    df['str_modelo'] = df['str_modelo'] + df['flag']
    df = df[['id', 'str_modelo']]

    answer = list(df.itertuples(index=False))
    
    data["data"] = dict((str(id_rodada), modelo) for id_rodada, modelo in answer)
    return data

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


def comparativo_carga_newave(dtSelecionada):
	
    data_string_selecionada = [dtSelecionada]
    
    json_resultados = {}

    for dtSelecionada in data_string_selecionada:
        
        # strData = str(dtSelecionada)
        strData = dtSelecionada
        
        db_decks = wx_dbClass.db_mysql_master('db_decks')
        db_decks.connect()
        tb_nw_sist_energia = db_decks.getSchema('tb_nw_sist_energia')
        tb_nw_cadic = db_decks.getSchema('tb_nw_cadic')
            
        # query para pegar todas as colunas da tabela tb_nw_sist_energia filtrando pela data do deck selecionado
        query_data_deck_sistemas = db.select(tb_nw_sist_energia).where(tb_nw_sist_energia.c.dt_deck == strData)
        carga_sistema_nw = db_decks.conn.execute(query_data_deck_sistemas).all()

        colunas = ["cd_submercado", "vl_ano", "vl_mes", "vl_energia_total","vl_geracao_pch","vl_geracao_pct","vl_geracao_eol","vl_geracao_ufv","vl_geracao_pch_mmgd","vl_geracao_pct_mmgd","vl_geracao_eol_mmgd","vl_geracao_ufv_mmgd","dt_deck"]

        # mapeando os submercados trocando de numeros para letras
        mapeamento_submercado = {
                1: 'SE',
                2: 'S',
                3: 'NE',
                4: 'N'
        }

        df_cargasNewave_Sistemas = pd.DataFrame(carga_sistema_nw, columns=colunas)
        # removendo a coluna dt_deck pois nao tem necessidade
        df_cargasNewave_Sistemas = df_cargasNewave_Sistemas.drop('dt_deck', axis=1)
    
        query_data_deck_cAdic = db.select(tb_nw_cadic.c.vl_ano,tb_nw_cadic.c.vl_mes,tb_nw_cadic.c.vl_mmgd_se,tb_nw_cadic.c.vl_mmgd_s,tb_nw_cadic.c.vl_mmgd_ne,tb_nw_cadic.c.vl_mmgd_n).where(tb_nw_sist_energia.c.dt_deck == strData)
        carga_cAdic_nw = db_decks.conn.execute(query_data_deck_cAdic).all()

        colunas = ["vl_ano", "vl_mes", "vl_mmgd_se","vl_mmgd_s","vl_mmgd_ne","vl_mmgd_n"]

        df_carga_cAdic_nw = pd.DataFrame(carga_cAdic_nw, columns=colunas)

        mapeamento_submercado_cAdic = {
        1: 'vl_mmgd_se',
        2: 'vl_mmgd_s',
        3: 'vl_mmgd_ne',
        4: 'vl_mmgd_n'
        }

        merged_df_cargasNewave_Sistemas_cAdic = pd.merge(df_cargasNewave_Sistemas, df_carga_cAdic_nw, on=['vl_ano', 'vl_mes'], how='left')

        # Criando as novas colunas com base no mapeamento
        for submercado, coluna in mapeamento_submercado_cAdic.items():
            merged_df_cargasNewave_Sistemas_cAdic[coluna] = merged_df_cargasNewave_Sistemas_cAdic.apply(lambda row: row[coluna] if row['cd_submercado'] == submercado else None, axis=1)



        df_linhas_Unicas = merged_df_cargasNewave_Sistemas_cAdic.drop_duplicates(subset=['cd_submercado', 'vl_ano', 'vl_mes'])


        # Aplicando a função lambda para criar a coluna carga_global
        df_linhas_Unicas['carga_global'] = df_linhas_Unicas.apply(lambda row: (
            row['vl_energia_total'] + 
                {
                        1: row['vl_mmgd_se'],
                        2: row['vl_mmgd_s'],
                        3: row['vl_mmgd_ne'],
                        4: row['vl_mmgd_n']
                }.get(row['cd_submercado'], np.nan)
        ), axis=1)


        # Criando o dicionário que será convertido em JSON
        json_cargasNewave_Sistemas_cAdic = {}

        # Agrupando os dados por submercado, ano e mês
        agrupamento_Dados = df_linhas_Unicas.groupby(['cd_submercado', 'vl_ano', 'vl_mes'])

        # Iterando sobre os grupos
        for (codigo_submercado, ano, mes), grupo_dados in agrupamento_Dados:
            submercado = mapeamento_submercado[codigo_submercado]
            key = str(ano) + "{:02d}".format(mes)
            
            # Criando a estrutura do JSON para cada submercado, ano e mês.Se o valor estiver NAN nao será adicionado ao dicionario.
            if submercado not in json_cargasNewave_Sistemas_cAdic:
                json_cargasNewave_Sistemas_cAdic[submercado] = {}
            json_cargasNewave_Sistemas_cAdic[submercado][key] = {}

            # Preenchendo os valores no JSON
            for coluna, valor in grupo_dados.iloc[0, 3:].items():
                if not np.isnan(valor):
                    json_cargasNewave_Sistemas_cAdic[submercado][key][coluna] = valor
        
        ano_mes_deck = strData[:7]  # Obtém os primeiros 7 caracteres, que representam o ano e o mês
        ano_mes_deck_sem_hifen = ano_mes_deck.replace('-', '')

        # json_Sistemas_cAdic = {f'{ano_mes_deck_sem_hifen}': json_cargasNewave_Sistemas_cAdic}
        json_resultados[ano_mes_deck_sem_hifen] = json_cargasNewave_Sistemas_cAdic

    return json_resultados

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
    # print(get_postos_rdh())
    # getRevBacias(datetime.datetime.now())
    print(get_previsao_dessem())
    pass
