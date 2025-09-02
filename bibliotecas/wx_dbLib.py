import os 
import sys
import time
import pymysql
import datetime
import pandas as pd
import sqlalchemy as db
from unidecode import unidecode
import matplotlib.pyplot as plt
from matplotlib import gridspec
import matplotlib.dates as mdates

sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.bibliotecas import wx_dbClass,wx_opweek
from PMO.scripts_unificados.apps.web_modelos.server.caches import rz_cache
from middle.utils import setup_logger
from middle.message import send_whatsapp_message

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")),'.env'))
logger = setup_logger()

class WxDataB:

    def __init__(self, DBM='mssql'):

        #DB MYSQL CONFIGURATION
        __HOST_MYSQL = os.getenv('HOST_MYSQL')
        __PORT_DB_MYSQL = os.getenv('PORT_DB_MYSQL')

        __USER_DB_MYSQL = os.getenv('USER_DB_MYSQL')
        __PASSWORD_DB_MYSQL = os.getenv('PASSWORD_DB_MYSQL')
        # DEBUG
        send_whatsapp_message("debug", "Iniciando conexão com banco de dados " + DBM, None)
        if DBM == 'mysqlWx':
            self.dbHost = __HOST_MYSQL
            self.dbUser = __USER_DB_MYSQL
            self.dbPassword = __PASSWORD_DB_MYSQL
            self.dbDatabase = 'bbce'
            self.port = int(__PORT_DB_MYSQL)

        elif DBM == 'mysql':
            self.dbHost = '127.0.0.1'
            self.dbUser = 'root'
            self.dbPassword = ''
            self.dbDatabase = 'climenergy'
            self.port = 3306

        self.dbm = DBM

    def getDateFormat(self):
        return "%Y-%m-%d"

    def getDatetimeFormat(self):
        return self.getDateFormat() + ' %H:%M'

    def changeDatabase(self, databaseName):
        self.dbDatabase = databaseName

    def connectHost(self):
        if self.dbm in ['mysql', 'mysqlWx']:
            conn = pymysql.connect(host=self.dbHost, user=self.dbUser, password=self.dbPassword, database=self.dbDatabase, port=self.port)
        return conn

    def requestServer(self, actionDb):

        tentativas = 0
        while tentativas < 3:
            try:
                conn = self.connectHost()
                cursor = conn.cursor()
                cursor.execute(actionDb)
                answerDb = ''
                if actionDb.split()[0] in ["SELECT", "DECLARE", "EXEC", "WITH"]:
                    answerDb = cursor.fetchall()
                elif actionDb.split()[0] in ["CREATE", "INSERT", "DELETE", "UPDATE"]:
                    conn.commit()
                    answerDb = cursor.rowcount
                conn.close()
                tentativas = 3

            except Exception as e:
                tentativas += 1
                print(e)
                print('Nova tentativa de importar em 60 segundos!')
                time.sleep(60)

        return answerDb

    def executemany(self, actionDb, values):

        tentativas = 0
        while tentativas < 3:
            try:
                conn = self.connectHost()
                cursor = conn.cursor()

                if type(values) == list:
                    if self.dbm in ['mssql_odbc']:
                        cursor.fast_executemany = True
                    cursor.executemany(actionDb, values)
                elif type(values) == tuple:
                    cursor.execute(actionDb, values)

                conn.commit()
                numLinhasInseridas = cursor.rowcount
                conn.close()
                tentativas = 3

            except Exception as e:
                tentativas += 1
                print(e)
                print('Nova tentativa de importar em 60 segundos!')
                time.sleep(60)
        
        return numLinhasInseridas

    def execute(self, actionDb, values):
        if actionDb.lower().split()[0] == "select":
            # Values deve ser uma tuplas com os valores na mesma ordem da query
            conn = self.connectHost()
            cursor = conn.cursor()
            cursor.execute(actionDb, values)
            return cursor.fetchall()
        else:
            # Values deve ser um array de tuplas
            tentativas = 0
            while tentativas < 3:
                try:
                    conn = self.connectHost()
                    cursor = conn.cursor()
                    cursor.execute(actionDb, values)
                    numLinhasInseridas = cursor.rowcount
                    conn.commit()
                    conn.close()
                    tentativas = 3

                except Exception as e:
                    tentativas += 1
                    print(e)
                    print('Nova tentativa de importar em 60 segundos!')
                    time.sleep(60)

            return numLinhasInseridas


def getCadastrosPrevsCv(self, data, modelo='', horario_rodada='', preliminar='', dt_rev='', pdp='', psat=''):

        dataBase = wx_dbClass.db_mysql_master("db_rodadas")
        dataBase.connect()

        tb_cadastro_rodadas = dataBase.getSchema("tb_cadastro_rodadas")
    
        querySelect = db.select([tb_cadastro_rodadas.c.dt_rodada, tb_cadastro_rodadas.c.str_modelo, tb_cadastro_rodadas.c.hr_rodada,tb_cadastro_rodadas.c.fl_preliminar, tb_cadastro_rodadas.c.fl_pdp, tb_cadastro_rodadas.c.fl_psat])

        querySelect = querySelect.where(tb_cadastro_rodadas.c.dt_rodada == data)
        if modelo != "":
            querySelect = querySelect.where(tb_cadastro_rodadas.c.str_modelo == modelo)
        if horario_rodada != "":
            querySelect = querySelect.where(tb_cadastro_rodadas.c.hr_rodada == horario_rodada)
        if preliminar != "":
            querySelect = querySelect.where(tb_cadastro_rodadas.c.fl_preliminar == preliminar)
        if dt_rev != "":
            querySelect = querySelect.where(tb_cadastro_rodadas.c.dt_revisao == dt_rev)
        if pdp != "":
            querySelect = querySelect.where(tb_cadastro_rodadas.c.fl_pdp == pdp)
        if psat != "":
            querySelect = querySelect.where(tb_cadastro_rodadas.c.fl_psat == psat)

        querySelect = querySelect.group_by(tb_cadastro_rodadas.c.dt_rodada, tb_cadastro_rodadas.c.str_modelo, tb_cadastro_rodadas.c.hr_rodada,tb_cadastro_rodadas.c.fl_preliminar,tb_cadastro_rodadas.c.dt_revisao,tb_cadastro_rodadas.c.fl_pdp,tb_cadastro_rodadas.c.fl_psat)\
                    .order_by(tb_cadastro_rodadas.c.fl_preliminar,tb_cadastro_rodadas.c.fl_pdp.desc(),tb_cadastro_rodadas.c.fl_psat.desc(),tb_cadastro_rodadas.c.hr_rodada.desc())
     
        values_list = dataBase.conn.execute(querySelect).fetchall()

        rodadas = list()
        revisao = wx_opweek.ElecData(dt_rev).atualRevisao

        for value in values_list:
            
            rodadas += tuple(value) + (revisao,),

        return rodadas


def getPrevsCv(data, modelo, horario_rodada, preliminar, dt_rev, pdp, psat):
    
        dataBase = wx_dbClass.db_mysql_master("db_rodadas")
        dataBase.connect()

        tb_prevs = dataBase.getSchema("tb_prevs")
        tb_cadastro_rodadas = dataBase.getSchema("tb_cadastro_rodadas")
        querySelect = db.select([tb_prevs.c.cd_posto, tb_prevs.c.vl_vazao, tb_prevs.c.dt_prevista,tb_cadastro_rodadas.c.str_modelo])\
                .select_from(tb_prevs.join(tb_cadastro_rodadas, tb_prevs.c.id==tb_cadastro_rodadas.c.id_previvaz))\
                    .where(db.and_(tb_cadastro_rodadas.c.dt_rodada == data, tb_cadastro_rodadas.c.str_modelo == modelo,tb_cadastro_rodadas.c.hr_rodada == horario_rodada,tb_cadastro_rodadas.c.fl_preliminar == preliminar,tb_cadastro_rodadas.c.dt_revisao == dt_rev, tb_cadastro_rodadas.c.fl_pdp == pdp, tb_cadastro_rodadas.c.fl_psat == psat))\
                        .order_by(tb_prevs.c.dt_prevista)
            
        values_list = dataBase.conn.execute(querySelect).fetchall()

        vazao = list()
        revisao = wx_opweek.ElecData(dt_rev).atualRevisao

        for values in values_list:
            
            vazao += tuple(values) + (revisao,),
                
        return vazao

def getCoeficienteRegres():

    db_ons = wx_dbClass.db_mysql_master("db_ons", connect=True)
    tb_regressoes = db_ons.db_schemas['tb_regressoes']

    sql_select = db.select(
        tb_regressoes.c.cd_posto_regredido,
        tb_regressoes.c.cd_posto_base,
        tb_regressoes.c.vl_mes,
        tb_regressoes.c.vl_A0,
        tb_regressoes.c.vl_A1,
        )
    answer = db_ons.db_execute(sql_select).fetchall()
    db_ons.db_dispose()

    return answer


def getMlt():

    db_ons = wx_dbClass.db_mysql_master("db_ons",connect=True)
    tb_mlt = db_ons.db_schemas["tb_mlt"]

    sql_select = db.select(
        tb_mlt.c.cd_submercado,
        tb_mlt.c.vl_mes,
        tb_mlt.c.vl_mlt,
    ).order_by(
        tb_mlt.c.cd_submercado,
        tb_mlt.c.vl_mes,
        )
    answer = db_ons.db_execute(sql_select).fetchall()
    db_ons.db_dispose()

    return answer

def get_acomph_cached(dataInicial,granularidade='submercado', dataFinal = None):
    prefixo = "ACOMPH"
    acomph_values = rz_cache.cache_acomph(prefixo, granularidade, dataInicial,dataFinal)
    return acomph_values

def getAcomphEspecifico(dtAcomph):

    dataBase = wx_dbClass.db_mysql_master("db_ons")
    dataBase.connect()
    tb_acomph = dataBase.getSchema("tb_acomph")
    sql_select = tb_acomph.select().where(tb_acomph.c.dt_acomph == dtAcomph.strftime('%Y-%m-%d'))
    values_list = dataBase.conn.execute(sql_select).fetchall()
    return values_list


def getPrevisaoCargaDs(data:datetime):

    db_decks = wx_dbClass.db_mysql_master('db_decks')
    db_decks.connect()
    tb_ds_carga = db_decks.getSchema('tb_ds_carga')
    tb_cadastro_dessem = db_decks.getSchema('tb_cadastro_dessem')

    query  = db.select([
        tb_ds_carga.c.cd_submercado,
        tb_ds_carga.c.dataHora,
        tb_ds_carga.c.vl_carga,
        tb_cadastro_dessem.c.dt_referente,
        db.case([(tb_cadastro_dessem.c.id_fonte == 1, 1)], else_=2).label('id_fonte')
        ]).join(
            tb_cadastro_dessem,
            tb_cadastro_dessem.c.id == tb_ds_carga.c.id_deck
        ).where(
            db.and_(
            tb_cadastro_dessem.c.dt_referente == data,
            tb_cadastro_dessem.c.id_fonte.in_([1])
            )
        )

    answer = db_decks.conn.execute(query).fetchall()
    return answer


def get_deck_dessem_renovaveis(data_deck):


    query_get_rodada = '''SELECT tdr.vl_periodo, tdr.id_submercado, tdr.vl_geracao_uhe, tdr.vl_geracao_ute, 
                        tdr.vl_geracao_cgh, tdr.vl_geracao_pch, tdr.vl_geracao_ufv, tdr.vl_geracao_uee , tdr.vl_geracao_mgd FROM 
                                tb_cadastro_dessem as tcd right join tb_ds_renovaveis as tdr on tcd.id=tdr.id_deck where tcd.dt_referente = %s and tcd.id_fonte = 1'''
    datab = WxDataB('mysqlWx')
    datab.dbDatabase = 'db_decks'
    answer = datab.execute(query_get_rodada, (data_deck))
    return answer

def getGeracaoHoraria(data):

    dateFormat = "%Y-%m-%d"

    db_ons = wx_dbClass.db_mysql_master("db_ons", connect=True)
    tb_geracao_horaria = db_ons.db_schemas['tb_geracao_horaria']
    tb_tipo_geracao = db_ons.db_schemas['tb_tipo_geracao']

    sql_select = db.select([
        tb_geracao_horaria.c.str_submercado,
        tb_geracao_horaria.c.dt_referente,
        tb_geracao_horaria.c.vl_carga,
        tb_tipo_geracao.c.str_geracao,
    ]).join(
        tb_geracao_horaria, tb_geracao_horaria.c.cd_geracao == tb_tipo_geracao.c.cd_geracao
    ).where(
        tb_geracao_horaria.c.dt_update == data.strftime(dateFormat)
    ).order_by(
        tb_tipo_geracao.c.str_geracao,
        tb_geracao_horaria.c.dt_referente
        )
    answer = db_ons.db_execute(sql_select).fetchall()
    db_ons.db_dispose()
    return answer

def getCargaHoraria(data):
    logger.info(f"Iniciando busca de carga horária para data: {data}")
    
    dateFormat = "%Y-%m-%d"
    formatted_date = data.strftime(dateFormat)
    logger.debug(f"Data formatada: {formatted_date}")

    try:
        logger.info("Conectando ao banco de dados db_ons")
        db_ons = wx_dbClass.db_mysql_master("db_ons", connect=True)
        tb_carga_horaria = db_ons.db_schemas['tb_carga_horaria']
        logger.debug("Conexão com banco estabelecida e tabela tb_carga_horaria carregada")
        
        logger.info(f"Executando consulta SQL para data: {formatted_date}")
        sql_select = db.select(
            tb_carga_horaria.c.str_submercado,
            tb_carga_horaria.c.dt_referente,
            tb_carga_horaria.c.vl_carga,
        ).where(
            tb_carga_horaria.c.dt_update == formatted_date
        ).order_by(
            tb_carga_horaria.c.dt_referente
        )
        
        answer = db_ons.db_execute(sql_select).fetchall()
        logger.info(f"Consulta executada com sucesso. {len(answer)} registros encontrados")
        
        if not answer:
            logger.warning(f"Nenhum registro encontrado para a data: {formatted_date}")
        
    except Exception as e:
        logger.error(f"Erro ao executar consulta de carga horária: {str(e)}")
        raise
    logger.info("Função getCargaHoraria finalizada com sucesso")
    return answer


def get_Balanco_Dessem(data):
    logger.info(f"Iniciando busca de balanço DESSEM para data: {data}")
    
    try:
        logger.info("Conectando ao banco de dados db_decks")
        db_decks = wx_dbClass.db_mysql_master("db_decks")
        db_decks.connect()
        logger.info("Conexão com banco de dados estabelecida com sucesso")
        
        logger.debug("Obtendo schema da tabela tb_balanco_dessem")
        tb_balanco_dessem = db_decks.getSchema('tb_balanco_dessem')
        
        logger.debug(f"Executando consulta SQL com filtro de data >= {data}")
        sql_select = tb_balanco_dessem.select().where(tb_balanco_dessem.c.dt_data_hora >= data)
        answer = db_decks.db_execute(sql_select).all()
        
        logger.info(f"Consulta executada com sucesso. Registros encontrados: {len(answer)}")
        
        if not answer:
            logger.warning(f"Nenhum registro encontrado para data >= {data}")
        
        return answer
        
    except Exception as e:
        logger.error(f"Erro ao buscar balanço DESSEM para data {data}: {str(e)}")
        raise


def prev_carga_dessem(dt_prev):

    str_dt = dt_prev.strftime('%d/%m/%Y')

    carga = getPrevisaoCargaDs(dt_prev)

    if carga:
        df_carga= pd.DataFrame(carga, columns = ['cd_submerc','dt_ref','vl_carga','dt_ini','id_fonte'])
        df_aux = df_carga.groupby('dt_ref')['vl_carga'].sum()

        df_aux = df_aux.resample('1H').mean().ffill().reset_index()

        df_aux['hr'] = pd.to_datetime(df_aux['dt_ref']).dt.strftime('%H:%M')
        df_aux['dt_ref'] = pd.to_datetime(df_aux['dt_ref']).dt.strftime('%d/%m/%Y')
        df_values = df_aux[df_aux['dt_ref'] == str_dt]

        horas = pd.to_datetime(df_values['hr'], format = '%H:%M').tolist()
        carga_values = df_values['vl_carga'].tolist()
        media = df_values['vl_carga'].mean()
        return horas,carga_values,media

    else:
        print(f"Previsão do dia {str_dt}, não encontrada!")
        quit()


def semana_deck_prev_carga_dessem_horaria(dt_prev):

    valores = {}
    dt_ini_semana = wx_opweek.getLastSaturday(dt_prev)
    dt_prox_ini_semana = dt_ini_semana + datetime.timedelta(days=7)

    dt_aux = dt_prev

    carga = getPrevisaoCargaDs(dt_prev)
    if carga:
        df_carga= pd.DataFrame(carga, columns = ['str_submercado','horario','vl_carga','dt_ini','id_fonte'])
        df_carga['str_submercado'] = df_carga['str_submercado'].map(lambda x: 'SUDESTE' if x == 1 else 'SUL' if x == 2 else 'NORDESTE' if x == 3 else 'NORTE')

        while dt_aux < dt_prox_ini_semana:

            dt_str = dt_aux.strftime('%d/%m/%Y')

            df_values = df_carga[(df_carga['horario'] >= dt_aux) & (df_carga['horario'] < dt_aux + datetime.timedelta(days=1))]

            #se a primeira data começa em 00:00
            if df_values.iloc[0]['horario'] == dt_aux:
                df_values = df_values.set_index('horario').groupby('str_submercado')
                
            values = df_values['vl_carga'].resample('1H').mean().ffill()

            valores[dt_str]	= values

            dt_aux += datetime.timedelta(days=1)

    return valores

def semana_carga_verificada_horaria(data):
    dt_ini_semana = wx_opweek.getLastSaturday(data)
    dt_prox_ini_semana = dt_ini_semana + datetime.timedelta(days=7)


    valores_carga = {}

    dt_aux = dt_ini_semana
    while dt_aux < dt_prox_ini_semana:

        carga_verificada = getCargaHoraria(dt_aux)

        dt_str = dt_aux.strftime('%d/%m/%Y')

        if carga_verificada:
            df_carga = pd.DataFrame(carga_verificada, columns=['str_submercado','horario','vl_carga'])
            df_carga = df_carga[df_carga['str_submercado'] != "SIN"] 
            values_carga  = df_carga.set_index('horario').groupby(['str_submercado'])['vl_carga'].resample('1H').mean()

            valores_carga[dt_str] = values_carga

        dt_aux += datetime.timedelta(days=1)

    return valores_carga


def semana_deck_prev_renovaveis_dessem_horaria(dt_prev):

    fl_preliminar = False

    valores = {}
    dt_ini_semana = wx_opweek.getLastSaturday(dt_prev)
    dt_prox_ini_semana = dt_ini_semana + datetime.timedelta(days=7)

    carga = get_deck_dessem_renovaveis(dt_prev)
    colunas = ['vl_periodo', 'str_submercado', 'vl_geracao_uhe', 'vl_geracao_ute','vl_geracao_cgh', 'vl_geracao_pch', 'vl_geracao_ufv', 'vl_geracao_uee','vl_geracao_mgd']
    df_carga = pd.DataFrame(carga, columns=colunas)
    

    if not carga:
        if dt_prev.weekday() == 5:
            df_completo = pd.DataFrame()

            fl_preliminar = True
            
            dt_anterior = dt_prev - datetime.timedelta(days=1)
            carga = get_deck_dessem_renovaveis(dt_anterior)

            df_carga = pd.DataFrame(carga, columns=colunas)
            dt_aux = dt_ini_semana

            while(dt_aux < dt_prox_ini_semana):
                df_aux = df_carga
                df_aux['horario'] = dt_aux + pd.to_timedelta((df_aux['vl_periodo']-1)*30, unit='m')
                df_completo = pd.concat([df_completo,df_aux])

                dt_aux += datetime.timedelta(days=1)

            df_carga = df_completo
    else:
        df_carga['horario'] = dt_prev + pd.to_timedelta((df_carga['vl_periodo']-1)*30, unit='m')

    if not df_carga.empty:
        df_carga['str_submercado'] = df_carga['str_submercado'].map(lambda x: 'SUDESTE' if x == 1 else 'SUL' if x == 2 else 'NORDESTE' if x == 3 else 'NORTE')
        dt_aux = dt_prev
        while dt_aux < dt_prox_ini_semana:
            dt_str = dt_aux.strftime('%d/%m/%Y')
            df_prev_renovaveis = df_carga[(df_carga['horario'] >= dt_aux) & (df_carga['horario'] < dt_aux + datetime.timedelta(days=1))]
            df_prev_renovaveis = df_prev_renovaveis.set_index(['horario','str_submercado'])
            df_prev_renovaveis = df_prev_renovaveis[['vl_geracao_uhe', 'vl_geracao_ute','vl_geracao_cgh', 'vl_geracao_pch', 'vl_geracao_ufv', 'vl_geracao_uee','vl_geracao_mgd']]
            df_prev_renovaveis = df_prev_renovaveis.sum(axis=1).reset_index().rename(columns={0:'total geracao'})

            df_prev_renovaveis_resample = df_prev_renovaveis.set_index('horario').groupby('str_submercado')
            values = df_prev_renovaveis_resample['total geracao'].resample('1H').mean().ffill()

            valores[dt_str]	= values

            dt_aux += datetime.timedelta(days=1)

    return fl_preliminar, valores


def get_gereracao_termica_min_semana(dt_prev):

    valores = {}
    dt_ini_semana = wx_opweek.getLastSaturday(dt_prev)
    dt_prox_ini_semana = dt_ini_semana + datetime.timedelta(days=7)

    dt_aux = dt_ini_semana

    while dt_aux < dt_prox_ini_semana:

        dt_str = dt_aux.strftime('%d/%m/%Y')

        balanco = get_Balanco_Dessem(dt_aux)

        if balanco:
            ultimo_balanco_com_valor = balanco

        else:
            if dt_aux.weekday() == 5:
                dt_anterior = dt_prev - datetime.timedelta(days=1)
                balanco = get_Balanco_Dessem(dt_anterior)
                ultimo_balanco_com_valor = balanco
            else:
                balanco = ultimo_balanco_com_valor
        try:
            days_to_sum = dt_aux - balanco[0][0]
        except:
            logger.error("Sem dados de balanco do dessem, verificar dag DOWNLOAD CCEE")
        df_balanco = pd.DataFrame(balanco, columns = ['dt_data_hora', 'str_subsistema', 'vl_cmo', 'vl_demanda','vl_geracao_renovaveis', 'vl_geracao_hidreletrica','vl_geracao_termica', 'vl_gtmin', 'vl_gtmax', 'vl_intercambio','vl_pld'])
        df_geracao_termica_min = df_balanco[df_balanco['dt_data_hora'] < dt_aux + datetime.timedelta(days=1) ][['dt_data_hora','str_subsistema','vl_gtmin']]
        df_geracao_termica_min['horario'] = df_geracao_termica_min['dt_data_hora'] + days_to_sum
    
        
        df_geracao_termica_min['str_submercado'] = df_geracao_termica_min['str_subsistema'].map(lambda x: 'SUDESTE' if x == 'SE' else 'SUL' if x == 'S' else 'NORDESTE' if x == 'NE' else 'NORTE')

        df_geracao_termica_min = df_geracao_termica_min.set_index('horario').groupby('str_submercado')
        values = df_geracao_termica_min['vl_gtmin'].resample('1H').mean().ffill()

        valores[dt_str]	= values

        dt_aux += datetime.timedelta(days=1)

    return valores


def get_prev_carga_dessem_semana(dt_prev):
    valores_carga_out = {}
    valores_carga_liquida_out = {}

    datas_preliminares = {}
    ultimo_valor_carga=''

    dt_anterior = dt_prev - datetime.timedelta(days=1)

    dt_ini_semana = wx_opweek.getLastSaturday(dt_prev)
    dt_prox_ini_semana = dt_ini_semana + datetime.timedelta(days=7)

    valores_carga_verificada = semana_carga_verificada_horaria(dt_prev)

    valores_termica_semana = get_gereracao_termica_min_semana(dt_prev)

    #verifica se os dados verificados diarios do dia está completo pelo menos até as 22h
    try:
        if valores_carga_verificada[dt_anterior.strftime("%d/%m/%Y")].index.max()[1].hour < 22:
            del valores_carga_verificada[dt_anterior.strftime("%d/%m/%Y")]
        try:
            if valores_carga_verificada[dt_prev.strftime("%d/%m/%Y")].index.max()[1].hour < 22:
                del valores_carga_verificada[dt_prev.strftime("%d/%m/%Y")]
        except:
            pass
    except:
        pass

    dt_aux = dt_ini_semana

    while dt_aux < dt_prox_ini_semana:

        dt_str = dt_aux.strftime('%d/%m/%Y')
        datas_preliminares[dt_str] = []

        fl_preliminar , valores_renovaveis = semana_deck_prev_renovaveis_dessem_horaria(dt_aux)

        
        if valores_renovaveis:
            ultimo_valor_renovaveis = valores_renovaveis
            
            if not fl_preliminar:
                print(dt_str + "ATUAL")
                datas_preliminares[dt_str] += "ATUAL",
            else:
                print(dt_str + "PRELIMINAR")
                datas_preliminares[dt_str] += "PRELIMINAR",

        else:
            valores_renovaveis = ultimo_valor_renovaveis
            print(dt_str + "PRELIMINAR")
            datas_preliminares[dt_str] += "PRELIMINAR",

        try:
            carga_verificada = valores_carga_verificada[dt_str]
            valores_carga = valores_carga_verificada
            print(dt_str + "VERIFICADO")
            datas_preliminares[dt_str] += "VERIFICADO",
        except:
            valores_carga = semana_deck_prev_carga_dessem_horaria(dt_aux)
            print(dt_str + "PREVISTO")
            datas_preliminares[dt_str] += "PREVISTO",

            if valores_carga:
                ultimo_valor_carga = valores_carga
            elif not ultimo_valor_carga:
                valores_carga = semana_deck_prev_carga_dessem_horaria(dt_aux - datetime.timedelta(days=1))
                ultimo_valor_carga = valores_carga
            else:
                valores_carga = ultimo_valor_carga

        valor_carga_liquida = valores_carga[dt_str] - valores_renovaveis[dt_str] - valores_termica_semana[dt_str]

        valores_carga_out[dt_str] = valores_carga[dt_str]
        valores_carga_liquida_out[dt_str] = valor_carga_liquida

        dt_aux += datetime.timedelta(days=1)

    return datas_preliminares, valores_carga_out, valores_carga_liquida_out



def table_media_carga_dessem_sin(dt_prev):

    medias =[]
    dt_ini_semana = wx_opweek.getLastSaturday(dt_prev)

    dt_aux = dt_ini_semana

    while dt_aux <= dt_prev:

        dt_str = dt_aux.strftime('%d/%m/%Y')

        carga = getPrevisaoCargaDs(dt_aux)
        df_carga = pd.DataFrame(carga, columns = ['cd_submerc','dt_ref','vl_carga','dt_ini','id_fonte'])

        #medias para o dia escolhido no parametro "dt_prev" de todos os decks desde o inicio da semana 
        teste = df_carga[(df_carga['dt_ref'] >= dt_prev) & (df_carga['dt_ref'] < dt_prev + datetime.timedelta(days=1))]
        media_prev_dt_prev = teste.groupby('dt_ref')['vl_carga'].sum().mean().round(2)
        
        medias += [dt_str,media_prev_dt_prev],

        dt_aux += datetime.timedelta(days=1)


    return medias



def carga_dessem_plot(dt_prev,pathFileOut):

    flag_preliminar=False

    fig = plt.figure(figsize=(16,5))
    gs = gridspec.GridSpec(2, 2, width_ratios=[3, 1])
    ax1 = plt.subplot(gs[:,0])

    File = os.path.join(pathFileOut, 'Plot_carga_ds.PNG')

    dt_anterior_prev = dt_prev - datetime.timedelta(days=1)
    dt_sem_anterior_prev = dt_prev - datetime.timedelta(days=7)
    
    color = {dt_anterior_prev:'blue',dt_sem_anterior_prev:'red',dt_prev:'green'}
    datas = [dt_prev,dt_sem_anterior_prev,dt_anterior_prev]
    

    #carga liquida prevista
    #====================================================

    str_dt = dt_prev.strftime('%d/%m/%Y')
    datas_preliminares, valores_carga, valores_carga_liquida = get_prev_carga_dessem_semana(dt_prev)

    df_carga_liquida_sin_dt_prev = valores_carga_liquida[str_dt].reset_index().groupby('horario')[0].sum().reset_index()
    df_carga_liquida_sin_dt_prev['hr'] = pd.to_datetime(df_carga_liquida_sin_dt_prev['horario']).dt.strftime('%H:%M')

    horas = pd.to_datetime(df_carga_liquida_sin_dt_prev['hr'],format='%H:%M' ).tolist()
    carga_values = df_carga_liquida_sin_dt_prev[0].tolist()	
    media = df_carga_liquida_sin_dt_prev[0].mean()

    ax1.plot(horas, carga_values, label=f"{str_dt}(Liquida) - média : {media:.2f} MW ", linestyle='-', color='green')
    x_media = [media]*len(carga_values)
    ax1.plot(horas, x_media, linewidth=.8, linestyle='--', color='green')

    ponto_mais_alto = (horas[carga_values.index(max(carga_values))], max(carga_values))
    plt.text(ponto_mais_alto[0], ponto_mais_alto[1], f'{round(ponto_mais_alto[1])}', ha='center', va='bottom')


    #tabela de pico de carga liquida
    #===================================================================

    dt_ini_semana = wx_opweek.getLastSaturday(dt_prev)
    dt_prox_ini_semana = dt_ini_semana + datetime.timedelta(days=7)
    dt_aux = dt_ini_semana

    table_liquida=[]
    while dt_aux < dt_prox_ini_semana:

        dt_str_aux = dt_aux.strftime('%d/%m/%Y')
        dt_str_table = dt_str_aux

        if "PRELIMINAR" in datas_preliminares[dt_str_aux]:
            dt_str_table += " *"
            if dt_aux == dt_prev:
                flag_preliminar = True

        if "PREVISTO" in datas_preliminares[dt_str_aux]:
            dt_str_table += "*"

        liquida_max_value_sin = valores_carga_liquida[dt_str_aux].reset_index().groupby('horario')[0].sum().max().round(2)

        media_carga_sin = valores_carga[dt_str_aux].reset_index().groupby('horario')['vl_carga'].sum().mean().round(2)

        table_liquida	+= [dt_str_table, media_carga_sin,liquida_max_value_sin],

        dt_aux += datetime.timedelta(days=1)

    #carga prevista
    #==========================================================

    for date in datas:

        str_dt_aux = date.strftime('%d/%m/%Y')

        horas,carga_values,media = prev_carga_dessem(date)

        ax1.plot(horas, carga_values, label=f"{str_dt_aux} - média : {media:.2f} MW ", linestyle='-', color=color[date])
        x_media = [media]*len(carga_values)

        ax1.plot(horas, x_media, linewidth=.8, linestyle='--', color=color[date])

    ax2 = plt.subplot(gs[0, 1])
    ax3 = plt.subplot(gs[1, 1])

    ax2.axis('off')
    ax3.axis('off')

    #Tabelas
    table_data = table_media_carga_dessem_sin(dt_prev)

    #tabela media para cada deck prevendo o dia de previsão em questão
    table_data.insert(0, [f'Data da previsão',f'Média do dia {dt_prev.strftime("%d/%m/%Y")}'])
    table = ax2.table(cellText=table_data, loc='center')
    table.scale(1, 1.5)
    
    table_pos_data = table.properties()['children'][0].get_bbox()
    text_y_data = table_pos_data.y1 +1
    ax2.annotate(f'Referente a previsão do dia {str_dt} em cada deck desde o inicio da semana',xy=(0.5, text_y_data), xycoords='axes fraction', ha='center', va='center', fontsize=6, color='black')

    #tabela media para cada dia e sua carga liquida
    table_liquida.insert(0, [f'Dia da Semana','Média de Carga','Pico Carga Liquida'])
    table = ax3.table(cellText=table_liquida, loc='center')
    table.scale(1, 1.5)
    ax3.annotate('* Previsão e Carga Liquida | ** Previsão e Carga Liquida preliminar'  , xy=(0.5, -0.15), xycoords='axes fraction', ha='center', va='center', fontsize=6, color='black')

    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax1.xaxis.set_major_locator(mdates.HourLocator(interval=1))

    fig.suptitle("Carga Horária - SIN ", fontsize=16)
    ax1.set_xlabel("Horário")
    ax1.set_ylabel("Carga (MW)")
    ax1.tick_params(axis='x',rotation=45)
    ax1.grid(True)
    ax1.legend(loc='upper left')
    plt.savefig(File)   

    return File,flag_preliminar


def get_df_info_rdh(dt_ini:datetime,num_dias:int=0,tipos:str=['ear'],data_type:str='Ano',granularidade:str='posto'):

    db_ons = wx_dbClass.db_mysql_master('db_ons')
    db_ons.connect()

    if granularidade == 'posto':
        tb_rdh = db_ons.getSchema('tb_rdh')
        tb_postos_completo = db_ons.getSchema('tb_postos_completo')

        tb_rdh_granularidade, tb_granularidade = tb_rdh.c.cd_posto ,tb_postos_completo.c.cd_posto
        columns_fetch_table = [tb_postos_completo.c.cd_posto,tb_postos_completo.c.str_posto,tb_rdh.c.dt_referente]

        tipos_corverter= {
            'ear':'vl_vol_arm_perc',
            'vaz_turbinada':'vl_vaz_turb',
            'vaz_afluente': 'vl_vaz_afl',
            'vaz_defluente': 'vl_vaz_dfl',
            'vaz_incremental': 'vl_vaz_inc',
            'vaz_vertida' : 'vl_vaz_vert'
            }

    elif granularidade == 'submercado':

        tb_rdh = db_ons.getSchema('tb_rdh_submercado')
        tb_submercado = db_ons.getSchema('tb_submercado')
        
        tb_rdh_granularidade, tb_granularidade = tb_rdh.c.cd_submercado ,tb_submercado.c.cd_submercado
        columns_fetch_table = [ tb_submercado.c.cd_submercado,tb_submercado.c.str_submercado,tb_rdh.c.dt_referente]
        
        tipos_corverter= {
            'ear':'vl_vol_arm_perc'
            }
    
    columns = ['cd_granularidade','str_granularidade', 'dt_referente']
    for tipo in tipos:
        coluna = tipos_corverter[tipo]
        columns_fetch_table+= tb_rdh.c[coluna],
        columns += tipo,

    sql_select = db.select(
            columns_fetch_table
        ).join(
            tb_rdh, tb_rdh_granularidade == tb_granularidade
            )
    
    if data_type == 'ano': 
        sql_select = sql_select.where(db.extract('year', tb_rdh.c.dt_referente) == dt_ini.year)

    elif data_type == 'dia':
        sql_select = sql_select.where(
            tb_rdh.c.dt_referente >= dt_ini.strftime("%y-%m-%d"),
            tb_rdh.c.dt_referente <= (dt_ini+ datetime.timedelta(days=num_dias))
            )
    values = db_ons.conn.execute(sql_select).fetchall()

    df_rdh = pd.DataFrame(values, columns=columns)
    df_rdh['dt_referente'] = pd.to_datetime(df_rdh['dt_referente']).dt.strftime('%Y-%m-%d')
    df_rdh['str_granularidade'] = df_rdh['str_granularidade'].apply(lambda x: unidecode(x))

    if df_rdh.empty:
        return None
    
    return df_rdh


def get_pesos_agrupados_ec(dtRef:datetime):

    db_rodadas = wx_dbClass.db_mysql_master('db_rodadas')
    db_rodadas.connect()
    
    tb_pesos_grupos_ecmwf = db_rodadas.getSchema('tb_pesos_grupos_ecmwf')
    tb_cadastro_rodadas = db_rodadas.getSchema('tb_cadastro_rodadas')


    sql_select = db.select(
            tb_pesos_grupos_ecmwf.c.vl_peso_g1,
            tb_pesos_grupos_ecmwf.c.vl_peso_g2,
            tb_pesos_grupos_ecmwf.c.vl_peso_g3,
            tb_pesos_grupos_ecmwf.c.vl_peso_g4,
            tb_pesos_grupos_ecmwf.c.vl_peso_g5,
            tb_pesos_grupos_ecmwf.c.vl_peso_g6,
            tb_pesos_grupos_ecmwf.c.vl_peso_g7,
            tb_pesos_grupos_ecmwf.c.vl_peso_g8,
            tb_pesos_grupos_ecmwf.c.vl_peso_g9,
            tb_pesos_grupos_ecmwf.c.vl_peso_g10
        ).join(
            tb_cadastro_rodadas, tb_pesos_grupos_ecmwf.c.id_deck == tb_cadastro_rodadas.c.id
        ).where(
            tb_cadastro_rodadas.c.dt_rodada == dtRef)
    
    values = db_rodadas.conn.execute(sql_select).fetchall()
    
    values_dict = {}
    
    # Retorna apenas se achar 1
    if len(values) == 1:
        for idx, prob in enumerate(values[0]):
            values_dict[f'grupo_{idx+1}'] = prob
            
    return values_dict
    

if __name__ == '__main__':


    getGeracaoHoraria(datetime.datetime.now())

    # testesUnitarios()
    # quit()

    year = 2020
    month = 11
    rev = 1

    sql = '''SELECT CD_RODADA,TX_COMENTARIO,DT_RODADA,FL_EXCLUIDA
                FROM TB_CADASTRO_RODADA_DECOMP
                WHERE   VL_ANO = {} AND 
                        VL_MES = {} AND 
                        VL_REVISAO = {}
                ORDER BY CD_RODADA DESC'''.format(year, month, rev)
    # datab = WxDataB()
    # rows = datab.requestServer(sql)
    # for row in rows:
    #     print(row)
    # datab.closeConnection()

    sql = "INSERT INTO customers (name, address) VALUES (%s, %s)"
    val = ("John", "Highway 21")
    # datab.execute(sql, val)

    sql = "SELECT * from customers where name = %s and address = %s"
    val = ("John", "Highway 21")
    # datab.execute(sql, val)
    

    sql = "INSERT INTO customers (name, address) VALUES (%s, %s)"
    val = [
    ('Peter', 'Lowstreet 4'),
    ('Amy', 'Apple st 652'),
    ('Hannah', 'Mountain 21')
    ]
    # datab.executemany(sql, val)

