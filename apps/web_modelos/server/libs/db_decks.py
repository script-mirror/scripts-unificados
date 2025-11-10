import pdb
import datetime
import pandas as pd
import sys
import sqlalchemy as db
sys.path.insert(1,"/WX2TB/Documentos/fontes/PMO/scripts_unificados/")
from bibliotecas.wx_dbClass import db_mysql_master


def nome_coluna_para_coluna_tabela(table: db.Table, column: list):
    if column in table.c:
        table_column = table.c[column]
        return table_column
    else:
        print(f'coluna {column} nao existe na tabela {table.alias()}')
        return None


def get_colunas_tabela(tabela: db.Table, colunas_index:db.Column, colunas:db.Column):
    return list([nome_coluna_para_coluna_tabela(tabela, x) for x in colunas_index]) + list([nome_coluna_para_coluna_tabela(tabela, x) for x in colunas])

def query_para_dict(query, colunas_index, colunas):
    DB_DECKS = db_mysql_master('db_decks')
    DB_DECKS.connect()
    result = DB_DECKS.conn.execute(query)
    result = result.all()

    df = pd.DataFrame(result, columns=colunas_index+colunas)
    df['dt_data_hora'] = [x.strftime('%Y-%m-%d %H:%M:%S') for x in df['dt_data_hora']]
    
    valores:list = []
    for coluna in colunas:
        valor = dict(zip(df['dt_data_hora'], df[coluna]))
        valores.append(valor)
    colunas = [x.replace('vl_', '') for x in colunas]
    
    return dict(zip(colunas, valores))

def ajustar_datas(inicio:datetime.datetime, fim:datetime.datetime):
    return inicio, fim.replace(hour=23)

class tb_balanco_dessem():
    def datas_atualizadas_dessem():
        DB_DECKS = db_mysql_master('db_decks')
        DB_DECKS.connect()
        balanco_dessem = DB_DECKS.getSchema('tb_balanco_dessem')

        query = db.select(
            db.sql.func.min(balanco_dessem.c['dt_data_hora']),
            db.sql.func.max(balanco_dessem.c['dt_data_hora']),
            )
        result = DB_DECKS.conn.execute(query)
        result = result.all()
        df = pd.DataFrame(result, columns=['dt_inicial', 'dt_final'])
        df['dt_inicial'] = [x.strftime('%Y-%m-%d') for x in df['dt_inicial']]
        df['dt_final'] = [x.strftime('%Y-%m-%d') for x in df['dt_final']]
        return dict(zip(df.columns.to_list(), df.values.tolist()[0]))
    
    def range_datas():
        DB_DECKS = db_mysql_master('db_decks')
        DB_DECKS.connect()
        balanco_dessem = DB_DECKS.getSchema('tb_balanco_dessem')

        query = db.select(
            db.distinct(db.sql.func.date(balanco_dessem.c['dt_data_hora']))
        )
        result = DB_DECKS.conn.execute(query).all()
        df = pd.DataFrame(result, columns=['data'])
        return {'range_datas': [x.strftime('%Y-%m-%d') for x in df['data']]}
    
    def demanda_geracao_intercambio_total_data_entre(inicio:datetime.datetime, fim:datetime.datetime):
        DB_DECKS = db_mysql_master('db_decks')
        DB_DECKS.connect()
        balanco_dessem = DB_DECKS.getSchema('tb_balanco_dessem')
        inicio, fim = ajustar_datas(inicio, fim)
        resposta:dict = {'subsistema':'todos'}
        colunas_index:list = ['dt_data_hora']
        colunas:list = ['vl_demanda', 'vl_geracao_renovaveis', 'vl_geracao_hidreletrica', 'vl_geracao_termica', 'vl_intercambio']

        query = db.select(
            balanco_dessem.c['dt_data_hora'],
            db.sql.func.sum(balanco_dessem.c['vl_demanda']),
            db.sql.func.sum(balanco_dessem.c['vl_geracao_renovaveis']),
            db.sql.func.sum(balanco_dessem.c['vl_geracao_hidreletrica']),
            db.sql.func.sum(balanco_dessem.c['vl_geracao_termica']),
            db.sql.func.sum(balanco_dessem.c['vl_intercambio'])
            ).where(
                balanco_dessem.c.dt_data_hora.between(inicio, fim)
        ).group_by(balanco_dessem.c['dt_data_hora'])

        resposta.update(query_para_dict(query, colunas_index, colunas))
        return resposta

    def demanda_geracao_intercambio_por_subsistema_data_entre(subsistema: str,inicio:datetime.datetime, fim:datetime.datetime):
        DB_DECKS = db_mysql_master('db_decks')
        DB_DECKS.connect()
        balanco_dessem = DB_DECKS.getSchema('tb_balanco_dessem')
        inicio, fim = ajustar_datas(inicio, fim)
        resposta:dict = {'subsistema': subsistema}
        colunas_index:list = ['dt_data_hora', 'str_subsistema']
        colunas:list = ['vl_demanda', 'vl_geracao_renovaveis', 'vl_geracao_hidreletrica', 'vl_geracao_termica', 'vl_intercambio']

        query = db.select(
            balanco_dessem.c['dt_data_hora'],
            balanco_dessem.c['str_subsistema'],
            balanco_dessem.c['vl_demanda'],
            balanco_dessem.c['vl_geracao_renovaveis'],
            balanco_dessem.c['vl_geracao_hidreletrica'],
            balanco_dessem.c['vl_geracao_termica'],
            balanco_dessem.c['vl_intercambio']
            ).where(
                db.and_(
                balanco_dessem.c['dt_data_hora'].between(inicio, fim),
                balanco_dessem.c['str_subsistema'] == subsistema
            )
        )

        resposta.update(query_para_dict(query, colunas_index,colunas))

        return resposta
    def total_geracao_data_hora_entre(inicio:datetime.datetime, fim:datetime.datetime):
        DB_DECKS = db_mysql_master('db_decks')
        DB_DECKS.connect()
        balanco_dessem = DB_DECKS.getSchema('tb_balanco_dessem')
        inicio, fim = ajustar_datas(inicio, fim)
        colunas:list = ['geracao_renovaveis', 'geracao_hidreletrica', 'geracao_termica']

        query = db.select(
            db.sql.func.sum(balanco_dessem.c['vl_geracao_renovaveis']),
            db.sql.func.sum(balanco_dessem.c['vl_geracao_hidreletrica']),
            db.sql.func.sum(balanco_dessem.c['vl_geracao_termica']),
            ).where(
                balanco_dessem.c.dt_data_hora.between(inicio, fim)
        )
        result = DB_DECKS.conn.execute(query)
        result = result.all()

        return dict(zip(colunas, result[0])) 

    def total_geracao_por_subsistema_data_hora_entre(subsistema: str,inicio:datetime.datetime, fim:datetime.datetime):
        DB_DECKS = db_mysql_master('db_decks')
        DB_DECKS.connect()
        balanco_dessem = DB_DECKS.getSchema('tb_balanco_dessem')
        inicio, fim = ajustar_datas(inicio, fim)
        colunas:list = ['geracao_renovaveis', 'geracao_hidreletrica', 'geracao_termica']

        query = db.select(
            db.sql.func.sum(balanco_dessem.c['vl_geracao_renovaveis']),
            db.sql.func.sum(balanco_dessem.c['vl_geracao_hidreletrica']),
            db.sql.func.sum(balanco_dessem.c['vl_geracao_termica']),
            ).where(
                db.and_(
                balanco_dessem.c.dt_data_hora.between(inicio, fim),
                balanco_dessem.c.str_subsistema == subsistema
            )
        )
        result = DB_DECKS.conn.execute(query)
        result = result.all()

        return dict(zip(colunas, result[0])) 

    def pld_cmo_por_subsistema_data_entre(subsistema:str, inicio:datetime.datetime, fim:datetime.datetime):
        DB_DECKS = db_mysql_master('db_decks')
        DB_DECKS.connect()
        balanco_dessem = DB_DECKS.getSchema('tb_balanco_dessem')
        inicio, fim = ajustar_datas(inicio, fim)
        resposta:dict = {'subsistema': subsistema}
        colunas_index:list = ['dt_data_hora', 'str_subsistema']
        colunas:list = ['vl_pld', 'vl_cmo']

        query = db.select(
            balanco_dessem.c['dt_data_hora'],
            balanco_dessem.c['str_subsistema'],
            balanco_dessem.c['vl_pld'],
            balanco_dessem.c['vl_cmo']
            ).where(
                db.and_(
                balanco_dessem.c['dt_data_hora'].between(inicio, fim),
                balanco_dessem.c['str_subsistema'] == subsistema
            )
        )

        resposta.update(query_para_dict(query, colunas_index,colunas))

        return resposta
    def pld_cmo_data_entre(inicio:datetime.datetime, fim:datetime.datetime):
        SUBSISTEMAS = ['N', 'NE', 'S', 'SE']
        DB_DECKS = db_mysql_master('db_decks')
        DB_DECKS.connect()
        balanco_dessem = DB_DECKS.getSchema('tb_balanco_dessem')
        inicio, fim = ajustar_datas(inicio, fim)
        resposta:dict = {}
        colunas_index:list = ['dt_data_hora', 'str_subsistema']
        colunas:list = ['pld', 'cmo']

        query = db.select(
            balanco_dessem.c['dt_data_hora'],
            balanco_dessem.c['str_subsistema'],
            balanco_dessem.c['vl_pld'],
            balanco_dessem.c['vl_cmo']
            ).where(
                db.and_(
                balanco_dessem.c['dt_data_hora'].between(inicio, fim)
            )
        )

        result = DB_DECKS.conn.execute(query)
        result = result.all()
        df = pd.DataFrame(result, columns=colunas_index+colunas)
        df['dt_data_hora'] = [x.strftime('%Y-%m-%d %H:%M:%S') for x in df['dt_data_hora']]

        df_groupby_subsistema = df.groupby(['str_subsistema'])
        for coluna in colunas:
            valores:list = []
            for subsistema in SUBSISTEMAS:
                valor = dict(zip(df_groupby_subsistema.get_group(subsistema)['dt_data_hora'], df_groupby_subsistema.get_group(subsistema)[coluna]))
                valores.append(valor)
                resposta[coluna] = (dict(zip(SUBSISTEMAS, valores)))

        return resposta

    def geracao_termica_data_entre(inicio:datetime.datetime, fim:datetime.datetime):
        SUBSISTEMAS = ['N', 'NE', 'S', 'SE']
        DB_DECKS = db_mysql_master('db_decks')
        DB_DECKS.connect()
        balanco_dessem = DB_DECKS.getSchema('tb_balanco_dessem')
        inicio, fim = ajustar_datas(inicio, fim)
        resposta:dict = {}
        colunas_index_gtmax_sin:list = ['dt_data_hora']
        colunas_gtmax_sin:list = ['gtmax', 'vl_geracao_termica']
        
        query_gtmax_sin = db.select(
            balanco_dessem.c['dt_data_hora'],
            db.sql.func.sum(balanco_dessem.c['vl_gtmax']),
            db.sql.func.sum(balanco_dessem.c['vl_geracao_termica'])
            ).where(
                db.and_(
                balanco_dessem.c['dt_data_hora'].between(inicio, fim),
            )
        ).group_by(balanco_dessem.c['dt_data_hora'])
        resposta.update(query_para_dict(query_gtmax_sin, colunas_index_gtmax_sin,colunas_gtmax_sin))

        colunas_index_gtmin_subsistema:list = ['data_hora', 'subsistema']
        colunas_gtmin_subsistema:list = ['gtmin']

        query_gtmin_subsitema = db.select(
            balanco_dessem.c['dt_data_hora'],
            balanco_dessem.c['str_subsistema'],
            db.sql.func.sum(balanco_dessem.c['vl_gtmin'])
            ).where(
                db.and_(
                balanco_dessem.c['dt_data_hora'].between(inicio, fim),
            )
        ).group_by(balanco_dessem.c['dt_data_hora'], balanco_dessem.c['str_subsistema'])


        result = DB_DECKS.conn.execute(query_gtmin_subsitema)
        result = result.all()
        df = pd.DataFrame(result, columns=colunas_index_gtmin_subsistema+colunas_gtmin_subsistema)
        df['data_hora'] = [x.strftime('%Y-%m-%d %H:%M:%S') for x in df['data_hora']]
        df_groupby_subsistema = df.groupby(['subsistema'])
        valores:list = []
        for subsistema in SUBSISTEMAS:
            valor = dict(zip(df_groupby_subsistema.get_group(subsistema)['data_hora'], df_groupby_subsistema.get_group(subsistema)['gtmin']))
            valores.append(valor)
        resposta.update(dict(zip(SUBSISTEMAS, valores)))

        return resposta
class tb_intercambio_dessem():

    def limites_intercambio_por_nome_re_data_entre(nome_re:str, inicio:datetime.datetime, fim:datetime.datetime):
        DB_DECKS = db_mysql_master('db_decks')
        DB_DECKS.connect()
        intercambio_dessem = DB_DECKS.getSchema('tb_intercambio_dessem')
        resposta: dict = {'nome_re': nome_re}
        inicio, fim = ajustar_datas(inicio, fim)
        colunas_index:list = ['dt_data_hora', 'str_nomeRE']
        colunas:list = ['vl_limite_inferior', 'vl_limite_superior', 'vl_limite_utilizado']
        query = db.select(
            intercambio_dessem.c['dt_data_hora'],
            intercambio_dessem.c['str_nomeRE'],
            intercambio_dessem.c['vl_limite_inferior'],
            intercambio_dessem.c['vl_limite_superior'],
            intercambio_dessem.c['vl_limite_utilizado']
            ).where(
                db.and_(
                intercambio_dessem.c['dt_data_hora'].between(inicio, fim),
                intercambio_dessem.c['str_nomeRE'] == nome_re
            )
        )
        resposta.update(query_para_dict(query, colunas_index,colunas))
        return resposta
    def limites_intercambio_data_entre(inicio:datetime.datetime, fim:datetime.datetime):
        DB_DECKS = db_mysql_master('db_decks')
        DB_DECKS.connect()
        intercambio_dessem = DB_DECKS.getSchema('tb_intercambio_dessem')
        inicio, fim = ajustar_datas(inicio, fim)
        colunas_index:list = ['dt_data_hora']
        colunas:list = ['vl_limite_inferior', 'vl_limite_superior', 'vl_limite_utilizado']
        query = db.select(
            intercambio_dessem.c['dt_data_hora'],
            intercambio_dessem.c['vl_limite_inferior'],
            intercambio_dessem.c['vl_limite_superior'],
            intercambio_dessem.c['vl_limite_utilizado']
            ).where(
                db.and_(
                intercambio_dessem.c['dt_data_hora'].between(inicio, fim)
            )
        )
        return query_para_dict(query, colunas_index,colunas)
    def get_nomes_re():
        DB_DECKS = db_mysql_master('db_decks')
        DB_DECKS.connect()
        intercambio_dessem = DB_DECKS.getSchema('tb_intercambio_dessem')
        query = db.select(
            intercambio_dessem.c['str_nomeRE']
        ).distinct()
        result = DB_DECKS.conn.execute(query)
        result = result.all()
        df = pd.DataFrame(result, columns=['str_nomeRE'])
        resposta = {}
        resposta['nomesRE'] =list(df['str_nomeRE'])
        return resposta


    
if __name__ == '__main__':
    # print(teste.geracao_termica_data_entre(datetime.datetime(2024, 1, 25, 0), datetime.datetime(2024, 1, 25, 23)))
    # print(teste.demanda_geracao_intercambio_total_data_entre(datetime.datetime(2024, 4, 16, 0), datetime.datetime(2024, 4, 16, 23)))
    # print(teste.limites_intercambio_por_nome_re_data_entre('(-)FNE',datetime.datetime(2023, 1, 16, 0), datetime.datetime(2024, 4, 16, 23)))
    # print(teste.get_nome_re())
    pass