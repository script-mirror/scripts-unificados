import pdb
import datetime
import pandas as pd
import sys
import sqlalchemy as db
from numpy import nan

sys.path.insert("/WX2TB/Documentos/fontes/PMO/scripts_unificados/")
from bibliotecas.wx_dbClass import db_mysql_master


class tb_inmet_estacoes():
    def listar_estacoes():
        DB_METEOROLOGIA = db_mysql_master('db_meteorologia')
        DB_METEOROLOGIA.connect()
        inmet_estacoes = DB_METEOROLOGIA.getSchema('tb_inmet_estacoes')

        query = db.select(
            inmet_estacoes.c['cd_estacao'],
            inmet_estacoes.c['str_nome'],
            inmet_estacoes.c['str_estado'],
            inmet_estacoes.c['vl_lon'],
            inmet_estacoes.c['vl_lat'],
            inmet_estacoes.c['str_bacia'],
            )
        result = DB_METEOROLOGIA.conn.execute(query)
        result = result.all()
        df = pd.DataFrame(result, columns=['cd_estacao','str_nome','str_estado','vl_lon','vl_lat','str_bacia'])
        df.fillna('', inplace=True)
        return df.to_dict('records')
    
    def count_estacoes_por_bacia():
        DB_METEOROLOGIA = db_mysql_master('db_meteorologia')
        DB_METEOROLOGIA.connect()
        inmet_estacoes = DB_METEOROLOGIA.getSchema('tb_inmet_estacoes')
        query = db.select(
                inmet_estacoes.c['str_bacia'],
                db.sql.func.count(inmet_estacoes.c['str_bacia'])
            ).group_by(
                inmet_estacoes.c['str_bacia']
                ).filter(inmet_estacoes.c['str_bacia'].isnot(None))
        result = DB_METEOROLOGIA.conn.execute(query)
        result = result.all()
        return pd.DataFrame(result, columns=['bacia','bacias_totais'])
    
    def listar_bacias():
        DB_METEOROLOGIA = db_mysql_master('db_meteorologia')
        DB_METEOROLOGIA.connect()
        inmet_estacoes = DB_METEOROLOGIA.getSchema('tb_inmet_estacoes')

        query = db.select(
            db.distinct(inmet_estacoes.c['str_bacia'])
            ).filter(inmet_estacoes.c['str_bacia'].isnot(None))
        result = DB_METEOROLOGIA.conn.execute(query)
        result = result.all()
        df = pd.DataFrame(result, columns=['nome'])
        return df.to_dict('records')
    

class tb_inmet_dados_estacoes():
    def listar_datas_coleta():
        DB_METEOROLOGIA = db_mysql_master('db_meteorologia')
        DB_METEOROLOGIA.connect()
        inmet_dados_estacoes = DB_METEOROLOGIA.getSchema('tb_inmet_dados_estacoes')
        query = db.select(
            db.distinct(inmet_dados_estacoes.c['dt_coleta'])
        ).where(
            inmet_dados_estacoes.c['dt_coleta'] <= (datetime.datetime.now() + datetime.timedelta(hours=3))
            ).filter(inmet_dados_estacoes.c['vl_chuva'].isnot(None))
        result = DB_METEOROLOGIA.conn.execute(query)
        result = result.all()
        df = pd.DataFrame(result, columns=['dt_coleta'])
        df.sort_values(by=['dt_coleta'], inplace=True)
        df['dt_coleta'] = df['dt_coleta'].dt.strftime('%Y-%m-%dT%H:%M')
        return {'datas': df['dt_coleta'].to_list()}
        
    def chuva_acumulada_por_estacao_data_entre(inicio: datetime.datetime, fim:datetime.datetime):
        DB_METEOROLOGIA = db_mysql_master('db_meteorologia')
        DB_METEOROLOGIA.connect()
        inmet_estacoes = DB_METEOROLOGIA.getSchema('tb_inmet_estacoes')
        inmet_dados_estacoes = DB_METEOROLOGIA.getSchema('tb_inmet_dados_estacoes')

        query = db.select(
            db.sql.func.sum(inmet_dados_estacoes.c['vl_chuva']),
            inmet_estacoes.c['cd_estacao'],
            inmet_estacoes.c['str_nome'],
            inmet_estacoes.c['str_estado'],
            inmet_estacoes.c['vl_lon'],
            inmet_estacoes.c['vl_lat'],
            inmet_estacoes.c['str_bacia']
            ).join(
                 inmet_estacoes, inmet_estacoes.c['cd_estacao'] == inmet_dados_estacoes.c['cd_estacao']
            ).where(
                db.and_(
                    inmet_dados_estacoes.c['dt_coleta'].between(inicio, fim)
                )
            ).group_by(inmet_dados_estacoes.c['cd_estacao'],inmet_estacoes.c['str_nome'],inmet_estacoes.c['str_estado'],inmet_estacoes.c['vl_lon'],inmet_estacoes.c['vl_lat'],inmet_estacoes.c['str_bacia'])
        result = DB_METEOROLOGIA.conn.execute(query)
        result = result.all()
        df = pd.DataFrame(result, columns=['chuva_acumulada', 'cod_estacao','nome','estado','vl_lon','vl_lat','bacia'])
        df['bacia'].fillna('', inplace=True)
        df.dropna(inplace=True)
        df['chuva_acumulada'] = df['chuva_acumulada'].round()
        df.sort_values(['chuva_acumulada'], inplace=True)

        return df.to_dict('records')
    


    def media_chuva_acumulada_por_bacia_data_entre(inicio: datetime.datetime, fim:datetime.datetime):
        df = pd.DataFrame(tb_inmet_dados_estacoes.chuva_acumulada_por_estacao_data_entre(inicio, fim))
        df = df[['bacia', 'chuva_acumulada']]
        df = df.rename(columns={'chuva_acumulada':'precipitacao_media'})
        df['bacia'] = df['bacia'].replace('',nan)
        df.dropna(inplace=True)
        df = df.groupby('bacia').agg(bacias_utilizadas=('bacia', 'size'), precipitacao_media=('precipitacao_media', 'mean')).reset_index()
        df_count_estacoes = tb_inmet_estacoes.count_estacoes_por_bacia()
        df = pd.merge(df, df_count_estacoes, on='bacia')
        df.sort_values(['precipitacao_media'], inplace=True,  ascending=False)
        df['porcentagem_bacias_utilizadas'] = df['bacias_utilizadas'] / df['bacias_totais'] * 100
        df['porcentagem_bacias_utilizadas'] = df['porcentagem_bacias_utilizadas'].round().astype(int)
        df['precipitacao_media'] = df['precipitacao_media'].round().astype(int)
        
        return df.to_dict('records')

    

if __name__ == '__main__':
    # print(tb_inmet_dados_estacoes.media_chuva_acumulada_por_bacia_data_entre(datetime.datetime(2024,6,1), datetime.datetime(2024,6,30)))
    print(tb_inmet_estacoes.listar_bacias())
    pass