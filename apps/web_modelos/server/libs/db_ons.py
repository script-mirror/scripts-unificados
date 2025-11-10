import sys
import pdb
import datetime
import sqlalchemy as db

import pandas as pd
from numpy import array

import warnings
warnings.filterwarnings("ignore")

sys.path.insert("/WX2TB/Documentos/fontes/PMO/scripts_unificados/")
from bibliotecas.wx_dbClass import db_mysql_master

def listar_datas():
    df = pd.date_range(datetime.datetime(2020, 1, 1), datetime.datetime.now(), freq='D').strftime('%Y-%m-%d')
    return {'datas' : array(df).tolist()}
class tb_submercado:
    @staticmethod
    def listar():
        DB_ONS = db_mysql_master('db_ons')
        submercado = DB_ONS.getSchema('tb_submercado')
        query = db.select(
            submercado
        )
        result = DB_ONS.db_execute(query).all()

        df = pd.DataFrame(result, columns=['codigo', 'nome', 'sigla'])
        return df.to_dict('records')

class tb_estado:
    def dados_join_submercado(submercado):
        DB_DADOS_ABERTOS = db_mysql_master('db_ons_dados_abertos')
        estado = DB_DADOS_ABERTOS.getSchema('tb_estado')
        df_submercado = pd.DataFrame(tb_submercado.listar())
        submercado = df_submercado[df_submercado['nome'].str.lower() == submercado.lower()]['codigo'].iloc[0] if submercado != '%' else submercado
        return estado, df_submercado, submercado
    

class tb_restricoes_coff:
    @staticmethod
    def buscar_geracao_limitada_por_data_entre(tb:str, data_inicio:datetime.datetime, data_fim:datetime.datetime, cd_restricao:str, submercado:str  = '%', pivot = False):
        dict_tb = {'eolica':'tb_restricoes_coff_eolica', 'solar':'tb_restricoes_coff_solar'}
        tb = dict_tb[tb]
        DB_DADOS_ABERTOS = db_mysql_master('db_ons_dados_abertos')
        restricoes_coff = DB_DADOS_ABERTOS.getSchema(tb)
        if submercado.lower() in ['%', 'sin']:
            query = db.select(
            restricoes_coff.c['dt_data_hora'],
            db.sql.func.sum(restricoes_coff.c['vl_geracao']),
            db.sql.func.sum(restricoes_coff.c['vl_geracao_referencia'])
            ).where(db.and_(
                restricoes_coff.c['cd_razao_restricao'] == cd_restricao,
                restricoes_coff.c['dt_data_hora'].between(data_inicio.replace(hour=0, minute=0, second=0), data_fim.replace(hour=23, minute=59, second=59))
            )
            ).group_by(
                restricoes_coff.c['dt_data_hora'],
            )
            
            result = DB_DADOS_ABERTOS.db_execute(query)
            result = result.all()
            df_sin = pd.DataFrame(result, columns=['dt_data_hora', 'geracao', 'referencia'])
            df_sin['valor'] = df_sin['referencia'] - df_sin['geracao']
            df_sin['valor'][df_sin['valor'] < 0] = 0 
            
            df_sin['dt_data_hora'] = pd.to_datetime(df_sin['dt_data_hora'].dt).strftime('%Y-%m-%d %H:%M:%S')
            df_sin['id_submercado'] = 'SIN'
            
            df_sin = df_sin[['id_submercado', 'dt_data_hora', 'valor']]
            
            if submercado.lower() == 'sin':
                df_sin = df_sin.sort_values('dt_data_hora')
                return df_sin.to_dict('records')
        
        estado, df_submercado, submercado = tb_estado.dados_join_submercado(submercado)
        
        query = db.select(
            estado.c['id_submercado'],
            restricoes_coff.c['dt_data_hora'],
            db.sql.func.sum(restricoes_coff.c['vl_geracao']),
            db.sql.func.sum(restricoes_coff.c['vl_geracao_referencia'])
            ).join(
                estado, estado.c['id_estado']==restricoes_coff.c['id_estado']
            ).where(db.and_(
                estado.c['id_submercado'].like(submercado),
                restricoes_coff.c['cd_razao_restricao'] == cd_restricao,
                restricoes_coff.c['dt_data_hora'].between(data_inicio.replace(hour=0, minute=0, second=0), data_fim.replace(hour=23, minute=59, second=59))
            )
            ).group_by(
                estado.c['id_submercado'],
                restricoes_coff.c['dt_data_hora'],
            )
            
        result = DB_DADOS_ABERTOS.db_execute(query)
        result = result.all()
        df = pd.DataFrame(result, columns=['id_submercado', 'dt_data_hora', 'geracao', 'referencia'])
        df['valor'] = df['referencia'] - df['geracao']
        df['valor'][df['valor'] < 0] = 0 
        df = df[['id_submercado', 'dt_data_hora', 'valor']]
        if df.empty:
            return [{}]
        df['id_submercado'].replace(df_submercado['codigo'].tolist(), df_submercado['nome'].tolist(), inplace=True)
        df['dt_data_hora'] = df['dt_data_hora'].dt.strftime('%Y-%m-%d %H:%M:%S')
        if pivot:
            return df.pivot(index='dt_data_hora', columns='id_submercado', values='valor').to_dict()
        
        if submercado == '%':
            df = pd.concat([df, df_sin])
            
        df = df.sort_values('dt_data_hora')
        return df.to_dict('records')
class tb_geracao_usina:
    @staticmethod
    def buscar_por_data_entre(tb:str, data_inicio:datetime.datetime, data_fim:datetime.datetime, submercado:str  = '%'):
        dict_tb = {'eolica':'tb_geracao_usina_eolica', 'solar':'tb_geracao_usina_solar', 'hidraulica':'tb_geracao_usina_hidraulica','nuclear':'tb_geracao_usina_nuclear', 'termica':'tb_geracao_usina_termica' }
        tb = dict_tb[tb]
        
        DB_DADOS_ABERTOS = db_mysql_master('db_ons_dados_abertos')
        
        geracao_usina = DB_DADOS_ABERTOS.getSchema(tb)
        if submercado.lower() in ['%', 'sin']:
            query = db.select(
                geracao_usina.c['dt_data_hora'],
                db.sql.func.sum(geracao_usina.c['vl_geracao'])
            ).where(db.and_(
                    geracao_usina.c['dt_data_hora'].between(data_inicio.replace(hour=0, minute=0, second=0), data_fim.replace(hour=23, minute=59, second=59)))
                ).group_by(
                    geracao_usina.c['dt_data_hora'],
                )
            result = DB_DADOS_ABERTOS.db_execute(query)
            result = result.all()
            df_sin = pd.DataFrame(result, columns=['dt_data_hora', 'valor'])
            if df_sin.empty:
                return [{}]
            df_sin['dt_data_hora'] = pd.to_datetime(df_sin['dt_data_hora'].dt).strftime('%Y-%m-%d %H:%M:%S')
            df_sin['id_submercado'] = 'SIN'
            
            if submercado.lower() == 'sin':
                return df_sin.to_dict('records')
            

        estado, df_submercado, submercado = tb_estado.dados_join_submercado(submercado)
        query = db.select(
            estado.c['id_submercado'],
            geracao_usina.c['dt_data_hora'],
            db.sql.func.sum(geracao_usina.c['vl_geracao'])
        ).join(
            estado, estado.c['id_estado']==geracao_usina.c['id_estado']
        ).where(db.and_(
                geracao_usina.c['dt_data_hora'].between(data_inicio.replace(hour=0, minute=0, second=0), data_fim.replace(hour=23, minute=59, second=59)),
                estado.c['id_submercado'].like(submercado)
            )
            ).group_by(
                estado.c['id_submercado'],
                geracao_usina.c['dt_data_hora'],
            )
        result = DB_DADOS_ABERTOS.db_execute(query)
        result = result.all()
        df = pd.DataFrame(result, columns=['id_submercado', 'dt_data_hora', 'valor'])
        if df.empty:
            return [{}]
        df['id_submercado'].replace(df_submercado['codigo'].tolist(), df_submercado['nome'].tolist(), inplace=True)
        df['dt_data_hora'] = df['dt_data_hora'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        if submercado == '%':
            df = pd.concat([df, df_sin])
        return df.to_dict('records')
    
    @staticmethod
    def buscar_total_por_data_entre(data_inicio:datetime.datetime, data_fim:datetime.datetime, submercado:str  = '%'):
        tabelas =  ['eolica', 'solar', 'hidraulica','nuclear', 'termica' ]
        df = pd.DataFrame(columns=['dt_data_hora', 'id_submercado'])
        df.set_index(['dt_data_hora', 'id_submercado'], inplace=True)
        
        for tabela in tabelas:
            df2 = pd.DataFrame(tb_geracao_usina.buscar_por_data_entre(tabela, data_inicio, data_fim, submercado)).set_index(['dt_data_hora', 'id_submercado'])
            df = df.add(df2, fill_value=0)
        return df.reset_index().to_dict('records')
    
class tb_carga:
    @staticmethod
    def buscar_carga_global_por_data_entre(data_inicio:datetime.datetime, data_fim:datetime.datetime, submercado:str  = '%'):
        DB_DADOS_ABERTOS = db_mysql_master('db_ons_dados_abertos')
        
        carga = DB_DADOS_ABERTOS.getSchema('tb_carga')

        if submercado.lower() in ['%', 'sin']:
            query = db.select(
                carga.c['dt_data_hora'],
                db.sql.func.sum(carga.c['vl_carga_global'])
            ).where(db.and_(
                    carga.c['dt_data_hora'].between(data_inicio.replace(hour=0, minute=0, second=0), data_fim.replace(hour=23, minute=59, second=59))
                )
                ).group_by(
                    carga.c['dt_data_hora']
                )

            result = DB_DADOS_ABERTOS.db_execute(query)
            result = result.all()
            df_sin = pd.DataFrame(result, columns=['dt_data_hora', 'valor'])
            if df_sin.empty:
                return [{}]
            df_sin['dt_data_hora'] = pd.to_datetime(df_sin['dt_data_hora'].dt).strftime('%Y-%m-%d %H:%M:%S')
            df_sin['id_submercado'] = 'SIN'
            
            if submercado.lower() == 'sin':
                return df_sin.to_dict('records')

        df_submercado = pd.DataFrame(tb_submercado.listar())
        submercado = df_submercado[df_submercado['nome'].str.lower() == submercado.lower()]['codigo'].iloc[0] if submercado != '%' else submercado
        
        query = db.select(
            carga.c['id_subsistema'],
            carga.c['dt_data_hora'],
            carga.c['vl_carga_global']
        ).where(db.and_(
                carga.c['dt_data_hora'].between(data_inicio.replace(hour=0, minute=0, second=0), data_fim.replace(hour=23, minute=59, second=59)),
                carga.c['id_subsistema'].like(submercado)
            ))
        
        result = DB_DADOS_ABERTOS.db_execute(query)
        result = result.all()
        df = pd.DataFrame(result, columns=['id_submercado', 'dt_data_hora', 'valor'])
        if df.empty:
            return [{}]
        df['id_submercado'].replace(df_submercado['codigo'].tolist(), df_submercado['nome'].tolist(), inplace=True)
        df['dt_data_hora'] = df['dt_data_hora'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        if submercado == '%':
            df = pd.concat([df, df_sin])
        
        return df.to_dict('records')
    


if __name__ == '__main__':
    # print(tb_submercado.listar())
    # tb_submercado.listar()
    print(tb_geracao_usina.buscar_por_data_entre('tb_geracao_usina_solar', data_inicio=datetime.datetime(2024,6,15),data_fim=datetime.datetime(2024,6,22)))