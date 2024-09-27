import sys
import pdb
import sqlalchemy as db
import pandas as pd
import locale 
import datetime



sys.path.append('/WX2TB/Documentos/fontes/')
from PMO.scripts_unificados.bibliotecas.wx_dbClass import db_mysql_master
from PMO.scripts_unificados.apps.web_modelos.server.libs import db_config

class tb_negociacoes:
  @staticmethod
  def get_datahora_ultima_negociacao():
    __DB_BBCE = db_mysql_master('bbce')
    negociacoes = __DB_BBCE.getSchema('tb_negociacoes')
    query = db.select(
      db.func.max(
        negociacoes.c['dt_criacao']
      )
    )
    result = __DB_BBCE.db_execute(query).fetchall()
    df = pd.DataFrame(result, columns=['ultimo_update'])
    df['ultimo_update'] = df['ultimo_update'].dt.strftime('%Y-%m-%d %H:%M')
    return df.to_dict('records')[0]
    

  
class tb_negociacoes_resumo:
  @staticmethod
  def get_negociacao_bbce(produto:str):
    __DB_BBCE = db_mysql_master('bbce')
    negociacoes_resumo = __DB_BBCE.getSchema('tb_negociacoes_resumo')
    produtos = __DB_BBCE.getSchema('tb_produtos')
    query = db.select(
      negociacoes_resumo.c['data'],
      negociacoes_resumo.c['preco_abertura'],
      negociacoes_resumo.c['preco_maximo'],
      negociacoes_resumo.c['preco_minimo'],
      negociacoes_resumo.c['preco_fechamento'],
      negociacoes_resumo.c['volume'],
      negociacoes_resumo.c['preco_medio']
      ).join(
        produtos, produtos.c['id_produto'] == negociacoes_resumo.c['id_produto']
        ).where(
          produtos.c['str_produto'] == produto
          )
    result = __DB_BBCE.db_execute(query).fetchall()
    df = pd.DataFrame(result, columns=['date', 'open', 'high', 'low', 'close', 'volume', 'preco_medio'])
    df['date'] = df['date'].astype('datetime64[ns]').dt.strftime('%Y-%m-%d')
    return df.to_dict('records')

  @staticmethod
  def spread_preco_medio_negociacoes(produto1:str, produto2:str):
    resposta1 = tb_negociacoes_resumo.get_negociacao_bbce(produto1)
    resposta2 = tb_negociacoes_resumo.get_negociacao_bbce(produto2)

    df1 = pd.DataFrame(resposta1)
    df2 = pd.DataFrame(resposta2)
    df1['date'] = pd.to_datetime(df1['date'])
    df2['date'] = pd.to_datetime(df2['date'])


    dt_fim = max(df1['date'].max(), df2['date'].max())
    dt_inicio = min(df1['date'].min(), df2['date'].min())

    df1 = df1.set_index('date')
    df2 = df2.set_index('date')

    index_datas = pd.date_range(start=dt_inicio, end=dt_fim)

    df_combinado = pd.DataFrame(index=index_datas)
    df_combinado['preco_medio_1'] = df1['preco_medio']
    df_combinado['preco_medio_2'] = df2['preco_medio']
    df_combinado.ffill(inplace=True) 
    df_combinado.bfill(inplace=True) 
    df_combinado['spread'] = df_combinado['preco_medio_2']-df_combinado['preco_medio_1']
    df_combinado  = df_combinado.reset_index()

    df_spread = df_combinado[['index','spread']]
    df_spread.loc[:,'index'] = df_spread['index'].dt.strftime('%Y-%m-%d')
    df_spread.dropna(inplace=True)
    df_spread = df_spread.rename({'index':'date'},axis=1)
    
    response = df_spread[['date', 'spread']].to_dict(orient='records')
    return response


  @staticmethod
  def get_negociacoes_fechamento_interesse_por_data(data:datetime):
    __DB_BBCE = db_mysql_master('bbce')
    negociacoes_resumo = __DB_BBCE.getSchema('tb_negociacoes_resumo')
    produtos = __DB_BBCE.getSchema('tb_produtos')
    df_produtos_interesse = pd.DataFrame(db_config.tb_produtos_bbce.get_produtos_interesse())
    df_produtos_interesse = df_produtos_interesse.rename(columns={'palavra':'produto'})
        
    cte = (
    db.select(
        produtos.c['str_produto'],
        negociacoes_resumo.c['id_produto'],
        db.func.timestamp(negociacoes_resumo.c['data'], negociacoes_resumo.c['hora_fechamento']),
        negociacoes_resumo.c['preco_fechamento'],
        db.func.row_number().over(
            partition_by=[negociacoes_resumo.c['id_produto']],
            order_by=db.desc(negociacoes_resumo.c['data'])
        ).label('row_num')
    ).join(
        produtos, produtos.c['id_produto'] == negociacoes_resumo.c['id_produto']
        )
   .where(db.and_(
        negociacoes_resumo.c['data'] <= data,
        produtos.c['str_produto'].in_(df_produtos_interesse['produto'].tolist())
        ))
    .cte('cte_groups')
    )
    query = (
        db.select([cte])
        .where(cte.c['row_num'] == 1)
        .order_by(cte.c['id_produto'])
    )
    
    result = __DB_BBCE.db_execute(query).all()
    df = pd.DataFrame(result, columns=['produto','id_produto','datetime_fechamento','preco_fechamento', 'rn'])
    df['datetime_fechamento'] = pd.to_datetime(df['datetime_fechamento']).dt.strftime('%Y-%m-%d %H:%M')
    
    df = df.round(2)
    
    df = pd.merge(df_produtos_interesse, df, on='produto', how='left')
    df['produto'] = df['produto'].str.extract(r'SE CON (.{1,}) - Preço Fixo')
    df = df.fillna('-')
    df = df[['produto', 'preco_fechamento', 'datetime_fechamento']]
    
    return df.to_dict('records')

  @staticmethod
  def get_negociacoes_interesse_por_data(data:datetime.datetime):
    __DB_BBCE = db_mysql_master('bbce')
    negociacoes_resumo = __DB_BBCE.getSchema('tb_negociacoes_resumo')
    produtos = __DB_BBCE.getSchema('tb_produtos')
    df_produtos_interesse = pd.DataFrame(db_config.tb_produtos_bbce.get_produtos_interesse())
    df_produtos_interesse = df_produtos_interesse.rename(columns={'palavra':'produto'})
    query = db.select(
      produtos.c['str_produto'],
      negociacoes_resumo.c['data'],
      negociacoes_resumo.c['preco_abertura'],
      negociacoes_resumo.c['preco_maximo'], 
      negociacoes_resumo.c['preco_minimo'],
      negociacoes_resumo.c['preco_fechamento'],
      negociacoes_resumo.c['volume'],
      negociacoes_resumo.c['preco_medio'],
      negociacoes_resumo.c['total_negociacoes'],
      negociacoes_resumo.c['hora_fechamento'],
    ).join(
        produtos, produtos.c['id_produto'] == negociacoes_resumo.c['id_produto']
        ).where(db.and_(
        negociacoes_resumo.c['data'] == data,
        produtos.c['str_produto'].in_(df_produtos_interesse['produto'].tolist())
        ))
    
    result = __DB_BBCE.db_execute(query).all()
    df = pd.DataFrame(result, columns=['produto', 'date', 'open', 'high', 'low', 'close', 'volume', 'preco_medio', 'total', 'hora_fechamento'])
    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
    df['hora_fechamento'] = df['hora_fechamento'].astype(str)
    

    df_fechamento_anterior = pd.DataFrame(tb_negociacoes_resumo.get_negociacoes_fechamento_interesse_por_data(data-datetime.timedelta(days=1)))
    df_fechamento_anterior.columns = ['produto', 'preco_fechamento_anterior', 'datetime_fechamento_anterior']
    

    
    df = pd.merge(df_produtos_interesse, df, on='produto', how='left')
    df['produto'] = df['produto'].str.extract(r'SE CON (.{1,}) - Preço Fixo')
    df = pd.merge(df_fechamento_anterior, df, on='produto', how='left')
    
    df['change_percent'] = ((df['close'] / df['preco_fechamento_anterior']) * 100) - 100
    df['change_value'] = df['close'] - df['preco_fechamento_anterior']
    
    df = df.fillna('-')
    
    df = df.round(2)
    
    return df.to_dict('records')
  

    
class tb_produtos:
  __DB_BBCE = db_mysql_master('bbce')
  produtos = __DB_BBCE.getSchema('tb_produtos')
  
  pass
if __name__ == '__main__':
  # print(tb_negociacoes.get_negociacao_bbce('SE CON MEN JUL/24 - Preço Fixo'))
  print(tb_negociacoes_resumo.get_negociacoes_interesse_por_data([datetime.datetime.now().strftime('%Y-%m-%d')]))
  pass



