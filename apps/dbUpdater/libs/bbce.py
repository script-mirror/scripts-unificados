import re
import os
import sys
import requests
import datetime
import pandas as pd
import sqlalchemy as sa
from typing import Optional
import pdb
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")),'.env'))

__USER_BBCE = os.getenv('USER_BBCE') 
__PASSWORD_BBCE = os.getenv('PASSWORD_BBCE')  
__API_URL_BBCE = os.getenv('API_URL_BBCE') 
__API_KEY_BBCE= os.getenv('API_KEY_BBCE') 
__CODE_COMPANY_BBCE= os.getenv('CODE_COMPANY_BBCE') 


path_fontes = '/WX2TB/Documentos/fontes/'
sys.path.insert(1,"/WX2TB/Documentos/fontes/PMO/scripts_unificados/")
from bibliotecas import wx_dbClass


headers = {
    'Accept': 'application/json',
    'apiKey': __API_KEY_BBCE,
    'page' : None,
    'items-per-page' : '1000',
}

__db_bbce = wx_dbClass.db_mysql_master('bbce')
tb_produtos = __db_bbce.getSchema('tb_produtos')
tb_negociacoes = __db_bbce.getSchema('tb_negociacoes')

def inserir_resumo(
        db: wx_dbClass.db_mysql_master,
        tabela:str,
        df: pd.DataFrame
        ) -> None:
    
    tabela = db.getSchema(tabela)

    insert = sa.insert(tabela).values(
        df.to_dict('records')
    )
    result = db.db_execute(insert)
    print(f'{result.rowcount} linha(s) inserida(s)')
    return None

def get_auth_token():
    json_data = {
        'companyExternalCode': __CODE_COMPANY_BBCE,
        'email': __USER_BBCE,
        'password': __PASSWORD_BBCE,
    }
    response = requests.post(f'{__API_URL_BBCE}/v2/login', headers=headers, json=json_data).json()
    
    headers['Authorization'] = f'Bearer {response["idToken"]}'
    return response

def get_produtos():
    url = f'{__API_URL_BBCE}/v2/tickers'
    payload={}
    response = requests.get(url, headers=headers, data=payload)
    response.raise_for_status()
    
    return pd.DataFrame(response.json())

def get_negociacoes(data:str):
    url = f'{__API_URL_BBCE}/v1/all-deals/report?initialPeriod={data}&finalPeriod={data}'
    payload={}

    response = requests.get(url, headers=headers, data=payload)
    response.raise_for_status()    

    return pd.DataFrame(response.json())

def inserir_produtos(produtos):
        
    if not produtos.empty:
        produtos = produtos.drop_duplicates(subset='description')
        produtos = produtos.reset_index()
        produtos['createdAt'] = pd.to_datetime(produtos['createdAt']).apply(lambda x: datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S'))
        produtos['description'] = produtos['description'].apply(lambda x: x.strip())

        ##INSERT DOS PRODUTOS   
        produtos_df = produtos['description'].unique()

        produtos_db = __db_bbce.db_execute(
            sa.select(
                [tb_produtos.c.str_produto, tb_produtos.c.id_produto]
                )
            ).fetchall()
        
        df_produtos_db = pd.DataFrame(produtos_db,columns=['str_produto', 'id_produto'])
        df_produtos_db['str_produto'] = df_produtos_db['str_produto'].apply(lambda x: x.strip())
        
        inserir=[]
        
        df_produtos_db['str_produto_filtrado'] = df_produtos_db['str_produto'].apply(lambda x: re.sub(r'[^a-zA-Z0-9]', '', x).lower())
        produtos_db_filtrado = df_produtos_db.set_index('str_produto_filtrado').to_dict()
        
        for produto in produtos_df: 
            # filtrando o nome produto para remover espaços, hifen, acentos etc...
            produto_filtrado = re.sub(r'[^a-zA-Z0-9]', '', produto).lower()
            if produto_filtrado not in produtos_db_filtrado['id_produto'].keys():
                inserir += [produto]
        
        if inserir:
            values_list = list(produtos[produtos['description'].isin(inserir)][['id','description','createdAt']].values.tolist())
            sql_insert_produtos = tb_produtos.insert().values(values_list)
            num_produtos_inseridos = __db_bbce.db_execute(sql_insert_produtos).rowcount
            print(f'{num_produtos_inseridos} Produtos inseridos')
        else: 
            print('Nenhum produto a ser inserido')
        return None

def inserir_negociacoes(negociacoes:pd.DataFrame):
    if negociacoes.empty:
        return None
    negociacoes = negociacoes[negociacoes['status']=='Ativo']
    # Match = mesa; Registro = Boleta Eletronica
    negociacoes["originOperationType"] = negociacoes["originOperationType"].replace("Registro", "Boleta Eletronica").replace("Match", "Mesa")
    

    negociacoes = negociacoes.reset_index()
    negociacoes['createdAt'] = pd.to_datetime(negociacoes['createdAt']).apply(lambda x: datetime.datetime.strftime(x,'%Y-%m-%d %H:%M:%S'))

    produtos_db = __db_bbce.db_execute(sa.select([tb_produtos.c.str_produto, tb_produtos.c.id_produto])).fetchall()
    df_produtos_db = pd.DataFrame(produtos_db, columns=['str_produto', 'id_produto'])
    df_produtos_db['str_produto'] = df_produtos_db['str_produto'].apply(lambda x: x.strip())
    produtos_db_dict = df_produtos_db.set_index('str_produto').to_dict()

    ids_produto_db = produtos_db_dict['id_produto'].values()
    ids_to_replace = negociacoes[negociacoes['productId'].isin(ids_produto_db) == False]['productId'].unique()

    #Mudança de IDS filhos (que são os mesmos produtos com ids diferentes) para o ID pai do banco de dados
    for id in ids_to_replace:
        print(f'Pesquisando ID: {id}')
        url = f'{__API_URL_BBCE}/v2/tickers/{id}'
        response = requests.get(url, headers=headers, data={})
        json = response.json()
        descrip = json['description'].strip()
        print(f'Nome do produto: {descrip}')
        try:
            original_id = produtos_db_dict['id_produto'][descrip]

            negociacoes['productId'] = negociacoes['productId'].apply(lambda x: original_id if x == id else x)
            print(f'ID {id} substituido por {original_id}\n')
        except:
            print(f'ID: {id} com nome de {descrip} Não foi encontrado na lista de tickers do banco, portanto, séra inserido como um novo produto')
            dt_cria = pd.to_datetime(json['createdAt']).strftime('%Y-%m-%d %H:%M:%S')
            sql_insert_produtos = tb_produtos.insert().values((id,descrip,dt_cria))
            num_produtos_inseridos = __db_bbce.db_execute(sql_insert_produtos).rowcount
            print(f'{num_produtos_inseridos} Produtos com mudança de id inseridos')

    print('INSERINDO NEGOCIACOES!')
    ids_df = negociacoes['id'].values
    sql_delete_negociacoes = tb_negociacoes.delete().where(tb_negociacoes.c.id_negociacao.in_(ids_df))
    num_negociacoes_deletadas = __db_bbce.db_execute(sql_delete_negociacoes).rowcount
    print(f'{num_negociacoes_deletadas} Negociaçoes deletadas')


    negociacoes = negociacoes[['id','productId','quantity','unitPrice','createdAt', 'originOperationType']].rename(columns={"id":"id_negociacao", "productId":"id_produto", "quantity":"vl_quantidade", "unitPrice":"vl_preco", "createdAt":"dt_criacao", "originOperationType":"categoria"})
    
    df_categorias = get_categorias().rename(columns={"id":"id_categoria_negociacao"})
    negociacoes = negociacoes.merge(df_categorias)
    negociacoes = negociacoes[["id_negociacao", "id_produto", "vl_quantidade", "vl_preco", "dt_criacao", "id_categoria_negociacao"]]
    sql_insert_negociacoes = tb_negociacoes.insert().values(negociacoes.to_dict("records"))
    num_negociacoes_inseridas = __db_bbce.db_execute(sql_insert_negociacoes).rowcount
    print(f'{num_negociacoes_inseridas} Negociaçoes inseridas')
    return negociacoes.to_dict("records")
        
def resumo_negociacoes(negociacoes):
    df_negociacoes = pd.DataFrame(negociacoes)
    df_negociacoes.drop(columns=['id_negociacao'])
    df_negociacoes['dt_criacao'] = pd.to_datetime(df_negociacoes['dt_criacao'])
  
    df_negociacoes['data'] = df_negociacoes['dt_criacao'].dt.date
    df_negociacoes['hora_fechamento'] = df_negociacoes['dt_criacao']

    df_negociacoes['preco_total'] = df_negociacoes['vl_preco'] * df_negociacoes['vl_quantidade']
    df_preco_medio = df_negociacoes.groupby(['id_produto','data', 'id_categoria_negociacao']).agg({'preco_total':'sum', 'vl_quantidade':'sum'})
    df_preco_medio['preco_medio'] = df_preco_medio['preco_total'] / df_preco_medio['vl_quantidade']
    grouped = df_negociacoes.groupby(['id_produto', 'data', 'id_categoria_negociacao'])

    ohlc = grouped['vl_preco'].agg(['first', 'max', 'min', 'last']).rename(
        columns={'first': 'preco_fechamento', 'max': 'preco_maximo', 'min': 'preco_minimo', 'last': 'preco_abertura'}
    )
  
    df_result = pd.concat([ohlc,
                           grouped['vl_quantidade'].sum().rename('volume'),
                           df_preco_medio['preco_medio'], 
                           grouped.size().rename('total_negociacoes'),
                           grouped['hora_fechamento'].max().dt.strftime('%H:%M:%S')], axis=1).reset_index()
    df_result['data'] = pd.to_datetime(df_result['data'], errors='coerce').dt.strftime('%Y-%m-%d')
    df_result.ffill(inplace=True)
    df_result.bfill(inplace=True)
    
    return df_result

def deletar_resumo(
        db: wx_dbClass.db_mysql_master,
        tabela:str,
        df: pd.DataFrame
        ) -> None:
    
    tabela = db.getSchema(tabela)
    delete = tabela.delete().where(tabela.c.data.in_(df['data'].tolist()))

    result = db.db_execute(delete)
    print(f'{result.rowcount} linha(s) deletada(s)')
    return None

def get_categorias() -> pd.DataFrame:
    db = wx_dbClass.db_mysql_master('bbce')
    table_id = db.getSchema("tb_categoria_negociacao")
    select_fks = sa.select(
        table_id.c['id'],
        table_id.c['nome']
        )
    result_fk = db.db_execute(select_fks)
    return pd.DataFrame(result_fk, columns=['id', 'categoria'])
   
    
def importar_operacoes_bbce(data:datetime.datetime = datetime.datetime.now()):
    data = data.strftime('%Y-%m-%d')
    get_auth_token()
    inserir_produtos(get_produtos())
    negociacoes = inserir_negociacoes(get_negociacoes(data))
    if not negociacoes:
        print(f'Nenhuma negociacao na {data}')
    else:
        df_resumo_negociacoes = resumo_negociacoes(negociacoes)
        deletar_resumo(__db_bbce, 'tb_negociacoes_resumo', df_resumo_negociacoes)
        inserir_resumo(__db_bbce, 'tb_negociacoes_resumo', df_resumo_negociacoes)

def main():
    importar_operacoes_bbce(datetime.datetime(2024, 9, 26))
    return None

if __name__ == '__main__':
    main()