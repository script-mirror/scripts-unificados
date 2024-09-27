
import os
import sys
import glob
import re
import pdb
import datetime
import pandas as pd
import sqlalchemy as db

path_fontes = "/WX2TB/Documentos/fontes/"
sys.path.insert(1,path_fontes)
from PMO.scripts_unificados.bibliotecas import wx_dbClass , wx_opweek , rz_dir_tools
from PMO.scripts_unificados.apps.decomp.libs import wx_dadger

PATH_DIR_LOCAL = os.path.dirname(os.path.abspath(__file__))

DIR_TOOLS = rz_dir_tools.DirTools()


def importar_id_deck_dc(dt_inicio_rv:datetime, str_fonte:str):

    dicionario_fonte = {'ons': 1, 'ccee':2}
     
    db_decks = wx_dbClass.db_mysql_master('db_decks')
    db_decks.connect()
    tb_cadastro_decomp = db_decks.getSchema("tb_cadastro_decomp")

    query_get_id_rodada = db.select(
        tb_cadastro_decomp.c.id
        ).where(
            tb_cadastro_decomp.c.dt_inicio_rv == dt_inicio_rv.strftime("%Y-%m-%d"), 
            tb_cadastro_decomp.c.id_fonte == dicionario_fonte[str_fonte.lower()]
            )
    answer_tb_cadastro = db_decks.conn.execute(query_get_id_rodada).scalar()

    if not answer_tb_cadastro:

        query_get_max_id_rodada = db.select(db.func.max(tb_cadastro_decomp.c.id))
        answer_tb_cadastro = db_decks.conn.execute(query_get_max_id_rodada).scalar()
        id_deck = answer_tb_cadastro + 1
        
        query_insert_id_rodada = tb_cadastro_decomp.insert().values(
            [   
                id_deck,
                dt_inicio_rv.strftime("%Y-%m-%d"),
                dicionario_fonte[str_fonte.lower()],
                None
            ]
        )
        answer = db_decks.conn.execute(query_insert_id_rodada).rowcount
        if answer:
            print(f'Rodada {id_deck} cadastrada com sucesso')
    else:
        id_deck = answer_tb_cadastro
        print(f'Rodada já estava cadastrada no banco de dados com ID {answer_tb_cadastro}!')

    db_decks.conn.close()

    return id_deck

    
def importar_dc_bloco_pq(path_zip, str_fonte='ons'):

    dt_now = datetime.datetime.now().strftime("%d%m%Y %H%M%S")
    path_dst = os.path.join(PATH_DIR_LOCAL,"tmp",dt_now)

    #o deck vem dentro de um zip com um nome padrao
    if os.path.basename(path_zip).lower() == "pmo_deck_preliminar.zip":
        DIR_TOOLS.extract(path_zip, path_dst)
        deck_entrada_zip = glob.glob(os.path.join(path_dst, 'DEC_ONS_*'))[0]
    else:
        deck_entrada_zip = path_zip

    extracted_file = DIR_TOOLS.extract_specific_files_from_zip(path=deck_entrada_zip,files_name_template=["dadger*"] ,dst=path_dst, extracted_files = [])
    path_dadger = extracted_file[0]

    match = re.match(r'DEC_ONS_([0-9]{2})([0-9]{4})_RV([0-9]{1})_VE', os.path.basename(deck_entrada_zip))
    mes = int(match.group(1))
    ano = int(match.group(2))
    rv = int(match.group(3))

    data_inicio_mes = wx_opweek.getLastSaturday(datetime.date(ano, mes, 1))
    data_inicio_rv = data_inicio_mes + datetime.timedelta(days=7*rv)

    df_dadger, comentarios = wx_dadger.leituraArquivo(path_dadger)
    
    df_bloco_pq = df_dadger['PQ']
    
    df_bloco_pq['tipo'] = df_bloco_pq['nome'].str.strip().str[-3:]
    df_bloco_pq['sub'] = df_bloco_pq['sub'].astype(int)
    df_bloco_pq['estagio'] = df_bloco_pq['estagio'].astype(int)
    df_bloco_pq['gerac_p1'] = df_bloco_pq['gerac_p1'].astype(int)
    df_bloco_pq['gerac_p2'] = df_bloco_pq['gerac_p2'].astype(int)
    df_bloco_pq['gerac_p3'] = df_bloco_pq['gerac_p3'].astype(int)
    

    id_deck =  importar_id_deck_dc(data_inicio_rv, str_fonte)
    
    df_geracao_p1 = df_bloco_pq.pivot(index=['sub','estagio'], values='gerac_p1', columns='tipo').reset_index()
    df_geracao_p1['patamar'] = 1
    df_geracao_p1['id_deck'] = id_deck
    df_geracao_p1 = df_geracao_p1.dropna()
    
		
    df_geracao_p2 = df_bloco_pq.pivot(index=['sub','estagio'], values='gerac_p2', columns='tipo').reset_index()
    df_geracao_p2['patamar'] = 2
    df_geracao_p2['id_deck'] = id_deck 
    df_geracao_p2 = df_geracao_p2.dropna()
    
    df_geracao_p3 = df_bloco_pq.pivot(index=['sub','estagio'], values='gerac_p3', columns='tipo').reset_index()
    df_geracao_p3['patamar'] = 3
    df_geracao_p3['id_deck'] = id_deck
    df_geracao_p3 = df_geracao_p3.dropna()
        
    orden_colunas = ['id_deck', 'estagio', 'patamar', 'sub', 'PCT','PCH','EOL','UFV']
    geracoes = []
    geracoes += list(df_geracao_p1[orden_colunas].itertuples(index=False, name=None))
    geracoes += list(df_geracao_p2[orden_colunas].itertuples(index=False, name=None))
    geracoes += list(df_geracao_p3[orden_colunas].itertuples(index=False, name=None))

    db_decks = wx_dbClass.db_mysql_master('db_decks')
    db_decks.connect()
    tb_dc_dadger_pq = db_decks.getSchema('tb_dc_dadger_pq')

    sql_delete = tb_dc_dadger_pq.delete().where(tb_dc_dadger_pq.c.id_deck == id_deck)
    n_values = db_decks.conn.execute(sql_delete).rowcount
    print(f"{n_values} Linhas deletadas na tb_dc_dadger_pq!")
		
    insert_pq = tb_dc_dadger_pq.insert().values(geracoes)
    n_values = db_decks.conn.execute(insert_pq).rowcount
    print(f"{n_values} Linhas inseridas na tabela tb_dc_dadger_pq")

    DIR_TOOLS.remove_src(path_dst)


    
def importar_dc_bloco_dp(path_zip, str_fonte='ons'):

    dt_now = datetime.datetime.now().strftime("%d%m%Y %H%M%S")
    path_dst = os.path.join(PATH_DIR_LOCAL,"tmp",dt_now)

    #o deck vem dentro de um zip com um nome padrao
    if os.path.basename(path_zip).lower() == "pmo_deck_preliminar.zip":
        DIR_TOOLS.extract(path_zip, path_dst)
        deck_entrada_zip = glob.glob(os.path.join(path_dst, 'DEC_ONS_*'))[0]
    else:
        deck_entrada_zip = path_zip

    extracted_file = DIR_TOOLS.extract_specific_files_from_zip(path=deck_entrada_zip,files_name_template=["dadger*"] ,dst=path_dst, extracted_files = [])
    path_dadger = extracted_file[0]

    match = re.match(r'DEC_ONS_([0-9]{2})([0-9]{4})_RV([0-9]{1})_VE', os.path.basename(deck_entrada_zip))
    mes = int(match.group(1))
    ano = int(match.group(2))
    rv = int(match.group(3))

    data_inicio_mes = wx_opweek.getLastSaturday(datetime.date(ano, mes, 1))
    data_inicio_rv = data_inicio_mes + datetime.timedelta(days=7*rv)

    id_deck =  importar_id_deck_dc(data_inicio_rv, str_fonte)
		
    df_dadger, comentarios = wx_dadger.leituraArquivo(path_dadger)
    df_bloco_dp = df_dadger['DP']
    df_bloco_dp=df_bloco_dp.drop(['pat','mnemonico'],axis=1)
    df_bloco_dp=df_bloco_dp.apply(pd.to_numeric, errors='coerce').dropna()
    df_bloco_dp['id_deck'] = id_deck

    df_carga = df_bloco_dp[['id_deck','ip','mwmed_p1','mwmed_p2','mwmed_p3','sub']]
    tb_carga_values = df_carga.values.tolist()

    df_patamar = df_bloco_dp[['id_deck','ip','horas_p1','horas_p2','horas_p3']].drop_duplicates()
    tb_patamar_values = df_patamar.values.tolist()

    db_decks = wx_dbClass.db_mysql_master('db_decks')
    db_decks.connect()
    tb_dc_dadger_dp = db_decks.getSchema('tb_dc_dadger_dp')
    tb_dc_patamar = db_decks.getSchema('tb_dc_patamar')

    delete_dp = tb_dc_dadger_dp.delete().where(tb_dc_dadger_dp.c.id_deck == id_deck)
    n_values = db_decks.conn.execute(delete_dp).rowcount
    print(f"{n_values} Linhas deletadas na tabela tb_dc_dadger_dp")

    insert_dp = tb_dc_dadger_dp.insert().values(tb_carga_values)
    n_values = db_decks.conn.execute(insert_dp).rowcount
    print(f"{n_values} Linhas inseridas na tabela tb_dc_dadger_dp")

    delete_patamar = tb_dc_patamar.delete().where(tb_dc_patamar.c.id_deck == id_deck)
    n_values = db_decks.conn.execute(delete_patamar).rowcount
    print(f"{n_values} Linhas deletadas na tabela tb_dc_patamar")

    insert_patamar = tb_dc_patamar.insert().values(tb_patamar_values)
    n_values = db_decks.conn.execute(insert_patamar).rowcount
    print(f"{n_values} Linhas inseridas na tabela tb_dc_patamar")

    DIR_TOOLS.remove_src(path_dst)


if __name__ == '__main__':
    pass 

    # path_zip = r"C:/Users/cs399274/Downloads/PMO_deck_preliminar.zip"
    # path_zip_interno = r"C:\Users\cs399274\Downloads\PMO_deck_preliminar (1)\DEC_ONS_052024_RV1_VE.zip"

    # importar_dc_bloco_dp(path_zip_interno)
    # importar_dc_bloco_pq(path_zip)