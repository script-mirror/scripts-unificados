import sys
import pdb
import datetime
import numpy as np
import pandas as pd
import sqlalchemy as db
import concurrent.futures
from sqlalchemy.sql.expression import func
import os
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")), ".env"))
PATH_PROJETO = os.getenv("PATH_PROJETO", "/WX2TB/Documentos/fontes/PMO")
sys.path.insert(1,f"{PATH_PROJETO}/scripts_unificados")

from bibliotecas import wx_dbClass
from apps.web_modelos.server.libs import wx_calcEna 
from apps.smap.libs import SmapTools 
from apps.smap.libs.Rodadas import tb_smap


def get_ena_smap(
        id,
        data:str,
        df_smap_result_aux:pd.DataFrame,
        df_pluviaBaciasinteresse:pd.DataFrame,
        granularidade:str,
        dias:int=7
    ):
    data = datetime.datetime.strptime(data, '%Y-%m-%d')
    # primeiro_dia = df_smap_result_aux['dt_prevista'].min()

    vazao = df_smap_result_aux.pivot(index='cd_posto', columns='dt_prevista', values='vl_vazao_vna')

    vazao = wx_calcEna.calcPostosArtificiais_df(vazao, ignorar_erros=True)
    # INCREMENTAL_ITAIPU = vazao.loc[66].copy()
    
    # vazao = vazao.drop([169, 176, 178, 172],errors='ignore')

    df_ena = wx_calcEna.gera_ena_df(vazao,granularidade)
    df_ena = df_ena.T

    if granularidade == 'bacia':
        # df_ena['INCREMENTAL DE ITAIPU'] = INCREMENTAL_ITAIPU
        df_pluviaBaciasinteresse = df_pluviaBaciasinteresse.pivot(index='DT_REFERENTE', columns='CD_BACIA', values='VL_ENA')
        df_pluviaBaciasinteresse = df_pluviaBaciasinteresse.rename(columns={21:'SÃO FRANCISCO (NE)'})
    
    if granularidade == "submercado":

        df_pluviaBaciasinteresse = (
        df_pluviaBaciasinteresse[['VL_ENA', 'DT_REFERENTE', 'STR_SUBMERCADO']]
        .groupby(['STR_SUBMERCADO', 'DT_REFERENTE'])
        .sum()
        .reset_index()
        .pivot(index='DT_REFERENTE', columns='STR_SUBMERCADO', values='VL_ENA')
    )
    
    df_ena.index = pd.to_datetime(df_ena.index, format="%Y/%m/%d")
    df_ena = df_ena.add(df_pluviaBaciasinteresse, fill_value=0).dropna()

    # limite_dias = {'PZERADA':30 ,'GFS':16 ,'ETA40':10 ,'PRECMEDIA_ETA40.GEFS':12 ,
    # 'GEFS':16, 'EC':9, 'EC-ens':14, 'PMEDIA':14, 'PDESSEM':14, 'ECMWF-tok':9,
    # 'PCONJUNTO':14,'PCONJUNTO-EXT':17, 'PCONJUNTO2':14, 'EC-ensremvies':14,
    # 'ETA40remvies':10 , 'GEFSremvies':16,'PZERADA':14}
    
    # mapa = df_smap_result_aux['str_modelo'].unique().item()

    # if mapa in limite_dias:
    #     ultimo_dia = primeiro_dia + datetime.timedelta(days=limite_dias[mapa])
    # else:
    #     ultimo_dia = primeiro_dia + datetime.timedelta(days=15)

    # df_ena = df_ena.loc[primeiro_dia:ultimo_dia]
    df_ena.index = pd.to_datetime(df_ena.index).strftime("%Y/%m/%d")

    id_rodada = df_smap_result_aux['id_x'].unique()[0]
    modelo = df_smap_result_aux['str_modelo'].unique()[0]
    horario_rodada = df_smap_result_aux['hr_rodada'].unique()[0]

    dict_modelo = {}
    dict_modelo['id_rodada'] = str(id_rodada)
    dict_modelo["modelo"] = modelo
    dict_modelo["horario_rodada"] = str(horario_rodada).zfill(2)
    dict_modelo["granularidade"] = granularidade
    dict_modelo["valores"] = df_ena.to_dict()


    return dict_modelo  



def query_ultimo_id_pluvia_df(dt_rodada, flag_pzerada):
    db_pluvia = wx_dbClass.db_mysql_master('db_pluvia')
    db_pluvia.connect()
    tb_pluvia_ena = db_pluvia.getSchema('tb_pluvia_ena')

    db_ons = wx_dbClass.db_mysql_master('db_ons')
    db_ons.connect()
    tb_bacias_segmentadas = db_ons.getSchema('tb_bacias_segmentadas')
        
    id_rodada_pluvia = db_pluvia.conn.execute(
      db.select(func.max(tb_pluvia_ena.c.ID_RODADA))
        .where(
            
            db.and_(
                tb_pluvia_ena.c.DT_RODADA == dt_rodada,
                tb_pluvia_ena.c.STR_MAPA == "Prec. Zero" if flag_pzerada else tb_pluvia_ena.c.STR_MAPA != "Prec. Zero",
            ))
    ).scalar()

    if not id_rodada_pluvia:
        print("Resultados do pluvia não estão disponíveis!")
        quit()
    print(f"Utilizando ID do pluvia: {id_rodada_pluvia}")

    lista_bacias_interesses = {'SÃO FRANCISCO (NE)': 21}
    answer = db_pluvia.conn.execute(
        db.select(
            tb_pluvia_ena.c.DT_REFERENTE,
            tb_pluvia_ena.c.VL_ENA,
            tb_pluvia_ena.c.CD_BACIA,
        ).where(
            db.and_(tb_pluvia_ena.c.ID_RODADA == id_rodada_pluvia, tb_pluvia_ena.c.CD_BACIA == 21)
        )
    )
    df_rodada_pluvia = pd.DataFrame(answer.fetchall(), columns= ['DT_REFERENTE','VL_ENA','CD_BACIA'])

    answer_bacias = db_ons.conn.execute(
       db.select(
            tb_bacias_segmentadas.c.cd_bacia,
            tb_bacias_segmentadas.c.cd_submercado,
        ).where(
          tb_bacias_segmentadas.c.cd_bacia == 21
        )
      )
    df_bacias = pd.DataFrame(answer_bacias.fetchall(),columns=['CD_BACIA','cd_submercado'])

    df_rodada_pluvia = pd.merge(df_rodada_pluvia,df_bacias, on=['CD_BACIA'])
    
    # df_rodada_pluvia = pd.DataFrame(answer.fetchall(), columns= ['DT_REFERENTE','VL_ENA','CD_BACIA','cd_submercado'])

    df_rodada_pluvia['STR_SUBMERCADO'] = df_rodada_pluvia.apply(lambda x: "SE" if x['cd_submercado'] == 1 else "S" if x['cd_submercado'] == 2 else "NE" if x['cd_submercado'] == 3 else "N",axis=1)
 
    df_rodada_pluvia['DT_REFERENTE'] = pd.to_datetime(df_rodada_pluvia['DT_REFERENTE'], format="%Y/%m/%d")

    print("terminei funçao pluvia")
    return df_rodada_pluvia
    

def query_smap_df(ids_cadastro_rodadas):
    dataBase = wx_dbClass.db_mysql_master('db_rodadas')
    dataBase.connect()
    tb_cadastro_rodadas = dataBase.getSchema('tb_cadastro_rodadas')
    tb_smap = dataBase.getSchema('tb_smap')

    query_tb_rodadas = db.select(tb_cadastro_rodadas.c.id,tb_cadastro_rodadas.c.id_smap, tb_cadastro_rodadas.c.str_modelo, tb_cadastro_rodadas.c.hr_rodada)\
        .where(tb_cadastro_rodadas.c.id.in_(ids_cadastro_rodadas))
    result_tb_rodadas = dataBase.conn.execute(query_tb_rodadas)
    
    df_tb_rodadas = pd.DataFrame(result_tb_rodadas.fetchall(), columns=['id','id_smap','str_modelo', 'hr_rodada'])
    df_tb_rodadas = df_tb_rodadas.fillna(np.nan)
    ids_smap = df_tb_rodadas['id_smap'].unique()

    if np.isnan(ids_smap).all():
        return pd.DataFrame()

    ids_smap = ids_smap[~np.isnan(ids_smap)]

    query_tb_smap = db.select(tb_smap.c.id, tb_smap.c.cd_posto, tb_smap.c.dt_prevista, tb_smap.c.vl_vazao_vna)\
                .where(tb_smap.c.id.in_(ids_smap))
    result_tb_smap = dataBase.conn.execute(query_tb_smap)
    
    df_tb_smap = pd.DataFrame(result_tb_smap.fetchall(), columns=['id', 'cd_posto', 'dt_prevista', 'vl_vazao_vna'])

    # Juntar os DataFrames utilizando a coluna id e id_smap
    df_smap_result = pd.merge(df_tb_rodadas, df_tb_smap, left_on='id_smap', right_on='id', how='inner')
    df_smap_result['dt_prevista'] = pd.to_datetime(df_smap_result['dt_prevista'], format="%Y-%m-%d")
    
    print("terminei função smap")
    
    return df_smap_result



def get_ena_modelos_smap(ids_to_search,dt_rodada, granularidade, dias: int = 7):
    resultado=[]

    if ids_to_search:

        list_prev_modelos=[]
        futures=[]

        ids_sem_pzeraza = [chave for chave, modelo in ids_to_search if "PZERADA" not in modelo]
        ids_com_pzeraza = [chave for chave, modelo in ids_to_search if "PZERADA" in modelo]

        df_smap_result= query_smap_df(ids_sem_pzeraza+ids_com_pzeraza)

        if df_smap_result.empty:
            print(f'As rodadas com id {ids_sem_pzeraza+ids_com_pzeraza}, não rodaram o smap!')
            return resultado

        ids_rodada_total = df_smap_result['id_x'].astype(str).unique()
        ids_rodada_sem_smap = set(ids_sem_pzeraza+ids_com_pzeraza) - set(ids_rodada_total)

        if ids_rodada_sem_smap:
            print(f'As rodadas com id {ids_rodada_sem_smap}, não rodaram o smap!')
            
        with concurrent.futures.ThreadPoolExecutor() as executor:

            if ids_sem_pzeraza:

                df_pluviaBaciasinteresse = query_ultimo_id_pluvia_df(dt_rodada,flag_pzerada=False)
                
                ids_smap = df_smap_result[df_smap_result['str_modelo'] != "PZERADA"]['id_smap'].unique()
                futures += [executor.submit(get_ena_smap, id,dt_rodada,df_smap_result[df_smap_result['id_smap'] == id],df_pluviaBaciasinteresse,granularidade, dias=dias) for id in ids_smap]

            if ids_com_pzeraza:

                df_pluviaBaciasinteresse = query_ultimo_id_pluvia_df(dt_rodada,flag_pzerada=True)

                ids_smap = df_smap_result[df_smap_result['str_modelo'] == "PZERADA"]['id_smap'].unique()
                futures += [executor.submit(get_ena_smap, id,dt_rodada,df_smap_result[df_smap_result['id_smap'] == id],df_pluviaBaciasinteresse,granularidade, dias=dias) for id in ids_smap]

            for future in concurrent.futures.as_completed(futures):
                resultado += future.result(),

    return resultado


def getAcomph(data_inicial, data_final=None):

    db_ons = wx_dbClass.db_mysql_master('db_ons',connect=True)
    tb_acomph = db_ons.getSchema('tb_acomph')

    cte = (
    db.select(
        tb_acomph.c.cd_posto,
        tb_acomph.c.dt_referente,
        tb_acomph.c.vl_vaz_inc_conso,
        tb_acomph.c.vl_vaz_nat_conso,
        tb_acomph.c.dt_acomph,
        func.row_number().over(
            partition_by=[tb_acomph.c.cd_posto, tb_acomph.c.dt_referente],
            order_by=db.desc(tb_acomph.c.dt_acomph)
        ).label('row_num')
    )
    .where(tb_acomph.c.dt_referente.between(data_inicial, data_final) if data_final != None else tb_acomph.c.dt_referente >= data_inicial)
    .cte('cte_groups')
    )
    query = (
        db.select(cte)
        .where(cte.c.row_num == 1)
        .order_by(cte.c.cd_posto, cte.c.dt_referente)
    )

    answer = db_ons.db_execute(query).fetchall() 
    db_ons.db_dispose()
    return answer


def get_ena_acomph(data_inicial,granularidade,data_final=None):

    acomph = getAcomph(data_inicial,data_final)
    df_acomph = pd.DataFrame(acomph, columns=['CD_POSTO', 'DT_REFERENTE', 'VL_VAZ_INC_CONSO', 'VL_VAZ_NAT_CONSO', 'DT_ACOMPH','ROW'])
    df_vazNat = df_acomph.pivot(index='CD_POSTO', columns='DT_REFERENTE', values='VL_VAZ_NAT_CONSO')
    acomph_nat = wx_calcEna.calcPostosArtificiais_df(df_vazNat, ignorar_erros=True)

    if granularidade == 'bacia':
    #     # Sao francisco
        postos_removidos = [158]
        acomph_nat = acomph_nat.drop(postos_removidos)

    ena = wx_calcEna.gera_ena_df(acomph_nat, granularidade)

    if granularidade == 'bacia':
        df_vazInc = df_acomph.pivot(index='CD_POSTO', columns='DT_REFERENTE', values='VL_VAZ_INC_CONSO')
        # Adicao da vazao incremental de itaipu
        ena.loc['INCREMENTAL DE ITAIPU'] = df_vazInc.loc[266]
    
    ena = ena.replace(np.nan, 0)
    ena.columns = ena.columns.strftime('%Y/%m/%d')
    ena = ena.T

    return ena




def get_previsao_ena_smap(modelos_list:list,granularidade:str='submercado',priority:bool=False):
    
    TB_SMAP = tb_smap()
    df_modelos = TB_SMAP.get_vazao_modelos(modelos_list= modelos_list,priority=priority)
    
    df_modelos = df_modelos.drop(["id_chuva", "id_previvaz" ,"id_prospec"], axis=1)

    df_modelos = df_modelos.dropna(subset=['id_smap'])
    
    df_modelos['cenario'] = ( 
                        df_modelos['id'].astype(str)
                        + '_' 
                        + df_modelos['str_modelo']
                        + '_' 
                        + df_modelos['dt_rodada'].astype(str)
                        +'_'
                        +  df_modelos['hr_rodada'].astype(str)
                        )
    df_modelos['dt_prevista'] = pd.to_datetime(df_modelos['dt_prevista'])
    df_modelos['dt_rodada'] = pd.to_datetime(df_modelos['dt_rodada'])

    return calc_previsao_smap(df_modelos=df_modelos,granularidade=granularidade)

def calc_previsao_smap(df_modelos:pd.DataFrame,granularidade:str='submercado'):

    acomph = {}
    list_valores = []

    for cenario in df_modelos['cenario'].unique():
        print(cenario)
        df_cenario = df_modelos[df_modelos['cenario']==cenario]
        df_pivot = df_cenario.pivot(index='dt_prevista', columns='cd_posto', values='vl_vazao_vna').T
        data = df_cenario['dt_rodada'].unique()[0]

        acomph[data] = getAcomph(data-datetime.timedelta(days=7)) if not acomph.get(data) else acomph[data]

        vazao =wx_calcEna.propagarCalcularNaturais(data,df_pivot,acomph[data],dias=7)
        # vazao.loc[118] = 0.185 + (vazao.loc[119]*0.8103)
        vazao = wx_calcEna.calcPostosArtificiais_df(vazao, ignorar_erros=True)
        df_ena = wx_calcEna.gera_ena_df(vazao,granularidade)

        df_ena = df_ena.T.round(2)
        df_ena.index = pd.to_datetime(df_ena.index).strftime('%Y-%m-%d')

        id_rodada,modelo,dt_rodada,hr_rodada = cenario.split('_')
        df_ena['id_rodada'] = id_rodada
        df_ena['modelo'] = modelo
        df_ena['hr_rodada'] = hr_rodada
        df_ena['dt_rodada'] = dt_rodada
        df_ena['prioridade'] = df_cenario['priority'].values[0]

        records_values = df_ena.reset_index().to_dict('records')

        list_valores += records_values
    return list_valores


if __name__ == '__main__':
    import pdb
    # modelos_list=[]
    # for dt_rodada in dts_rodada_list:
    #     rodadas = SmapTools.convert_modelos_to_json(dt_rodada = datetime.datetime.strptime(dt_rodada,'%Y-%m-%d'))
    #     modelos_list += [(modelo, hr, datetime.datetime.strptime(dt, '%d/%m/%Y')) for modelo,hr,dt in rodadas['modelos']] 

    modelos_list=[('PCONJUNTO', 0, '2024-09-04'),('PCONJUNTO', 0, '2024-09-03')]
    # teste = get_previsao_ena_smap(modelos_list)
    TB_SMAP = tb_smap()
    df_modelos = TB_SMAP.get_rodadas_do_dia('2024-09-03',column_data='id_smap')
    pdb.set_trace()


