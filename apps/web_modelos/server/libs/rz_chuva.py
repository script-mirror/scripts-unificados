import os
import sys
import pdb
import glob
import zipfile
import datetime
import pandas as pd
import sqlalchemy as db
from unidecode import unidecode
from sqlalchemy.sql.expression import func
from matplotlib.colors import LinearSegmentedColormap


sys.path.insert(1,"/WX2TB/Documentos/fontes")
from PMO.scripts_unificados.bibliotecas import wx_dbClass,wx_opweek,wx_emailSender,rz_dir_tools

DIR_TOOLS = rz_dir_tools.DirTools()

def get_resultado_chuva(dt_rodada,modelo,hr,granularidade='bacia'):
    db_rodadas = wx_dbClass.db_mysql_master('db_rodadas')
    db_rodadas.connect()
    tb_chuva = db_rodadas.getSchema('tb_chuva')
    tb_cadastro_rodadas = db_rodadas.getSchema('tb_cadastro_rodadas')
    tb_subbacia = db_rodadas.getSchema('tb_subbacia')
 
    sql_select_chuva = (
        db.select(tb_cadastro_rodadas.c.str_modelo,tb_chuva.c.cd_subbacia,tb_chuva.c.dt_prevista, tb_chuva.c.vl_chuva)\
        .join(tb_cadastro_rodadas, tb_chuva.c.id == tb_cadastro_rodadas.c.id_chuva)\
        .where(
                (tb_cadastro_rodadas.c.dt_rodada == dt_rodada),
                (tb_cadastro_rodadas.c.str_modelo == modelo),
                (tb_cadastro_rodadas.c.hr_rodada == hr)
        )
        )
    result_tb_chuva = db_rodadas.conn.execute(sql_select_chuva).fetchall()
    df_chuva = pd.DataFrame(result_tb_chuva, columns=['str_modelo','cd_subbacia','dt_prevista','vl_chuva'])
 
    sql_select_subbacia = db.select(tb_subbacia.c.cd_subbacia,tb_subbacia.c.txt_nome_subbacia,tb_subbacia.c.txt_submercado, tb_subbacia.c.txt_bacia)
    result_tb_subbacia = db_rodadas.conn.execute(sql_select_subbacia).fetchall()
    df_subbacia = pd.DataFrame(result_tb_subbacia, columns=['cd_subbacia','txt_subbacia','txt_submercado','txt_bacia'])
 
    df_merged = pd.merge(df_chuva, df_subbacia, on='cd_subbacia', how='inner')
    
    if granularidade == 'bacia':
        df_media_bacia = df_merged.groupby(['txt_submercado','txt_bacia','dt_prevista'])[['vl_chuva']].mean().round(2).reset_index()
        df_media_bacia['txt_bacia'] = df_media_bacia['txt_bacia'].apply(lambda x: unidecode(x))
        granularidade_column_name = 'txt_bacia'
    
    elif granularidade == 'subbacia':
        df_media_bacia = df_merged.groupby(['txt_submercado','txt_subbacia','dt_prevista'])[['vl_chuva']].mean().round(2).reset_index()
        granularidade_column_name = 'txt_subbacia'

    df_media_bacia['dt_prevista'] = pd.to_datetime(df_media_bacia['dt_prevista']).dt.strftime("%Y/%m/%d")
    
    dict_master = {}
 
    for submercado in ['Sudeste','Sul','Nordeste','Norte']:
        df_submercado = df_media_bacia[df_media_bacia['txt_submercado'] == submercado]
 
        df_pivot = df_submercado.pivot(index=granularidade_column_name, columns='dt_prevista', values='vl_chuva')

        dict_values = df_pivot.to_dict('index')
        dict_master[submercado] = dict_values
    return dict_master

def dif_tb_chuva(dados_modelo1, dados_modelo2,granularidade):

    import locale
    locale.setlocale(locale.LC_ALL, 'pt_BR.utf8')

    data_rodada1, modelo_name1, hr_rodada1 = dados_modelo1
    data_rodada2, modelo_name2, hr_rodada2 = dados_modelo2

    #tranformando para datetime
    data_rodada1 = datetime.datetime.strptime(data_rodada1,"%d/%m/%Y")
    data_rodada2 = datetime.datetime.strptime(data_rodada2,"%d/%m/%Y")
    
    chuva_values1 = get_resultado_chuva(data_rodada1,modelo_name1,hr_rodada1,granularidade=granularidade)
    chuva_values2 = get_resultado_chuva(data_rodada2,modelo_name2,hr_rodada2,granularidade=granularidade)

    df1 = pd.DataFrame([{
        'submercado': submercado,
        granularidade: bacia,
        'Data': data,
        'Valor': valor} 
        for submercado, bacias in chuva_values1.items() for bacia, datas in bacias.items() for data, valor in datas.items()
        ])
    
    df2 = pd.DataFrame([{
        'submercado': submercado,
        granularidade: bacia,
        'Data': data,
        'Valor': valor} 
        for submercado, bacias in chuva_values2.items() for bacia, datas in bacias.items() for data, valor in datas.items()
        ])
    
    #diferenca
    df_diff = df1.groupby(['submercado',granularidade,'Data'])[['Valor']].mean() - df2.groupby(['submercado',granularidade,'Data'])[['Valor']].mean()
    df_pivot = df_diff.pivot_table(index=['submercado', granularidade], columns='Data', values='Valor').round(2)
    df_pivot = df_pivot.reindex(['Sudeste', 'Sul', 'Nordeste', 'Norte'], level='submercado')
    df_pivot.columns = pd.to_datetime(df_pivot.columns)

    df_pivot_semanas = df_pivot.T.resample('W-SAT').sum().round().T

    semanas_agrupadas = {}
    semanas_title=[]
    for semana, grupo in df_pivot.T.resample('W-SAT'): 
        semanas_agrupadas = grupo.index.strftime('%A').tolist()
        semanas_title += ''.join([week_day[0].upper() for week_day in semanas_agrupadas]),
    
    df_pivot_semanas.columns = semanas_title
    df_pivot.columns = pd.to_datetime(df_pivot.columns).strftime('%d/%m')
    
    return df_pivot,df_pivot_semanas


def get_chuva_smap_ponderada(ids,dt_rodada,granularidade):
    
    #climeenergy
    db_ons = wx_dbClass.db_mysql_master('db_ons')
    db_ons.connect()

    tb_ve_bacias = db_ons.getSchema('tb_ve_bacias')
    tb_bacias_segmentadas = db_ons.getSchema('tb_bacias_segmentadas')
            
    #rodadas
    dataBase = wx_dbClass.db_mysql_master('db_rodadas')
    dataBase.connect()

    tb_chuva = dataBase.getSchema('tb_chuva')
    tb_cadastro_rodadas = dataBase.getSchema('tb_cadastro_rodadas')
    tb_subbacia = dataBase.getSchema('tb_subbacia')


    query_bacia = db.select(tb_bacias_segmentadas.c.cd_bacia,tb_bacias_segmentadas.c.str_bacia)
    answer_tb_bacias_segmentadas = db_ons.conn.execute(query_bacia).fetchall()
    df_bacia = pd.DataFrame(answer_tb_bacias_segmentadas, columns=['cd_bacia','str_bacia'])


    query_chuva = db.select(tb_cadastro_rodadas.c.id,tb_cadastro_rodadas.c.str_modelo,tb_cadastro_rodadas.c.hr_rodada,tb_chuva.c.cd_subbacia,tb_chuva.c.dt_prevista,tb_chuva.c.vl_chuva,tb_cadastro_rodadas.c.dt_revisao)\
                                            .join(tb_chuva, tb_chuva.c.id==tb_cadastro_rodadas.c.id_chuva)\
                                            .where(tb_cadastro_rodadas.c.id.in_(ids))

    answer_tb_chuva = dataBase.conn.execute(query_chuva).fetchall()
    df_chuva = pd.DataFrame(answer_tb_chuva, columns= ['id', 'str_modelo',  'hr_rodada',  'cd_subbacia', 'dt_prevista',  'vl_chuva',  'dt_revisao'])
    if df_chuva.empty:
        print(f'Não há chuva cadastrada para os ids: {ids}')
        return []

    cds_subbacia = df_chuva['cd_subbacia'].unique()

    query_subbac = db.select(tb_subbacia.c.cd_subbacia, tb_subbacia.c.cd_bacia_mlt, tb_subbacia.c.txt_submercado)\
                                            .where(tb_subbacia.c.cd_subbacia.in_(cds_subbacia))
    answer_tb_subbacia = dataBase.conn.execute(query_subbac).fetchall()
    df_subbacia = pd.DataFrame(answer_tb_subbacia, columns=['cd_subbacia','cd_bacia','txt_submercado'])


    df_chuva_concat= pd.merge(df_chuva,df_subbacia, on=['cd_subbacia'], how='inner')
    df_chuva_concat= pd.merge(df_chuva_concat,df_bacia, on=['cd_bacia'], how='inner')

    df_chuva_concat['dt_prevista'] = pd.to_datetime(df_chuva_concat['dt_prevista'])
    

    if granularidade == 'submercado':

        df_chuva_concat['dt_inicio_semana'] = df_chuva_concat['dt_prevista'].apply(lambda x: wx_opweek.getLastSaturday(x)).dt.strftime('%Y-%m-%d')

        dt = datetime.datetime.strptime(dt_rodada, "%Y-%m-%d")

        dt = wx_opweek.getLastSaturday(dt)
        dt_inicio_semana = datetime.datetime.strftime(dt,"%Y-%m-%d")
        

        select_query = db.select(tb_ve_bacias.c.cd_bacia,tb_ve_bacias.c.vl_mes,tb_ve_bacias.c.dt_inicio_semana,tb_ve_bacias.c.cd_revisao,((tb_ve_bacias.c.vl_ena * 100) / func.nullif(tb_ve_bacias.c.vl_perc_mlt, 0)).label('mlt'))\
                                        .where(tb_ve_bacias.c.dt_inicio_semana >= dt_inicio_semana)
        answer_tb_ve_bacias = db_ons.conn.execute(select_query).fetchall()
        
        df_mlt = pd.DataFrame(answer_tb_ve_bacias, columns=['cd_bacia','vl_mes','dt_inicio_semana','cd_revisao','mlt'])
        df_mlt = df_mlt.sort_values(['vl_mes','cd_revisao'], ascending=False)
        # Remover as linhas duplicadas na ColunaA, mantendo apenas a linha com o valor máximo em ColunaB
        df_mlt['dt_inicio_semana'] = pd.to_datetime(df_mlt['dt_inicio_semana']).dt.strftime('%Y-%m-%d')
        df_mlt = df_mlt.drop_duplicates(['dt_inicio_semana','cd_bacia'], keep='first')
        
        df_mlt = df_mlt.sort_values('dt_inicio_semana')
        

        df_concatenado = pd.merge(df_chuva_concat, df_mlt, on=['dt_inicio_semana', 'cd_bacia'], how='inner')
        df_concatenado['chuvaxmlt'] = df_concatenado['vl_chuva']*df_concatenado['mlt']

        df_concatenado['txt_submercado'] = df_concatenado['txt_submercado'].apply(lambda x: 'SE' if x == 'Sudeste' else 'S' if x == 'Sul'  else 'N' if x == 'Norte'  else 'NE')
        df = df_concatenado.groupby(["str_modelo","id",'hr_rodada',"txt_submercado",'dt_prevista'])[['chuvaxmlt','mlt']].sum()
        df['chuva_pond'] = df['chuvaxmlt'] / df['mlt']
        column_granularidade = "txt_submercado"

    elif granularidade == 'subbacia':
        df_chuva_subbacias = df_chuva_concat[['dt_prevista', 'cd_subbacia', 'vl_chuva']].copy()
        df_chuva_subbacias['dt_prevista'] = [data.strftime("%Y-%m-%d") for data in df_chuva_subbacias['dt_prevista']]
        df_chuva_subbacias['cd_subbacia'] = [str(data) for data in df_chuva_subbacias['cd_subbacia']] 

        resultado = df_chuva_subbacias.groupby('dt_prevista').apply(lambda x: dict(zip(x['cd_subbacia'], x['vl_chuva']))).to_dict()
        return resultado
    
    else:
        df_concatenado = df_chuva_concat
        incremental_itaipu = df_concatenado[df_concatenado['cd_subbacia'].isin([21,22,23,24,25])]
        incremental_itaipu.loc[:,'str_bacia'] = 'INCREMENTAL DE ITAIPU'

        df_concatenado = pd.concat([df_concatenado,incremental_itaipu])
        df = df_concatenado.groupby(["str_modelo","id",'hr_rodada',"str_bacia",'dt_prevista'])[['vl_chuva']].mean()
        df['chuva_pond'] = df['vl_chuva']
        column_granularidade = "str_bacia"

    
    list_prev_modelos=[]

    df = df.reset_index()
    df['dt_prevista'] = pd.to_datetime(df['dt_prevista']).dt.strftime('%Y-%m-%d')
    ids_unicos = df['id'].unique()

    for id in ids_unicos:
        dict_modelo = {}

        filtered_df = df[df['id'] == id]
        values = filtered_df.pivot_table(values='chuva_pond', index='dt_prevista', columns=column_granularidade).to_dict()

        dict_modelo['id_rodada'] = str(id)
        dict_modelo["modelo"] = filtered_df['str_modelo'].unique()[0] 
        dict_modelo["horario_rodada"] = str(filtered_df['hr_rodada'].unique()[0]).zfill(2)
        dict_modelo["granularidade"] = granularidade
        dict_modelo["valores"] = values
        list_prev_modelos += dict_modelo,

            
    return list_prev_modelos


def getChuvaObservada(dataInicio: datetime,granularidade: str, data_fim: datetime):
  
    global dateFormat

    #climeenergy
    db_ons = wx_dbClass.db_mysql_master('db_ons')
    db_ons.connect()

    tb_ve_bacias = db_ons.getSchema('tb_ve_bacias')
    tb_bacias_segmentadas = db_ons.getSchema('tb_bacias_segmentadas')

    #rodadas
    dataBase = wx_dbClass.db_mysql_master('db_rodadas')
    dataBase.connect()

    tb_chuva_obs = dataBase.getSchema('tb_chuva_obs')
    tb_subbacia = dataBase.getSchema('tb_subbacia')

    # query para pegar os nomes das subbacias
    query_subbacia = db.select(tb_subbacia.c.cd_subbacia, tb_subbacia.c.cd_bacia_mlt, tb_subbacia.c.txt_submercado)
    answer_subbacia = dataBase.conn.execute(query_subbacia).fetchall()
    df_subbacia = pd.DataFrame(answer_subbacia, columns=['cd_subbacia','cd_bacia_mlt','txt_submercado'])

    # query para pegar os nomes das bacias
    query_bacia = db.select(tb_bacias_segmentadas.c.cd_bacia,tb_bacias_segmentadas.c.str_bacia)
    answer_tb_bacias_segmentadas = db_ons.conn.execute(query_bacia).fetchall()
    df_bacia = pd.DataFrame(answer_tb_bacias_segmentadas, columns=['cd_bacia_mlt','str_bacia'])
    
    # concatenando dataframe valores de chuva e nomes subbacias
    df_bacia_subbacia_concat= pd.merge(df_subbacia,df_bacia, on=['cd_bacia_mlt'], how='inner')  

    dtInicial = datetime.datetime.strftime(dataInicio, "%Y-%m-%d")
    data_final = datetime.datetime.strftime(data_fim, "%Y-%m-%d")


    # query para pegar valores de chuva observada
    query_chuva_obs = db.select(tb_chuva_obs.c.cd_subbacia, tb_chuva_obs.c.dt_observado, tb_chuva_obs.c.vl_chuva).where(tb_chuva_obs.c.dt_observado.between(dtInicial, data_final))
    
    answer_tb_chuva_obs = dataBase.conn.execute(query_chuva_obs).fetchall()
    df_chuva_obs = pd.DataFrame(answer_tb_chuva_obs, columns = ['cd_subbacia','dt_observado','vl_chuva'])

    # concatenando dataframe valores de chuva e nomes subbacias
    df_chuva_concat= pd.merge(df_chuva_obs,df_bacia_subbacia_concat, on=['cd_subbacia'], how='inner')


    if granularidade == 'submercado':
        # pegando data de revisao que sempre é o sabado anterior ao dia escolhido
        ultimoSabado = wx_opweek.getLastSaturday(dataInicio)
        
        # query para transformar porcentagem mlt em valor MLT
        select_query = db.select(tb_ve_bacias.c.cd_bacia,tb_ve_bacias.c.vl_mes,tb_ve_bacias.c.dt_inicio_semana,tb_ve_bacias.c.cd_revisao,((tb_ve_bacias.c.vl_ena * 100) / func.nullif(tb_ve_bacias.c.vl_perc_mlt, 0)).label('mlt'))\
                                                                                        .where(tb_ve_bacias.c.dt_inicio_semana >= ultimoSabado)
        answer_tb_ve_bacias = db_ons.conn.execute(select_query).fetchall()
        # criando dataframe mlt ordenando pela data inicial da semana
        df_mlt = pd.DataFrame(answer_tb_ve_bacias, columns=['cd_bacia_mlt','vl_mes','dt_inicio_semana','cd_revisao','mlt'])
        df_mlt = df_mlt.sort_values(['vl_mes','cd_revisao'], ascending=False)
        # Remover as linhas duplicadas na ColunaA, mantendo apenas a linha com o valor máximo em ColunaB
        df_mlt['dt_inicio_semana'] = pd.to_datetime(df_mlt['dt_inicio_semana']).dt.strftime('%Y-%m-%d')
        df_mlt = df_mlt.drop_duplicates(['dt_inicio_semana','cd_bacia_mlt'], keep='first')
        
        # concatenando valores de mlt, mltxchuva e valores de chuva em um unico dataframe
        df_chuva_concat['dt_observado'] = pd.to_datetime(df_chuva_concat['dt_observado'])
        df_chuva_concat['dt_inicio_semana'] = df_chuva_concat['dt_observado'].apply(lambda x: wx_opweek.getLastSaturday(x)).dt.strftime('%Y-%m-%d')
        df_concatenado = pd.merge(df_chuva_concat, df_mlt, on=['dt_inicio_semana', 'cd_bacia_mlt'], how='inner')
        df_concatenado['chuvaxmlt'] = df_concatenado['vl_chuva']*df_concatenado['mlt']
        df_concatenado['txt_submercado'] = df_concatenado['txt_submercado'].apply(lambda x: 'SE' if x == 'Sudeste' else 'S' if x == 'Sul'  else 'N' if x == 'Norte'  else 'NE')

        df_concatenado['dt_observado'] = pd.to_datetime(df_concatenado['dt_observado']).dt.strftime('%Y-%m-%d')
        df = df_concatenado.groupby(["txt_submercado",'dt_observado'])[['chuvaxmlt','mlt']].sum()
        df['chuva_pond'] = df['chuvaxmlt'] / df['mlt']
        resultado = df.pivot_table(values='chuva_pond', index='dt_observado', columns='txt_submercado').to_dict()

    elif granularidade == "subbacia":
       df_chuva_obs['dt_observado'] = [data.strftime("%Y-%m-%d") for data in df_chuva_obs['dt_observado']]
       df_chuva_obs['cd_subbacia'] = [str(data) for data in df_chuva_obs['cd_subbacia']]
       
       resultado = df_chuva_obs.groupby('dt_observado').apply(lambda x: dict(zip(x['cd_subbacia'], x['vl_chuva']))).to_dict()

    else:
        df_concatenado = df_chuva_concat
        incremental_itaipu = df_concatenado[df_concatenado['cd_subbacia'].isin([21,22,23,24,25])]
        incremental_itaipu.loc[:,'str_bacia'] = 'INCREMENTAL DE ITAIPU' 
        df_concatenado = pd.concat([df_concatenado,incremental_itaipu])
        df_concatenado['dt_observado'] = pd.to_datetime(df_concatenado['dt_observado']).dt.strftime('%Y-%m-%d')
        df = df_concatenado.groupby(["str_bacia",'dt_observado'])[['vl_chuva']].mean()
        df['chuva_pond'] = df['vl_chuva']
        resultado = df.pivot_table(values='chuva_pond', index='dt_observado', columns='str_bacia').to_dict()


    return resultado
def get_chuvas(dt_rodada,modelo,hr):
    global dateFormat
 
    db_rodadas = wx_dbClass.db_mysql_master('db_rodadas')
    db_rodadas.connect()
    tb_chuva = db_rodadas.getSchema('tb_chuva')
    tb_cadastro_rodadas = db_rodadas.getSchema('tb_cadastro_rodadas')
    tb_subbacia = db_rodadas.getSchema('tb_subbacia')
 
    query_tb_cadastro_rodadas = db.select([tb_cadastro_rodadas.c.id_chuva, tb_cadastro_rodadas.c.dt_rodada,tb_cadastro_rodadas.c.str_modelo])\
    .where(tb_cadastro_rodadas.c.dt_rodada == dt_rodada, tb_cadastro_rodadas.c.str_modelo == modelo, tb_cadastro_rodadas.c.hr_rodada == int(hr))
 
    lista_cadastro_rodadas = db_rodadas.conn.execute(query_tb_cadastro_rodadas).first()
    df_tb_rodadas = pd.DataFrame([lista_cadastro_rodadas], columns=['id_chuva', 'dt_rodada','str_modelo'])
    id_cadastro_rodadas = lista_cadastro_rodadas[0]
 
    query_tb_chuva = db.select([tb_chuva.c.id,tb_chuva.c.dt_prevista, tb_chuva.c.vl_chuva, tb_subbacia.c.txt_nome_subbacia, tb_subbacia.c.txt_submercado, tb_subbacia.c.txt_bacia])\
        .join(tb_chuva, tb_chuva.c.cd_subbacia == tb_subbacia.c.cd_subbacia)\
        .where(tb_chuva.c.id == id_cadastro_rodadas)
    lista_resultado_chuva = db_rodadas.conn.execute(query_tb_chuva).all()
    df_tb_chuva = pd.DataFrame(lista_resultado_chuva, columns=['id_chuva','dt_prevista','vl_chuva','txt_nome_subbacia','txt_submercado','txt_bacia'])
 
    df_merged = pd.merge(df_tb_rodadas, df_tb_chuva, on='id_chuva', how='inner')
    values = df_merged[['dt_prevista', 'vl_chuva', 'dt_rodada', 'str_modelo', 'txt_nome_subbacia', 'txt_submercado', 'txt_bacia']].values
    return values


def get_zip_psath_df(path_arq):

    with zipfile.ZipFile(path_arq, 'r') as zip_file:

        files = zip_file.namelist()
        
        df_completo = pd.DataFrame()
        
        for file in files:

            xlfile = zip_file.open(file)
            df = pd.read_csv(xlfile, delimiter=r"\s+", header=None)
            
            dt_ref = datetime.datetime.strptime(file,'psat_%d%m%Y.txt')
            df.columns = ["subbacia", 'lat', 'lon', dt_ref]

            if df_completo.empty:
                df_completo = df["subbacia"]

            df['subbacia'] = df['subbacia'].apply(unidecode).apply(lambda x: x.strip())

            df_completo = pd.merge(df_completo,df[["subbacia",dt_ref]], on=["subbacia"])

        xlfile.close()


    return df_completo

def get_psath_diff(path_psath_atual, limiar = 1):

    dir_psath = os.path.dirname(path_psath_atual)

    arq_atual = os.path.basename(path_psath_atual)
    date_atual = datetime.datetime.strptime(arq_atual, "psath_%d%m%Y.zip")
    
    date_anterior = (date_atual - datetime.timedelta(days=1)).strftime('%d%m%Y')
    arq_anterior = f"psath_{date_anterior}.zip"
    path_psath_anterior = os.path.join(dir_psath,arq_anterior)
    path_psath_anterior = DIR_TOOLS.get_name_insentive_name(path_psath_anterior)

    df_chuva = get_zip_psath_df(path_psath_atual)
    df_chuva_anterior = get_zip_psath_df(path_psath_anterior)

    df_diff = df_chuva.set_index('subbacia') - df_chuva_anterior.set_index('subbacia')

    df_diff_export = df_diff[abs(df_diff)>limiar].dropna(how='all',axis=1).dropna(how='all',axis=0)

    if not df_diff_export.empty:

        df_diff_export = df_diff_export.fillna('-')
        df_diff_export.index.name = None

        df_diff_export.columns = [date.strftime('%d-%m-%Y') for date in df_diff_export.columns]

        df_diff_export = df_diff_export.style.set_caption('').format(precision=2)
        html = df_diff_export.render()
        template = wx_emailSender.get_css_df_style(html)

    else:
        print(f'Não há diferença considerando o limiar = {limiar}!')
        template = None


    return template
