import os
import sys
import datetime
import numpy as np
import pandas as pd
import sqlalchemy as db
import os
import requests
import logging
logging.basicConfig(level=logging.INFO,
                    format='%(levelname)s:\t%(asctime)s\t %(name)s.py:%(lineno)d\t %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers=[
                        logging.StreamHandler()
                    ])

logger = logging.getLogger(__name__)
path_fontes = "/WX2TB/Documentos/fontes/"
sys.path.insert(1,path_fontes)
from PMO.scripts_unificados.bibliotecas import wx_dbClass
from PMO.scripts_unificados.apps.web_modelos.server.libs import wx_calcEna 
import os
from middle.utils import Constants
constants = Constants()

PATH_LISTA_VAZOES= "/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/smap/arquivos/opera-smap/smap_novo/info_vazao_obs.json"
PATH_CACHE = os.path.join(path_fontes,"PMO","scripts_unificados","apps","web_modelos","server","caches")



URL_COGNITO = os.getenv('URL_COGNITO')
CONFIG_COGNITO = os.getenv('CONFIG_COGNITO')
def get_access_token() -> str:
    response = requests.post(
        URL_COGNITO,
        data=CONFIG_COGNITO,
        headers={'Content-Type': 'application/x-www-form-urlencoded'}
    )
    return response.json()['access_token']

def update_acomph_cache(dtRef):
    cmd = F"cd {PATH_CACHE};"
    cmd += f"python rz_cache.py atualizar_cache_acomph data {dtRef}"
    result = os.popen(cmd)
    print(result.read())    
    


def importAcomph(path):

    
    """ Importa o Acomph disponibilizado para o banco de dados
    :param path: caminho da planilha do acomph
    :return None: 
    """

    print("Leitura do arquivo: {0}".format(path))
    df = pd.ExcelFile(path)

    # nome das abas do excel (bacias)
    BACIAS = df.sheet_names

    # Armazena o codigo de todos os postos da planilha 
    POSTOS = []

    # Armazena as tuplas com as informações diarias de cada posto (30 dias x #postos = #linhas a serem inseridas no db) 
    ACOMPH = []
    
    # Leitura de cada aba
    for bacia in BACIAS:
        df_bacia = df.parse(bacia, skiprows=[1,2,3], header=None)

        # Lables para auxiliar o nome das colunas (lv1: cod_posto, lv2: parametro. lv3: lido ou consolidado)     
        label_lv1 = []
        label_lv2 = []
        label_lv3 = []

        postos = []
        for i in range(int(len(df_bacia.columns)/8)):
            cod_posto = df_bacia.loc[0][8*(i+1)]
            postos.append(cod_posto)
            
            # Configuracao dos nomes das colunas do dataframe
            label_lv1 += [i for x in range(8)]
            label_lv2 += [0,0,1,1,2,2,3,4]
            label_lv3 += [0,1,0,1,0,1,1,1]

        # Retirada dos valores nan do dataframe  
        # df_bacia = df_bacia.dropna()
        df_bacia = df_bacia[2:32]
        df_bacia = df_bacia.where(df_bacia.notnull(), None)

        # Configuracao para a data ser o index do dataframe    
        df_bacia = df_bacia.set_index(0)

        # Insercao de multiindex na nomeclatura das colunas
        cols = pd.MultiIndex(levels=[postos, ['nivel_res', 'defluente', 'afluente', 'inc', 'nat'], ['lido', 'conso']],
                              codes=[label_lv1, label_lv2, label_lv3])
        df_bacia.columns = cols
        
        # Data em que o ACOMPH foi lido
        DT_ACOMPH = max(df_bacia.index).strftime('%Y-%m-%d')

        for cd_posto in postos:
            for dt_ref in  df_bacia.index:
                
                # separacao dos dados a serem inseridos
                nivel_lido = df_bacia[cd_posto].loc[dt_ref]['nivel_res']['lido']
                nivel_conso = df_bacia[cd_posto].loc[dt_ref]['nivel_res']['conso']
                defluente_lido = df_bacia[cd_posto].loc[dt_ref]['defluente']['lido']
                defluente_conso = df_bacia[cd_posto].loc[dt_ref]['defluente']['conso']
                afluente_lido = df_bacia[cd_posto].loc[dt_ref]['afluente']['lido']
                afluente_conso = df_bacia[cd_posto].loc[dt_ref]['afluente']['conso']
                incremental_conso = df_bacia[cd_posto].loc[dt_ref]['inc']['conso']
                natural_conso = df_bacia[cd_posto].loc[dt_ref]['nat']['conso']

                # Append os dados lidos a variavel ACOMPH
                ACOMPH.append((dt_ref.strftime('%Y-%m-%d'), cd_posto, nivel_lido, nivel_conso, defluente_lido, defluente_conso, afluente_lido, afluente_conso, incremental_conso, natural_conso, DT_ACOMPH))
        # Append os postos da bacia ao array de todos os postos    
        POSTOS += postos
    df_acomph_post = pd.DataFrame(ACOMPH, columns=["dt_referente","cd_posto","nivel_lido","nivel_conso","defluente_lido","vl_vaz_def_conso","afluente_lido","afluente_conso","vl_vaz_inc_conso","vl_vaz_nat_conso", "dt_acomph"])
    df_acomph_post = df_acomph_post[["dt_referente","cd_posto","vl_vaz_def_conso","vl_vaz_inc_conso","vl_vaz_nat_conso", "dt_acomph"]]
    
    # Round float values to 2 decimal places
    numeric_columns = ["vl_vaz_def_conso", "vl_vaz_inc_conso", "vl_vaz_nat_conso"]
    df_acomph_post[numeric_columns] = df_acomph_post[numeric_columns].round(2)
    df_acomph_post = df_acomph_post.replace({np.nan: None, np.inf: None, -np.inf: None})
    res = requests.post(f'{constants.BASE_URL}/api/v2/ons/acomph',
                        json=df_acomph_post.to_dict('records'),
                        headers={
                            'Content-Type': 'application/json',
                            'Authorization': f'Bearer {get_access_token()}'
                        })
    print(res.status_code)
    print(res.text)

    update_acomph_cache(DT_ACOMPH)
    

    try:
        df_acomph = pd.DataFrame(ACOMPH, columns=['DT_REFERENTE', 'CD_POSTO', 'VL_NIVEL_LIDO', 'VL_NIVEL_CONSO', 'VL_VAZ_DEFLUENTE_LIDO', 'VL_VAZ_DEFLUENTE_CONSO', 'VL_VAZ_AFLUENTE_LIDO', 'VL_VAZ_AFLUENTE_CONSO', 'VL_VAZ_INC_CONSO', 'VL_VAZ_NAT_CONSO', 'DT_ACOMPH'])

        return True
    except Exception as e:
        print(f"Erro ao calcular postos artificiais ou exportar dados: {str(e)}")
        return False

def exportAcomph_toDataviz(
    df_acomph: pd.DataFrame, 
    ):
    
    df_acomph = df_acomph[['CD_POSTO', 'DT_REFERENTE', 'VL_VAZ_INC_CONSO', 'VL_VAZ_NAT_CONSO', 'DT_ACOMPH']]
    df_acomph = df_acomph.sort_values(by=['CD_POSTO', 'DT_REFERENTE', 'DT_ACOMPH'], ascending=[True, True, False])
    df_acomph['ROW'] = df_acomph.groupby(['CD_POSTO', 'DT_REFERENTE']).cumcount() + 1
    df_acomph = df_acomph[df_acomph['ROW'] == 1]
    df_acomph['CD_POSTO'] = pd.to_numeric(df_acomph['CD_POSTO'])
    df_acomph['DT_REFERENTE'] = pd.to_datetime(df_acomph['DT_REFERENTE'])
    df_vazNat = df_acomph.pivot(index='CD_POSTO', columns='DT_REFERENTE', values='VL_VAZ_NAT_CONSO')

    acomph_nat = wx_calcEna.calcPostosArtificiais_df(df_vazNat, ignorar_erros=True)
    
    acomph_nat = acomph_nat.replace([np.inf, -np.inf], np.nan)  
    acomph_nat = acomph_nat.where(pd.notna(acomph_nat), None)
    
    dataRodada = (acomph_nat.columns.max() + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    
    acomph_nat = acomph_nat.stack().reset_index()
    acomph_nat.columns = ['CD_POSTO', 'DT_REFERENTE', 'VL_VAZ_NAT_CONSO']
    
    valoresMapaPosto = get_valoresMapa(acomph_nat, 'posto')
    valoresMapaUsina = get_valoresMapa(acomph_nat, 'usina')
    
    mongo_template = {
        "dataRodada": dataRodada,
        "dataFinal": dataRodada,
        "mapType": "vazao",
        "idType": "",
        "modelo": "ACOMPH",
        "priority": None,
        "grupo": "ONS",
        "rodada": "0",
        "viez": True,
        "membro": "0",
        "measuringUnit": "m³/s",
        "propagationBase": "VNA",
        "generationProcess": "SMAP",
        "data": [
            {
                "valoresMapa": valoresMapaPosto,
                "agrupamento": "posto"
            },
            {
                "valoresMapa": valoresMapaUsina,
                "agrupamento": "usina"
            },
        ],
        "relatedMaps": [],
    }
    
    
    accessToken = get_access_token()
    
    try:
        
        res = requests.post(
                f'{constants.BASE_URL}/backend/api/map',
                json=mongo_template,
                headers={
                    'Content-Type': 'application/json',
                    'Authorization': f'Bearer {accessToken}'
                }
                )
        
        if res.status_code == 200 or res.status_code == 201:
            print("Exportado com sucesso para o Dataviz")
        else:
            raise ValueError(f"Erro ao exportar previsao: {res.content}")
    except Exception as e:
        print(f"Erro na serialização JSON: {str(e)}")
        raise
    
    

def get_valoresMapa(df_acomph, agrupamento):
    dbOns = wx_dbClass.db_mysql_master('db_ons')
    dbOns.connect()
    tb_postos_completo = dbOns.getSchema('tb_postos_completo')
        
    query = db.select(
        tb_postos_completo.c.cd_posto, 
        tb_postos_completo.c.str_posto, 
    )
        
    record_postos = dbOns.conn.execute(query).fetchall()
    
    df_postos = pd.DataFrame(record_postos, columns=['CD_POSTO', 'STR_POSTO'])
    
    if agrupamento == 'posto':
        
        df_merged = pd.merge(df_acomph, df_postos, on='CD_POSTO', how='left')
        df_merged = df_merged[['VL_VAZ_NAT_CONSO', 'DT_REFERENTE', 'STR_POSTO']]
        df_merged = df_merged[pd.notna(df_merged['STR_POSTO'])]
        
        
        result = []
        
        for _, row in df_merged.iterrows():
            result.append({
                "valor": round(float(row['VL_VAZ_NAT_CONSO']), 2) if pd.notna(row['VL_VAZ_NAT_CONSO']) else None,
                "dataReferente": row['DT_REFERENTE'].strftime('%Y-%m-%d'),
                "valorAgrupamento": row['STR_POSTO'],
            })
        return result
    elif agrupamento == 'usina':
        
        tb_posto_uhe = dbOns.getSchema('tb_posto_uhe')
        
        query = db.select(
            tb_posto_uhe.c.cd_posto,
            tb_posto_uhe.c.str_usina
        )
        
        record_usina = dbOns.conn.execute(query).fetchall()
        
        df_usinas = pd.DataFrame(record_usina, columns=['CD_POSTO', 'STR_USINA'])
        
        df_merged = pd.merge(df_acomph, df_usinas, on='CD_POSTO', how='left')
        df_merged = df_merged[['VL_VAZ_NAT_CONSO', 'DT_REFERENTE', 'STR_USINA']]
        df_merged = df_merged[pd.notna(df_merged['STR_USINA'])]
        
        result = []
        
        for _, row in df_merged.iterrows():
            result.append({
                "valor": round(float(row['VL_VAZ_NAT_CONSO']), 2) if pd.notna(row['VL_VAZ_NAT_CONSO']) else None,
                "dataReferente": row['DT_REFERENTE'].strftime('%Y-%m-%d'),
                "valorAgrupamento": row['STR_USINA']
            })
            
        return result
        
    else:
        print(f"Tipo de agregação não reconhecido: {agrupamento}")
        return []
    
    
        

   
def importRdh(path):
    """ Importa o RDH disponibilizado para o banco de dados
    :param path: caminho da planilha do acomph
    :return None: 
    """

    dbOns = wx_dbClass.db_mysql_master('db_ons')
    dbOns.connect()

    tb_postos_completo = dbOns.getSchema('tb_postos_completo')
    tb_ree = dbOns.getSchema('tb_ree')
    tb_rdh = dbOns.getSchema('tb_rdh')
    tb_rdh_submercado = dbOns.getSchema('tb_rdh_submercado')
    tb_rdh_ree = dbOns.getSchema('tb_rdh_ree')
    tb_submercado = dbOns.getSchema('tb_submercado')

    df_excel = pd.ExcelFile(path)
    print("Leitura do arquivo: {0}".format(path))

    # nome das abas do excel
    ABAS = df_excel.sheet_names
    df_hidraul_Hidrol = df_excel.parse('Hidráulico-Hidrológica', header=None)

    if len(df_hidraul_Hidrol.loc[1][~df_hidraul_Hidrol.loc[1].isna()]) != 1:
        print('Não foi possivel achar a Data considerada')
        quit()
    else:
        data_considerada = datetime.datetime.strptime(df_hidraul_Hidrol.loc[1][~df_hidraul_Hidrol.loc[1].isna()].to_list()[0], 'Data considerada: %d/%m/%Y')

    print("Data considerada: "+  data_considerada.strftime('%d/%m/%Y'))

    # Limpeza do dataframe
    df_hidraul_Hidrol = df_hidraul_Hidrol[~df_hidraul_Hidrol[2].isna()]  # limpeza linhas desnecessarias
    df_hidraul_Hidrol = df_hidraul_Hidrol.dropna(axis=1, how='all') # limpeza colunas com valores nan
    df_hidraul_Hidrol = df_hidraul_Hidrol.drop([0,3])         # Remocao de linhas
    df_hidraul_Hidrol = df_hidraul_Hidrol.drop([0, 1, 3], axis=1)   # Remocao de colunas

    postos_planilha = df_hidraul_Hidrol[[2,4]].rename({4:'cd_posto',2:'str_posto'}, axis=1)

    select_posto = db.select(tb_postos_completo.c.cd_posto)
    answer = dbOns.conn.execute(select_posto).fetchall()

    df_postos_completo = pd.DataFrame(answer,columns=['cd_posto'])
    
    novos_postos = set(postos_planilha['cd_posto'].astype(int).unique()) - set(df_postos_completo['cd_posto'].astype(int).unique())
    if novos_postos:
        values = []
        for posto in novos_postos: 
            try:
                cd_posto = int(posto)
                str_posto = postos_planilha[postos_planilha['cd_posto'] == posto]['str_posto'].values.item()
                cd_bacia = None
                values += (cd_posto,str_posto,cd_bacia),
                print(posto, ' sera inserido no banco de dados!')
            except:
                print(f"Não foi possivel inserir o posto: {posto}")
                pass
        if values:
            insert_posto = tb_postos_completo.insert().values(values)
            num_postos_inseridos = dbOns.conn.execute(insert_posto).rowcount
            print(f"{num_postos_inseridos} linhas inseridas na tabela tb_posto")

    dt_referente_str = data_considerada.strftime('%Y-%m-%d')

    #VALORES POSTOS 

    df_values = df_hidraul_Hidrol[[4,15,13,17,18,20,21,22,23,24,25,7,8]]
    df_values.columns = ['cd_posto',
                        'vl_vol_arm',
                        'vl_vazao_dia',
                        'vl_vazao_turb',
                        'vl_vazao_vert',
                        'vl_vazao_dfl',
                        'vl_vazao_transf',
                        'vl_vazao_afl',
                        'vl_vazao_inc',
                        'vl_vazao_consunt',
                        'vl_vazao_evp',
                        'vl_vazao_mes',
                        'vl_mlt']

    df_values = df_values.replace('ND', np.nan)
    df_values['vl_mlt_vazao'] = df_values.apply(lambda row: row['vl_vazao_mes'] / (row['vl_mlt'] / 100) if row['vl_mlt'] != 0 else np.nan, axis=1)
    df_values['dt_referente'] = dt_referente_str
    df_values = df_values.astype(object).where(pd.notnull(df_values), None)

    colunas_desejadas = ['cd_posto',
                        'vl_vol_arm',
                        'vl_mlt_vazao',
                        'vl_vazao_dia',
                        'vl_vazao_turb',
                        'vl_vazao_vert',
                        'vl_vazao_dfl',
                        'vl_vazao_transf',
                        'vl_vazao_afl',
                        'vl_vazao_inc',
                        'vl_vazao_consunt',
                        'vl_vazao_evp',
                        'dt_referente']



    values_to_insert = df_values[colunas_desejadas].values.tolist()

    delete_dados = tb_rdh.delete().where(tb_rdh.c.dt_referente == dt_referente_str)
    num_dados_deletados = dbOns.conn.execute(delete_dados).rowcount
    print( f"{num_dados_deletados} linhas deletadas da tabela tb_rdh!")

    insert_dados = tb_rdh.insert().values(values_to_insert)
    num_dados_inseridos = dbOns.conn.execute(insert_dados).rowcount
    print(f'{num_dados_inseridos} linhas inseridas na tb_rdh!')


    #VALORES SUBMERCADO

    df_hidroenerg_subs = df_excel.parse('Hidroenergética-Subsistemas', header=None)

    pos_cels = {'nome_submercado':[3,0], 'media_mes_65':[6,7], 'media_semana_65':[6,6], 'media_mes_queda':[9,7], 'media_semana_queda':[9,6], 'armazenamento':[6,11]}

    media_mes_65 = {}
    media_semana_65 = {}
    media_mes_queda = {}
    media_semana_queda = {}
    armazenamento = {}

    cd_submercado ={}
    select_submercado = db.select(tb_submercado.c.cd_submercado,tb_submercado.c.str_submercado)
    answer = dbOns.conn.execute(select_submercado).fetchall()

    for submercado_info in answer:
        cd_submercado[submercado_info[1]] = submercado_info[0]
    
    for i in range(len(cd_submercado)):
        pos = pos_cels['nome_submercado']
        sub = df_hidroenerg_subs.loc[pos[0]+8*i][pos[1]].split(' ')[0]
        cod_sub = cd_submercado[sub]

        pos = pos_cels['media_mes_65']
        media_mes_65[cod_sub] = df_hidroenerg_subs.loc[pos[0]+8*i][pos[1]]

        pos = pos_cels['media_semana_65']
        media_semana_65[cod_sub] = df_hidroenerg_subs.loc[pos[0]+8*i][pos[1]]

        pos = pos_cels['media_mes_queda']
        media_mes_queda[cod_sub] = df_hidroenerg_subs.loc[pos[0]+8*i][pos[1]]

        pos = pos_cels['media_semana_queda']
        media_semana_queda[cod_sub] = df_hidroenerg_subs.loc[pos[0]+8*i][pos[1]]

        pos = pos_cels['armazenamento']
        armazenamento[cod_sub] = df_hidroenerg_subs.loc[pos[0]+8*i][pos[1]]

    
    values_to_insert = []
    for cd_submerc in media_mes_65:
        values_to_insert.append((cd_submerc, armazenamento[cd_submerc], media_mes_65[cd_submerc], media_semana_65[cd_submerc], media_mes_queda[cd_submerc], media_semana_queda[cd_submerc], dt_referente_str))
    
    delete_dados = tb_rdh_submercado.delete().where(tb_rdh_submercado.c.dt_referente == dt_referente_str)
    num_dados_deletados = dbOns.conn.execute(delete_dados).rowcount
    print( f"{num_dados_deletados} linhas deletadas da tabela tb_rdh_submercado!")

    insert_dados = tb_rdh_submercado.insert().values(values_to_insert)
    num_dados_inseridos = dbOns.conn.execute(insert_dados).rowcount
    print(f'{num_dados_inseridos} linhas inseridas na tb_rdh_submercado!')


    #VALORES REE

    df_hidroenerg_ree = df_excel.parse('Hidroenergética-REEs', header=None)

    pos_cels = {'nome_ree':[3,0], 'media_mes_65':[6,7], 'media_semana_65':[6,6], 'media_mes_queda':[9,7], 'media_semana_queda':[9,6], 'armazenamento':[6,11]}

    media_mes_65 = {}
    media_semana_65 = {}
    media_mes_queda = {}
    media_semana_queda = {}
    armazenamento = {}

    cd_reservatorio ={}

    select_ree = db.select(tb_ree.c.cd_ree,tb_ree.c.str_ree)
    answer = dbOns.conn.execute(select_ree).fetchall()

    for reservatorio_info in answer:
        cd_reservatorio[reservatorio_info[1].strip()] = reservatorio_info[0]

    i = -1
    while 1:
        i += 1
        pos = pos_cels['nome_ree']
        sub = df_hidroenerg_ree.loc[pos[0]+8*i][pos[1]]

        # loop ate a ultima bacia/submercado (valor encontrado = nan)
        if type(sub) != str:
            break

        # Verificacao se o bacia/submercado esta cadastrado no banco
        try:
            cod_sub = cd_reservatorio[sub]
        except Exception as e:
            print('Reservatorio nao cadastradao no banco: %s' %sub)
            continue

        pos = pos_cels['media_mes_65']
        media_mes_65[cod_sub] = df_hidroenerg_ree.loc[pos[0]+8*i][pos[1]]

        pos = pos_cels['media_semana_65']
        media_semana_65[cod_sub] = df_hidroenerg_ree.loc[pos[0]+8*i][pos[1]]

        pos = pos_cels['media_mes_queda']
        media_mes_queda[cod_sub] = df_hidroenerg_ree.loc[pos[0]+8*i][pos[1]]

        pos = pos_cels['media_semana_queda']
        media_semana_queda[cod_sub] = df_hidroenerg_ree.loc[pos[0]+8*i][pos[1]]

        pos = pos_cels['armazenamento']
        armazenamento[cod_sub] = df_hidroenerg_ree.loc[pos[0]+8*i][pos[1]]

    values_to_insert = []
    for cod_ree in media_mes_65:
        values_to_insert.append((cod_ree,armazenamento[cod_ree], media_mes_65[cod_ree], media_semana_65[cod_ree], media_mes_queda[cod_ree], media_semana_queda[cod_ree], dt_referente_str))
    
    delete_dados = tb_rdh_ree.delete().where(tb_rdh_ree.c.dt_referente == dt_referente_str)
    num_dados_deletados = dbOns.conn.execute(delete_dados).rowcount
    print( f"{num_dados_deletados} linhas deletadas da tabela tb_rdh_ree!")

    insert_dados = tb_rdh_ree.insert().values(values_to_insert)
    num_dados_inseridos = dbOns.conn.execute(insert_dados).rowcount
    print(f'{num_dados_inseridos} linhas inseridas na tb_rdh_ree!')

    cmd = f"cd {PATH_CACHE};"
    cmd += f"python rz_cache.py atualizar_cache_rdh"
    os.system(cmd)



def importar_vazoes_obs_smap(vazoes_values):
    
    db_rodadas = wx_dbClass.db_mysql_master('db_rodadas')
    db_rodadas.connect()
    tb_vazoes_obs = db_rodadas.getSchema('tb_vazoes_obs')

    df_values_total = pd.DataFrame(vazoes_values)
    
    df_contagem_dias = df_values_total.groupby(0)[[4]].count().reset_index().set_index(4)
    for num_dias in df_contagem_dias.index.unique():

        subbacias = df_contagem_dias.loc[num_dias][0].unique()
        df_values = df_values_total[df_values_total[0].isin(subbacias)]

        df_values = df_values.fillna(0)

        dt_min = df_values[3].min()
        dt_max = df_values[3].max()

        delete_values = (
            tb_vazoes_obs.delete()
            .where(
                db.and_(
                    tb_vazoes_obs.c.dt_referente >= dt_min,
                    tb_vazoes_obs.c.dt_referente <= dt_max,
                    tb_vazoes_obs.c.txt_subbacia.in_(subbacias)
                )
            )
        )
        num_deletadas = db_rodadas.conn.execute(delete_values).rowcount
        print(f"{num_deletadas} Linhas deletadas na tb_vazoes_obs" )
        
        first_values = df_values.values.tolist()
        insert_values = tb_vazoes_obs.insert().values(first_values)
        num_inseridas = db_rodadas.conn.execute(insert_values).rowcount
        print(f"{num_inseridas} Linhas inseridas na tb_vazoes_obs" )


    print('Dados tratados com sucesso!\n')
    return True



if __name__ == '__main__':
    path = "/home/diogopolastrine/Documentos/produtos/ACOMPH/ACOMPH_13.05.2025.xls"
    
    importAcomph(path)