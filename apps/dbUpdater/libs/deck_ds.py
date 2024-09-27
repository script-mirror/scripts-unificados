import os
import re
import sys
import pdb
import zipfile
import datetime
import numpy as np
import pandas as pd
import sqlalchemy as db

path_fontes = "/WX2TB/Documentos/fontes/"
sys.path.insert(1,path_fontes)
from PMO.scripts_unificados.bibliotecas import wx_dbClass , wx_opweek , rz_dir_tools
from PMO.scripts_unificados.apps.dessem.libs import wx_renovaveis, wx_pdoCmo

PATH_DIR_LOCAL = os.path.dirname(os.path.abspath(__file__))

DIR_TOOLS = rz_dir_tools.DirTools()


def read_deck_ds_renovaveis(path_file:str,dt_ref:datetime):
    
    data_deck = dt_ref
    print('\nInserindo dados do arquivo renoveis do dia {}'.format(data_deck.strftime('%d/%m/%Y')))

    if '.zip' in path_file.lower():
        dt_now = datetime.datetime.now().strftime("%d%m%Y %H%M%S")
        path_dst = os.path.join(PATH_DIR_LOCAL,"tmp",dt_now)
        extracted_file = DIR_TOOLS.extract_specific_files_from_zip(path=path_file,files_name_template=['renovaveis.dat'] ,dst=path_dst, extracted_files = [])
        path_arquivo = extracted_file[0]
        baseRenovaveis = wx_renovaveis.leituraArquivo(path_arquivo)
        DIR_TOOLS.remove_src(path_dst)
    else:
        path_arquivo = path_file
        baseRenovaveis = wx_renovaveis.leituraArquivo(path_arquivo)

    print('Arquivo: {}'.format(path_arquivo))

    dicionario_sub = {'SE': 1, 'S': 2, 'NE': 3, 'N': 4}
 
    # Numero de dias no deck
    ndias = 5 - data_deck.weekday()
    if ndias <= 0:
        ndias = 12 - data_deck.weekday()

    # Dicionario mapeando o dia com datetime
    dias_deck = {}
    for d in range(ndias+1):
        dt = data_deck + datetime.timedelta(days=d)
        dias_deck[dt.day] = dt

    discretizacao_semana = pd.date_range(data_deck, periods=ndias*48, freq='30min')

    infoBlocos = wx_renovaveis.getInfoBlocos()
    
    blocoEOLICA = wx_renovaveis.extrairInfoBloco(baseRenovaveis, 'EOLICA', infoBlocos['EOLICA']['regex'])
    df_blocoEolica = pd.DataFrame(blocoEOLICA, columns=infoBlocos['EOLICA']['campos'])
    df_blocoEolica['tipo'] = df_blocoEolica['nome'].str.strip().str[-3:]
    df_blocoEolica['codigo'] = df_blocoEolica['codigo'].astype(int)
 
    blocoEOLICASUBM = wx_renovaveis.extrairInfoBloco(baseRenovaveis, 'EOLICASUBM', infoBlocos['EOLICASUBM']['regex'])
    df_blocoEolicasubm = pd.DataFrame(blocoEOLICASUBM, columns=infoBlocos['EOLICASUBM']['campos'])
    df_blocoEolicasubm['codigo'] = df_blocoEolicasubm['codigo'].astype(int)
    df_blocoEolicasubm['sbm'] = df_blocoEolicasubm['sbm'].str.strip()
     
    blocoEOLICAGERACAO = wx_renovaveis.extrairInfoBloco(baseRenovaveis, 'EOLICA-GERACAO', infoBlocos['EOLICA-GERACAO']['regex'])
    df_blocoEolicaGeracao = pd.DataFrame(blocoEOLICAGERACAO, columns=infoBlocos['EOLICA-GERACAO']['campos'])
    df_blocoEolicaGeracao['codigo'] = df_blocoEolicaGeracao['codigo'].astype(int)
    df_blocoEolicaGeracao['geracao'] = df_blocoEolicaGeracao['geracao'].astype(float)
    
    df_blocoEolicaGeracao['di'] = df_blocoEolicaGeracao['di'].astype(int)
    df_blocoEolicaGeracao['hi'] = df_blocoEolicaGeracao['hi'].astype(int)
    df_blocoEolicaGeracao['mi'] = df_blocoEolicaGeracao['mi'].astype(int)*30
    df_blocoEolicaGeracao['df'] = df_blocoEolicaGeracao['df'].astype(int)
    df_blocoEolicaGeracao['hf'] = df_blocoEolicaGeracao['hf'].astype(int)
    df_blocoEolicaGeracao['mf'] = df_blocoEolicaGeracao['mf'].astype(int)*30

    # Juntando as horas e minutos a data inicial
    df_blocoEolicaGeracao['di'] = df_blocoEolicaGeracao['di'].replace(dias_deck)
    df_blocoEolicaGeracao['di'] += pd.to_timedelta(df_blocoEolicaGeracao['hi'], unit='H')
    df_blocoEolicaGeracao['di'] += pd.to_timedelta(df_blocoEolicaGeracao['mi'], unit='m')

    # Juntando as horas e minutos a data final
    df_blocoEolicaGeracao['df'] = df_blocoEolicaGeracao['df'].replace(dias_deck)
    df_blocoEolicaGeracao['df'] += pd.to_timedelta(df_blocoEolicaGeracao['hf'], unit='H')
    df_blocoEolicaGeracao['df'] += pd.to_timedelta(df_blocoEolicaGeracao['mf'], unit='m')
 
    df_blocoEolicaGeracao = pd.merge(df_blocoEolicaGeracao, df_blocoEolicasubm[['codigo','sbm']], how='inner', on=['codigo'])
    df_blocoEolicaGeracao = pd.merge(df_blocoEolicaGeracao, df_blocoEolica[['codigo','tipo']], how='inner', on=['codigo'])

    tipos_geracao = df_blocoEolica['tipo'].unique()
    geracao = {}
    geracao['SE'] = pd.DataFrame(0, index=discretizacao_semana, columns=tipos_geracao)
    geracao['S'] = pd.DataFrame(0, index=discretizacao_semana, columns=tipos_geracao)
    geracao['NE'] = pd.DataFrame(0, index=discretizacao_semana, columns=tipos_geracao)
    geracao['N'] = pd.DataFrame(0, index=discretizacao_semana, columns=tipos_geracao)

    for idx, r in df_blocoEolicaGeracao.iterrows():
        geracao[r['sbm']].loc[pd.date_range(r['di'], r['df'], freq='30min')[:-1], r['tipo']] += r['geracao']

    valores_inserir = []
    for sub in geracao:
        geracao[sub]['periodo'] = np.arange(1, len(discretizacao_semana) + 1)
        geracao[sub]['subm'] = dicionario_sub[sub]
        valores_inserir +=  list(geracao[sub][['periodo', 'subm', 'UHE', 'UTE', 'CGH', 'PCH', 'UFV', 'UEE', 'MGD']].itertuples(index=False, name=None))

    return valores_inserir


def importar_id_deck_ds(dt_referente:datetime, str_fonte:str):

    dicionario_fonte = {'ons': 1, 'ccee':2, 'ons_convertido':3}
     
    db_decks = wx_dbClass.db_mysql_master('db_decks')
    db_decks.connect()
    tb_cadastro_dessem = db_decks.getSchema("tb_cadastro_dessem")

    query_get_id_rodada = db.select(
        tb_cadastro_dessem.c.id
        ).where(
            tb_cadastro_dessem.c.dt_referente == dt_referente.strftime("%Y-%m-%d"), 
            tb_cadastro_dessem.c.id_fonte == dicionario_fonte[str_fonte.lower()]
            )
    answer_tb_cadastro = db_decks.conn.execute(query_get_id_rodada).scalar()

    if not answer_tb_cadastro:

        query_get_max_id_rodada = db.select(db.func.max(tb_cadastro_dessem.c.id))
        answer_tb_cadastro = db_decks.conn.execute(query_get_max_id_rodada).scalar()
        id_deck = answer_tb_cadastro + 1
        
        query_insert_id_rodada = tb_cadastro_dessem.insert().values(
            [   
                id_deck,
                (dt_referente-datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
                dt_referente.strftime("%Y-%m-%d"),
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

def importar_renovaveis_ds(path_file:str,dt_ref:datetime, str_fonte:str='ons'):

    valores_inserir = read_deck_ds_renovaveis(path_file=path_file,dt_ref=dt_ref)

    ##importar
    db_decks = wx_dbClass.db_mysql_master('db_decks')
    db_decks.connect()
    tb_ds_renovaveis = db_decks.getSchema("tb_ds_renovaveis")

    id_deck = importar_id_deck_ds(dt_referente=dt_ref, str_fonte=str_fonte)

    df_valores_renovaveis = pd.DataFrame(valores_inserir, columns=['periodo', 'subm', 'UHE', 'UTE', 'CGH', 'PCH', 'UFV', 'UEE', 'MGD'])
    df_valores_renovaveis.loc[:,'id_deck'] = id_deck
    values_to_insert = df_valores_renovaveis[['id_deck','periodo', 'subm', 'UHE', 'UTE', 'CGH', 'PCH', 'UFV', 'UEE', 'MGD']].values.tolist()

    sql_delete = tb_ds_renovaveis.delete().where(tb_ds_renovaveis.c.id_deck == id_deck)
    n_values = db_decks.conn.execute(sql_delete).rowcount
    print(f"{n_values} Linhas deletadas na tb_ds_renovaveis!")

    sql_insert = tb_ds_renovaveis.insert().values(values_to_insert)
    n_values = db_decks.conn.execute(sql_insert).rowcount
    print(f"{n_values} Linhas inseridas na tb_ds_renovaveis!")


def importar_pdo_cmosist_ds(path_file:str, dt_ref:datetime, str_fonte:str):

    dtRodada = dt_ref - datetime.timedelta(days=1)

    if '.zip' in path_file.lower():
        dt_now = datetime.datetime.now().strftime("%d%m%Y %H%M%S")
        path_dst = os.path.join(PATH_DIR_LOCAL,"tmp",dt_now)
        extracted_file = DIR_TOOLS.extract_specific_files_from_zip(path=path_file,files_name_template=['pdo_cmosist.dat'] ,dst=path_dst, extracted_files = [])
        pdocmoPath = extracted_file[0]
        print('\nInserindo arquivo:\n{}'.format(pdocmoPath))
        valores = wx_pdoCmo.leituraCmo(pdocmoPath, dtRodada)
        DIR_TOOLS.remove_src(path_dst)
    else:
        pdocmoPath = path_file
        print('\nInserindo arquivo:\n{}'.format(pdocmoPath))
        valores = wx_pdoCmo.leituraCmo(pdocmoPath, dtRodada)

    ##importar
    db_decks = wx_dbClass.db_mysql_master('db_decks')
    db_decks.connect()
    tb_ds_pdo_cmosist = db_decks.getSchema("tb_ds_pdo_cmosist")
    tb_submercado = db_decks.getSchema("tb_submercado")
 
    id_deck = importar_id_deck_ds(dt_referente=dt_ref, str_fonte=str_fonte)

    sql_select_submercado = tb_submercado.select()
    answer = db_decks.conn.execute(sql_select_submercado).fetchall()

    idSubmercados = {}
    for row in answer:
        idSubmercados[row[2].strip()] = row[0]

    values_to_insert = []
    for index, row in valores.iterrows():
        for submerc in valores.columns:
            if submerc == 'FC':
                continue

            submercadoId = idSubmercados[submerc]
            horario = row.name.strftime('%H:%M')
            preco = float(row[submerc])

            values_to_insert.append((id_deck, submercadoId, horario, preco))

    sql_delete = tb_ds_pdo_cmosist.delete().where(tb_ds_pdo_cmosist.c.id_deck == id_deck)
    n_values = db_decks.conn.execute(sql_delete).rowcount
    print(f"{n_values} Linhas deletadas na tb_ds_pdo_cmosist!")

    sql_insert = tb_ds_pdo_cmosist.insert().values(values_to_insert)
    n_values = db_decks.conn.execute(sql_insert).rowcount
    print(f"{n_values} Linhas inseridas na tb_ds_pdo_cmosist!")


def importar_ds_bloco_dp(path_zip:str, dt_ref:datetime=None, str_fonte:str ='ons'):

    dt_now = datetime.datetime.now().strftime("%d%m%Y %H%M%S")
    path_dst = os.path.join(PATH_DIR_LOCAL,"tmp",dt_now)

    extracted_file = DIR_TOOLS.extract_specific_files_from_zip(path=path_zip,files_name_template=['DP.txt'] ,dst=path_dst, extracted_files = [])
    if not extracted_file:
        extracted_file = DIR_TOOLS.extract_specific_files_from_zip(path=path_zip,files_name_template=['entdados.dat'] ,dst=path_dst, extracted_files = [])
    
    file = extracted_file[0]
    dtrodada = dt_ref - datetime.timedelta(days=1)


    print("Importando as previsoes de carga do DESSEM")
    print("Leitura do arquivo: {}".format(path_dst))

    id_deck = importar_id_deck_ds(dt_referente=dt_ref, str_fonte=str_fonte)

    regex = '(.{2})  (.{2})  (.{2}) (.{2}) (.{1}) (.{2}) (.{2}) (.{1}) (.{10})(.*)'

    dadosCarga = []
    file = open(file, 'r')
    for i, linha in enumerate(file):
        if linha[0:2] != 'DP':
            continue
        infoLinha = re.split(regex, linha)
        dadosCarga.append(infoLinha[1:-2])
    file.close()
    
    carga = pd.DataFrame(dadosCarga,columns=['mnemonico', 'ss', 'di', 'hi', 'mi', 'df', 'hf', 'mf', 'demanda'])

    DIR_TOOLS.remove_src(path_dst)

    carga['ss'] = carga['ss'].apply(pd.to_numeric)
    carga['di'] = carga['di'].apply(pd.to_numeric)
    carga['hi'] = carga['hi'].str.strip()
    carga['mi'] = carga['mi'].replace({'0':'00', '1':'30'})
    carga['demanda'] = carga['demanda'].apply(pd.to_numeric)

    # remocao do subsistema 11 (FC)
    carga.drop(carga[carga['ss'] == 11].index, inplace=True) 

    if type(dtrodada) == str:
        dtrodada = datetime.datetime.strptime(dtrodada, '%d/%m/%Y')

    # Mudanca do dia para data completa
    if dtrodada.weekday() == 4:
                dtrodada = dtrodada + datetime.timedelta(days=1)

    semanaEletrica = {}
    for i in range(7):
        dia = dtrodada + datetime.timedelta(days=i)
        semanaEletrica[dia.day] = dia.strftime("%Y-%m-%d")
    carga['di'] = carga['di'].replace(semanaEletrica)
    carga['di'] = carga['di'].astype(str)
    # Formatacao da data e do horario
    carga['dataHora'] = pd.to_datetime(carga['di'] + ' ' + carga['hi'].str.zfill(2) + ':' + carga['mi'].str.zfill(2), format="%Y-%m-%d %H:%M")

    # Formatacao da data e do horario
    carga.drop(['mnemonico','di', 'hi', 'mi', 'df', 'hf', 'mf'], axis='columns', inplace=True)

    carga['id_deck'] = id_deck
    df_carga = carga.rename(columns={'id_deck':'id_deck','ss':'cd_submercado','dataHora':'dataHora','demanda':'vl_carga'})
    df_carga[['id_deck','cd_submercado','dataHora','vl_carga']]
    df_carga_dessem_list = list(df_carga[['id_deck','cd_submercado','dataHora','vl_carga']].itertuples(index=False, name=None))


    db_decks = wx_dbClass.db_mysql_master('db_decks')
    db_decks.connect()
    tb_ds_carga = db_decks.getSchema('tb_ds_carga')

    sql_delete = tb_ds_carga.delete().where(tb_ds_carga.c.id_deck == id_deck)
    n_values = db_decks.conn.execute(sql_delete).rowcount
    print(f"{n_values} Linhas deletadas na tb_ds_carga!")

    sql_insert = tb_ds_carga.insert().values(df_carga_dessem_list)
    n_values = db_decks.conn.execute(sql_insert).rowcount
    print(f"{n_values} Linhas inseridas na tb_ds_carga!")



def importar_ds_bloco_tm(path_zip:str, dt_ref:datetime):

    #IMPLEMENTAR DE FORMA A FUNCIONAR PARA O ARQUIVO BLOCO_TM E PARA O ENTDADOS, BUSCANDO O BLOCO DENTRO DO ARQUIVO 
    if dt_ref.weekday() == 5:
        dt_now = datetime.datetime.now().strftime("%d%m%Y %H%M%S")
        path_dst = os.path.join(PATH_DIR_LOCAL,"tmp",dt_now)
        extracted_file = DIR_TOOLS.extract_specific_files_from_zip(path=path_zip,files_name_template=["Bloco_TM.txt"] ,dst=path_dst, extracted_files = [])

        path = extracted_file[0]

        cod_patamar = {}
        cod_patamar['LEVE'] = 1
        cod_patamar['MEDIA'] = 2
        cod_patamar['PESADA'] = 3
        
        dias_deck = {}
        for dia in range(7):
            dt = dt_ref + datetime.timedelta(days=dia)
            dias_deck[dt.day] = dt


        dt_final = dt_ref + datetime.timedelta(days=7)

        #verificar porque somar um dia na data referente
        # dt_ref = dt_ref + datetime.timedelta(days=1)

        df_bloco_tm = pd.read_csv(path, sep=" ", skipinitialspace=True, header=None, skiprows=4)
        DIR_TOOLS.remove_src(path_dst)

        df_bloco_tm[1].replace(dias_deck, inplace=True)
        df_bloco_tm[6].replace(cod_patamar, inplace=True)
        
        df_bloco_tm[3] = df_bloco_tm[3]*30
        
        df_bloco_tm[1] += pd.to_timedelta(df_bloco_tm[2], unit='H')
        df_bloco_tm[1] += pd.to_timedelta(df_bloco_tm[3], unit='m')
        
        discretizacao = []
        
        for idx, row in df_bloco_tm.iterrows():
            if len(discretizacao) == 0:
                disc = {'inicio': row[1], 'patamar': row[6]}
                discretizacao.append((row[1],row[6]))
            elif disc['patamar'] != row[6]:
                disc = {'inicio': row[1], 'patamar': row[6]}
                discretizacao.append((row[1],row[6]))

        db_decks = wx_dbClass.db_mysql_master('db_decks')
        db_decks.connect()
        tb_ds_bloco_tm = db_decks.getSchema('tb_ds_bloco_tm')

        sql_delete = tb_ds_bloco_tm.delete().where(
            tb_ds_bloco_tm.c.dt_inicio_patamar >= dt_ref,
            tb_ds_bloco_tm.c.dt_inicio_patamar < dt_final,
            )
        n_values = db_decks.conn.execute(sql_delete).rowcount
        print(f"{n_values} Linhas deletadas na tb_ds_bloco_tm!")

        sql_insert = tb_ds_bloco_tm.insert().values(discretizacao)
        n_values = db_decks.conn.execute(sql_insert).rowcount
        print(f"{n_values} Linhas inseridas na tb_ds_bloco_tm!")

    else: 
        print("A inserção no banco é feita somente nos dias de previsão com inicio no sábado!")


def importar_deck_values_ds(path_zip:str,dt_ref:datetime=None,str_fonte:str='ons'):

    dt_ref = dt_ref if dt_ref else datetime.datetime.now() + datetime.timedelta(days=1)

    ultimoSabado = wx_opweek.getLastSaturday(dt_ref)
    semanaEletrica = wx_opweek.ElecData(ultimoSabado.date())

    if str_fonte == 'ccee':
        resultDiaZip = 'Resultado_DS_CCEE_{:0>2}{}_SEMREDE_RV{}D{}.zip'
        resultDiaZip =resultDiaZip.format(
            semanaEletrica.mesReferente, 
            semanaEletrica.anoReferente, 
            semanaEletrica.atualRevisao, 
            dt_ref.strftime('%d'))
        
        dt_now = datetime.datetime.now().strftime("%d%m%Y %H%M%S")
        path_dst = os.path.join(PATH_DIR_LOCAL,"tmp",dt_now)

        with zipfile.ZipFile(path_zip, 'r') as zip_ref:
            zip_ref.extract(resultDiaZip,path_dst)

        path_zip_interno = os.path.join(path_dst,resultDiaZip)
        importar_pdo_cmosist_ds(path_zip_interno,dt_ref,str_fonte='ccee')
    
    elif str_fonte == 'ons':
        importar_pdo_cmosist_ds(path_zip,dt_ref,str_fonte='ons')
        importar_renovaveis_ds(path_zip,dt_ref,str_fonte='ons')    
        importar_ds_bloco_dp(path_zip,dt_ref,str_fonte='ons')
        importar_ds_bloco_tm(path_zip,dt_ref)


if __name__ == '__main__':
    pass

    # path_zip = r"C:\Users\cs399274\Downloads\DES_202405.zip"
    # pathDessemZip = r"C:\Users\cs399274\Downloads\Blocos_2024-04-27.zip"


    # dt_ref = datetime.datetime(2024,4,27)
    # importar_ds_bloco_tm(path_zip, dt_ref)

    # dt_ref = datetime.datetime.now() + datetime.timedelta(days=1)
    # pathDessemZip = r"C:\Users\cs399274\Downloads\DES_202405 (1).zip"
    # pathDessemZip = r"C:\Users\cs399274\Downloads\ds_ons_052024_rv1d06.zip"
    # pathDessemZip= "/WX/WX2TB/Documentos/fontes/outros/webhook/arquivos/tmp/Decks de entrada e saída - Modelo DESSEM/ds_ons_052024_rv1d06.zip"

    # importar_deck_values_ds(pathDessemZip,dt_ref,str_fonte='ccee')
