import os
import re
import sys
import pdb
import zipfile
import datetime 
import pdfplumber
import pandas as pd
import sqlalchemy as db

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")), ".env"))
PATH_PROJETO = os.getenv("PATH_PROJETO", "/WX2TB/Documentos/fontes/PMO")
sys.path.insert(1,f"{PATH_PROJETO}/scripts_unificados")

from bibliotecas import wx_dbClass

def read_PrevCargaDessem_files_saida(path_zip:str):
    
    padrao_arquivo = re.compile(r'_(\d{4}-\d{2}-\d{2})\.zip')
    dt_deck = padrao_arquivo.search(os.path.basename(path_zip)).group(1)
    print(f"Leitura do deck do dia {dt_deck}")
    
    padrao_submerc = re.compile(r'_(NE|N|SECO|S)_')
    padrao_file = re.compile(r'_\d{4}-\d{2}-\d{2}_([\w-]+)\.csv')
    deck_values = {}

    with zipfile.ZipFile(path_zip, 'r') as zip_file:
        files_submercado = zip_file.filelist
        for file_submerc in files_submercado:
            submercado = padrao_submerc.search(file_submerc.filename).group(1)

            deck_values[submercado] = None
            
            with zip_file.open(file_submerc) as csv_file:
                                
                df_file = pd.read_csv(csv_file, delimiter = ";",  decimal=',', thousands='.')

                deck_values[submercado] = df_file

    return deck_values

def importar_prev_carga_dessem_saida(path_zip:str):

    submercados = {"SECO":1,"S":2,"NE":3,'N':4}

    df_final = pd.DataFrame()

    values =  read_PrevCargaDessem_files_saida(path_zip)

    for submercado in values.keys():

        df_aux = values[submercado]
        df_aux['DataHora'] = pd.to_datetime(df_aux['din_referencia']).dt.strftime('%Y-%m-%d %H:%M')
        df_aux['val_previsaocarga'] = df_aux['val_previsaocarga'].astype(float)
        df_aux['cd_submercado'] = submercados[submercado]
        df_aux['dt_ini'] = pd.to_datetime(df_aux['DataHora'].min()).strftime('%Y-%m-%d')

        df_final = pd.concat([df_final,df_aux])

    df_final = df_final[['cd_submercado','DataHora','val_previsaocarga','dt_ini']]

    values = df_final.values.tolist()

    db_ons = wx_dbClass.db_mysql_master('db_ons')
    db_ons.connect()
    tb_prev_carga = db_ons.getSchema('tb_prev_carga')

    sql_delete = tb_prev_carga.delete().where(tb_prev_carga.c.dt_inicio == df_final['dt_ini'].min())
    n_values = db_ons.conn.execute(sql_delete).rowcount
    print(f"{n_values} Linhas deletadas na tb_prev_carga!")

    sql_insert = tb_prev_carga.insert().values(values)
    n_values = db_ons.conn.execute(sql_insert).rowcount
    print(f"{n_values} Linhas inseridas na tb_prev_carga!")


def importar_carga_ipdo(path:str,dtRef:str):

    print("Importando o IPDO do sistema interligado nacional")
    print("Leitura do arquivo: {}".format(path))
    # leitura de arquivo em pdf para captura dos dados de carga do IPDO, capturando todo o texto em todas as paginas do PDF.
    texto = ""
    with pdfplumber.open(path) as pdf:
        num_paginas = len(pdf.pages)
        for pagina in range(num_paginas):
                        conteudo_pagina = pdf.pages[pagina].extract_text()
                        texto += conteudo_pagina
    textoLista = texto.split('\n')
    # Apos transformar pdf em texto, foi feito uma busca pegado a posição dos dados de carga IPDO que precisamos.

    meses = {
    'Janeiro': 1,
    'Fevereiro': 2,
    'Março': 3,
    'Abril': 4,
    'Maio': 5,
    'Junho': 6,
    'Julho': 7,
    'Agosto': 8,
    'Setembro': 9,
    'Outubro': 10,
    'Novembro': 11,
    'Dezembro': 12
       }
    try:
        dtRef = datetime.datetime.strptime(dtRef.strftime('%Y-%m-%d'), '%Y-%m-%d').date()
        data = textoLista[1]
        data = data.split(',')
        data = data[1].strip()
        dataFormat = data.split(' ')
        mes = meses[dataFormat[1]]
        dia = dataFormat[0]
        ano = dataFormat[3]
        dataFormatada = f"{dia}/{mes}/{ano}"
        dataFinal = datetime.datetime.strptime(dataFormatada,"%d/%m/%Y").date()
    except IndexError:
        print(f"IndexError, verificar linhas do PDF a posição da data não está batendo com dados solicitados.")
    if dataFinal == dtRef:
        sul = textoLista[40].split()
        sul = sul[1].replace('.', '')
        cargaSul = int(sul)
        sudeste = textoLista[41].split()
        sudeste = sudeste[3].replace('.','')
        cargaSudeste = int(sudeste)
        norte = textoLista[42].split()
        norte = norte[1].replace('.','')
        cargaNorte = int(norte)
        nordeste = textoLista[43].split()
        nordeste = nordeste[1].replace('.','')
        cargaNordeste = int(nordeste)
        listaCargaIpdo = [dataFinal,cargaSudeste,cargaSul,cargaNordeste,cargaNorte]

        db_ons = wx_dbClass.db_mysql_master('db_ons')
        db_ons.connect()
        tb_carga_ipdo = db_ons.getSchema('tb_carga_ipdo')
        query_get_dtReferente = db.select(tb_carga_ipdo.c.dt_referente).where(db.and_(tb_carga_ipdo.c.dt_referente == dtRef))
        dtReferente = db_ons.conn.execute(query_get_dtReferente).scalar()
        dataf = datetime.datetime.strftime(dataFinal, '%d-%m-%Y')
        if dtReferente:
            print(f'O IPDO do dia: {dataf} já estava cadastrado no banco de dados!')
            delete_dtReferente = tb_carga_ipdo.delete().where(tb_carga_ipdo.c.dt_referente == dtReferente)
            db_ons.conn.execute(delete_dtReferente)
            insert_carga_ipdo = tb_carga_ipdo.insert().values((listaCargaIpdo))
            db_ons.conn.execute(insert_carga_ipdo)
        else:
            insert_carga_ipdo = tb_carga_ipdo.insert().values((listaCargaIpdo))
            db_ons.conn.execute(insert_carga_ipdo)
            print(f'IPDO do dia {dataf}')
    else:
        print(f'A data {data} do arquivo solicitado não é a mesma data {dtRef} do dia que você quer cadastrar, Não foi cadastrado a valores do IPDO no banco de dados')
        quit()


if __name__ == '__main__':

    pass

    
