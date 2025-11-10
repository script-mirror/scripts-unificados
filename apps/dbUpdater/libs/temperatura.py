
import os
import re
import sys
import pdb
import zipfile 
import datetime
import pandas as pd
import sqlalchemy as db

sys.path.insert(1,"/WX2TB/Documentos/fontes/PMO/scripts_unificados/")
from bibliotecas import wx_dbClass

submercados = {"SECO":1,"S":2,"NE":3,'N':4}

def read_PrevCargaDessem_files(path_zip, names = ['CARGAHIST','COMBINA','EXOGENAHIST', 'EXOGENAPREV','FERIADOS','HISTCOMB','PATAMARES']):
    
    padrao_arquivo = re.compile(r'_(\d{4}-\d{2}-\d{2})\.zip')
    dt_deck = padrao_arquivo.search(os.path.basename(path_zip)).group(1)
    print(f"Leitura do deck do dia {dt_deck}")
    
    padrao_submerc = re.compile(r'(NE|N|SECO|S)')
    padrao_file = re.compile(r'_\d{4}-\d{2}-\d{2}_([\w-]+)\.csv')
    deck_values = {}

    with zipfile.ZipFile(path_zip, 'r') as zip_file_externo:
        zip_files_submercado = zip_file_externo.filelist
        for zip_submerc in zip_files_submercado:
            try:
                submercado = padrao_submerc.search(zip_submerc.filename).group(1)
            except:
                continue
            deck_values[submercado] = {}
            with zip_file_externo.open(zip_submerc) as dir_submerc:
                with zipfile.ZipFile(dir_submerc, 'r') as files_dir:
                    files = files_dir.filelist
                    for file_csv in files:
                        
                        file_name = padrao_file.search(file_csv.filename).group(1)
                        
                        if file_name in names:
                            deck_values[submercado][file_name] = None
                            with files_dir.open(file_csv) as csv_file:
                                
                                df_file = pd.read_csv(csv_file, delimiter = ";",  decimal=',', thousands='.')

                                deck_values[submercado][file_name] = df_file

    return deck_values



def importar_temperatura_prev_dessem(path_zip):
    df_final = pd.DataFrame()

    values =  read_PrevCargaDessem_files(path_zip, names = ['EXOGENAPREV'])

    for submercado in values.keys():

        df_aux = values[submercado]['EXOGENAPREV']
        df_aux['DataHora'] = pd.to_datetime(df_aux['DataHora']).dt.strftime('%Y-%m-%d %H:%M')
        df_aux['Exo_Temperatura'] = df_aux['Exo_Temperatura'].astype(float)
        df_aux['cd_submercado'] = submercados[submercado]
        df_aux['dt_ini'] = df_aux['DataHora'].min()

        df_final = pd.concat([df_final,df_aux])

    df_final = df_final[['cd_submercado','DataHora','Exo_Temperatura','dt_ini']]

    values = df_final.values.tolist()

    db_ons = wx_dbClass.db_mysql_master('db_ons')
    db_ons.connect()
    tb_prev_carga_temperatura = db_ons.getSchema('tb_prev_carga_temperatura')

    sql_delete = tb_prev_carga_temperatura.delete().where(tb_prev_carga_temperatura.c.dt_inicio == df_aux['DataHora'].min())
    n_values = db_ons.conn.execute(sql_delete).rowcount
    print(f"{n_values} Linhas deletadas na tb_prev_carga_temperatura!")

    sql_insert = tb_prev_carga_temperatura.insert().values(values)
    n_values = db_ons.conn.execute(sql_insert).rowcount
    print(f"{n_values} Linhas inseridas na tb_prev_carga_temperatura!")


def importar_temperatura_obs(path_zip,insertAll = False):

    df_final = pd.DataFrame()

    values =  read_PrevCargaDessem_files(path_zip, names = ['EXOGENAHIST'])

    for submercado in values.keys():

        df_aux = values[submercado]['EXOGENAHIST']
        df_aux['DataHora'] = pd.to_datetime(df_aux['DataHora']).dt.strftime('%Y-%m-%d %H:%M')
        df_aux['Exo_Temperatura'] = df_aux['Exo_Temperatura'].astype(float)
        df_aux['cd_submercado'] = submercados[submercado]

        df_final = pd.concat([df_final,df_aux])

    df_final = df_final[['cd_submercado','DataHora','Exo_Temperatura']]

    ultimo_dia_historico = datetime.datetime.strptime(df_final['DataHora'].max(),"%Y-%m-%d %H:%M")
    ultimo_dia_historico_str = ultimo_dia_historico.strftime("%Y-%m-%d")

    dt_deck = (ultimo_dia_historico + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    

    db_ons = wx_dbClass.db_mysql_master('db_ons')
    db_ons.connect()
    tb_temperatura_obs = db_ons.getSchema('tb_temperatura_obs')


    if insertAll:

        sql_delete = tb_temperatura_obs.delete()
        n_values = db_ons.conn.execute(sql_delete).rowcount
        print(f"{n_values} Linhas deletadas na tb_temperatura_obs!")

        years_in_history = pd.to_datetime(df_final['DataHora']).dt.year.unique()
        for year in years_in_history:
            df_aux_final = df_final[(df_final['DataHora'] <f"{year+1}-01-01")&(df_final['DataHora'] >f"{year}-01-01")]
            values = df_aux_final.values.tolist()
            sql_insert = tb_temperatura_obs.insert().values(values)
            n_values = db_ons.conn.execute(sql_insert).rowcount
            print(f"{n_values} Linhas inseridas na tb_temperatura_obs para o ano de {year}!")
    else:

        sql_delete = tb_temperatura_obs.delete().where(db.and_(tb_temperatura_obs.c.dt_referente >= ultimo_dia_historico_str, tb_temperatura_obs.c.dt_referente < dt_deck))
        n_values = db_ons.conn.execute(sql_delete).rowcount
        print(f"{n_values} Linhas deletadas na tb_temperatura_obs para o dia {ultimo_dia_historico_str}!")
        
        df_aux_final = df_final[(df_final['DataHora'] >= ultimo_dia_historico_str)&(df_final['DataHora'] < dt_deck)]
        insert_values = df_aux_final.values.tolist()
        sql_insert = tb_temperatura_obs.insert().values(insert_values)
        n_values = db_ons.conn.execute(sql_insert).rowcount
        print(f"{n_values} Linhas inseridas na tb_temperatura_obs para o dia {ultimo_dia_historico_str}!")




if __name__ == '__main__':


    pass

    # path_zip = r"C:\Users\cs399274\Downloads\Deck_2023-12-05.zip"

    #inserir previsão do deck
    #importar_temperatura_prev_dessem(path_zip)

    #inserir ultimo dia de temperatura observada do arquivo
    # importar_temperatura_obs(path_zip)

    #inserir histórico inteiro do arquivo
    # importar_temperatura_obs(path_zip,insertAll=True)












    


    











