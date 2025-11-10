
import os
import sys
import pdb
import zipfile 
import datetime
import pandas as pd

sys.path.insert("/WX2TB/Documentos/fontes/PMO/scripts_unificados/")
from bibliotecas import wx_dbClass

def read_eolica_files(path_zip):

    submercados = {'S':2, 'NE':3}
    
    archive = zipfile.ZipFile(path_zip, 'r')
    file_name = archive.filelist[0].filename
    path_interno = os.path.join(file_name,"Previsoes por Usinas/Previsao combinada/Previsoes_")

    lista_de_arquivos = archive.filelist

    files = [arquivo for arquivo in lista_de_arquivos if path_interno in arquivo.filename]

    df_values= pd.DataFrame()
    for file in files:
        
        file_name = os.path.basename(file.filename)
        title, submercado, dt_ini, dt_fim  = file_name.replace(".txt","").split("_") 
        
        data_file = datetime.datetime.strptime(dt_ini,"%Y%m%d")
        data_ref = datetime.datetime.strptime(dt_fim,"%Y%m%d")

        df_teste = pd.read_csv(archive.open(file), sep=';', encoding='latin', header=None).T

        df_teste.columns = df_teste.loc[0].values
        df_teste = df_teste[1:].reset_index(drop=True).astype(float)
        df_teste = df_teste.reset_index()
        
        df_teste['horario'] = df_teste['index'].apply(lambda x: data_ref + datetime.timedelta(minutes=30*x)) 
        
        df_teste = df_teste.set_index('horario').sum(axis=1).reset_index()
        df_teste['submercado'] = submercados[submercado]
        df_teste['dt_deck'] = data_file 
        df_values = pd.concat([df_values,df_teste])
        
    
    archive.close()
    return df_values

def importar_eolica_files(path_zip):
    df_eolica = read_eolica_files(path_zip)
    dt_deck = df_eolica['dt_deck'].unique()[0]
    df_eolica['id'] = None
    values = df_eolica[['id','submercado', 0,'horario','dt_deck']].values.tolist()

    db_ons = wx_dbClass.db_mysql_master('db_ons')
    db_ons.connect()
    tb_weol_eolica = db_ons.getSchema('tb_weol_eolica')

    sql_delete = tb_weol_eolica.delete().where(tb_weol_eolica.c.dt_deck == dt_deck)
    n_values = db_ons.conn.execute(sql_delete).rowcount
    print(f"{n_values} Linhas deletadas na tb_weol_eolica!")

    sql_insert = tb_weol_eolica.insert().values(values)
    n_values = db_ons.conn.execute(sql_insert).rowcount
    print(f"{n_values} Linhas inseridas na tb_weol_eolica!")



if __name__ == '__main__':

    pass





