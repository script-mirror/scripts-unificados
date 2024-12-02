

import os
import re
import sys
import pandas as pd
import datetime
import locale
import pdb
locale.setlocale(locale.LC_TIME, 'pt_BR.utf8')

sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.bibliotecas import rz_dir_tools
PATH_DOWNLOAD_TMP = os.path.join(os.path.dirname(__file__),"info_arquivos_externos","tmp")


def extract_file_estudo(path_estudo,path_saida=PATH_DOWNLOAD_TMP):

    DIR_TOOLS = rz_dir_tools.DirTools()

    extracted_zip_estudo = DIR_TOOLS.extrair_zip_mantendo_nome_diretorio(
        path_estudo,
        path_saida,
        deleteAfterExtract=True
    )

    return extracted_zip_estudo

def get_rv_atual_from_zip(nome_arquivo_zip) -> datetime.datetime:

    match = re.match(r'RV([0-9]{1})_PMO_([A-z]+)([0-9]{4})_carga_semanal', nome_arquivo_zip)
    rv = int(match.group(1))
    mes = match.group(2)
    ano = match.group(3)
    try:
        mes_ref = datetime.datetime.strptime(mes+ano, '%B_%Y')
    except:
        mes_ref = datetime.datetime.strptime(mes+ano, '%B%Y')
    
    inicio_mes_eletrico = mes_ref
    while inicio_mes_eletrico.weekday() != 5:
        inicio_mes_eletrico = inicio_mes_eletrico - datetime.timedelta(days=1)
    inicio_rv_atual = inicio_mes_eletrico + datetime.timedelta(days=7*rv)
    return inicio_rv_atual 


def get_rv_atual_from_dadger(path_dadger) -> datetime.datetime:
    data = path_dadger[path_dadger.index("/DC")+3:path_dadger.index("-sem")]
    mes_ref = datetime.datetime.strptime(data, "%Y%m")
    pdb.set_trace()
    rv = int(path_dadger[path_dadger.index("rv")+2:])
    
    inicio_mes_eletrico = mes_ref
    while inicio_mes_eletrico.weekday() != 5:
        inicio_mes_eletrico = inicio_mes_eletrico - datetime.timedelta(days=1)
    inicio_rv_atual = inicio_mes_eletrico + datetime.timedelta(days=7*rv)
    return inicio_rv_atual 


def trim_df(df:pd.DataFrame) -> pd.DataFrame:
    df_obj = df.select_dtypes('object')
    df[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())
    return df


def adicionar_simbolo(linhas: list, simbolos_sequenciais: list = ["&"], intervalo_linhas: int = 5):
    """
    Adiciona símbolos em sequência após cada intervalo de linhas.
    Repete a sequência de símbolos se a lista for menor que o número de intervalos.
    """
    linhas_modificadas = []
    simbolos_len = len(simbolos_sequenciais)

    for i, linha in enumerate(linhas, 1):
        linhas_modificadas.append(linha)
        if i % intervalo_linhas == 0:
            simbolo = simbolos_sequenciais[(i // intervalo_linhas - 1) % simbolos_len]
            linhas_modificadas.append(simbolo)
    
    return linhas_modificadas
