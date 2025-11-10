import sys
import time
import datetime
import requests
import pandas as pd

sys.path.insert(1,"/WX2TB/Documentos/fontes/PMO/scripts_unificados/")
from bibliotecas import  wx_dbLib


mapping_postos_names ={'posto': {
    #SE

    "M. MORAES": 7 ,
    "FURNAS" : 6,
    "MARIMBONDO" : 17,
    "A. VERMELHA" : 18,
    "PARAIBUNA": 121,
    "I. SOLTEIRA": 34,
    "BATALHA": 22,
    "SÃO SIMÃO":33,
    "ITUMBIARA":31,
    "EMBORCAÇÃO":24,
    "NOVA PONTE":25,
    "SERRA DO FACÃO":251,
    "CHAVANTES":49,
    "CAPIVARA":61,
    "JURUMIRIM":47,
    "TRÊS MARIAS":156,
    "TRÊS IRMÃOS":243,
    "BILLINGS":118,
    "B. BONITA":237,
    "SERRA DA MESA":270,

    #SUL
    "G. P. SOUZA": 115,
    "SALTO SANTIAGO":77,
    "G. B. MUNHOZ":74,
    "SEGREDO":76,
    "SANTA CLARA-PR":71,
    "PASSO REAL":111,
    "MAUA":57,
    "PASSO FUNDO":93,
    "CAMPOS NOVOS":216,
    "SAO ROQUE":88,
    "MACHADINHO":217,
    "BARRA GRANDE":215,
    #NE
    "IRAPE":255,
    "SOBRADINHO":169,
    "ITAPARICA":172,

    #NORTE
    "BALBINA":269,
    "TUCURUI":275,
    },
    'submercado' : {'SUDESTE':1,'SUL':2,'NORDESTE':3,'NORTE':4}
    }

def request_reservatorios_page():
    url='http://tr.ons.org.br/Content/Get/SituacaoDosReservatorios'
    
    response = requests.get(url)
    response_json = response.json()
    return response_json


def compare_page_values_rdh(response_json, dt_rdh):
    
    df_situacao_reservatorios = pd.DataFrame(response_json)
    dt_atualizacao_pagina = pd.to_datetime(df_situacao_reservatorios['Data']).max()

    mapping_submercado = {'Sudeste / Centro-Oeste':'SUDESTE','Sul':'SUL','Nordeste':'NORDESTE','Norte':'NORTE'}
    df_situacao_reservatorios['Subsistema'] = df_situacao_reservatorios["Subsistema"].replace(mapping_submercado)
    
    values_granularidade = []
    for granularidade, column_name in [('posto','Reservatorio'),('submercado','Subsistema')]:

        db_values = wx_dbLib.get_df_info_rdh(
                dt_ini=dt_rdh,
                tipos=['ear'],
                data_type = "dia",
                granularidade= granularidade
                )
        
        df_situacao_reservatorios['cd_granularidade'] = df_situacao_reservatorios[column_name].replace(mapping_postos_names[granularidade])
        df_merged = pd.merge(df_situacao_reservatorios,db_values[['cd_granularidade','ear']], on='cd_granularidade')
        df_merged['Diferenca percentual'] =(df_merged[f'{column_name}ValorUtil'] - df_merged['ear'])

        df_merged['Subsistema'] = pd.Categorical(df_merged['Subsistema'], categories=['SUDESTE','SUL','NORDESTE','NORTE'], ordered=True)
        df_merged = df_merged.sort_values(['Subsistema','Bacia'])
        df_merged['Subsistema'] = df_merged['Subsistema'].astype('str')

        if granularidade == 'posto': group_list = ['Subsistema','Bacia','Reservatorio']
        else: group_list = ['Subsistema']

        df_final = df_merged.groupby(group_list,sort=False)[[f'{column_name}ValorUtil','Diferenca percentual']].mean()
        df_final.columns = [ f'ARM({dt_atualizacao_pagina.strftime("%d/%m/%Y")})', 'Difereça %']
        
        values_granularidade += df_final,
    
    return values_granularidade[0], values_granularidade[1]


def rotina_situacao_reservatorios():
    print("Dados do dia foram atualizados!")
    print("Iniciando Rotina de comparação das situações dos reservatórios.")

    #control dates
    dt_now = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    dt_expectativa_atualizacao = dt_now - datetime.timedelta(days=1)

    #espera pera atualização
    while 1:
        response_json = request_reservatorios_page()
        df_situacao_reservatorios = pd.DataFrame(response_json)
        dt_atualizacao_pagina = pd.to_datetime(df_situacao_reservatorios['Data']).max()

        #compare condition
        if dt_expectativa_atualizacao == dt_atualizacao_pagina:
            break

        print("Não encontrado, reiniciando busca em 5 min")
        time.sleep(60*5)

    #load
    df_situacao_reservatorios = pd.DataFrame(response_json)
    dt_atualizacao_pagina = pd.to_datetime(df_situacao_reservatorios['Data']).max()

    #rdh dia anterior
    dt_rdh_anterior = dt_atualizacao_pagina - datetime.timedelta(days=1)
    print(f"Comparando valores da pagina com o rdh ({dt_rdh_anterior.strftime('%d/%m/%Y')})")
    df_posto, df_submercado = compare_page_values_rdh(response_json=response_json,dt_rdh=dt_rdh_anterior)

    return df_posto, df_submercado


if __name__ == "__main__":

    df_posto, df_submercado = rotina_situacao_reservatorios()