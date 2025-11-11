import os
import sys
import pdb
import logging
import datetime
import pandas as pd
import requests as req
from typing import Callable
from dotenv import load_dotenv
sys.path.insert(1,"/WX2TB/Documentos/fontes/PMO/scripts_unificados/")
from apps.web_modelos.server.server import cache
from apps.web_modelos.server.libs import rz_ena, rz_dbLib

load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")),'.env'))

logging.basicConfig(level=logging.INFO,
                    format='%(levelname)s:\t%(asctime)s\t %(name)s.py:%(lineno)d\t %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers=[
                        logging.StreamHandler()
                    ])
logger = logging.getLogger(__name__)

from middle.utils import Constants
constants = Constants()

URL_COGNITO = os.getenv('URL_COGNITO')
CONFIG_COGNITO = os.getenv('CONFIG_COGNITO')

def get_access_token() -> str:
    response = req.post(
        URL_COGNITO,
        data=CONFIG_COGNITO,

        headers={'Content-Type': 'application/x-www-form-urlencoded'}
    )
    return response.json()['access_token']


def cache_acomph(prefixo,granularidade,dataInicial,dataFinal=None,flag_atualizar=None,reset=None):

  dt_ontem = datetime.datetime.now() - datetime.timedelta(days=1)

  key = prefixo+f':{granularidade}'
  cache_values_acomph  = cache.get(key)

  #se existir o cache
  if cache_values_acomph and not reset:
    df_ena_aux = pd.DataFrame(cache_values_acomph)
    ultima_data_cache = pd.to_datetime(df_ena_aux.index.max())
    
    #se não for para atualizar retorna os resultados
    if not flag_atualizar:
      send_cache_data = pd.DataFrame(cache_values_acomph).loc[dataInicial.strftime('%Y/%m/%d'):]
      if dataFinal:
        send_cache_data = send_cache_data.loc[:dataFinal.strftime('%Y/%m/%d')]
      return send_cache_data.to_dict()

  #se não existir cria com todos os dados do banco
  else:
    print("reset cache")
    df_ena_aux = pd.DataFrame()
    ultima_data_cache = datetime.datetime.strptime("2013/01/01", "%Y/%m/%d")

  #se ja estiver atualizado retorna dados
  if ultima_data_cache == dt_ontem.strftime('%Y/%m/%d'):
      send_cache_data = pd.DataFrame(cache_values_acomph).loc[dataInicial.strftime('%Y/%m/%d'):]
      if dataFinal:
        send_cache_data = send_cache_data.loc[:dataFinal.strftime('%Y/%m/%d')]
      return send_cache_data.to_dict() 
  
  #se não busca dados no banco desde a ultima data em cache - 30 dias
  else:
    dt_acomph_seguinte = ultima_data_cache + datetime.timedelta(days=1)
    acomph = rz_ena.getAcomph(dt_acomph_seguinte,dataFinal)

    if acomph:
      dt_ini = dt_acomph_seguinte - datetime.timedelta(days=29)
      ena = rz_ena.get_ena_acomph(dt_ini, granularidade)

      if not df_ena_aux.empty:
        ena_aux = df_ena_aux[:min(ena.index)][:-1]
        ena = pd.concat([ena_aux,ena])

      send_cache_data =  ena

      cache.set(key,send_cache_data.to_dict(), timeout=60*60*24*7)

      send_cache_data = ena.loc[dataInicial.strftime('%Y/%m/%d'):]
      if dataFinal:
        send_cache_data = send_cache_data.loc[:dataFinal.strftime('%Y/%m/%d')]

      return send_cache_data.to_dict()

def atualizar_cache_acomph(dt_inicial, reset=False):
  
  if reset:
    print("Resentando cache!")
  cache_acomph(prefixo="ACOMPH",granularidade='submercado',dataInicial=dt_inicial,flag_atualizar=True,reset=reset)
  cache_acomph(prefixo="ACOMPH",granularidade='bacia',dataInicial=dt_inicial,flag_atualizar=True,reset=reset)
  import_acomph_visualization_api(dt_inicial)
  import_acomph_consolidado(dt_inicial)
  

  print("CACHE ACOMPH ATUALIZADO!")
 
def cache_rdh(ano: str, tipo:str, atualizar:bool = False):
  key = f'{tipo}:{ano}'

  if not atualizar:
    cached = cache.get(key)
    if cached:
      return cached
    df_rdh = rz_dbLib.get_df_info_rdh(ano = ano, tipo = tipo)
    cache.set(key, df_rdh, timeout=60*60*24*7)
    return df_rdh
  else:
    df_rdh = rz_dbLib.get_df_info_rdh(ano = ano, tipo = tipo)
    cache.set(key, df_rdh, timeout=60*60*24*7)
    return df_rdh
  
def atualizar_cache_rdh(ano: str = str(datetime.datetime.now().year)):
  tipos= ['ear', 'vaz_turbinada', 'vaz_afluente', 'vaz_defluente', 'vaz_incremental', 'vaz_vertida']
  print('Atualizando cache de RDH')
  for tipo in tipos:
    get_cached("get_df_info_rdh", ano, tipo, atualizar=True)
  print('CACHE RDH ATUALIZADO')


def get_key(function_key, *args):
  key = str(args).replace('(', '').replace(')', '').replace(',', ':').replace('\'', '')
  return f'{function_key}:{key}'

def get_cached(function:Callable, *args, atualizar:bool = False, timeout=60*60*24*7) -> dict:
  key = get_key(function.__name__, *args)
  if not atualizar:
    cached = cache.get(key)
    if cached:
      return cached
    data = function(*args)
    cache.set(key, data, timeout=timeout)
    return data
  data = function(*args)
  cache.set(key, data, timeout=timeout)
  return data


def get_cached_values(key):
    return cache.get(key) if cache.get(key) else {}

def set_cache_values(key, data, timeout=60*60*24*7):
    return cache.set(key, data, timeout=timeout) if data else None

def import_acomph_visualization_api(data_rodada:datetime.date):
  
  data_rodada_date = datetime.datetime.combine(data_rodada + datetime.timedelta(days=1), datetime.time(0))
  data_rodada_str = data_rodada_date.isoformat()
  data_rodada_str = f'{data_rodada_str}.000Z'
  
  body:dict = {
  "dataRodada": data_rodada_str,
  "dataFinal": data_rodada_str,
  "mapType": "ena",
  "idType": "",
  "modelo": "ACOMPH",
  "priority": None,
  "grupo": "ONS",
  "rodada": "0",
  "viez": True,
  "membro":"0",
  "measuringUnit": "MWm",
  "propagationBase": "VNA",
  "generationProcess": "SMAP",
  "data": []
  }
  

  for granularidade in ['submercado','bacia']:
        acomph_cache = cache_acomph(prefixo="ACOMPH",granularidade=granularidade,
                                  dataInicial=data_rodada-datetime.timedelta(days=30))
        
        df = pd.DataFrame(acomph_cache).reset_index()
        df.rename(columns={'index':'dataReferente'}, inplace=True)
        
        df['dataReferente'] = pd.to_datetime(df['dataReferente'])
        df['dataReferente'] = df['dataReferente'].dt.strftime('%Y-%m-%d')
        
        valores_mapa = []
        for idx, row in df.iterrows():
            data = row['dataReferente']
            for col in df.columns[1:]:
                valores_mapa.append({
                    'dataReferente': data,
                    'valor': row[col],
                    'valorAgrupamento': col
                })
        
        body["data"].append({
            "valoresMapa": valores_mapa,
            "agrupamento": granularidade
        })
      
  res = req.post('https://tradingenergiarz.com/backend/api/map', json=body, headers={'Content-Type': 'application/json', "Authorization":f"Bearer {get_access_token()}"})
  
  if res.status_code == 201:
    logger.info(f"Modelo do ACOMPH da data {data_rodada} inserido na API de visualizacao")
  else:
    logger.warning(res.text)
    logger.warning(f"Erro ao tentar inserir modelo do ACOMPH da data {data_rodada} na API de visualizacao")
    

def import_acomph_consolidado(data_rodada:datetime.date):
    valores_mapa = []
    for granularidade in ['submercado','bacia']:
        acomph_cache = cache_acomph(prefixo="ACOMPH",granularidade=granularidade,
                                  dataInicial=data_rodada-datetime.timedelta(days=30))
        
        df = pd.DataFrame(acomph_cache).reset_index()
        df.rename(columns={'index':'dataReferente'}, inplace=True)
        
        df['dataReferente'] = pd.to_datetime(df['dataReferente'])
        df['dataReferente'] = df['dataReferente'].dt.strftime('%Y-%m-%d')
        
        for idx, row in df.iterrows():
            data = row['dataReferente']
            for col in df.columns[1:]:
                valores_mapa.append({
                    'data': data,
                    'granularidade': granularidade,
                    'ena': row[col],
                    'localizacao': col
                })
    df = pd.DataFrame(valores_mapa)
    df.sort_values(['data', 'granularidade', 'localizacao'])
    res = req.post('https://tradingenergiarz.com/api/v2/ons/ena-acomph', json=df.to_dict('records'), headers={"Authorization":f"Bearer {get_access_token()}"})
    if res.status_code >= 200 and res.status_code < 300:
        logger.info(f"Modelo do ACOMPH da data {data_rodada} inserido na tabela ena_acomph")
    else:
        logger.warning(res.text)
        logger.warning(f"Erro {res.status_code} ao tentar inserir modelo do ACOMPH da data {data_rodada} na tabela ena_acomph")
                


def printHelper():

    hoje = datetime.datetime.now()
    modelo = 'pconjunto'
    horaRodada = '00'
    atualizar = 'atualizar'


def runWithParams():

    reset = False
    data = datetime.datetime.now().replace(hour=0,minute=0,second=0)

    if len(sys.argv) > 1:

      #parametros de inicialização
      for i in range(1, len(sys.argv)):
        argumento = sys.argv[i].lower()

        if argumento == 'dt_rodada' or argumento == 'data':
          print(sys.argv[i+1])
          try:
            data = datetime.datetime.strptime(sys.argv[i+1], '%Y-%m-%d')
          except Exception as e:
            print('Erro ao tentar converter a data, entre com a data no seguinte formato: yyyy/mm/dd')
            quit()

        if argumento == 'reset':
          reset = True
          
        if argumento == 'atualizar':
          atualizar = True

        if argumento == 'id_nova_rodada':
          id_nova_rodada = sys.argv[i+1]
          
        if argumento == 'id_dataviz_chuva':
          id_dataviz_chuva = sys.argv[i+1]
       
      if sys.argv[1].lower() == 'atualizar_cache_acomph':
        atualizar_cache_acomph(data, reset= reset)

      elif sys.argv[1].lower() == 'atualizar_cache_rdh':
        atualizar_cache_rdh()
        
      elif sys.argv[1].lower() == 'get_resultado_chuva':
          get_cached('get_resultado_chuva',*sys.argv[2:-1], atualizar=atualizar)

    else:
        printHelper()


if __name__ == '__main__':
  runWithParams()
  