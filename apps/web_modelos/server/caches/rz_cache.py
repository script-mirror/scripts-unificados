import os
import sys
import pdb
import logging
import datetime
import pandas as pd
import requests as req
from typing import Callable
from dotenv import load_dotenv
sys.path.insert(1,"/WX2TB/Documentos/fontes")
from PMO.scripts_unificados.apps.rodadas import rz_rodadasModelos
from PMO.scripts_unificados.apps.web_modelos.server.server import cache
from PMO.scripts_unificados.apps.web_modelos.server.libs import rz_ena, rz_chuva,rz_dbLib,db_decks,db_meteorologia,db_ons

load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")),'.env'))

logging.basicConfig(level=logging.INFO,
                    format='%(levelname)s:\t%(asctime)s\t %(name)s.py:%(lineno)d\t %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers=[
                        logging.StreamHandler()
                    ])
logger = logging.getLogger(__name__)



URL_COGNITO = os.getenv('URL_COGNITO')
CONFIG_COGNITO = os.getenv('CONFIG_COGNITO')

def get_access_token() -> str:
    response = req.post(
        URL_COGNITO,
        data=CONFIG_COGNITO,

        headers={'Content-Type': 'application/x-www-form-urlencoded'}
    )
    return response.json()['access_token']

def cache_rodadas_modelos(prefixo,rodadas,reset=False, dias=7):

  granularidade = rodadas['granularidade']
  dt_rodada = rodadas['dt_rodada']
  search_list = list(rodadas['rodadas'].keys())

  key = f'{prefixo}:{granularidade}:{dt_rodada}'

  ids_in_cache = []
  send_cache_data = []

  cache_values_dt_rodada  = cache.get(key)
  if not cache_values_dt_rodada:
    cache_values_dt_rodada=[]

  else:
    for item in cache_values_dt_rodada:
      
      ids_in_cache += item['id_rodada'],

      if item['id_rodada'] in search_list:
        send_cache_data += item,

  ids_faltantes_no_cache = list(set(search_list) - set(ids_in_cache))

  if ids_faltantes_no_cache or reset:

    if reset:
      ids_faltantes_no_cache = search_list
      print("Resetando Cache. ", ids_faltantes_no_cache)
    else:
      print("Ids Faltantes no Cache. ", ids_faltantes_no_cache)

    if prefixo == 'PREVISAO_ENA':
      ids_to_search = []
      for id_faltante in ids_faltantes_no_cache:

        modelo = rodadas['rodadas'][id_faltante]
        chave = id_faltante
        ids_to_search += (chave,modelo),

      resultado = rz_ena.get_ena_modelos_smap(ids_to_search,dt_rodada,granularidade, dias=dias)
    
    elif prefixo == 'PREVISAO_CHUVA':
      resultado = rz_chuva.get_chuva_smap_ponderada(ids_faltantes_no_cache,dt_rodada,granularidade)
      
    if not resultado:
      return send_cache_data
    
    send_cache_data += resultado
    cache_values_dt_rodada += resultado
    
    cache.set(key,cache_values_dt_rodada, timeout=60*60*24*7)

  return send_cache_data


def atualizar_cache_rodada_modelos(dt_rodada,reset=False): 
  print(dt_rodada)
  rodadas = rz_rodadasModelos.Rodadas(dt_rodada = dt_rodada)
  for granularidade in ['submercado','bacia']:
    params = rodadas.build_modelo_info_dict(granularidade = granularidade, build_all_models=True)
    cache_data_ena = cache_rodadas_modelos("PREVISAO_ENA",params,reset)
    print(f"Cache ENA {granularidade}, ({dt_rodada}) atualizado!")
    cache_data_chuva = cache_rodadas_modelos("PREVISAO_CHUVA",params,reset)
    print(f"Cache CHUVA {granularidade} ({dt_rodada}), atualizado!")


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
  

  print("CACHE ACOMPH ATUALIZADO!")
  
  
def import_ena_visualization_api(dt_rodada, id_nova_rodada:str, id_dataviz_chuva:str): 
  print(dt_rodada)
  rodadas = rz_rodadasModelos.Rodadas(dt_rodada = dt_rodada)
  params = rodadas.build_modelo_info_dict(granularidade = "submercado", build_all_models=True)
  print(params)
  print(id_nova_rodada)
  print(params['rodadas'][id_nova_rodada])
  
  params['rodadas'] = [{x:params['rodadas'][x]} for x in params['rodadas'] if str(x) == id_nova_rodada][0]

  for id_rodada in params['rodadas']:
    print(id_rodada)
    info_rodada = req.get(f"https://tradingenergiarz.com/api/v2/rodadas/por-id/{id_rodada}", 
                          headers={
                'Authorization': f'Bearer {get_access_token()}'
            }).json()
    
    viez = 'remvies' not in info_rodada['str_modelo'].lower()

    if 'ons' in info_rodada['str_modelo'].lower(): grupo = 'ons'
    elif 'rz' in info_rodada['str_modelo'].lower(): grupo = 'rz'
    else: grupo = 'rz'

    if info_rodada['fl_preliminar']: prioridade = 'preliminar'
    elif info_rodada['fl_pdp']: prioridade = 'pdp'
    elif info_rodada['fl_psat']: prioridade = 'psat'
    else: prioridade = None

    modelos = ['ECMWF-AIFS','ECMWF-ENS','ECMWF-EST','ECMWF','ETA','GEFS-EST','GEFS','GFS','PCONJUNTO2','PCONJUNTO','PMEDIA']
    modelo:str
    for modelo_base in modelos:
        if modelo_base in info_rodada['str_modelo'].upper():
            modelo = modelo_base
            break

    payload = {
            "dataRodada": f"{info_rodada['dt_rodada']}T00:00:00.000Z",
            "dataFinal":  None,
            "mapType": 'ena',
            "idType": f"{info_rodada['id']}",
            "modelo": modelo,
            "grupo": grupo,
            "rodada": str(int(info_rodada['hr_rodada'])),
            "viez": viez,
            "membro": "0",
            "propagationBase": "VNA",
            "priority": prioridade,
            "measuringUnit": "MWm",
            "generationProcess": "SMAP",
            "data": [],
            "relatedMaps": [
              {
                "type": "chuva",
                "mapId": id_dataviz_chuva,
              }
            ]
        }
    

    for granularidade in ['submercado','bacia']:
      params['granularidade'] = granularidade
      ena = cache_rodadas_modelos("PREVISAO_ENA",params,False)[0]['valores']
      df = pd.DataFrame(ena).reset_index().rename(columns={'index': 'dt_prevista'})
      df = df.astype({'dt_prevista': 'datetime64[ns]'})
      valores_mapa = []
      
      for i, row in df.iterrows():
          for row_index in range(1, len(row.index)):
              valores_mapa.append({
                  "valor": row.values[row_index],
                  "dataReferente": f"{row['dt_prevista']}.000Z".replace(' ', 'T'),
                  "valorAgrupamento": row.index[row_index]
              })
      payload['data'].append({'valoresMapa': valores_mapa, 'agrupamento': granularidade})

    payload['dataFinal'] = f"{df['dt_prevista'].max()}.000Z".replace(' ', 'T')
    res = req.post('https://tradingenergiarz.com/backend/api/map', json=payload, headers={'Content-Type': 'application/json', "Authorization":f"Bearer {get_access_token()}"})

    if res.status_code == 201:
      logger.info(f"Rodada id {id_rodada} inserido na API de visualizacao")
    else:
      logger.warning(f"Erro ao tentar inserir rodada {id_rodada} na API de visualizacao")
    
    # cache_data_chuva = cache_rodadas_modelos("PREVISAO_CHUVA",params,reset)
    # print(f"Cache CHUVA {granularidade} ({dt_rodada}), atualizado!")

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


def cache_comparativo_carga_newave(dt_referente: str,atualizar:bool = False):
  
  key = f'NEWAVE:{dt_referente}'

  if not atualizar:
    cached = cache.get(key)
    if cached:
      return cached
    df_carga_newave_Sistema_cAdic = rz_dbLib.comparativo_carga_newave(dt_referente)
    
    cache.set(key, df_carga_newave_Sistema_cAdic, timeout=60*60*24*7)
    return df_carga_newave_Sistema_cAdic
  else:
    df_carga_newave_Sistema_cAdic = rz_dbLib.comparativo_carga_newave(dt_referente)
    cache.set(key, df_carga_newave_Sistema_cAdic, timeout=60*60*24*7)
    return df_carga_newave_Sistema_cAdic
  
  
def atualizar_cache_comparativo_carga_newave(dt_referente: str,reset=False): 

  print('Atualizando cache de Carga Newave')
  
  cache_comparativo_carga_newave(dt_referente=dt_referente, atualizar=True)
  
  print('CACHE CARGA NEWAVE ATUALIZADO')

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


def cache_previsao_modelos(key:str, dts_rodada_list:list,granularidade:str='submercado',atualizar=False):

    cached_values =  get_cached_values(key.lower())
        
    df_cached_values = pd.DataFrame(cached_values)

    dts_faltantes = dts_rodada_list
    if not df_cached_values.empty:

        df_target = df_cached_values[df_cached_values['dt_rodada'].isin(dts_rodada_list)]
        dts_faltantes = set(dts_rodada_list) - set(df_target['dt_rodada'].unique())
        
        if not dts_faltantes and not atualizar:
            
            return df_target.to_dict('records')
        
    if 'previsao_ena' in key.lower() :
        previsao_values = rz_ena.get_previsao_ena_smap(dts_faltantes,granularidade=granularidade,priority=False)

    elif 'previsao_chuva' in key.lower():
        previsao_values = rz_ena.get_previsao_chuva(dts_faltantes,granularidade=granularidade)

    df_values_request = pd.DataFrame(previsao_values)
    ids_rodadas_request = df_values_request['id_rodada'].unique()

    df_to_append = df_cached_values[~df_cached_values['id_rodada'].isin(ids_rodadas_request)] if not df_cached_values.empty else df_cached_values
    df_to_cache = pd.concat([df_to_append,df_values_request])
    cached_values = df_to_cache.to_dict('records')

    set_cache_values(key,cached_values)

    return df_to_cache[df_to_cache['dt_rodada'].isin(dts_rodada_list)].to_dict('records')


  
  
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
            for col in df.columns[1:]:  # Skip dataReferente column
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
    

def printHelper():

  hoje = datetime.datetime.now()
  modelo = 'pconjunto'
  horaRodada = '00'
  atualizar = 'atualizar'

  print("python {} atualizar_cache_acomph data {}".format(
              sys.argv[0], hoje.strftime("%Y-%m-%d")
              ))

  print("python {} atualizar_cache_rodada_modelos dt_rodada {}".format(
              sys.argv[0], 
              hoje.strftime("%Y-%m-%d"),
              ))
  print("python {} import_ena_visualization_api dt_rodada {}".format(
              sys.argv[0], 
              hoje.strftime("%Y-%m-%d"),
              ))
  
  print("python {} atualizar_cache_comparativo_carga_newave data {}".format(
              sys.argv[0], 
              hoje.strftime("%Y-%m-%d")
              ))
  
  print("python {} get_resultado_chuva {} {} {} atualizar".format(
            sys.argv[0], 
            hoje.strftime("%Y-%m-%d"),
            modelo,
            horaRodada,
            atualizar
            ))

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

      #funções
      if sys.argv[1].lower() == 'atualizar_cache_rodada_modelos':
        atualizar_cache_rodada_modelos(data.strftime("%Y-%m-%d"),reset= reset)
        
      if sys.argv[1].lower() == 'import_ena_visualization_api':
        import_ena_visualization_api(data.strftime("%Y-%m-%d"), id_nova_rodada, id_dataviz_chuva)
        
      elif sys.argv[1].lower() == 'atualizar_cache_acomph':
        atualizar_cache_acomph(data, reset= reset)

      elif sys.argv[1].lower() == 'atualizar_cache_rdh':
        atualizar_cache_rdh()
        
      elif sys.argv[1].lower() == 'atualizar_cache_comparativo_carga_newave':
        atualizar_cache_comparativo_carga_newave(data.strftime('%Y-%m-%d'))
        
      elif sys.argv[1].lower() == 'get_resultado_chuva':
          get_cached('get_resultado_chuva',*sys.argv[2:-1], atualizar=atualizar)

    else:
        printHelper()


if __name__ == '__main__':
  # rodadas = rz_rodadasModelos.Rodadas(dt_rodada = dt_rodada)
  # rodadas.build_modelo_info_dict(granularidade = 'submercado', build_all_models=True)

  # teste = cache_rodadas_modelos('PREVISAO_ENA', rodadas)
  # import_acomph_visualization_api(datetime.date.today())
  runWithParams()
  