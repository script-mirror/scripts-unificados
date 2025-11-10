# -*- coding: utf-8 -*-
import gc
import os
import sys
import pandas as pd

from flask import render_template, request, redirect, url_for, jsonify
from werkzeug.security import generate_password_hash, check_password_hash
from flask_login import login_user, login_required, logout_user, current_user
import datetime
import requests
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")),'.env'))
sys.path.insert(1,"/WX2TB/Documentos/fontes/PMO/scripts_unificados/")
from bibliotecas import wx_opweek,wx_dbLib
from apps.web_modelos.server import utils
from apps.web_modelos.server.models import RegistrationForm, LoginForm, User
from apps.web_modelos.server.server import app, db_login, bp
from apps.web_modelos.server.caches import rz_cache
from apps.web_modelos.server.libs import rz_dbLib,rz_temperatura,rz_ena,db_decks,db_meteorologia
from apps.web_modelos.server.controller.ons_dados_abertos_controller import *
from apps.web_modelos.server.controller.bbce_controller import *

import urllib.parse
from middle.message import send_whatsapp_message
from middle.utils import Constants
constants = Constants()

pathBancoProspec = os.path.abspath('/WX2TB/Documentos/fontes/PMO/scripts_unificados/arquivos/db_rodadas_prospec.db')

URL_COGNITO = os.getenv('URL_COGNITO')
CONFIG_COGNITO = os.getenv('CONFIG_COGNITO')
URL_HTML_TO_IMAGE = os.getenv('URL_HTML_TO_IMAGE')

def get_access_token() -> str:
    response = requests.post(
        URL_COGNITO,
        data=CONFIG_COGNITO,
        headers={'Content-Type': 'application/x-www-form-urlencoded'}
    )
    return response.json()['access_token']
  
def post_event_trackin(path, query_string):
  if "static" in path:
    return None
  user_id = f"{current_user.username}|{current_user.email}" if current_user.is_authenticated else None
  res = requests.post(
    f"{constants.BASE_URL}/backend/api/events",
    json={
      "eventType": "http request",
      "systemOrigin": "/middle",
      "payload": {"path":path, "query":urllib.parse.unquote(query_string)},
      "value": 0,
      "userId": user_id
    },
        headers={
            "Authorization": f"Bearer {get_access_token()}"
        }
  )
  return res
def send_message(mensagem):
    dest:str="Debug"
    fields = {
        "destinatario": dest,
        "mensagem": mensagem
    }

    res = requests.post(
        f"{constants.BASE_URL}/bot-whatsapp/whatsapp-message",
        data=fields,
        files=None,
        headers={
            "Authorization": f"Bearer {get_access_token()}"
        }
    )
    print(f"Whatsapp status code: {res.status_code}")

@app.before_request
def add_prefix():
    path = request.path
    query_string = request.query_string.decode()
    res = post_event_trackin(path, query_string)
    if not path.startswith('/middle') and not path.startswith('/static'):
        new_path = '/middle' + path
        if query_string:
            new_path += '?' + query_string
        return redirect(new_path)


@bp.route('/login', methods=['GET', 'POST'])
def login():
  form = LoginForm()
  # flag=1 -> Usuário não encontrado | flag=2 -> Senha errada
  flag = 0
  if form.validate_on_submit():
    user = User.query.filter_by(username=form.username.data).first()
    if user:
      if check_password_hash(user.password, form.password.data):
        login_user(user, remember=form.remember.data)
        return redirect(url_for('middle_app.index'))
      else:
        flag = 1
    else:
      flag = 2
  return render_template('login.html', form=form, flag=flag)


@bp.route('/signup', methods=['GET', 'POST'])
def signup():
  form = RegistrationForm()
  if form.validate_on_submit():
    hashed_password = generate_password_hash(form.password.data, method='pbkdf2:sha256')
    new_user = User(username=form.username.data, email=form.email.data, password=hashed_password)
    if '@raizen.com' in form.email.data:
      send_whatsapp_message("debug", f"usuario: {form.username.data}\nemail: {form.email.data}", None)
      db_login.session.add(new_user)
      db_login.session.commit()
    else:
       send_whatsapp_message("debug", f"Tentativa de cadastro não aceita\nusuario: {form.username.data}\nemail: {form.email.data}", None)
    return redirect(url_for('middle_app.index'))
  return render_template('signup.html', form=form )


@bp.route('/logout')
@login_required
def logout():
  logout_user()
  return redirect(url_for('middle_app.login'))


@bp.route('/')
@login_required
def index():
  return render_template('menu.html')


@bp.route('/menu')
@login_required
def modelos():
  return render_template('menu.html')


@bp.route('/carga-dados-abertos')
@login_required
def carga_dados_abertos():
  return render_template('carga_dados_abertos/index.html')


@bp.route('/mapa-interativo')
@login_required
def mapa_interativo():
  return render_template('mapa_interativo.html')

@bp.route('/mapa-interativo-tv')
@login_required
def mapa_interativo_tv():
  return render_template('mapa_interativo_tv.html')


@bp.route('/geracao_eolica')
@login_required
def geracao_eolica():
  return render_template('geracao_eolica.html')


@bp.route('/estudos-dessem')
@login_required
def estudos_dessem():
  return render_template('estudos_dessem/estudos_dessem.html')

@bp.route('/curvas-anuais-armazenamento')
@login_required
def compara_armazenamentos():
  return render_template('curvas_anuais_armazenamento.html')

@bp.route('/curvas-anuais-acomph')
@login_required
def compara_acomph():
  return render_template('curvas_anuais_acomph.html')

@bp.route('/resumo-negociacoes-bbce')
@login_required
def resumo_negociacoes_bbce():
  return render_template('resumo_negociacoes_bbce.html')


@bp.route('/negociacoes-bbce')
@login_required
def negociacoes_bbce():
  return render_template('negociacoesBBCE.html')


@bp.route('/rdh')
@login_required
def rdh_values():
  return render_template('rdh/curvas_anuais_rdh.html')


@bp.route('/carga')
@login_required
def carga_subsistema():
  return render_template('carga_subsistema.html')

@bp.route('/rdh_submercados')
@login_required
def rdhSubmercados():
  return render_template('rdh_submercados.html')

@bp.route('/dash_historico_decomp')
@login_required
def dash_hist_decomp():
  return render_template('dashDecomp.html')

@bp.route('/ena-mlt',methods=['GET'])
@login_required
def ena_mlt():
  return render_template('ena_mlt.html')



@bp.route('/mapas-auxiliares')
@login_required
def mapas_auxiliares():
  return render_template('mapas_auxiliares.html')



@bp.route('API/get/estacoes-meteorologicas', methods=['GET'])
def API_get_lista_estacoes():
  return db_meteorologia.tb_inmet_estacoes.listar_estacoes(), 200

@bp.route('API/get/estacoes-meteorologicas/bacias', methods=['GET'])
def API_get_lista_bacias_estacoes_meteorologicas():
  return db_meteorologia.tb_inmet_estacoes.listar_bacias(), 200

@bp.route('API/get/estacoes-meteorologicas/chuva-acumulada', methods=['GET'])
def API_get_chuva_acumulada_por_estacao_data_entre():
  no_cache = request.args.get('noCache', default=False)
  atualizar = request.args.get('atualizar', default=False, type=utils.str_to_bool)
  dt_inicial = request.args.get('dtInicial', type=utils.formatar_data)
  dt_final = request.args.get('dtFinal', type=utils.formatar_data)
  
  if no_cache:
    return db_meteorologia.tb_inmet_dados_estacoes.chuva_acumulada_por_estacao_data_entre(dt_inicial, dt_final), 200
  return rz_cache.get_cached(db_meteorologia.tb_inmet_dados_estacoes.chuva_acumulada_por_estacao_data_entre, dt_inicial, dt_final, atualizar=atualizar), 200


@bp.route('API/get/estacoes-meteorologicas/datas-coleta', methods=['GET'])
def API_get_listar_datas_coleta():
  no_cache = request.args.get('noCache', default=True)
  atualizar = request.args.get('atualizar', default=False, type=utils.str_to_bool)

  if no_cache:
    return db_meteorologia.tb_inmet_dados_estacoes.listar_datas_coleta(), 200
  return rz_cache.get_cached(db_meteorologia.tb_inmet_dados_estacoes.listar_datas_coleta, timeout=60*30, atualizar=atualizar), 200


@bp.route('API/get/estacoes-meteorologicas/chuva-acumulada/media', methods=['GET'])
def API_get_media_chuva_acumulada_por_bacia_data_entre():
  no_cache = request.args.get('noCache', default=False)
  atualizar = request.args.get('atualizar', default=False, type=utils.str_to_bool)
  dt_inicial = request.args.get('dtInicial', type=utils.formatar_data)
  dt_final = request.args.get('dtFinal', type=utils.formatar_data)
  if no_cache:
    return db_meteorologia.tb_inmet_dados_estacoes.media_chuva_acumulada_por_bacia_data_entre(dt_inicial, dt_final), 200
  return rz_cache.get_cached(db_meteorologia.tb_inmet_dados_estacoes.media_chuva_acumulada_por_bacia_data_entre, dt_inicial, dt_final, atualizar=atualizar), 200





@bp.route('/API/get/ena-mlt/<granularidade>', methods=['GET'])
@login_required
def API_get_ena_mlt(granularidade):
  response = {}
  data_inicial = request.args.get('data_inicial')
  data_final = request.args.get('data_final')

  if data_inicial and data_final:
    data_inicial = datetime.datetime.strptime(data_inicial, '%d/%m/%Y')
    data_final = datetime.datetime.strptime(data_final, '%d/%m/%Y')

    if granularidade == 'submercado':
      ena_mlt = rz_dbLib.get_mlt_submercado(data_inicial, data_final)

    elif granularidade == 'bacia':
      ena_mlt = rz_dbLib.get_mlt_bacia(data_inicial, data_final)

    else:
      return f'granularidade {granularidade} nao encontrada!', 404
    
  else:
    if granularidade == 'submercado':
      ena_mlt = rz_dbLib.get_mlt_submercado()

    elif granularidade == 'bacia':
      ena_mlt = rz_dbLib.get_mlt_bacia()

    else:
      return f'granularidade {granularidade} nao encontrada!', 404
  
  ena_mlt = ena_mlt.groupby([granularidade]).apply(lambda x: dict(zip(x['dt_ref'], x['mlt'])))
  response = ena_mlt.to_dict()

  return jsonify(response), 200



@bp.route('/API/get/mlt-eolica', methods=['GET'])
@login_required
def API_get_mlt_eolica():
    
    #os dados retornam desde o inicio da semana da dt_ini, até o final da semana da dt_fim 
    dt_ini = request.args.get('dtInicial')
    num_semanas = int(request.args.get('numSemanas')) if request.args.get('numSemanas') else None 
    data_type = request.args.get('dataType')

    dt_ini_date = datetime.datetime.strptime(dt_ini,"%Y-%m-%d") 

    mlt_eol_values = rz_dbLib.get_eol_mlt_nw(dt_ini_date,num_semanas,data_type=data_type)
    return jsonify(mlt_eol_values)




@bp.route('/API/get/geracao_eolica_verificada')
@login_required
def API_verificada_geracao_eolica():
  
  dt_ini_semana = request.args.get('dtInicial', default=None, type=utils.formatar_data)
  dt_fim_semana = request.args.get('dtFinal', default=None, type=utils.formatar_data)
  data_referente = request.args.get('data_referente', default=None, type=utils.formatar_data)

  if data_referente != None:

    dt_ini_semana = wx_opweek.getLastSaturday(data_referente)
    dt_fim_semana = dt_ini_semana + datetime.timedelta(days=7)

  verificada = rz_dbLib.get_geracao_eolica_verificada(dt_ini_semana, dt_fim_semana)
  df_verificada = pd.DataFrame(verificada, columns=['submercado', 'dt_referente', 'vl_carga_verificada'])
  # df_verificada = df_verificada[df_verificada['submercado'].isin(['NORDESTE','SUL'])]

  resposta = {}
  for submercado in ['NORDESTE','SUL']:
    
    resposta[submercado] = {} 

    df_metrics = df_verificada[df_verificada['submercado'] == submercado]
    df_metrics = df_metrics.set_index('dt_referente')['vl_carga_verificada']
    media_horaria = df_metrics.resample('1h').mean().ffill().round(2)
    media_diaria = df_metrics.resample('24h').mean().ffill().round(2)
    media_semanal = df_metrics.resample('168h').mean().ffill().round(2)

    media_horaria.index = media_horaria.index.strftime('%Y/%m/%d %H:%M:%S')
    media_diaria.index = media_diaria.index.strftime('%Y/%m/%d %H:%M:%S')
    media_semanal.index = media_semanal.index.strftime('%Y/%m/%d %H:%M:%S')

    resposta[submercado]['horaria'] =  media_horaria.to_dict()
    resposta[submercado]['diaria'] = media_diaria.to_dict()
    resposta[submercado]['semanal'] = media_semanal.to_dict()


  df_verificada['dt_referente'] = pd.to_datetime(df_verificada['dt_referente']).dt.strftime('%Y-%m-%d %H:%M')

  df_verificada_pivot = df_verificada.pivot(index='submercado', columns='dt_referente', values='vl_carga_verificada')

  values_verificada = df_verificada_pivot.to_dict('index')
  valores = {}
    # valores['verificado'] = values_verificada
  valores['media'] = resposta

  return jsonify(valores)


@bp.route('/API/get/geracao_eolica_prevista_ds')
@login_required
def API_prev_geracao_eolica_dessem():

  data_referente = request.args['data_referente']
  data_deck = request.args['data_deck']

  dt_referente = datetime.datetime.strptime(data_referente, '%Y-%m-%d')
  dt_deck = datetime.datetime.strptime(data_deck, '%Y-%m-%d')

  dt_ini_semana = wx_opweek.getLastSaturday(dt_referente)
  dt_fim_semana = dt_ini_semana + datetime.timedelta(days= 7)

  df_geracao_eol = rz_dbLib.get_deck_geracao_dessem_eolica(dt_ini_semana,dt_fim_semana,dt_deck)
  df_geracao_eol['dt_referente'] = pd.to_datetime(df_geracao_eol['dt_referente'])

  resposta = {}
  for submercado in ['NORDESTE','SUL']:
    
    resposta[submercado] = {} 

    df_metrics = df_geracao_eol[df_geracao_eol['submercado'] == submercado]
    df_metrics = df_metrics.set_index('dt_referente')['vl_geracao_eol']
    media_horaria = df_metrics.resample('1h').mean().ffill().round(2)
    media_diaria = df_metrics.resample('24h').mean().ffill().round(2)
    media_semanal = df_metrics.resample('168h').mean().ffill().round(2)

    media_horaria.index = media_horaria.index.strftime('%Y/%m/%d %H:%M:%S')
    media_diaria.index = media_diaria.index.strftime('%Y/%m/%d %H:%M:%S')
    media_semanal.index = media_semanal.index.strftime('%Y/%m/%d %H:%M:%S')
    

    resposta[submercado]['horaria'] =  media_horaria.to_dict()
    resposta[submercado]['diaria'] = media_diaria.to_dict()
    resposta[submercado]['semanal'] =  media_semanal.to_dict()

  df_geracao_eol['dt_referente'] = pd.to_datetime(df_geracao_eol['dt_referente']).dt.strftime('%Y-%m-%d %H:%M')
  df_geracao_pivot = df_geracao_eol.pivot(index='submercado', columns='dt_referente', values='vl_geracao_eol')
  values_ds = df_geracao_pivot.to_dict('index')

  valores = {}
  valores['prev'] = values_ds
  valores['media'] = resposta

  

  return jsonify(valores)




@bp.route('/API/get/demanda-geracao-intercambio', methods=['GET'])
@login_required
def API_get_demanda_geracao_intercambio_total_data_entre():
  dt_inicial = request.args.get('dtInicial', type=utils.formatar_data)
  dt_final = request.args.get('dtFinal', type=utils.formatar_data)
  diferenca =  dt_final.date() - dt_inicial.date()
  no_cache = request.args.get('no_cache')
  if no_cache:
    demanda_geracao_intercambio = db_decks.tb_balanco_dessem.demanda_geracao_intercambio_total_data_entre(dt_inicial, dt_final)
  else:
    demanda_geracao_intercambio = rz_cache.get_cached(db_decks.tb_balanco_dessem.demanda_geracao_intercambio_total_data_entre, dt_inicial, dt_final)  
  if not demanda_geracao_intercambio:
    return {}, 204
  return demanda_geracao_intercambio, 200

@bp.route('/API/get/demanda-geracao-intercambio/<subsistema>', methods=['GET'])
@login_required
def API_get_demanda_geracao_intercambio_por_subsistema_data_entre(subsistema:str):
  subsistema = utils.subsistema_para_sigla(subsistema)
  dt_inicial = request.args.get('dtInicial', type=utils.formatar_data)
  dt_final = request.args.get('dtFinal', type=utils.formatar_data)
  diferenca =  dt_final.date() - dt_inicial.date()
  no_cache = request.args.get('no_cache')
  if no_cache:
    demanda_geracao_intercambio = db_decks.tb_balanco_dessem.demanda_geracao_intercambio_por_subsistema_data_entre(subsistema, dt_inicial, dt_final)
  else:
    demanda_geracao_intercambio = rz_cache.get_cached(db_decks.tb_balanco_dessem.demanda_geracao_intercambio_por_subsistema_data_entre, subsistema, dt_inicial, dt_final)  
  if not demanda_geracao_intercambio:
    return {}, 204
  return demanda_geracao_intercambio, 200

@bp.route('/API/get/pld-cmo', methods=['GET'])
@login_required
def API_get_pld_cmo_data_entre():
  dt_inicial = request.args.get('dtInicial', type=utils.formatar_data)
  dt_final = request.args.get('dtFinal', type=utils.formatar_data)
  diferenca =  dt_final.date() - dt_inicial.date()
  no_cache = request.args.get('no_cache')
  if no_cache:
    pld_cmo = db_decks.tb_balanco_dessem.pld_cmo_data_entre(dt_inicial, dt_final)
  else:
    pld_cmo = rz_cache.get_cached(db_decks.tb_balanco_dessem.pld_cmo_data_entre, dt_inicial, dt_final)  
  if not pld_cmo:
    return {}, 204
  return pld_cmo, 200

@bp.route('/API/get/pld-cmo/<subsistema>', methods=['GET'])
@login_required
def API_get_pld_cmo_por_subsistema_data_entre(subsistema:str):
  subsistema = utils.subsistema_para_sigla(subsistema)
  dt_inicial = request.args.get('dtInicial', type=utils.formatar_data)
  dt_final = request.args.get('dtFinal', type=utils.formatar_data)
  diferenca =  dt_final.date() - dt_inicial.date()
  no_cache = request.args.get('no_cache')
  if no_cache:
    pld_cmo = db_decks.tb_balanco_dessem.pld_cmo_por_subsistema_data_entre(subsistema, dt_inicial, dt_final)
  else:
    pld_cmo = rz_cache.get_cached(db_decks.tb_balanco_dessem.pld_cmo_por_subsistema_data_entre, subsistema, dt_inicial, dt_final)  
  if not pld_cmo:
    return {}, 204
  return pld_cmo, 200

@bp.route('/API/get/geracao-termica', methods=['GET'])
@login_required 
def API_get_geracao_termica_data_entre():
  dt_inicial = request.args.get('dtInicial', type=utils.formatar_data)
  dt_final = request.args.get('dtFinal', type=utils.formatar_data)
  diferenca =  dt_final.date() - dt_inicial.date()
  no_cache = request.args.get('no_cache')
  if no_cache:
    geracao_termica = db_decks.tb_balanco_dessem.geracao_termica_data_entre(dt_inicial, dt_final)
  else:
    geracao_termica = rz_cache.get_cached(db_decks.tb_balanco_dessem.geracao_termica_data_entre, dt_inicial, dt_final)
  if not geracao_termica:
    return {}, 204
  return geracao_termica, 200

@bp.route('/API/get/nomes-re', methods=['GET'])
@login_required 
def API_get_nomes_re():
  return db_decks.tb_intercambio_dessem.get_nomes_re(), 200

@bp.route('/API/get/limites-intercambio', methods=['GET'])
@login_required 
def API_get_limites_intercambio_data_entre():
  dt_inicial = request.args.get('dtInicial', type=utils.formatar_data)
  dt_final = request.args.get('dtFinal', type=utils.formatar_data)
  diferenca =  dt_final.date() - dt_inicial.date()
  no_cache = request.args.get('no_cache')
  if no_cache:
    limites_intercambio = db_decks.tb_intercambio_dessem.limites_intercambio_data_entre(dt_inicial, dt_final)
  else:
    limites_intercambio = rz_cache.get_cached(db_decks.tb_intercambio_dessem.limites_intercambio_data_entre, dt_inicial, dt_final)  
  if not limites_intercambio:
    return {}, 204
  return limites_intercambio, 200

@bp.route('/API/get/limites-intercambio/<nome_re>', methods=['GET'])
@login_required 
def API_get_limites_intercambio_por_nome_re_data_entre(nome_re:str):
  nome_re = nome_re.replace('BARRA', '/')
  dt_inicial = request.args.get('dtInicial', type=utils.formatar_data)
  dt_final = request.args.get('dtFinal', type=utils.formatar_data)
  diferenca =  dt_final.date() - dt_inicial.date()
  no_cache = request.args.get('no_cache')
  limites_intercambio = {}
  limites_intercambio = db_decks.tb_intercambio_dessem.limites_intercambio_por_nome_re_data_entre(nome_re, dt_inicial, dt_final)
  if not limites_intercambio:
    return {}, 204
  return limites_intercambio, 200

@bp.route('/API/get/datas-atualizadas-dessem', methods=['GET'])
@login_required
def API_get_datas_atualizadas_dessem():
  datas = db_decks.tb_balanco_dessem.range_datas()
  if not datas:
    return {}, 204
  return datas, 200



@bp.route("/API/GET/earm_submercado",methods=['GET'])
@login_required
def get_earm_submercado():

  data_inicial = request.args['dataInicial']

  data_inicial = datetime.datetime.strptime(data_inicial, "%d/%m/%Y")
  values = rz_dbLib.get_Earm_submercado(data_inicial)

  return jsonify(values)



@bp.route('/API/getEnaAcomph')
# @cache.cached(timeout=60*60*24*7,query_string=True)
@login_required
def API_getEnaAcomph():
  
  dataInicial = request.args.get('dataInicial')
  dataFinal = request.args.get('dataFinal')
  granularidade = request.args.get('divisao')
  no_cache = request.args.get('flagNoCache')
  dataFinal = request.args.get('dataFinal')
  granularidade = request.args.get('divisao')
  no_cache = request.args.get('flagNoCache')
  
  
  if not dataInicial and not granularidade:
    return jsonify({"message": "Adicione os parametros dataInicial e dataFinal, no seguinte formato (%d/%m/%Y), e granularidade (submercado/bacia)"})
  
  dataInicial = datetime.datetime.strptime(dataInicial, "%d/%m/%Y")
  if dataFinal: dataFinal = datetime.datetime.strptime(dataFinal, "%d/%m/%Y")
  prefixo = "ACOMPH"

  if no_cache == '1':
    acomph_values = rz_ena.get_ena_acomph(dataInicial,granularidade)
    acomph_values = acomph_values.to_dict()
  else:
    acomph_values = rz_cache.cache_acomph(prefixo, granularidade, dataInicial, dataFinal)

  return jsonify(acomph_values)


@bp.route('/API/getmlt')
@login_required
def API_getMlt():
    answer = wx_dbLib.getMlt()
    submercados = {1:'NORTE', 2: 'NORDESTE', 3: 'SUDESTE', 4: 'SUL'}

    mlt = {}
    for row in answer:
        if submercados[row[0]] not in mlt:
            mlt[submercados[row[0]]] = {}

        mlt[submercados[row[0]]][row[1]] = row[2]
    return jsonify(mlt)



@bp.route("/API/get/rdh/<posto>", methods = ["GET"])
@login_required 
def API_rdh_earm_por_posto(posto:str):

  ano:str = request.args.get('ano', type=str)
  tipo:str = request.args.get('tipo', type=str)
  mes_inicio:int = request.args.get('mes_inicio', type=int, default=1)
  mes_fim:int = request.args.get('mes_fim', type=int, default=12)

  no_cache = request.args.get('no_cache')

  if no_cache:
    rdh = rz_dbLib.get_info_rdh_por_posto(ano, tipo, posto)
  else:
    rdh = rz_cache.get_cached(rz_dbLib.get_info_rdh_por_posto, ano, tipo, posto)
  
  if rdh:
    for posto_aux in rdh['valores'][tipo]:
      rdh['valores'][tipo][posto_aux] = utils.filtrar_dict_por_mes(rdh['valores'][tipo][posto_aux], mes_inicio, mes_fim)
    return rdh, 200
  return rdh, 204


@bp.route('/API/get/rdh/utils/anos', methods=['GET'])
@login_required
def API_get_rdh_datas():
  no_cache = request.args.get('no_cache')
  if no_cache:
    datas = rz_dbLib.get_anos_disponiveis_rdh()
  else:
    datas = rz_cache.get_cached(rz_dbLib.get_anos_disponiveis_rdh)
  if not datas:
    return {}, 204
  return datas, 200

@bp.route('/API/get/rdh/utils/postos', methods=['GET'])
@login_required
def API_get_rdh_postos():
  no_cache = request.args.get('no_cache')
  if no_cache:
    datas = rz_dbLib.get_postos_rdh()
  else:
    datas = rz_cache.get_cached(rz_dbLib.get_postos_rdh)
  if not datas:
    return {}, 204
  return datas, 200



@bp.route('/API/get/temperatura_obs')
@login_required
def API_temperatura_obs():

  valores_out = {}

  dt_ini_str = request.args['dt_ini']
  dt_fim_str = request.args['dt_fim']

  dt_ini = datetime.datetime.strptime(dt_ini_str, '%Y-%m-%d')
  dt_fim = datetime.datetime.strptime(dt_fim_str, '%Y-%m-%d')

  df_temperatura_obs = rz_temperatura.get_temperatura_obs(dt_ini,dt_fim)

  if not df_temperatura_obs.empty:

    submercados = df_temperatura_obs['str_submercado'].unique()
    valores_out['temperatura'] = {}
    for submercado in submercados:
        df_aux = df_temperatura_obs[df_temperatura_obs['str_submercado']==submercado]
        df_aux_pivot = df_aux.pivot(index='data', columns='horario', values='vl_temperatura')
        temperatura_values = df_aux_pivot.to_dict('index')

        valores_out['temperatura'][submercado] = temperatura_values

    return jsonify(valores_out)
  else:
    return jsonify({"mensagem":"0 valores encontrados"})


@bp.route('/API/get/temperatura_prev')
@login_required
def API_temperatura_prev():

  valores_out = {}
  valores_out['temperatura prev']={}

  dt_deck_str = request.args['dt_deck']

  dt_deck = datetime.datetime.strptime(dt_deck_str, '%Y-%m-%d')

  df_temperatura_prev = rz_temperatura.get_temperatura_prev(dt_deck)

  if df_temperatura_prev.empty:
    dt_deck_anterior = dt_deck-datetime.timedelta(days=1)
    df_temperatura_prev = rz_temperatura.get_temperatura_prev(dt_deck_anterior)
    valores_out['mensagem']='Para Previsão de temperatura iniciando no dia {} foram utilizados dados de previsão do dia {}'.format(dt_deck.strftime('%d/%m/%Y'),dt_deck_anterior.strftime('%d/%m/%Y'))
    if df_temperatura_prev.empty:
      return jsonify({"mensagem":"0 valores encontrados"})

  df_temperatura_prev = df_temperatura_prev[df_temperatura_prev['dt_referente'] >= dt_deck]
  submercados = df_temperatura_prev['str_submercado'].unique()
  valores_out['temperatura prev'][dt_deck_str] = {}
  for submercado in submercados:
      df_aux = df_temperatura_prev[df_temperatura_prev['str_submercado']==submercado]
      df_aux_pivot = df_aux.pivot(index='data', columns='horario', values='vl_temperatura')
      temperatura_values = df_aux_pivot.to_dict('index')

      valores_out['temperatura prev'][dt_deck_str][submercado] = temperatura_values


  return jsonify(valores_out)


@bp.route("/API/get/rdh", methods = ["GET"])
@login_required 
def API_rdh_earm():

  ano:str = request.args.get('ano', type=str)
  tipo:str = request.args.get('tipo', type=str)
  mes_inicio:int = request.args.get('mes_inicio', type=int, default=1)
  mes_fim:int = request.args.get('mes_fim', type=int, default=12)

  no_cache = request.args.get('no_cache')

  if no_cache:
    rdh = rz_dbLib.get_df_info_rdh(ano, tipo)
  else:
    rdh = rz_cache.get_cached(rz_dbLib.get_df_info_rdh, ano, tipo)
  
  if rdh:
    return rdh, 200
  return rdh, 204



@bp.route('/API/database/get/cargaHoraria')
@login_required
def API_getCargaHoraria():
  data = datetime.datetime.strptime(request.args['data'], '%Y-%m-%d')
  carga_db = wx_dbLib.getCargaHoraria(data)
  
  gc.collect()

  carga = {}
  for row in carga_db:
    if row[0] not in carga.keys():
      carga[row[0]] = {}

    try:
      horario = row[1].strftime('%H:%M')
      carga[row[0]][horario] = row[2]
    except Exception as e:
      carga[row[0]][row[1]] = row[2]

  return jsonify({'carga':carga, 'data':data.strftime('%Y-%m-%d')})


@bp.route('/API/database/get/geracaoHoraria')
@login_required
def API_getGeracaoHoraria():
  data = datetime.datetime.strptime(request.args['data'], '%Y-%m-%d')
  geracao_db = wx_dbLib.getGeracaoHoraria(data)

  geracao = {}
  geracao_total = {}
  for row in geracao_db:
    try:
      horario = row[1].strftime('%H:%M')
    except Exception as e:
      horario = row[1]

    if row[0] not in geracao.keys():
      geracao[row[0]] = {}

    if row[3] not in geracao[row[0]].keys():
      geracao[row[0]][row[3]] = {}

    geracao[row[0]][row[3]][horario] = row[2]

    # if row[0] != 'SIN':
    if row[0] not in geracao_total.keys():
      geracao_total[row[0]] = {}

    if horario not in geracao_total[row[0]].keys():
      geracao_total[row[0]][horario] = 0

    geracao_total[row[0]][horario] += float(row[2])

  for sub in geracao_total:
    geracao[sub]['Geração Total'] = geracao_total[sub]

  return jsonify({'geracao':geracao, 'data':data.strftime('%Y-%m-%d')})


@bp.route('/API/database/get/previsaoCargaDs_DP')
@login_required
def API_getPrevisaoCargaDs_DP():
  data = datetime.datetime.strptime(request.args['data'], '%Y-%m-%d')

  dict_master = {}
  dict_master['data'] = data.strftime('%Y-%m-%d')

  df_carga_bloco = rz_dbLib.get_df_prev_carga_ds_DP(data)

  if df_carga_bloco.empty:
    dict_master['mensagem'] = 'Não Há valores para a previsão de carga do bloco DP'
    return jsonify(dict_master)
  else:
    df_carga_bloco['horario'] = pd.to_datetime(df_carga_bloco['dt_referente']).dt.strftime('%d/%m/%Y')
    df_carga_bloco['hr'] = pd.to_datetime(df_carga_bloco['dt_referente']).dt.strftime('%H:%M')

    dict_master['carga'] = {}
    codigoSubmercados = {1:'SUDESTE', 2:'SUL', 3:'NORDESTE', 4:'NORTE'}
    valores = {}
    df_carga_sin = pd.DataFrame()
    for submercado in df_carga_bloco['cd_submercado'].unique():

      df_submerc = df_carga_bloco[df_carga_bloco['cd_submercado'] == submercado]
      df_values = df_submerc.pivot(index='horario', columns='hr', values='vl_carga')
      values = df_values.to_dict('index')
      valores[codigoSubmercados[submercado]] = values

      if df_carga_sin.empty:
        df_carga_sin = df_values
      else:
        df_carga_sin += df_values

    valores['SIN'] = df_carga_sin.to_dict('index')

    dict_master['carga']['Bloco_DP'] = valores

    return jsonify(dict_master)


@bp.route('/API/database/get/previsaoCargaDs')
@login_required
def API_getPrevisaoCargaDs():
  data = datetime.datetime.strptime(request.args['data'], '%Y-%m-%d')

  dict_master = {}
  dict_master['data'] = data.strftime('%Y-%m-%d')

  df_carga_PrevCarga = rz_dbLib.get_df_prev_carga_ds(data)

  if df_carga_PrevCarga.empty:
    dict_master['mensagem'] = 'Não Há valores para a previsão de carga do PrevCargaDessem'
    return jsonify(dict_master)
  else:
    df_carga_PrevCarga['horario'] = pd.to_datetime(df_carga_PrevCarga['dt_referente']).dt.strftime('%d/%m/%Y')
    df_carga_PrevCarga['hr'] = pd.to_datetime(df_carga_PrevCarga['dt_referente']).dt.strftime('%H:%M')

    dict_master['carga'] = {}
    codigoSubmercados = {1:'SUDESTE', 2:'SUL', 3:'NORDESTE', 4:'NORTE'}
    valores = {}
    df_carga_sin = pd.DataFrame()
    for submercado in df_carga_PrevCarga['cd_submercado'].unique():

      df_submerc = df_carga_PrevCarga[df_carga_PrevCarga['cd_submercado'] == submercado]
      df_submerc = df_submerc[:-1]
      df_values = df_submerc.pivot(index='horario', columns='hr', values='vl_carga')
      values = df_values.to_dict('index')
      valores[codigoSubmercados[submercado]] = values

      if df_carga_sin.empty:
        df_carga_sin = df_values
      else:
        df_carga_sin += df_values

    valores['SIN'] = df_carga_sin.to_dict('index')

    dict_master['carga']['PrevCarga'] = valores

    return jsonify(dict_master)

@bp.route('/API/get/prev_carga_liquida')
@login_required
def API_prev_carga_liquida():

  valores = {}
  submercados = {"N":'NORTE','S':'SUL','SE':'SUDESTE','NE':'NORDESTE','SIN':'SIN'}

  dt_referente = request.args['data_referente']
  fonte_carga = request.args['fonte_carga']

  dt_prev = datetime.datetime.strptime(dt_referente, '%Y-%m-%d')
  df_carga_liquida = rz_dbLib.prev_carga_liquida_ds(dt_prev, fonte_carga = fonte_carga)

  valores['carga liquida'] = {}

  for submercado in df_carga_liquida['str_subsistema'].unique():
    df_submerc = df_carga_liquida[df_carga_liquida['str_subsistema'] == submercado]

    df_values = df_submerc.pivot(index='horario', columns='hr', values='vl_carga_liquida')

    values = df_values.to_dict('index')
    
    valores['carga liquida'][submercados[submercado]] = values

  valores['fonte'] = fonte_carga
  
  return jsonify(valores)

@bp.route('/API/getlstrev')
@login_required
def API_getLstRev():
  ena, revisao = rz_dbLib.getLstRev()
  json = {}
  for sub in ena:
    if sub not in json:
      json[sub] = {}
    for i, data in enumerate(ena[sub]):
      json[sub][data.strftime("%Y/%m/%d")] = ena[sub][data]
  json['revisao'] = revisao
  return jsonify(json)


