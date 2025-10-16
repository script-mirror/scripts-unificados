# -*- coding: utf-8 -*-
import gc
import os
import sys
import pdb
import shutil
import zipfile
import sqlite3
import pandas as pd
from io import BytesIO
from flask import send_file

from flask import render_template, request, redirect, url_for, jsonify,send_file
from werkzeug.security import generate_password_hash, check_password_hash
from flask_login import login_user, login_required, logout_user, current_user
import datetime
import requests
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")),'.env'))
sys.path.insert(1,"/WX2TB/Documentos/fontes")
from PMO.scripts_unificados.bibliotecas import wx_dbClass,wx_opweek,wx_dbLib
from PMO.scripts_unificados.apps.previvaz.libs import wx_geradorPrevs
from PMO.scripts_unificados.apps.web_modelos.server import utils
from PMO.scripts_unificados.apps.web_modelos.server.models import RegistrationForm, LoginForm, User
from PMO.scripts_unificados.apps.web_modelos.server.server import app, db_login, bp,cache
from PMO.scripts_unificados.apps.web_modelos.server.caches import rz_cache
from PMO.scripts_unificados.apps.web_modelos.server.libs import rz_dbLib,rz_temperatura,rz_ena,rz_chuva,db_decks,db_meteorologia

from PMO.scripts_unificados.apps.web_modelos.server.controller.ons_dados_abertos_controller import *
from PMO.scripts_unificados.apps.web_modelos.server.controller.bbce_controller import *

import urllib.parse
import json
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
    send_whatsapp_message("debug", f"usuario: {form.username.data}\nemail: {form.email.data}", None)
    db_login.session.add(new_user)
    db_login.session.commit()
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

# @bp.route('/palavras-Aneel',methods=['GET'])
# @login_required
# def palavras_Aneel():
#   return render_template('pauta_Aneel.html')

# @bp.route('/precoPLD',methods=['GET'])
# @login_required
# def preco_PLD():
#   return render_template('precoPLD.html')

# @bp.route('/config-assuntos-Aneel',methods=['GET'])
# @login_required
# def config_assunto_Aneel():
#   return render_template('config_por_assuntoAneel.html')

# @bp.route('/config-processos-Aneel',methods=['GET'])
# @login_required
# def config_processo_Aneel():
#   return render_template('config_por_processoAneel.html')

# @bp.route('/config-produtos-bbce',methods=['GET'])
# @login_required
# def config_produtos_bbce():
#   return render_template('config_produtos_bbce.html')

# @bp.route('/menuConfiguration')
# @login_required
# def menu_config():
#   return render_template('menuConfiguration.html')

# @bp.route('/chuva-ena-diaria')
# @login_required
# def teste_config():
#   return render_template('chuva_ena_diaria.html')

# @bp.route('/acompanhamento_ipdo')
# @login_required
# def acompanhamento_ipdo():
#   return render_template('acompanhamento_ipdo.html')

# @bp.route('/acompanhamento_newave')
# @login_required
# def acompanhamento_newave():
#   return render_template('acompanhamento_newave.html')




# @bp.route('/curvas-carga-dessem-decomp-newave')
# @login_required
# def carga_dessem_decomp_newave():
#   return render_template('curvas_carga_dessem_decomp_newave.html')

# @bp.route('/calculadora_pld')
# def calculadora_pld():
#   return render_template('calculadora_pld.html')

# @bp.route('/comparativo_carga_newave')
# def comparativo_carga_newave():
#   return render_template('comparativo_carga_newave.html')




# @bp.route('/Path')
# @login_required
# def path_config():
#   return render_template('configPath.html')



# @bp.route('/geradorPrevs',methods=['GET'])
# @login_required
# def geradorPrevs():
#   return render_template('geradorPrevs.html')




# @bp.route('/vnc_kill_session',methods=['GET'])
# @login_required
# def vnc_kill_session():
#   return render_template('vnc_kill_session.html')










# def treatListRevisions(lstRv):
#   dicRev = {}
#   for rev in lstRv:
#     if rev[0] not in dicRev:
#       dicRev[rev[0]]={}
#     if rev[1] not in dicRev[rev[0]]:
#       dicRev[rev[0]][rev[1]]={}
#     if rev[2] not in dicRev[rev[0]][rev[1]]:
#       dicRev[rev[0]][rev[1]][rev[2]]=[]
#     if rev[4]:
#       if rev[4] not in dicRev[rev[0]][rev[1]][rev[2]]:
#         dicRev[rev[0]][rev[1]][rev[2]].append(rev[4])
#   return dicRev




# @bp.route('/acomph_comp')
# @login_required
# def acomphComparacao():
#   return render_template('acomph_submercados_comparacao.html')



# @bp.route('/ena_diaria_bacia_pluvia')
# @login_required
# def ena_diaria_bacia_pluvia():
#   return render_template('ena_diaria_bacia_pluvia.html')



# @bp.route('/ena_pluvia')
# @login_required
# def ena_pluvia():
#   return render_template('ena_pluvia.html')

# @bp.route('/monitoramento_precipitacao_bacia')
# @login_required
# def monitoramento_precipitacao_bacia():
#   return render_template('monitoramento_preciptacao_bacias.html')

# @bp.route('/diferencaEnaBacia')
# @login_required
# def diferenca_ena_bacia():
#   return render_template('diferenca_ena_submercado.html')


# @bp.route('/dashbbce')
# @login_required
# def dashBbce_old():
#   return render_template('dashBbce.html')

# @bp.route('/dash_bbce')
# @login_required
# def dashBbce():
#   return render_template('dashBbce.html')

# @bp.route('/dash_rdh')
# @login_required
# def dashRdh():
#   return render_template('dashRdh.html')

# @bp.route('/dash_dessemOficial')
# @login_required
# def dessemOficia():
#   return render_template('dashDessemOficial.html')

# @bp.route('/dash_historico_decomp')
# @login_required
# def dash_hist_decomp():
#   return render_template('dashDecomp.html')

# @bp.route('/dash_comparativo_carga')
# @login_required
# def dash_comp_carga():
#   return render_template('dash_comparativoCarga.html')
  

# @bp.route('/cadastroProdutosBbce')
# @login_required
# def cadastroProdutosBbce():
#   return render_template('BBCE_cadastroProdutosEmail.html')

# @bp.route('/prospec_preco')
# @login_required
# def prospecPreco():
#   return render_template('prospec_precos.html' )

# @bp.route('/precos_ec')
# @login_required
# def precos_ec():
#   return render_template('prospec_precos_ec.html' )

# @bp.route('/tabela_previsao_modelos')
# @login_required
# def tabela_previsao_modelos():
#   return render_template('tabela_previsao_modelos2.html' )


# @bp.route('/API/prospec/get-rodada-ec')
# @login_required
# def API_precos_ec():
    
#   dtRodada = request.args['data']

#   conn = sqlite3.connect(pathBancoProspec)
#   cursor = conn.cursor()

#   query = """
#       SELECT
#         tc.TX_MODELO,
#         tr.CD_SUBMERCADO,
#         tr.DT_REFERENTE,
#         tr.VL_CMO_MEDIO,
#         tr.VL_EA_INICIAL,
#         tr.VL_ENA,
#         tr.VL_ENA_PERCENT_MENSAL
#       FROM
#         TB_CADASTRO_RODADAS_PROSPEC tc
#       left join TB_RESULTADOS_RODADAS_PROSPEC tr on
#         tc.ID = tr.ID_CADASTRO
#       WHERE
#         DT_RODADA = '{}'
#         AND TX_MODELO LIKE 'ecmwf_ens_ext-%_pluvia'""".format(dtRodada)

#   cursor.execute(query)
#   rodadas = cursor.fetchall()
#   conn.close()
 
#   if rodadas == []:
#     return jsonify({'status': 'success', 'message': 'Nenhuma rodada encontrada'})
  
 
#   df_rodadas = pd.DataFrame(rodadas)
#   df_rodadas.columns = ['TX_MODELO', 'CD_SUBMERCADO', 'DT_REFERENTE', 'VL_CMO_MEDIO', 'VL_EA_INICIAL', 'VL_ENA', 'VL_ENA_PERCENT_MENSAL']
 
#   header = []
#   for dt in sorted(df_rodadas['DT_REFERENTE'].unique()):
#     dt_ = datetime.datetime.strptime(dt, '%Y-%m-%d')
#     dt_opw = wx_opweek.ElecData(dt_.date())
#     header.append('RV{}</br>{}'.format(dt_opw.atualRevisao, (dt_opw.primeiroDiaMes + datetime.timedelta(days=7)).strftime('%b/%y')))
 
#   df_rodadas['VL_CMO_MEDIO'] = df_rodadas['VL_CMO_MEDIO'].apply(lambda x: '{:.0f}'.format(x))
#   df_rodadas['VL_EA_INICIAL'] = df_rodadas['VL_EA_INICIAL'].apply(lambda x: '{:.0f}'.format(x))
#   df_rodadas['VL_ENA'] = df_rodadas['VL_ENA'].apply(lambda x: '{:.1f}'.format(x/1000))
#   df_rodadas['VL_ENA_PERCENT_MENSAL'] = df_rodadas['VL_ENA_PERCENT_MENSAL'].apply(lambda x: '{:.0f}'.format(x))
  
 
#   rodadas_agrupadas = df_rodadas.groupby(['TX_MODELO','DT_REFERENTE']).groups
#   rodadas_dict = {}
#   for membro, dt in sorted(rodadas_agrupadas.keys()):
#     if membro not in rodadas_dict:
#       # rodadas_dict[membro] = [ None ] * len(headers)
#       rodadas_dict[membro] = []

#     rod = df_rodadas.loc[rodadas_agrupadas[(membro,dt)]].sort_values('CD_SUBMERCADO')
#     vals_rodada = '-'.join(rod['VL_ENA_PERCENT_MENSAL']) + '</br>'
#     vals_rodada += '-'.join(rod['VL_EA_INICIAL']) + '</br>'
#     vals_rodada += '-'.join(rod['VL_ENA']) + '</br>'
#     vals_rodada += '-'.join(rod['VL_CMO_MEDIO'])
#     rodadas_dict[membro].append(vals_rodada)

#   return jsonify({'tabela':rodadas_dict, 'header':header})

# @bp.route('/mapas_ec')
# @login_required
# def mapas_ec():
#   return render_template('mapas_ec_ext.html')

# @bp.route('/teste')
# @login_required
# def test():
#   return render_template('mapas_ec_ext.html?data=2022-07-18')

# @bp.route('/teste2')
# @login_required
# def test2():
#   return render_template('teste.html')

# @bp.route('/testebbce')
# @login_required
# def testeBbce():
#   return render_template('produtosBbce.html')

# @bp.route('/modelos/clima/prec')
# @login_required
# def prevmodels():
#   return render_template('previsaoModelos.html')







# @bp.route('/gera-chuva')
# @login_required
# def gera_chuva():
#   return render_template('/gera_chuva/index.html')



# @bp.route('/API/getallrevs')
# @login_required
# def API_getallRevs():
#   ena = rz_dbLib.getAllRvs()
#   df_ena = pd.DataFrame(ena)
#   df_ena.columns = ['DT_INICIO_SEMANA', 'VL_ENA', 'CD_REVISAO', 'CD_SUBMERCADO']
#   submercados = {4:'NORTE', 3: 'NORDESTE', 1: 'SUDESTE', 2: 'SUL'}

#   df_ena['SUBMERCADO'] = df_ena['CD_SUBMERCADO'].map(submercados)

#   json = {}
#   for rv in df_ena['CD_REVISAO'].unique():
#     json[int(rv)] = {}
#     for sub in df_ena['SUBMERCADO'].unique():

#       df_byRevSub = df_ena[(df_ena['CD_REVISAO'] == rv) & (df_ena['SUBMERCADO'] == sub)]
#       df_byRevSub.set_index('DT_INICIO_SEMANA', inplace=True)

#       dtInicial = min(df_byRevSub.index)
#       dtFinal = max(df_byRevSub.index)
#       dtFinal += datetime.timedelta(days=7)

#       idx = pd.date_range(dtInicial, dtFinal)
#       df_byRevSub = df_byRevSub.sort_index().reindex(idx,  method='ffill')

#       json[rv][sub] = dict(zip(df_byRevSub.index.strftime("%Y/%m/%d"), df_byRevSub['VL_ENA']))


#   return jsonify(json)

# @bp.route('/API/getrevbacias')
# @login_required
# def API_getRevBacias():

#   dataInicial = datetime.datetime.strptime(request.args['dataInicial'], "%d/%m/%Y")
#   answer = rz_dbLib.getRevBacias(dataInicial)

#   rev = pd.DataFrame(answer, columns=['VL_ANO', 'VL_MES', 'CD_REVISAO', 'DT_INICIO_SEMANA', 'VL_ENA', 'STR_BACIA', 'VL_PERC_MLT'])
  
#   anoUltimaRev = rev['VL_ANO'].max()
#   mesUltimaRev = rev.loc[rev['VL_ANO'] == anoUltimaRev]['VL_MES'].max()

#   rev['MLT'] =  rev['VL_ENA']*100/rev['VL_PERC_MLT']

#   # rev['DT_INICIO_SEMANA'] = rev['DT_INICIO_SEMANA'].dt.strftime('%Y/%m/%d')
#   mlt = rev.pivot(index='DT_INICIO_SEMANA', columns='STR_BACIA', values='MLT')

#   idx = pd.date_range(mlt.index.min(), mlt.index.max()+ datetime.timedelta(days=7))
#   mlt = mlt.reindex(idx, method='ffill')
#   mlt.index = mlt.index.strftime('%Y/%m/%d')

#   ultimaRev = rev.loc[(rev['VL_MES']==mesUltimaRev) & (rev['VL_ANO']==anoUltimaRev)].copy()
#   numeroRev = int(ultimaRev['CD_REVISAO'].unique()[0])
#   ultimaRev = ultimaRev.pivot(index='DT_INICIO_SEMANA', columns='STR_BACIA', values='VL_ENA')
#   idx = pd.date_range(ultimaRev.index.min(), ultimaRev.index.max()+ datetime.timedelta(days=7))
#   ultimaRev = ultimaRev.reindex(idx, method='ffill')
#   ultimaRev.index = ultimaRev.index.strftime('%Y/%m/%d')

#   json = {'ena':ultimaRev.to_dict(), 'mlt':mlt.to_dict(), 'revisao':numeroRev}
#   return jsonify(json)



# @bp.route('/API/getchuvaObservada')
# @login_required
# def API_getChuvaObservada():

#   range_dias = request.args.get("range_dias")
#   granularidade = request.args.get('granularidade')
#   data_final = request.args.get("data_final", datetime.date.today() - datetime.timedelta(days=1))

#   if range_dias == None:
#     dataInicio = utils.formatar_data(request.args.get('data_inicial'))
#   else:
#     range_dias = int(range_dias)
#     dataInicio = datetime.datetime.today() - datetime.timedelta(days=range_dias)
    
#   if type(data_final) == str:
#    data_final = utils.formatar_data(data_final)

#   chuvaObservada = rz_chuva.getChuvaObservada(dataInicio,granularidade, data_final)
#   return jsonify(chuvaObservada)






# @bp.route('/API/prospec/getcadastrorodadas')
# @login_required
# def API_getCadastrosRodadasProspec():

#   nDias = int(request.args['historico'])
#   flagMatriz = int(request.args['flagMatriz'])
#   dataRodada = datetime.datetime.strptime(request.args['dataRodada'],'%Y-%m-%d').date()

#   conn = sqlite3.connect(pathBancoProspec)
#   cursor = conn.cursor()

#   query = """
#         SELECT
#           ID_PROSPEC,
#           DT_RODADA,
#           DT_PRIMEIRA_RV,
#           TX_COMENTARIO
#         FROM
#           TB_CADASTRO_RODADAS_PROSPEC
#         WHERE 
#           FL_MATRIX = ?"""

#   parametrosQuery = [flagMatriz]
#   if nDias > 0:
#     query += " AND DT_RODADA >= ? and DT_RODADA <= ?"
#     dtInicio = dataRodada - datetime.timedelta(days=nDias)
#     dtFim = dataRodada
#     parametrosQuery += [dtInicio,dtFim]
#   else:
#     query += " AND DT_RODADA = ?"
#     parametrosQuery.append(dataRodada)

#   query += """
#         GROUP BY
#           ID_PROSPEC,
#           DT_RODADA,
#           DT_PRIMEIRA_RV,
#           TX_COMENTARIO"""
  
#   cursor.execute(query,tuple(parametrosQuery))
#   rodadas = cursor.fetchall()
#   conn.close()

#   rodadas_formatadas = []
#   for rod in rodadas:
#     mes_rv = datetime.datetime.strptime(rod[2],'%Y-%m-%d') + datetime.timedelta(days=6)
#     rodadas_formatadas.append((rod[0], mes_rv.strftime('%m/%Y'), rod[1], rod[3]))

#   return jsonify(rodadas_formatadas)






# @bp.route('/API/get/geracao_eolica_observada_prevista_dc')
# @login_required
# def API_geracao_eolica_observada_prevista_decomp():
    
#     data_referente = request.args['data_referente']
#     dt_referente = datetime.datetime.strptime(data_referente, '%Y-%m-%d')
    
#     dt_inicial = wx_opweek.getLastSaturday(dt_referente)
#     dt_final = dt_inicial + datetime.timedelta(days=7)
        
#     verificada = rz_dbLib.get_geracao_eolica_verificada(dt_inicial, dt_final)
#     df_verificada = pd.DataFrame(verificada, columns=['submercado', 'dt_referente', 'vl_carga'])

#     discretizacao = rz_dbLib.get_discretizacao_tempo(dt_inicial,  dt_final)
#     df_discretizacao = pd.DataFrame(discretizacao, columns=['dt_inicio_patamar', 'vl_patamar'])
 
#     bloco_pq = rz_dbLib.get_deck_decomp_bloco_pq(dt_inicio_deck=dt_inicial)
#     colunas = ['vl_estagio', 'vl_patamar', 'cd_submercado', 'vl_geracao_eol']
#     df_bloco_pq = pd.DataFrame(bloco_pq, columns=colunas)
#     df_bloco_pq.set_index(['cd_submercado', 'vl_estagio'], inplace=True)
    
#     valor_estagio = 1
    
#     submercados = {'NORDESTE':3, 'NORTE':4, 'SUL':2}
#     resposta = {}
    
#     resposta['verificada'] = {}
#     resposta['verificada']['horaria'] = {}
#     resposta['verificada']['diaria'] = {}
    
#     resposta['dessem'] = {}
#     resposta['dessem']['horaria'] = {}
#     resposta['dessem']['diaria'] = {}
    
#     resposta['decomp'] = {}
#     resposta['decomp']['horaria'] = {}
#     resposta['decomp']['diaria'] = {}
#     resposta['decomp']['semanal'] = {}
    
#     resposta['verificada_dessem'] = {}
    
#     for sub in submercados:
#         id = submercados[sub]
#         ger_ver = df_verificada[df_verificada['submercado']==sub].sort_values('dt_referente')
#         ger_ver.set_index('dt_referente', inplace=True)
        
#         df_media_horaria_verif = ger_ver.resample('1h').mean()
#         df_media_horaria_verif.fillna(0, inplace=True)
#         df_media_diaria_verif = ger_ver.resample('24h').mean()
#         df_media_diaria_verif.fillna(0, inplace=True)
        
#         dict_ger_patamar = dict(zip(df_bloco_pq.loc[(id,valor_estagio)]['vl_patamar'], df_bloco_pq.loc[(id,valor_estagio)]['vl_geracao_eol']))
#         ger_dc = df_discretizacao.copy()
        
#         ger_dc['vl_patamar'].replace(dict_ger_patamar, inplace=True)
#         ger_dc.set_index('dt_inicio_patamar', inplace=True)
#         ger_dc.loc[dt_final, 'vl_patamar'] = ger_dc.iloc[-1]['vl_patamar']
        
#         media_horaria_dc = ger_dc.resample('1h').mean()
#         media_horaria_dc.fillna(method='ffill', inplace=True)
#         media_diaria_dc = ger_dc.resample('24h').mean()
#         media_diaria_dc.fillna(method='ffill', inplace=True)
#         media_semanal_dc = ger_dc.resample('168h').mean()
#         media_semanal_dc.fillna(method='ffill', inplace=True)
        
#         df_media_horaria_verif.columns = ['vl_geracao_uee']

#         df_media_horaria_verif.index = df_media_horaria_verif.index.strftime('%Y/%m/%d %H:%M:%S')
#         df_media_diaria_verif.index = df_media_diaria_verif.index.strftime('%Y/%m/%d %H:%M:%S')
        
#         resposta['verificada']['horaria'][sub] = list(df_media_horaria_verif.itertuples( name=None))
#         resposta['verificada']['diaria'][sub] = list(df_media_diaria_verif.itertuples( name=None))

#         media_horaria_dc.index = media_horaria_dc.index.strftime('%Y/%m/%d %H:%M:%S')
#         media_diaria_dc.index = media_diaria_dc.index.strftime('%Y/%m/%d %H:%M:%S')
#         media_semanal_dc.index = media_semanal_dc.index.strftime('%Y/%m/%d %H:%M:%S')
        
#         resposta['decomp']['horaria'][sub] = list(media_horaria_dc.itertuples( name=None))
#         resposta['decomp']['diaria'][sub] = list(media_diaria_dc.itertuples( name=None))
#         resposta['decomp']['semanal'][sub] = list(media_semanal_dc.itertuples( name=None))
        
#     return jsonify(resposta)


# @bp.route('/API/get/geracao_eolica_prevista_dessem')
# @login_required
# def API_geracao_eolica_dessem():
    
#     submercados = {'NORDESTE':3, 'NORTE':4, 'SUL':2}
#     resposta = {}
#     resposta['horaria'] = {}
#     resposta['diaria'] = {}
#     resposta['semanal'] = {}
    
#     data_referente = request.args['data_referente']
#     dt_referente = datetime.datetime.strptime(data_referente, '%Y-%m-%d')
    
#     dt_inicial = wx_opweek.getLastSaturday(dt_referente)
#     dt_final = dt_inicial + datetime.timedelta(days=7)
        
#     verificada = rz_dbLib.get_geracao_eolica_verificada(dt_inicial, dt_final)

#     df_verificada = pd.DataFrame(verificada, columns=['submercado', 'dt_referente', 'vl_carga'])
    
#     previsao = rz_dbLib.get_deck_dessem_renovaveis_eolica(dt_referente)
#     colunas = ['vl_periodo', 'id_submercado', 'vl_geracao_uhe', 'vl_geracao_ute',
#                'vl_geracao_cgh', 'vl_geracao_pch', 'vl_geracao_ufv', 'vl_geracao_uee']
#     df_previsao = pd.DataFrame(previsao, columns=colunas)
#     df_previsao['horario'] = dt_referente + pd.to_timedelta((df_previsao['vl_periodo']-1)*30, unit='m')
    
    
#     for sub in submercados:
#         id = submercados[sub]
        
#         ger_ver = df_verificada[df_verificada['submercado']==sub].sort_values('dt_referente')
#         ger_ver.set_index('dt_referente', inplace=True)
        
#         df_media_horaria_verif = ger_ver.resample('1h').mean()
        
#         ger_ds = df_previsao[df_previsao['id_submercado']==id][['vl_geracao_uee','horario']]
#         ger_ds.set_index('horario', inplace=True)
#         ger_ds.loc[dt_final, 'vl_geracao_uee'] = ger_ds.iloc[-1]['vl_geracao_uee']
        
#         df_media_horaria_ds = ger_ds.resample('1h').mean()
#         df_media_diaria_ds = ger_ds.resample('24h').mean()
        
#         df_media_horaria_verif.columns = ['vl_geracao_uee']
#         # idx_concatenar = ~df_media_horaria_ds.index.isin(df_media_horaria_verif.index)
#         idx_concatenar = ~df_media_horaria_verif.index.isin(df_media_horaria_ds.index)
        
#         verificada_dessem = pd.concat([df_media_horaria_verif[idx_concatenar], df_media_horaria_ds], verify_integrity=True)
        
#         df_media_semanal_verif_dessem = verificada_dessem.resample('168h').mean()
        
#         df_media_horaria_ds.index = df_media_horaria_ds.index.strftime('%Y/%m/%d %H:%M:%S')
#         df_media_diaria_ds.index = df_media_diaria_ds.index.strftime('%Y/%m/%d %H:%M:%S')
#         df_media_semanal_verif_dessem.index = df_media_semanal_verif_dessem.index.strftime('%Y/%m/%d %H:%M:%S')
        
#         resposta['horaria'][sub] = list(df_media_horaria_ds.itertuples( name=None))
#         resposta['diaria'][sub] = list(df_media_diaria_ds.itertuples( name=None))
#         resposta['semanal'][sub] = list(df_media_semanal_verif_dessem.itertuples( name=None))
    
#     return jsonify(resposta)



# @bp.route("/API/get/chuvas")
# @login_required
# def API_rodadas_chuvas():
#   dt_rodada = request.args['dt_rodada']
#   modelo = request.args['modelo']
#   hr =request.args['hr']
#   grupo = request.args['grupo']


#   dt_rodada = datetime.datetime.strptime(dt_rodada,"%Y%m%d")
#   data = rz_chuva.get_chuvas(dt_rodada, modelo, hr)
#   df = pd.DataFrame(data, columns=['dt_prevista','vl_chuva','dt_rodada','modelo','subbacia', 'submercado', 'bacia'])
#   df['dt_prevista']=df['dt_prevista'].astype(str)
#   df['dt_prevista'] = pd.to_datetime(df['dt_prevista']).dt.strftime("%y/%m/%d")


#   data = df.groupby(['dt_prevista',f'{grupo}']).mean().round(2)
#   data = data.reset_index(level=0)
#   dic = data.sort_index()
#   names = list(dic.index.unique())

#   grup = {}
#   for name in names:
#     values = dict(dic.loc[name].values)
#     dics = {name : values}
#     grup.update(dics)

#   return jsonify({dt_rodada.strftime("%d/%m/%Y"): grup})

# @bp.route("/API/get/resultado_chuvas")
# @login_required
# def API_resultado_rodadas_chuvas():
#   dt_rodada = request.args['dt_rodada']
#   modelo = request.args['modelo']
#   hr =request.args['hr']
#   grupo = request.args['grupo']
#   dt_rodada = datetime.datetime.strptime(dt_rodada,"%Y%m%d")
#   dt_rodada_formatada = dt_rodada.strftime('%Y-%m-%d')
#   no_cache = request.args.get('no_cache')
  
#   if no_cache:
  
#     data = rz_chuva.get_resultado_chuva(dt_rodada_formatada, modelo, hr)
  
#     if data:
#       return jsonify(data), 200
#     return jsonify(data), 204
  
#   else:
    
#     data = rz_cache.get_cached(rz_chuva.get_resultado_chuva, dt_rodada_formatada, modelo, hr)
#     if data:
#       return jsonify(data), 200
#     return {}, 204


# @bp.route("/API/get/palavras-Aneel",methods=['GET'])
# @login_required
# def get_palavras_aneel():

#     database = wx_dbClass.db_mysql_master('db_config')
    
#     database.connect()
#     palavras_aneel = database.getSchema('tb_palavras_pauta_aneel')

#     select = palavras_aneel.select()
#     palavras = database.conn.execute(select).fetchall()
#     palavras_ = []
#     for row in palavras:
#       palavras_.append({'id':row[0],'palavra':row[1],'tag':row[2]})
#     return jsonify(palavras_)

# @bp.route("/API/set/palavras-Aneel",methods=['POST'])
# @login_required
# def set_palavras_aneel():

#   database = wx_dbClass.db_mysql_master('db_config')	

#   database.connect()
#   palavras_aneel = database.getSchema('tb_palavras_pauta_aneel')


#   Palavras_list = request.get_json()
#   values=[]
#   for row in Palavras_list:
#     values.append((row['id'],row['palavra'],row['tag']))


#   delete = palavras_aneel.delete()
#   database.conn.execute(delete)

#   insert = palavras_aneel.insert(values)
#   database.conn.execute(insert)

#   return jsonify('Lista atualizada com sucesso!')

# @bp.route("/API/get/assuntos-Aneel",methods=['GET'])
# @login_required
# def get_assuntos_aneel():

#     database = wx_dbClass.db_mysql_master('db_config')
#     database.connect()
#     tb_assuntos_aneel = database.getSchema('tb_assuntos_aneel')

#     select = tb_assuntos_aneel.select()
#     assuntos = database.conn.execute(select).fetchall()

#     palavras_ = []
#     for row in assuntos:
#         palavras_.append({'id':row[0],'assunto':row[1]})
#     return jsonify(palavras_)

# @bp.route("/API/set/assuntos-Aneel",methods=['POST'])
# @login_required
# def set_assuntos_Aneel():

#     database = wx_dbClass.db_mysql_master('db_config')
#     database.connect()
#     tb_assuntos_aneel = database.getSchema('tb_assuntos_aneel')


#     assuntos_list = request.get_json()
#     values=[]
#     for row in assuntos_list:
#         values.append((row['id'],row['assunto']))

#     delete = tb_assuntos_aneel.delete()
#     database.conn.execute(delete)

#     insert = tb_assuntos_aneel.insert(values)
#     database.conn.execute(insert)

#     return jsonify('Lista atualizada com sucesso!')




# @bp.route("/API/get/processos-Aneel",methods=['GET'])
# @login_required
# def get_processos_Aneel():

#     database = wx_dbClass.db_mysql_master('db_config')
#     database.connect()
#     tb_produtos_bbce = database.getSchema('tb_processos_aneel')

#     select = tb_produtos_bbce.select()
#     values = database.conn.execute(select).fetchall()
#     processos = []
#     for row in values:
#         processos.append({'id':row[0],'processo':row[1],'efeito':row[2],'assunto':row[3],'relator':row[5],'superintendencia':row[6],'ultimo_documento':row[4]})
#     return jsonify(processos)

# @bp.route("/API/set/processos-Aneel",methods=['POST'])
# @login_required
# def set_processos_Aneel():

#     database = wx_dbClass.db_mysql_master('db_config')
#     database.connect()
#     tb_produtos_bbce = database.getSchema('tb_processos_aneel')
#     processos_list = request.get_json()
#     values=[]
#     for row in processos_list:
#         values.append((row["id"], row["processo"], row["efeito"],row["assunto"],row["ultimo_documento"],row["relator"],row["superintendencia"]))
#     delete = tb_produtos_bbce.delete()
#     database.conn.execute(delete)
#     insert = tb_produtos_bbce.insert(values)
#     database.conn.execute(insert)

#     return jsonify('Lista atualizada com sucesso!')
  
# @bp.route("/API/get/precoPLD",methods=['GET'])
# def get_preco_PLD():

#   database = wx_dbClass.db_mysql_master('db_ons')
  
#   database.connect()
#   preco_PLD = database.getSchema('tb_pld')

#   select = preco_PLD.select()
#   ano = database.conn.execute(select).fetchall()
#   ano_ = []
#   for row in ano:
#     ano_.append({'ano':row[0],'pldMaxHora':row[1],'pldMaxEstr':row[2],'pldMin':row[3],'custoDeficit':row[4]})
#   return jsonify(ano_)

# @bp.route("/API/set/precoPLD",methods=['POST'])
# def set_preco_PLD():

#   database = wx_dbClass.db_mysql_master('db_ons')

#   database.connect()
#   preco_PLD = database.getSchema('tb_pld')


#   preco_PLD_list = request.get_json()
#   values=[]
#   for row in preco_PLD_list:
#     values.append((row['ano'],row['pldMaxHora'],row['pldMaxEstr'],row['pldMin'],row['custoDeficit']))


#   delete = preco_PLD.delete()
#   database.conn.execute(delete)

#   insert = preco_PLD.insert(values)
#   database.conn.execute(insert)

#   return jsonify('Lista atualizada com sucesso!')

# @bp.route("/API/get/rodadas",methods=['GET'])
# @login_required
# def get_rodadas():
#     dt_rodada = pd.to_datetime(request.args['dt_rodada'])
#     rodadas = rz_dbLib.query_rodadas_do_dia(dt_rodada)

#     df = pd.DataFrame(rodadas, columns = ['id','str_modelo','hr_rodada','fl_psat','fl_pdp','fl_preliminar'])
#     df['str_modelo'] = df.apply(lambda x: x['str_modelo'] + 
#                                 (".PRE" if (x['fl_preliminar']) else ".Pdp" if (x['fl_pdp']and not x['fl_preliminar']) else  ".Psat" if x['fl_psat'] else ".GPM")
#                                 if ((x['str_modelo'] in ["ETA40remvies", "PCONJUNTO", "PCONJUNTO2"]) or (x['fl_preliminar'] == 1))else x['str_modelo'], axis=1)
#     df['str_modelo'] = df['str_modelo'] + "_" + df['id'].astype(str)
#     df = df.drop(columns=['id'])
#     df['num'] = df['str_modelo'].apply(lambda x: 1 if "GEFS" in x  else 2 if "GFS" in x else 3 if "EC" in x else 4 if "PCONJUNTO" in x else 5)
#     df = df.sort_values(by=['num'])

#     array = df.groupby(['hr_rodada'])['str_modelo'].apply(list).to_dict()
#     return jsonify(array)


# @bp.route("/API/get/dados-rodadas-ena",methods=['GET'])
# @login_required
# def get_dados_rodadas_ena():
    
#     # INPUTS DO USUSARIO
#     rodadas = eval(request.args['data'])
#     dt_rodada = rodadas['dt_rodada']
#     granularidade = rodadas['granularidade']
#     modelos_list = list(rodadas['rodadas'].items())

#     no_cache= request.args.get('flagNoCache')
  
#     if True:
#       ena = rz_ena.get_ena_modelos_smap(modelos_list,dt_rodada, granularidade)
#     else:
#       prefixo = "PREVISAO_ENA"
#       ena = rz_cache.cache_rodadas_modelos(prefixo,rodadas)
#     dict_master = {}
#     dict_master["PREVISAO_ENA"] = ena

#     return jsonify(dict_master)

    
# @bp.route("/API/get/dados-rodadas-chuva",methods=['GET'])
# @login_required
# def get_dados_rodadas_chuva():


#     rodadas = request.args.get('data')
#     if rodadas:
#       rodadas = eval(rodadas)
#       granularidade = rodadas['granularidade']
#       dt_rodada = rodadas['dt_rodada']
#     else:
#       granularidade = request.args.get('granularidade')
#       dt_rodada = utils.formatar_data(request.args.get('dt_rodada'))
#       modelo = request.args.get('modelo')

#       rodadas = {}

#       id = rz_dbLib.query_rodadas_ndias_atras(dt_rodada=dt_rodada, ndias=0, hr_rodada= 0,modelo= modelo)
#       rodadas['rodadas'] = id['data']
#       rodadas['granularidade'] = granularidade 
#       rodadas['dt_rodada'] = dt_rodada

#     no_cache= request.args.get('flagNoCache')
#     if no_cache or granularidade.lower() == 'subbacia':

#       ids_sem_pzeraza = [chave for chave, modelo in rodadas['rodadas'].items() if "PZERADA" not in modelo]
#       chuva = rz_chuva.get_chuva_smap_ponderada(ids_sem_pzeraza,dt_rodada,granularidade)

#     else:
#       chaves_sem_pzerada = {chave: valor for chave, valor in rodadas['rodadas'].items() if "PZERADA" not in valor}
#       rodadas['rodadas'] = chaves_sem_pzerada
#       prefixo = "PREVISAO_CHUVA"
#       chuva = rz_cache.cache_rodadas_modelos(prefixo,rodadas)
#     if not chuva:
#       return jsonify({'204':f'sem previsao de chuva na rodada do dia {dt_rodada}'}), 204
#     dict_master = {}
#     dict_master["PREVISAO_CHUVA"] = chuva

#     return jsonify(dict_master)


  

# #========================================================= COMPARATIVO CARGA ===================================================================================
# @bp.route("/API/get/acompanhamento_ipdo",methods=['GET'])
# @login_required
# def get_cargas_ipdo():
#   dtref = request.args['dtref']
#   dtref = datetime.datetime.strptime(dtref,"%Y%m%d")
#   ipdo_dict_verificado = rz_dbLib.get_valores_IPDO(dtref)
#   return jsonify(ipdo_dict_verificado)

# @bp.route("/API/get/acompanhamento_ipdo_previsao",methods=['GET'])
# @login_required
# def get_cargas_ipdo_previsao():
#   dtref = request.args['dtref']
#   dtref = (datetime.datetime.now())
#   ipdo_dict_previsao = rz_dbLib.get_valores_IPDO_previsao(dtref)
#   return jsonify(ipdo_dict_previsao)

# @bp.route("/API/get/previsao_dessem",methods=['GET'])
# @login_required
# def get_previsao_dessem():
#   previsao = rz_dbLib.get_previsao_dessem()
#   return jsonify(previsao)

# @bp.route("/API/get/acompanhamento_ipdo_verificado_e_previsao",methods=['GET'])
# @login_required
# def get_ipdo_verificado_e_previsao():
#   dtref = request.args['dtref']
#   dtref = datetime.datetime.strptime(dtref,"%Y%m%d")
#   dtrefPrevisao = (datetime.datetime.now())
#   ipdo_dict_verificado = rz_dbLib.get_valores_IPDO(dtref)
#   ipdo_dict_previsao = rz_dbLib.get_valores_IPDO_previsao(dtrefPrevisao)
#   tabela_completa = rz_dbLib.juntaTabela_IPDO(ipdo_dict_verificado,ipdo_dict_previsao)
#   return jsonify(tabela_completa)


# @bp.route("/API/get/acompanhamento_decomp",methods=['GET'])
# @login_required
# def get_valores_decomp():
#   dtref = request.args['dtref']
#   dtref = datetime.datetime.strptime(dtref,"%Y%m%d")
#   decomp_dict,dataFormatadaPrimeiraPosicao = rz_dbLib.get_valores_DECOMP(dtref)
#   return jsonify(decomp_dict)

# @bp.route("/API/get/expectativa_realizado",methods=['GET'])
# @login_required
# def get_valores_expectativa_realizado():
#   dtref = request.args['dtref']
#   dtref = datetime.datetime.strptime(dtref,"%Y%m%d")
#   decomp_dict,dataFormatadaPrimeiraPosicao = rz_dbLib.get_valores_DECOMP(dtref)
#   dtrefPrevisao = (datetime.datetime.now())
#   ipdo_dict_verificado = rz_dbLib.get_valores_IPDO(dtref)
#   ipdo_dict_previsao = rz_dbLib.get_valores_IPDO_previsao(dtrefPrevisao)
#   tabela_completa_IPDO = rz_dbLib.juntaTabela_IPDO(ipdo_dict_verificado,ipdo_dict_previsao)
  
#   expectativa_realizado = rz_dbLib.calc_expectativa_realizado(dtref,decomp_dict,dataFormatadaPrimeiraPosicao,tabela_completa_IPDO)
#   return jsonify(expectativa_realizado)

# @bp.route("/API/get/acompanhamento_newave",methods=['GET'])
# @login_required
# def get_cargas_newave():
#   dtref = request.args['dtref']
#   dtref = datetime.datetime.strptime(dtref,"%Y%m%d")
#   newave_dict = rz_dbLib.get_valores_Newave(dtref)
#   return jsonify(newave_dict)

# @bp.route('/API/get/geracao_eolica_prevista_dc')
# @login_required
# def API_geracao_eolica_prevista_decomp():
    
#     data_referente = request.args['data_referente']
#     dt_referente = datetime.datetime.strptime(data_referente, '%Y-%m-%d')
    
#     dt_inicial = wx_opweek.getLastSaturday(dt_referente)
#     dt_final = dt_inicial + datetime.timedelta(days=7)

#     submercados = {3:'NORDESTE', 2:'SUL'}
#     valor_estagio = 1
        
#     discretizacao = rz_dbLib.get_discretizacao_tempo(dt_inicial,  dt_final)
#     df_discretizacao = pd.DataFrame(discretizacao, columns=['dt_inicio_patamar', 'vl_patamar'])

    
#     bloco_pq = rz_dbLib.get_deck_decomp_bloco_pq(dt_inicio_deck=dt_inicial)
#     colunas = ['vl_estagio', 'vl_patamar', 'cd_submercado', 'vl_geracao_eol']
#     df_bloco_pq = pd.DataFrame(bloco_pq, columns=colunas)

#     df_dc_primeira_semana = df_bloco_pq[df_bloco_pq['vl_estagio'] == valor_estagio]

#     df_dc_primeira_semana = pd.merge(df_dc_primeira_semana, df_discretizacao, on='vl_patamar')
#     df_dc_primeira_semana = df_dc_primeira_semana[df_dc_primeira_semana['cd_submercado'].isin(submercados.keys())]
#     df_dc_primeira_semana['submercado'] = df_dc_primeira_semana['cd_submercado'].map(submercados)
    
#     resposta = {}

#     for cd in submercados:

#         sub = submercados[cd]
#         resposta[sub] = {}

#         df_metrics = df_dc_primeira_semana[df_dc_primeira_semana['submercado'] == sub].set_index('dt_inicio_patamar')['vl_geracao_eol']
#         media_horaria = df_metrics.resample('1h').mean().ffill().round(2)
#         media_diaria = df_metrics.resample('24h').mean().ffill().round(2)
#         media_semanal = df_metrics.resample('168h').mean().ffill().round(2)

#         media_horaria.index = media_horaria.index.strftime('%Y/%m/%d %H:%M:%S')
#         media_diaria.index = media_diaria.index.strftime('%Y/%m/%d %H:%M:%S')
#         media_semanal.index = media_semanal.index.strftime('%Y/%m/%d %H:%M:%S')

#         resposta[sub]['horaria'] = media_horaria.to_dict()
#         resposta[sub]['diaria'] = media_diaria.to_dict()
#         resposta[sub]['semanal'] = media_semanal.to_dict()

#     df_dc_primeira_semana['dt_inicio_patamar'] = pd.to_datetime(df_dc_primeira_semana['dt_inicio_patamar']).dt.strftime('%Y-%m-%d %H:%M')
#     df_dc_primeira_semana_pivot = df_dc_primeira_semana.pivot(index='submercado', columns='dt_inicio_patamar', values='vl_geracao_eol')
#     values_dc = df_dc_primeira_semana_pivot.to_dict('index')

#     valores = {}
#     valores['prev'] = values_dc
#     valores['media'] = resposta

#     return jsonify(valores)





# @bp.route('/API/get/comparativo_carga_newave', methods = ["GET"])
# @login_required
# def API_comparativo_carga_newave():

#   dt_referente = request.args.get('data_referente')
#   no_cache = request.args.get('no_cache')
  
#   if no_cache:

#     df_carga_newave_Sistema_cAdic = rz_dbLib.comparativo_carga_newave(dt_referente = dt_referente)

#     if df_carga_newave_Sistema_cAdic:
#       return jsonify(df_carga_newave_Sistema_cAdic), 200
#     return jsonify(df_carga_newave_Sistema_cAdic), 204
    
#   else:
#     comparativo_carga_newave = rz_cache.cache_comparativo_carga_newave(dt_referente)
#     if comparativo_carga_newave:
#       return jsonify(comparativo_carga_newave), 200
#     return {}, 204
  
  
# #==============================================================================================================================================================




# @bp.route("/API/GET/carga_newavePatamar",methods=['GET'])
# @login_required
# def get_carga_newavePatamar():
#   data_inicial = request.args['dataDeck']
#   lista_de_datas = data_inicial.split(',')  # Supondo que as datas sejam separadas por vírgulas
#   values = []  # Lista para armazenar os resultados
#   for data in lista_de_datas:
#     resultado = rz_dbLib.get_newave_submercado(data_inicial)
#     values.append(resultado)

#   return jsonify(values)

# @bp.route("/API/GET/carga_dessemPatamar",methods=['GET'])
# @login_required
# def get_carga_dessemPatamar():
#   data_inicial = request.args['dataDeck']
#   lista_de_datas = data_inicial.split(',')  # Supondo que as datas sejam separadas por vírgulas
#   values = []  # Lista para armazenar os resultados
#   for data in lista_de_datas:
#     resultado = rz_dbLib.get_dessem_submercado(data)
#     values.append(resultado)

#   return jsonify(values)

# @bp.route("/API/GET/carga_decompPatamar",methods=['GET'])
# @login_required
# def get_carga_decompPatamar():
#   data_inicial = request.args['dataDeck']
#   lista_de_datas = data_inicial.split(',')  # Supondo que as datas sejam separadas por vírgulas
#   values = []  # Lista para armazenar os resultados
  
#   for data in lista_de_datas:
#         resultado = rz_dbLib.get_decomp_submercado(data)
#         values.append(resultado)
        
#   return jsonify(values)


# @bp.route("/API/GET/valor_pld",methods=['GET'])
# @login_required 
# def get_valor_PLD():
  
#   data_inicial = request.args['dataInicial']

#   values = rz_dbLib.get_mediaMensal_PLD(data_inicial)
  
#   return jsonify(values)


# @bp.route("/API/set/vnc_kill_session",methods=['POST'])
# @login_required 
# def reset_vnc_session():
  
#   sessaoSolicitada = request.get_json()
  
#   cmd = f'vnc_restart {sessaoSolicitada}'

#   os.system(cmd)

#   return jsonify(sessaoSolicitada)

# @bp.route("/API/remove/geradorPrevs", methods = ["GET"])
# @login_required 
# def remove_arquivosZipPrevs():
  
#   caminhoArquivos = r'/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/previvaz/libs'

#   try:
#     # Lista todos os arquivos no diretório
#     arquivos = os.listdir(caminhoArquivos)

#     # Itera sobre os arquivos e remove os arquivos zip
#     for arquivo in arquivos:
#         if arquivo.endswith('.zip'):
#             caminhoCompleto = os.path.join(caminhoArquivos, arquivo)
#             os.remove(caminhoCompleto)
#     print('Arquivos zip removidos com sucesso!')
#     return "Arquivos zip removidos com sucesso!"

#   except Exception as e:
#       return f"Erro ao remover arquivos zip: {str(e)}"
      
  
# @bp.route("/API/GET/geradorPrevs", methods=['GET'])
# @login_required 

# def get_gerarPrevs():
        
#     dt_anoEscolhido = int(request.args.get('anoDesejado'))
#     anoComparativo_str = request.args.get('anoComparativo')
  
#     ano = int(anoComparativo_str)
    
#     # if type(ano) == type(1):
#     #     ano = [ano]

#     # Chama a função desejada com o ano atual
#     path_saida = wx_geradorPrevs.gerarador_prevs(dt_anoEscolhido, ano)

#     # Restante do seu código para manipular os resultados conforme necessário
#     pasta_dst = os.path.join(os.path.dirname(path_saida), f'{ano}')
#     shutil.move(path_saida, pasta_dst)

#     # criando pasta zip para cada ano
#     shutil.make_archive(pasta_dst, 'zip', pasta_dst)

#     # removendo a pasta que não é zip de cada ano
#     shutil.rmtree(pasta_dst)

#     return jsonify({"Arquivos foram gerados e salvos nos caminhos": pasta_dst})

  
# @bp.route("/API/download/geradorPrevs", methods=['GET'])
# @login_required
# def download_arquivo_geradorPrevs():
#     caminhoArquivos = r'/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/previvaz/libs'
#     # /WX/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/previvaz/libs
#     try:
#         # Obtenha a lista de arquivos com extensão .zip na pasta
#         arquivos_zip = [f for f in os.listdir(caminhoArquivos) if f.endswith('.zip')]

#         # Verifique se há pelo menos um arquivo .zip na pasta
#         if arquivos_zip:
#             # Crie um buffer de memória para armazenar o arquivo zip
#             zip_buffer = BytesIO()

#             with zipfile.ZipFile(zip_buffer, 'w') as zipf:
#                 for arquivo_zip in arquivos_zip:
#                     # Adicione os arquivos sem a estrutura de pastas
#                     zipf.write(os.path.join(caminhoArquivos, arquivo_zip), arcname=os.path.basename(arquivo_zip))
#                     caminho_completo = os.path.join(caminhoArquivos, arquivo_zip)
#                     lista_arq_sem_extensao = [arquivo.split('.')[0] for arquivo in arquivos_zip]

#                 with zipfile.ZipFile(caminho_completo, 'r') as zip_ref:
#                   # Obter a lista de nomes de arquivos dentro do arquivo ZIP
#                   lista_nomes_arquivos = zip_ref.namelist()
#                   anoInicial = lista_nomes_arquivos[0][0:4]
                  
#             # Volte para o início do buffer
#             zip_buffer.seek(0)
            
#             # Envie o buffer de memória como resposta para download
#             return send_file(zip_buffer, as_attachment=True, download_name=f'prevs_{anoInicial}_{lista_arq_sem_extensao}.zip')

#         else:
#             return jsonify({'error': 'Nenhum arquivo .zip encontrado para download'})

#     except Exception as e:
#         return jsonify({'error': f'Erro durante o download do arquivo: {str(e)}'})



# #========================================================================================





# @bp.route('/relatorio-diario',methods=['GET'])
# @login_required
# def relatorio_diario():
#   return render_template('relatorio_diario.html')


# @bp.route('/API/get/dados-rodadas-ndias-atras', methods=['GET'])
# @login_required
# def API_get_rodadas_ndias_atras():
#   dict_master = {}
#   rodadas = {}
#   qtd_dias_rodada = 20
#   dt_rodada = datetime.datetime.now()
#   ndias = request.args.get('ndias', '0')
#   ndias = int(ndias)
#   hr_rodada = request.args.get('hr_rodada')
#   modelo = request.args.get('modelo')
#   granularidade = request.args.get('granularidade')
#   # flag_atualizar = request.args.get('flagAtualizar')
#   # flag_reset = request.args.get('reset')
#   no_cache= request.args.get('flagNoCache')


#   ids_rodadas = rz_dbLib.query_rodadas_ndias_atras(dt_rodada, ndias, hr_rodada, modelo)
#   rodadas["rodadas"] = ids_rodadas["data"]
#   rodadas["dt_rodada"] = (dt_rodada - datetime.timedelta(days=ndias)).strftime("%Y-%m-%d")
#   rodadas["granularidade"] = granularidade
#   if no_cache:
#     ena = rz_ena.get_ena_modelos_smap(ids_rodadas,dt_rodada, granularidade, dias=qtd_dias_rodada)
#     acomph = rz_ena.get_ena_acomph(dt_rodada - datetime.timedelta(days=35), granularidade)
#     acomph = acomph.to_dict()
#   else:
#     prefixo = "PREVISAO_ENA"
#     ena = rz_cache.cache_rodadas_modelos(prefixo,rodadas, dias=qtd_dias_rodada)

#     prefixo = "ACOMPH"
#     acomph = rz_cache.cache_acomph(prefixo, granularidade, dt_rodada - datetime.timedelta(days=35),dt_rodada)

#     for rodada in ena:
#       for id_rodada in ids_rodadas['data']:
#         if id_rodada == rodada['id_rodada']:
#           rodada['modelo'] = ids_rodadas['data'][id_rodada][:-2]


#   ena.sort(key=lambda chave: chave["id_rodada"], reverse =False)
#   dict_master["PREVISAO_ENA"] = ena
#   dict_master["ACOMPH"] = acomph
#   return jsonify(dict_master)

# @bp.route('/API/get/tabela-diferenca-modelos', methods=['GET'])
# @login_required
# def API_get_tabela_diferenca_modelos():
#   pathfile = '/WX2TB/Documentos/fontes/outros/web_modelos/server/'
#   nome_modelo1 = request.args.get('nome_modelo_1')
#   nome_modelo2 = request.args.get('nome_modelo_2', "observado")
#   data_rodada = request.args.get('data_rodada', (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%d/%m/%Y'))
#   hr_rodada = request.args.get('hr_rodada', 0)

#   dados_modelo1 = [data_rodada, nome_modelo1, hr_rodada]
#   dados_modelo2 = [data_rodada, nome_modelo2, hr_rodada]
#   # TODO: definir como ficara a tabela para chuva observada, para, por exemplo, plot de diferenca de 1 dia atrás
#   return rz_chuva.dif_tbmaps_chuva_html(dados_modelo1, dados_modelo2, plotar_mapas=False)





# @bp.route('/API/get/diferenca-chuva-prevista-observada', methods=["GET"])
# @login_required
# def API_get_diferenca_chuva_prevista_observada():
#   modelo = request.args.get('modelo')
#   granularidade = request.args.get('granularidade')
#   no_cache = request.args.get('flagNoCache')
#   rodadas = {}
#   dt_str = request.args.get('data_rodada')
#   dt_rodada = utils.formatar_data(dt_str) - datetime.timedelta(days=1)
#   # pdb_login.set_trace()

#   if granularidade not in ['bacia', 'subbacia', 'submercado']:
#     return jsonify({"erro":f"granularidade {granularidade} invalida"}), 400

#   id = rz_dbLib.query_rodadas_ndias_atras(dt_rodada=dt_rodada, ndias=0, hr_rodada= 0,modelo= modelo)
#   rodadas["rodadas"] = id['data']
#   rodadas["granularidade"] = granularidade
#   rodadas["dt_rodada"] = dt_str
#   id = [chave for chave, modelo in id['data'].items()]
#   if not id:
#     return jsonify({'erro' : f'id da rodada de {dt_rodada} não encontrados'}), 404
#   if no_cache or granularidade == "subbacia":
#     chuva_prevista = rz_chuva.get_chuva_smap_ponderada(id,dt_str,granularidade)
#   else:
#       prefixo = "PREVISAO_CHUVA"
#       chuva_prevista = rz_cache.cache_rodadas_modelos(prefixo,rodadas)

#   diferenca_prevista_observada = {}

#   if granularidade == 'subbacia':
#     datas = list(chuva_prevista.keys())
#     data_inicio = utils.formatar_data(datas[0])
#     data_final = utils.formatar_data(datas[len(datas) - 1])
#     chuva_observada = rz_chuva.getChuvaObservada(data_inicio,granularidade, data_final)

#     for dia in chuva_prevista:
#       if dia in chuva_observada:
#         diferenca_prevista_observada[dia] = {}
#         for id_subbacia in chuva_prevista[dia]:
#           if id_subbacia in chuva_prevista[dia]:
#             diferenca_prevista_observada[dia][id_subbacia] = chuva_prevista[dia][id_subbacia] - chuva_observada[dia][id_subbacia]

#   else:
#     chuva_prevista = chuva_prevista[0]["valores"]
#     datas = list(chuva_prevista[next(iter(chuva_prevista))].keys())
#     data_inicio = utils.formatar_data(datas[0])
#     data_final = utils.formatar_data(datas[len(datas) - 1])
#     chuva_observada = rz_chuva.getChuvaObservada(data_inicio,granularidade, data_final)

#     for regiao in chuva_prevista:
#       diferenca_prevista_observada[regiao] = {}
#       for dia in datas:
#         if dia in chuva_observada[regiao]:
#           diferenca_prevista_observada[regiao][dia] = chuva_prevista[regiao][dia] - chuva_observada[regiao][dia]
#           pass

#       pass
#   if diferenca_prevista_observada:
#     return jsonify(diferenca_prevista_observada), 200
#   else:
#     return jsonify(diferenca_prevista_observada), 404
  


# @bp.route('/API/get/total-geracao', methods=['GET'])
# @login_required
# def API_get_total_geracao_data_hora_entre():
#   dt_inicial = request.args.get('dtInicial', type=utils.formatar_data)
#   dt_final = request.args.get('dtFinal', type=utils.formatar_data)
#   diferenca =  dt_final.date() - dt_inicial.date()
#   no_cache = request.args.get('no_cache')
#   if no_cache:
#     total_geracao = db_decks.tb_balanco_dessem.total_geracao_data_hora_entre(dt_inicial, dt_final)
#   else:
#     total_geracao = rz_cache.get_cached(db_decks.tb_balanco_dessem.total_geracao_data_hora_entre, dt_inicial, dt_final)  
#   if not total_geracao:
#     return {}, 204
#   return total_geracao, 200

# @bp.route('/API/get/total-geracao/<subsistema>', methods=['GET'])
# @login_required
# def API_get_total_geracao_por_subsistema_data_hora_entre(subsistema:str):
#   subsistema = utils.subsistema_para_sigla(subsistema)
#   dt_inicial = request.args.get('dtInicial', type=utils.formatar_data)
#   dt_final = request.args.get('dtFinal', type=utils.formatar_data)
#   diferenca =  dt_final.date() - dt_inicial.date()
#   no_cache = request.args.get('no_cache')
#   if no_cache:
#     total_geracao = db_decks.tb_balanco_dessem.total_geracao_por_subsistema_data_hora_entre(subsistema, dt_inicial, dt_final)
#   else:
#     total_geracao = rz_cache.get_cached(db_decks.tb_balanco_dessem.total_geracao_por_subsistema_data_hora_entre, subsistema, dt_inicial, dt_final)  
#   if not total_geracao:
#     return {}, 204
#   return total_geracao, 200





# @bp.route('API/get/previsao/ena-modelos', methods=['GET'])
# @login_required
# def API_get_ena_modelos():
#     no_cache = request.args.get('noCache', default=False)
#     modelos_list = request.args.get('modelos_list')
#     granularidade = request.args.get('granularidade')
#     if no_cache:
#         return rz_ena.get_previsao_ena_smap(eval(modelos_list), granularidade=granularidade), 200
#     # return rz_cache.get_cached_previsao_modelos(key=f'previsao_ena.{granularidade.lower()}',modelos_list=eval(modelos_list)), 200

# @bp.route('API/get/previsao/modelos-disponiveis', methods=['GET'])
# @login_required
# def API_get_modelos_disponiveis():
#     from PMO.scripts_unificados.apps.smap.libs.Rodadas import tb_smap
#     dt_rodada = request.args.get('dt_rodada')
#     TB_SMAP = tb_smap()
#     df_modelos = TB_SMAP.get_rodadas_do_dia(dt_rodada)
#     df_modelos['dt_rodada'] = pd.to_datetime(df_modelos['dt_rodada']).dt.strftime('%Y-%m-%d')
#     return df_modelos.to_dict('records')

