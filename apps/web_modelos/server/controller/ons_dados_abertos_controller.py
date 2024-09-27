# -*- coding: utf-8 -*-
import sys
import pdb

from flask import request
import datetime
from flask_login import login_required

sys.path.insert(1,"/WX2TB/Documentos/fontes")
from PMO.scripts_unificados.apps.web_modelos.server.server import bp
from PMO.scripts_unificados.apps.web_modelos.server import utils 
from PMO.scripts_unificados.apps.web_modelos.server.libs import db_ons
from PMO.scripts_unificados.apps.web_modelos.server.caches import rz_cache

@bp.route('API/get/ons-dados-abertos/datas', methods=['GET'])
def API_get_ons_dados_abertos_lista_datas():
  return db_ons.listar_datas()

@bp.route('API/get/ons-dados-abertos/constrained-off/geracao-limitada/<tipo>/<razao_restricao>', methods=['GET'])
@login_required
def API_get_coff_buscar_geracao_limitada_por_data_entre(tipo:str, razao_restricao:str):
  
  no_cache = request.args.get('noCache', default=True)
  atualizar = request.args.get('atualizar', default=True)
  dt_inicial = request.args.get('dtInicial', type=utils.formatar_data)
  dt_final = request.args.get('dtFinal', type=utils.formatar_data)
  submercado = request.args.get('submercado', type=str, default='%')
  pivot = request.args.get('pivot', default=False)
  
  if no_cache:
    return db_ons.tb_restricoes_coff.buscar_geracao_limitada_por_data_entre(tipo, dt_inicial,dt_final,razao_restricao, submercado, pivot), 200
  return rz_cache.get_cached(db_ons.tb_restricoes_coff.buscar_geracao_limitada_por_data_entre,tipo,  dt_inicial, dt_final, razao_restricao, submercado, pivot, atualizar=atualizar)


@bp.route('API/get/ons-dados-abertos/geracao-usina/geracao/<tipo>', methods=['GET'])
@login_required
def API_get_geracao_usina_geracao(tipo:str):
  no_cache = request.args.get('noCache', default=True)
  atualizar = request.args.get('atualizar', default=True)
  dt_inicial = request.args.get('dtInicial', type=utils.formatar_data)
  dt_final = request.args.get('dtFinal', type=utils.formatar_data)
  submercado = request.args.get('submercado', type=str, default='%')
  
  if no_cache:
    return db_ons.tb_geracao_usina.buscar_por_data_entre(tipo, dt_inicial, dt_final, submercado)
  return rz_cache.get_cached(db_ons.tb_geracao_usina.buscar_por_data_entre, tipo, dt_inicial, dt_final,submercado, atualizar=atualizar)


@bp.route('API/get/ons-dados-abertos/carga/global', methods=['GET'])
@login_required
def API_get_carga_global():
  no_cache = request.args.get('noCache', default=True)
  atualizar = request.args.get('atualizar', default=True)
  dt_inicial = request.args.get('dtInicial', type=utils.formatar_data)
  dt_final = request.args.get('dtFinal', type=utils.formatar_data)
  submercado = request.args.get('submercado', type=str, default='%')
  
  if no_cache:
    return db_ons.tb_carga.buscar_carga_global_por_data_entre(dt_inicial, dt_final, submercado)
  return rz_cache.get_cached(db_ons.tb_carga.buscar_carga_global_por_data_entre, dt_inicial, dt_final,submercado, atualizar=atualizar)

@bp.route('API/get/ons-dados-abertos/geracao-total', methods=['GET'])
@login_required
def API_get_geracao_usina_total():
  no_cache = request.args.get('noCache', default=True)
  atualizar = request.args.get('atualizar', default=True)
  dt_inicial = request.args.get('dtInicial', type=utils.formatar_data)
  dt_final = request.args.get('dtFinal', type=utils.formatar_data)
  submercado = request.args.get('submercado', type=str, default='%')
  
  if no_cache:
    return db_ons.tb_geracao_usina.buscar_total_por_data_entre(dt_inicial, dt_final, submercado)
  return rz_cache.get_cached(db_ons.tb_geracao_usina.buscar_total_por_data_entre, dt_inicial, dt_final,submercado, atualizar=atualizar)