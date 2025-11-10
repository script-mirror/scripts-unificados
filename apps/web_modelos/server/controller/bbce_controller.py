# -*- coding: utf-8 -*-
import sys
import pdb

from flask import request, jsonify
import datetime
from flask_login import login_required
import sqlalchemy as db
import pandas as pd

sys.path.insert("/WX2TB/Documentos/fontes/PMO/scripts_unificados/")
from bibliotecas import wx_dbClass
from apps.web_modelos.server.server import bp
from apps.web_modelos.server import utils 
from apps.web_modelos.server.libs import db_bbce
from apps.web_modelos.server.caches import rz_cache


@bp.route('/API/get/produtos-bbce',methods=['GET'])
@login_required
def get_produtos_bbce():

    database = wx_dbClass.db_mysql_master('bbce')
    database.connect()
    tb_produtos_interesse = database.getSchema('tb_produtos_interesse')
    tb_produtos = database.getSchema('tb_produtos')

    select = db.select(
        tb_produtos_interesse.c['id_produto'],
        tb_produtos_interesse.c['ordem'],
        tb_produtos.c['str_produto']
    ).join(
        tb_produtos, tb_produtos.c['id_produto'] == tb_produtos_interesse.c['id_produto']
    )
    result = database.db_execute(select).fetchall()
    df = pd.DataFrame(result, columns=["id", "ordem", "str_produto"])
    return df.to_dict("records")
@bp.route('/API/set/produtos-bbce',methods=['POST'])
@login_required
def set_produtos_bbce():

    database = wx_dbClass.db_mysql_master('db_config')
    database.connect()
    tb_produtos_bbce = database.getSchema('tb_produtos_bbce')


    assuntos_list = request.get_json()
    values=[]
    for row in assuntos_list:
        values.append((row['id'],row['palavra']))

    delete = tb_produtos_bbce.delete()
    database.conn.execute(delete)

    insert = tb_produtos_bbce.insert(values)
    database.conn.execute(insert)

    return jsonify('Lista atualizada com sucesso!')

@bp.route('/API/get/bbce/resumos-negociacoes',methods=['GET'])
@login_required
def get_negociacoes_bbce():

    produto = request.args.get('produto')
    no_cache = request.args.get('noCache', default=True)
    atualizar = request.args.get('atualizar', default=False)
    if no_cache:
      return db_bbce.tb_negociacoes_resumo.get_negociacao_bbce(produto)
    return rz_cache.get_cached(db_bbce.tb_negociacoes_resumo.get_negociacao_bbce, produto, atualizar=atualizar)

@bp.route('/API/get/bbce/resumos-negociacoes/negociacoes-de-interesse', methods=['GET'])
@login_required
def get_negociacoes_interesse_bbce_por_data():
    no_cache = request.args.get('noCache', default=True)
    atualizar = request.args.get('atualizar', default=False)
    categoria_negociacao = request.args.get('categoria_negociacao', default="Mesa")
    datas = request.args.get('datas', default=datetime.datetime.now(), type=utils.formatar_data)
    if no_cache:
        return db_bbce.tb_negociacoes_resumo.get_negociacoes_interesse_por_data(datas, categoria_negociacao)
    return rz_cache.get_cached(db_bbce.tb_negociacoes_resumo.get_negociacoes_interesse_por_data, datas, atualizar=atualizar)


@bp.route('/API/get/bbce/resumos-negociacoes/negociacoes-de-interesse/fechamento', methods=['GET'])
@login_required
def get_negociacoes_fechamento_interesse_por_data():
    no_cache = request.args.get('noCache', default=True)
    atualizar = request.args.get('atualizar', default=False)
    datas = request.args.get('datas', default=datetime.datetime.now(), type=utils.formatar_data)
    if no_cache:
        return db_bbce.tb_negociacoes_resumo.get_negociacoes_fechamento_interesse_por_data(datas)
    return rz_cache.get_cached(db_bbce.tb_negociacoes_resumo.get_negociacoes_fechamento_interesse_por_data, datas, atualizar=atualizar)

@bp.route('/API/get/bbce/resumos-negociacoes/negociacoes-de-interesse/ultima-atualizacao')
@login_required
def get_datahora_ultima_negociacao():
    no_cache = request.args.get('noCache', default=True)
    atualizar = request.args.get('atualizar', default=False)
    categoria_negociacao = request.args.get('categoria_negociacao', default="Mesa")
    if no_cache:
        return db_bbce.tb_negociacoes.get_datahora_ultima_negociacao(categoria_negociacao)
    return rz_cache.get_cached(db_bbce.tb_negociacoes.get_datahora_ultima_negociacao, atualizar=atualizar)

@bp.route('/API/get/bbce/resumos-negociacoes/spread/preco-medio', methods=['GET'])
@login_required
def get_spread_preco_medio_negociacoes():
  
  no_cache = request.args.get('noCache', default=True)
  atualizar = request.args.get('atualizar', default=False)

  produto1 = request.args.get('produto1')
  produto2 = request.args.get('produto2')
  if no_cache:
    return db_bbce.tb_negociacoes_resumo.spread_preco_medio_negociacoes(produto1, produto2), 200
  return rz_cache.get_cached(db_bbce.tb_negociacoes_resumo.spread_preco_medio_negociacoes, produto1, produto2, atualizar=atualizar)

