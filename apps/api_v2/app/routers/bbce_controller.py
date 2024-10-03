from sys import path
path.insert(1,"/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/api_v2")

from fastapi import APIRouter
from typing import List, Optional
from app.crud import bbce_crud
from app.utils import cache
from app.schemas import ProdutoInteresseSchema, CategoriaNegociacaoEnum
import datetime


router = APIRouter(prefix="/bbce")


@router.get("/produtos-interesse",tags=["BBCE"])
async def get_produtos_interesse():
    return bbce_crud.ProdutoInteresse.get_all()

@router.post("/produtos-interesse", tags=["BBCE"])
async def post_produtos_interesse(
        produtos_interesse: List[ProdutoInteresseSchema]
):
    return bbce_crud.ProdutoInteresse.post(produtos_interesse)

@router.get("/produtos-interesse/html", tags=["BBCE"])
async def get_produtos_interesse(
    data:datetime.date,
    tipo_negociacao:Optional[CategoriaNegociacaoEnum] = None
):
    return bbce_crud.NegociacoesResumo.get_html_table_negociacoes_bbce(data, tipo_negociacao)

@router.get("/negociacoes/categorias", tags=["BBCE"])
async def get_categorias_negociacoes():
    return bbce_crud.CategoriaNegociacao.get_all()


@router.get('/resumos-negociacoes',tags=['BBCE'])
def get_negociacoes_bbce(
    produto:str,
    categoria_negociacao:Optional[CategoriaNegociacaoEnum] = None,
    no_cache:bool=True,
    atualizar:bool=False
):
    if no_cache:
      return bbce_crud.NegociacoesResumo.get_negociacao_bbce(produto, categoria_negociacao)
    return cache.get_cached(bbce_crud.NegociacoesResumo.get_negociacao_bbce, produto, categoria_negociacao, atualizar=atualizar)

@router.get('/resumos-negociacoes/negociacoes-de-interesse', tags=['BBCE'])
def get_negociacoes_interesse_bbce_por_data(
    datas:datetime.datetime,
    categoria_negociacao:Optional[CategoriaNegociacaoEnum] = None,
    no_cache:bool = True,
    atualizar:bool = False
):

    if no_cache:
        return bbce_crud.NegociacoesResumo.get_negociacoes_interesse_por_data(datas,categoria_negociacao)
    return cache.get_cached(bbce_crud.NegociacoesResumo.get_negociacoes_interesse_por_data, datas, categoria_negociacao, atualizar=atualizar)


@router.get('/resumos-negociacoes/negociacoes-de-interesse/fechamento', tags=['BBCE'])
def get_negociacoes_fechamento_interesse_por_data(
    datas:datetime.datetime,
    categoria_negociacao:Optional[CategoriaNegociacaoEnum] = None,
    no_cache:bool = True,
    atualizar:bool = False
):
   
    if no_cache:
        return bbce_crud.NegociacoesResumo.get_negociacoes_fechamento_interesse_por_data(datas,categoria_negociacao)
    return cache.get_cached(bbce_crud.NegociacoesResumo.get_negociacoes_fechamento_interesse_por_data, datas, categoria_negociacao,atualizar=atualizar)

@router.get('/resumos-negociacoes/negociacoes-de-interesse/ultima-atualizacao', tags=['BBCE'])
def get_datahora_ultima_negociacao(
    categoria_negociacao:Optional[CategoriaNegociacaoEnum] = None,
    no_cache:bool=True,
    atualizar:bool=False
):
    if no_cache:
        return bbce_crud.Negociacoes.get_datahora_ultima_negociacao(categoria_negociacao)
    return cache.get_cached(bbce_crud.Negociacoes.get_datahora_ultima_negociacao, categoria_negociacao, atualizar=atualizar)

@router.get('/resumos-negociacoes/spread/preco-medio', tags=['BBCE'])
def get_spread_preco_medio_negociacoes(
    produto1:str,
    produto2:str,
    categoria_negociacao:Optional[CategoriaNegociacaoEnum] = None,
    no_cache:bool=True,
    atualizar:bool=False
):

  if no_cache:
    return bbce_crud.NegociacoesResumo.spread_preco_medio_negociacoes(produto1, produto2, categoria_negociacao)
  return cache.get_cached(bbce_crud.NegociacoesResumo.spread_preco_medio_negociacoes, produto1, produto2, categoria_negociacao, atualizar=atualizar)
