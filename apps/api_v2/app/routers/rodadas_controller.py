from sys import path


from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from app.crud import rodadas_crud
from app.utils import cache
from app.schemas import GranularidadeEnum, PesquisaPrevisaoChuva, RodadaCriacao, RodadaSmap, TipoRodadaEnum, ChuvaObsReq
from app.schemas.chuvaprevisao import ChuvaPrevisaoCriacao, ChuvaPrevisaoResposta, ChuvaPrevisaoCriacaoMembro

from app.auth import auth_dependency 
import datetime
from typing import Optional, List

router = APIRouter(prefix='/rodadas')


@router.get('',tags=['Rodadas'])
def get_rodadas(
    dt:Optional[datetime.date] = None,
    no_cache:Optional[bool] = True,
    atualizar:Optional[bool] = False
    ):
    if no_cache:
        return rodadas_crud.CadastroRodadas.get_rodadas_por_dt(dt)
    return cache.get_cached(rodadas_crud.CadastroRodadas.get_rodadas_por_dt, dt, atualizar=atualizar)


@router.get('/historico',tags=['Rodadas'])
def get_historico_rodadas_por_nome(
    nomeModelo:str
    ):
    return rodadas_crud.CadastroRodadas.get_historico_rodadas_por_nome(nomeModelo)



@router.get('/chuva/previsao', tags=['Rodadas'])
def get_chuva_previsao_por_id_data_entre_granularidade(
    nome_modelo:Optional[str] = None,
    dt_hr_rodada:Optional[datetime.datetime] = None,
    granularidade:Optional[GranularidadeEnum] = GranularidadeEnum.subbacia,
    id_chuva:Optional[int] = None,
    dt_inicio_previsao:Optional[datetime.date] = None,
    dt_fim_previsao:Optional[datetime.date] = None,
    no_cache:Optional[bool] = False,
    atualizar:Optional[bool] = False
    ):
    if id_chuva:
        return rodadas_crud.Chuva.get_chuva_por_id_data_entre_granularidade(id_chuva, granularidade.name, dt_inicio_previsao, dt_fim_previsao, no_cache, atualizar)
    elif nome_modelo and dt_hr_rodada:
        return rodadas_crud.Chuva.get_chuva_por_nome_modelo_data_entre_granularidade(nome_modelo, dt_hr_rodada, granularidade.name, dt_inicio_previsao, dt_fim_previsao, no_cache, atualizar)

@router.get('/subbacias', tags=['Rodadas'])
def get_subbacias():
    return rodadas_crud.Subbacia.get_subbacia()

@router.post('/chuva/previsao/pesquisa/{granularidade}', tags=['Rodadas'], response_model=List[ChuvaPrevisaoResposta])
def get_previsao_chuva_modelos_combinados(
    previsao:List[PesquisaPrevisaoChuva],
    granularidade:GranularidadeEnum,
    no_cache:Optional[bool] = False,
    atualizar:Optional[bool] = False
    ):
    return rodadas_crud.Chuva.get_previsao_chuva_modelos_combinados(previsao, granularidade.name, no_cache, atualizar)

@router.post('/chuva/previsao/modelos', tags=['Rodadas'])
def post_chuva_modelo_combinados(
    chuva_prev: List[ChuvaPrevisaoCriacao],
    rodar_smap:bool = True,
    prev_estendida:bool = False
):  
    return rodadas_crud.Chuva.post_chuva_modelo_combinados(chuva_prev, rodar_smap, prev_estendida)

@router.post('/chuva/previsao/membros', tags=['Rodadas'])
def post_chuva_membros(
    chuva_prev: List[ChuvaPrevisaoCriacaoMembro],
    rodar_smap:bool = True
):  
    return rodadas_crud.ChuvaMembro.post_chuva_membro(chuva_prev, rodar_smap)

@router.delete('/chuva/previsao', tags=['Rodadas'])
def delete_rodada_chuva_smap_por_id_rodada(
    id_rodada:int
):
    return rodadas_crud.CadastroRodadas.delete_rodada_chuva_smap_por_id_rodada(id_rodada)

@router.get('/chuva/observada', tags=['Rodadas'])
def get_chuva_observada_por_data(
    dt_observada:datetime.date
    ):
    return rodadas_crud.ChuvaObs.get_chuva_observada_por_data(dt_observada)

@router.post('/chuva/observada', tags=['Rodadas'])
def post_chuva_observada(
    chuva_obs:List[ChuvaObsReq]
):
    return rodadas_crud.ChuvaObs.post_chuva_obs(chuva_obs)

@router.get('/chuva/observada/psat', tags=['Rodadas'])
def get_chuva_observada_psat_por_data(
    dt_observada:datetime.date
    ):
    return rodadas_crud.ChuvaObsPsat.get_chuva_observada_psat_por_data(dt_observada)


@router.post('/chuva/observada/psat', tags=['Rodadas'])
def post_chuva_observada_psat(
    chuva_obs:List[ChuvaObsReq]
):
    return rodadas_crud.ChuvaObsPsat.post_chuva_obs_psat(chuva_obs)

@router.post('/smap', tags=['Rodadas'])
def post_smap(
    rodada:RodadaSmap
):  
    return rodadas_crud.Smap.post_rodada_smap(rodada)

@router.get('/chuva/previsao/membros', tags=['Rodadas'])
def get_chuva_por_nome_modelo_data_entre_granularidade(
    nome_modelo:Optional[str] = None,
    dt_hr_rodada:Optional[datetime.datetime] = None,
    granularidade:Optional[GranularidadeEnum] = GranularidadeEnum.subbacia,
    dt_inicio_previsao:Optional[datetime.date] = None,
    dt_fim_previsao:Optional[datetime.date] = None,
    no_cache:Optional[bool] = False,
    atualizar:Optional[bool] = False
):
    return rodadas_crud.ChuvaMembro.get_chuva_por_nome_modelo_data_entre_granularidade(nome_modelo,dt_hr_rodada,granularidade,dt_inicio_previsao,dt_fim_previsao,no_cache,atualizar)
    

# @router.get("/protected-endpoint")
# def protected_endpoint(session_data: dict = Depends(auth_dependency)):
#     user_id = session_data["user_id"]
#     return {"message": "User is authenticated", "user_id": user_id}