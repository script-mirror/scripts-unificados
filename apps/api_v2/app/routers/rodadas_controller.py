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
async def get_rodadas(
    dt:Optional[datetime.date] = None,
    no_cache:Optional[bool] = True,
    atualizar:Optional[bool] = False
    ):
    if no_cache:
        return rodadas_crud.CadastroRodadas.get_rodadas_por_dt(dt)
    return cache.get_cached(rodadas_crud.CadastroRodadas.get_rodadas_por_dt, dt, atualizar=atualizar)

@router.get('/chuva/previsao/{granularidade}', tags=['Rodadas'])
async def get_chuva_previsao_por_id_data_entre_granularidade(
    id:int,
    dt_inicio:datetime.date,
    dt_fim:datetime.date,
    granularidade:GranularidadeEnum,
    no_cache:Optional[bool] = False,
    atualizar:Optional[bool] = False
    ):
    
    return rodadas_crud.Chuva.get_chuva_por_id_data_entre_granularidade(id, dt_inicio, dt_fim, granularidade.name, no_cache, atualizar)

@router.get('/subbacias', tags=['Rodadas'])
async def get_subbacias():
    return rodadas_crud.Subbacia.get_subbacia()

@router.post('/chuva/previsao/pesquisa/{granularidade}', tags=['Rodadas'], response_model=List[ChuvaPrevisaoResposta])
async def get_previsao_chuva_modelos_combinados(
    previsao:List[PesquisaPrevisaoChuva],
    granularidade:GranularidadeEnum,
    no_cache:Optional[bool] = False,
    atualizar:Optional[bool] = False
    ):
    return rodadas_crud.Chuva.get_previsao_chuva_modelos_combinados(previsao, granularidade.name, no_cache, atualizar)

@router.post('/chuva/previsao/modelos', tags=['Rodadas'])
async def post_chuva_modelo_combinados(
    chuva_prev: List[ChuvaPrevisaoCriacao],
):  
    return rodadas_crud.Chuva.post_chuva_modelo_combinados(chuva_prev)

@router.post('/chuva/previsao/membros', tags=['Rodadas'])
async def post_chuva_membros(
    chuva_prev: List[ChuvaPrevisaoCriacaoMembro],
):  
    return rodadas_crud.ChuvaMembro.post_chuva_membro(chuva_prev)


@router.post('/smap', tags=['Rodadas'])
async def post_smap(
    rodada:RodadaSmap
):  
    return rodadas_crud.Smap.post_rodada_smap(rodada)

@router.post('/chuva/observada', tags=['Rodadas'])
async def post_chuva_observada(
    chuva_obs:List[ChuvaObsReq]
):
    return rodadas_crud.ChuvaObs.post_chuva_obs(chuva_obs)

# @router.get("/protected-endpoint")
# async def protected_endpoint(session_data: dict = Depends(auth_dependency)):
#     user_id = session_data["user_id"]
#     return {"message": "User is authenticated", "user_id": user_id}