from fastapi import APIRouter, HTTPException, Query, Cookie, Header
import datetime
from typing import Annotated
from random import randint
from sys import path



router = APIRouter(prefix='/testes')

@router.get('/', description='Descricao da rota', tags=['teste'])
async def endpoint_teste():
    return {'random':randint(1, 100000)}

@router.get('/depreciado', deprecated=True, tags=['teste'])
async def endpoint_velho():
    return {'nao funciona':500}

@router.post('/', tags=['teste'])
async def post_teste():
    return {'post':200}

@router.get('/teste/{id}', tags=['teste'])
async def path_param(id: int):
    return {'id': id}


@router.get('/data/{data}', tags=['teste'])
async def get_data(data:datetime.datetime):
    return {'data_informada': data}

@router.get('/teste', tags=['teste'])
async def query_param(inteiro: int = Query(1), string:str = Query('str', max_length=10), data:datetime.datetime = Query(datetime.datetime.now())):
    return {
        'int':inteiro,
        'string':string,
        'datetime':data,
        'teste': datetime.datetime.now()
    }

@router.get("/items/cookie", tags=['teste'])
async def read_items(ads_id: Annotated[str | None, Cookie()] = None):
    return {"ads_id": ads_id}

@router.get("/items/header", tags=['teste'])
async def read_items(user_agent: Annotated[str | None, Header()] = None):
    return {"User-Agent": user_agent}