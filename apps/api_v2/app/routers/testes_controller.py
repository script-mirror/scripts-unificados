from fastapi import APIRouter, HTTPException, Query
import datetime
from random import randint
from sys import path
path.insert(1,"/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/api_v2")


router = APIRouter(prefix='/testes')

@router.get('/', description='Descricao da rota')
async def endpoint_teste():
    return {'random':randint(1, 100000)}

@router.get('/depreciado', deprecated=True)
async def endpoint_velho():
    return {'nao funciona':500}

@router.post('/')
async def post_teste():
    return {'post':200}

@router.get('/teste/{id}')
async def path_param(id: int):
    return {'id': id}


@router.get('/data/{data}')
async def get_data(data:datetime.datetime):
    return {'data_informada': data}

@router.get('/teste')
async def query_param(inteiro: int = Query(1), string:str = Query('str', max_length=10), data:datetime.datetime = Query(datetime.datetime.now())):
    return {
        'int':inteiro,
        'string':string,
        'datetime':data,
        'teste': datetime.datetime.now()
    }
