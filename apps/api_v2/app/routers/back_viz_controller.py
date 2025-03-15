from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from app.crud import rodadas_crud

router = APIRouter(prefix='/visualizacao')

@router.get('/chuva', tags=['BackMiddle'])
def export_rain(id_chuva: int):
    return rodadas_crud.Chuva.export_rain(id_chuva)
