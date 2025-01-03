from sys import path

from fastapi import APIRouter
from typing import List, Optional
from app.crud import decks_crud
from app.utils import cache
from app.schemas import WeolSemanalSchema, PatamaresDecompSchema
import datetime


router = APIRouter(prefix="/decks")


@router.post("/weol",tags=["Decomp"])
def post_weol(
    body: List[WeolSemanalSchema]
):
    return decks_crud.WeolSemanal.create(body)
# @router.get("/weol",tags=["Decomp"])
# def get_weol_all():
#     return decks_crud.WeolSemanal.get_all()
@router.get("/weol",tags=["Decomp"])
def get_weol(
    data_produto: datetime.date
):
    return decks_crud.WeolSemanal.get_by_product_date(data_produto)

@router.get("/weol/product-date", tags=["Decomp"])
def get_by_product_date_start_week_year_month_rv(
    dataProduto: datetime.date,
    mesEletrico: int,
    ano: int,
    rv: int
):
    return decks_crud.WeolSemanal.get_by_product_date_start_week_year_month_rv(dataProduto, mesEletrico, ano, rv)

@router.get("/weol/start-week-date", tags=["Decomp"])
def get_weol_by_product_date_start_week(
    inicioSemana: datetime.date,
    dataProduto: datetime.date
):
    return decks_crud.WeolSemanal.get_by_product_start_week_date_product_date(inicioSemana, dataProduto)

@router.delete("/weol",tags=["Decomp"])
def delete_weol(
    data_produto: datetime.date
):
    return decks_crud.WeolSemanal.delete_by_product_date(data_produto)

@router.post("/patamares", tags=["Decomp"])
def post_patamares(
    body: List[PatamaresDecompSchema]
    
):
    return decks_crud.Patamares.create(body)

@router.delete("/patamares", tags=["Decomp"])
def delete_patamares(
    data_inicio: datetime.date
):
    return decks_crud.Patamares.delete_by_start_date(data_inicio)

@router.get("/patamares", tags=["Decomp"])
def get_weol_by_product_date_start_week_year_month_rv(
    inicioSemana: datetime.date,
    fimSemana: datetime.date
):
    return decks_crud.Patamares.get_horas_por_patamar_por_inicio_semana_data(inicioSemana, fimSemana)

@router.get("/patamares/weighted-average", tags=["Decomp"])
def get_weighted_avg_by_product_date(
    dataProduto: datetime.date
):
    return decks_crud.WeolSemanal.get_weighted_avg_by_product_date(dataProduto)


@router.get("/patamares/weighted-average/table", tags=["Decomp"])
def get_weighted_avg_table_by_product_date(
    dataProduto: datetime.date,
    quantidadeProdutos: int
):
    return decks_crud.WeolSemanal.get_weighted_avg_table_by_product_date(dataProduto, quantidadeProdutos)
