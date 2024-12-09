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

@router.post("/patamares", tags=["Decomp"])
def post_patamares(
    body: List[PatamaresDecompSchema]
    
):
    return decks_crud.Patamares.create(body)

