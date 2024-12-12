from sys import path
import pdb
import sqlalchemy as sa
import pandas as pd
import numpy as np
import datetime
from typing import List, Optional
from app.schemas import WeolSemanalSchema, PatamaresDecompSchema
import sqlalchemy as db
from app.database.wx_dbClass import db_mysql_master
from ..utils.logger import logging
logger = logging.getLogger(__name__)


prod = True
__DB__ = db_mysql_master('db_decks')

class WeolSemanal:
    tb:db.Table = __DB__.getSchema('tb_dc_weol_semanal')
    @staticmethod
    def create(body: List[WeolSemanalSchema]):
        body_dict = [x.model_dump() for x in body]
        delete_dates = list(set([x['data_produto'] for x in body_dict]))
        for date in delete_dates:
            WeolSemanal.delete_by_product_date(date)
        query = db.insert(WeolSemanal.tb).values(body_dict)
        rows = __DB__.db_execute(query, commit=prod).rowcount
        logger.info(f"{rows} linhas adicionadas na tb_dc_weol_semanal")
        return None
    
    @staticmethod
    def delete_by_product_date(date:datetime.date):
        query = db.delete(WeolSemanal.tb).where(WeolSemanal.tb.c.data_produto == date)
        rows = __DB__.db_execute(query, commit=prod).rowcount
        logger.info(f"{rows} linhas deletadas da tb_dc_weol_semanal")
        return None
    
    @staticmethod
    def get_by_product_date(date:datetime.date):
        query = db.select(WeolSemanal.tb).where(WeolSemanal.tb.c.data_produto == date)
        result = __DB__.db_execute(query).fetchall()
        result = pd.DataFrame(result, columns=['id', 'inicio_semana', 'final_semana', 'data_produto', 'submercado', 'patamar', 'valor', 'rv_atual', 'mes_eletrico'])
        return result.to_dict('records')
    
    @staticmethod
    def get_all():
        query = db.select(WeolSemanal.tb)
        result = __DB__.db_execute(query).fetchall()
        result = pd.DataFrame(result, columns=['id', 'inicio_semana', 'final_semana', 'data_produto', 'submercado', 'patamar', 'valor', 'rv_atual', 'mes_eletrico'])
        result = result.to_dict('records')
        return result
    
    @staticmethod
    def get_by_product_date_start_week_year_month_rv(data_produto:datetime.date, mes_eletrico:int, ano:int, rv:int):
        query = db.select(
            WeolSemanal.tb
            ).where(db.and_(
                WeolSemanal.tb.c.data_produto == data_produto,
                WeolSemanal.tb.c.mes_eletrico == mes_eletrico,
                db.func.year(WeolSemanal.tb.c.inicio_semana) == ano,
                WeolSemanal.tb.c.rv_atual == rv
            ))
        result = __DB__.db_execute(query).fetchall()
        result = pd.DataFrame(result, columns=['id', 'inicio_semana', 'final_semana', 'data_produto', 'submercado', 'patamar', 'valor', 'rv_atual', 'mes_eletrico'])
        return result.to_dict('records')

    @staticmethod
    def get_by_product_start_week_date_product_date(inicio_semana:datetime.date, data_produto:datetime.date):
        query = db.select(
            WeolSemanal.tb
            ).where(db.and_(
                WeolSemanal.tb.c.data_produto == data_produto,
                WeolSemanal.tb.c.inicio_semana >= inicio_semana
            ))
        result = __DB__.db_execute(query).fetchall()
        result = pd.DataFrame(result, columns=['id', 'inicio_semana', 'final_semana', 'data_produto', 'submercado', 'patamar', 'valor', 'rv_atual', 'mes_eletrico'])
        return result.to_dict('records')


class Patamares:
    tb:db.Table = __DB__.getSchema('tb_patamar_decomp')
    @staticmethod
    def create(body: List[PatamaresDecompSchema]):
        body_dict = [x.model_dump() for x in body]
        dates = list(set([x['inicio'] for x in body_dict]))
        dates.sort()
        Patamares.delete_by_start_date_between(dates[0].date(), dates[-1].date())
        query = db.insert(Patamares.tb).values(body_dict)
        rows = __DB__.db_execute(query, commit=prod).rowcount
        logger.info(f"{rows} linhas adicionadas na tb_patamar_decomp")
        return None

    @staticmethod
    def delete_by_start_date(date:datetime.date):
        query = db.delete(Patamares.tb).where(db.func.date(Patamares.tb.c.inicio) == date)
        rows = __DB__.db_execute(query, commit=prod).rowcount
        logger.info(f"{rows} linhas deletadas da tb_patamar_decomp")
        return None
    
    @staticmethod
    def delete_by_start_date_between(start:datetime.date, end:datetime.date):
        query = db.delete(Patamares.tb).where(db.func.date(Patamares.tb.c.inicio).between(start, end))
        rows = __DB__.db_execute(query, commit=prod).rowcount
        logger.info(f"{rows} linhas deletadas da tb_patamar_decomp")
        return None
    
    