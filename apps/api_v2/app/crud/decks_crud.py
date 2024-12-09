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

prod = True
__DB__ = db_mysql_master('db_decks')

class WeolSemanal:
    tb:db.Table = __DB__.getSchema('tb_dc_weol_semanal')
    @staticmethod
    def create(body: List[WeolSemanalSchema]):
        body_dict = [x.model_dump() for x in body]
        
        query = db.insert(WeolSemanal.tb).values(body_dict)
        rows = __DB__.db_execute(query, commit=prod).rowcount
        print(f"{rows} linhas adicionadas na tb_dc_weol_semanal")
        return None

class Patamares:
    tb:db.Table = __DB__.getSchema('tb_patamar_decomp')
    @staticmethod
    def create(body: List[PatamaresDecompSchema]):
        body_dict = [x.model_dump() for x in body]
        
        query = db.insert(Patamares.tb).values(body_dict)
        rows = __DB__.db_execute(query, commit=prod).rowcount
        print(f"{rows} linhas adicionadas na tb_patamar_decomp")
        return None
    
    