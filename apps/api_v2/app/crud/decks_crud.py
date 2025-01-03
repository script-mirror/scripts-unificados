import pdb
import datetime
import numpy as np
import numpy as np
import pandas as pd
from sys import path
import sqlalchemy as sa
import sqlalchemy as db
from typing import List, Optional
from fastapi import HTTPException
from ..utils.logger import logging
from app.database.wx_dbClass import db_mysql_master
from app.schemas import WeolSemanalSchema, PatamaresDecompSchema
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
    
    @staticmethod
    def get_by_product_date(data_produto:datetime.date):
        query = db.select(
            WeolSemanal.tb
            ).where(WeolSemanal.tb.c.data_produto == data_produto)
        result = __DB__.db_execute(query).fetchall()
        result = pd.DataFrame(result, columns=['id', 'inicioSemana', 'finalSemana', 'dataProduto', 'submercado', 'patamar', 'valor', 'rvAtual', 'mesEletrico'])
        if result.empty:
            raise HTTPException(status_code=404, detail=f"Produto da data {data_produto} não encontrado")
        return result.to_dict('records')
    
    @staticmethod
    def get_by_product_date_between(start_date:datetime.date, end_date:datetime.date):
        query = db.select(
            WeolSemanal.tb
            ).where(WeolSemanal.tb.c.data_produto.between(start_date, end_date))
        result = __DB__.db_execute(query).fetchall()
        result = pd.DataFrame(result, columns=['id', 'inicioSemana', 'finalSemana', 'dataProduto', 'submercado', 'patamar', 'valor', 'rvAtual', 'mesEletrico'])
        if result.empty:
            raise HTTPException(status_code=404, detail=f"Produtos entre as datas {start_date} e {end_date} não encontrados")
        return result.to_dict('records')

    @staticmethod
    def get_weighted_avg_by_product_date(start_date:datetime.date, end_date:datetime.date):
        df = pd.DataFrame(WeolSemanal.get_by_product_date_between(start_date, end_date))
        
        df_horas_por_patamar = pd.DataFrame(Patamares.get_horas_por_patamar_por_inicio_semana_data(df['inicioSemana'].min(), df['finalSemana'].max()))
        merged_df = pd.merge(df, df_horas_por_patamar, on=['inicioSemana', 'patamar'], how='left')
        
        df_weighted = merged_df[['dataProduto', 'inicioSemana', 'qtdHoras']][merged_df['submercado'] == "S"]
        df_weighted = df_weighted.groupby(['dataProduto', 'inicioSemana']).agg({'qtdHoras':'sum'}).rename({'qtdHoras':'totalHoras'}, axis=1)

        
        df_group  = merged_df.groupby(['dataProduto', 'inicioSemana', 'patamar']).agg({'valor':'sum', 'qtdHoras':'max', 'patamar':'first'})
        
        df_group = pd.merge(df_group, df_weighted, on=['dataProduto', 'inicioSemana'], how='left')
        df_group['mediaPonderada'] = df_group['valor'] * df_group['qtdHoras']
        df_group['mediaPonderada'] = df_group['mediaPonderada'] / df_group['totalHoras']
        df_group = df_group.reset_index()
        
        df_group.drop(columns=['patamar', 'qtdHoras'], inplace=True)
        
        df_group = df_group.groupby(['dataProduto', 'inicioSemana']).agg({'mediaPonderada':'sum', 'totalHoras':'first'}).reset_index()
        df_group['mediaPonderada'] = df_group['mediaPonderada'] / 3
        df = df_group.pivot_table(index=['inicioSemana', 'totalHoras'], columns='dataProduto', values='mediaPonderada', aggfunc='mean').reset_index()
        df.drop(columns=['totalHoras'], inplace=True)
        return df.to_dict('records')

    @staticmethod
    def get_weighted_avg_table_by_product_date(data_produto:datetime.date, quantidade_produtos:int):
        df = pd.DataFrame(WeolSemanal.get_weighted_avg_by_product_date(data_produto - datetime.timedelta(days=quantidade_produtos), data_produto))
        mean_values = df.mean(numeric_only=True)
        mean_row = {'inicioSemana': 'media'}
        mean_row.update(mean_values)
        mean_df = pd.DataFrame([mean_row])

        df = pd.concat([df, mean_df], ignore_index=True)
        
        df.rename(columns={'inicioSemana': 'Semana'}, inplace=True)
        df.columns = [df.columns[0], 'Eolica Oficial'] + [x.strftime('WEOL %d/%m') for x in df.columns[2:]]
        
        html:str = '''<style> body { font-family: sans-serif; } th, td { padding: 4px; text-align: center; border: 0.5px solid; } table { border-collapse: collapse; } thead, .gray { background-color: #d9d9d9; border: 1px solid; } .none{ background-color: #e6e6e6; } tbody *{ border: none; } tbody{ border: 1px solid; } .n1{background-color: #63be7b;} .n2{background-color: #aad380;} .n3{background-color: #efe784;} .n4{background-color: #fcbc7b;} .n5{background-color: #fba777;} .n6{background-color: #f8696b;}</style><table> <thead> <tr>'''
        for col in df.columns:
            html += f'<th>{col}</th>'
        html += ' </tr></thead><tbody>'
            
        
        for i, row in df.iterrows():
            html += '<tr>'
            for j, col in enumerate(row):
                if j == 0:
                    html += f'<td class="gray">{col}</td>'
                elif j == 1:
                    html += f'<td class="n3">{col:.2f}</td>'
                else:
                    if bool(np.isnan(col)):
                        html += f'<td class="none"></td>'
                        continue
                    difference_percent:float = col / row['Eolica Oficial']
                    if difference_percent >= 1.30:
                        html += f'<td class="n1">{col:.2f}</td>'
                    elif difference_percent > 1.10:
                        html += f'<td class="n2">{col:.2f}</td>'
                    elif difference_percent > 0.9:
                        html += f'<td class="n3">{col:.2f}</td>'
                    elif difference_percent > 0.8:
                        html += f'<td class="n4">{col:.2f}</td>'
                    elif difference_percent > 0.6:
                        html += f'<td class="n5">{col:.2f}</td>'
                    else:
                        html += f'<td class="n6">{col:.2f}</td>'
                        
            html += '</tr>'
        html += '</tbody></table>'
        return {"html" : html}
        # with open('/WX2TB/Documentos/fontes/sample.html', 'w') as f:
        #     f.write(html)


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
    
    @staticmethod
    def get_horas_por_patamar_por_inicio_semana_data(inicio_semana:datetime.date, fim_semana:datetime.date):
        query = db.select(
            db.func.count(),
            Patamares.tb.c["patamar"],
            db.func.min(db.func.date(Patamares.tb.c["inicio"]))
        ).where(
            db.func.date(db.func.date_sub(Patamares.tb.c["inicio"], db.text("interval 1 hour"))).between(inicio_semana, fim_semana)
        ).group_by(
            Patamares.tb.c["semana"],
            Patamares.tb.c["patamar"]
        )
        result = __DB__.db_execute(query).fetchall()
        result = pd.DataFrame(result, columns=["qtdHoras", "patamar","inicio"])
        result = result.sort_values(by=["inicio", "patamar"])
        
        for i in range(2, len(result), 3):
            result.at[i, 'inicio'] = result.at[i-1, 'inicio']
        result['patamar'][result['patamar'] == 'Pesada'] = 'pesado'
        result['patamar'][result['patamar'] == 'Média'] = 'medio'
        result['patamar'][result['patamar'] == 'Leve'] = 'leve'
        result = result.rename(columns={'inicio': 'inicioSemana'})
        return result.to_dict("records")
    
    
    
    