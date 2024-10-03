from sys import path
import pdb
import sqlalchemy as db
import pandas as pd
import numpy as np
import datetime
from typing import Optional, List
from fastapi import HTTPException

path.insert(1,"/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/api_v2")

from app.schemas.chuvaprevisao import ChuvaPrevisaoCriacao, ChuvaPrevisaoCriacaoMembro
from app.schemas import PesquisaPrevisaoChuva, RodadaSmap, RodadaCriacao, TipoRodadaEnum, MembrosModeloSchema

from app.utils import cache
from app.crud import ons_crud
path.insert(2,"/WX2TB/Documentos/fontes/PMO/scripts_unificados")
from apps.smap.libs.SmapTools import trigger_dag_SMAP, get_dag_smap_run_status
from bibliotecas.wx_dbClass import db_mysql_master
from bibliotecas.wx_emailSender import WxEmail

__DB__ = db_mysql_master('db_rodadas')
class CadastroRodadas:
    tb:db.Table = __DB__.getSchema('tb_cadastro_rodadas')

    @staticmethod
    def get_rodadas_por_dt(dt:datetime.date) -> List[dict]:
        __DB__.connect()
        dt = datetime.date.today() if dt == None else dt

        query = db.select(
        CadastroRodadas.tb.c['id'],
        CadastroRodadas.tb.c['id_chuva'],
        CadastroRodadas.tb.c['id_smap'],
        CadastroRodadas.tb.c['id_previvaz'],
        CadastroRodadas.tb.c['id_prospec'],
        CadastroRodadas.tb.c['dt_rodada'],
        CadastroRodadas.tb.c['hr_rodada'],
        CadastroRodadas.tb.c['str_modelo'],
        CadastroRodadas.tb.c['fl_preliminar'],
        CadastroRodadas.tb.c['fl_pdp'],
        CadastroRodadas.tb.c['fl_psat'],
        CadastroRodadas.tb.c['fl_estudo'],
        CadastroRodadas.tb.c['dt_revisao']
        ).where(
            CadastroRodadas.tb.c['dt_rodada'] == dt
        )
        result = __DB__.db_execute(query).fetchall()
        df = pd.DataFrame(result, columns=['id','id_chuva','id_smap','id_previvaz','id_prospec','dt_rodada','hr_rodada','str_modelo','fl_preliminar','fl_pdp','fl_psat','fl_estudo','dt_revisao'])
        df['dt_rodada'] = df['dt_rodada'].astype('datetime64[ns]').dt.strftime('%Y-%m-%d')
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        df['id_smap'] = df['id_smap'].astype(pd.Int64Dtype())
        df['id_previvaz'] = df['id_previvaz'].astype(pd.Int64Dtype())
        df['id_prospec'] = df['id_prospec'].astype(pd.Int64Dtype())
        return df.to_dict('records')
    

    @staticmethod
    def get_rodadas_por_dt_hr_nome(dt:datetime.datetime, nome:str) -> List[dict]:
        __DB__.connect()
        dt = datetime.datetime(datetime.date.today().year, datetime.date.today().month, datetime.date.today().day, 0) if dt == None else dt

        query = db.select(
        CadastroRodadas.tb.c['id'],
        CadastroRodadas.tb.c['id_chuva'],
        CadastroRodadas.tb.c['id_smap'],
        CadastroRodadas.tb.c['id_previvaz'],
        CadastroRodadas.tb.c['id_prospec'],
        CadastroRodadas.tb.c['dt_rodada'],
        CadastroRodadas.tb.c['hr_rodada'],
        CadastroRodadas.tb.c['str_modelo'],
        CadastroRodadas.tb.c['fl_preliminar'],
        CadastroRodadas.tb.c['fl_pdp'],
        CadastroRodadas.tb.c['fl_psat'],
        CadastroRodadas.tb.c['fl_estudo'],
        CadastroRodadas.tb.c['dt_revisao']
        ).where(db.and_(
            CadastroRodadas.tb.c['dt_rodada'] == dt.date(),
            CadastroRodadas.tb.c['hr_rodada'] == dt.hour,
            CadastroRodadas.tb.c['str_modelo'] == nome            
        
        )).order_by(CadastroRodadas.tb.c['fl_preliminar'], CadastroRodadas.tb.c['fl_pdp'].desc(),CadastroRodadas.tb.c['fl_psat'].desc())
        
        result = __DB__.db_execute(query).fetchall()
        df = pd.DataFrame(result, columns=['id','id_chuva','id_smap','id_previvaz','id_prospec','dt_rodada','hr_rodada','str_modelo','fl_preliminar','fl_pdp','fl_psat','fl_estudo','dt_revisao'])
        if df.empty:
            raise HTTPException(404, {"erro":f"Nenhum modelo encontrado com nome {nome} e data de rodada {dt}"})

        df['dt_rodada'] = df['dt_rodada'].astype('datetime64[ns]').dt.strftime('%Y-%m-%d')
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        df['id_smap'] = df['id_smap'].astype(pd.Int64Dtype())
        df['id_previvaz'] = df['id_previvaz'].astype(pd.Int64Dtype())
        df['id_prospec'] = df['id_prospec'].astype(pd.Int64Dtype())
        return df.to_dict('records')

    @staticmethod
    def info_rodadas(modelos:list, columns_data:list=[]): 
        __DB__.connect()
        if not columns_data: selected_columns = [column_name for column_name in CadastroRodadas.tb.columns] 
        else:
            conditional_columns = [
                CadastroRodadas.tb.c['id_chuva'] if 'id_chuva' in columns_data else None,
                CadastroRodadas.tb.c['id_smap'] if 'id_smap' in columns_data else None,
            ]
            base_columns = [
                CadastroRodadas.tb.c['id'],
                CadastroRodadas.tb.c['str_modelo'],
                CadastroRodadas.tb.c['hr_rodada'],
                CadastroRodadas.tb.c['dt_rodada']
            ]

            selected_columns = base_columns + [col for col in conditional_columns if col is not None]

        order_priority = db.case(
                
                ((CadastroRodadas.tb.c['fl_pdp'] == 1) & (CadastroRodadas.tb.c['fl_preliminar'] == 0), 1),
                ((CadastroRodadas.tb.c['fl_psat'] == 1) & (CadastroRodadas.tb.c['fl_preliminar'] == 0), 2),
                ((CadastroRodadas.tb.c['fl_pdp'] == 0) & (CadastroRodadas.tb.c['fl_psat'] == 0) & (CadastroRodadas.tb.c['fl_preliminar'] == 0), 3),
                (CadastroRodadas.tb.c['fl_preliminar'] == 1, 4),
            
                else_=None
            ).label('order_priority')
        
        subquery_cadastro_rodadas = db.select(selected_columns+ [order_priority]).where(
            db.tuple_(
                CadastroRodadas.tb.c['str_modelo'],
                CadastroRodadas.tb.c['hr_rodada'],
                CadastroRodadas.tb.c['dt_rodada']
            ).in_(modelos)\
            )\
            .order_by(db.desc(CadastroRodadas.tb.c['dt_rodada']),db.asc(order_priority))

        rodadas_values = __DB__.db_execute(subquery_cadastro_rodadas).fetchall()
        return pd.DataFrame(rodadas_values, columns=[column_name.name for column_name in selected_columns]+['priority'])
    
    @staticmethod
    def get_last_column_id(column_name:str):
        __DB__.connect()
        query_get_max_id_column = db.select(db.func.max(CadastroRodadas.tb.c[column_name]))
        max_id = __DB__.db_execute(query_get_max_id_column).scalar()
        return max_id
    
    @staticmethod
    def inserir_cadastro_rodadas(rodadas_values:list):
        __DB__.connect()
        query_update = CadastroRodadas.tb.insert().values(rodadas_values)
        n_value = __DB__.db_execute(query_update).rowcount

        print(f"{n_value} Linhas inseridas na tb_cadastro_rodadas")
class Chuva:
    tb:db.Table = __DB__.getSchema('tb_chuva')

    @staticmethod
    def get_chuva_por_id_subbacia(id_chuva:int):
        __DB__.connect()
        query = db.select(
            CadastroRodadas.tb.c['str_modelo'],
            CadastroRodadas.tb.c['dt_rodada'],
            CadastroRodadas.tb.c['hr_rodada'],
            Chuva.tb.c['cd_subbacia'],
            Chuva.tb.c['dt_prevista'],
            Chuva.tb.c['vl_chuva']
            ).where(db.and_(
                Chuva.tb.c['id'] == id_chuva
                )
            ).join(
                CadastroRodadas.tb, CadastroRodadas.tb.c['id_chuva'] == Chuva.tb.c['id']
        )
        result = __DB__.db_execute(query).fetchall()
        df = pd.DataFrame(result, columns=['modelo', 'dt_rodada', 'hr_rodada', 'id', 'dt_prevista', 'vl_chuva'])
        df['dia_semana'] = df['dt_prevista'].astype('datetime64[ns]').dt.strftime('%A')
        df['dt_prevista'] = df['dt_prevista'].astype('datetime64[ns]').dt.strftime('%Y-%m-%d')
        df = df.drop_duplicates()
        df = df.sort_values('dt_prevista')
        
        dfs = [g for _,g in df.groupby((pd.to_datetime(df['dt_prevista']) + pd.to_timedelta(1, unit='D')).dt.to_period('W'))]
        for i, item in enumerate(dfs):
            item['semana'] = i+1
            dfs[i] = item
            pass
        df = pd.concat(dfs)
        return df.to_dict('records')
            
    @staticmethod
    def get_chuva_por_id_data_entre_granularidade(
        id_chuva:int,
        dt_inicio:datetime.date,
        dt_fim:datetime.date,
        granularidade:str,
        no_cache: Optional[bool] = False,
        atualizar:Optional[bool] = False):
        __DB__.connect()
        
        if no_cache:
            df = pd.DataFrame(Chuva.get_chuva_por_id_subbacia(id_chuva))
        else:
            df = pd.DataFrame(cache.get_cached(Chuva.get_chuva_por_id_subbacia, id_chuva, atualizar=atualizar))
        if df.empty:
            return df
        df = df[(df['dt_prevista'] >= dt_inicio.strftime('%Y-%m-%d')) & (df['dt_prevista'] <= dt_fim.strftime('%Y-%m-%d'))]
        df = df.sort_values(['dt_prevista', 'id'])
        if granularidade == 'subbacia':
            return df.to_dict('records')
        df_subbacia = pd.DataFrame(Subbacia.get_subbacia())
        merged = df.merge(df_subbacia[['id', 'nome_bacia', 'nome_submercado']], on='id')
        if granularidade == 'bacia':
            df_bacia = pd.DataFrame(ons_crud.tb_bacias.get_bacias('tb_chuva'))
            merged['nome_bacia'] = merged['nome_bacia'].str.upper()
            merged = merged.replace(
                'STA. MARIA DA VITÓRIA', 'SANTA MARIA VITORIA').replace(
                'SÃO FRANCI', 'SÃO FRANCISCO').replace(
                'JEQUITINHO', 'JEQUITINHONHA').replace(
                'PARANAPANE', 'PARANAPANEMA'
                )
            grouped = merged.groupby(['nome_bacia', 'dt_prevista', 'dia_semana', 'semana', 'hr_rodada', 'dt_rodada', 'modelo']).agg({'vl_chuva':'mean'}).reset_index()
            grouped = grouped.rename(columns={'nome_bacia':'nome'}).merge(df_bacia[['id', 'nome']], on='nome')
            print(grouped.drop(columns=['nome']).to_dict('records'))
            return grouped.drop(columns=['nome']).to_dict('records')

        if granularidade == 'submercado':
            df_submercado = pd.DataFrame(ons_crud.tb_submercado.get_submercados())
            df_submercado['nome'] = df_submercado['nome'].str.capitalize()
            grouped = merged.groupby(['nome_submercado', 'dt_prevista', 'dia_semana', 'semana', 'hr_rodada', 'dt_rodada', 'modelo']).agg({'vl_chuva':'mean'}).reset_index()
            grouped = grouped.rename(columns={'nome_submercado':'nome'}).merge(df_submercado[['id', 'nome']], on='nome')
            return grouped.drop(columns=['nome']).to_dict('records')
    @staticmethod
    def get_previsao_chuva_modelos_combinados(
        query_obj: List[PesquisaPrevisaoChuva],
        granularidade:str,
        no_cache: Optional[bool] = False,
        atualizar:Optional[bool] = False):
        __DB__.connect()
        
        df = pd.DataFrame()
        for q in query_obj:
            df = pd.concat([df, pd.DataFrame(Chuva.get_chuva_por_id_data_entre_granularidade(q.id, q.dt_inicio, q.dt_fim, granularidade, no_cache, atualizar))])
        return df.to_dict('records')
    
    @staticmethod
    def post_chuva_modelo_combinados(chuva_prev:List[ChuvaPrevisaoCriacao]) -> None:
        __DB__.connect()
        prevs:List[dict] = []
        for prev in chuva_prev:
            prevs.append(prev.model_dump())
        modelo = (prevs[0]['modelo'], 0, prevs[0]['dt_rodada'])
        df = pd.DataFrame(prevs)
        df['cenario'] = f'{modelo[0]}_{modelo[1]}_{modelo[2]}'
        
        Chuva.inserir_chuva_modelos([modelo], df)
    
        return None
    
    
    @staticmethod
    def inserir_prev_chuva(df_prev_vazao_out:pd.DataFrame):
        __DB__.connect()

        values_chuva = df_prev_vazao_out[['id_chuva','cd_subbacia','dt_prevista','vl_chuva']].values.tolist()
        ids_chuva = df_prev_vazao_out['id_chuva'].unique() 
        query_delete = Chuva.tb.delete().where(Chuva.tb.c['id'].in_(ids_chuva))
        n_value = __DB__.db_execute(query_delete).rowcount
        print(f"{n_value} Linhas deletadas na Chuva")

        query_insert = Chuva.tb.insert().values(values_chuva)
        n_value = __DB__.db_execute(query_insert).rowcount
        print(f"{n_value} Linhas inseridas na Chuva")
        
    @staticmethod
    def inserir_chuva_modelos(modelos:list,df_prev_chuva_out:pd.DataFrame):
        __DB__.connect()
        df_info_subbacias = Subbacia.info_subbacias()
        df_chuva_final = pd.merge(df_info_subbacias[['cd_subbacia' ,'vl_lon'  ,'vl_lat']], df_prev_chuva_out)
        df_prev_chuva = df_chuva_final.drop(['vl_lat','vl_lon'],axis=1)
        # df_prev_chuva = df_chuva_final.melt(id_vars=['cd_subbacia','cenario', 'dt_prevista'] , value_vars='vl_chuva')

        df_info_rodadas = CadastroRodadas.info_rodadas(modelos)
        
        new_chuva_id = CadastroRodadas.get_last_column_id('id_chuva') + 1
        insert_cadastro_values = []
        for cenario in df_prev_chuva['cenario'].unique():

            str_modelo ,hr_rodada, dt_rodada = cenario.split('_')

            mask_id_chuva = \
            (df_info_rodadas['str_modelo'].str.upper() == str_modelo.upper()) & \
            (pd.to_datetime(df_info_rodadas['dt_rodada']).dt.strftime('%Y-%m-%d') == dt_rodada) & \
            (df_info_rodadas['hr_rodada'] == int(hr_rodada))

            if df_info_rodadas[mask_id_chuva].empty:
                insert_cadastro_values += [None, new_chuva_id, None, None,None,dt_rodada,int(hr_rodada),str_modelo,None,None,None,None,None],
                df_prev_chuva.loc[df_prev_chuva['cenario']== cenario,'id_chuva'] = new_chuva_id
                new_chuva_id += 1
            else:
                df_info_rodadas['id_chuva'] = df_info_rodadas['id_chuva'].astype(str).str.replace('nan','None')
                id_chuva = df_info_rodadas[mask_id_chuva]['id_chuva'].unique()[0]
                print(f"    cenario: {cenario} || modelo: {str_modelo} -> rodada ja esta cadastrada com id_chuva: {id_chuva}")
                
                if id_chuva !='None':
                    df_prev_chuva.loc[df_prev_chuva['cenario']== cenario,'id_chuva'] = id_chuva
                else:
                    print(f"    cenario: {cenario} || modelo: {str_modelo} -> rodada ja esta cadastrada porem sem id_chuva, será cadastrado com id_chuva: {new_chuva_id}")
                    df_prev_chuva.loc[df_prev_chuva['cenario']== cenario,'id_chuva'] = new_chuva_id
                    new_chuva_id +=1

            if insert_cadastro_values: CadastroRodadas.inserir_cadastro_rodadas(insert_cadastro_values)
            Chuva.inserir_prev_chuva(df_prev_chuva.round(2))


    
    
class ChuvaMembro:
    tb:db.Table = __DB__.getSchema('tb_chuva_membro')
    @staticmethod
    def post_chuva_membro(chuva_prev:List[ChuvaPrevisaoCriacaoMembro]) -> None:
        __DB__.connect()
        records = [obj.model_dump() for obj in chuva_prev]
        df = pd.DataFrame(records)
        dt_hr_rodada:datetime.datetime = df[["dt_hr_rodada"]].drop_duplicates().to_dict("records")[0]["dt_hr_rodada"].to_pydatetime()
        modelo:str = df[["modelo"]].drop_duplicates().to_dict("records")[0]["modelo"]
        df = df.rename(columns={"membro":"nome"})
        df_membro_modelo = df[["nome", "dt_hr_rodada"]].drop_duplicates()
        df_membro_modelo["id_rodada"] = CadastroRodadas.get_rodadas_por_dt_hr_nome(dt_hr_rodada, modelo)[0]["id"]
        df_membro_modelo = pd.DataFrame(MembrosModelo.inserir(df_membro_modelo.to_dict("records")))
        
        df = df.merge(df_membro_modelo).rename(columns={"id":"id_membro_modelo"})[["id_membro_modelo", "vl_chuva", "cd_subbacia", "dt_prevista"]]
        ChuvaMembro.inserir(df.to_dict("records"))
        return None
    
    @staticmethod
    def inserir(body:List[dict]):
        __DB__.connect()
        id_membro_modelo = []
        cd_subbacia = []
        dt_prevista = []
        for membro in body:
            id_membro_modelo.append(membro["id_membro_modelo"])
            cd_subbacia.append(membro["cd_subbacia"])
            dt_prevista.append(membro["dt_prevista"])
            
        search_params = (ChuvaMembro.tb.c["id_membro_modelo"].in_(id_membro_modelo),
                ChuvaMembro.tb.c["cd_subbacia"].in_(cd_subbacia),
                ChuvaMembro.tb.c["dt_prevista"].in_(dt_prevista))
        
        q_delete = ChuvaMembro.tb.delete().where(db.and_(
            *search_params
        ))
        __DB__.db_execute(q_delete)
        
        query = ChuvaMembro.tb.insert(body)
        linhas = __DB__.db_execute(query).rowcount
        print(f"{linhas} linhas inseridas")

    pass
class Subbacia:
    tb:db.Table = __DB__.getSchema('tb_subbacia')
    @staticmethod
    def get_subbacia():
        __DB__.connect()
        query = db.select(
            Subbacia.tb.c['cd_subbacia'],
            Subbacia.tb.c['txt_nome_subbacia'],
            Subbacia.tb.c['txt_submercado'],
            Subbacia.tb.c['txt_bacia'],
            Subbacia.tb.c['vl_lat'],
            Subbacia.tb.c['vl_lon'],
            Subbacia.tb.c['txt_nome_smap'],
            Subbacia.tb.c['txt_pasta_contorno'],
            Subbacia.tb.c['cd_bacia_mlt'],
        )
        result = __DB__.db_execute(query)
        df = pd.DataFrame(result, columns=['id', 'nome', 'nome_submercado', 'nome_bacia', 'vl_lat', 'vl_lon', 'nome_smap', 'pasta_contorno', 'cd_bacia_mlt'])
        df = df.sort_values('id')
        df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
        return df.to_dict('records')
    
    @staticmethod
    def get_bacias():
        __DB__.connect()
        query = db.select(
            db.distinct(Subbacia.tb.c['txt_bacia'])
        )
        result = __DB__.db_execute(query)
        df = pd.DataFrame(result, '')
        
    @staticmethod
    def info_subbacias():
        __DB__.connect()
        query = db.select(Subbacia.tb.c['cd_subbacia'],Subbacia.tb.c['vl_lon'],Subbacia.tb.c['vl_lat'],Subbacia.tb.c['txt_nome_subbacia'])
        answer_tb_subbacia = __DB__.db_execute(query)
        
        df_subbac = pd.DataFrame(answer_tb_subbacia, columns=['cd_subbacia','vl_lon','vl_lat','nome'])
        df_subbac['nome'] = df_subbac['nome'].str.lower()
        return df_subbac
        
        
class Smap:
    @staticmethod
    def post_rodada_smap(rodada:RodadaSmap):
        __DB__.connect()
        momento_req:datetime.datetime = datetime.datetime.now()
        
        trigger_dag_SMAP(rodada.dt_rodada, [rodada.str_modelo], rodada.hr_rodada, momento_req)
        response:dict = get_dag_smap_run_status(rodada.str_modelo, momento_req)
        
        falhou = response['state'] == 'failed'
        
        Smap.email_smap(falhou, response['end_datetime'], response["url"])
        if falhou:
            raise HTTPException(400, f'{response}')
        return response
    
    @staticmethod
    def email_smap(sucesso: bool, momento:datetime.datetime, dag_url:str):
        __DB__.connect()
        email = WxEmail()
        
        status, cor_status = ("Concluido", "#88B04B") if sucesso else ("Falha", "#b04b4b")
        
        style = f"""<style> body {{ font-family: Arial, Helvetica, sans-serif; text-align: center; padding: 40px 0; margin: 0; background: #EBF0F5; }} h1 {{ color: {cor_status}; font-weight: 900; font-size: 40px; margin-bottom: 10px; }} p {{ color: #404F5E; font-size: 20px; margin: 0; }} .card {{ background: white; padding: 30px; border-radius: 4px; box-shadow: 0 2px 3px #C8D0D8; display: inline-block; margin: 0 auto; }}</style>"""
        html = f"""<html><head></head>{style}<body> <div class="card"> <h1>{status}</h1> <p>Fim da execução do Airflow {momento.strftime("%d/%m/%Y %H:%M:%S")}<br /><a href="{dag_url}">DAG Airflow</a></p> </div></body></html>"""
        
        email.sendEmail(texto=html, assunto="SMAP - Gera Chuva", send_to=["arthur.moraes@raizen.com"])

class MembrosModelo:
    tb:db.Table = __DB__.getSchema('tb_membro_modelo')
    
    @staticmethod
    def inserir(body:List[dict]) -> List[dict]:
        __DB__.connect()
        dt_hr_rodada = []
        nome = []
        id_rodada = []
        for membro in body:
            dt_hr_rodada.append(membro["dt_hr_rodada"])
            nome.append(membro["nome"])
            id_rodada.append(membro["id_rodada"])
            
        search_params = (MembrosModelo.tb.c["dt_hr_rodada"].in_(dt_hr_rodada),
                MembrosModelo.tb.c["nome"].in_(nome),
                MembrosModelo.tb.c["id_rodada"].in_(id_rodada))
        
        q_delete = MembrosModelo.tb.delete().where(db.and_(
            *search_params
        ))
        __DB__.db_execute(q_delete)

        q_insert = MembrosModelo.tb.insert(body)
        __DB__.db_execute(q_insert)
        q_select = db.select(
            MembrosModelo.tb.c["id"],
            MembrosModelo.tb.c["dt_hr_rodada"],
            MembrosModelo.tb.c["nome"],
            MembrosModelo.tb.c["id_rodada"]
            ).where(db.and_(
                *search_params
            )
                    )
        result = __DB__.db_execute(q_select).fetchall()
        df = pd.DataFrame(result, columns=["id", "dt_hr_rodada", "nome", "id_rodada"])
        return df.to_dict("records")
        
if __name__ == '__main__':
    Smap.email_smap(False, datetime.datetime.now(), "https://tradingenergiarz.com/airflow/dags/ONS_DADOS_ABERTOS/grid?tab=graph&dag_run_id=scheduled__2024-09-17T20%3A00%3A00%2B00%3A00")
    # CadastroRodadas.get_rodadas_por_dt(datetime.date.today())
    # teste = MembrosModeloSchema()
    
    # MembrosModelo.inserir()
    # Chuva.get_chuva_por_id_data_entre_granularidade(11438, datetime.date.today(),datetime.date.today()+datetime.timedelta(days=15), 'bacia')
    pass