import sys
import pdb
import datetime
import pandas as pd
import sqlalchemy as db

sys.path.insert(1,"/WX2TB/Documentos/fontes")
from PMO.scripts_unificados.bibliotecas.wx_dbClass import db_mysql_master

class VazaoObservada(db_mysql_master):

    def __init__(self) -> None:
        self.db_tmp = db_mysql_master('db_tmp',connect=True)

    def get_vazao_for_smap(self,dt_ini,dt_fim):
        
        tb_tmp_smap = self.db_tmp.db_schemas['tb_tmp_smap']
        select_values = tb_tmp_smap.select().where(tb_tmp_smap.c.dt_referente >= dt_ini, tb_tmp_smap.c.dt_referente < dt_fim )
        vazoes_obs = self.db_tmp.db_execute(select_values).fetchall()
        
        df_vazoes_obs = pd.DataFrame(vazoes_obs, columns = [ "txt_nome_subbacia","cd_estacao","txt_tipo_vaz" ,"data","vl_vaz"])
        if not vazoes_obs: return df_vazoes_obs
        
        df_vazoes_obs['txt_nome_subbacia'] = df_vazoes_obs['txt_nome_subbacia'].str.lower() 
        return df_vazoes_obs.pivot_table(index='data', columns= 'txt_nome_subbacia', values='vl_vaz')


class ChuvaObservada(db_mysql_master):

    def __init__(self) -> None:
        self.db_rodadas = db_mysql_master('db_rodadas',connect=True)

    def get_chuva_psat(self,dt_ini_obs):

        tb_subbacia = self.db_rodadas.db_schemas['tb_subbacia']
        tb_chuva_psat = self.db_rodadas.db_schemas['tb_chuva_psat']

        select_psat = db.select(
            tb_subbacia.c.txt_nome_subbacia,
            tb_chuva_psat.c.dt_ini_observado,
            tb_chuva_psat.c.vl_chuva
            )\
            .join(
                tb_subbacia, 
                tb_subbacia.c.cd_subbacia==tb_chuva_psat.c.cd_subbacia
            )\
            .where(
                tb_chuva_psat.c.dt_ini_observado >= dt_ini_obs.strftime("%Y-%m-%d")
                )
        psat_values = self.db_rodadas.db_execute(select_psat).fetchall()
        
        df_psath = pd.DataFrame(psat_values, columns=["txt_nome_subbacia","dt_ini_observado","valor"])
        df_psath['data'] = df_psath['dt_ini_observado'] + datetime.timedelta(days=1)
        df_psath['txt_nome_subbacia'] = df_psath['txt_nome_subbacia'].str.lower()
        df_psath_to_write = df_psath.pivot_table(index='data',columns='txt_nome_subbacia',values='valor')
        return df_psath_to_write
    

    def get_chuva_mergeCptec(self,dt_ini_obs):

        tb_subbacia = self.db_rodadas.db_schemas['tb_subbacia']
        tb_chuva_obs = self.db_rodadas.db_schemas['tb_chuva_obs']

        select_gpm = db.select(
            tb_subbacia.c.txt_nome_subbacia,
            tb_subbacia.c.vl_lat,
            tb_subbacia.c.vl_lon,
            tb_chuva_obs.c.dt_observado,
            tb_chuva_obs.c.vl_chuva
            ).join(
                tb_subbacia, 
                tb_subbacia.c.cd_subbacia==tb_chuva_obs.c.cd_subbacia
            ).where(
                tb_chuva_obs.c.dt_observado >= dt_ini_obs.strftime("%Y-%m-%d")
            )
        gpm_values = self.db_rodadas.db_execute(select_gpm).fetchall()

        df_gpm = pd.DataFrame(gpm_values, columns=["txt_nome_subbacia","vl_lat","vl_lon","dt_ini_observado","valor"])
        df_gpm['data'] = df_gpm['dt_ini_observado'] + datetime.timedelta(days=1)
        df_gpm['txt_nome_subbacia'] = df_gpm['txt_nome_subbacia'].str.lower()
        df_gpm_to_write = df_gpm.pivot_table(index='data',columns='txt_nome_subbacia',values='valor')

        return df_gpm_to_write


