import sys
import pdb
import datetime
import pandas as pd
import sqlalchemy as db


sys.path.insert(1,"/WX2TB/Documentos/fontes")
from PMO.scripts_unificados.bibliotecas.wx_dbClass import db_mysql_master


class tb_cadastro_rodada():

    def __init__(self,db_rodadas=None) -> None:
        self.DB_RODADAS=db_rodadas if db_rodadas else db_mysql_master('db_rodadas',connect=True)
        self.tb_cadastro_rodadas = self.DB_RODADAS.db_schemas['tb_cadastro_rodadas']
        self.tb_smap = self.DB_RODADAS.db_schemas['tb_smap']
        self.tb_chuva = self.DB_RODADAS.db_schemas['tb_chuva']

    def flags_priority(self):
        priority_order = db.case(
                ((self.tb_cadastro_rodadas.c.fl_pdp == 1) & (self.tb_cadastro_rodadas.c.fl_preliminar == 0), 1),
                ((self.tb_cadastro_rodadas.c.fl_psat == 1) & (self.tb_cadastro_rodadas.c.fl_preliminar == 0), 2),
                ((self.tb_cadastro_rodadas.c.fl_pdp == 0) & (self.tb_cadastro_rodadas.c.fl_psat == 0) & (self.tb_cadastro_rodadas.c.fl_preliminar == 0), 3),
                (self.tb_cadastro_rodadas.c.fl_preliminar == 1, 4),
                else_=None
            ).label('order_priority')
        return priority_order
    
    def base_columns(self):
        base_columns = [
                self.tb_cadastro_rodadas.c.id,
                self.tb_cadastro_rodadas.c.str_modelo,
                self.tb_cadastro_rodadas.c.hr_rodada,
                self.tb_cadastro_rodadas.c.dt_rodada
            ]
        return base_columns
    
    def conditional_data_columns(self,columns_data=['id_smap','id_chuva','id_previvaz','fl_estudo','fl_preliminar','fl_pdp','fl_psat','dt_revisao']):
        conditional_columns = [
                self.tb_cadastro_rodadas.c.id_chuva if 'id_chuva' in columns_data else None,
                self.tb_cadastro_rodadas.c.id_smap if 'id_smap' in columns_data else None,
                self.tb_cadastro_rodadas.c.id_previvaz if 'id_previvaz' in columns_data else None,
                self.tb_cadastro_rodadas.c.fl_estudo if 'fl_estudo' in columns_data else None,
                self.tb_cadastro_rodadas.c.fl_preliminar if 'fl_preliminar' in columns_data else None,
                self.tb_cadastro_rodadas.c.fl_pdp if 'fl_pdp' in columns_data else None,
                self.tb_cadastro_rodadas.c.fl_psat if 'fl_psat' in columns_data else None,
                self.tb_cadastro_rodadas.c.dt_revisao if 'dt_revisao' in columns_data else None,

            ]
        return conditional_columns
        
    def info_rodadas(self, modelos_list:list=[], column_data:str='id_chuva',ids_rodadas:list=[]): 

        priority_order = self.flags_priority()
        columns = [column.name if column.name !='id' else 'id_rodada' for column in self.tb_cadastro_rodadas.columns] 
        
        if modelos_list:
            subquery_cadastro_rodadas = db.select(self.tb_cadastro_rodadas,priority_order).where(
                db.tuple_(
                    self.tb_cadastro_rodadas.c.str_modelo,
                    self.tb_cadastro_rodadas.c.hr_rodada,
                    self.tb_cadastro_rodadas.c.dt_rodada
                    
                ).in_(modelos_list)
            )
            rodadas_values = self.DB_RODADAS.db_execute(subquery_cadastro_rodadas).fetchall()
            ids_rodadas = [id_rodada[0] for id_rodada in rodadas_values]
        
        if ids_rodadas and column_data == 'id_smap' :
            subquery_cadastro_rodadas = db.select(self.tb_cadastro_rodadas,self.tb_smap,priority_order
            ).join(
                self.tb_cadastro_rodadas, self.tb_cadastro_rodadas.c.id_smap == self.tb_smap.c.id
            ).where(
                self.tb_cadastro_rodadas.c.id.in_(ids_rodadas)
            )
            columns +=  [column.name for column in self.tb_smap.columns]
            rodadas_values = self.DB_RODADAS.db_execute(subquery_cadastro_rodadas).fetchall()


        elif ids_rodadas  and column_data=='id_chuva':
            subquery_cadastro_rodadas = db.select(self.tb_cadastro_rodadas,self.tb_chuva,priority_order
            ).join(
                self.tb_cadastro_rodadas, self.tb_cadastro_rodadas.c.id_chuva == self.tb_chuva.c.id
            ).where(
                self.tb_cadastro_rodadas.c.id.in_(ids_rodadas)
            )
            columns +=  [column.name if column.name !='id' else 'id_chuva2' for column in self.tb_chuva.columns]

        
            rodadas_values = self.DB_RODADAS.db_execute(subquery_cadastro_rodadas).fetchall()
            
        return pd.DataFrame(rodadas_values,columns=columns+["order_priority"] )
    # pd.DataFrame(rodadas_values, columns=[column_name.name for column_name in selected_columns]+['priority'])

    def get_rodadas_do_dia(self, dt_rodada):

        priority_order = self.flags_priority()
        base_columns = self.base_columns()
        conditional_columns = self.conditional_data_columns()
        selected_columns = base_columns + [col for col in conditional_columns if col is not None] +[priority_order]
        
        query_rodadas_do_dia = db.select(selected_columns).where(
            self.tb_cadastro_rodadas.c.dt_rodada == dt_rodada,
            )
        
        rodadas_values = self.DB_RODADAS.db_execute(query_rodadas_do_dia).fetchall()
        selected_columns = [column_name.key for column_name in selected_columns]
        return pd.DataFrame(rodadas_values, columns=selected_columns)

    def get_last_column_id(self, column_name:str):

        query_get_max_id_column = db.select(db.func.max(self.tb_cadastro_rodadas.c[column_name]))
        max_id = self.DB_RODADAS.db_execute(query_get_max_id_column).scalar()
        return max_id
    
    def update_cadastro_rodadas(self,id_rodada:int,values:str):

        query_update = self.tb_cadastro_rodadas.update().values(values).where(
                self.tb_cadastro_rodadas.c.id == id_rodada
                )
        n_value = self.DB_RODADAS.db_execute(query_update).rowcount
        print(f"{n_value} Linha com id {id_rodada} foi atualizada")

    def importar_cadastro_rodadas(self, rodadas_values:list):
        
        query_update = self.tb_cadastro_rodadas.insert().values(rodadas_values)
        n_value = self.DB_RODADAS.db_execute(query_update).rowcount

        print(f"{n_value} Linhas inseridas na tb_cadastro_rodadas.")

class tb_smap(tb_cadastro_rodada):

    def __init__(self,db_rodadas=None) -> None:
        self.DB_RODADAS = db_rodadas if db_rodadas else db_mysql_master('db_rodadas',connect=True)
        self.tb_smap = self.DB_RODADAS.db_schemas['tb_smap']
        tb_cadastro_rodada.__init__(self,self.DB_RODADAS)


    def get_vazao_modelos(self, modelos_list:list=[],ids_rodadas:list=[], priority:bool=False):

        df_rodadas_vazao_unica = self.info_rodadas(modelos_list=modelos_list,ids_rodadas=ids_rodadas,column_data='id_smap')
        return df_rodadas_vazao_unica
        # pd.merge(df_rodadas_vazao_unica,df_prev_vazao.rename({'id':'id_smap'},axis=1), on=['id_smap']) 

    def importar_prev_vazoes_smap(self, df_prev_vazao_out:pd.DataFrame):
        
        values_smap = df_prev_vazao_out[['id_smap','cd_posto','dt_prevista','vl_vazao']].values.tolist()
        ids_smap = df_prev_vazao_out['id_smap'].unique() 

        query_delete = self.tb_smap.delete().where(self.tb_smap.c.id.in_(ids_smap))
        n_value = self.DB_RODADAS.db_execute(query_delete).rowcount
        print(f"{n_value} Linhas deletadas na tb_smap.")

        query_insert = self.tb_smap.insert().values(values_smap)
        n_value = self.DB_RODADAS.db_execute(query_insert).rowcount
        print(f"{n_value} Linhas inseridas na tb_smap.")

class tb_subbacia():

    def __init__(self,db_rodadas=None) -> None:
        self.DB_RODADAS = db_rodadas if db_rodadas else db_mysql_master('db_rodadas',connect=True)
        self.tb_subbacia = self.DB_RODADAS.db_schemas['tb_subbacia']

    def info_subbacias(self):
        query_subbac = db.select(self.tb_subbacia.c.cd_subbacia,self.tb_subbacia.c.vl_lon,self.tb_subbacia.c.vl_lat,self.tb_subbacia.c.txt_nome_subbacia)
        answer_tb_subbac = self.DB_RODADAS.db_execute(query_subbac)
        
        df_subbac = pd.DataFrame(answer_tb_subbac, columns=['cd_subbacia','vl_lon','vl_lat','nome'])
        df_subbac['nome'] = df_subbac['nome'].str.lower()
        return df_subbac

class tb_chuva(tb_cadastro_rodada,tb_subbacia):

    def __init__(self,db_rodadas=None) -> None:
        self.DB_RODADAS = db_rodadas if db_rodadas else db_mysql_master('db_rodadas',connect=True)
        self.tb_chuva = self.DB_RODADAS.db_schemas['tb_chuva']

        tb_cadastro_rodada.__init__(self,self.DB_RODADAS)
        tb_subbacia.__init__(self,self.DB_RODADAS)

    def get_chuva_modelos(self, modelos_list:list, unique=True):

        df_rodadas_chuva_unica = self.info_rodadas(modelos_list = modelos_list,column_data='id_chuva')
        if df_rodadas_chuva_unica.empty: return df_rodadas_chuva_unica

        if unique: df_rodadas_chuva_unica = df_rodadas_chuva_unica.drop_duplicates(subset=['id_chuva','dt_prevista','cd_subbacia'],keep='first')

        df_chuva_prevista = df_rodadas_chuva_unica[['str_modelo','hr_rodada','dt_rodada','cd_subbacia','dt_prevista','vl_chuva']].copy()
        df_chuva_prevista.columns=['modelo','hr_rodada','data_rodada','cd_subbacia','data_previsao','valor']

        df_chuva_prevista_modelos = df_chuva_prevista[df_chuva_prevista['modelo'] != 'PZERADA']
        test_pzerada= df_chuva_prevista[df_chuva_prevista['modelo'] == 'PZERADA']

        df_subbac = self.info_subbacias()

        #add chuva pzerada ao dataframe
        if not test_pzerada.empty:
            pzerada_values = test_pzerada[["modelo","hr_rodada","data_rodada"]].values.tolist()
            for modelo_pzerada,hr_rodada,dt_rodada in pzerada_values:
                df_pzerada = df_subbac[['cd_subbacia','nome']].copy()
                df_pzerada['modelo'] = modelo_pzerada
                df_pzerada['hr_rodada'] = hr_rodada
                df_pzerada['data_rodada'] = dt_rodada
                df_pzerada['data_previsao'] = dt_rodada + datetime.timedelta(days=1)
                df_pzerada['valor'] = 0
                df_pzerada = df_pzerada[['modelo','hr_rodada','data_rodada','cd_subbacia','data_previsao','valor']]

                df_chuva_prevista_modelos = pd.concat([df_chuva_prevista_modelos,df_pzerada])
        
        df_chuva_subbac = pd.merge(df_chuva_prevista_modelos,df_subbac[['cd_subbacia','nome']], on=['cd_subbacia'])
        
        return df_chuva_subbac

    def get_prob_grup_ec(self,modelo_EC_list):

        query = db.select(self.tb_cadastro_rodadas.c.id).where(
                db.tuple_(
                    self.tb_cadastro_rodadas.c.str_modelo,
                    self.tb_cadastro_rodadas.c.hr_rodada,
                    self.tb_cadastro_rodadas.c.dt_rodada
                    
                ).in_(modelo_EC_list)
            )
        id_cadastro_ecmwf = self.DB_RODADAS.db_execute(query).scalar()
        tb_pesos_grupos_ecmwf = self.DB_RODADAS.db_schemas['tb_pesos_grupos_ecmwf']
        query = tb_pesos_grupos_ecmwf.select().where(tb_pesos_grupos_ecmwf.c.id_deck == id_cadastro_ecmwf)
        teste = pd.DataFrame(self.DB_RODADAS.db_execute(query).fetchall())
        teste.columns = ['id'] + [f'ECMWF-G{i}' for i in range(1,11)]
        return teste.drop('id',axis=1).to_dict('records')

    def get_chuva_extendida(self,modelos_list):

        df_chuva_prevista = self.get_chuva_modelos(modelos_list)
        if df_chuva_prevista.empty: return df_chuva_prevista,None

        #padrao para diferenciar os modelos
        df_chuva_prevista['cenario'] = (
                df_chuva_prevista['modelo']
                + '_' 
                + df_chuva_prevista['data_rodada'].astype(str)
                +'_'
                +  df_chuva_prevista['hr_rodada'].astype(str)
        )

        chuva= {}
        ec_probs = {}
        

        df_chuva_modelo_cluster = pd.DataFrame()
        for cenario in df_chuva_prevista['cenario'].unique():

            df_cenario = df_chuva_prevista[df_chuva_prevista['cenario']==cenario]

            modelo,dt_rodada_cenario,hr_rodada = cenario.split("_")
            dt_rodada_cenario = pd.to_datetime(dt_rodada_cenario).date()
            max_dt_previsao_cenario = df_cenario['data_previsao'].max()

            ec_probs[dt_rodada_cenario] = self.get_prob_grup_ec(
                modelo_EC_list=[('ECMWF-CLUST', 18, dt_rodada_cenario - datetime.timedelta(days=1))]
                ) if not ec_probs.get(dt_rodada_cenario,[]) else ec_probs[dt_rodada_cenario]

            modelos_list= [(f'ECMWF-G{i}', 18, dt_rodada_cenario - datetime.timedelta(days=1)) for i in range(1,11)]

            chuva[dt_rodada_cenario] = self.get_chuva_modelos(
                modelos_list=modelos_list

                ) if chuva.get(dt_rodada_cenario,pd.DataFrame()).empty else chuva[dt_rodada_cenario]

            if not chuva[dt_rodada_cenario].empty:
                chuva_grupos = chuva[dt_rodada_cenario]

                for modelo_ec_clust in chuva[dt_rodada_cenario]['modelo'].unique():
                    chuva_grupo_especifico = chuva_grupos[chuva_grupos['modelo']==modelo_ec_clust]

                    df_ec_to_concat = chuva_grupo_especifico[chuva_grupo_especifico['data_previsao'] > max_dt_previsao_cenario].copy()
                    df_ec_to_concat['modelo'] = df_ec_to_concat['modelo'].replace(modelo_ec_clust,modelo)
                    df_ec_to_concat['hr_rodada'] = hr_rodada
                    df_ec_to_concat['data_rodada'] = dt_rodada_cenario

                
                    #caso falte dias para completar, repetir valores do ultimo dia da previsao
                    last_date_rows = df_ec_to_concat[df_ec_to_concat['data_previsao']==df_ec_to_concat['data_previsao'].max()].copy()
                    last_date_rows['data_previsao'] += datetime.timedelta(days=1)


                    df_junta_modelo_clust = pd.concat([df_cenario,df_ec_to_concat,last_date_rows]) 
                    df_junta_modelo_clust['modelo'] = df_junta_modelo_clust['modelo'].replace(modelo,f"{modelo}-{modelo_ec_clust}")

                    if df_chuva_modelo_cluster.empty: df_chuva_modelo_cluster = df_junta_modelo_clust
                    else:
                        df_chuva_modelo_cluster = pd.concat([df_chuva_modelo_cluster, df_junta_modelo_clust]) 
        return df_chuva_modelo_cluster.drop("cenario",axis=1), ec_probs

    def importar_prev_chuva(self, df_prev_vazao_out:pd.DataFrame):
        
        values_chuva = df_prev_vazao_out[['id_chuva','cd_subbacia','dt_referente','vl_chuva']].values.tolist()
        ids_chuva = df_prev_vazao_out['id_chuva'].unique() 

        query_delete = self.tb_chuva.delete().where(self.tb_chuva.c.id.in_(ids_chuva))
        n_value = self.DB_RODADAS.db_execute(query_delete).rowcount
        print(f"{n_value} Linhas deletadas na tb_chuva.")

        query_insert = self.tb_chuva.insert().values(values_chuva)
        n_value = self.DB_RODADAS.db_execute(query_insert).rowcount
        print(f"{n_value} Linhas inseridas na tb_chuva.")
        
    def importar_prob_grup_ec(self, df_probabilidade_grupos:pd.DataFrame):
        
        tb_pesos_grupos_ecmwf = self.DB_RODADAS.db_schemas['tb_pesos_grupos_ecmwf']
        
        values_probabilidades = df_probabilidade_grupos[['id_rodada','grupo_1','grupo_2','grupo_3','grupo_4','grupo_5','grupo_6','grupo_7','grupo_8','grupo_9','grupo_10']].values.tolist()
        id_ = df_probabilidade_grupos['id_rodada'].unique() 

        query_delete = tb_pesos_grupos_ecmwf.delete().where(tb_pesos_grupos_ecmwf.c.id_deck.in_(id_))
        n_value = self.DB_RODADAS.db_execute(query_delete).rowcount
        print(f"{n_value} Linhas deletadas na tb_chuva.")

        query_insert = tb_pesos_grupos_ecmwf.insert().values(values_probabilidades)
        n_value = self.DB_RODADAS.db_execute(query_insert).rowcount
        print(f"{n_value} Linhas inseridas na tb_chuva.")


# MAIN CLASS
class Rodadas(tb_smap,tb_chuva,tb_cadastro_rodada):

    def __init__(self) -> None:
        self.DB_RODADAS = db_mysql_master('db_rodadas',connect=True)
        tb_smap.__init__(self,self.DB_RODADAS)
        tb_chuva.__init__(self,self.DB_RODADAS)
        tb_cadastro_rodada.__init__(self,self.DB_RODADAS)
        
    def importar_resultados_smap(self,modelos_list, df_prev_vazao_out:pd.DataFrame):

        df_prev_vazao_out.loc[df_prev_vazao_out['cenario'].str.contains('.PDP'),'cenario'] = df_prev_vazao_out.loc[df_prev_vazao_out['cenario'].str.contains('.PDP'),'cenario'].str.replace('.PSAT','',regex=True)
        df_prev_vazao_out['id_smap'] = ''
        
        df_info_rodadas = self.info_rodadas(modelos_list)

        new_smap_id = self.get_last_column_id(column_name='id_smap') + 1
        insert_cadastro_values = []
        for cenario in df_prev_vazao_out['cenario'].unique():

            modelo, dt_rodada ,hr_rodada, = cenario.split('_')
            modelo_splited = modelo.split('.')
            str_modelo = modelo_splited[0]
            flags = modelo_splited[1:]
            flags = [flags] if type(flags) == str else flags

            flag_preliminar,flag_psat,flag_pdp = 0,0,0
            for flag in flags:
                if flag == "PRELIMINAR" : 
                    flag_preliminar = 1
                    #data real da rodada preliminar
                    dt_rodada = (pd.to_datetime(dt_rodada) + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
                elif flag == "PDP": flag_pdp = 1 
                elif flag == "PSAT": flag_psat = 1
                if flag == "GPM": break

            mask_id_chuva = (df_info_rodadas['str_modelo'].str.upper() == str_modelo.upper()) & (pd.to_datetime(df_info_rodadas['dt_rodada']).dt.strftime('%Y-%m-%d') == dt_rodada) & (df_info_rodadas['hr_rodada'] == int(hr_rodada))
            
            mask_flags = (df_info_rodadas['fl_preliminar'] == flag_preliminar) & (df_info_rodadas['fl_pdp'] == flag_pdp) & (df_info_rodadas['fl_psat'] == flag_psat)
            mask_flags_null = (pd.isnull(df_info_rodadas['fl_preliminar'])) & (pd.isnull(df_info_rodadas['fl_pdp'])) & (pd.isnull(df_info_rodadas['fl_psat']))  
            

            if df_info_rodadas[mask_id_chuva & mask_flags].empty:

                #se existir o cadastro mas as flags estao nulas, update no id_smap e flags
                if not df_info_rodadas[mask_id_chuva & mask_flags_null].empty:
                    id_rodada = df_info_rodadas[mask_id_chuva & mask_flags_null]['id_rodada'].unique()[0]
                    df_prev_vazao_out.loc[df_prev_vazao_out['cenario']== cenario,'id_smap'] = new_smap_id
                    self.update_cadastro_rodadas(
                        id_rodada=id_rodada,
                        values={
                        "id_smap":new_smap_id, 
                        "fl_preliminar":flag_preliminar,
                        'fl_pdp':flag_pdp,
                        'fl_psat':flag_psat
                    })
                #se nao existir cadastra uma nova rodada com suas respectivas flags
                else:
                    print(f"    cenario: {cenario} || modelo: {str_modelo} -> rodada nao encontrada, será cadastrada com id {new_smap_id}")
                    df_prev_vazao_out.loc[df_prev_vazao_out['cenario']== cenario,'id_smap'] = new_smap_id
                    id_chuva = df_info_rodadas[mask_id_chuva]['id_chuva'].unique()[0]
                    insert_cadastro_values += [
                        None, id_chuva, new_smap_id, None,None,dt_rodada,int(hr_rodada),
                        str_modelo,flag_preliminar,flag_pdp,flag_psat,None,None],
                
                new_smap_id+=1

            #se existir a rodada com os mesmo parametros de flags, só atualiza os resultados na tb_smap
            else:
                df_info_rodadas['id_smap'] = df_info_rodadas['id_smap'].astype(str).str.replace('nan','None')
                id_smap = df_info_rodadas[mask_id_chuva & mask_flags]['id_smap'].unique()[0]
                print(f"    cenario: {cenario} || modelo: {str_modelo} -> rodada ja esta cadastrada com id_smap: {id_smap}")
                df_prev_vazao_out.loc[df_prev_vazao_out['cenario']== cenario,'id_smap'] = id_smap

        if insert_cadastro_values: self.importar_cadastro_rodadas(insert_cadastro_values)
        self.importar_prev_vazoes_smap(df_prev_vazao_out)

    def importar_probabilidade_grupos_ecmwf(self,modelos_list,df_probabilidade_grupos:pd.DataFrame):
        df_info_subbacias = self.info_subbacias()
        df_info_rodadas = self.info_rodadas(modelos_list)
        
        model = modelos_list[0]
        str_modelo = model[0]
        dt_rodada = model[2].strftime('%Y-%m-%d')
        hr_rodada = model[1]
        mask_id_chuva = \
            (df_info_rodadas['str_modelo'].str.upper() == str_modelo.upper()) & \
            (pd.to_datetime(df_info_rodadas['dt_rodada']).dt.strftime('%Y-%m-%d') == dt_rodada) & \
            (df_info_rodadas['hr_rodada'] == int(hr_rodada))
            
        if mask_id_chuva.sum() == 1:
            id_ = df_info_rodadas[mask_id_chuva]['id_rodada'].unique()[0]
            df_probabilidade_grupos = df_probabilidade_grupos.T
            df_probabilidade_grupos['id_rodada'] = id_
            df_probabilidade_grupos.columns = ['grupo_1','grupo_2','grupo_3','grupo_4','grupo_5','grupo_6','grupo_7','grupo_8','grupo_9','grupo_10','id_rodada']
            df_probabilidade_grupos = df_probabilidade_grupos[['id_rodada','grupo_1','grupo_2','grupo_3','grupo_4','grupo_5','grupo_6','grupo_7','grupo_8','grupo_9','grupo_10']]
            self.importar_prob_grup_ec(df_probabilidade_grupos)
        
    def importar_chuva_modelos(self,modelos_list,df_prev_chuva_out:pd.DataFrame):
        df_info_subbacias = self.info_subbacias()
        df_chuva_final = pd.merge(df_info_subbacias[['cd_subbacia' ,'vl_lon'  ,'vl_lat']], df_prev_chuva_out)
        df_chuva_final = df_chuva_final.drop(['vl_lat','vl_lon'],axis=1)
        df_prev_chuva = df_chuva_final.melt(id_vars=['cd_subbacia','cenario'], var_name='dt_referente', value_name='vl_chuva')
        
        df_info_rodadas = self.info_rodadas(modelos_list)

        new_chuva_id = self.get_last_column_id('id_chuva') + 1
        insert_cadastro_values = []

        for cenario in df_prev_chuva['cenario'].unique():

            str_modelo, dt_rodada ,hr_rodada, = cenario.split('_')

            mask_id_chuva = \
            (df_info_rodadas['str_modelo'].str.upper() == str_modelo.upper()) & \
            (pd.to_datetime(df_info_rodadas['dt_rodada']).dt.strftime('%Y-%m-%d') == dt_rodada) & \
            (df_info_rodadas['hr_rodada'] == int(hr_rodada))

            if df_info_rodadas[mask_id_chuva].empty:
                insert_cadastro_values += [None, new_chuva_id, None, None,None,dt_rodada,int(hr_rodada),str_modelo,None,None,None,None,None],
                df_prev_chuva.loc[df_prev_chuva['cenario']== cenario,'id_chuva'] = new_chuva_id
                new_chuva_id +=1
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

            if insert_cadastro_values: self.importar_cadastro_rodadas(insert_cadastro_values)
            self.importar_prev_chuva(df_prev_chuva.round(2))


    
        