import os
import sys
import pdb
import datetime
import pandas as pd


sys.path.insert(1,"/WX2TB/Documentos/fontes")
from PMO.scripts_unificados.apps.smap.libs.DadosObservados import VazaoObservada, ChuvaObservada
from PMO.scripts_unificados.apps.smap.libs.Rodadas import Rodadas

path_diretorio_libs = os.path.dirname(os.path.abspath(__file__))
path_diretorio_app_smap = os.path.dirname(path_diretorio_libs)
path_diretorioArquivos = os.path.join(path_diretorio_app_smap,'arquivos')


#MAQUINA COM S3
PATH_S3_BUCKET = "S:"
PATH_AUXILIO_AJUSTE = os.path.join(PATH_S3_BUCKET,'Programas','Aplicativo_SMAP','auxilio_ajuste')

#MAQUINA LOCAL
# path_home = os.path.expanduser('~')
# PATH_DROPBOX_MIDDLE = os.path.join(path_home, "Dropbox", "WX - Middle")
# PATH_AUXILIO_AJUSTE =  os.path.join(PATH_DROPBOX_MIDDLE,'Programas','Aplicativo_SMAP','auxilio_ajuste')

#path diretorios de operação
PATH_ESTRUTURA_BASE_SMAP = os.path.join(path_diretorio_app_smap,'arquivos','estrutura_base','SMAP_EXT')
PATH_EXECUCAO_SMAP = os.path.join(path_diretorio_app_smap,'arquivos','opera-smap', 'smap_novo')

class StructureSMAP(VazaoObservada,ChuvaObservada,Rodadas):

    def __init__(self,modelos,dt_exec=datetime.datetime.now()) -> None:

        self.arquivos = {
            "SUB_BACIAS":"sub_bacias.csv",
            "PRECIPITACAO_OBSERVADA":"psat.csv",
            "PRECIPITACAO_PREVISTA":"prec_prevista.csv",
            "VAZAO_OBSERVADA":"vazoes.csv",
            "EVAPOTRANSPIRACAO_NC":"evapotranspitacao.csv",
            "DATAS_RODADAS":"datasRodadas.csv",
            "POSTOS_PLUVIOMETRICOS":"postos_plu.csv",
            "PARAMETROS":"parametros.csv",
            "INICIALIZACAO":"inicializacao.csv"
        }
        
        self.bacias = {}
        self.postos = {}
        self.modelos = modelos

        self.ndia_prev = 44
        self.dias_aquec = 31   # numero de dias para o aquecimento do modelo
        self.dt_exec = dt_exec 
        self.ec_probs = None # somente para o PCONJUNTO + ECMWF clusters

        os.makedirs(os.path.join(PATH_EXECUCAO_SMAP,'Arq_entrada'), exist_ok=True)
        os.makedirs(os.path.join(PATH_EXECUCAO_SMAP,'Arq_saida'), exist_ok=True)
        
        Rodadas.__init__(self)  
        VazaoObservada.__init__(self)  
        ChuvaObservada.__init__(self)
        
    def info_bacias(self):
        
        #BACIAS
        for bac in os.listdir(PATH_ESTRUTURA_BASE_SMAP):
            self.bacias[bac] = []
            estruturaBacia = os.path.join(PATH_ESTRUTURA_BASE_SMAP,bac,'ARQ_ENTRADA')
            listaArquivos = os.listdir(estruturaBacia)
            for f in listaArquivos:
                if '_PMEDIA.txt' in f or '_PM.ECMWF0.txt' in f:
                    self.bacias[bac].append(f.replace('_PMEDIA.txt','').replace('_PM.ECMWF0.txt',''))

        print("[INICIALIZAÇÂO] Infos bacias, OK!")


    def historico_vazao(self,dt_ini:datetime.datetime,dt_fim:datetime.datetime):
        df_vazao_obs = self.get_vazao_for_smap(dt_ini=dt_ini,dt_fim=dt_fim)
        if df_vazao_obs.empty: 
            print("[ERROR] Nao ha historico de vazao disponivel!")
            quit()
        ultimo_dt_historico_vazao = max(pd.to_datetime(df_vazao_obs.index)).date()
        return df_vazao_obs,ultimo_dt_historico_vazao
    
    def historico_psat(self,dt_ini:datetime.datetime,dt_fim:datetime.datetime):

        df_chuva_obs = self.get_chuva_psat(dt_ini_obs=(dt_ini-datetime.timedelta(days=1)))
        ultima_dt_historico_chuva = max(pd.to_datetime(df_chuva_obs.index)).date()    
        dias_faltantes_chuva = (dt_fim - ultima_dt_historico_chuva).days

        if dias_faltantes_chuva >0:
            df_gpm = self.get_chuva_mergeCptec(ultima_dt_historico_chuva)
            ultima_dt_historico_gpm = max(pd.to_datetime(df_gpm.index)).date()   
            dias_faltantes_chuva = (dt_fim - ultima_dt_historico_gpm).days
        
            if dias_faltantes_chuva <= 0:
                print(f'Sera utilizado chuva do gpm para datas da rodada > {ultima_dt_historico_chuva}.')
                df_chuva_obs = pd.concat([df_chuva_obs,df_gpm])
            else:
                print('Faltando chuva observada, SMAP não sera rodado!')
                quit()
        
        print('[INICIALIZAÇÂO] Histórico de chuva, OK!')
        return df_chuva_obs, ultima_dt_historico_chuva
    
    def last_pdp_dt_ajuste(self):

        import glob
        teste = pd.read_fwf(glob.glob(os.path.join(PATH_AUXILIO_AJUSTE,"**"))[0]) 
        teste['Data'] = pd.to_datetime(teste['Data'], dayfirst=True)
        
        df_valores_ajuste = teste.loc[~(teste[['TU','EB','SUP']] == 0).all(axis=1)]
        min_dt_ajuste = df_valores_ajuste['Data'].min()
        max_dt_ajuste = df_valores_ajuste['Data'].max()

        return max_dt_ajuste,min_dt_ajuste

    def tranform_to_preliminar(self,ultimo_dt_historico_vazao:datetime.datetime,df_chuva_prevista:pd.DataFrame ):   

        print(f'Ultimo dia de historico de vazao -> {ultimo_dt_historico_vazao}')
        print(f'O modelo será considerado como preliminar, rodando com a data anterior mas com a previsao chuva atual.')
        
        df_preliminares = df_chuva_prevista[df_chuva_prevista['data_rodada'] == (ultimo_dt_historico_vazao + datetime.timedelta(days=2))]
        df_preliminares.loc[:,'modelo'] = df_preliminares['modelo'] + '.PRELIMINAR'
        df_preliminares.loc[:,'data_rodada'] = df_preliminares['data_rodada'] - datetime.timedelta(days=1)

        dt_rodada_preliminar = df_preliminares['data_rodada'].unique()[0]
        chuva_faltante = self.get_chuva_mergeCptec(dt_ini_obs=dt_rodada_preliminar)
        if chuva_faltante.empty:
            print('Faltando chuva obs para o primeiro dia de previsao, SMAP não sera rodado!')
            quit()
        print("Utilizando o gpm no primeiro dia de previsão para os preliminares.")

        chuva_faltante = chuva_faltante.reset_index().melt(id_vars=['data'], var_name='nome', value_name='valor')

        df = df_preliminares.drop_duplicates(subset=['modelo', 'hr_rodada','nome']).drop(['valor','data_previsao'],axis=1)
        df_primeiro_dia_previsao = df.merge(chuva_faltante,on='nome').rename({'data':'data_previsao'},axis=1)
        df_chuva_preliminar = pd.concat([df_preliminares,df_primeiro_dia_previsao])

        #mostrando os modelos que nao podem rodar por falta de dados de vazao obs
        df_nao_rodados = df_chuva_prevista[df_chuva_prevista['data_rodada'] > (ultimo_dt_historico_vazao + datetime.timedelta(days=2))]
        if not df_nao_rodados.empty: print(
            "Não será possivel rodar os seguintes modelos por falta de dados de vazão obs:",
            df_nao_rodados['cenario'].unique()
            )

        #filtrando somente os modelos que estao aptos a rodar
        df_chuva_prevista_smap = df_chuva_prevista[df_chuva_prevista['data_rodada'] <= (ultimo_dt_historico_vazao + datetime.timedelta(days=1))]
        df_chuva_prevista_smap = pd.concat([df_chuva_prevista_smap,df_chuva_preliminar])

        return df_chuva_prevista_smap
    
    def check_if_preliminar(self,ultimo_dt_historico_vazao:datetime.datetime,df_chuva_prevista:pd.DataFrame):

        max_dt_rodada = max(df_chuva_prevista['data_rodada'])

        #historico de vazao precisa ter até a data anterior
        dias_faltantes_vazao = ((max_dt_rodada - datetime.timedelta(days=1)) - ultimo_dt_historico_vazao).days
        if dias_faltantes_vazao > 0:
            df_chuva_prevista_smap = self.tranform_to_preliminar(ultimo_dt_historico_vazao,df_chuva_prevista)
        else:
            df_chuva_prevista_smap = df_chuva_prevista
        print('[INICIALIZAÇÂO] Histórico de vazao, OK!')
        return df_chuva_prevista_smap
    
    def write_arquivos_names(self):
        pd.DataFrame(self.arquivos.items(), columns=['arquivo','nome_arquivo']).to_csv(
            os.path.join(PATH_EXECUCAO_SMAP,'Arq_entrada','arquivos.csv'),sep=';',index=False
            )

    def write_dt_rodada_file(self, list_dt_rodadas:list):

        list_ndias_prev = [self.ndia_prev]*len(list_dt_rodadas)
        pd.DataFrame(list(zip(list_dt_rodadas,list_ndias_prev)),columns=['data','numero_dias_previsao']).to_csv(
                os.path.join(PATH_EXECUCAO_SMAP,'Arq_entrada',self.arquivos['DATAS_RODADAS']),sep=';',index=False
                )
    
    def write_time_series_datasets(self, modelos_list:list,chuva_ext:bool):

        #chuva obs é armazenada com a data de inicio do acumulado e no smap é com a data final do acumulado (12z:12z UTC) == (9h:9h UTC-3 Brasilia)
        #considerando que acumulado das 12z as 12z é 9h as 9h aqui no brasil, maior parte do acumulado está na data inicial, por isso armazenamos assim

        '''Se algum modelo, para uma mesma data da rodada, não tiver o mesmo
        numero de dias de previsao em relacao aos demais, acontece um erro no R. 
        '''

        if chuva_ext:
            df_chuva_prevista, self.ec_probs = self.get_chuva_extendida(modelos_list=modelos_list)
        else:
            df_chuva_prevista = self.get_chuva_modelos(modelos_list=modelos_list)


        if df_chuva_prevista.empty:
            print("Não há chuva prevista para os modelos! Não será possivel rodar!")
            quit()
        
        max_dt_rodada = max(df_chuva_prevista['data_rodada'])
        dt_inicial = min(df_chuva_prevista['data_rodada']) - datetime.timedelta(days=120)
        df_vazao_obs,ultimo_dt_historico_vazao = self.historico_vazao(dt_ini=dt_inicial,dt_fim=max_dt_rodada)

        #muda os modelos para .preliminar se necessario
        df_chuva_prevista_smap = self.check_if_preliminar(
            ultimo_dt_historico_vazao=ultimo_dt_historico_vazao,
            df_chuva_prevista=df_chuva_prevista
            )
        df_chuva_prevista_smap.reset_index(drop=True, inplace=True)
        
        #se for preliminar altera data_rodada, portanto pegamos novamente as datas
        max_dt_rodada = max(df_chuva_prevista_smap['data_rodada'])
        dt_inicial = min(df_chuva_prevista_smap['data_rodada']) - datetime.timedelta(days=120)
        df_chuva_obs,ultima_dt_historico_chuva = self.historico_psat(dt_ini=dt_inicial,dt_fim=max_dt_rodada)

        #adiciona tipo de chuva .GPM ou .PSAT ao nome do modelo, por default todos sao .PSAT
        df_chuva_prevista_smap['modelo'] = df_chuva_prevista_smap['modelo'] + ".PSAT"
        df_chuva_prevista_smap.loc[df_chuva_prevista_smap['data_rodada'] > ultima_dt_historico_chuva, 'modelo'] = df_chuva_prevista_smap['modelo'].str.replace(".PSAT", ".GPM")

        #ultima data dos arquivos de ajuste do pdp, coloca .pdp apenas nas rodadas que tem a mesma data do pdp (dt_ult_pdp_disponivel)
        # .pdp significa que o historico de vazao e a inicializacao de ajuste sao os mesmos do arquivo pdp mas a chuva ainda é psat (rever isso) 
        dt_max, dt_min = self.last_pdp_dt_ajuste()
        dt_ult_pdp_disponivel = (dt_max + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        df_chuva_prevista_smap.loc[pd.to_datetime(df_chuva_prevista_smap['data_rodada']).dt.strftime('%Y-%m-%d') == dt_ult_pdp_disponivel ,'modelo'] += '.PDP'

        #ciarndo cenario unico
        df_chuva_prevista_smap['cenario'] = (
                df_chuva_prevista_smap['modelo']
                + '_' 
                + df_chuva_prevista_smap['data_rodada'].astype(str)
                +'_'
                +  df_chuva_prevista_smap['hr_rodada'].astype(str)
        )

        print('[INICIALIZAÇÂO] Modelos:')
        for cenario in df_chuva_prevista_smap['cenario'].unique():
            print(f"    -> cenario:{cenario}")

        #formatando valores de entrada
        df_chuva_prevista_smap = df_chuva_prevista_smap[["data_rodada","data_previsao","cenario","nome","valor"]].copy()
        df_chuva_prevista_smap['data_rodada'] = pd.to_datetime(df_chuva_prevista_smap['data_rodada']).dt.strftime("%d/%m/%Y")
        df_chuva_prevista_smap['data_previsao'] = pd.to_datetime(df_chuva_prevista_smap['data_previsao']).dt.strftime("%d/%m/%Y")
        df_vazao_obs.index = pd.to_datetime(df_vazao_obs.index).strftime("%d/%m/%Y")
        df_chuva_obs.index= pd.to_datetime(df_chuva_obs.index).strftime("%d/%m/%Y")

        #escreve os arquivos na na pasta
        df_vazao_obs.to_csv(os.path.join(PATH_EXECUCAO_SMAP,'Arq_entrada',self.arquivos['VAZAO_OBSERVADA']), sep=';')
        df_chuva_obs.to_csv(os.path.join(PATH_EXECUCAO_SMAP,'Arq_entrada',self.arquivos['PRECIPITACAO_OBSERVADA']), sep=';')
        df_chuva_prevista_smap.to_csv(os.path.join(PATH_EXECUCAO_SMAP,'Arq_entrada',self.arquivos['PRECIPITACAO_PREVISTA']),sep=';',index=False)
        
        return df_vazao_obs,df_chuva_obs,df_chuva_prevista_smap

    def write_init_files(self):

        df_postos_plu = pd.DataFrame()
        df_evp = pd.DataFrame()
        df_params = pd.DataFrame()
        df_inicializacao = pd.DataFrame()

		#copiando arquivos com as coordenadas de cada subbacia
        for bacia in self.bacias:

            src = os.path.join(PATH_ESTRUTURA_BASE_SMAP,bacia,'ARQ_ENTRADA')

            for f in os.listdir(src):
                
                if("_POSTOS_PLU" in f):
                    df_plu = pd.read_fwf(os.path.join(src,f),header=None).dropna()
                    df_plu.columns = ['posto','valor']
                    df_plu['nome'] = f.replace('_POSTOS_PLU.txt','').lower()
                    df_plu['posto'] = df_plu['posto'].str.replace('0','').str.lower()
                    df_postos_plu = pd.concat([df_postos_plu,df_plu]) if not df_postos_plu.empty else df_plu
                    
                #kts
                elif "_PARAMETROS.txt" in f: 

                    nome_subbacia = f.replace('_PARAMETROS.txt','').upper()
                    df_params_subbacia = pd.read_fwf(os.path.join(src,f),header=None)
                   
                    list_kts = df_params_subbacia.loc[1].tolist()
                    kts_separados_da_celula = str(list_kts).replace(",","").replace("[","").replace("]","").replace("'","").replace('"','').split()
                    nKt = int(float(kts_separados_da_celula[0]))
                    kts_to_write = kts_separados_da_celula[:nKt+1]

                    kts_to_write.reverse()
                    
                    kts_names = [f'Kt{i}' for i in range(2,-61,-1)] + ['nKt']
                    kts_values = kts_to_write[:-1] + [0]*(63 - nKt) + kts_to_write[-1:]
                    
                    combined_list_kts = list(zip(kts_values,kts_names))
                    df_kts = pd.DataFrame(combined_list_kts)
                    
                    params_names = ['Area','Str','K2t' ,'Crec','Ai','Capc','K_kt','K2t2','H1','H','K3t','K1t','Ecof','Pcof','Ecof2'] 
                    df_params_tratato = df_params_subbacia.drop(1, axis=0)[[0,1]][:-4]
                    df_params_tratato.loc[:,1] = params_names
                    df_params_aux = pd.concat([df_params_tratato,df_kts])

                    df_params_aux['nome'] = nome_subbacia.lower()
                    df_params = pd.concat([df_params,df_params_aux]) if not df_params.empty else df_params_aux

                    #Utilizado no arquivo de inicializacao
                    diaaquecimento = (self.dt_exec - datetime.timedelta(days=self.dias_aquec))
                    teste = os.path.join(PATH_AUXILIO_AJUSTE,nome_subbacia+"_AJUSTE.txt")
                    dt_max, dt_min = self.last_pdp_dt_ajuste()

                    if dt_min > diaaquecimento:
                        print(f"Não há valores disponiveis para a data de aquecimento {diaaquecimento.strftime('%d/%m/%Y')}. Será utilizada a ultima data disponivel: {dt_min.strftime('%d/%m/%Y')}")
                        diaaquecimento = dt_min

                    ini_values = pd.read_fwf(teste).set_index('Data')[['EB','SUP','TU']].loc[diaaquecimento.strftime('%d/%m/%Y')]
                    ini_values = ini_values.tolist() + [self.dias_aquec]
                    df_ini_values = pd.DataFrame(list(zip(ini_values,['Ebin','Supin','Tuin','numero_dias_assimilacao'])), columns=['valor','variavel'])
                    
                    limites_names = ["limite_superior_ebin","limite_inferior_ebin","limite_superior_prec","limite_inferior_prec"]
                    df_ini_limites = df_params_subbacia[[0,1]][-4:]
                    df_ini_limites.columns = ['valor','variavel']
                    df_ini_limites.loc[:,'variavel'] = limites_names

                    df_ini = pd.concat([df_ini_values,df_ini_limites])
                    df_ini['nome'] = nome_subbacia.lower()

                    df_inicializacao= pd.concat([df_inicializacao,df_ini]) if not df_inicializacao.empty else df_ini

                elif "_EVAPOTRANSPIRACAO" in f:
                    df_evp_subbacia = pd.read_fwf(os.path.join(src,f),header=None).dropna()
                    df_evp_subbacia.columns = ['mes','valor']
                    df_evp_subbacia['nome'] = f.replace('_EVAPOTRANSPIRACAO.txt','').lower()
                    df_evp = pd.concat([df_evp,df_evp_subbacia]) if not df_evp.empty else df_evp_subbacia


        #escreve arquivos
        df_params.columns = ['valor','parametro','nome']
        df_params[['nome','parametro','valor']].to_csv(os.path.join(PATH_EXECUCAO_SMAP,'Arq_entrada',self.arquivos['PARAMETROS']),sep=';', index=False)
        df_inicializacao[['nome','variavel','valor']].to_csv(os.path.join(PATH_EXECUCAO_SMAP,'Arq_entrada',self.arquivos['INICIALIZACAO']),sep=';',index=False) 
        df_postos_plu[['nome','posto','valor']].to_csv(os.path.join(PATH_EXECUCAO_SMAP,'Arq_entrada',self.arquivos['POSTOS_PLUVIOMETRICOS']),sep=';',index=False)
        df_evp[['mes','nome','valor']].to_csv(os.path.join(PATH_EXECUCAO_SMAP,'Arq_entrada',self.arquivos['EVAPOTRANSPIRACAO_NC']),sep=';',index=False)

        print("[INICIALIZAÇÂO] Arquivos base gerados, OK!")

    def write_subbacias_file(self):
        subbacias = [item.lower() for sublist in self.bacias.values() for item in sublist]
        pd.DataFrame(subbacias,columns=['nome']).to_csv(os.path.join(PATH_EXECUCAO_SMAP,'Arq_entrada',self.arquivos['SUB_BACIAS']),sep=';',index=False)

    def build(self,modelos_list:list, chuva_ext:bool):
        df_vazao_obs,df_chuva_obs,df_chuva_prevista_smap = self.write_time_series_datasets(modelos_list,chuva_ext)
        self.info_bacias()
        self.write_dt_rodada_file(df_chuva_prevista_smap['data_rodada'].unique())
        self.write_arquivos_names()
        self.write_subbacias_file()
        self.write_init_files()

        print(f"[INICIALIZAÇÂO] ARQUIVOS DE ENTRADA GERADOS CORRETAMENTE!")

    def info_postos_pos_processamento(self): 
        import re

        if not self.bacias: self.info_bacias()

        for bacia in self.bacias: 
            
            file_pos_processamento = os.path.join(PATH_ESTRUTURA_BASE_SMAP,bacia,'Arq_Pos_Processamento')
            try:
                with open(os.path.join(file_pos_processamento,os.listdir(file_pos_processamento)[0]),'r',encoding='utf-8') as file: 
                    file_read = file.readlines()
            except:
                with open(os.path.join(file_pos_processamento,os.listdir(file_pos_processamento)[0]),'r') as file: 
                    file_read = file.readlines()

            nomes_postos = re.split(r'\s{2,}', file_read[0].strip())
            bacias = [bacia for i in range(len(nomes_postos))]

            tuple_name_n_type = list(zip(nomes_postos,file_read[2].split(),bacias))
            cd_postos = pd.DataFrame(file_read[1].split()).astype(int)[0].tolist()
            dict_info_posto = dict(zip(cd_postos,tuple_name_n_type))
            self.postos.update(dict_info_posto)

    def write_file_pos_processamento(self,modelo:str,df_prev_postos:pd.DataFrame,df_info_postos:pd.DataFrame,path_out:str):

        #escreve arquivo de pos processamento        
        vz_inc  = df_prev_postos[df_prev_postos['tipo_vazao']=='VNI'][['cenario','posto','nome_pos','data_previsao','data_rodada','previsao_incremental']]
        vz_nat  = df_prev_postos[df_prev_postos['tipo_vazao']=='VNA'][['cenario','posto','nome_pos','data_previsao','data_rodada','previsao_total']]
        vz_inc = vz_inc.rename({'previsao_incremental':'vz_previsao'},axis=1)
        vz_nat = vz_nat.rename({'previsao_total':'vz_previsao'},axis=1)
        df_vz_previsao = pd.concat([vz_inc,vz_nat],axis=0)

        #diario
        df_pos_diario = df_vz_previsao.pivot_table(index = 'data_previsao', columns = 'posto',values='vz_previsao')
        df_write_diario = pd.concat([df_info_postos,df_pos_diario],axis=0)
        with open(os.path.join(path_out,f'{df_prev_postos["data_rodada"].values[0]}_{modelo}_PREVISAO_D.txt'), 'w',encoding='utf-8') as file: file.write(df_write_diario.to_string())

        #semanal
        df_pos_diario.index = pd.to_datetime(df_pos_diario.index)
        df_pos_semanal = df_pos_diario.resample('W-SAT',label='left',closed='left').mean()
        df_write_semanal = pd.concat([df_info_postos,df_pos_semanal],axis=0)
        with open(os.path.join(path_out,f'{df_prev_postos["data_rodada"].values[0]}_{modelo}_PREVISAO_S.txt'), 'w',encoding='utf-8') as file: file.write(df_write_semanal.to_string())

        return df_pos_diario, df_write_semanal