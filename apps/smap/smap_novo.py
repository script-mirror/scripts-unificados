import os
import pdb
import sys
import time
import datetime 
import subprocess
import pandas as pd
from libs.BuildStructure import StructureSMAP
from libs.SmapTools import trigger_dag_SMAP

PATH_S3_BUCKET = "S:"
PATH_CPINS = os.path.join(PATH_S3_BUCKET,'Chuva-vazão')

# path_home = os.path.expanduser('~')
# PATH_DROPBOX_MIDDLE = os.path.join(path_home, "Dropbox", "WX - Middle")
# PATH_CPINS = os.path.join(PATH_DROPBOX_MIDDLE,'NovoSMAP')

path_diretorio_app_smap = os.path.dirname(os.path.abspath(__file__))
PATH_EXECUCAO_SMAP = os.path.join(path_diretorio_app_smap,'arquivos','opera-smap', 'smap_novo')


class SMAP(StructureSMAP):

    def __init__(self,
                 dt_exec=datetime.datetime.now(), 
                 modelos:list=[('PCONJUNTO',0,datetime.datetime.now())],
                 importar_resultados=True,
                 write_out_files=False
                 ) -> None:
        
        self.modelos = modelos
        self.leitura_nova = True
        self.write_out_files = write_out_files
        self.importar_resultados = importar_resultados
        
        StructureSMAP.__init__(self,modelos,dt_exec)

    def getCpins(self, primeiroDia, ultimoDia):

        cpins_df = pd.read_excel(os.path.join(PATH_CPINS,'PlanilhaUSB.xls'),usecols=["DATA", "INC.", "NAT."], index_col="DATA")
        mask1 = cpins_df.index >= primeiroDia
        mask2 = cpins_df.index <= ultimoDia

        cpins_df = cpins_df[mask1 & mask2]
        cpins_df = cpins_df.rename(columns={'INC.':168, 'NAT.':169})

        return cpins_df

    def agrega_vazao_saida(self,df_prev_postos):

        #naturais e incrementais 
        subBaciasIncrementais = [160, 239, 242] + [34, 154, 243, 245, 246, 266] + [191, 253, 257, 271, 273, 275] + [89]
        df_nat_teste = df_prev_postos[~df_prev_postos['posto'].isin(subBaciasIncrementais)][['cenario','posto','data_previsao','data_rodada','previsao_total']]
        df_inc_teste = df_prev_postos[df_prev_postos['posto'].isin(subBaciasIncrementais)][['cenario','posto','data_previsao','data_rodada','previsao_incremental']]

        df_nat_teste.columns = ['cenario','cd_posto','dt_prevista','data_rodada','vl_vazao']
        df_inc_teste.columns = ['cenario','cd_posto','dt_prevista','data_rodada','vl_vazao']
        df = pd.concat([df_nat_teste,df_inc_teste],axis=0)
        df.columns = ['cenario','cd_posto','dt_prevista','data_rodada','vl_vazao']

        #agregando cpins a previsao dos modelos
        for dt_rodada in df['data_rodada'].unique():
            df_dt_rodada = df[df['data_rodada']==dt_rodada]
            for cenario in df_dt_rodada['cenario'].unique():

                df_cenario = df_dt_rodada[df_dt_rodada['cenario'] == cenario]

                dt_final = pd.to_datetime(df_cenario['dt_prevista']).max()
                dt_inicial = pd.to_datetime(df_cenario['dt_prevista']).min()
                df_cpins = self.getCpins(dt_inicial,dt_final)
                cpins = df_cpins.reset_index().melt(
                    id_vars=['DATA'], var_name=['cd_posto'], value_name='vl_vazao'
                    ).rename({"DATA":"dt_prevista"},axis=1)

                df_cenario_to_concat = df_cenario.head((len(df_cpins))*2).drop(['cd_posto','dt_prevista','vl_vazao'],axis=1).reset_index(drop=True)
                df_cpins_agregado = pd.concat([df_cenario_to_concat,cpins],axis=1)
                df = pd.concat([df,df_cpins_agregado])

        #postos do smap anterior
        lista = [
            1, 2, 211, 6, 7, 8, 9, 10, 11, 12, 14, 15, 16, 17, 18, 205, 23, 209, 22, 251, 24, 25, 206, 207, 28, 31, 32, 33, 99, 
            247, 294, 241, 248, 261, 188, 190, 255, 254, 155, 156, 158, 285, 287, 145, 279, 291, 296, 204, 269, 277, 280, 290,
            297, 270, 257, 191, 253, 273, 271, 275, 227, 228, 288, 229, 230, 149, 262, 134, 263, 141, 148, 144, 259, 278, 281, 
            295, 196, 283, 121, 120, 122, 123, 125, 197, 198, 201, 202, 129, 130, 135, 34, 154, 243, 245, 246, 266, 47, 48, 49,
            249, 50, 51, 52, 57, 61, 62, 63, 119, 117, 160, 161, 237, 238, 239, 240, 242, 71, 72, 73, 74, 76, 77, 78, 222, 110,
            111, 112, 113, 114, 98, 97, 284, 115, 101, 215, 88, 89, 216, 217, 92, 93, 220, 94, 286, 102, 103, 81
            ]
        df['data_rodada'] = pd.to_datetime(df['data_rodada']).dt.strftime('%Y-%m-%d')
        df['dt_prevista'] = pd.to_datetime(df['dt_prevista']).dt.strftime('%Y-%m-%d')
        return df
    
    
    def leitura_saida(self):
        path_vaz_smap_out = os.path.join(PATH_EXECUCAO_SMAP,'Arq_saida',"vazoes_prevista_totais_propagadas.csv")
        df_prev_vazoes_propagadas = pd.read_csv(path_vaz_smap_out, sep=';')
        df_prev_vazoes_propagadas['nome'] = df_prev_vazoes_propagadas['nome'].str.lower() 
        
        df_configuracoes = pd.read_csv(os.path.join(PATH_EXECUCAO_SMAP,"configuracao.csv"),sep=';')
        df_configuracoes['nome_real'] = df_configuracoes['nome_real'].str.lower().str.strip()
        df_prev_postos =  pd.merge(df_prev_vazoes_propagadas,df_configuracoes[['posto','nome_real']].rename({'nome_real':'nome'},axis=1), on=['nome']) 
        self.info_postos_pos_processamento()
        
        df_info_postos = pd.DataFrame(self.postos)
        df_info_postos_smap = df_info_postos.T.reset_index(names=['posto'])
        df_info_postos_smap.columns = ['posto','nome_pos','tipo_vazao','bacia']
        
        df_prev_postos = pd.merge(df_prev_postos,df_info_postos_smap, on='posto')

        if self.write_out_files:
            for cenario in df_prev_postos['cenario'].unique():
                df_prev_postos_cenario = df_prev_postos[df_prev_postos['cenario']==cenario]
                self.write_file_pos_processamento(
                    modelo=cenario,
                    df_prev_postos=df_prev_postos_cenario,
                    df_info_postos=df_info_postos,
                    path_out=os.path.join(PATH_EXECUCAO_SMAP,'Arq_saida')
                    )
        
        
        df_prev_vazao_out = self.agrega_vazao_saida(df_prev_postos)

        if self.importar_resultados:
            print('[IMPORT]: ')
            self.importar_resultados_smap(self.modelos,df_prev_vazao_out)
            self.DB_RODADAS.db_dispose()
            
        # df_ena = self.get_ena_modelos(df_prev_vazao_out)
        
        return df_prev_vazoes_propagadas
    

    def init(self):
        print(f"Iniciado previsão SMAP! {datetime.datetime.now().strftime('%d/%m/%Y %H:%M')}")
        if self.leitura_nova:
            self.build(self.modelos)
            comando = ["Rscript", os.path.join(PATH_EXECUCAO_SMAP,"roda_smapOnsR_paralelo.R"), os.path.join(PATH_EXECUCAO_SMAP,'Arq_entrada')]
        else:
            comando = ["Rscript", "roda_smapOnsR_paralelo.R", self.path_testes]
        
        start_time_total = time.time()
        process = subprocess.run(comando, capture_output=True, text=True)
        print(process.stdout)
        print('\nFinalizado para todas as bacias em {:.2f} segundos!'.format(time.time() - start_time_total))
        
        if process.returncode != 0:
            print("Erro no script R:")
            print(process.stderr)

        else:
            self.leitura_saida()

def trigar_dag_roda_smap(dt:datetime.datetime, modelos_names:list, rodada:int):
    trigger_dag_SMAP(dt_rodada=dt,modelos_names=modelos,rodada=hr)
    

def helper():
    dt = datetime.datetime.now()
    dt_str = dt.strftime('%d/%m/%Y')
    print("Verificar paramentros de entrada, nao sera Rodado!")
    print('Exemplo:')
    print(f"python {sys.argv[0]} modelos \"(('PCONJUNTO',0,'{dt_str}'),('GEFS',0,'{dt_str}'),('GFS',0,'{dt_str}'))\"")
    print(f"python {sys.argv[0]} trigar_dag \"[('PCONJUNTO',0,'{dt_str}'),('GEFS',0,'{dt_str}'),('GFS',0,'{dt_str}')]\"")
    quit()


if __name__ == '__main__':

    if len(sys.argv) > 1:
        if 'help' in sys.argv or '--h' in sys.argv or '-help' in sys.argv:
            helper()

        fl_run = False
        for i in range(len(sys.argv)):
            argumento = sys.argv[i].lower()

            #ARGUMENTOS INICIAIS
            if argumento == 'modelos':
                lista_modelos = sys.argv[i+1]
                lista_modelos = eval(lista_modelos)
                modelos = [(item[0], item[1], datetime.datetime.strptime(item[2], '%d/%m/%Y')) for item in lista_modelos] 
                fl_run = True
                
            if argumento == 'trigar_dag':
                lista_modelos = sys.argv[i+1]
                lista_modelos = eval(lista_modelos)
                fl_trig_dag = True

        if fl_run:
            smap_operator = SMAP(
                modelos=modelos,
                importar_resultados=True
                )
            smap_operator.init()
            # smap_operator.leitura_saida()
        elif fl_trig_dag:
            for model in lista_modelos:
                dt = datetime.datetime.strptime(model[2],'%d/%m/%Y')
                modelos = [model[0]]
                hr = model[1]
                trigger_dag_SMAP(dt_rodada=dt,modelos_names=modelos,rodada=hr)
        else:
            helper()
    else:
        helper()


