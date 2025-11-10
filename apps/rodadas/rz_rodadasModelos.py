import os
import sys
import pdb
import datetime
import pandas as pd
import sqlalchemy as db
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.dates import MO, TU, WE, TH, FR, SA, SU
sys.path.insert("/WX2TB/Documentos/fontes/PMO/scripts_unificados/")
from bibliotecas import  wx_dbClass,wx_opweek,wx_dbLib
from apps.web_modelos.server.caches import rz_cache


COLORS = {
    'ACOMPH': '#06bbc7',
    'PCONJUNTO': '#0000ff',
    'GEFS': '#2e8b57',
    'GFS': '#daa520',
    'PZERADA': '#f20909',
    'EC': '#00ff00',
    'EC-ensremvies': '#00ae00',
    'ETA40': '#ff00ff',
    'ETA40remvies': '#d500ff',
    'PCONJUNTO-EXT': '#81d600',
    'GEFSremvies': '#e06b0b',
    'PMEDIA': '#343837',
    'PCONJUNTO2': '#5d21d0',
    'MLT': '#0e0000'
    }

class Rodadas():

    def __init__(self,modelo=None,hr_rodada=None,dt_rodada=None,flag_pdp=None,flag_psat=None,flag_preliminar=None ,granularidade='submercado'):
        
        self.modelo = modelo
        self.hr_rodada = hr_rodada
        
        self.flag_pdp = bool(flag_pdp) if isinstance(flag_pdp, int) else flag_pdp
        self.flag_psat = bool(flag_psat) if isinstance(flag_psat, int) else flag_psat
        self.flag_preliminar = bool(flag_preliminar) if isinstance(flag_preliminar, int) else flag_preliminar

        if not dt_rodada:
            self.dt_rodada = datetime.datetime.now().strftime("%Y-%m-%d")
        else:
            self.dt_rodada = dt_rodada

        self.dt_rodada_date = datetime.datetime.strptime(self.dt_rodada,"%Y-%m-%d")
        self.granularidade = granularidade

        self.db_instance = wx_dbClass.db_mysql_master('db_rodadas')
        self.tb_rodadas = self.db_instance.getSchema('tb_cadastro_rodadas')

    
    def query_rodadas_do_dia(self,dt_rodada_date):

        if not dt_rodada_date:
            dt_rodada_date = self.dt_rodada_date
    
        self.db_instance.connect()
        tb_cadastro_rodadas = self.tb_rodadas
        
        query = db.select(tb_cadastro_rodadas.c.id,tb_cadastro_rodadas.c.str_modelo,tb_cadastro_rodadas.c.hr_rodada,tb_cadastro_rodadas.c.fl_psat,tb_cadastro_rodadas.c.fl_pdp,tb_cadastro_rodadas.c.fl_preliminar)\
                .where(db.and_(tb_cadastro_rodadas.c.dt_rodada == dt_rodada_date)) 
        answer = self.db_instance.conn.execute(query)

        self.db_instance.conn.close()

        return answer.all()


    def get_all_rodadas_info(self,dt_rodada_date=None):


        rodadas = self.query_rodadas_do_dia(dt_rodada_date)
        df = pd.DataFrame(rodadas, columns = ['id_rodada','str_modelo','hr_rodada','fl_psat','fl_pdp','fl_preliminar'])
        df['flag_status'] = df.apply(lambda x: 
                                    (
                                        "PRE" if (x['fl_preliminar']) 
                                        else "PDP" if (x['fl_pdp']and not x['fl_preliminar'])
                                        else  "PSAT" if x['fl_psat'] else "GPM"
                                    )
                                    if (
                                        (x['fl_preliminar']!= None) 
                                        or (x['fl_psat'] != None) 
                                        or (x['fl_pdp'] != None)
                                        ) 
                                    else None, axis=1)

        df_rodadas = df[['id_rodada','str_modelo','hr_rodada','flag_status','fl_pdp','fl_psat','fl_preliminar']]

        return df_rodadas
    

    

    def build_modelo_info_dict(self,str_dt_rodada=None,granularidade=None,interest_model=None,flag_pdp=None, flag_psat=None, flag_preliminar=None,build_all_models=False):
        
        if not interest_model and not build_all_models:
            if not self.modelo and not self.hr_rodada:
                raise(Exception("Informe o modelo e a rodada de interesse"))
            else:
                interest_model = [(self.modelo,self.hr_rodada)]

        if build_all_models:
            print("Criando info para todos os modelos!")
        
        if not str_dt_rodada:
            str_dt_rodada = self.dt_rodada
        dt_rodada_date = datetime.datetime.strptime(str_dt_rodada,"%Y-%m-%d")

        if not granularidade:
            granularidade = self.granularidade

        df_values_rodada = self.get_all_rodadas_info(dt_rodada_date)

        if not build_all_models:    
            df_filtrado = df_values_rodada[df_values_rodada.apply(lambda x: (x['str_modelo'],x['hr_rodada']) in interest_model, axis=1)].copy()
            df_filtrado['flag_status'] = pd.Categorical(df_filtrado['flag_status'], categories=['PDP', 'PSAT', 'GPM', 'PRE'], ordered=True)
            df_filtrado = df_filtrado.sort_values('flag_status')

            if all(var is not None for var in [flag_pdp, flag_psat, flag_preliminar]):

                df_filtrado = df_filtrado.loc[(df_filtrado['fl_pdp'] == flag_pdp) & 
                            (df_filtrado['fl_psat'] ==flag_psat) & 
                            (df_filtrado['fl_preliminar'] == flag_preliminar)]
                
            df_filtrado = df_filtrado[['id_rodada','str_modelo','hr_rodada','flag_status']]

            df_filtrado = df_filtrado.head(1)
        else:
            df_filtrado = df_values_rodada

        dict_values_rodada = df_filtrado[['id_rodada','str_modelo','hr_rodada','flag_status']].to_dict('records')

        params = {}
        params["dt_rodada"] = str_dt_rodada
        params["granularidade"] = granularidade
        params["rodadas"] = {}

        for rodada_item in dict_values_rodada: 
            item_values = tuple(rodada_item.values())
            id_modelo, modelo, hr, flag = item_values
            params["flag_type"] = flag
            params["rodadas"][str(id_modelo)] = modelo+str(hr).zfill(2)
        return params

    def get_enas_interesse(self):
        enas_list = []

        #datas
        dt_rodada_date_anterior = self.dt_rodada_date  - datetime.timedelta(days=1)

        #rodadas
        interest_model = [(self.modelo,self.hr_rodada)]
        model_00_base= [(self.modelo,0)]

        #ACOMPH VALUES
        dt_inicial = self.dt_rodada_date  - datetime.timedelta(days=30)
        dt_inicial = wx_opweek.getLastSaturday(dt_inicial)
        ena_acomph = rz_cache.cache_acomph(prefixo="ACOMPH",granularidade=self.granularidade,dataInicial=dt_inicial)
        if not ena_acomph:
            raise(Exception("Não há dados de Ena para o Acomph"))
        else:
            enas_list += ena_acomph,
        
        #MLT VALUES
        ena_mlt = wx_dbLib.getMlt()
        df_mlt = pd.DataFrame(ena_mlt, columns=['SUBMERCADO', 'MES', 'MLT'])
        if df_mlt.empty:
            raise(Exception("Não há dados de Ena para MLT"))
        else:
            dict_ena_mlt = df_mlt.pivot_table(columns = 'SUBMERCADO', index='MES',values='MLT').to_dict()
            enas_list += dict_ena_mlt,

        #rodada de interesse
        params_interest_model = self.build_modelo_info_dict(
            self.dt_rodada_date.strftime("%Y-%m-%d"),
            self.granularidade,
            interest_model=interest_model,
            flag_pdp=self.flag_pdp, 
            flag_psat=self.flag_psat, 
            flag_preliminar=self.flag_preliminar)
        
        ena_interesse = rz_cache.cache_rodadas_modelos(prefixo="PREVISAO_ENA", rodadas=params_interest_model)

        if not ena_interesse:
            raise(Exception("Não há dados de Ena {} do dia {} com os parametros especificados".format(interest_model,self.dt_rodada_date.strftime("%Y-%m-%d"))))
        else:
            enas_list += (self.dt_rodada_date,ena_interesse,params_interest_model['flag_type']),
        
        #rodada 00z de hoje
        if self.hr_rodada != 0:
            params_00_base_hoje = self.build_modelo_info_dict(
                self.dt_rodada_date.strftime("%Y-%m-%d"),
                self.granularidade,model_00_base)
            
            ena_00_base_hoje = rz_cache.cache_rodadas_modelos(prefixo="PREVISAO_ENA", rodadas=params_00_base_hoje)
            if not ena_00_base_hoje:
                raise(Exception("Não há dados de Ena {} do dia {}".format(model_00_base,self.dt_rodada_date.strftime("%Y-%m-%d"))))
            else:
                enas_list += (self.dt_rodada_date,ena_00_base_hoje,params_00_base_hoje['flag_type']),

        #rodada de interesse de ontem 
        params_base_ontem = self.build_modelo_info_dict(dt_rodada_date_anterior.strftime("%Y-%m-%d"),self.granularidade,interest_model)
        ena_base_ontem = rz_cache.cache_rodadas_modelos(prefixo="PREVISAO_ENA", rodadas=params_base_ontem)
        if not ena_base_ontem:
            raise(Exception("Não há dados de Ena {} do dia {}".format(interest_model,dt_rodada_date_anterior.strftime("%Y-%m-%d"))))
        else:
            enas_list += (dt_rodada_date_anterior,ena_base_ontem,params_base_ontem['flag_type']),


        return enas_list


    def gerar_plot_ena_modelos(self):

        enas_list = self.get_enas_interesse()
        if not enas_list:
            return None
        
        ena_acomph = enas_list[0]
        ena_mlt = enas_list[1]

        # Cria uma figura e uma grade de subplots 2x2
        fig, axs = plt.subplots(2, 2, figsize=(16, 10))
        axs = axs.flatten()  
        
        plot_acomph = True
        plot_mlt = True

        #(alpha,line_width, line_style)
        if len(enas_list[2:])==2:
            color_alpha_modelo=[(1,2,'-'),(1,2,':')]
        else:
            color_alpha_modelo=[(1,2,'-'),(1,2,'--'),(1,2,':')]

        for j ,(dt, ena, flag_type) in enumerate(enas_list[2:]):

                modelo_values = ena[0]
                modelo_name = modelo_values['modelo']
                ena_values = modelo_values['valores']
                hr_rodada = modelo_values['horario_rodada']

                ena_values_items = list(ena_values.items())
                ena_values_items.reverse()

                for i, (submercado, values) in enumerate(ena_values_items):

                    if plot_acomph:
                        dates_acomph = ena_acomph[submercado].keys()
                        values_acomph = ena_acomph[submercado].values()
                        axs[i].plot(dates_acomph, values_acomph, label="ACOMPH", color="darkturquoise")  # Label para acomph

                    dates_modelo = values.keys()
                    values_modelo = values.values()

                    if plot_mlt:
                        translate_submerc = {'N':1,'NE':2,'SE':3,'S':4}
                        dates_mlt = sorted(set(list(dates_acomph) + list(dates_modelo)))
                        values_mlt = [ena_mlt[translate_submerc[submercado]][datetime.datetime.strptime(date,"%Y/%m/%d").month] for date in dates_mlt]
                        axs[i].step(dates_mlt, values_mlt, label="MLT", color="black")  # Label para acomph

                    label = f"{modelo_name}_{hr_rodada}z_{flag_type}"
                    if dt != self.dt_rodada_date:
                        label+= " ({})".format(dt.strftime("%d/%m"))

                    axs[i].plot(dates_modelo, 
                                values_modelo, 
                                label=label, 
                                color=COLORS[modelo_name], 
                                alpha=color_alpha_modelo[j][0],
                                linewidth=color_alpha_modelo[j][1],
                                linestyle = color_alpha_modelo[j][2])  # Label para cada modelo

                    axs[i].set_title(submercado)
                    axs[i].legend(loc="upper left")  # Chamar a legenda sem argumentos
                    axs[i].tick_params(axis='x', rotation=45)
                    axs[i].set_ylabel("ENA (MW)")
                    axs[i].xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=TH))
                    
                    axs[i].grid(True)
                plot_acomph = False
                plot_mlt = False
        fig.suptitle("ENA Diaria - SIN", fontsize=16)
        plt.tight_layout()

        pathLocal = os.path.abspath('.')
        dir_saida = os.path.join(pathLocal, 'arquivos', 'tmp')

        if not os.path.exists(dir_saida):
            os.makedirs(dir_saida)

        pathFileOut = os.path.join(dir_saida, "ENA_Modelos.png")
        plt.savefig(pathFileOut)
        print(pathFileOut)
        return pathFileOut

if __name__ == '__main__':

    pass

    #PARAMS ENTRADA
    # dt_rodada = '28/02/2024'
    # modelo='GEFS'
    # hr = 6
    # flag_pdp=True
    # flag_psat=False
    # flag_preliminar=True

    # rodada = Rodadas(modelo=modelo,dt_rodada=dt_rodada,hr_rodada=hr,flag_pdp=flag_pdp,flag_psat=flag_psat,flag_preliminar=flag_preliminar)
    # rodada.gerar_plot_ena_modelos()

    # rodadas = Rodadas()
    # params = rodadas.build_modelo_info_dict(build_all_models=True)
    # cache_data = rz_cache.cache_rodadas_modelos("PREVISAO_ENA",params)
