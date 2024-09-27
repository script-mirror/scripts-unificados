import os 
import sys
import base64
import requests
import datetime 
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC



sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.bibliotecas import  wx_dbClass

PATH_PROFILE="/tmp/rust_mozprofile0vnzgM"

# Gerar strings aleatórias
def generate_random_string(length:int):
    import random
    import string
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def open_firefox(firefox_binary:str=None,driver_path:str=None):
    from selenium import webdriver
    from selenium.webdriver.firefox.options import Options
    from selenium.webdriver.firefox.service import Service

    if not firefox_binary:
        firefox_binary = "/usr/bin/firefox"
        # firefox_binary = r"C:\Users\cs399274\AppData\Local\Mozilla Firefox\firefox.exe"

    if not driver_path:
        driver_path = "/usr/bin/geckodriver"
        # driver_path= os.path.join(file_path,"geckodriver")

    options = Options()
    options.binary_location = firefox_binary
    options.add_argument('--headless')
    # options.set_preference('profile', PATH_PROFILE)
    serv = Service(driver_path)
    driver = webdriver.Firefox(options=options,service=serv)

    return driver

def get_info_estacoes():
    
    db_meteorologia = wx_dbClass.db_mysql_master('db_meteorologia')
    db_meteorologia.connect()
    tb_inmet_estacoes = db_meteorologia.getSchema("tb_inmet_estacoes")

    query_get_info_estacoes = tb_inmet_estacoes.select()
    answer = db_meteorologia.conn.execute(query_get_info_estacoes).fetchall()
    df_estacoes = pd.DataFrame(answer,columns=['cd_estacao','str_nome','str_estado','vl_lon','vl_lat','str_bacia','str_fonte'])
    return df_estacoes
 
def request_inmet_api(estacao_codigo:str,dataInicio:datetime.datetime,dataFim:datetime.datetime,token:str):

    vro2 = base64.b64encode(generate_random_string(50).encode()).decode()
    dec2 = base64.b64decode(vro2).decode()
    decvro2 = base64.b64encode(generate_random_string(50).encode()).decode()[:-1]
    seed2 = f"{dec2}&{decvro2}{vro2}"

    # Definir o payload para a requisição POST
    payload = {
        "data_inicio": dataInicio.strftime('%Y-%m-%d'),
        "data_fim": dataFim.strftime('%Y-%m-%d'),
        "estacao": estacao_codigo,
        "seed": seed2,
        "gcap": token
    }

    # URL da API
    url = "https://apitempo.inmet.gov.br/estacao/front/"

    response = requests.post(url, json=payload)

    return response

def process_inmet_data(dados_horarios:list):

    columns = ['DC_NOME', 'PRE_INS', 'TEM_SEN', 'VL_LATITUDE', 'PRE_MAX', 'UF',
       'RAD_GLO', 'PTO_INS', 'TEM_MIN', 'VL_LONGITUDE', 'UMD_MIN', 'PTO_MAX',
       'VEN_DIR', 'DT_MEDICAO', 'CHUVA', 'PRE_MIN', 'UMD_MAX', 'VEN_VEL',
       'PTO_MIN', 'TEM_MAX', 'TEN_BAT', 'VEN_RAJ', 'TEM_CPU', 'TEM_INS',
       'UMD_INS', 'CD_ESTACAO', 'HR_MEDICAO']

    df_dados_leitura_estacoes = pd.DataFrame(dados_horarios,columns=columns)
    df_dados_leitura_estacoes['DT_COLETA'] = pd.to_datetime(df_dados_leitura_estacoes[['DT_MEDICAO','HR_MEDICAO']].sum(axis=1), format='%Y-%m-%d%H%M')
    df_dados_estacoes = df_dados_leitura_estacoes[['CD_ESTACAO', 'DC_NOME', 'UF', 'VL_LONGITUDE', 'VL_LATITUDE']]
    df_dados_estacoes = df_dados_estacoes.drop_duplicates()
    
    df_dados_chuva = df_dados_leitura_estacoes[['CD_ESTACAO','DT_COLETA','CHUVA']].copy()
    
    return df_dados_estacoes, df_dados_chuva

def insert_chuva_observada(df_dados_chuva:pd.DataFrame):

    dataInicial = min(df_dados_chuva['DT_COLETA'])
    dataFinal = max(df_dados_chuva['DT_COLETA'])

    db_meteorologia = wx_dbClass.db_mysql_master('db_meteorologia')
    db_meteorologia.connect()
    tb_inmet_dados_estacoes = db_meteorologia.getSchema("tb_inmet_dados_estacoes")

    sql_delete = tb_inmet_dados_estacoes.delete().where(
        tb_inmet_dados_estacoes.c.dt_coleta.between(dataInicial, dataFinal),
        tb_inmet_dados_estacoes.c.cd_estacao.in_(df_dados_chuva['CD_ESTACAO'].unique().tolist())
        )
    
    n_values = db_meteorologia.conn.execute(sql_delete).rowcount
    print(f"{n_values} Linhas deletadas na tb_inmet_dados_estacoes!")
    
    dados_chuva = df_dados_chuva.values.tolist()
    sql_insert = tb_inmet_dados_estacoes.insert().values(dados_chuva)
    n_values = db_meteorologia.conn.execute(sql_insert).rowcount
    print(f"{n_values} Linhas inseridas na tb_inmet_dados_estacoes!")

def rotina_extract_inmet_data(dataInicio:datetime.datetime,dataFim:datetime.datetime):
    print('Iniciado Rotina Inmet.')
    if dataInicio > dataFim: data_inicial, data_fim = data_fim, data_inicial
    print(f"data inicial: {dataInicio.strftime('%d/%m/%Y')} / data final: {dataFim.strftime('%d/%m/%Y')} ")

    dados_horarios = []
    driver = open_firefox()
    driver.get("https://tempo.inmet.gov.br")

    df_info_estacoes = get_info_estacoes()
    list_estacoes = df_info_estacoes[df_info_estacoes['str_fonte'] == 'inmet']['cd_estacao'].values
    n_estacoes = len(list_estacoes)
    for i, estacao_codigo in enumerate(list_estacoes):
        try:
            # Executar o código JavaScript para obter o token
            token = driver.execute_script("""
            return new Promise((resolve) => {
                window.grecaptcha.ready(function() {
                    window.grecaptcha.execute('6LdqhOkjAAAAAHqjdF9WSjtatHC0bEohqmvrADrL', {action: 'create_comment'}).then(function(token) {
                        resolve(token);
                    });
                });
            });
            """)
            response = request_inmet_api(estacao_codigo,dataInicio,dataFim,token)
            if response.status_code == 200:
                print(f"{estacao_codigo} ({i+1}/{n_estacoes})" )
                dados_horarios += response.json()
            else:
                print(f"Falha ao recuperar a página. Código de status: {response.status_code}")
        except:
            print(f"Erro na estacao:  {estacao_codigo}")
    driver.quit()

    if dados_horarios:
        df_dados_estacoes, df_dados_chuva = process_inmet_data(dados_horarios)
        insert_chuva_observada(df_dados_chuva)

    return dados_horarios


def rotina_extract_simepar_data():
    print('Iniciado Rotina Simepar.')
    df_info_estacoes = get_info_estacoes()
    list_estacoes = df_info_estacoes[df_info_estacoes['str_fonte'] == 'simepar']['cd_estacao'].values
    driver = open_firefox() 
    values=[]
    n_estacoes = len(list_estacoes)
    for i, estacao in  enumerate(list_estacoes):
        print(f"{estacao} ({i+1}/{n_estacoes})" )

        driver.get(f"http://www.simepar.br/prognozweb/simepar/dados_estacoes/{estacao}")
        
        try:
            WebDriverWait(driver, 5).until(EC.visibility_of_element_located((By.ID, "chart2")))
            script = """var data = $('#chart2').highcharts().series[0].data;var optionsList = data.map(point => point.options);return optionsList;"""
            data = driver.execute_script(script)
            df_values = pd.DataFrame(data)

            df_values['cd_estacao'] = estacao

            #tranformando para utc
            df_values['name'] = (pd.to_datetime(df_values['name'], dayfirst=True) + datetime.timedelta(hours=3)).dt.strftime("%Y-%m-%d %H:%M:%S")
            values+= df_values.values.tolist()
        except:
            print(f"Estacao com problema ou não há dados: {estacao}")
    driver.close()
    driver.quit()

    df_dados_chuva = pd.DataFrame(values, columns = ['DT_COLETA','vl_chuva','CD_ESTACAO'])
    insert_chuva_observada(df_dados_chuva[['CD_ESTACAO','DT_COLETA','vl_chuva']])


        
def helper():
    print(f"Opcao 1: {sys.argv[0]} range %d/%m/%Y(inicial) %d/%m/%Y(final)")
    print(f"Opcao 2: {sys.argv[0]} data %d/%m/%Y(inicial e final)")


if __name__ == '__main__':

    if len(sys.argv) > 1:

        fl_run = False
        for i in range(len(sys.argv)):
            argumento = sys.argv[i].lower()

            #ARGUMENTOS INICIAIS
            if argumento == 'range':
                dt_ini=datetime.datetime.strptime(sys.argv[i+1], '%d/%m/%Y')
                dt_fim=datetime.datetime.strptime(sys.argv[i+2], '%d/%m/%Y')
                fl_run = True
                fl_run = 'inmet'
                break

            elif argumento == 'data':
                dt_ini=datetime.datetime.strptime(sys.argv[i+1], '%d/%m/%Y')
                dt_fim = dt_ini
                fl_run = 'inmet'
                break

            elif argumento == 'simepar':
                fl_run = 'simepar'
                break

        if fl_run == 'inmet':
            dados_horarios = rotina_extract_inmet_data(dt_ini,dt_fim)
        
        elif fl_run == 'simepar':
            rotina_extract_simepar_data()
            
        else:
            print("Verificar paramentros de entrada, nao sera Rodado!")
            helper()

    else:
        dt_ini=datetime.datetime.now()
        dt_fim = dt_ini
        dados_horarios = rotina_extract_inmet_data(dt_ini,dt_fim)

        
    
