import os 
import pdb
import sys
import glob
import time
import shutil
import datetime
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.bibliotecas import rz_dir_tools
from PMO.scripts_unificados.apps.smap.libs.Rodadas import Rodadas
from PMO.scripts_unificados.apps.verificadores import rz_selenium
from PMO.scripts_unificados.apps.verificadores.ons import rz_ons


file_path = os.path.dirname(os.path.abspath(__file__))
path_app = os.path.dirname(file_path)
path_local_tmp = os.path.join(path_app,'tmp')

PATH_TMP_DOWNLOAD = os.path.join(path_local_tmp,str(time.time()))

DIR_TOOLS = rz_dir_tools.DirTools()




def check_target_file(driver ,dt_rodada):

    
    driver.get("https://sintegre.ons.org.br/sites/9/38/paginas/servicos/historico-de-produtos.aspx?produto=Previs%C3%A3o%20Estendida%20de%20Precipita%C3%A7%C3%A3o%20do%20ECMWF%20com%20remo%C3%A7%C3%A3o%20de%20vi%C3%A9s%20(EST_rv)")
    
    WebDriverWait(driver, 60).until(
    EC.visibility_of_element_located((By.ID, "produtos"))
    )

    dt_file_ons = (dt_rodada + datetime.timedelta(days=1)).strftime('%Y%m%d')


    script = """
    for (var div of document.getElementById("produtos").children) {{
    fileDiv = div.children[1].children[1].children[0]
        if(fileDiv.textContent.includes('{}')){{
        return fileDiv.href
        }}
    }}
    """.format(dt_file_ons)
    file_link = driver.execute_script(script)

    if not file_link: return None
    driver.get(file_link)
    path_filename = os.path.join(PATH_TMP_DOWNLOAD,os.path.basename(file_link))
    
    if rz_selenium.is_file_downloaded(path_filename):
        DIR_TOOLS.extract(path_filename, PATH_TMP_DOWNLOAD)
        return PATH_TMP_DOWNLOAD   
    else:
        print(f"Arquivos para data {dt_rodada.strftime('%Y-%m-%d')}, não encontrados!")
        DIR_TOOLS.remove_src(PATH_TMP_DOWNLOAD)
        return None
    
def get_prob(path):
    
    prob_file = glob.glob(os.path.join(path,"**","prob.dat"), recursive=True)
    if not prob_file: 
        print(
            f"Não foi possivel encontrar o arquivo prob.dat"
            )
        return None
    return pd.read_csv(prob_file[0], header=None) 
    
def calc_ecmwf_clust(path,dt_rodada):

    df_prob = get_prob(path)
    if isinstance(df_prob, type(None)):
        return None
    
    dataframes = []
    for clust in range(1,11):

        clust_file = glob.glob(os.path.join(
            path,"**",f"ECMWF_m_{dt_rodada.strftime('%d%m%y')}_c{clust}.dat"), 
            recursive=True
            )
        if not clust_file: 
            print(
            f"Não foi possivel encontrar o arquivo ECMWF_m_{dt_rodada.strftime('%d%m%y')}_c{clust}.dat"
            )
            return None
        
        df_ecmwf_values = pd.read_fwf(clust_file[0], header=None)
        df_ecmwf_values.columns = ['vl_lon','vl_lat'] + pd.date_range(start=dt_rodada.strftime("%Y-%m-%d"), end=((dt_rodada+datetime.timedelta(days=44)).strftime("%Y-%m-%d"))).tolist()            
        df_ecmwf_values['clust_prob'] = df_prob.loc[clust-1][0]
        dataframes += [df_ecmwf_values]


    teste = pd.concat(dataframes)
    prob_sum = teste['clust_prob'].unique().sum()
    for col in teste.columns[2:]: teste[col] = teste[col]* teste['clust_prob'] 
    df_clusts = teste.groupby(['vl_lat','vl_lon']).sum() / prob_sum
    
    df_clusts['cenario'] = f"ECMWF-CLUST_{dt_rodada.strftime('%Y-%m-%d')}_18"
    df_clusts = df_clusts.reset_index().drop("clust_prob",axis=1)

    df_ec_grupos = None
    for grupo in range(10):
        df_grupo_tmp = dataframes[grupo].drop("clust_prob",axis=1)
        df_grupo_tmp['cenario'] = f"ECMWF-G{grupo+1}_{dt_rodada.strftime('%Y-%m-%d')}_18"
        df_clusts = pd.concat([df_clusts,df_grupo_tmp])
        
    return df_clusts, df_prob

def copiar_arquivos_previsao(path, dt_rodada):
    arquivos = glob.glob(os.path.join(path,"**",f"ECMWF_m_{dt_rodada.strftime('%d%m%y')}_c*.dat"), recursive=True)
    arquivos += glob.glob(os.path.join(path,"**",f"prob.dat"), recursive=True)
    pathDest = os.path.abspath('/WX2TB/Documentos/saidas-modelos/cluster_ons')
    for pathSrc in arquivos:
        fullPathDst = os.path.join(pathDest, os.path.basename(pathSrc))
        shutil.copyfile(pathSrc, fullPathDst)
        print(fullPathDst)    
    
def rotina_ecmwf_ons(dt_rodada:datetime.datetime):
    
    driver = rz_selenium.abrir_undetected_chrome(path_tmp_download=PATH_TMP_DOWNLOAD)

    os.makedirs(PATH_TMP_DOWNLOAD, exist_ok=True)

    dt_rodada = dt_rodada.replace(hour=0,minute=0,second=0)
    while 1:

        rz_ons.login_ons(driver)

        path = check_target_file(driver,dt_rodada)
        driver.close()
        driver.quit()
        if path: break
        print("Não encontrado, reiniciando busca em 5 min")
        time.sleep(60*5)

    df_clusts_ponderado, df_prob = calc_ecmwf_clust(path,dt_rodada)
    
    copiar_arquivos_previsao(path, dt_rodada)

    DIR_TOOLS.remove_src(PATH_TMP_DOWNLOAD)

    if type(df_clusts_ponderado) != type(pd.DataFrame()) : 
        raise "Não foi possivel concluir a rotina, Encerrando"
    
    if not df_clusts_ponderado.empty:
        RODADAS = Rodadas()

        filtro = df_clusts_ponderado['cenario'] == f'ECMWF-CLUST_{dt_rodada.strftime("%Y-%m-%d")}_18'
        RODADAS.importar_chuva_modelos(
            modelos_list=[
                ('ECMWF-CLUST',18,dt_rodada),
            ],
            df_prev_chuva_out=df_clusts_ponderado[filtro]
        )
        
        for i_grupo in range(1,11):
            filtro = df_clusts_ponderado['cenario'] == f'ECMWF-G{i_grupo}_{dt_rodada.strftime("%Y-%m-%d")}_18'
            RODADAS.importar_chuva_modelos(
                modelos_list=[
                    (f'ECMWF-G{i_grupo}',18,dt_rodada),
                ],
                df_prev_chuva_out=df_clusts_ponderado[filtro]
            )
        
        RODADAS.importar_probabilidade_grupos_ecmwf(
            modelos_list=[('ECMWF-CLUST',18,dt_rodada)],
            df_probabilidade_grupos=df_prob
            )
    

    
if __name__ == "__main__":

    dt = datetime.datetime.now().replace(hour=0,minute=0,second=0)
    if len(sys.argv) > 1:

        fl_run = False
        for i in range(len(sys.argv)):
            argumento = sys.argv[i].lower()

            #ARGUMENTOS INICIAIS
            if argumento == 'data_rodada':
                dt = datetime.datetime.strptime(sys.argv[i+1],'%Y-%m-%d')

    rotina_ecmwf_ons(dt)
                

