import os
import sys
import time
import zipfile
import datetime

from airflow.models.dag import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowException
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")), ".env"))

PATH_PROJETO = os.getenv("PATH_PROJETO", "/WX2TB/Documentos/fontes/PMO")
sys.path.insert(1,f"{PATH_PROJETO}/scripts_unificados")
from apps.dessem.libs import wx_mainBalancoDS
from apps.dbUpdater.libs import deck_nw,deck_ds
from apps.verificadores.ccee import rz_download_decks_ccee
from apps.gerarProdutos import gerarProdutos2 
from bibliotecas import wx_opweek, rz_dir_tools




def check_file_exist(path_zip,dt_ref):

    ultimoSabado = wx_opweek.getLastSaturday(dt_ref)
    semanaEletrica = wx_opweek.ElecData(ultimoSabado.date())

    resultDiaZip = 'DS_CCEE_{:0>2}{}_SEMREDE_RV{}D{}.zip'
    resultDiaZip =resultDiaZip.format(
        semanaEletrica.mesReferente, 
        semanaEletrica.anoReferente, 
        semanaEletrica.atualRevisao, 
        dt_ref.strftime('%d'))

    with zipfile.ZipFile(path_zip, 'r') as zip_file:
        for file in zip_file.filelist:
            if file.filename.lower() == resultDiaZip.lower():
                path_saida = zip_file.extract("Resultado_"+file.filename)
                path_entrada = zip_file.extract(file)
                print(path_saida, path_entrada)
                return path_entrada, path_saida
        return None, None
    
    
def download_deck_ds(**kwargs):
    try:

        dt_ref = kwargs.get('dag_run').conf.get('dt_ref')
        dt_ref = (datetime.datetime.now().replace(hour=0,minute=0,second=0) + datetime.timedelta(days=1)) if not dt_ref else datetime.datetime.strptime(dt_ref,'%Y-%m-%d')
        
        print(dt_ref)

        path_arquivos = "/WX2TB/Documentos/fontes/PMO/decks/ccee/ds"

        pastaDeck = dt_ref.strftime('%Y%m%d')
        path_out_deck = '/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/dessem/arquivos/'+ pastaDeck 
        
        while 1:
            path_zip = rz_download_decks_ccee.download_decks_acervo_ccee(
                pathArquivos = path_arquivos, 
                nome_deck_pesquisado = "Dessem"
            )
            zip_interno_entrada, zip_interno_saida =  check_file_exist(path_zip,dt_ref)
            if zip_interno_entrada:
                DIR_TOOLS = rz_dir_tools.DirTools()
                path_entrada = os.path.join(path_out_deck,'entrada','ccee_entrada') 
                path_saida = os.path.join(path_out_deck,'entrada','ccee_saida') 

                DIR_TOOLS.extract(zip_interno_entrada, path_entrada)
                DIR_TOOLS.extract(zip_interno_saida, path_saida,deleteAfterExtract=True)
                
                print(DIR_TOOLS.extrair_zip_mantendo_nome_diretorio(zip_interno_entrada, path_out=path_arquivos, deleteAfterExtract=True))
                break
            
            print("Não encontrado, nova tentativa em 5 min!")
            time.sleep(600)

        kwargs['ti'].xcom_push(key='path', value=path_zip)
        kwargs['ti'].xcom_push(key='dt_ref', value=dt_ref)
        return ['dbUpdater_ds']
    
    except Exception as e:
        raise AirflowException(f"[ERROR] Não foi possivel executar a funcao!: {str(e)}")
    

def importar_deck_ds(**kwargs):

    GERAR_PRODUTO = gerarProdutos2.GeradorProdutos()

    try:
        ti = kwargs['ti']
        path = ti.xcom_pull(task_ids='download_deck_ds', key='path')
        dt_ref = ti.xcom_pull(task_ids='download_deck_ds', key='dt_ref')

        print(dt_ref)

        deck_ds.importar_deck_values_ds(path_zip=path,dt_ref= dt_ref,str_fonte='ccee')

        GERAR_PRODUTO.enviar({
            "produto":"RESULTADO_DESSEM",
            "data":dt_ref,
        })
        wx_mainBalancoDS.main(dt_ref.strftime("%Y%m%d"))
        
        #se for quinta
        if datetime.datetime.now().weekday() == 3:
            return ['download_deck_nw']
        else:
            return ['fim']
    
    except Exception as e:
        raise AirflowException(f"[ERROR] Não foi possivel executar a funcao!: {str(e)}")

def download_deck_nw(**kwargs):
    try:
        path = rz_download_decks_ccee.download_decks_acervo_ccee(
            pathArquivos = "/WX2TB/Documentos/fontes/PMO/decks/ccee/nw", 
            nome_deck_pesquisado = "Newave"
        )
        kwargs['ti'].xcom_push(key='path', value=path)
        return ['dbUpdater_nw']
    except Exception as e:
        raise AirflowException(f"[ERROR] Não foi possivel executar a funcao!: {str(e)}")

def importar_deck_nw(**kwargs):
    try:
        ti = kwargs['ti']
        path = ti.xcom_pull(task_ids='download_deck_nw', key='path')

        data_produto = deck_nw.importar_deck_values_nw(path)
        kwargs['dag_run'].conf.update(
            {'origem': 'ccee',
             'product_details': {'dataProduto': data_produto}}
        )
        return ['fim']
    
    except Exception as e:
        raise AirflowException(f"[ERROR] Não foi possivel executar a funcao!: {str(e)}")
    
    
default_args = {

    'execution_timeout': datetime.timedelta(hours=8)
}
    
with DAG(
    default_args = default_args,
    dag_id='DOWNLOAD_CCEE',
    tags=["Verificador","CCEE","DS","DC","NW","WhatsApp"],
    start_date=datetime.datetime(2024, 4, 28),
    catchup=False,
    schedule="0 16 * * *"
    
) as dag:

    # começo estrutura para rodar a sequencia das tarefas
    inicio = DummyOperator(
        task_id='inicio',
        trigger_rule="none_failed_min_one_success",
    )

    downloadCCEE_ds = BranchPythonOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='download_deck_ds',
        python_callable=download_deck_ds,
        provide_context=True,
        execution_timeout=datetime.timedelta(hours=8),
    )

    dbUpdater_ds = BranchPythonOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='dbUpdater_ds',
        python_callable=importar_deck_ds,
        
    )

    downloadCCEE_nw = BranchPythonOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='download_deck_nw',
        python_callable=download_deck_nw,
        provide_context=True,
    )

    dbUpdater_nw = PythonOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='dbUpdater_nw',
        python_callable=importar_deck_nw,
        
    )
#     {
#     'origem':'ccee',
#     'product_details':{
#     'dataProduto':'202501'
#     }
    
# }
    trigger_dag_diff_carga = TriggerDagRunOperator(
        task_id="trigger_dag_diff_carga",
        trigger_dag_id='DECK_PRELIMINAR_NEWAVE',
        conf={'{{dag_run.conf}}'},  
        wait_for_completion=False,  
        trigger_rule="none_failed_min_one_success",
    )

    fim = DummyOperator(
        task_id='fim',
        trigger_rule="none_failed_min_one_success",
    )

    inicio >> downloadCCEE_ds >> dbUpdater_ds
    dbUpdater_ds >> fim
    dbUpdater_ds >> downloadCCEE_nw >> dbUpdater_nw >> trigger_dag_diff_carga >> fim