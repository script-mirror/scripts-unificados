import os
import sys
import datetime
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

path_fontes = "/WX2TB/Documentos/fontes/"
sys.path.insert(1,path_fontes)
from PMO.scripts_unificados.apps.pconjunto import previsao_conjunto_ONS_shadow

PATH_PCONJUNTO = os.path.join(path_fontes, "PMO", "scripts_unificados","apps","pconjunto") 
PATH_ROTINA_CONJUNTO = os.path.join(PATH_PCONJUNTO,"pconjunto-ONS")


def testa_modelo_file(str_modelo):
    
    str_modelo =str_modelo.upper()
    
    test_dst = os.path.join(PATH_ROTINA_CONJUNTO, 'Arq_Entrada', str_modelo, f'{str_modelo}_m_{datetime.datetime.now().strftime("%d%m%y")}.dat')
    
    print(test_dst)
    if not os.path.exists(test_dst):
        raise AirflowException(f"Arquivo não encontrado!")
    return 

def run_pconjunto(**kwargs):

    params = kwargs.get('dag_run').conf
    dt_rodada = params.get('dt_rodada')
    forcar_rodar = params.get('forcar_rodar',False)

    if not dt_rodada: dt_rodada = datetime.datetime.now().replace(hour=0,minute=0,second=0)
    if type(dt_rodada) == str:
        dt_rodada = datetime.datetime.strptime(dt_rodada, '%Y-%m-%d')
    
    previsao_conjunto_ONS_shadow.previsao_conjunto_ONS(
        {
            "data": dt_rodada,
            "forcarRodar": forcar_rodar
        }
    )


with DAG(
    dag_id = 'PCONJUNTO', 
    tags=["ONS","SMAP","Chuva Prevista"],
    start_date=datetime.datetime(2024, 4, 28), 
    schedule_interval=None, 
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    ) as dag:

    inicio = DummyOperator(
        task_id='inicio',
        trigger_rule="none_failed_min_one_success",

    )

    modeloGefs = PythonOperator(
        trigger_rule="all_done",
        task_id='GEFS_file',
        python_callable=testa_modelo_file,
        op_args=["GEFS"]
    )
    
    modeloETA = PythonOperator(
        trigger_rule="all_done",
        task_id='ETA_file',
        python_callable=testa_modelo_file,
        op_args=["ETA40"]
    )

    modeloECMWF = PythonOperator(
        trigger_rule="all_done",
        task_id='ECMWF_file',
        python_callable=testa_modelo_file,
        op_args=["ECMWF"]
    )
    
    RodaPconjunto = PythonOperator(
        trigger_rule="all_done",
        task_id='roda_pconjunto',
        python_callable=run_pconjunto,
    )


    inicio >> [modeloGefs >> modeloETA >> modeloECMWF] >> RodaPconjunto


