
import sys
import json
import datetime

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator



sys.path.insert(0, "/WX2TB/Documentos/fontes/PMO/raizen-power-trading-previsao-hidrologia/smap")
from main import SMAP


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2024, 7, 10),
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=5),
    
}

def create_smap_object(**kwargs):

    # Se precisar inserir caminho no sys.path:

    lista_modelos = kwargs["dag_run"].conf.get("modelos", [])
    flag_estendido = kwargs["dag_run"].conf.get("prev_estendida", False)
    modelos = [(item[0], item[1], datetime.datetime.strptime(item[2], '%Y-%m-%d').date()) for item in lista_modelos] 
    
    smap_operator = SMAP(modelos=modelos,flag_estendido=flag_estendido)

    return smap_operator

def build_arq_entrada(**kwargs):
    """
    Recupera o objeto SMAP via XCom e chama build_arq_entrada().
    """
    ti = kwargs['ti']
    smap_operator = ti.xcom_pull(task_ids='create_smap_object')
    smap_operator.build_arq_entrada()


def import_vazao_prevista(**kwargs):
    """
    Recupera o objeto SMAP via XCom e chama import_vazao_prevista().
    """
    ti = kwargs['ti']
    smap_operator = ti.xcom_pull(task_ids='create_smap_object')
    cenarios_inseridos = smap_operator.import_vazao_prevista()
    cenario = cenarios_inseridos[0]
    
    if isinstance(cenario['dt_rodada'], datetime.date):
        cenario['dt_rodada'] = cenario['dt_rodada'].isoformat()

    kwargs.get('dag_run').conf['cenario'] = cenario
    

with DAG(
    dag_id='PREV_SMAP',
    default_args=default_args,
    description='DAG que instancia SMAP e chama métodos em tasks separadas',
    schedule_interval=None,
    max_active_runs=1,
    concurrency=1,
    render_template_as_native_obj=True, 
    catchup=False
) as dag:

    
    t_create_smap = PythonOperator(
        task_id='create_smap_object',
        python_callable=create_smap_object
    )

    # Task 1
    t_build_arq_entrada = PythonOperator(
        task_id='build_arq_entrada',
        python_callable=build_arq_entrada
    )

    t_run_smap = SSHOperator(
        task_id='run_container',
        ssh_conn_id='ssh_master',
        command='cd /WX2TB/Documentos/fontes/PMO/raizen-power-trading-previsao-hidrologia/smap; docker-compose up',
        get_pty=True,
        conn_timeout = 36000,
        cmd_timeout = 28800,
    )

    # Task 4
    t_import_vazao_prevista = PythonOperator(
        task_id='import_vazao_prevista',
        python_callable=import_vazao_prevista
    )

    t_run_previvaz = TriggerDagRunOperator(
        task_id="trigger_previvaz",
        trigger_dag_id='PREV_PREVIVAZ',
        conf={'cenario': "{{ dag_run.conf.get('cenario')}}"},  
        wait_for_completion=False,  
        trigger_rule="none_failed_min_one_success",

    )
    
    # Definindo a sequência
    t_create_smap >> t_build_arq_entrada >> t_run_smap >> t_import_vazao_prevista >> t_run_previvaz
