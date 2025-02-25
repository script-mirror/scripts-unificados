
import datetime
import sys

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator


sys.path.insert(0, "/WX2TB/Documentos/fontes/PMO/raizen-power-trading-previsao-hidrologia/previvaz")
from main import PrevivazRunner


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2024, 7, 10),
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=5),
    
}

def create_previvaz_object(**kwargs):

    # Se precisar inserir caminho no sys.path:

    cenario = kwargs["dag_run"].conf.get("cenario", {})
    if isinstance(cenario, str):
        cenario = eval(cenario)
    print(cenario)
    previvaz_operator = PrevivazRunner(cenario=cenario,encadeado=False)
    previvaz_operator.get_info_vazoes()


    return previvaz_operator

def build_arq_entrada(**kwargs):
    """
    Recupera o objeto previvaz via XCom e chama build_arq_entrada().
    """

    ti = kwargs['ti']
    previvaz_operator = ti.xcom_pull(task_ids='create_previvaz_object')
    previvaz_operator.build_arq_entrada(previvaz_operator.prox_rv_date)


def import_vazao_prevista(**kwargs):
    """
    Recupera o objeto previvaz via XCom e chama import_vazao_prevista().
    """
    ti = kwargs['ti']
    previvaz_operator = ti.xcom_pull(task_ids='create_previvaz_object')
    previvaz_operator.import_vazao_prevs(previvaz_operator.prox_rv_date)



with DAG(
    dag_id='PREV_PREVIVAZ',
    default_args=default_args,
    description='DAG que instancia previvaz e chama métodos em tasks separadas',
    schedule_interval=None,
    max_active_runs=1,
    concurrency=1,
    render_template_as_native_obj=True, 
    catchup=False
) as dag:
    
    t_create_previvaz = PythonOperator(
        task_id='create_previvaz_object',
        python_callable=create_previvaz_object
    )

    # Task 1
    t_build_arq_entrada = PythonOperator(
        task_id='build_arq_entrada',
        python_callable=build_arq_entrada
    )

    t_run_previvaz = SSHOperator(
        task_id='run_container',
        ssh_conn_id='ssh_master',
        command='cd /WX2TB/Documentos/fontes/PMO/raizen-power-trading-previsao-hidrologia/previvaz; docker-compose up',
        get_pty=True,
        conn_timeout = 36000,
        cmd_timeout = 28800,
    )

    # Task 4
    t_import_vazao_prevista = PythonOperator(
        task_id='import_vazao_prevista',
        python_callable=import_vazao_prevista
    )

    # Definindo a sequência
    t_create_previvaz >> t_build_arq_entrada >> t_run_previvaz >> t_import_vazao_prevista