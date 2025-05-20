from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import BranchPythonOperator,PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from airflow.operators.dummy import DummyOperator

import sys
sys.path.insert(0, "/WX2TB/Documentos/fontes/PMO/raizen-power-trading-api-ccee")
from main import ApiCCEE
from src.constants import MAPPING


default_args = {
    'execution_timeout': timedelta(hours=8),
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}


def check_atualizacao(**kwargs):

    execution_date = kwargs.get('dag_run').conf.get('execution_date')
    titles_to_search = kwargs.get('dag_run').conf.get('titles_to_search',[])
    ids_to_modify = kwargs.get('dag_run').conf.get('ids_to_modify',[])

    execution_date = (
        datetime.now() 
        if not execution_date 
        else datetime.strptime(execution_date,'%Y-%m-%d')
        )
    ultima_data_atualizacao = datetime.datetime.min
    if not titles_to_search:
        
        for title_name in MAPPING.keys():
            dt_atualizacao = ApiCCEE(title_name).get_date_atualizacao(title_name)

            if dt_atualizacao:
                if dt_atualizacao > ultima_data_atualizacao:
                    ultima_data_atualizacao = dt_atualizacao
                if dt_atualizacao.date() == execution_date.date():
                    titles_to_search.append(title_name)
    
    if titles_to_search:
        fontes_to_search = [title.replace("custo_variavel_unitario_","CCEE_") for title in titles_to_search]
        
        kwargs.get('dag_run').conf.update(
            {
                'fontes_to_search': fontes_to_search,
                "dt_atualizacao": ultima_data_atualizacao.strftime('%Y-%m-%d'),
                'task_to_execute': 'revisao_cvu',
                'ids_to_modify': ids_to_modify
            })
        
        kwargs['ti'].xcom_push(key='titles_to_search', value=titles_to_search)

        return "create_ccee_instance"
    return "end_task"


def create_ccee_instance(**kwargs):

    titles_to_search = kwargs['ti'].xcom_pull(task_ids='check_atualizacao', key='titles_to_search')
    ccee_operator = ApiCCEE(titles_to_search)
    return ccee_operator

def export_data_to_db(**kwargs):
    ccee_operator = kwargs['ti'].xcom_pull(task_ids='create_ccee_instance')
    ccee_operator.export_data()


with DAG(
    'API_CCEE_CVU',
    default_args=default_args,
    description='Consulta a api da ccee',
    schedule_interval = '0 14-16,18-20 * * *', 
    tags=["PROSPEC","CCEE","CVU"],
    start_date=datetime(2024, 4, 28),
    catchup=False,
    render_template_as_native_obj=True, 

) as dag:

    check_atualizacao = BranchPythonOperator(
        task_id='check_atualizacao',
        python_callable=check_atualizacao,
        provide_context=True,
    )

    create_ccee_instance = PythonOperator(
        task_id='create_ccee_instance',
        python_callable=create_ccee_instance,
        provide_context=True,
    )

    export_data_to_db = PythonOperator(
        task_id='export_data_to_db',
        python_callable=export_data_to_db,
        provide_context=True,
    )

    trigger_updater_estudo = TriggerDagRunOperator(
        task_id="trigger_prospec_updater",
        trigger_dag_id='PROSPEC_UPDATER',
        conf={'external_params': "{{dag_run.conf}}"},  
        wait_for_completion=False,  
        trigger_rule="none_failed_min_one_success",
    )

    end_task = DummyOperator(
        task_id='end_task',
    )


    check_atualizacao >> create_ccee_instance >> export_data_to_db >> trigger_updater_estudo >> end_task
    check_atualizacao >> end_task