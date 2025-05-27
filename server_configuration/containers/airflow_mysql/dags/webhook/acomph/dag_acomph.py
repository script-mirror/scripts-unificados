import sys
import os
import datetime

# Adicionar o diretório atual ao path para encontrar o serviço
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
# Agora importa o serviço do mesmo diretório
from service_acomph import AcomphService

from airflow.models.dag import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id='ACOMPH',
    start_date=datetime.datetime(2024, 4, 28),
    catchup=False,
    schedule=None,
    tags=['Webhook', 'Acomph'],
    default_args={
        'retries': 4,
        'retry_delay': datetime.timedelta(minutes=5),
        
    },
    render_template_as_native_obj=True

) as dag:
    
    importar_acomph = BranchPythonOperator(
        task_id='importar_acomph',
        python_callable=AcomphService.importar_acomph,
        provide_context=True,
    )
    
    exportar_acomph = BranchPythonOperator(
        task_id='exportar_acomph',
        python_callable=AcomphService.exportar_acomph,
        provide_context=True,
    )
    
    enviar_conteudo = BranchPythonOperator(
        task_id='enviar_conteudo',
        python_callable=AcomphService.enviar_conteudo,
        provide_context=True,
    )
    
    triggar_dag_PROSPEC = TriggerDagRunOperator(
        task_id='trigger_dag_PROSPEC',
        trigger_rule="none_failed_min_one_success",
        trigger_dag_id='',
        execution_date="{{execution_date}}",
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=60,
        conf={"product_details": "{{dag_run.conf.get('product_details')}}"}
    )



