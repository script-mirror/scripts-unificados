import datetime
import sys
import os
from airflow.models.dag import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)
from service_previsoes_carga_mensal_patamar_newave import PrevisoesCargaNewaveService

utils_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'utils')
sys.path.insert(0, utils_path)
from utils.whatsapp_message import WhatsappMessageSender


with DAG(
    dag_id='PREVISOES_CARGA_MENSAL_PATAMAR_NEWAVE',
    start_date=datetime.datetime(2024, 4, 28),
    catchup=False,
    schedule=None,
    tags=['Carga Mensal', 'NEWAVE', 'WEBHOOK'],
    default_args={
        'retries': 3,
        'retry_delay': datetime.timedelta(minutes=2),
        'on_failure_callback': WhatsappMessageSender.enviar_whatsapp_erro,
    },
    description='DAG simplificada para processamento das Previsões de Carga Mensal por Patamar do Newave.'
) as dag:
    
    # 1. Validar dados de entrada
    validar_dados_entrada = PythonOperator(
        task_id='validar_dados_entrada',
        python_callable=PrevisoesCargaNewaveService.validar_dados_entrada,
        provide_context=True,
    )
    
    # 2. Download dos arquivos que chegaram via webhook
    download_arquivos = PythonOperator(
        task_id='download_arquivos',
        python_callable=PrevisoesCargaNewaveService.download_arquivos,
        provide_context=True,
    )

    # 3. Extrair os arquivos do ZIP
    extrair_arquivos = PythonOperator(
        task_id='extrair_arquivos',
        python_callable=PrevisoesCargaNewaveService.extrair_arquivos,
        provide_context=True,
    )
    
    # 4. Importar os dados via POST para ApiV2 no banco 'db_decks' e na entidade 'tb_nw_carga'
    enviar_dados_para_api = PythonOperator(
        task_id='enviar_dados_para_api',
        python_callable=PrevisoesCargaNewaveService.enviar_dados_para_api,
        provide_context=True,
    )
    
    # 5. Gerar HTML com dados do produto e enviar para o WhatsApp e Email
    enviar_produto = PythonOperator(
        task_id='enviar_produto',
        python_callable=PrevisoesCargaNewaveService.enviar_produto,
        provide_context=True,
    )
    
    # 6. Triggar DAG externa do Prospec Updater
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
    
    # 7. Finalizar
    finalizar = DummyOperator(
        task_id='finalizar', 
        on_success_callback=WhatsappMessageSender.enviar_whatsapp_sucesso,
    )

    validar_dados_entrada >> download_arquivos >> extrair_arquivos >> enviar_dados_para_api >> enviar_produto >> triggar_dag_PROSPEC >> finalizar