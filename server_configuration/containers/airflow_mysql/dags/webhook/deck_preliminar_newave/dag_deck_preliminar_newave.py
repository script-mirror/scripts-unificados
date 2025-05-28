import datetime
import sys
import os
from airflow.models.dag import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)
from service_deck_preliminar_newave import DeckPreliminarNewaveService

utils_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'utils')
sys.path.insert(0, utils_path)
from utils.whatsapp_message import WhatsappMessageSender


# Definição da DAG simplificada
with DAG(
    dag_id='DECK_PRELIMINAR_NEWAVE',
    start_date=datetime.datetime(2024, 4, 28),
    catchup=False,
    schedule=None,
    tags=['Deck Preliminar', 'NEWAVE', 'WEBHOOK'],
    default_args={
        'retries': 3,
        'retry_delay': datetime.timedelta(minutes=2),
        'on_failure_callback': WhatsappMessageSender.enviar_whatsapp_erro,
    },
    description='DAG simplificada para processamento do Deck Preliminar Newave'
) as dag:

    # 1. Validar dados de entrada
    importar_deck = PythonOperator(
        task_id='importar_deck_preliminar_newave',
        python_callable=DeckPreliminarNewaveService.importar_deck_preliminar_newave,
        provide_context=True,
    )

    # 3. Finalizar
    finalizar = DummyOperator(
        task_id='finalizar', 
        on_success_callback=WhatsappMessageSender.enviar_whatsapp_sucesso,
    )

    # Fluxo linear atualizado com nova task para envio de dados à API
    importar_deck >> finalizar