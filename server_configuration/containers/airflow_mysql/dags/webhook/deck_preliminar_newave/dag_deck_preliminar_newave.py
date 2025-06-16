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
    validar_dados_entrada = PythonOperator(
        task_id='validar_dados_entrada',
        python_callable=DeckPreliminarNewaveService.validar_dados_entrada,
        provide_context=True,
    )
    
    # 2. Download dos arquivos que chegaram via webhook
    download_arquivos = PythonOperator(
        task_id='download_arquivos',
        python_callable=DeckPreliminarNewaveService.download_arquivos,
        provide_context=True,
    )

    # 3. Extrair os arquivos do ZIP
    extrair_arquivos = PythonOperator(
        task_id='extrair_arquivos',
        python_callable=DeckPreliminarNewaveService.extrair_arquivos,
        provide_context=True,
    )
    
    # 4. Processar os arquivos C_ADIC.DAT do produto com o script da ONS
    processar_deck_nw_cadic = PythonOperator(
        task_id='processar_deck_nw_cadic',
        python_callable=DeckPreliminarNewaveService.processar_deck_nw_cadic,
        provide_context=True,
    )
    
    # 5. Processar os arquivos SISTEMA.DAT do produto  com o script da ONS
    processar_deck_nw_sist = PythonOperator(
        task_id='processar_deck_nw_sist',
        python_callable=DeckPreliminarNewaveService.processar_deck_nw_sist,
        provide_context=True,
    )
    
    # 6. Enviar os dados processados para a API do PMO
    enviar_dados_para_api = PythonOperator(
        task_id='enviar_dados_para_api',
        python_callable=DeckPreliminarNewaveService.enviar_dados_para_api,
        provide_context=True,
    )
    
    # 7. Gerar tabela de diferença de cargas
    gerar_tabela_diferenca_cargas = PythonOperator(
        task_id='gerar_tabela_diferenca_cargas',
        python_callable=DeckPreliminarNewaveService.gerar_tabela_diferenca_cargas,
        provide_context=True,
    )
    
    # 8. Enviar tabela para whatsapp e e-mail
    enviar_tabela_whatsapp_email = PythonOperator(
        task_id='enviar_tabela_whatsapp_email',
        python_callable=DeckPreliminarNewaveService.enviar_tabela_whatsapp_email,
        provide_context=True,
    )
    
    # 9. Finalizar
    finalizar = DummyOperator(
        task_id='finalizar', 
        on_success_callback=WhatsappMessageSender.enviar_whatsapp_sucesso,
    )

    validar_dados_entrada >> download_arquivos >> extrair_arquivos >> [processar_deck_nw_cadic, processar_deck_nw_sist] >> enviar_dados_para_api >> gerar_tabela_diferenca_cargas >> enviar_tabela_whatsapp_email >> finalizar 