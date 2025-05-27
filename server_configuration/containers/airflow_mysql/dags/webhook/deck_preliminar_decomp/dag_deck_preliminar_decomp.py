import datetime
import sys
import os
from airflow.models.dag import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator

# Adicionando o path do diretório atual para importar o service
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from service_deck_preliminar_decomp import DeckPreliminarDecompService
# # Importando o service layer


# Definição da DAG simplificada
with DAG(
    dag_id='deck_preliminar_decomp',
    start_date=datetime.datetime(2024, 4, 28),
    catchup=False,
    schedule=None,
    tags=['Deck Preliminar', 'DECOMP', 'WEBHOOK'],
    default_args={
        'retries': 3,
        'retry_delay': datetime.timedelta(minutes=2),
        'on_failure_callback': DeckPreliminarDecompService.enviar_whatsapp_erro,
    },
    description='DAG simplificada para processamento do Deck Preliminar Decomp'
) as dag:

    # 1. Validar dados de entrada
    validar_dados = PythonOperator(
        task_id='validar_dados_entrada',
        python_callable=DeckPreliminarDecompService.validar_dados_entrada,
        provide_context=True,
    )

    # 2. baixar arquivos
    download_arquivos = PythonOperator(
        task_id='download_arquivos',
        python_callable=DeckPreliminarDecompService.download_arquivos,
        provide_context=True,
    )

    # 3. Extrair arquivos 
    extrair_arquivos = PythonOperator(
        task_id='extrair_arquivos',
        python_callable=DeckPreliminarDecompService.extrair_arquivos,
        provide_context=True,
    )


    # 4. Processar arquivos
    processar_arquivos = PythonOperator(
        task_id='processar_arquivos',
        python_callable=DeckPreliminarDecompService.processar_deck_preliminar_decomp,
        provide_context=True
    )
    
    # 5. Enviar dados para API
    enviar_para_api = PythonOperator(
        task_id='enviar_dados_para_api',
        python_callable=DeckPreliminarDecompService.enviar_dados_para_api,
        provide_context=True
    )

    # 6. Finalizar
    finalizar = DummyOperator(
        task_id='finalizar', 
        on_success_callback=DeckPreliminarDecompService.enviar_whatsapp_sucesso,
    )

    # Fluxo linear atualizado com nova task para envio de dados à API
    validar_dados >> download_arquivos >> extrair_arquivos >> processar_arquivos >> enviar_para_api >> finalizar