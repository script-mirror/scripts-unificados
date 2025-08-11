"""
DAG para processamento de Carga por Patamar DECOMP.
"""

import datetime
import sys
import os
from dotenv import load_dotenv
from airflow.models.dag import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# Configuração de paths
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

# Importar tasks específicas
from tasks import CargaPatamarDecompService

# Importar utilitários
utils_path = os.path.join(os.path.dirname(os.path.dirname(current_dir)), 'utils')
sys.path.insert(0, utils_path)
from whatsapp_message import WhatsappMessageSender

# Carregar variáveis de ambiente
load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")), '.env'))

with DAG(
    dag_id='CARGA_PATAMAR_DECOMP',
    start_date=datetime.datetime(2024, 4, 28),
    catchup=False,
    schedule=None,
    tags=['CARGA', 'PATAMAR', 'DECOMP', 'WEBHOOK'],
    default_args={
        'retries': 3,
        'retry_delay': datetime.timedelta(minutes=2),
        'on_failure_callback': WhatsappMessageSender.enviar_whatsapp_erro,
    },
    description='DAG para processamento de Carga por Patamar DECOMP'
) as dag:

    # 1. Validar dados de entrada
    validar_dados = PythonOperator(
        task_id='validar_dados_entrada',
        python_callable=CargaPatamarDecompService.validar_dados,
        provide_context=True,
        doc_md='Valida os dados de entrada recebidos pelo webhook',
    )
    
    # 2. baixar arquivos
    download_arquivos = PythonOperator(
        task_id='download_arquivos',
        python_callable=CargaPatamarDecompService.download_arquivos,
        provide_context=True,
    )

    # 3. Extrair arquivos 
    extrair_arquivos = PythonOperator(
        task_id='extrair_arquivos',
        python_callable=CargaPatamarDecompService.extrair_arquivos,
        provide_context=True,
    )


    # 4. Processar arquivos
    processar_arquivos = PythonOperator(
        task_id='processar_arquivos',
        python_callable=CargaPatamarDecompService.processar_carga_patamar_decomp,
        provide_context=True
    )
    
    # 5. Enviar dados para API
    enviar_para_api = PythonOperator(
        task_id='enviar_dados_para_api',
        python_callable=CargaPatamarDecompService.enviar_dados_para_api,
        provide_context=True
    )

    # 6. Finalizar
    finalizar = DummyOperator(
        task_id='finalizar', 
        on_success_callback=WhatsappMessageSender.enviar_whatsapp_sucesso,
    )

    # Fluxo linear atualizado com nova task para envio de dados à API
    validar_dados >> download_arquivos >> extrair_arquivos >> processar_arquivos >> enviar_para_api >> finalizar
