"""
DAG para processamento de Previsões de Carga Mensal por Patamar NEWAVE.
"""

import datetime
import sys
import os
from dotenv import load_dotenv
from airflow.models.dag import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


# Configuração de paths
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

# Importar tasks específicas
from tasks import PrevisoesCargaMensalNewaveService

# Importar utilitários
utils_path = os.path.join(os.path.dirname(os.path.dirname(current_dir)), 'utils')
sys.path.insert(0, utils_path)
from whatsapp_message import WhatsappMessageSender

# Carregar variáveis de ambiente
load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")), '.env'))

with DAG(
    dag_id='PREVISOES_CARGA_MENSAL_PATAMAR_NEWAVE',
    start_date=datetime.datetime(2024, 4, 28),
    catchup=False,
    schedule=None,
    tags=["CARGA", 'Patamares', 'NEWAVE', 'WEBHOOK'],
    default_args={
        'retries': 3,
        'retry_delay': datetime.timedelta(minutes=2),
        'on_failure_callback': WhatsappMessageSender.enviar_whatsapp_erro,
    },
    description='DAG para processamento de Previsões de Carga Mensal por Patamar NEWAVE'
) as dag:

    # 1. Validar dados de entrada
    validar_dados_entrada = PythonOperator(
        task_id='validar_dados_entrada',
        python_callable=PrevisoesCargaMensalNewaveService.validar_dados_entrada,
        provide_context=True,
        doc_md='Valida os dados de entrada recebidos pelo webhook',
    )
    
    # 2. Download dos arquivos que chegaram via webhook
    download_arquivos = PythonOperator(
        task_id='download_arquivos',
        python_callable=PrevisoesCargaMensalNewaveService.download_arquivos,
        provide_context=True,
        doc_md='Faz o download dos arquivos recebidos via webhook',
    )

    # 3. Extrair os arquivos do ZIP
    extrair_arquivos = PythonOperator(
        task_id='extrair_arquivos',
        python_callable=PrevisoesCargaMensalNewaveService.extrair_arquivos,
        provide_context=True,
        doc_md='Extrai os arquivos do ZIP recebido via webhook',
    )
    
    # 4. Processar arquivo de carga mensal
    processar_carga_mensal = PythonOperator(
        task_id='processar_carga_mensal',
        python_callable=PrevisoesCargaMensalNewaveService.processar_carga_mensal,
        provide_context=True,
        doc_md='Processa o arquivo C_ADIC.DAT para extrair dados de carga mensal',
    )
    
    # 5. Atualizar SISTEMA e C_ADIC com as cargas processadas
    atualizar_sist_cadic_com_cargas = PythonOperator(
        task_id='atualizar_sist_cadic_com_cargas',
        python_callable=PrevisoesCargaMensalNewaveService.atualizar_sist_cadic_com_cargas,
        provide_context=True,
        doc_md='Atualiza os arquivos SISTEMA.DAT e C_ADIC.DAT com as cargas processadas',
    )
    
    # 5. Triggar task de DAG externa (DECKS_NEWAVE.gerar_tabela_diferenca_cargas)
    gerar_tabela_diferenca_cargas = TriggerDagRunOperator(
        task_id='gerar_tabela_diferenca_cargas',
        trigger_dag_id='DECKS_NEWAVE',
        trigger_run_id=f"triggered_by_previsoes_carga_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}",
        conf="{{ dag_run.conf | tojson }}",
        wait_for_completion=True,  # Aguarda conclusão da DAG triggada
        poke_interval=30,  # Intervalo de verificação em segundos
        allowed_states=['success'],  # Estados considerados como sucesso
        failed_states=['failed'],  # Estados considerados como falha
        doc_md='Trigga a task gerar_tabela_diferenca_cargas da DAG DECKS_NEWAVE',
    )
    
    # 6. Finalizar processamento
    finalizar = DummyOperator(
        task_id='finalizar',
        doc_md='Finaliza o processamento de Previsões de Carga Mensal',
    )

    # ====== DEFINIÇÃO DAS DEPENDÊNCIAS ======
    
    validar_dados_entrada >> download_arquivos >> extrair_arquivos >> processar_carga_mensal >> atualizar_sist_cadic_com_cargas >> gerar_tabela_diferenca_cargas >> finalizar