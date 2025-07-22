import datetime
import sys
import os
from dotenv import load_dotenv
from airflow.models.dag import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)
from service_decks_newave import DecksNewaveService

utils_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'utils')
sys.path.insert(0, utils_path)
from utils.whatsapp_message import WhatsappMessageSender

load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")), '.env'))

with DAG(
    dag_id='DECKS_NEWAVE',
    start_date=datetime.datetime(2024, 4, 28),
    catchup=False,
    schedule=None,
    tags=['Deck Preliminar', 'Deck Definitivo', 'Patamares', 'NEWAVE', 'WEBHOOK'],
    default_args={
        'retries': 3,
        'retry_delay': datetime.timedelta(minutes=2),
        'on_failure_callback': WhatsappMessageSender.enviar_whatsapp_erro,
    },
    description='DAG simplificada para processamento dos Decks do Newave'
) as dag:

    # 1. Validar dados de entrada
    validar_dados_entrada = PythonOperator(
        task_id='validar_dados_entrada',
        python_callable=DecksNewaveService.validar_dados_entrada,
        provide_context=True,
        doc_md='Valida os dados de entrada recebidos pelo webhook',
    )
    
    # 2. Download dos arquivos que chegaram via webhook
    download_arquivos = PythonOperator(
        task_id='download_arquivos',
        python_callable=DecksNewaveService.download_arquivos,
        provide_context=True,
        doc_md='Faz o download dos arquivos recebidos via webhook',
    )

    # 3. Extrair os arquivos do ZIP
    extrair_arquivos = PythonOperator(
        task_id='extrair_arquivos',
        python_callable=DecksNewaveService.extrair_arquivos,
        provide_context=True,
        doc_md='Extrai os arquivos do ZIP recebido via webhook',
    )
    
    # ====== FLUXO 1: SISTEMA E CADIC (com tabela e WhatsApp) ======
    
    # 4. Processar o arquivo C_ADIC.DAT do produto com a lib Inewave
    processar_deck_nw_cadic = PythonOperator(
        task_id='processar_deck_nw_cadic',
        python_callable=DecksNewaveService.processar_deck_nw_cadic,
        provide_context=True,
        doc_md='Processa o arquivo C_ADIC.DAT do produto com a lib Inewave',
    )
    
    # 5. Processar o arquivo SISTEMA.DAT do produto com a lib Inewave
    processar_deck_nw_sist = PythonOperator(
        task_id='processar_deck_nw_sist',
        python_callable=DecksNewaveService.processar_deck_nw_sist,
        provide_context=True,
        doc_md='Processa o arquivo SISTEMA.DAT do produto com a lib Inewave',
    )
    
    # 6. Enviar os dados processados do SISTEMA e CADIC para a API Middle
    enviar_dados_sistema_cadic_para_api = PythonOperator(
        task_id='enviar_dados_sistema_cadic_para_api',
        python_callable=DecksNewaveService.enviar_dados_sistema_cadic_para_api,
        provide_context=True,
        doc_md='Envia os dados do SISTEMA e CADIC para a API Middle',
        trigger_rule=TriggerRule.ALL_SUCCESS,  # Só executa se ambos processamentos foram bem-sucedidos
    )
    
    # 7. Gerar tabela de diferença de cargas
    gerar_tabela_diferenca_cargas = PythonOperator(
        task_id='gerar_tabela_diferenca_cargas',
        python_callable=DecksNewaveService.gerar_tabela_diferenca_cargas,
        provide_context=True,
        doc_md='Gera tabela de diferença de cargas entre os decks processados',
    )
    
    # 8. Enviar tabela para whatsapp e e-mail
    enviar_tabela_whatsapp_email = PythonOperator(
        task_id='enviar_tabela_whatsapp_email',
        python_callable=DecksNewaveService.enviar_tabela_whatsapp_email,
        provide_context=True,
        doc_md='Envia a tabela de diferença de cargas via WhatsApp e e-mail',
    )
    
    # 9. Finalizar o fluxo Sistema/Cadic
    finalizar_sistema_cadic = DummyOperator(
        task_id='finalizar_sistema_cadic',
        doc_md='Finaliza o processamento do fluxo Sistema/Cadic',
    )
    
    # ====== FLUXO 2: PATAMARES (apenas envio para API) ======
    
    # 10. Processar o arquivo PATAMAR.DAT do produto com a lib Inewave
    processar_patamar_nw = PythonOperator(
        task_id='processar_patamar_nw',
        python_callable=DecksNewaveService.processar_patamar_nw,
        provide_context=True,
        doc_md='Processa o arquivo PATAMAR.DAT do produto com a lib Inewave',
    )
    
    # 11. Enviar os dados dos Patamares para a API Middle
    enviar_dados_patamares_para_api = PythonOperator(
        task_id='enviar_dados_patamares_para_api',
        python_callable=DecksNewaveService.enviar_dados_patamares_para_api,
        provide_context=True,
        doc_md='Envia os dados dos Patamares para a API Middle',
    )
    
    # 12. Finalizar o fluxo Patamares
    finalizar_patamares = DummyOperator(
        task_id='finalizar_patamares',
        doc_md='Finaliza o processamento do fluxo Patamares',
    )
    
    # ====== FINALIZAÇÃO GERAL ======
    
    # 13. Finalizar DAG (executa independentemente do sucesso/falha dos fluxos)
    finalizar = DummyOperator(
        task_id='finalizar',
        doc_md='Finaliza o processamento do Deck Preliminar Newave',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,  # Executa se pelo menos um fluxo teve sucesso
    )

    # ====== DEFINIÇÃO DAS DEPENDÊNCIAS ======
    
    # Fluxo comum inicial
    validar_dados_entrada >> download_arquivos >> extrair_arquivos
    
    # FLUXO 1: Sistema e Cadic (independente)
    extrair_arquivos >> [processar_deck_nw_cadic, processar_deck_nw_sist]
    [processar_deck_nw_cadic, processar_deck_nw_sist] >> enviar_dados_sistema_cadic_para_api
    enviar_dados_sistema_cadic_para_api >> gerar_tabela_diferenca_cargas >> enviar_tabela_whatsapp_email >> finalizar_sistema_cadic
    
    # FLUXO 2: Patamares (independente)
    extrair_arquivos >> processar_patamar_nw >> enviar_dados_patamares_para_api >> finalizar_patamares
    
    # Branch direto para geração de tabela (quando vem de outra origem)
    gerar_tabela_diferenca_cargas >> enviar_tabela_whatsapp_email >> finalizar_sistema_cadic
    
    # Finalização geral (aguarda ambos os fluxos terminarem, com sucesso ou falha)
    [finalizar_sistema_cadic, finalizar_patamares] >> finalizar