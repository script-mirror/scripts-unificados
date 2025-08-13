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

from tasks import DecksNewaveService

utils_path = os.path.join(os.path.dirname(os.path.dirname(current_dir)), 'utils')
sys.path.insert(0, utils_path)
from whatsapp_message import WhatsappMessageSender

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
    description='DAG para processamento dos Decks do NEWAVE (Preliminar e Definitivo)'
) as dag:

    should_execute_normal_flow = BranchPythonOperator(
        task_id='should_execute_normal_flow',
        python_callable=DecksNewaveService.should_execute_normal_flow,
        provide_context=True,
    )

    validar_dados_entrada = PythonOperator(
        task_id='validar_dados_entrada',
        python_callable=DecksNewaveService.validar_dados_entrada,
        provide_context=True,
    )

    download_arquivos = PythonOperator(
        task_id='download_arquivos',
        python_callable=DecksNewaveService.download_arquivos,
        provide_context=True,
    )

    extrair_arquivos = PythonOperator(
        task_id='extrair_arquivos',
        python_callable=DecksNewaveService.extrair_arquivos,
        provide_context=True,
    )

    processar_deck_nw_cadic = PythonOperator(
        task_id='processar_deck_nw_cadic',
        python_callable=DecksNewaveService.processar_deck_nw_cadic,
        provide_context=True,
    )

    processar_deck_nw_sist = PythonOperator(
        task_id='processar_deck_nw_sist',
        python_callable=DecksNewaveService.processar_deck_nw_sist,
        provide_context=True,
    )

    should_execute_weol_update = BranchPythonOperator(
        task_id='should_execute_weol_update',
        python_callable=DecksNewaveService.should_execute_weol_update_branch,
        provide_context=True,
    )

    atualizar_sist_com_weol = PythonOperator(
        task_id='atualizar_sist_com_weol',
        python_callable=DecksNewaveService.atualizar_sist_com_weol,
        provide_context=True,
    )

    enviar_dados_sistema_cadic_para_api = PythonOperator(
        task_id='enviar_dados_sistema_cadic_para_api',
        python_callable=DecksNewaveService.enviar_dados_sistema_cadic_para_api,
        provide_context=True,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    processar_patamar_nw = PythonOperator(
        task_id='processar_patamar_nw',
        python_callable=DecksNewaveService.processar_patamar_nw,
        provide_context=True,
    )

    enviar_dados_patamares_para_api = PythonOperator(
        task_id='enviar_dados_patamares_para_api',
        python_callable=DecksNewaveService.enviar_dados_patamares_para_api,
        provide_context=True,
    )

    gerar_tabela_diferenca_cargas_normal = PythonOperator(
        task_id='gerar_tabela_diferenca_cargas_normal',
        python_callable=DecksNewaveService.gerar_tabela_diferenca_cargas,
        provide_context=True,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    gerar_tabela_diferenca_cargas_externo = PythonOperator(
        task_id='gerar_tabela_diferenca_cargas_externo',
        python_callable=DecksNewaveService.gerar_tabela_diferenca_cargas,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    enviar_tabela_whatsapp_email_normal = PythonOperator(
        task_id='enviar_tabela_whatsapp_email_normal',
        python_callable=DecksNewaveService.enviar_tabela_whatsapp_email,
        provide_context=True,
    )

    enviar_tabela_whatsapp_email_externo = PythonOperator(
        task_id='enviar_tabela_whatsapp_email_externo',
        python_callable=DecksNewaveService.enviar_tabela_whatsapp_email,
        provide_context=True,
    )
    
    finalizar_sistema_cadic = DummyOperator(
        task_id='finalizar_sistema_cadic',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    finalizar_patamares = DummyOperator(task_id='finalizar_patamares')

    finalizar_fluxo_externo = DummyOperator(
        task_id='finalizar_fluxo_externo',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    finalizar = DummyOperator(
        task_id='finalizar',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # FN (Deck Newave Preliminar e Definitivo)
    should_execute_normal_flow >> validar_dados_entrada
    
    # FE (Cargas mensais por Patamar)
    should_execute_normal_flow >> gerar_tabela_diferenca_cargas_externo

    # FN (Processando SISTEMA.DAT e C_ADIC.DAT)
    validar_dados_entrada >> download_arquivos >> extrair_arquivos
    extrair_arquivos >> [processar_deck_nw_cadic, processar_deck_nw_sist]
    processar_deck_nw_sist >> should_execute_weol_update
    should_execute_weol_update >> atualizar_sist_com_weol
    [atualizar_sist_com_weol, should_execute_weol_update, processar_deck_nw_cadic] >> enviar_dados_sistema_cadic_para_api
    enviar_dados_sistema_cadic_para_api >> gerar_tabela_diferenca_cargas_normal
    gerar_tabela_diferenca_cargas_normal >> enviar_tabela_whatsapp_email_normal >> finalizar_sistema_cadic
    # FN (Processando PATAMAR.DAT)
    extrair_arquivos >> processar_patamar_nw >> enviar_dados_patamares_para_api >> finalizar_patamares

    # FE
    gerar_tabela_diferenca_cargas_externo >> enviar_tabela_whatsapp_email_externo >> finalizar_fluxo_externo

    # Finalização dos Fluxos
    [finalizar_sistema_cadic, finalizar_patamares, finalizar_fluxo_externo] >> finalizar
