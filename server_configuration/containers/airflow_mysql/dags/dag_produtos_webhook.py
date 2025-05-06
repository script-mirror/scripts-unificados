
import datetime
import sys
from dotenv import load_dotenv
from airflow.exceptions import AirflowException, AirflowFailException, AirflowSkipException
from airflow.models.dag import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import requests
import traceback
import os
load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")),'.env'))
WHATSAPP_API = os.getenv("WHATSAPP_API")
EVENTS_API = os.getenv("URL_EVENTS_API")
URL_COGNITO = os.getenv("URL_COGNITO")
CONFIG_COGNITO = os.getenv("CONFIG_COGNITO")

sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.apps.webhook.my_run import PRODUCT_MAPPING, PRODUCT_MAPPING2
from PMO.scripts_unificados.apps.webhook.libs import manipularArquivosShadow #comentario
from PMO.scripts_unificados.apps.prospec.libs.update_estudo import update_weol_dadger_dc_estudo, update_weol_sistema_nw_estudo

def remover_acentos_e_caracteres_especiais(texto):
    import re
    import unicodedata
    texto_norm = unicodedata.normalize('NFD', texto)
    texto_limpo = re.sub(r'[^\w\s]', '', texto_norm)
    texto_limpo = re.sub(r'\W+', '_', texto_limpo)
    
    return texto_limpo

def convert_to_clean_name(**kwargs):
    
    try:
        params = kwargs.get('dag_run').conf
        product_details = params.get('product_details')
        product_name = product_details.get('nome')

    except KeyError as e:
        raise AirflowException(f"KeyError: {e}. {product_name} não encontrado no dicionario.")

    resposta = remover_acentos_e_caracteres_especiais(product_name)
    return [resposta]


def trigger_function(**kwargs):

    params = kwargs.get('dag_run').conf
    product_details = params.get('product_details')
    product_name = product_details.get('nome')

    function_name = params.get('function_name')

    if not function_name or function_name == "WEBHOOK":
        function_name = PRODUCT_MAPPING.get(product_name)
        if function_name == None:
            raise AirflowFailException("Produto nao mapeado")
    try:

        func = getattr(manipularArquivosShadow, function_name, None)
        aditional_params = func(product_details)
        if aditional_params != None:
            kwargs.get('dag_run').conf.update(aditional_params)	
            return['trigger_external_dag']

    except AirflowSkipException:
        raise AirflowSkipException("Produto ja inserido")
    except Exception as e:
        error_message = f"Erro ao chamar a função {function_name}.\nDetalhes do erro:\n{str(e)}\n\n\n{traceback.format_exc()}"
        print(error_message)
        raise AirflowException(error_message)

    return ['fim']
    
def get_access_token() -> str:
    response = requests.post(
        URL_COGNITO,
        data=CONFIG_COGNITO,
        headers={'Content-Type': 'application/x-www-form-urlencoded'}
    )
    return response.json()['access_token']
 
def enviar_whatsapp_erro(context):
    # O context contém informações sobre a falha
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = task_instance.task_id
    
    msg = f"❌ Erro no Produto: *{context['params']['product_details']['nome']}*"
    fields = {
        "destinatario": "Airflow",
        "mensagem": msg
    }
    headers = {'accept': 'application/json', 'Authorization': f'Bearer {get_access_token()}'}
    response = requests.post(WHATSAPP_API, data=fields, headers=headers)
    print("Status Code:", response.status_code)
    
def enviar_whatsapp_erro_weol(context):
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = task_instance.task_id
    
    msg = f"❌ Erro em {dag_id} na task {task_id}"
    fields = {
        "destinatario": "Airflow",
        "mensagem": msg
    }
    headers = {'accept': 'application/json', 'Authorization': f'Bearer {get_access_token()}'}
    response = requests.post(WHATSAPP_API, data=fields, headers=headers)
    print("Status Code:", response.status_code)
 
def enviar_whatsapp_sucesso(context):
    task_instance = context['task_instance']
    if task_instance.state == 'skipped':
        return
    
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = task_instance.task_id
    
    msg = f"✅ Sucesso no Produto: *{context['params']['product_details']['nome']}*"
    fields = {
        "destinatario": "Airflow",
        "mensagem": msg
    }
    headers = {'accept': 'application/json', 'Authorization': f'Bearer {get_access_token()}'}
    response = requests.post(WHATSAPP_API, data=fields, headers=headers)
    enviar_evento_event_tracker(context)
    print("Status Code:", response.status_code)

def enviar_evento_event_tracker(context):
    headers = {'accept': 'application/json', 'Authorization': f'Bearer {get_access_token()}'}
    # O formato do evento deve ser semelhante a este:{
    #   "eventType": "string",
    #   "systemOrigin": "string",
    #   "payload": {},
    #   "value": 0,
    #   "userId": "string",
    #   "sessionId": "string",
    #   "correlationId": "string",
    #   "metadata": {}
    # }

    print("product_details", context['params'])
    fields = {
        "eventType": "product_processed",
        "systemOrigin": "airflow_"+context['params']['function_name'],
        "sessionId": context['params']['product_details']['nome'],
        # "value": 0
    }
    #debug events_api url
    print("EVENTS_API:", EVENTS_API)
    #debug fields
    print("fields:", fields)
    response = requests.post(EVENTS_API, data=fields, headers=headers)
    print("text", response.text)
    print("Status Code:", response.status_code)


    
with DAG(
    dag_id='WEBHOOK',
    start_date=datetime.datetime(2024, 4, 28),
    catchup=False,
    schedule=None,
    tags=['webhook'],
    default_args={
        'retries': 4,
        'retry_delay': datetime.timedelta(minutes=5),
        
    },
    render_template_as_native_obj=True

) as dag:

    # começo estrutura para rodar a sequencia das tarefas
    inicio = BranchPythonOperator(
        task_id='inicio',
        python_callable=convert_to_clean_name,
        provide_context=True,
    )

    trigger_updater_estudo = TriggerDagRunOperator(
        task_id="trigger_external_dag",
        trigger_dag_id='{{ dag_run.conf.get("trigger_dag_id")}}',
        conf={'external_params': "{{dag_run.conf}}"},  
        wait_for_completion=False,  
        trigger_rule="one_success",

    )

    fim = DummyOperator(
        task_id='fim',
        trigger_rule="dummy",
    )

    for webhookProdut in PRODUCT_MAPPING.keys():

        produtoEscolhido = BranchPythonOperator(
            task_id=remover_acentos_e_caracteres_especiais(webhookProdut),
            trigger_rule="none_failed_min_one_success",
            python_callable = trigger_function,
            provide_context=True,
            on_failure_callback=enviar_whatsapp_erro,
            on_success_callback=enviar_whatsapp_sucesso
        )

        inicio >> produtoEscolhido
        produtoEscolhido >> trigger_updater_estudo >> fim
        produtoEscolhido >> fim


#=================================================================================================

def deck_prev_eolica_semanal_patamares(**kwargs):
    params = kwargs.get('dag_run').conf
    manipularArquivosShadow.deck_prev_eolica_semanal_patamares(params)

def deck_prev_eolica_semanal_previsao_final(**kwargs):
    params = kwargs.get('dag_run').conf
    manipularArquivosShadow.deck_prev_eolica_semanal_previsao_final(params)
    
def deck_prev_eolica_semanal_update_estudos(**kwargs):
    params = kwargs.get('dag_run').conf
    data_produto = datetime.datetime.strptime(params.get('dataProduto'), "%d/%m/%Y")
    update_weol_dadger_dc_estudo(data_produto.date())
    update_weol_sistema_nw_estudo(data_produto.date())
    
def gerar_produto(**kwargs):
    params = kwargs.get('dag_run').conf
    manipularArquivosShadow.enviar_tabela_comparacao_weol_whatsapp_email(params)

with DAG(
    dag_id='webhook_deck_prev_eolica_semanal_weol',
    start_date=datetime.datetime(2024, 4, 28),
    catchup=False,
    schedule=None,
    default_args={
        'retries': 4,
        'retry_delay': datetime.timedelta(minutes=5),
        'on_failure_callback': enviar_whatsapp_erro_weol,
    },
    tags=['Webhook', 'Decks', 'Decomp']
    ) as dag:
    
    inicio = DummyOperator(
        task_id='inicio',
        trigger_rule="none_failed_min_one_success",
    )
    
    patamares = PythonOperator(
        task_id='inserir_patamares_decomp',
        trigger_rule="none_failed_min_one_success",
        python_callable=deck_prev_eolica_semanal_patamares,
        provide_context=True
    )
    
    previsao_final = PythonOperator(
        task_id='inserir_previsao_final_weol',
        python_callable=deck_prev_eolica_semanal_previsao_final,
        provide_context=True
    )
    atualizar_estudos = PythonOperator(
        task_id='atualizar_estudos_prospec_eolica',
        python_callable=deck_prev_eolica_semanal_update_estudos,
        provide_context=True
    )
    gerar_produtos = PythonOperator(
        task_id='enviar_tabela_comparacao_weol_whatsapp_email',
        python_callable=gerar_produto,
        provide_context=True
    )
    
    trigger_dag_prospec = TriggerDagRunOperator(
        task_id='trigger_dag_prospec_2.0',
        trigger_dag_id='2.0-PROSPEC_ATUALIZACAO',
        conf={'nome_estudo': "revisao_weol"},  
        wait_for_completion=False,  
        trigger_rule="none_failed_min_one_success",

    )

    inicio >> patamares
    patamares >> previsao_final >> gerar_produtos >> atualizar_estudos >> trigger_dag_prospec
    
    