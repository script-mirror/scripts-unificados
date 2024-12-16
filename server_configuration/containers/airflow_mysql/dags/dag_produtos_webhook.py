
import datetime
import sys

from airflow.exceptions import AirflowException
from airflow.models.dag import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator


sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.apps.webhook.my_run import PRODUCT_MAPPING
from PMO.scripts_unificados.apps.webhook.libs import manipularArquivosShadow
from PMO.scripts_unificados.apps.prospec.libs.update_estudo import update_weol_estudo

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
    function_name = params.get('function_name')
    product_details = params.get('product_details')
    product_name = product_details.get('nome')

    try:
        # Obtendo a função do módulo importado baseada no nome
        func = getattr(manipularArquivosShadow, function_name, None)
        func(product_details)
    except Exception as e:
        raise AirflowException(f"[ERROR] Não foi possivel executar a funcao!: {str(e)}")
    
    return ['fim']
    
def deck_prev_eolica_semanal_patamares(**kwargs):
    params = kwargs.get('dag_run').conf
    manipularArquivosShadow.deck_prev_eolica_semanal_patamares(params)

def deck_prev_eolica_semanal_previsao_final(**kwargs):
    params = kwargs.get('dag_run').conf
    manipularArquivosShadow.deck_prev_eolica_semanal_previsao_final(params)
    
def deck_prev_eolica_semanal_update_estudos(**kwargs):
    params = kwargs.get('dag_run').conf
    # ids_to_modify = get_ids_estudos()

    ids_to_modify = [22152]
    data_produto = datetime.datetime.strptime(params.get('dataProduto'), "%d/%m/%Y")
    
    update_weol_estudo(ids_to_modify, data_produto.date)
    
with DAG(
    dag_id='WEEBHOOK',
    start_date=datetime.datetime(2024, 4, 28),
    catchup=False,
    schedule=None,
    tags=['webhook'],
    default_args={
        'retries': 1,
        'retry_delay': datetime.timedelta(minutes=5) 
    }
) as dag:

    # começo estrutura para rodar a sequencia das tarefas
    inicio = BranchPythonOperator(
        task_id='inicio',
        python_callable=convert_to_clean_name,
        provide_context=True,
    )

    fim = DummyOperator(
        task_id='fim',
        trigger_rule="none_failed_min_one_success",
    )

    for webhookProdut in PRODUCT_MAPPING.keys():

        produtoEscolhido = BranchPythonOperator(
            task_id=remover_acentos_e_caracteres_especiais(webhookProdut),
            trigger_rule="none_failed_min_one_success",
            python_callable = trigger_function,
            provide_context=True,
        )
        inicio >> produtoEscolhido
        produtoEscolhido >> fim

with DAG(
    dag_id='webhook_deck_prev_eolica_semanal_weol',
    start_date=datetime.datetime(2024, 4, 28),
    catchup=False,
    schedule=None,
    default_args={
        'retries': 1,
        'retry_delay': datetime.timedelta(minutes=5) 
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
        task_id='atualizar_estudos_prospec_bloco_pq',
        python_callable=deck_prev_eolica_semanal_update_estudos,
        provide_context=True
    )
    
    fim = DummyOperator(
        task_id='fim',
        trigger_rule="none_failed_min_one_success",
    )

    inicio >> patamares
    patamares >> previsao_final >> atualizar_estudos >> fim
    
    