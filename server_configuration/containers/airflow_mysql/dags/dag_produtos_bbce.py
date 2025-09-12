import sys
import datetime

from airflow.models.dag import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator


sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.apps.dbUpdater.libs import bbce
from PMO.scripts_unificados.apps.gerarProdutos import gerarProdutos2


def importar_operacoes():

    try:
        data = datetime.datetime.now()
        return bbce.importar_operacoes_bbce(data)
    except Exception as e:
        raise AirflowException(f"[ERROR] Não foi possivel executar a funcao!: {str(e)}")
    

def gerar_produto():
    
    try:
        GERAR_PRODUTO = gerarProdutos2.GeradorProdutos()
        GERAR_PRODUTO.enviar({
            "produto":"RELATORIO_BBCE",
        })
        return ['fim']
    except Exception as e:
        raise AirflowException(f"[ERROR] Não foi possivel executar a funcao!: {str(e)}")
    
    
with DAG(
    dag_id='gerar_produto_bbce',
    start_date=datetime.datetime(2024, 4, 28),
    catchup=False,
    schedule="15 9,11,13,16,18 * * 1-5",
    default_args={'timezone': 'America/Sao_Paulo'}
) as dag:


    inicio = DummyOperator(
        task_id='inicio',
        trigger_rule="none_failed_min_one_success",
    )

    gerarProduto = PythonOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='gerarProduto',
        python_callable=gerar_produto,
        
    )
    fim = DummyOperator(
        task_id='fim',
        trigger_rule="none_failed_min_one_success",
    )

    inicio >> gerarProduto >> fim

with DAG(
    dag_id='atualizar_bbce',
    start_date=datetime.datetime(2024, 7, 23),
    catchup=False,
    schedule="*/5 9-18 * * 1-5",
    default_args={'timezone': 'America/Sao_Paulo'} 
) as dag:
    inicio = DummyOperator(
        task_id='inicio',
        trigger_rule="none_failed_min_one_success",
    )
    dbUpdater = PythonOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='dbUpdater',
        python_callable=importar_operacoes,
        
    )
    fim = DummyOperator(
        task_id='fim',
        trigger_rule="none_failed_min_one_success",
    )
    inicio >> dbUpdater >> fim