import os
import sys
import json
import datetime
from dotenv import load_dotenv
from airflow.models.dag import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
import requests
load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")),'.env'))
WHATSAPP_API = os.getenv("WHATSAPP_API")
URL_COGNITO = os.getenv("URL_COGNITO")
CONFIG_COGNITO = os.getenv("CONFIG_COGNITO")
sys.path.insert(1,"/WX2TB/Documentos/fontes")

from PMO.scripts_unificados.apps.prospec.libs import update_estudo

def get_access_token() -> str:
    response = requests.post(
        URL_COGNITO,
        data=CONFIG_COGNITO,
        headers={'Content-Type': 'application/x-www-form-urlencoded'}
    )
    return response.json()['access_token']
 
def enviar_whatsapp_erro(context):
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
    print("Status Code:", response.status_code)
    
def trigger_task_sequence(**kwargs):
    kwargs.get('dag_run').conf['ids_to_modify'] = kwargs.get('dag_run').conf.get('external_params').get('ids_to_modify',[])

    if not kwargs.get('dag_run').conf.get('ids_to_modify'):
        kwargs.get('dag_run').conf['ids_to_modify'] =  update_estudo.get_ids_to_modify()

    print(kwargs.get('dag_run').conf['ids_to_modify'])

    task_name = kwargs.get('dag_run').conf.get('external_params').get('task_to_execute')
    return [task_name]

def update_carga_pq_dadger_dc(data_produto, ids_to_modify):
    update_estudo.update_carga_pq_dadger_dc_estudo(
        datetime.datetime.strptime(data_produto[:10], "%d/%m/%Y").date(),
        ids_to_modify
    )

with DAG(
    dag_id='PROSPEC_UPDATER',
    tags=["PROSPEC"],
    start_date= datetime.datetime(2024, 4, 28),
    catchup=False,
    schedule=None,   
    render_template_as_native_obj=True,
    default_args={
        'owner': 'airflow',
        'on_failure_callback': enviar_whatsapp_erro,
    },
) as dag:
    
    inicio = BranchPythonOperator(
        task_id='inicio',
        python_callable=trigger_task_sequence,
        provide_context=True,
    )

    revisao_cvu = DummyOperator(
        task_id='revisao_cvu',
    )
    revisao_carga_dc = DummyOperator(
        task_id='revisao_carga_dc',
    )
    revisao_carga_nw = DummyOperator(
        task_id='revisao_carga_nw',
    )
    revisao_restricao = DummyOperator(
        task_id='revisao_restricao',
    )

    cvu_dadger_decomp = PythonOperator(
        task_id='cvu_dadger_decomp',
        python_callable=update_estudo.update_cvu_dadger_dc_estudo,
        provide_context=True,
        op_kwargs={
            "fontes_to_search": '{{ dag_run.conf.get("external_params").get("fontes_to_search")}}',
            "dt_atualizacao": '{{ dag_run.conf.get("external_params").get("dt_atualizacao")}}',
            "ids_to_modify":'{{ dag_run.conf.get("ids_to_modify") }}'
            },
    )

    carga_dadger_decomp = PythonOperator(
        task_id='carga_dadger_decomp',
        python_callable=update_estudo.update_carga_dadger_dc_estudo,
        provide_context=True,
        op_kwargs={
            "file_path": '{{ dag_run.conf.get("external_params").get("file_path")}}',
            "ids_to_modify":'{{ dag_run.conf.get("ids_to_modify") }}'
            },
        
    )
    carga_pq_dadger_dc = PythonOperator(
        task_id='carga_pq_dadger_dc',
        python_callable=update_carga_pq_dadger_dc,
        provide_context=True,
        op_kwargs={
            "data_produto": '{{ dag_run.conf.get("external_params").get("product_details").get("dataProduto")}}',
            "ids_to_modify":'{{ dag_run.conf.get("ids_to_modify") }}'
            },
    )
    


    cvu_clast_newave = PythonOperator(
        task_id='cvu_clast_newave',
        python_callable=update_estudo.update_cvu_clast_nw_estudo,
        provide_context=True,
        op_kwargs={
            "fontes_to_search": '{{ dag_run.conf.get("external_params").get("fontes_to_search")}}',
            "dt_atualizacao": '{{ dag_run.conf.get("external_params").get("dt_atualizacao")}}',
            "ids_to_modify":'{{ dag_run.conf.get("ids_to_modify") }}'
            },
        
    )

    carga_c_adic_newave = PythonOperator(
        task_id='carga_c_adic_newave',
        python_callable=update_estudo.update_carga_c_adic_nw_estudo,
        provide_context=True,
        op_kwargs={
            "file_path": '{{ dag_run.conf.get("external_params").get("file_path")}}',
            "ids_to_modify":'{{ dag_run.conf.get("ids_to_modify") }}'
            },

    )

    carga_sistema_newave = PythonOperator(
        task_id='carga_sistema_newave',
        python_callable=update_estudo.update_carga_sistema_nw_estudo,
        provide_context=True,
        op_kwargs={
            "file_path": '{{ dag_run.conf.get("external_params").get("file_path")}}',
            "ids_to_modify":'{{ dag_run.conf.get("ids_to_modify") }}'
            },
        
    )

    restricao_dadger_decomp = PythonOperator(
        task_id='restricao_dadger_decomp',
        python_callable=update_estudo.update_restricoes_dadger_dc_estudo,
        provide_context=True,
        op_kwargs={
            "file_path": '{{ dag_run.conf.get("external_params").get("file_path")}}',
            "ids_to_modify":'{{ dag_run.conf.get("ids_to_modify") }}'
            },
    )

    trigger_dag_prospec = TriggerDagRunOperator(
        task_id='trigger_dag_prospec_2.0',
        trigger_dag_id='2.0-PROSPEC_ATUALIZACAO',
        conf={'nome_estudo': "{{dag_run.conf.external_params.task_to_execute}}"},  
        wait_for_completion=False,  
        trigger_rule="none_failed_min_one_success",

    )


# Definindo dependências com base na decisão

inicio >> revisao_cvu >> cvu_dadger_decomp >> cvu_clast_newave >> trigger_dag_prospec
inicio >> revisao_cvu >> cvu_dadger_decomp >> trigger_dag_prospec
inicio >> revisao_carga_dc >> carga_dadger_decomp >> carga_pq_dadger_dc >> trigger_dag_prospec
inicio >> revisao_carga_nw >> carga_c_adic_newave >> carga_sistema_newave >> trigger_dag_prospec
inicio >> revisao_restricao >> restricao_dadger_decomp >> trigger_dag_prospec