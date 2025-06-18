import sys
import datetime
import requests as req
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator,PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from dotenv import load_dotenv
import os

load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")),'.env'))
URL_COGNITO = os.getenv("URL_COGNITO")
CONFIG_COGNITO = os.getenv("CONFIG_COGNITO")
PATH_PROJETO = os.getenv("PATH_PROJETO", "/WX2TB/Documentos/fontes/PMO")

sys.path.insert(0, f"{PATH_PROJETO}/raizen-power-trading-api-ccee")
from main import ApiCCEE
from src.constants import MAPPING

def get_access_token() -> str:
    response = req.post(
        URL_COGNITO,
        data=CONFIG_COGNITO,
        headers={'Content-Type': 'application/x-www-form-urlencoded'}
    )
    print(response.status_code, response.text)
    return response.json()['access_token']

def enviar_whatsapp_erro(context):
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = task_instance.task_id
    
    msg = f"❌ Erro na DAG: *{dag_id}*\nTask: *{task_id}*"
    fields = {
        "destinatario": "Airflow",
        "mensagem": msg
    }
    headers = {'accept': 'application/json', 'Authorization': f'Bearer {get_access_token()}'}
    response = req.post(WHATSAPP_API, data=fields, headers=headers)
    print("Enviando log para whatsapp:", response.status_code)
    
default_args = {
    'execution_timeout': datetime.timedelta(hours=8),
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=1),
    'on_failure_callback': enviar_whatsapp_erro,
}

def check_cvu_status_processamento(title:str, data_atualizacao:datetime.datetime):
    res = req.get("https://tradingenergiarz.com/api/v2/decks/check-cvu", params={
        'title': title,
        'data_atualizacao': data_atualizacao.strftime('%Y-%m-%dT%H:%M:%S'),
        },
        headers={'Authorization': f'Bearer {get_access_token()}'}
    )
    print(res.status_code, res.text)
    print(title)
    print(data_atualizacao.strftime('%Y-%m-%dT%H:%M:%S'))
    if res.status_code == 404:
        res = req.post("https://tradingenergiarz.com/api/v2/decks/check-cvu", json={
                'tipo_cvu': title,
                'data_atualizacao': data_atualizacao.strftime('%Y-%m-%dT%H:%M:%S'),
                'status': 'processando',
                },
                headers={'Authorization': f'Bearer {get_access_token()}'}     
            )
        print(res.status_code, res.text)
        return res.json()
    return res.json()

def mark_cvu_as_processed(ids_cvu:list):
    for id_check_cvu in ids_cvu:
        res = req.patch(f"https://tradingenergiarz.com/api/v2/decks/check-cvu/{id_check_cvu}/status", params={
            'status': 'processado',
            },
            headers={'Authorization': f'Bearer {get_access_token()}'}
        )
        print(res.status_code, res.text)
    return res.json()

def check_atualizacao(**kwargs):

    titles_to_search = kwargs.get('dag_run').conf.get('titles_to_search',[])
    ids_to_modify = kwargs.get('dag_run').conf.get('ids_to_modify',[])

    ultima_data_atualizacao = datetime.datetime.min
    ids_cvu_nao_processados = []
    if not titles_to_search:
        for title_name in MAPPING.keys():
            dt_atualizacao = ApiCCEE(title_name).get_date_atualizacao(title_name)
            check_cvu = check_cvu_status_processamento(
                title_name, 
                dt_atualizacao
            )
            if check_cvu.get('status') == 'processando':
                ids_cvu_nao_processados.append(check_cvu.get('id'))
                if dt_atualizacao:
                    titles_to_search.append(title_name)
                    if dt_atualizacao > ultima_data_atualizacao:
                        ultima_data_atualizacao = dt_atualizacao
        kwargs.get('dag_run').conf.update()
    
    if titles_to_search:
        fontes_to_search = [title.replace("custo_variavel_unitario_","CCEE_") for title in titles_to_search]
        
        kwargs.get('dag_run').conf.update(
            {   'titles_to_search':titles_to_search, 
                'fontes_to_search': fontes_to_search,
                "dt_atualizacao": ultima_data_atualizacao.strftime('%Y-%m-%d'),
                'task_to_execute': 'revisao_cvu',
                'ids_to_modify': ids_to_modify
            })
        kwargs['ti'].xcom_push(key='ids_cvu_nao_processados', value=ids_cvu_nao_processados)
        kwargs['ti'].xcom_push(key='titles_to_search', value=titles_to_search)

        return "create_ccee_instance"
    return "end_task"


def create_ccee_instance(**kwargs):

    titles_to_search = kwargs['ti'].xcom_pull(task_ids='check_atualizacao', key='titles_to_search')
    ccee_operator = ApiCCEE(titles_to_search)
    return ccee_operator

def export_data_to_db(**kwargs):
    ccee_operator = kwargs['ti'].xcom_pull(task_ids='create_ccee_instance')
    ids_cvu = kwargs['ti'].xcom_pull(task_ids='check_atualizacao', key='ids_cvu_nao_processados')
    ccee_operator.export_data()
    mark_cvu_as_processed(ids_cvu)


with DAG(
    'API_CCEE_CVU',
    default_args=default_args,
    description='Consulta a api da ccee',
    schedule_interval = '*/5 7-23 * * *', 
    tags=["PROSPEC","CCEE","CVU"],
    start_date=datetime.datetime(2024, 4, 28),
    catchup=False,
    render_template_as_native_obj=True, 

) as dag:

    check_atualizacao = BranchPythonOperator(
        task_id='check_atualizacao',
        python_callable=check_atualizacao,
        provide_context=True,
    )

    create_ccee_instance = PythonOperator(
        task_id='create_ccee_instance',
        python_callable=create_ccee_instance,
        provide_context=True,
    )

    export_data_to_db = PythonOperator(
        task_id='export_data_to_db',
        python_callable=export_data_to_db,
        provide_context=True,
    )

    trigger_updater_estudo = TriggerDagRunOperator(
        task_id="trigger_prospec_updater",
        trigger_dag_id='PROSPEC_UPDATER',
        conf={'external_params': "{{dag_run.conf}}"},  
        wait_for_completion=False,  
        trigger_rule="none_failed_min_one_success",
    )

    end_task = DummyOperator(
        task_id='end_task',
    )


    check_atualizacao >> create_ccee_instance >> export_data_to_db >> trigger_updater_estudo >> end_task
    check_atualizacao >> end_task