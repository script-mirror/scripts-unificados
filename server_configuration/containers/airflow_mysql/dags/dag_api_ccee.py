import os
import datetime
import requests as req
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from middle.utils import get_auth_header
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")), '.env'))

WHATSAPP_API = os.getenv("WHATSAPP_API")


def enviar_whatsapp_erro(context):
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = task_instance.task_id

    msg = f"❌ Erro na DAG: *{dag_id}*\nTask: *{task_id}*"
    fields = {
        "destinatario": "Airflow",
        "mensagem": msg
    }
    headers = get_auth_header()
    response = req.post(WHATSAPP_API, data=fields, headers=headers)
    print("Enviando log para whatsapp:", response.status_code)


def get_cvu_mapping():
    res = req.get('https://tradingenergiarz.com/'
                  'estudos-middle/api/ccee/cvu-mapping',
                  headers=get_auth_header())
    if res.status_code == 200:
        return res.json()
    else:
        raise Exception("Erro ao obter mapeamento CVU:"
                        f" {res.status_code} - {res.text}")


def get_date_atualizacao(tipo_cvu: str):
    res = req.get('https://tradingenergiarz.com/'
                  f'estudos-middle/api/ccee/{tipo_cvu}/ultima-atualizacao',
                  headers=get_auth_header())
    if res.status_code == 200:
        return res.json()
    else:
        raise Exception("Erro ao ultima atualizacao cvu:"
                        f" {res.status_code} - {res.text}")


def get_cvu_data(tipo_cvu: str):
    res = req.get('https://tradingenergiarz.com/'
                  f'estudos-middle/api/ccee/{tipo_cvu}',
                  headers=get_auth_header())
    if res.status_code == 200:
        return res.json()
    else:
        raise Exception("Erro ao obter dados CVU:"
                        f" {res.status_code} - {res.text}")


def post_cvu(body: dict, tipo_cvu: str):
    url = 'https://tradingenergiarz.com/api/v2/decks/cvu'
    if tipo_cvu == 'merchant':
        url = url + '/merchant'
    res = req.post(url,
                   json=body,
                   headers=get_auth_header())
    if res.status_code == 201:
        return res.json()
    else:
        raise Exception("Erro ao postar dados CVU:"
                        f" {res.status_code} - {res.text}")

def check_cvu_status_processamento(tipo_cvu: str, data_atualizacao: str):
    res = req.get("https://tradingenergiarz.com/api/v2/decks/check-cvu", params={
        'tipo_cvu': tipo_cvu,
        'data_atualizacao': data_atualizacao,
        },
        headers=get_auth_header()
    )
    print(res.status_code, res.text)
    print(data_atualizacao)
    if res.status_code == 404:
        res = req.post("https://tradingenergiarz.com/api/v2/decks/check-cvu", json={
                'tipo_cvu': tipo_cvu,
                'data_atualizacao': data_atualizacao,
                'status': 'processando',
                },
                headers=get_auth_header()     
            )
        print(res.status_code, res.text)
        return res.json()
    return res.json()


def mark_cvu_as_processed(ids_cvu:list):
    for id_check_cvu in ids_cvu:
        res = req.patch(f"https://tradingenergiarz.com/api/v2/decks/check-cvu/{id_check_cvu}/status", params={
            'status': 'processado',
            },
            headers=get_auth_header()
        )
        print(res.status_code, res.text)
    return res.json()

def check_atualizacao(**kwargs):

    cvus_to_search = kwargs.get('dag_run').conf.get('cvus_to_search',[])
    ids_to_modify = kwargs.get('dag_run').conf.get('ids_to_modify',[])

    ultima_data_atualizacao = datetime.datetime.min
    ids_cvu_nao_processados = []
    tipos_cvu = get_cvu_mapping()['tipos_cvu']
    if not cvus_to_search:
        for tipo_cvu in tipos_cvu:
            dt_atualizacao = get_date_atualizacao(tipo_cvu)['data_atualizacao']
            check_cvu = check_cvu_status_processamento(
                tipo_cvu,
                dt_atualizacao
            )
            if check_cvu.get('status') == 'processando':
                ids_cvu_nao_processados.append(check_cvu.get('id'))
                if dt_atualizacao:
                    cvus_to_search.append(tipo_cvu)
                    if datetime.datetime.strptime(
                        dt_atualizacao, '%Y-%m-%dT%H:%M:%S'
                    ) > ultima_data_atualizacao:
                        ultima_data_atualizacao = datetime.datetime.strptime(
                            dt_atualizacao, '%Y-%m-%dT%H:%M:%S'
                        )
        kwargs.get('dag_run').conf.update()
    
    if cvus_to_search:
        fontes_to_search = [f"CCEE_{title}" for title in cvus_to_search]
        
        kwargs.get('dag_run').conf.update(
            {   'cvus_to_search':cvus_to_search, 
                'fontes_to_search': fontes_to_search,
                "dt_atualizacao": ultima_data_atualizacao.strftime('%Y-%m-%d'),
                'task_to_execute': 'revisao_cvu',
                'ids_to_modify': ids_to_modify
            })
        kwargs['ti'].xcom_push(key='ids_cvu_nao_processados', value=ids_cvu_nao_processados)
        kwargs['ti'].xcom_push(key='cvus_to_search', value=cvus_to_search)

        return "export_data_to_db"
    return "end_task"


def export_data_to_db(**kwargs):
    cvus_to_search = kwargs['ti'].xcom_pull(
        task_ids='check_atualizacao',
        key='cvus_to_search'
    )
    ids_cvu = kwargs['ti'].xcom_pull(
        task_ids='check_atualizacao',
        key='ids_cvu_nao_processados'
    )
    for tipo_cvu in cvus_to_search:
        post_cvu(get_cvu_data(tipo_cvu), tipo_cvu)
    mark_cvu_as_processed(ids_cvu)


with DAG(
    'API_CCEE_CVU',
    default_args={
        'execution_timeout': datetime.timedelta(hours=8),
        'retries': 2,
        'retry_delay': datetime.timedelta(minutes=1),
        'on_failure_callback': enviar_whatsapp_erro,
    },
    description='Consulta a api da ccee',
    schedule_interval='*/5 7-23 * * *',
    tags=["PROSPEC", "CCEE", "CVU"],
    start_date=datetime.datetime(2024, 4, 28),
    catchup=False,
    render_template_as_native_obj=True, 

) as dag:

    check_atualizacao = BranchPythonOperator(
        task_id='check_atualizacao',
        python_callable=check_atualizacao,
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

    check_atualizacao >> export_data_to_db >> trigger_updater_estudo >> end_task
    check_atualizacao >> end_task