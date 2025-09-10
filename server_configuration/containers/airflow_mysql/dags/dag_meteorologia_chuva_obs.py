import os
import boto3
from airflow import DAG
from datetime import datetime, timedelta, date
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.python_operator import PythonOperator
from airflow.exceptions import AirflowSkipException
from dotenv import load_dotenv
import requests
load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")), '.env'))

HOST_SERVIDOR = os.getenv("HOST_SERVIDOR")
URL_COGNITO = os.getenv("URL_COGNITO")
CONFIG_COGNITO = os.getenv("CONFIG_COGNITO")

WHATSAPP_API = os.getenv("WHATSAPP_API")
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

TIME_OUT = 60 * 60 * 30


def enviar_whatsapp_erro(context):
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = task_instance.task_id
    if task_id == "start_ec2":
        return
    msg = f"❌ Erro {dag_id}: *{task_id}*"
    fields = {
        "destinatario": "Airflow - Metereologia",
        "mensagem": msg
    }
    headers = {'accept': 'application/json', 'Authorization': f'Bearer {get_access_token()}'}
    response = requests.post(WHATSAPP_API, data=fields, headers=headers)
    print("Status Code:", response.status_code)


def enviar_whatsapp_sucesso(context):
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = task_instance.task_id
    if task_instance.state == 'skipped' or task_id == "start_ec2":
        return
    msg = f"✅ Sucesso {dag_id}: *{task_id}*"
    fields = {
        "destinatario": "Airflow - Metereologia",
        "mensagem": msg
    }
    headers = {'accept': 'application/json', 'Authorization': f'Bearer {get_access_token()}'}
    response = requests.post(WHATSAPP_API, data=fields, headers=headers)
    print("Status Code:", response.status_code)


def get_access_token() -> str:
    response = requests.post(
        URL_COGNITO,
        data=CONFIG_COGNITO,
        headers={'Content-Type': 'application/x-www-form-urlencoded'}
    )
    return response.json()['access_token']


def create_ec2_client(region):
    return boto3.client('ec2',
                        aws_access_key_id=AWS_ACCESS_KEY_ID,
                        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                        region_name=region)


def check_instance_state(instance_id, region):
    """Verifica o estado atual da instância na região especificada."""
    ec2 = create_ec2_client(region)
    try:
        response = ec2.describe_instances(InstanceIds=[instance_id])
        state = response['Reservations'][0]['Instances'][0]['State']['Name']
        return state
    except Exception as e:
        print(f"Erro ao verificar o estado da instância: {e}")
        return None


def start_instance(**kwargs):
    """Inicia uma instância na região especificada se ela estiver parada."""

    instance_id = 'i-0edbeb5435710d5f3'
    region = 'us-east-1'

    ec2 = create_ec2_client(region)
    state = check_instance_state(instance_id, region)

    if state is None:
        return
    if state == 'running':
        print("A instância já está rodando.")
        public_ip = ec2.describe_instances(InstanceIds=[instance_id])[
            'Reservations'][0]['Instances'][0].get('PublicIpAddress')
        print(public_ip)
        kwargs['ti'].xcom_push(key='public_ip', value=public_ip)

    elif state == 'stopped':
        print("Iniciando a instância...")
        ec2.start_instances(InstanceIds=[instance_id])
        print("Aguardando a instância ficar disponível...")
        waiter = ec2.get_waiter('instance_running')
        waiter.wait(InstanceIds=[instance_id])

        public_ip = ec2.describe_instances(InstanceIds=[instance_id])[
            'Reservations'][0]['Instances'][0].get('PublicIpAddress')
        print(public_ip)
        kwargs['ti'].xcom_push(key='public_ip', value=public_ip)

        print("Instância está rodando.")
    else:
        print(f"A instância está em um estado não manipulável: {state}")

    params = kwargs.get('dag_run').conf
    flag = params.get('flag')

    return flag


def stop_instance(instance_id, region):
    """Para uma instância na região especificada se ela estiver rodando."""
    raise AirflowSkipException("SKIP STOP INSTANCE")
    ec2 = create_ec2_client(region)
    state = check_instance_state(instance_id, region)
    if state is None:
        return
    if state == 'stopped':
        print("A instância já está parada.")
    elif state == 'running':
        print("Parando a instância...")
        ec2.stop_instances(InstanceIds=[instance_id])
        print("Aguardando a instância parar...")
        waiter = ec2.get_waiter('instance_stopped')
        waiter.wait(InstanceIds=[instance_id])
        print("Instância foi parada.")
    else:
        print(f"A instância está em um estado não manipulável: {state}")


with DAG(
    dag_id='OBS_CHUVA_DB_INMET',
    tags=["Verificador", "Chuva Observada", "Metereologia"],
    start_date=datetime(2024, 4, 28),
    schedule_interval="5 * * * * ",
    catchup=False,
) as dag:

    run = SSHOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='get_inmet_data',
        ssh_conn_id='ssh_master',
        command=" source /WX2TB/pythonVersions/myVenv38/bin/activate; cd /WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/verificadores/inmet; python get_dados_inmet.py",
        conn_timeout=None,
        cmd_timeout=None,
        get_pty=True,
    )

with DAG(
    dag_id='OBS_CHUVA_DB_CEMADEN',
    tags=["Verificador", "Chuva Observada", "Metereologia"],
    start_date=datetime(2024, 4, 28),
    schedule_interval="5 * * * * ",
    catchup=False,
) as dag:

    run = SSHOperator(
        do_xcom_push=False,
        task_id='get_cemaden_data',
        command="{{'/WX2TB/Documentos/fontes/tempo/novos_produtos/dados_cemaden/produtos.sh'}}",
        dag=dag,
        ssh_conn_id='ssh_master',
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        get_pty=True,
    )


with DAG(
    dag_id='OBS_CHUVA_DB_SIMEPAR',
    tags=["Verificador", "Chuva Observada", "Metereologia"],
    start_date=datetime(2024, 4, 28),
    schedule_interval="30 * * * * ",
    catchup=False,
) as dag:

    run = SSHOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='get_simepar_data',
        ssh_conn_id='ssh_master',
        command=" source /WX2TB/pythonVersions/myVenv38/bin/activate; cd /WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/verificadores/inmet; python get_dados_inmet.py simepar",
        conn_timeout=None,
        cmd_timeout=None,
        get_pty=True,
    )

# ==============================================MERGE-CPTEC-HOURLY=====================================================


def cmd_command(**kwargs):

    params = kwargs.get('dag_run').conf
    dt_ini = params.get('dt_ini')
    dt_fim = params.get('dt_fim')

    # na ons o arquivo que sai no dia é na verdade o do dia anterior
    if (not dt_ini) or (not dt_fim):
        dt_now = datetime.now()
        dt_ini = dt_now.strftime("%Y-%m-%d")
        dt_fim = dt_ini

    cmd = f'''/home/admin/enviMetereologia/bin/python /home/admin/joao/download_merge.py {dt_ini} {dt_fim}; /home/admin/enviMetereologia/bin/python /home/admin/joao/extrair_chuva_merge.py {dt_ini}'''
    print(f"Comando: {cmd}")
    kwargs['ti'].xcom_push(key='command', value=cmd)


