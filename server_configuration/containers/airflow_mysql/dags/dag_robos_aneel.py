import boto3
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python_operator import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.operators.dummy_operator import DummyOperator

import datetime

def create_ec2_client(region):
    return boto3.client('ec2',
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
    instance_id = 'i-09ac17395e40969fa'
    region = 'sa-east-1'

    ec2 = create_ec2_client(region)
    state = check_instance_state(instance_id, region)

    if state is None:
        return
    if state == 'running':
        print("A instância já está rodando.")
        public_ip = ec2.describe_instances(InstanceIds=[instance_id])['Reservations'][0]['Instances'][0].get('PublicIpAddress')
        print(public_ip)
        kwargs['ti'].xcom_push(key='public_ip', value=public_ip)
        
    elif state == 'stopped':
        print("Iniciando a instância...")
        ec2.start_instances(InstanceIds=[instance_id])
        print("Aguardando a instância ficar disponível...")
        waiter = ec2.get_waiter('instance_running')
        waiter.wait(InstanceIds=[instance_id])

        public_ip = ec2.describe_instances(InstanceIds=[instance_id])['Reservations'][0]['Instances'][0].get('PublicIpAddress')
        print(public_ip)
        kwargs['ti'].xcom_push(key='public_ip', value=public_ip)
        
        print("Instância está rodando.")
    else:
        print(f"A instância está em um estado não manipulável: {state}")

def stop_instance(instance_id, region):
    """Para uma instância na região especificada se ela estiver rodando."""
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
    'SSH_ANEEL',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    # schedule_interval=timedelta(days=1),
    schedule='00 10,12,15,18 * * *',
    catchup=False,
) as dag:

    # começo estrutura para rodar a sequencia das tarefas
    inicio = DummyOperator(
        task_id='inicio',
        trigger_rule="none_failed_min_one_success",
        
    )


    start_ec2_task = PythonOperator(
        task_id='start_ec2',
        python_callable=start_instance,
        # provide_context=True,
    )

    # Task to run a command on the remote server
    run_pautas_aneel = SSHOperator(
        task_id='run_pautas_aneel',
        remote_host="{{ ti.xcom_pull(task_ids='start_ec2', key='public_ip') }}",
        ssh_conn_id='ssh_sp',
        command="{{'vncserver -kill :04; vncserver :04 -localhost=0;cd /home/ec2-user/scripts_unificados/apps/verificador_processos;./verifica_pautas.sh'}}",
        conn_timeout = None,
        cmd_timeout = None,
        get_pty=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    run_processos_aneel = SSHOperator(
        task_id='run_processos_aneel',
        remote_host="{{ ti.xcom_pull(task_ids='start_ec2', key='public_ip') }}",
        ssh_conn_id='ssh_sp',
        command="{{'vncserver -kill :04; vncserver :04 -localhost=0;cd /home/ec2-user/scripts_unificados/apps/verificador_processos;./verifica_processos.sh'}}",
        conn_timeout = None,
        cmd_timeout = None,
        get_pty=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    run_documentos_aneel = SSHOperator(
        task_id='run_documentos_aneel',
        remote_host="{{ ti.xcom_pull(task_ids='start_ec2', key='public_ip') }}",
        ssh_conn_id='ssh_sp',
        command="{{'cd /home/ec2-user/scripts_unificados/apps/verificador_processos;./verifica_documentos.sh'}}",
        conn_timeout = None,
        cmd_timeout = None,
        get_pty=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    stop_ec2_task = PythonOperator(
        task_id='stop_ec2',
        python_callable=stop_instance,
        op_kwargs={'instance_id': 'i-09ac17395e40969fa', 'region': 'sa-east-1'},
        trigger_rule=TriggerRule.ALL_DONE,
    )

    fim = DummyOperator(
        task_id='fim',
        trigger_rule="none_failed_min_one_success",
    )
    inicio >> start_ec2_task >> run_documentos_aneel >> run_processos_aneel >> run_pautas_aneel >> stop_ec2_task >> fim