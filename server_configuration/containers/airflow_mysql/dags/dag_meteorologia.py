import boto3
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.exceptions import AirflowSkipException

import datetime
import os
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")),'.env'))

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

TIME_OUT = 60*60*30

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

##################################################################################################

with DAG(
    'CPC',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    schedule=None,
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Metereologia']
) as dag:
    
    
    start_ec2_task = PythonOperator(
        task_id='start_ec2',
        python_callable=start_instance,
        # provide_context=True,
    )
    
     # Task to run a command on the remote server
    run_cpc = SSHOperator(
        task_id='run_cpc',
        remote_host="{{ ti.xcom_pull(task_ids='start_ec2', key='public_ip') }}",
        ssh_conn_id='ssh_ecmwf',
        command="{{ '/home/admin/rotinas/cpc_to_dat.sh' }}",
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
        trigger_rule=TriggerRule.ALL_DONE,

    )

    run_shell_script = SSHOperator(
        task_id='sobe_banco_cpc',
        command="{{'/home/admin/jose/cpc/produtos_banco.sh'}}",
        dag=dag,
        ssh_conn_id='ssh_master',
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,

        
    )

    stop_ec2_task = PythonOperator(
        task_id='stop_ec2',
        python_callable=stop_instance,
        op_kwargs={'instance_id': 'i-0edbeb5435710d5f3', 'region': 'us-east-1'},
        trigger_rule=TriggerRule.ALL_DONE,
    )

    fim = DummyOperator(
        task_id='fim',
        trigger_rule="none_failed_min_one_success",
    )
    
    start_ec2_task >> run_cpc >> run_shell_script >> stop_ec2_task >> fim

##################################################################################################

with DAG(
    'EC-FORECAST',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    schedule=None,
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Metereologia']
) as dag:
        
    start_ec2_task = BranchPythonOperator(
        task_id='start_ec2',
        python_callable=start_instance,
        # provide_context=True,
    )
    
     # Task to run a command on the remote server
    run_forecast = SSHOperator(
        task_id='run_forecast',
        remote_host="{{ ti.xcom_pull(task_ids='start_ec2', key='public_ip') }}",
        ssh_conn_id='ssh_ecmwf',
        command="{{ '/home/admin/rotinas/forecast_to_dat.sh' }}",
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
        trigger_rule=TriggerRule.ALL_DONE,

        
    )

     # Task to run a command on the remote server
    run_hindcast = SSHOperator(
        task_id='run_hindcast',
        remote_host="{{ ti.xcom_pull(task_ids='start_ec2', key='public_ip') }}",
        ssh_conn_id='ssh_ecmwf',
        command="{{ '/home/admin/rotinas/hindcast_to_dat.sh' }}",
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
        trigger_rule=TriggerRule.ALL_DONE,

        
    )

    stop_ec2_task = PythonOperator(
        task_id='stop_ec2',
        python_callable=stop_instance,
        op_kwargs={'instance_id': 'i-0edbeb5435710d5f3', 'region': 'us-east-1'},
        trigger_rule=TriggerRule.ALL_DONE,
    )

    fim = DummyOperator(
        task_id='fim',
        trigger_rule="none_failed_min_one_success",
    )
    start_ec2_task >> [run_forecast,run_hindcast]  >> stop_ec2_task >> fim

with DAG(
    'PREV_CHUVA_DB_ECMWF-ENS',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    schedule='0 4,17 * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Metereologia']
) as dag:
        
    start_ec2_task = PythonOperator(
        task_id='start_ec2',
        python_callable=start_instance,
        # provide_context=True,
    )
    
     # Task to run a command on the remote server
    
    run_forecast = SSHOperator(
        task_id='vl_chuva_to_db',
        remote_host="{{ ti.xcom_pull(task_ids='start_ec2', key='public_ip') }}",
        ssh_conn_id='ssh_ecmwf',
        command="{{ '/home/admin/enviMetereologia/bin/python /home/admin/rotinas/gera_forecast_to_db.py ecmwf-ens-membros' }}",
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
        trigger_rule=TriggerRule.ALL_DONE,

        
    )

    stop_ec2_task = PythonOperator(
        task_id='stop_ec2',
        python_callable=stop_instance,
        op_kwargs={'instance_id': 'i-0edbeb5435710d5f3', 'region': 'us-east-1'},
        trigger_rule=TriggerRule.ALL_DONE,
    )

    fim = DummyOperator(
        task_id='fim',
        trigger_rule="none_failed_min_one_success",
    )
    
    start_ec2_task >> [run_forecast]  >> stop_ec2_task >> fim

##################################################################################################

with DAG(
    'PREV_CHUVA_DB_ECMWF',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    schedule='0 4,16 * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Metereologia']
) as dag:
        
    start_ec2_task = PythonOperator(
        task_id='start_ec2',
        python_callable=start_instance,
        # provide_context=True,
    )
    
     # Task to run a command on the remote server
    
    run_forecast = SSHOperator(
        task_id='vl_chuva_to_db',
        remote_host="{{ ti.xcom_pull(task_ids='start_ec2', key='public_ip') }}",
        ssh_conn_id='ssh_ecmwf',
        command="{{ '/home/admin/enviMetereologia/bin/python /home/admin/rotinas/gera_forecast_to_db.py ecmwf' }}",
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
        trigger_rule=TriggerRule.ALL_DONE,

        
    )

    stop_ec2_task = PythonOperator(
        task_id='stop_ec2',
        python_callable=stop_instance,
        op_kwargs={'instance_id': 'i-0edbeb5435710d5f3', 'region': 'us-east-1'},
        trigger_rule=TriggerRule.ALL_DONE,
    )

    fim = DummyOperator(
        task_id='fim',
        trigger_rule="none_failed_min_one_success",
    )
    
    start_ec2_task >> [run_forecast]  >> stop_ec2_task >> fim    

##################################################################################################

with DAG(
    'PREV_CHUVA_DB_GEFS-EST',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    schedule='10 0 * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Metereologia']
) as dag:
        
    start_ec2_task = PythonOperator(
        task_id='start_ec2',
        python_callable=start_instance,
        # provide_context=True,
    )
    
     # Task to run a command on the remote server
    
    run_forecast = SSHOperator(
        task_id='vl_chuva_to_db',
        remote_host="{{ ti.xcom_pull(task_ids='start_ec2', key='public_ip') }}",
        ssh_conn_id='ssh_ecmwf',
        command="{{ '/home/admin/enviMetereologia/bin/python /home/admin/rotinas/gera_forecast_to_db.py gefs-membros-estendido' }}",
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
        trigger_rule=TriggerRule.ALL_DONE,

        
    )

    stop_ec2_task = PythonOperator(
        task_id='stop_ec2',
        python_callable=stop_instance,
        op_kwargs={'instance_id': 'i-0edbeb5435710d5f3', 'region': 'us-east-1'},
        trigger_rule=TriggerRule.ALL_DONE,
    )

    fim = DummyOperator(
        task_id='fim',
        trigger_rule="none_failed_min_one_success",
    )
    
    start_ec2_task >> [run_forecast]  >> stop_ec2_task >> fim

##################################################################################################

with DAG(
    'PREV_CHUVA_DB_GEFS',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    schedule='0 2,7,14,19 * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Metereologia']
) as dag:
        
    start_ec2_task = PythonOperator(
        task_id='start_ec2',
        python_callable=start_instance,
        # provide_context=True,
    )
    
     # Task to run a command on the remote server
    
    run_forecast = SSHOperator(
        task_id='vl_chuva_to_db',
        remote_host="{{ ti.xcom_pull(task_ids='start_ec2', key='public_ip') }}",
        ssh_conn_id='ssh_ecmwf',
        command="{{ '/home/admin/enviMetereologia/bin/python /home/admin/rotinas/gera_forecast_to_db.py gefs-membros' }}",
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
        trigger_rule=TriggerRule.ALL_DONE,

        
    )

    stop_ec2_task = PythonOperator(
        task_id='stop_ec2',
        python_callable=stop_instance,
        op_kwargs={'instance_id': 'i-0edbeb5435710d5f3', 'region': 'us-east-1'},
        trigger_rule=TriggerRule.ALL_DONE,
    )

    fim = DummyOperator(
        task_id='fim',
        trigger_rule="none_failed_min_one_success",
    )
    
    start_ec2_task >> [run_forecast]  >> stop_ec2_task >> fim

##################################################################################################

with DAG(
    'PREV_CHUVA_DB_GFS',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    schedule='0 2,7,14,19 * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Metereologia']
) as dag:
        
    start_ec2_task = PythonOperator(
        task_id='start_ec2',
        python_callable=start_instance,
        # provide_context=True,
    )
    
     # Task to run a command on the remote server
    
    run_forecast = SSHOperator(
        task_id='vl_chuva_to_db',
        remote_host="{{ ti.xcom_pull(task_ids='start_ec2', key='public_ip') }}",
        ssh_conn_id='ssh_ecmwf',
        command="{{ '/home/admin/enviMetereologia/bin/python /home/admin/rotinas/gera_forecast_to_db.py gfs' }}",
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
        trigger_rule=TriggerRule.ALL_DONE,

        
    )

    stop_ec2_task = PythonOperator(
        task_id='stop_ec2',
        python_callable=stop_instance,
        op_kwargs={'instance_id': 'i-0edbeb5435710d5f3', 'region': 'us-east-1'},
        trigger_rule=TriggerRule.ALL_DONE,
    )

    fim = DummyOperator(
        task_id='fim',
        trigger_rule="none_failed_min_one_success",
    )

    start_ec2_task >> run_forecast  >> stop_ec2_task >> fim

##################################################################################################

with DAG(
    'PREV_CHUVA_DB_ETA',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    schedule='10 5 * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Metereologia']
) as dag:
        
    start_ec2_task = PythonOperator(
        task_id='start_ec2',
        python_callable=start_instance,
        # provide_context=True,
    )
    
     # Task to run a command on the remote server
    
    run_forecast = SSHOperator(
        task_id='vl_chuva_to_db',
        remote_host="{{ ti.xcom_pull(task_ids='start_ec2', key='public_ip') }}",
        ssh_conn_id='ssh_ecmwf',
        command="{{ '/home/admin/enviMetereologia/bin/python /home/admin/rotinas/gera_forecast_to_db.py eta' }}",
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
        trigger_rule=TriggerRule.ALL_DONE,

        
    )

    stop_ec2_task = PythonOperator(
        task_id='stop_ec2',
        python_callable=stop_instance,
        op_kwargs={'instance_id': 'i-0edbeb5435710d5f3', 'region': 'us-east-1'},
        trigger_rule=TriggerRule.ALL_DONE,
    )

    fim = DummyOperator(
        task_id='fim',
        trigger_rule="none_failed_min_one_success",
    )

    start_ec2_task >> run_forecast  >> stop_ec2_task >> fim

##################################################################################################

with DAG(
    'C3S',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    schedule='30 13 10 * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Metereologia']
) as dag:


    start_ec2_task = PythonOperator(
        task_id='start_ec2',
        python_callable=start_instance,
        # provide_context=True,
    )

     # Task to run a command on the remote server

    run_cpc = SSHOperator(
        task_id='run_c3s',
        remote_host="{{ ti.xcom_pull(task_ids='start_ec2', key='public_ip') }}",
        ssh_conn_id='ssh_ecmwf',
        command="{{ '/home/admin/rotinas/c3s/produtos.sh' }}",
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
        trigger_rule=TriggerRule.ALL_DONE,

        
    )

    run_shell_script = SSHOperator(
        task_id='gera_produtos_c3s',
        command="{{'/home/admin/jose/c3s/produtos.sh'}}",
        dag=dag,
        ssh_conn_id='ssh_master',
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,

        
    )

    stop_ec2_task = PythonOperator(
        task_id='stop_ec2',
        python_callable=stop_instance,
        op_kwargs={'instance_id': 'i-0edbeb5435710d5f3', 'region': 'us-east-1'},
        trigger_rule=TriggerRule.ALL_DONE,
    )

    fim = DummyOperator(
        task_id='fim',
        trigger_rule="none_failed_min_one_success",
    )

    start_ec2_task >> run_cpc >> run_shell_script >> stop_ec2_task >> fim


##################################################################################################

DIR_PRODUTOS_MAPAS = '/WX2TB/Documentos/fontes/tempo/novos_produtos/'

with DAG(
    'Mapas_GFS',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    schedule='0 2,8,14,20 * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Metereologia', 'Mapas'],
    default_args={"do_xcom_push":False}


) as dag:

    run_shell_script = SSHOperator(
        task_id='roda_produtos_gfs',
        command="{{'/WX2TB/Documentos/fontes/tempo/novos_produtos/gfs/produtos.sh'}}",
        dag=dag,
        ssh_conn_id='ssh_master',
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
    )

##################################################################################################

with DAG(
    'Mapas_GEFS',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    schedule='0 3,8,15,20 * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Metereologia', 'Mapas'],
    default_args={"do_xcom_push":False}


) as dag:

    run_shell_script = SSHOperator(
        task_id='roda_produtos_gefs',
        command="{{'/WX2TB/Documentos/fontes/tempo/novos_produtos/gefs/produtos.sh'}}",
        dag=dag,
        ssh_conn_id='ssh_master',
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
    )

##################################################################################################

with DAG(
    'Mapas_GEFS-MEMBROS',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    schedule='0 3,8,15,20 * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Metereologia', 'Mapas'],
    default_args={"do_xcom_push":False}


) as dag:

    run_shell_script = SSHOperator(
        task_id='roda_produtos_gefs-membros',
        command="{{'/WX2TB/Documentos/fontes/tempo/novos_produtos/gefs-membros/produtos.sh'}}",
        dag=dag,
        ssh_conn_id='ssh_master',
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
    )

##################################################################################################

with DAG(
    'Mapas_ECMWF',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    schedule='0 4,16 * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Metereologia', 'Mapas'],
    default_args={"do_xcom_push":False}


) as dag:

    run_shell_script = SSHOperator(
        task_id='roda_produtos_ecmwf',
        command="{{'/WX2TB/Documentos/fontes/tempo/novos_produtos/ecmwf/produtos.sh'}}",
        dag=dag,
        ssh_conn_id='ssh_master',
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
    )

##################################################################################################

with DAG(
    'Mapas_ECMWF-NOVARES',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    schedule='0 4 * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Metereologia', 'Mapas'],
    default_args={"do_xcom_push":False}


) as dag:

    run_shell_script = SSHOperator(
        task_id='roda_produtos_ecmwfnovares',
        command="{{'/WX2TB/Documentos/fontes/tempo/novos_produtos/ecmwf-novares/produtos.sh'}}",
        dag=dag,
        ssh_conn_id='ssh_master',
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
    )

##################################################################################################

with DAG(
    'Mapas_ECMWF-ENS',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    schedule='0 6,18 * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Metereologia', 'Mapas'],
    default_args={"do_xcom_push":False}


) as dag:

    run_shell_script = SSHOperator(
        task_id='roda_produtos_ecmwf-ens',
        command="{{'/WX2TB/Documentos/fontes/tempo/novos_produtos/ecmwf-ens/produtos.sh'}}",
        dag=dag,
        ssh_conn_id='ssh_master',
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
    )

##################################################################################################

with DAG(
    'Mapas_ECMWF-ENS-CHUVA',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    schedule='0 6,18 * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Metereologia', 'Mapas'],
    default_args={"do_xcom_push":False}


) as dag:

    run_shell_script = SSHOperator(
        task_id='roda_produtos_ecmwf-ens-chuva',
        command="{{'/WX2TB/Documentos/fontes/tempo/novos_produtos/ecmwf-ens-chuva/produtos.sh'}}",
        dag=dag,
        ssh_conn_id='ssh_master',
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
    )

##################################################################################################

with DAG(
    'Mapas_ECMWF-ENS-MEMBROS',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    schedule='0 6,18 * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Metereologia', 'Mapas'],
    default_args={"do_xcom_push":False}


) as dag:

    run_shell_script = SSHOperator(
        task_id='roda_produtos_ecmwf-ens-membros',
        command="{{'/WX2TB/Documentos/fontes/tempo/novos_produtos/ecmwf-ens-membros/produtos.sh'}}",
        dag=dag,
        ssh_conn_id='ssh_master',
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
    )

##################################################################################################

with DAG(
    'Mapas_ECMWF-AIFS',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    schedule='0 5,10,18,23 * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Metereologia', 'Mapas'],
    default_args={"do_xcom_push":False}


) as dag:

    run_shell_script = SSHOperator(
        task_id='roda_produtos_ecmwf-aifs',
        command="{{'/WX2TB/Documentos/fontes/tempo/novos_produtos/ecmwf-aifs/produtos.sh'}}",
        dag=dag,
        ssh_conn_id='ssh_master',
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
    )

##################################################################################################

with DAG(
    'Mapas_ETA',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    schedule='0 8 * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Metereologia', 'Mapas'],
    default_args={"do_xcom_push":False}


) as dag:

    run_shell_script = SSHOperator(
        task_id='roda_produtos_eta',
        command="{{'/WX2TB/Documentos/fontes/tempo/novos_produtos/eta/produtos.sh'}}",
        dag=dag,
        ssh_conn_id='ssh_master',
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
    )

##################################################################################################

with DAG(
    'Mapas_ETA-CPTEC',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    schedule='10 5 * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Metereologia', 'Mapas'],
    default_args={"do_xcom_push":False}


) as dag:

    run_shell_script = SSHOperator(
        task_id='roda_produtos_eta-cptec',
        command="{{'/WX2TB/Documentos/fontes/tempo/novos_produtos/eta-cptec/produtos.sh'}}",
        dag=dag,
        ssh_conn_id='ssh_master',
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
    )

##################################################################################################

with DAG(
    'Mapas_GEFS-ESTENDIDO',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    schedule='2 0 * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Metereologia', 'Mapas'],
    default_args={"do_xcom_push":False}


) as dag:

    run_shell_script = SSHOperator(
        task_id='roda_produtos_gefs-est',
        command="{{'/WX2TB/Documentos/fontes/tempo/novos_produtos/gefs-estendido/produtos.sh'}}",
        dag=dag,
        ssh_conn_id='ssh_master',
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
    )

##################################################################################################

with DAG(
    'Mapas_GEFS-ESTENDIDO-MEMBROS',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    schedule='2 0 * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Metereologia', 'Mapas'],
    default_args={"do_xcom_push":False}


) as dag:

    run_shell_script = SSHOperator(
        task_id='roda_produtos_gefs-est-membros',
        command="{{'/WX2TB/Documentos/fontes/tempo/novos_produtos/gefs-membros-estendido/produtos.sh'}}",
        dag=dag,
        ssh_conn_id='ssh_master',
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
    )

##################################################################################################

with DAG(
    'Mapas_ECMWF-ESTENDIDO',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    schedule='5 17 * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Metereologia', 'Mapas'],
    default_args={"do_xcom_push":False}


) as dag:

    run_shell_script = SSHOperator(
        task_id='roda_produtos_ecmwf-est',
        command="{{'/WX2TB/Documentos/fontes/tempo/novos_produtos/ecmwf-ens-estendido/produtos.sh'}}",
        dag=dag,
        ssh_conn_id='ssh_master',
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
    )

##################################################################################################

with DAG(
    'Mapas_ECMWF-ESTENDIDO-MEMBROS',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    schedule='5 17 * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Metereologia', 'Mapas'],
    default_args={"do_xcom_push":False}


) as dag:

    run_shell_script = SSHOperator(
        task_id='roda_produtos_ecmwf-est-membros',
        command="{{'/WX2TB/Documentos/fontes/tempo/novos_produtos/ecmwf-ens-estendido-membros/produtos.sh'}}",
        dag=dag,
        ssh_conn_id='ssh_master',
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
    )

##################################################################################################

with DAG(
    'Mapas_ECMWF-ESTENDIDO-HINDCAST',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    schedule='50 18 * * 1,4',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Metereologia', 'Mapas'],
    default_args={"do_xcom_push":False}


) as dag:

    run_shell_script = SSHOperator(
        task_id='roda_produtos_ecmwf-est-hindcast',
        command="{{'/WX2TB/Documentos/fontes/tempo/novos_produtos/ecmwf-estendido-hindcast/produtos.sh'}}",
        dag=dag,
        ssh_conn_id='ssh_master',
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
    )

##################################################################################################

with DAG(
    'Mapas_MERGE-GPM',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    schedule='30 13 * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Metereologia', 'Mapas'],
    default_args={"do_xcom_push":False}


) as dag:

    run_shell_script = SSHOperator(
        task_id='roda_produtos_merge-gpm-daily',
        command="{{'/WX2TB/Documentos/fontes/tempo/novos_produtos/merge-gpm-daily/produtos.sh'}}",
        dag=dag,
        ssh_conn_id='ssh_master',
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
    )

##################################################################################################

with DAG(
    'Mapas_MERGE-GPM-PREVOBS',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    schedule='40 13 * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Metereologia', 'Mapas'],
    default_args={"do_xcom_push":False}


) as dag:

    run_shell_script = SSHOperator(
        task_id='roda_produtos_merge-gpm-daily-prevobs',
        command="{{'/WX2TB/Documentos/fontes/tempo/novos_produtos/merge-gpm-daily/produtos_prevobsparalelo.sh'}}",
        dag=dag,
        ssh_conn_id='ssh_master',
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
    )

##################################################################################################

with DAG(
    'Mapas_CPC',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    schedule='35 10 * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Metereologia', 'Mapas'],
    default_args={"do_xcom_push":False}


) as dag:

    run_shell_script = SSHOperator(
        task_id='roda_produtos_cpc',
        command="{{'/WX2TB/Documentos/fontes/tempo/novos_produtos/cpc/produtos.sh'}}",
        dag=dag,
        ssh_conn_id='ssh_master',
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
    )

##################################################################################################

with DAG(
    'Mapas_CFSv2_Semanal',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    schedule='10 6,12,18,0 * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Metereologia', 'Mapas'],
    default_args={"do_xcom_push":False}


) as dag:

    run_shell_script = SSHOperator(
        task_id='roda_produtos_cfsv2semanal',
        command="{{'/WX2TB/Documentos/fontes/tempo/novos_produtos/cfsv2/produtos.sh'}}",
        dag=dag,
        ssh_conn_id='ssh_master',
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
    )

##################################################################################################

with DAG(
    'Mapas_CFSv2_Mensal',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    schedule='10 6,12,18,0 * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Metereologia', 'Mapas'],
    default_args={"do_xcom_push":False}


) as dag:

    run_shell_script = SSHOperator(
        task_id='roda_produtos_cfsv2mensal',
        command="{{'/WX2TB/Documentos/fontes/tempo/novos_produtos/cfsv2-mensal/produtos.sh'}}",
        dag=dag,
        ssh_conn_id='ssh_master',
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
    )

##################################################################################################

with DAG(
    'Mapas_ClusterONS',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    schedule='24 6 * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Metereologia', 'Mapas'],
    default_args={"do_xcom_push":False}


) as dag:

    run_shell_script = SSHOperator(
        task_id='roda_produtos_clusterons',
        command="{{'/WX2TB/Documentos/fontes/tempo/novos_produtos/cluster_ons/produtos.sh'}}",
        dag=dag,
        ssh_conn_id='ssh_master',
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
    )

##################################################################################################

with DAG(
    'Mapas_PSAT',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    schedule='10 14 * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Metereologia', 'Mapas'],
    default_args={"do_xcom_push":False}


) as dag:

    run_shell_script = SSHOperator(
        task_id='roda_produtos_psat',
        command="{{'/WX2TB/Documentos/fontes/tempo/novos_produtos/psat/produtos.sh'}}",
        dag=dag,
        ssh_conn_id='ssh_master',
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
    )

##################################################################################################

with DAG(
    'Mapas_NMME',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    schedule='30 2 8 * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Metereologia', 'Mapas'],
    default_args={"do_xcom_push":False}


) as dag:

    run_shell_script = SSHOperator(
        task_id='roda_produtos_nmme',
        command="{{'/WX2TB/Documentos/fontes/tempo/novos_produtos/nmme/produtos.sh'}}",
        dag=dag,
        ssh_conn_id='ssh_master',
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
    )

##################################################################################################

with DAG(
    'Indices_teleconexao',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    schedule='10 12 * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Metereologia', 'Mapas'],
    default_args={"do_xcom_push":False}


) as dag:

    run_shell_script = SSHOperator(
        task_id='roda_produtos_indices_teleconexoes',
        command="{{'/WX2TB/Documentos/fontes/tempo/novos_produtos/csv_teleconexao/produtos.sh'}}",
        dag=dag,
        ssh_conn_id='ssh_master',
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
    )

##################################################################################################

with DAG(
    'Mapas_Temperatura_SaMET',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    schedule='0 1 * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Metereologia', 'Mapas'],
    default_args={"do_xcom_push":False}


) as dag:

    run_shell_script = SSHOperator(
        task_id='roda_produtos_temp_samet',
        command="{{'/WX2TB/Documentos/fontes/tempo/novos_produtos/temp_samet/produtos.sh'}}",
        dag=dag,
        ssh_conn_id='ssh_master',
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
    )

##################################################################################################

with DAG(
    'Mapas_Reanalises',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    schedule='30 14 * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Metereologia', 'Mapas'],
    default_args={"do_xcom_push":False}


) as dag:

    run_shell_script = SSHOperator(
        task_id='roda_produtos_reanalises',
        command="{{'/WX2TB/Documentos/fontes/tempo/novos_produtos/reanalises/novo/produtos.sh'}}",
        dag=dag,
        ssh_conn_id='ssh_master',
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
    )

##################################################################################################

with DAG(
    'Mapas_Reanalises_mensais',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    schedule='30 14 * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Metereologia', 'Mapas'],
    default_args={"do_xcom_push":False}


) as dag:

    run_shell_script = SSHOperator(
        task_id='roda_produtos_reanalises_mensais',
        command="{{'/WX2TB/Documentos/fontes/tempo/novos_produtos/reanalises/novo/produtos_mensais.sh'}}",
        dag=dag,
        ssh_conn_id='ssh_master',
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
    )

##################################################################################################

with DAG(
    'Dirs_CFSv2',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    schedule='10 0 * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Metereologia', 'Mapas'],
    default_args={"do_xcom_push":False}


) as dag:

    run_shell_script = SSHOperator(
        task_id='roda_produtos_limpadirscfsv2',
        command="{{'/WX2TB/Documentos/fontes/tempo/novos_produtos/limpa_dirs/produtos.sh'}}",
        dag=dag,
        ssh_conn_id='ssh_master',
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
    )

##################################################################################################

with DAG(
    'Dados_MJO_BoM',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    schedule='0 3 * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Metereologia', 'Mapas'],
    default_args={"do_xcom_push":False}


) as dag:

    run_shell_script = SSHOperator(
        task_id='roda_produtos_mjobom',
        command="{{'/WX2TB/Documentos/fontes/tempo/novos_produtos/mjo/produtos.sh'}}",
        dag=dag,
        ssh_conn_id='ssh_master',
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
    )

##################################################################################################

with DAG(
    'Mapas_ZCIT_Funceme',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    schedule='0 12 * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Metereologia', 'Mapas'],
    default_args={"do_xcom_push":False}


) as dag:

    run_shell_script = SSHOperator(
        task_id='roda_produtos_zcit_funceme',
        command="{{'/WX2TB/Documentos/fontes/tempo/novos_produtos/zcit/produtos.sh'}}",
        dag=dag,
        ssh_conn_id='ssh_master',
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
    )

##################################################################################################

with DAG(
    'Mapas_Umidade_do_solo',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    schedule='0 8 * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Metereologia', 'Mapas'],
    default_args={"do_xcom_push":False}


) as dag:

    run_shell_script = SSHOperator(
        task_id='roda_produtos_umidade_solo',
        command="{{'/WX2TB/Documentos/fontes/tempo/novos_produtos/umidade_solo/produtos.sh'}}",
        dag=dag,
        ssh_conn_id='ssh_master',
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
    )

##################################################################################################

with DAG(
    'Mapas_temperatura_subsuperficial_ECMWF',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    schedule='0 8 * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Metereologia', 'Mapas'],
    default_args={"do_xcom_push":False}


) as dag:

    run_shell_script = SSHOperator(
        task_id='roda_produtos_temp_subsuperficial',
        command="{{'/WX2TB/Documentos/fontes/tempo/novos_produtos/temp_subsuperficial/produtos.sh'}}",
        dag=dag,
        ssh_conn_id='ssh_master',
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
    )

##################################################################################################

with DAG(
    'Mapas_PCONJUNTO',
    start_date= datetime.datetime(2024, 4, 28),
    description='A simple SSH command execution example',
    schedule='5 8 * * *',
    catchup=False,
    max_active_runs=1,
    concurrency=1,
    tags=['Metereologia', 'Mapas'],
    default_args={"do_xcom_push":False}


) as dag:

    run_shell_script = SSHOperator(
        task_id='roda_produtos_pconjunto',
        command="{{'/WX2TB/Documentos/fontes/tempo/novos_produtos/pconjunto/produtos.sh'}}",
        dag=dag,
        ssh_conn_id='ssh_master',
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
    )

##################################################################################################