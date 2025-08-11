import boto3
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.ssh.operators.ssh import SSHOperator

import datetime

import os
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")),'.env'))

TIME_OUT = 60*60


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
    # Task to run a command on the remote server
    run_pautas_aneel = SSHOperator(
        do_xcom_push=False,
        task_id='run_pautas_aneel',
        ssh_conn_id='ssh_master',
        command="{{ 'source /WX2TB/pythonVersions/myVenv38/bin/activate; cd /WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/verificadores/aneel/verificador_pauta;  python verificador_pauta.py' }}",
        conn_timeout = TIME_OUT,
        cmd_timeout = TIME_OUT,
        execution_timeout = datetime.timedelta(hours=30),
        get_pty=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    fim = DummyOperator(
        task_id='fim',
        trigger_rule="none_failed_min_one_success",
    )
    inicio >> run_pautas_aneel >> fim