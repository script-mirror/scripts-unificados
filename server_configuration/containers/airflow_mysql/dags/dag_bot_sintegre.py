import datetime
from airflow.models.dag import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator



with DAG(
    dag_id='BOT_SINTEGRE',
    tags=["WEBHOOK", "SINTEGRE"],
    start_date= datetime.datetime(2025, 2, 10),
    catchup=False,
    schedule="*/3 4-23 * * *"
) as dag:
    start_bot = SSHOperator(
        task_id='start_bot',
        ssh_conn_id='ssh_master',
        command='cd /WX2TB/Documentos/fontes/PMO/raizen-power-trading-bot-sintegre/app; ../env/bin/python ./main.py',
        conn_timeout = 36000,
        cmd_timeout = 28800
    )
start_bot
