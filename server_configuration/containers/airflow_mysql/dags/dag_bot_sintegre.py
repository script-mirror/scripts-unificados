import datetime
from airflow.models.dag import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator



with DAG(
    dag_id='BOT_SINTEGRE',
    tags=["WEBHOOK", "SINTEGRE"],
    start_date= datetime.date(2025, 2, 10),
    catchup=False,
    schedule="0 23 * * *"
) as dag:
    start_bot = SSHOperator(
        task_id='start_bot',
        ssh_conn_id='ssh_master',
        command='/WX2TB/Documentos/fontes/PMO/raizen-power-trading-middle-bot-sintegre/env/bin/python main.py',
    )
start_bot
