from datetime import datetime
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.python_operator import PythonOperator



def cmd_command(**kwargs):
    params = kwargs.get('dag_run').conf
    dt_ref = params.get('dt_ref')
    cmd = f'source /WX2TB/pythonVersions/myVenv38/bin/activate; cd /WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/dessem;python dessem.py executar_deck_convertido data {dt_ref}'
    print(f"Comando: {cmd}")
    kwargs['ti'].xcom_push(key='command', value=cmd)

with DAG(
    dag_id = 'DESSEM_convertido', 
    start_date=datetime(2024, 4, 28), 
    schedule_interval=None, 
    catchup=False
    ) as dag:

    # começo estrutura para rodar a sequencia das tarefas
    inicio = PythonOperator(
        task_id='inicio',
        trigger_rule="none_failed_min_one_success",
        python_callable = cmd_command
    )
    
    run_dessem_on_host = SSHOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='run_dessem_on_host',
        ssh_conn_id='ssh_master',  # Ensure this matches the connection ID set in the Airflow UI
        command="{{ ti.xcom_pull(task_ids='inicio', key='command')}}",
        conn_timeout = 36000,
        cmd_timeout = 28800,
        get_pty=True,
    )

    inicio >> run_dessem_on_host

    


