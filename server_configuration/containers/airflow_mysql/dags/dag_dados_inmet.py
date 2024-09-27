from datetime import datetime
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator


with DAG(
    dag_id = 'INMET', 
    tags=["Verificador","INMET","Chuva Observada"],
    start_date=datetime(2024, 4, 28), 
    schedule_interval= "5 * * * * ", 
    catchup=False
    ) as dag:
    
    run_decomp_on_host = SSHOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='get_inmet_data',
        ssh_conn_id='ssh_master', 
        command=" source /WX2TB/pythonVersions/myVenv38/bin/activate; cd /WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/verificadores/inmet; python get_dados_inmet.py",
        conn_timeout = None,
        cmd_timeout = None,
        get_pty=True,
    )


with DAG(
    dag_id = 'SIMEPAR', 
    tags=["Verificador","SIMEPAR","Chuva Observada"],
    start_date=datetime(2024, 4, 28), 
    schedule_interval= "30 * * * * ", 
    catchup=False
    ) as dag:
    
    run_decomp_on_host = SSHOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='get_simepar_data',
        ssh_conn_id='ssh_master', 
        command=" source /WX2TB/pythonVersions/myVenv38/bin/activate; cd /WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/verificadores/inmet; python get_dados_inmet.py simepar",
        conn_timeout = None,
        cmd_timeout = None,
        get_pty=True,
    )
