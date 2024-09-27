from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator


with DAG(
    dag_id = 'DECOMP', 
    start_date=datetime(2024, 4, 28), 
    schedule_interval=None, 
    catchup=False
    ) as dag:
    
    run_decomp_on_host = SSHOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='run_decomp',
        ssh_conn_id='ssh_master',  # Ensure this matches the connection ID set in the Airflow UI
        command=" . /WX/WX2TB/Documentos/fontes/PMO/scripts_unificados/env/bin/activate; python /WX/WX2TB/Documentos/fontes/PMO/backTest_DC/script/back_teste_decomp.py",
        conn_timeout = None,
        cmd_timeout = None,
        get_pty=True,
    )



with DAG(
    dag_id = 'ONS-GRUPOS', 
    start_date=datetime(2024, 4, 28), 
    schedule_interval=None, 
    catchup=False
    ) as dag:
    
    run_decomp_ons_grupos = SSHOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='run_decomp_ons_grupos',
        ssh_conn_id='ssh_master',  # Ensure this matches the connection ID set in the Airflow UI
        command=" . /WX/WX2TB/Documentos/fontes/PMO/scripts_unificados/env/bin/activate; python /WX2TB/Documentos/fontes/PMO/rodada_automatica_prospec/script/mainRodadaAutoProspec.py prevs PREVS_ONS_GRUPOS preliminar 0",
        conn_timeout = None,
        cmd_timeout = None,
        get_pty=True,
        execution_timeout=timedelta(hours=20),
    )
    

with DAG(
    dag_id = 'PROSPEC_PCONJUNTO_PREL_PRECIPITACAO', 
    start_date=datetime(2024, 7, 30), 
    schedule_interval='00 07 * * 1-5', 
    catchup=False
    ) as dag:
    
    run_pconjunto_prel_precipitacao = SSHOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='run_pconjunto_prel_precipitacao',
        ssh_conn_id='ssh_master',  # Ensure this matches the connection ID set in the Airflow UI
        command=" . /WX/WX2TB/Documentos/fontes/PMO/scripts_unificados/env/bin/activate; python /WX2TB/Documentos/fontes/PMO/rodada_automatica_prospec/script/mainRodadaAutoProspec.py prevs PREVS_PLUVIA_APR rvs 2 preliminar 0",
        conn_timeout = None,
        cmd_timeout = None,
        get_pty=True,
        execution_timeout=timedelta(hours=20),
    )

