from datetime import datetime
from datetime import timedelta
from airflow import DAG

from airflow.providers.ssh.operators.ssh import SSHOperator



default_args = {

    'execution_timeout': timedelta(hours=8)
}

with DAG(
    default_args = default_args,
    dag_id = '1.1-PROSPEC_PCONJUNTO_DEFINITIVO', 
    start_date=datetime(2024, 4, 28), 
    schedule_interval=None, 
    catchup=False,

    ) as dag:
    
    run_prospec_on_host = SSHOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='run_prospec_on_host',
        ssh_conn_id='ssh_master',  # Ensure this matches the connection ID set in the Airflow UI
        command="{{ 'cd /WX2TB/Documentos/fontes/PMO/rodada_automatica_prospec/script; ./rodaProspec2rvsDefinitivo.ksh' }}",
        conn_timeout = 36000,
        cmd_timeout = 28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
    )


with DAG(
    default_args = default_args,
    dag_id = '1.3-PROSPEC_1RV', 
    start_date=datetime(2024, 4, 28), 
    schedule_interval="30 07 * * 1-4", 
    catchup=False
    ) as dag:
    
    run_prospec_on_host = SSHOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='run_prospec_1rv',
        ssh_conn_id='ssh_master',  # Ensure this matches the connection ID set in the Airflow UI
        command="{{ 'cd /WX2TB/Documentos/fontes/PMO/rodada_automatica_prospec/script; ./rodaProspec1rv_pluvia_raizen.ksh' }}",
        conn_timeout = 28800,
        cmd_timeout = 28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
    )


with DAG(
    default_args = default_args,
    dag_id = '1.4-PROSPEC_EC_EXT', 
    start_date=datetime(2024, 4, 28), 
    schedule_interval="00 22 * * 1-4", 
    catchup=False
    ) as dag:
    
    run_prospec_on_host = SSHOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='run_prospec_ec_ext',
        ssh_conn_id='ssh_master',  # Ensure this matches the connection ID set in the Airflow UI
        command="{{ 'cd /WX2TB/Documentos/fontes/PMO/rodada_automatica_prospec/script; ./rodaProspec5rvs.ksh' }}",
        conn_timeout = 28800,
        cmd_timeout = 28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
    )


with DAG(
    default_args = default_args,
    dag_id = '1.2-PROSPEC_PCONJUNTO_PREL', 
    start_date=datetime(2024, 4, 28), 
    schedule_interval='00 07 * * *', 
    catchup=False
    ) as dag:
    
    run_prospec_on_host = SSHOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='run_prospec_pconj_prel',
        ssh_conn_id='ssh_master',  # Ensure this matches the connection ID set in the Airflow UI
        command="{{ 'cd /WX2TB/Documentos/fontes/PMO/rodada_automatica_prospec/script; ./rodaProspec3rvs.ksh' }}",
        conn_timeout = 28800,
        cmd_timeout = 28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
    )


    
with DAG(
    default_args = default_args,
    dag_id = '1.7-PROSPEC_CHUVA_0', 
    start_date=datetime(2024, 4, 28), 
    schedule_interval='00 8 * * 1', 
    catchup=False
    ) as dag:
    
    run_prospec_on_host = SSHOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='run_prospec_chuva0',
        ssh_conn_id='ssh_master',  # Ensure this matches the connection ID set in the Airflow UI
        command="{{ 'cd /WX2TB/Documentos/fontes/PMO/rodada_automatica_prospec/script; ./rodaProspecPzero.ksh' }}",
        conn_timeout = 28800,
        cmd_timeout = 28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
    )


with DAG(
    default_args = default_args,
    dag_id = '1.5-PROSPEC_CENARIO_10', 
    start_date=datetime(2024, 4, 28), 
    schedule_interval='40 6 * * 1-5', 
    catchup=False
    ) as dag:
    
    run_prospec_on_host = SSHOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='run_prospec_cenario_10',
        ssh_conn_id='ssh_master',  # Ensure this matches the connection ID set in the Airflow UI
        command="{{ 'cd /WX2TB/Documentos/fontes/PMO/rodada_automatica_prospec/script; ./rodaProspecCenarios.ksh 10' }}",
        conn_timeout = 28800,
        cmd_timeout = 28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
    )

with DAG(
    default_args = default_args,
    dag_id = '1.6-PROSPEC_CENARIO_11', 
    start_date=datetime(2024, 4, 28), 
    schedule_interval='40 7 * * 1-5', 
    catchup=False
    ) as dag:
    
    run_prospec_on_host = SSHOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='run_prospec_cenario_11',
        ssh_conn_id='ssh_master',  # Ensure this matches the connection ID set in the Airflow UI
        command="{{ 'cd /WX2TB/Documentos/fontes/PMO/rodada_automatica_prospec/script; ./rodaProspecCenarios.ksh 11' }}",
        conn_timeout = 28800,
        cmd_timeout = 28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
    )