from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.models import DagBag
import subprocess


default_args = {
    'execution_timeout': timedelta(hours=8)
}


# Função que executa o script com parâmetros dinâmicos
def run_python_script_with_dynamic_params(**kwargs):
    # Recuperando todos os parâmetros passados para a DAG
    print(kwargs.get('params'))
    params = kwargs.get('params', {})
    print(params)
    # Iniciando a construção do comando para o script
    command = " . /WX/WX2TB/Documentos/fontes/PMO/scripts_unificados/env/bin/activate; python /WX2TB/Documentos/fontes/PMO/rodada_automatica_prospec/script/mainRodadaAutoProspec.py"
    
    # Adicionando parâmetros ao comando dinamicamente, se existirem
    for key, value in params.items():
        if value is not None:  # Verifica se o valor não é None
            # Adiciona o parâmetro e o valor ao comando
            command += f" {key} '{value}'"
    print(command)
    kwargs['ti'].xcom_push(key='command', value=command)

# Definindo a DAG para '1.0-ENVIAR-EMAIL-ESTUDOS' (adicionada)
with DAG(
    default_args=default_args,
    dag_id='1.0-ENVIAR-EMAIL-ESTUDOS', 
    start_date=datetime(2025, 1, 23), 
    schedule_interval=None, 
    catchup=False,
) as dag:
    run_script_task = PythonOperator(
        task_id='run_script_with_dynamic_params',
        python_callable=run_python_script_with_dynamic_params,
        #op_kwargs={
            #'params': {
                #'apenas_email': 1,  # Valor padrão para apenas_email
           # }
        #},
    )
    run_prospec_on_host = SSHOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='run',
        ssh_conn_id='ssh_master',  
        command="{{ ti.xcom_pull(task_ids='run_script_with_dynamic_params', key='command')}}",
        conn_timeout=36000,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
    )

    run_script_task >> run_prospec_on_host

# python /WX2TB/Documentos/fontes/PMO/rodada_automatica_prospec/script/mainRodadaAutoProspec.py prevs PREVS_PLUVIA_2_RV rvs 2 preliminar 0 nome_estudo CVU-ATUALIZADO
# Definindo a DAG para '1.1-PROSPEC_PCONJUNTO_DEFINITIVO'
with DAG(
    default_args=default_args,
    dag_id='1.1-PROSPEC_PCONJUNTO_DEFINITIVO', 
    start_date=datetime(2024, 4, 28), 
    schedule_interval=None, 
    catchup=False,
) as dag:
    run_prospec_on_host = SSHOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='run_prospec_on_host',
        ssh_conn_id='ssh_master',  
        command="{{ 'cd /WX2TB/Documentos/fontes/PMO/rodada_automatica_prospec/script; ./rodaProspec2rvsDefinitivo.ksh' }}",
        conn_timeout=36000,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
    )


# Definindo a DAG para '1.2-PROSPEC_PCONJUNTO_PREL'
with DAG(
    default_args=default_args,
    dag_id='1.2-PROSPEC_PCONJUNTO_PREL', 
    start_date=datetime(2024, 4, 28), 
    schedule_interval='02 07 * * *', 
    catchup=False,
) as dag:
    run_prospec_on_host = SSHOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='run_prospec_pconj_prel',
        ssh_conn_id='ssh_master',  
        command="{{ 'cd /WX2TB/Documentos/fontes/PMO/rodada_automatica_prospec/script; ./rodaProspec3rvs.ksh' }}",
        conn_timeout=28800,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
    )

 # 
# Definindo a DAG para '1.3-PROSPEC_1RV'
with DAG(
    default_args=default_args,
    dag_id='1.3-PROSPEC_1RV', 
    start_date=datetime(2024, 4, 28), 
    schedule_interval="51 06 * * 1-4", 
    catchup=False,
) as dag:
    run_prospec_on_host = SSHOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='run_prospec_1rv',
        ssh_conn_id='ssh_master',  
        command="{{ 'cd /WX2TB/Documentos/fontes/PMO/rodada_automatica_prospec/script; ./rodaProspec1rv_pluvia_raizen.ksh' }}",
        conn_timeout=28800,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
    )


# Definindo a DAG para '1.4-PROSPEC_EC_EXT'
with DAG(
    default_args=default_args,
    dag_id='1.4-PROSPEC_EC_EXT', 
    start_date=datetime(2024, 4, 28), 
    schedule_interval="00 19 * * 1-4", 
    catchup=False,
) as dag:
    run_prospec_on_host = SSHOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='run_prospec_ec_ext',
        ssh_conn_id='ssh_master',  
        command="{{ 'cd /WX2TB/Documentos/fontes/PMO/rodada_automatica_prospec/script; ./rodaProspec5rvs.ksh' }}",
        conn_timeout=28800,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
    )


# Definindo a DAG para '1.5-PROSPEC_CENARIO_10'
with DAG(
    default_args=default_args,
    dag_id='1.5-PROSPEC_CENARIO_10', 
    start_date=datetime(2024, 4, 28), 
    schedule_interval='42 6 * * 1-5', 
    catchup=False,
) as dag:
    run_prospec_on_host = SSHOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='run_prospec_cenario_10',
        ssh_conn_id='ssh_master',  
        command="{{ 'cd /WX2TB/Documentos/fontes/PMO/rodada_automatica_prospec/script; ./rodaProspecCenarios.ksh 10' }}",
        conn_timeout=28800,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
    )


# Definindo a DAG para '1.6-PROSPEC_CENARIO_11'
with DAG(
    default_args=default_args,
    dag_id='1.6-PROSPEC_CENARIO_11', 
    start_date=datetime(2024, 4, 28), 
    schedule_interval='33 7 * * 1-5', 
    catchup=False,
) as dag:
    run_prospec_on_host = SSHOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='run_prospec_cenario_11',
        ssh_conn_id='ssh_master',  
        command="{{ 'cd /WX2TB/Documentos/fontes/PMO/rodada_automatica_prospec/script; ./rodaProspecCenarios.ksh 11' }}",
        conn_timeout=28800,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
    )


# Definindo a DAG para '1.7-PROSPEC_CHUVA_0'
with DAG(
    default_args=default_args,
    dag_id='1.7-PROSPEC_CHUVA_0', 
    start_date=datetime(2024, 4, 28), 
    schedule_interval='00 8 * * 1', 
    catchup=False,
) as dag:
    run_prospec_on_host = SSHOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='run_prospec_chuva0',
        ssh_conn_id='ssh_master',  
        command="{{ 'cd /WX2TB/Documentos/fontes/PMO/rodada_automatica_prospec/script; ./rodaProspecPzero.ksh' }}",
        conn_timeout=28800,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
    )

# Definindo a DAG para '2.0-PROSPEC_ATUALIZAÇOÊS'

# Função que executa o script com parâmetros dinâmicos
def run_python_update_with_dynamic_params(**kwargs):
    # Recuperando todos os parâmetros passados para a DAG
    print(kwargs.get('params'))
    params = kwargs.get('params', {})
    print(params)
    # Iniciando a construção do comando para o script
    command = " . /WX/WX2TB/Documentos/fontes/PMO/scripts_unificados/env/bin/activate; python /WX2TB/Documentos/fontes/PMO/rodada_automatica_prospec/script/mainRodadaAutoProspec.py prevs PREVS_PLUVIA_RAIZEN rvs 1 mapas ONS_Pluvia"
    
    # Adicionando parâmetros ao comando dinamicamente, se existirem
    for key, value in params.items():
        if value is not None:  # Verifica se o valor não é None
            # Adiciona o parâmetro e o valor ao comando
            command += f" {key} '{value}'"
    print(command)
    kwargs['ti'].xcom_push(key='command', value=command)

# Definindo a DAG para '1.0-ENVIAR-EMAIL-ESTUDOS' (adicionada)
with DAG(
    default_args=default_args,
    dag_id='2.0-PROSPEC_ATUALIZACAO', 
    start_date=datetime(2025, 1, 23), 
    schedule_interval=None, 
    catchup=False,
) as dag:
    run_script_task = PythonOperator(
        task_id='run_update_with_dynamic_params',
        python_callable=run_python_update_with_dynamic_params,

    )
    run_prospec_on_host = SSHOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='run',
        ssh_conn_id='ssh_master',  
        command="{{ ti.xcom_pull(task_ids='run_update_with_dynamic_params', key='command')}}",
        conn_timeout=36000,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
    )

    run_script_task >> run_prospec_on_host

# Definindo a DAG para '2.1-PROSPEC_NAO-CONSISTIDO'

# Função que executa o script com parâmetros dinâmicos
def run_python_with_dynamic_params(**kwargs):
    # Recuperando todos os parâmetros passados para a DAG
    print(kwargs.get('params'))
    params = kwargs.get('params', {})
    print(params)
    # Iniciando a construção do comando para o script
    command = " . /WX/WX2TB/Documentos/fontes/PMO/scripts_unificados/env/bin/activate; python /WX2TB/Documentos/fontes/PMO/rodada_automatica_prospec/script/mainRodadaAutoProspec.py prevs PREVS_NAO_CONSISTIDO rvs 1"
    
    # Adicionando parâmetros ao comando dinamicamente, se existirem
    for key, value in params.items():
        if value is not None:  # Verifica se o valor não é None
            # Adiciona o parâmetro e o valor ao comando
            command += f" {key} '{value}'"
    print(command)
    kwargs['ti'].xcom_push(key='command', value=command)

with DAG(
    default_args=default_args,
    dag_id='2.1-PROSPEC_NAO_CONSISTIDO', 
    start_date=datetime(2025, 1, 23), 
    schedule_interval=None, 
    catchup=False,
) as dag:
    run_script_task = PythonOperator(
        task_id='run_update_dynamic_params',
        python_callable=run_python_with_dynamic_params,

    )
    run_prospec_on_host = SSHOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='run',
        ssh_conn_id='ssh_master',  
        command="{{ ti.xcom_pull(task_ids='run_update_dynamic_params', key='command')}}",
        conn_timeout=36000,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
    )

    run_script_task >> run_prospec_on_host 


# Definindo a DAG para 1.8-PROSPEC_GRUPOS-ONS
# Função que verifica o estado da DAG
def check_dag_state(dag_id='1.8-PROSPEC_GRUPOS-ONS'):
    # Carrega a DAG e verifica se está pausada
    dagbag = DagBag()
    dag = dagbag.get_dag(dag_id)
    
    if dag.is_paused:
        return 'skip_task'  # Se estiver pausada, vai para a tarefa "skip_task"
    else:
        return 'run_decomp_ons_grupos'  # Se não estiver pausada, continua para a tarefa SSH

# Tarefa que apenas "pula" a execução
def skip_task():
    print("A DAG está pausada, a tarefa não será executada.")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 28),
}

with DAG(
    dag_id='1.8-PROSPEC_GRUPOS-ONS', 
    start_date=datetime(2024, 4, 28), 
    schedule_interval=None, 
    catchup=False
) as dag:

    # Verifica o estado da DAG antes de executar qualquer tarefa
    check_dag_state_task = BranchPythonOperator(
        task_id='check_dag_state',
        python_callable=check_dag_state,
        op_args=['1.8-PROSPEC_GRUPOS-ONS'],
        provide_context=True
    )

    # Tarefa que simula o "skip" se a DAG estiver pausada
    skip_task = PythonOperator(
        task_id='skip_task',
        python_callable=skip_task,
    )

    # Tarefa SSH que será executada somente se a DAG não estiver pausada
    run_decomp_ons_grupos = SSHOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='run_decomp_ons_grupos',
        ssh_conn_id='ssh_master',  # Verifique se isso corresponde ao ID de conexão configurado na UI do Airflow
        command=" . /WX/WX2TB/Documentos/fontes/PMO/scripts_unificados/env/bin/activate; python /WX2TB/Documentos/fontes/PMO/rodada_automatica_prospec/script/mainRodadaAutoProspec.py prevs PREVS_ONS_GRUPOS preliminar 0",
        conn_timeout=None,
        cmd_timeout=None,
        get_pty=True,
        execution_timeout=timedelta(hours=20),
    )

    # Definindo a ordem das tarefas
    check_dag_state_task >> [run_decomp_ons_grupos, skip_task]

