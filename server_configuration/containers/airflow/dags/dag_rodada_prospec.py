import os
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.models import DagBag
from airflow.models import DagRun
from airflow.exceptions import AirflowSkipException
from airflow.utils.db import create_session
import subprocess
from middle.utils.constants import Constants 
consts = Constants()

CMD_BASE      = str(consts.ATIVAR_ENV) + " python " + str(consts.PATH_PROJETOS) + "/estudos-middle/estudos_prospec/main_roda_estudos.py "
CMD_BASE_SENS = str(consts.ATIVAR_ENV) + " python " + str(consts.PATH_PROJETOS) + "/estudos-middle/estudos_prospec/gerar_sensibilidade.py "
CMD_BASE_NW   = str(consts.ATIVAR_ENV) + " python " + str(consts.PATH_PROJETOS) + "/estudos-middle/estudos_prospec/run_nw_ons_to_ccee.py "
CMD_BASE_DC   = str(consts.ATIVAR_ENV) + " python " + str(consts.PATH_PROJETOS) + "/estudos-middle/estudos_prospec/run_dc_ons_to_ccee.py "

default_args = {
    'execution_timeout': timedelta(hours=8)
}

# Função para verificar se a DAG está em execução
def check_if_dag_is_running(**kwargs):
    dag_id = kwargs['dag'].dag_id
    # Obtém o execution_date da execução atual
    execution_date = kwargs['execution_date']

    # Cria uma sessão manualmente para consultar o banco de metadados
    with create_session() as session:
        # Consulta execuções ativas, excluindo a execução atual
        active_runs = session.query(DagRun).filter(
            DagRun.dag_id == dag_id,
            DagRun.state == 'running',
            DagRun.execution_date != execution_date  # Exclui a execução atual
        ).all()

        if active_runs:
            raise AirflowSkipException(f"DAG {dag_id} já está em execução. Pulando execução {active_runs}.")
        

# Função que executa o script com parâmetros dinâmicos
def run_python_script_with_dynamic_params(**kwargs):

    # Recuperando todos os parâmetros passados para a DAG
    params = kwargs.get('params', {})
    #conteudo = ' '.join(f'"{k}" "{v}"' for k, v in params.items())
    conteudo = ' '.join( f'"{k}" \'{v}\'' if k == "list_email" else f'"{k}" "{v}"' for k, v in params.items()
)    
    # Iniciando a construção do comando para o script
    command = CMD_BASE + conteudo
    
    print(command)
    kwargs['ti'].xcom_push(key='command', value=command)


# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Definindo a DAG para '1.00-ENVIAR-EMAIL-ESTUDOS' (adicionada)
with DAG(
    default_args=default_args,
    dag_id='1.00-ENVIAR-EMAIL-ESTUDOS', 
    start_date=datetime(2025, 1, 23), 
    schedule_interval=None, 
    catchup=False,
    tags=['Prospec'],
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

 # -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------      
# Definindo a DAG para '1.01-PROSPEC_PCONJUNTO_DEFINITIVO'
with DAG(
    dag_id='1.01-PROSPEC_PCONJUNTO_DEFINITIVO',
    default_args=default_args,
    start_date=datetime(2024, 4, 28),
    schedule_interval=None,
    catchup=False,
    tags=['Prospec'],
) as dag:
    # Tarefa de verificação
    check_running = PythonOperator(
        task_id='check_if_dag_is_running',
        python_callable=check_if_dag_is_running,
        provide_context=True,
    )

    # Tarefa principal
    run_prospec_on_host = SSHOperator(
        task_id='run_prospec_on_host',
        ssh_conn_id='ssh_master',
        command= CMD_BASE +"prevs P.CONJ rodada Definitiva",
        conn_timeout=36000,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule="none_failed_min_one_success",
    )

    # Dependência: check_running deve rodar antes
    check_running >> run_prospec_on_host

# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Definindo a DAG para '1.02-PROSPEC_PCONJUNTO_PREL'
with DAG(
    default_args=default_args,
    dag_id='1.02-PROSPEC_PCONJUNTO_PREL', 
    start_date=datetime(2024, 4, 28), 
    schedule_interval='02 07 * * *', 
    catchup=False,
    tags=['Prospec'],
) as dag:
    run_prospec_on_host = SSHOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='run_prospec_pconj_prel',
        ssh_conn_id='ssh_master',  
        command= CMD_BASE +"prevs P.CONJ rodada Preliminar",
        conn_timeout=28800,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
    )

# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Definindo a DAG para '1.03-PROSPEC_1RV'
with DAG(
    default_args=default_args,
    dag_id='1.03-PROSPEC_1RV', 
    start_date=datetime(2024, 4, 28), 
    schedule_interval="51 06 * * 1-4", 
    catchup=False,
    tags=['Prospec'],
) as dag:
    run_prospec_on_host = SSHOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='run_prospec_1rv',
        ssh_conn_id='ssh_master',  
        command= CMD_BASE +"prevs NEXT-RV rodada Preliminar",
        conn_timeout=28800,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
    )

# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Definindo a DAG para '1.04-PROSPEC_EC_EXT'
with DAG(
    default_args=default_args,
    dag_id='1.04-PROSPEC_EC_EXT', 
    start_date=datetime(2024, 4, 28), 
    schedule_interval="00 19 * * 1-4", 
    catchup=False,
    tags=['Prospec'],
) as dag:
    run_prospec_on_host = SSHOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='run_prospec_ec_ext',
        ssh_conn_id='ssh_master',
        command= CMD_BASE +"prevs EC-EXT rodada Definitiva", 
        conn_timeout=28800,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
    )

# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Definindo a DAG para '1.05-PROSPEC_CENARIO_10'
with DAG(
    default_args=default_args,
    dag_id='1.05-PROSPEC_CENARIO_10', 
    start_date=datetime(2024, 4, 28), 
    schedule_interval='42 6 * * 1-5', 
    catchup=False,
    tags=['Prospec'],
) as dag:
    run_prospec_on_host = SSHOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='run_prospec_cenario_10',
        ssh_conn_id='ssh_master',
        command= CMD_BASE +"prevs CENARIOS rodada Preliminar",  
        conn_timeout=28800,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
    )

# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Definindo a DAG para '1.06-PROSPEC_CENARIO_11'
with DAG(
    default_args=default_args,
    dag_id='1.06-PROSPEC_CENARIO_11', 
    start_date=datetime(2024, 4, 28), 
    schedule_interval='33 7 * * 1-5', 
    catchup=False,
    tags=['Prospec'],
) as dag:
    run_prospec_on_host = SSHOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='run_prospec_cenario_11',
        ssh_conn_id='ssh_master',  
        command= CMD_BASE +"prevs CENARIOS rodada Preliminar, cenario 11",
        conn_timeout=28800,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
    )

# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Definindo a DAG para '1.7-PROSPEC_CHUVA_0'
with DAG(
    default_args=default_args,
    dag_id='1.07-PROSPEC_CHUVA_0', 
    start_date=datetime(2024, 4, 28), 
    schedule_interval='00 8 * * 1', 
    catchup=False,
    tags=['Prospec'],
) as dag:
    run_prospec_on_host = SSHOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='run_prospec_chuva0',
        ssh_conn_id='ssh_master', 
        command= CMD_BASE +"prevs P.ZERO rodada Preliminar", 
        conn_timeout=28800,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
    )

# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Definindo a DAG para '1.10-PROSPEC_GFS'

# Função que executa o script com parâmetros dinâmicos
def run_python_gfs(**kwargs):
    # Recuperando todos os parâmetros passados para a DAG
    print(kwargs.get('params'))
    params = kwargs.get('params', {})
    print(params)
    # Iniciando a construção do comando para o script
    command= CMD_BASE + "prevs PREVS_PLUVIA_GFS rvs 8 mapas GFS",
    
    # Adicionando parâmetros ao comando dinamicamente, se existirem
    for key, value in params.items():
        if value is not None:  # Verifica se o valor não é None
            # Adiciona o parâmetro e o valor ao comando
            command += f" {key} '{value}'"
    print(command)
    kwargs['ti'].xcom_push(key='command', value=command)

# Definindo a DAG para '1.00-ENVIAR-EMAIL-ESTUDOS' (adicionada)
with DAG(
    default_args=default_args,
    dag_id='1.10-PROSPEC_GFS', 
    start_date=datetime(2025, 1, 23), 
    schedule_interval=None, 
    catchup=False,
    tags=['Prospec'],
) as dag:
    run_script_task = PythonOperator(
        task_id='run_python_gfs',
        python_callable=run_python_gfs,

    )
    run_prospec_on_host = SSHOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='run',
        ssh_conn_id='ssh_master',  
        command="{{ ti.xcom_pull(task_ids='run_python_gfs', key='command')}}",
        conn_timeout=36000,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
    )

    run_script_task >> run_prospec_on_host


# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Definindo a DAG para '2.0-PROSPEC_ATUALIZAÇOÊS'
def run_python_update_with_dynamic_params(**kwargs):
    # Recuperando todos os parâmetros passados para a DAG
    print(kwargs.get('params'))
    params = kwargs.get('params', {})
    print(params)
    # Iniciando a construção do comando para o script
    command= CMD_BASE + "prevs UPDATE rodada Preliminar"
    
    # Adicionando parâmetros ao comando dinamicamente, se existirem
    for key, value in params.items():
        if value is not None:  # Verifica se o valor não é None
            # Adiciona o parâmetro e o valor ao comando
            command += f" {key} '{value}'"
    print(command)
    kwargs['ti'].xcom_push(key='command', value=command)

    

# Definindo a DAG para '1.00-ENVIAR-EMAIL-ESTUDOS' (adicionada)
with DAG(
    default_args=default_args,
    dag_id='1.11-PROSPEC_ATUALIZACAO', 
    start_date=datetime(2025, 1, 23), 
    schedule_interval=None, 
    catchup=False,
    tags=['Prospec'],
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

# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Definindo a DAG para '2.1-PROSPEC_CONSISTIDO'
with DAG(
    default_args=default_args,
    dag_id='1.12-PROSPEC_CONSISTIDO', 
    start_date=datetime(2024, 4, 28), 
    schedule_interval='00 8 * * 1', 
    catchup=False,
    tags=['Prospec'],
) as dag:
    run_prospec_on_host = SSHOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='run_prospec_consistido',
        ssh_conn_id='ssh_master', 
        command= CMD_BASE +"prevs CONSISTIDO rodada Preliminar", 
        conn_timeout=28800,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
    )
# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Definindo a DAG para 1.08-PROSPEC_GRUPOS-ONS
# Função que verifica o estado da DAG
def check_dag_state(dag_id='1.08-PROSPEC_GRUPOS-ONS'):
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
    dag_id='1.08-PROSPEC_GRUPOS-ONS', 
    start_date=datetime(2024, 4, 28), 
    schedule_interval=None, 
    catchup=False,
    max_active_runs=1,
    tags=['Prospec'],
) as dag:

    # Verifica o estado da DAG antes de executar qualquer tarefa
    check_dag_state_task = BranchPythonOperator(
        task_id='check_dag_state',
        python_callable=check_dag_state,
        op_args=['1.08-PROSPEC_GRUPOS-ONS'],
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
        command= CMD_BASE + "prevs ONS-GRUPOS rodada Preliminar",
        conn_timeout=None,
        cmd_timeout=None,
        get_pty=True,
        execution_timeout=timedelta(hours=20),
    )

    # Definindo a ordem das tarefas
    check_dag_state_task >> [run_decomp_ons_grupos, skip_task]




# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
with DAG(
    dag_id = '1.13-PROSPEC_PCONJUNTO_PREL_PRECIPITACAO', 
    start_date=datetime(2024, 7, 30), 
    schedule_interval='00 07 * * 1-5', 
    catchup=False,
    tags=['Prospec'],
    ) as dag:
    
    run_pconjunto_prel_precipitacao = SSHOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='run_pconjunto_prel_precipitacao',
        ssh_conn_id='ssh_master',  # Ensure this matches the connection ID set in the Airflow UI
        command= CMD_BASE + "prevs P.APR rodada Preliminar",
        conn_timeout = None,
        cmd_timeout = None,
        get_pty=True,
        execution_timeout=timedelta(hours=20),
    )

# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Definindo a DAG para '1.15-PROSPEC_RODAR_SENSIBILIDADE'
# Função que executa o script com parâmetros dinâmicos
def run_sensibilidades_params(**kwargs):
    # Recuperando todos os parâmetros passados para a DAG
    #print(f"{kwargs.get('params')}")
    params = kwargs.get('params', {})
    #print(params)
    #params = f"{str(params)}"
    #print("parametros recebidos: ", type(params))
    
    # Iniciando a construção do comando para o script
    
    command = CMD_BASE_SENS + ' "'  + str(params) + '"'
    #command += f" str(kwargs.get('params'))'"
    print(command)
    kwargs['ti'].xcom_push(key='command', value=command)

with DAG(
    default_args=default_args,
    dag_id='1.14-PROSPEC_RODAR_SENSIBILIDADE', 
    start_date=datetime(2025, 1, 23), 
    schedule_interval=None, 
    catchup=False,
    tags=['Prospec'],
) as dag:
    run_script_task = PythonOperator(
        task_id='run_sensibilidades_params',
        python_callable=run_sensibilidades_params,

    )
    run_prospec_on_host = SSHOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='run',
        ssh_conn_id='ssh_master',  
        command="{{ ti.xcom_pull(task_ids='run_sensibilidades_params', key='command')}}",
        conn_timeout=36000,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
    )

    run_script_task >> run_prospec_on_host 
    
    
# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
with DAG(
    dag_id = '1.16-DECOMP_ONS-TO-CCEE', 
    start_date=datetime(2024, 4, 28), 
    schedule_interval=None, 
    catchup=False,
    tags=['Prospec'],
    ) as dag:
    
    run_decomp_on_host = SSHOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='run_decomp',
        ssh_conn_id='ssh_master',  # Ensure this matches the connection ID set in the Airflow UI
        command=CMD_BASE_DC,
        conn_timeout = None,
        cmd_timeout = None,
        get_pty=True,
    )
    

# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

with DAG(
    dag_id = '1.17-NEWAVE_ONS-TO-CCEE', 
    start_date=datetime(2024, 4, 28), 
    schedule_interval=None, 
    catchup=False,
    tags=['Prospec'],
    ) as dag:
    
    run_decomp_on_host = SSHOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='run_newave',
        ssh_conn_id='ssh_master',  
        command=CMD_BASE_DC,
        conn_timeout = None,
        cmd_timeout = None,
        get_pty=True,
    )
