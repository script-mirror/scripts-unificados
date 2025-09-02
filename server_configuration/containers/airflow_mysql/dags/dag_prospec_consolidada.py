import os
import sys
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import DagBag, DagRun
from airflow.exceptions import AirflowSkipException
from airflow.utils.db import create_session
import pendulum
from middle.utils import Constants
consts = Constants()

# Configure logging for debugging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Comandos base
CMD_BASE = f"{consts.ATIVAR_ENV} python {consts.PATH_PROJETOS}/estudos-middle/estudos_prospec/main_roda_estudos.py "
CMD_BASE_SENS = f"{consts.ATIVAR_ENV} python {consts.PATH_PROJETOS}/estudos-middle/estudos_prospec/gerar_sensibilidade.py "
CMD_BASE_NW = f"{consts.ATIVAR_ENV} python {consts.PATH_PROJETOS}/estudos-middle/estudos_prospec/run_nw_ons_to_ccee.py "
CMD_BASE_DC = f"{consts.ATIVAR_ENV} python {consts.PATH_PROJETOS}/estudos-middle/estudos_prospec/run_dc_ons_to_ccee.py "
CMD_UPDATE = f"{consts.ATIVAR_ENV} python {consts.PATH_PROJETOS}/estudos-middle/update_estudos/update_prospec.py "

# Argumentos padrão
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=8),
    'start_date': datetime(2024, 4, 28, tzinfo=pendulum.timezone('America/Sao_Paulo')),
}

# Funções auxiliares
def check_if_dag_is_running(**kwargs):
    dag_id = kwargs['dag'].dag_id
    execution_date = kwargs['execution_date']
    logger.info(f"Checking if DAG {dag_id} is running for execution_date {execution_date}")
    with create_session() as session:
        active_runs = session.query(DagRun).filter(
            DagRun.dag_id == dag_id,
            DagRun.state == 'running',
            DagRun.execution_date != execution_date
        ).count()
        if active_runs:
            logger.warning(f"DAG {dag_id} has {active_runs} active runs")
            raise AirflowSkipException(f"DAG {dag_id} already running. Skipping execution.")
    logger.info(f"No active runs for {dag_id}")

def run_python_script_with_dynamic_params(**kwargs):
    params = kwargs['dag_run'].conf.get('params', {}) if kwargs.get('dag_run') else {}
    logger.info(f"Building command with params: {params}")
    conteudo = ' '.join(f'"{k}" \'{v}\'' if k == "list_email" else f'"{k}" "{v}"' for k, v in params.items())
    command = CMD_BASE + conteudo
    logger.info(f"Generated command: {command}")
    kwargs['ti'].xcom_push(key='command', value=command)

def run_python_gfs(**kwargs):
    params = kwargs['dag_run'].conf.get('params', {}) if kwargs.get('dag_run') else {}
    logger.info(f"Building GFS command with params: {params}")
    command = CMD_BASE + "prevs PREVS_PLUVIA_GFS rvs 8 mapas GFS"
    for key, value in params.items():
        if value is not None:
            command += f" {key} '{value}'"
    logger.info(f"Generated GFS command: {command}")
    kwargs['ti'].xcom_push(key='command', value=command)

def run_sensibilidades_params(**kwargs):
    params = kwargs['dag_run'].conf.get('params', {}) if kwargs.get('dag_run') else {}
    logger.info(f"Building SENS command with params: {params}")
    command = f"{CMD_BASE_SENS} \"{str(params)}\""
    logger.info(f"Generated SENS command: {command}")
    kwargs['ti'].xcom_push(key='command', value=command)

def run_prospec_update(**kwargs):
    params = kwargs['dag_run'].conf.get('params', {}) if kwargs.get('dag_run') else {}
    produto = params.get('produto', 'DEFAULT')
    logger.info(f"Building UPDATE command with params: {params}")
    conteudo = ' '.join(f'"{k}" \'{v}\'' if k == "list_email" else f'"{k}" "{v}"' for k, v in params.items())
    command = CMD_UPDATE + conteudo
    logger.info(f"Generated UPDATE command: {command}")
    kwargs['ti'].xcom_push(key='command', value=command)
    kwargs['ti'].xcom_push(key='produto', value=f'REVISAO-{produto}')

def check_dag_state(**kwargs):
    dag_id = kwargs.get('dag_id', 'PROSPEC_MASTER')
    logger.info(f"Checking if DAG {dag_id} is paused")
    dagbag = DagBag()
    dag = dagbag.get_dag(dag_id)
    if dag is None:
        logger.error(f"DAG {dag_id} not found in DagBag")
        raise ValueError(f"DAG {dag_id} not found")
    if dag.is_paused:
        logger.info(f"DAG {dag_id} is paused")
        return 'skip_grupos_ons'
    logger.info(f"DAG {dag_id} is not paused")
    return 'run_1_08_prospec_grupos_ons'

def skip_task():
    logger.info("Task skipped due to DAG being paused")

def check_schedule(**kwargs):
    execution_date = kwargs['execution_date'].astimezone(pendulum.timezone('America/Sao_Paulo'))
    dag_run = kwargs.get('dag_run')
    tasks_to_run = []
    logger.info(f"Checking schedule for execution_date: {execution_date}")

    # Mapeamento dos schedule_interval originais
    schedules = {
        '1.00-ENVIAR-EMAIL-ESTUDOS': None,
        '1.01-PROSPEC_PCONJUNTO_DEFINITIVO': None,
        '1.02-PROSPEC_PCONJUNTO_PREL': '02 07 * * *',
        '1.03-PROSPEC_1RV': '21 06 * * *',
        '1.04-PROSPEC_EC_EXT': '00 19 * * *',
        '1.05-PROSPEC_CENARIO_10': '42 6 * * *',
        '1.06-PROSPEC_CENARIO_11': '33 7 * * 1-5',
        '1.07-PROSPEC_CHUVA_0': '00 8 * * 1',
        '1.08-PROSPEC_GRUPOS-ONS': None,
        '1.10-PROSPEC_GFS': None,
        '1.11-PROSPEC_ATUALIZACAO': None,
        '1.12-PROSPEC_CONSISTIDO': '00 8 * * 1',
        '1.13-PROSPEC_PCONJUNTO_PREL_PRECIPITACAO': '00 07 * * 1-5',
        '1.14-PROSPEC_RODAR_SENSIBILIDADE': None,
        '1.16-DECOMP_ONS-TO-CCEE': None,
        '1.17-NEWAVE_ONS-TO-CCEE': None,
        '1.18-PROSPEC_UPDATE': None,
    }

    # Verifica se é uma execução manual
    if dag_run and dag_run.conf:
        logger.info(f"Manual execution detected with conf: {dag_run.conf}")
        requested_tasks = dag_run.conf.get('tasks_to_run', [])
        if requested_tasks:
            tasks_to_run = []
            for task in requested_tasks:
                task_id = f'run_{task.lower().replace(".", "_").replace("-", "_")}'
                if task_id.startswith('run_') and task in schedules:
                    tasks_to_run.append(task_id)
                else:
                    logger.warning(f"Invalid task {task} in tasks_to_run")
            logger.info(f"Manually requested tasks: {tasks_to_run}")
        else:
            tasks_to_run = [f'run_{dag_name.lower().replace(".", "_").replace("-", "_")}' for dag_name in schedules.keys()]
            logger.info("No specific tasks requested, running all tasks")
    else:
        logger.info("Automatic execution, checking cron schedules")
        for dag_name, schedule in schedules.items():
            if schedule is None:
                continue
            try:
                cron = pendulum.CronExpression(schedule)
                if cron.is_due(execution_date):
                    tasks_to_run.append(f'run_{dag_name.lower().replace(".", "_").replace("-", "_")}')
                    logger.info(f"Task {dag_name} scheduled for execution")
            except Exception as e:
                logger.error(f"Error parsing cron for {dag_name}: {e}")

    if not tasks_to_run:
        tasks_to_run.append('skip_all')
        logger.info("No tasks to run, proceeding to skip_all")
    return tasks_to_run

# Definindo a DAG consolidada
with DAG(
    dag_id='PROSPEC_MASTER',
    default_args=default_args,
    schedule_interval='* * * * *',
    catchup=False,
    max_active_runs=1,
    tags=['Prospec'],
) as dag:

    # Tarefa de verificação de execução concorrente
    check_running = PythonOperator(
        task_id='check_if_dag_is_running',
        python_callable=check_if_dag_is_running,
        provide_context=True,
    )

    # Tarefa de verificação de schedule
    check_schedule_task = BranchPythonOperator(
        task_id='check_schedule',
        python_callable=check_schedule,
        provide_context=True,
    )

    # Tarefa dummy para pular todas as tasks
    skip_all = DummyOperator(
        task_id='skip_all',
        trigger_rule='all_done',
    )

    # Tarefa para 1.00-ENVIAR-EMAIL-ESTUDOS
    email_estudos = PythonOperator(
        task_id='run_send_e-mail',
        python_callable=run_python_script_with_dynamic_params,
        provide_context=True,
    )
    email_estudos_ssh = SSHOperator(
        task_id='run_send_e-mail_ssh',
        ssh_conn_id='ssh_master',
        command="{{ ti.xcom_pull(task_ids=''run_send_e-mail'', key='command') }}",
        conn_timeout=36000,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule="none_failed_min_one_success",
        do_xcom_push=False,
    )

    # Tarefa para 1.01-PROSPEC_PCONJUNTO_DEFINITIVO
    pconjunto_definitivo = SSHOperator(
        task_id='run_pconjunto_definitivo',
        ssh_conn_id='ssh_master',
        command=CMD_BASE + "prevs P.CONJ rodada Definitiva",
        conn_timeout=36000,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule="none_failed_min_one_success",
        do_xcom_push=False,
    )

    # Tarefa para 1.02-PROSPEC_PCONJUNTO_PREL
    pconjunto_prel = SSHOperator(
        task_id='run_pconjunto_prel',
        ssh_conn_id='ssh_master',
        command=CMD_BASE + "prevs P.CONJ rodada Preliminar",
        conn_timeout=28800,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule="none_failed_min_one_success",
        do_xcom_push=False,
    )

    # Tarefa para 1.03-PROSPEC_1RV
    prospec_1rv = SSHOperator(
        task_id='run_1rv',
        ssh_conn_id='ssh_master',
        command=CMD_BASE + "prevs NEXT-RV rodada Preliminar",
        conn_timeout=28800,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule="none_failed_min_one_success",
        do_xcom_push=False,
    )

    # Tarefa para 1.04-PROSPEC_EC_EXT
    prospec_ec_ext = SSHOperator(
        task_id='run_ec_ext',
        ssh_conn_id='ssh_master',
        command=CMD_BASE + "prevs EC-EXT rodada Definitiva",
        conn_timeout=28800,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule="none_failed_min_one_success",
        do_xcom_push=False,
    )

    # Tarefa para 1.05-PROSPEC_CENARIO_10
    prospec_cenario_10 = SSHOperator(
        task_id='run_cenario_10',
        ssh_conn_id='ssh_master',
        command=CMD_BASE + "prevs CENARIOS rodada Preliminar",
        conn_timeout=28800,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule="none_failed_min_one_success",
        do_xcom_push=False,
    )

    # Tarefa para 1.06-PROSPEC_CENARIO_11
    prospec_cenario_11 = SSHOperator(
        task_id='run_cenario_11',
        ssh_conn_id='ssh_master',
        command=CMD_BASE + "prevs CENARIOS rodada Preliminar, cenario 11",
        conn_timeout=28800,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule="none_failed_min_one_success",
        do_xcom_push=False,
    )

    # Tarefa para 1.07-PROSPEC_CHUVA_0
    prospec_chuva_0 = SSHOperator(
        task_id='run_chuva_0',
        ssh_conn_id='ssh_master',
        command=CMD_BASE + "prevs P.ZERO rodada Preliminar",
        conn_timeout=28800,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule="none_failed_min_one_success",
        do_xcom_push=False,
    )

    # Tarefa para 1.08-PROSPEC_GRUPOS-ONS
    check_dag_state_task = BranchPythonOperator(
        task_id='check_dag_state_grupos_ons',
        python_callable=check_dag_state,
        op_args=['PROSPEC_MASTER'],
        provide_context=True,
    )
    skip_grupos_ons = PythonOperator(
        task_id='skip_grupos_ons',
        python_callable=skip_task,
        provide_context=True,
    )
    run_decomp_ons_grupos = SSHOperator(
        task_id='run_grupos_ons',
        ssh_conn_id='ssh_master',
        command=CMD_BASE + "prevs ONS-GRUPOS rodada Preliminar",
        conn_timeout=None,
        cmd_timeout=None,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule="none_failed_min_one_success",
        do_xcom_push=False,
    )

    # Tarefa para 1.10-PROSPEC_GFS
    prospec_gfs = PythonOperator(
        task_id='run_gfs',
        python_callable=run_python_gfs,
        provide_context=True,
    )
    prospec_gfs_ssh = SSHOperator(
        task_id='run_gfs_ssh',
        ssh_conn_id='ssh_master',
        command="{{ ti.xcom_pull(task_ids='run_gfs', key='command') }}",
        conn_timeout=36000,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule="none_failed_min_one_success",
        do_xcom_push=False,
    )

    # Tarefa para 1.11-PROSPEC_ATUALIZACAO
    prospec_atualizacao = PythonOperator(
        task_id='run_atualizacao',
        python_callable=run_python_script_with_dynamic_params,
        provide_context=True,
    )
    prospec_atualizacao_ssh = SSHOperator(
        task_id='run_atualizacao_ssh',
        ssh_conn_id='ssh_master',
        command="{{ ti.xcom_pull(task_ids='run_atualizacao', key='command') }}",
        conn_timeout=36000,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule="none_failed_min_one_success",
        do_xcom_push=False,
    )

    # Tarefa para 1.12-PROSPEC_CONSISTIDO
    prospec_consistido = SSHOperator(
        task_id='run_consistido',
        ssh_conn_id='ssh_master',
        command=CMD_BASE + "prevs CONSISTIDO rodada Preliminar",
        conn_timeout=28800,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule="none_failed_min_one_success",
        do_xcom_push=False,
    )

    # Tarefa para 1.13-PROSPEC_PCONJUNTO_PREL_PRECIPITACAO
    pconjunto_prel_precipitacao = SSHOperator(
        task_id='run_pconjunto_prel_precipitacao',
        ssh_conn_id='ssh_master',
        command=CMD_BASE + "prevs P.APR rodada Preliminar",
        conn_timeout=None,
        cmd_timeout=None,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule="none_failed_min_one_success",
        do_xcom_push=False,
    )

    # Tarefa para 1.14-PROSPEC_RODAR_SENSIBILIDADE
    rodar_sensibilidade = PythonOperator(
        task_id='run_sensibilidade',
        python_callable=run_sensibilidades_params,
        provide_context=True,
    )
    rodar_sensibilidade_ssh = SSHOperator(
        task_id='run_sensibilidade_ssh',
        ssh_conn_id='ssh_master',
        command="{{ ti.xcom_pull(task_ids='run_sensibilidade', key='command') }}",
        conn_timeout=36000,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule="none_failed_min_one_success",
        do_xcom_push=False,
    )

    # Tarefa para 1.16-DECOMP_ONS-TO-CCEE
    decomp_ons_to_ccee = SSHOperator(
        task_id='run_decomp_ons_to_ccee',
        ssh_conn_id='ssh_master',
        command=CMD_BASE_DC,
        conn_timeout=None,
        cmd_timeout=None,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule="none_failed_min_one_success",
        do_xcom_push=False,
    )

    # Tarefa para 1.17-NEWAVE_ONS-TO-CCEE
    newave_ons_to_ccee = SSHOperator(
        task_id='run_newave_ons_to_ccee',
        ssh_conn_id='ssh_master',
        command=CMD_BASE_NW,
        conn_timeout=None,
        cmd_timeout=None,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule="none_failed_min_one_success",
        do_xcom_push=False,
    )

    # Tarefa para 1.18-PROSPEC_UPDATE
    prospec_update = PythonOperator(
        task_id='run_decks_update',
        python_callable=run_prospec_update,
        provide_context=True,
    )
    prospec_update_ssh = SSHOperator(
        task_id='run_decks_update_ssh',
        ssh_conn_id='ssh_master',
        command="{{ ti.xcom_pull(task_ids='run_decks_update', key='command') }}",
        conn_timeout=36000,
        cmd_timeout=28800,
        execution_timeout=timedelta(hours=20),
        get_pty=True,
        trigger_rule="none_failed_min_one_success",
        do_xcom_push=False,
    )

    # Definindo dependências
    check_running >> check_schedule_task
    check_schedule_task >> skip_all
    check_schedule_task >> [
        email_estudos,
        pconjunto_definitivo,
        pconjunto_prel,
        prospec_1rv,
        prospec_ec_ext,
        prospec_cenario_10,
        prospec_cenario_11,
        prospec_chuva_0,
        check_dag_state_task,
        prospec_gfs,
        prospec_atualizacao,
        prospec_consistido,
        pconjunto_prel_precipitacao,
        rodar_sensibilidade,
        decomp_ons_to_ccee,
        newave_ons_to_ccee,
        prospec_update,
    ]

    # Dependências específicas
    email_estudos >> email_estudos_ssh
    prospec_gfs >> prospec_gfs_ssh
    prospec_atualizacao >> prospec_atualizacao_ssh
    rodar_sensibilidade >> rodar_sensibilidade_ssh
    prospec_update >> prospec_update_ssh
    check_dag_state_task >> [run_decomp_ons_grupos, skip_grupos_ons]
    newave_ons_to_ccee >> prospec_atualizacao
    prospec_update_ssh >> prospec_atualizacao

# Log DAG creation for debugging
logger.info("DAG PROSPEC_MASTER created successfully")