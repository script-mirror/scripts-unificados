import json
import datetime
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2024, 7, 10),
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=5),

}


def update_conf(**kwargs):
    cenario_output = kwargs['ti'].xcom_pull(task_ids='run_smap').decode('utf-8')
    print(type(cenario_output))
    print((cenario_output))
    lines = cenario_output.strip().split('\n')
    last_line = lines[-1].strip()
    print(last_line)
    cenario = json.loads(last_line)
    kwargs.get('dag_run').conf['cenario'] = cenario


with DAG(
    dag_id='PREV_SMAP',
    default_args=default_args,
    description='DAG que instancia SMAP e chama métodos em tasks separadas',
    schedule_interval=None,
    max_active_runs=1,
    concurrency=1,
    render_template_as_native_obj=True, 
    catchup=False
) as dag:

    t_run_smap = SSHOperator(
        task_id='run_smap',
        ssh_conn_id='ssh_master',
        command='cd /WX2TB/Documentos/fontes/PMO/'
                'raizen-power-trading-smap && env/bin/python '
                'main.py {{ dag_run.conf.get("id_chuva") }}',
        do_xcom_push=True,
        conn_timeout=None,
        cmd_timeout=None,
        get_pty=True,
    )

    t_update_conf = PythonOperator(
        task_id='update_conf',
        python_callable=update_conf
    )

    t_run_previvaz = TriggerDagRunOperator(
        task_id="trigger_previvaz",
        trigger_dag_id='PREV_PREVIVAZ',
        conf={'cenario': "{{ dag_run.conf.get('cenario')}}"},
        wait_for_completion=False,  
        trigger_rule="none_failed_min_one_success",

    )

    atualizar_cache = SSHOperator(
        task_id='atualizar_cache',
        ssh_conn_id='ssh_master',
        command=". /WX2TB/pythonVersions/myVenv38/bin/activate;"
                "cd /WX2TB/Documentos/fontes/PMO/scripts_unificados/"
                "apps/web_modelos/server/caches;python rz_cache.py "
                "import_ena_visualization_api dt_rodada "
                "{{ dag_run.conf.get('cenario')['dt_rodada'] }} "
                "id_nova_rodada {{ dag_run.conf.get('cenario')['id'] }} "
                "id_dataviz_chuva {{ dag_run.conf.get('id_dataviz_chuva') }}",
        conn_timeout=None,
        cmd_timeout=None,
        get_pty=True,
        trigger_rule="all_done",
    )

    t_run_smap >> atualizar_cache >> t_run_previvaz
