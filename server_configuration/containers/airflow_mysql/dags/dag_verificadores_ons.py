import datetime
from airflow.models.dag import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator



with DAG(
    dag_id='SITUACAO_RESERVATORIOS',
    tags=["Verificador","ONS","WhatsApp"],
    start_date= datetime.datetime(2024, 4, 28),
    catchup=False,
    schedule="0 9 * * *"
) as dag:
    
    inicio = DummyOperator(
        task_id='inicio',
        trigger_rule="none_failed_min_one_success",

    )
    RotinaReservatorios = SSHOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='RotinaReservatorios',
        ssh_conn_id='ssh_master',  
        command='''. /WX2TB/pythonVersions/myVenv38/bin/activate;cd /WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/gerarProdutos; python gerarProdutos2.py --produto SITUACAO_RESERVATORIOS''',
        conn_timeout = None,
        cmd_timeout = None,
        get_pty=True,
    )

    fim = DummyOperator(
        task_id='fim',
        trigger_rule="none_failed_min_one_success",
    )

    inicio >> RotinaReservatorios >> fim

# =========================================DOWNALOD-EC-CLUST-ONS=====================================

def cmd_command(**kwargs):

    params = kwargs.get('dag_run').conf
    dt_rodada = params.get('dt_rodada')

    # na ons o arquivo que sai no dia é na verdade o do dia anterior
    if not dt_rodada:
        dt_rodada = datetime.datetime.now().replace(hour=0,minute=0,second=0) - datetime.timedelta(days=1)


    cmd = f'''. /WX2TB/pythonVersions/myVenv38/bin/activate;cd /WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/verificadores/ons; python verificador_ecmwf.py data_rodada {dt_rodada}'''
        
    print(f"Comando: {cmd}")
    kwargs['ti'].xcom_push(key='command', value=cmd)
    
with DAG(
    dag_id='DOWNALOD-EC-CLUST-ONS',
    tags=["Verificador","ONS","SMAP","Chuva Prevista"],
    start_date= datetime.datetime(2024, 4, 28),
    catchup=False,
    schedule="30 6 * * *"
) as dag:

    # começo estrutura para rodar a sequencia das tarefas
    inicio = PythonOperator(
        task_id='inicio',
        python_callable=cmd_command,
    )

    downaload_ec_clust = SSHOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='downaload_ec_clust',
        ssh_conn_id='ssh_master',  
        command="{{ ti.xcom_pull(task_ids='inicio', key='command')}}",
        conn_timeout = None,
        cmd_timeout = None,
        get_pty=True,
    )
    fim = DummyOperator(
        task_id='fim',
        trigger_rule="none_failed_min_one_success",
    )

    inicio >> downaload_ec_clust >> fim


# =========================================FSARH_RESTRICOES=====================================


with DAG(
    dag_id='FSARH_RESTRICOES',
    tags=["Verificador","ONS"],
    start_date= datetime.datetime(2024, 4, 28),
    catchup=False,
    schedule="30 8-20 * * *"
) as dag:

    # começo estrutura para rodar a sequencia das tarefas
    inicio = DummyOperator(
        task_id='inicio',
    )

    RotinaRestricoes = SSHOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='RotinaRestricoes',
        ssh_conn_id='ssh_master',  
        command=". /WX2TB/pythonVersions/myVenv38/bin/activate;cd /WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/verificadores/ons; python verificador_restricoes.py",
        conn_timeout = None,
        cmd_timeout = None,
        get_pty=True,
    )
    fim = DummyOperator(
        task_id='fim',
        trigger_rule="none_failed_min_one_success",
    )

    inicio >> RotinaRestricoes >> fim

    

# =========================================FSARH_RESTRICOES=====================================

with DAG(
    dag_id='CARGA_HORARIA',
    tags=["Verificador","ONS"],
    start_date= datetime.datetime(2024, 4, 28),
    catchup=False,
    schedule="54 * * * *"
) as dag:

    # começo estrutura para rodar a sequencia das tarefas
    inicio = DummyOperator(
        task_id='inicio',
    )

    RotinaCargaHoraria = SSHOperator(
        trigger_rule="none_failed_min_one_success",
        task_id='RotinaCargaHoraria',
        ssh_conn_id='ssh_master',  
        command=". /WX2TB/pythonVersions/myVenv38/bin/activate;cd /WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/verificadores/ons; python verificador_carga_horaria.py importar_carga_geracao",
        conn_timeout = None,
        cmd_timeout = None,
        get_pty=True,
    )
    fim = DummyOperator(
        task_id='fim',
        trigger_rule="none_failed_min_one_success",
    )

    inicio >> RotinaCargaHoraria >> fim