from airflow import DAG
from airflow.models import DagRun, TaskInstance
from airflow.utils.state import State
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from time import sleep as s
from datetime import datetime

def check_running_tasks_in_other_dag_runs(**kwargs):
    # Obtém a DAG atual
    dag = kwargs['dag'] 
    task_instance = kwargs['task_instance']
    print("---------------------------------------------------")
    print(kwargs)
    print("---------------------------------------------------")
    
    # Pega todas as execuções (DagRuns) da DAG
    all_dag_runs = DagRun.find(dag_id=dag.dag_id)
    
    running_tasks = []
    
    # Percorre todas as DAG runs para verificar instâncias de tasks
    print(all_dag_runs)
    print("---------------------------------------------------")
    for dag_run in all_dag_runs:
        task_instances = dag_run.get_task_instances()
        # Filtra tasks que estão em execução
        running_tasks += [
            ti for ti in task_instances 
            if ti.state == State.RUNNING and ti.task_id != task_instance.task_id
        ]
    
    if running_tasks:
        print(f"Tasks em execução em outras DAG runs: {[ti.task_id for ti in running_tasks]}")
        return False  # Há outras tasks em execução
    return True  # Nenhuma task em execução em outras DAG runs

def shutdown_ec2_if_no_tasks(**kwargs):

    if check_running_tasks_in_other_dag_runs(**kwargs):
        print("Desligando a instância EC2.")
    else:
        print("Ainda há tasks em execução. A instância EC2 não será desligada.")
        
def tesk_teste():
    print("Iniciando operacao")
    s(5)
    print("Terminando operacao")

with DAG(
    dag_id = 'TESTE', 
    start_date=datetime(2024, 4, 28), 
    schedule_interval= None, 
    catchup=False
    ) as dag:
    
    task_teste = PythonOperator(
        task_id='task_teste',
        trigger_rule="all_done",
        python_callable=tesk_teste
    )
    desligar_ec2 = PythonOperator(
        task_id="desligar_ec2",
        trigger_rule="all_done",
        python_callable=shutdown_ec2_if_no_tasks,
        provide_context=True 
    )
    fim = DummyOperator(
        task_id='fim',
        trigger_rule="none_failed_min_one_success",
    )
    
    task_teste >> desligar_ec2 >> fim
