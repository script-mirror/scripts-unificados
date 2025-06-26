from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta


default_args = {
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def print_name(name):
    print(f"Hello, {name}!")

def print_data(dtRef):
    dtRef = datetime.strptime(dtRef,'%Y-%m-%d')
    print(f"Hoje é {dtRef.strftime('%d/%m/%Y')}")
    
with DAG('dag_testes',
         default_args=default_args,
         tags=["tag1","tag2"]
        #  schedule_interval=timedelta(days=1)
    ) as dag:
    
    start = DummyOperator(
        task_id = 'start',
        # provide_context=True,
    )

    task_parelela1 = PythonOperator(
        task_id='task_parelela1',
        python_callable=print_name,
        provide_context=True,
        op_kwargs={
            'name': '{{ dag_run.conf.get("name", "pessoa") }}',
            }
    )
    
    task_parelela2 = PythonOperator(
        task_id='task_parelela2',
        python_callable=print_data,
        provide_context=True,
        op_kwargs={
            'dtRef': '{{ dag_run.conf.get("dtRef", ds ) }}'
            }
    )
    
    end = DummyOperator(
        task_id = 'end',
    )
    
start >> [task_parelela1, task_parelela2] >> end 
