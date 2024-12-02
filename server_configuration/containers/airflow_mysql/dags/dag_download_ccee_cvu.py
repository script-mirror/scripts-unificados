from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from workalendar.america import Brazil
from calendar import monthrange

import sys
sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.apps.prospec.libs import update_estudo


default_args = {
    'execution_timeout': timedelta(hours=8)
}

def is_target_day(**kwargs):
    """
    Verifica se é o 4º dia útil do mês ou o dia 17 útil (ou próximo dia útil).
    """

    cal = Brazil()

    execution_date_param = kwargs.get('dag_run').conf.get('execution_date')
    execution_date = (
        datetime.now()
        ) if not execution_date_param else datetime.strptime(execution_date_param,'%Y-%m-%d')

    execution_date = execution_date.date()
    year = execution_date.year
    month = execution_date.month
    day = execution_date.day

    business_days = [d for d in range(1, monthrange(year,month)[1]+1) if cal.is_working_day(datetime(year, month, d))]

    #4 dia util do mes
    fourth_business_day = business_days[3] if len(business_days) >= 4 else None

    #dia 17 ou proximo dia util
    seventeenth = datetime(year, month, 17)
    if not cal.is_working_day(seventeenth):
        seventeenth = next(
            d for d in range(18, 32)
            if cal.is_working_day(datetime(year, month, d))
        )

    if day == fourth_business_day or day == seventeenth:
        kwargs['ti'].xcom_push(key='mes_referencia', value=month)
        kwargs['ti'].xcom_push(key='ano_referencia', value=year)

        return "update_files"

    return "end_task"


def update_estudo_files(**kwargs):

    ti = kwargs['ti']
    mes_referencia = ti.xcom_pull(key='mes_referencia', task_ids='check_target_day')
    ano_referencia = ti.xcom_pull(key='ano_referencia', task_ids='check_target_day')

    ids_to_modify = update_estudo.get_ids_to_modify()
    print(ids_to_modify)
    ids_to_modify = [22152]

    update_estudo.update_cvu_estudo(
        ids_to_modify,
        ano_referencia,
        mes_referencia
        )

    return "end_task"


with DAG(
    'DOWNLOAD_CCEE_CVU',
    default_args=default_args,
    description='Download do cvu no acervo ccee no 4 dia util e no dia 17 ou proximo dia util',
    schedule_interval='0 9 * * *',  
    start_date=datetime(2024, 4, 28),
    catchup=False,
) as dag:

    check_target_day = BranchPythonOperator(
        task_id='check_target_day',
        python_callable=is_target_day,
        provide_context=True,
    )

    update_files = BranchPythonOperator(
        task_id='update_files',
        python_callable=update_estudo_files,
        provide_context=True,
    )

    end_task = DummyOperator(
        task_id='end_task',
    )

    check_target_day >> update_files >> end_task
    check_target_day >>  end_task
