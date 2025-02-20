from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from airflow.operators.dummy import DummyOperator

import sys
sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.apps.prospec.libs.ccee import service as ccee_api


default_args = {
    'execution_timeout': timedelta(hours=8)
}

def is_target_day(**kwargs):

    execution_date_param = kwargs.get('dag_run').conf.get('execution_date')
    titles_to_search = kwargs.get('dag_run').conf.get('titles_ccee',[])
    ids_to_modify = kwargs.get('dag_run').conf.get('ids_to_modify',[])

    execution_date = (
        datetime.now() 
        if not execution_date_param 
        else datetime.strptime(execution_date_param,'%Y-%m-%d')
        )

    if not titles_to_search:
        
        for title_name in ccee_api.MAPPING.keys():
            print(title_name)
            
            dt_atualizacao = ccee_api.get_date_atualizacao(title_name)
            if dt_atualizacao:
                if dt_atualizacao.date() == execution_date.date():
                    titles_to_search.append(title_name)

    if titles_to_search:
        kwargs.get('dag_run').conf.update(
            {
                'titles_to_search': titles_to_search,
                'task_to_execute': 'revisao_cvu',
                'ids_to_modify': ids_to_modify
            })

        return "trigger_prospec_updater"
    
    return "end_task"
    

with DAG(
    'API_CCEE',
    default_args=default_args,
    description='API do cvu no 4 dia util e no dia 17 ou proximo dia util',
    schedule_interval = '0 14-16,18-20 * * *', 
    tags=["PROSPEC","CCEE","CVU"],
    start_date=datetime(2024, 4, 28),
    catchup=False,
    render_template_as_native_obj=True, 

) as dag:

    check_target_day = BranchPythonOperator(
        task_id='check_target_day',
        python_callable=is_target_day,
        provide_context=True,
    )

    trigger_updater_estudo = TriggerDagRunOperator(
        task_id="trigger_prospec_updater",
        trigger_dag_id='PROSPEC_UPDATER',
        conf={'external_params': "{{dag_run.conf}}"},  
        wait_for_completion=False,  
        trigger_rule="none_failed_min_one_success",
    )

    end_task = DummyOperator(
        task_id='end_task',
    )

    check_target_day >> trigger_updater_estudo >> end_task
    check_target_day >>  end_task
