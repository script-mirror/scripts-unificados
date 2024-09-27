from datetime import datetime, timedelta
from airflow import DAG
import sys
from airflow.operators.python import PythonOperator

sys.path.insert(1,"/WX2TB/Documentos/fontes")
from PMO.scripts_unificados.apps.dadosAbertosOns.libs import coff, geracao_usina, carga, cmo

def get_mes_anterior():
    data = datetime.now()
    if data.month == 12:
        return data.replace(day=1, month=1, year=datetime.now().year-1)
    return data.replace(day=1, month=datetime.now().month-1)

with DAG(
    dag_id = 'ONS_DADOS_ABERTOS', 
    start_date=datetime(2024, 4, 28), 
    schedule_interval= "0 17 * * * ", 
    catchup=False
    ) as dag:
    
    coff_eolica_mes_atual = PythonOperator(
        task_id='coff_eolica_mes_atual',
        trigger_rule="all_done",
        python_callable=coff.inserir,
        op_kwargs={'tabela' : 'tb_restricoes_coff_eolica'}
    )
    
    coff_solar_mes_atual = PythonOperator(
        task_id='coff_solar_mes_atual',
        trigger_rule="all_done",
        python_callable=coff.inserir,
        op_kwargs={'tabela' : 'tb_restricoes_coff_solar'}
    )

    coff_eolica_mes_anterior = PythonOperator(
        task_id='coff_eolica_mes_anterior',
        trigger_rule="all_done",
        python_callable=coff.inserir,
        op_kwargs={'tabela' : 'tb_restricoes_coff_eolica', 'data':get_mes_anterior()}
    )
    
    coff_solar_mes_anterior = PythonOperator(
        task_id='coff_solar_mes_anterior',
        trigger_rule="all_done",
        python_callable=coff.inserir,
        op_kwargs={'tabela' : 'tb_restricoes_coff_solar', 'data':get_mes_anterior()}
    )
    
    geracao_mes_atual = PythonOperator(
        task_id='geracao_mes_atual',
        trigger_rule="all_done",
        python_callable=geracao_usina.inserir
    )
    
    carga_mes_atual = PythonOperator(
        task_id='carga_mes_atual',
        trigger_rule="all_done",
        python_callable=carga.inserir
    )

    geracao_mes_anterior = PythonOperator(
        task_id='geracao_mes_anterior',
        trigger_rule="all_done",
        python_callable=geracao_usina.inserir,
        op_kwargs={'data':get_mes_anterior()}
    )
    
    carga_mes_anterior = PythonOperator(
        task_id='carga_mes_anterior',
        trigger_rule="all_done",
        python_callable=carga.inserir,
        op_kwargs={'data':get_mes_anterior()}
    )
    
    cmo_semana_anterior = PythonOperator(
        task_id='cmo_semanal_ano_atual',
        trigger_rule="all_done",
        python_callable=cmo.inserir_cmo_semanal,
        op_kwargs={'tabela':'tb_cmo_semanal'}
    )
    
    cmo_hora_anterior = PythonOperator(
        task_id='cmo_horario_ano_atual',
        trigger_rule="all_done",
        python_callable=cmo.inserir_cmo_horario,
        op_kwargs={'tabela':'tb_cmo_horario'}
    )
    
    
