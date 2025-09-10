# from airflow import DAG
# from airflow.providers.ssh.operators.ssh import SSHOperator
# import datetime

# TIME_OUT = 60*60*30

# ##################################################################################################

# def create_model_dag(model_name, schedule):
#     with DAG(
#         dag_id=model_name.upper(),  # o nome do DAG fica em maiúsculo
#         start_date=datetime.datetime(2024, 4, 28),
#         description=f'DAG para rodar {model_name.upper()} via SSH',
#         schedule=schedule,
#         catchup=False,
#         max_active_runs=1,
#         concurrency=1,
#         tags=['Metereologia', 'Mapas']
#     ) as dag:

#         run_shell_script = SSHOperator(
#             do_xcom_push=False,
#             task_id=f'produtos_{model_name}',
#             command=f'/projetos/produtos-meteorologia/produtos.sh {model_name} "" "" "" "" ""',
#             ssh_conn_id='ssh_master',
#             conn_timeout=TIME_OUT,
#             cmd_timeout=TIME_OUT,
#             execution_timeout=datetime.timedelta(hours=30),
#             get_pty=True,
#         )

#         return dag

# ##################################################################################################

# # Criando as DAGs dinamicamente
# globals()['GFS'] = create_model_dag('gfs', '0 2,8,14,20 * * *')
# globals()['GEFS'] = create_model_dag('gefs', '0 3,8,15,20 * * *')
# globals()['GEFS-MEMBROS'] = create_model_dag('gefs-membros', '0 3,8,15,20 * * *')
# globals()['GEFS-ESTENDIDO'] = create_model_dag('gefs-estendido', '2 0 * * *')
# globals()['GEFS-ESTENDIDO-MEMBROS'] = create_model_dag('gefs-estendido-membros', '2 0 * * *')
# globals()['ECMWF'] = create_model_dag('ecmwf', '0 4,17 * * *')
# globals()['ECMWF-ENS'] = create_model_dag('ecmwf-ens', '0 6,18 * * *')
# globals()['ECMWF-ENS-MEMBROS'] = create_model_dag('ecmwf-ens-membros', '0 6,18 * * *')
# globals()['ECMWF-AIFS'] = create_model_dag('ecmwf-aifs', '0 5,16 * * *')
# globals()['ECMWF-AIFS-ENS'] = create_model_dag('ecmwf-aifs-ens', '0 5,16 * * *')
# globals()['ECMWF-AIFS-ENS-MEMBROS'] = create_model_dag('ecmwf-aifs-ens-membros', '0 5,16 * * *')
# globals()['ECMWF-ENS-ESTENDIDO'] = create_model_dag('ecmwf-ens-estendido', '5 17 * * *')
# globals()['ECMWF-ENS-ESTENDIDO-MEMBROS'] = create_model_dag('ecmwf-ens-estendido', '5 17 * * *')

# ##################################################################################################

# # with DAG(
# #     'GFS',
# #     start_date= datetime.datetime(2024, 4, 28),
# #     description='A simple SSH command execution example',
# #     schedule='0 2,8,14,20 * * *',
# #     catchup=False,
# #     max_active_runs=1,
# #     concurrency=1,
# #     tags=['Metereologia', 'Mapas']

# # ) as dag:

# #     run_shell_script = SSHOperator(
# #         do_xcom_push=False,
# #         task_id='produtos_gfs',
# #         command='/projetos/produtos-meteorologia/produtos.sh gfs "" "" "" "" ""',
# #         dag=dag,
# #         ssh_conn_id='ssh_master',
# #         conn_timeout = TIME_OUT,
# #         cmd_timeout = TIME_OUT,
# #         execution_timeout = datetime.timedelta(hours=30),
# #         get_pty=True,
# #     )

# # ##################################################################################################

# # with DAG(
# #     'GEFS',
# #     start_date= datetime.datetime(2024, 4, 28),
# #     description='A simple SSH command execution example',
# #     schedule='0 3,8,15,20 * * *',
# #     catchup=False,
# #     max_active_runs=1,
# #     concurrency=1,
# #     tags=['Metereologia', 'Mapas']


# # ) as dag:

# #     run_shell_script = SSHOperator(
# #         do_xcom_push=False,
# #         task_id='produtos_gefs',
# #         command='/projetos/produtos-meteorologia/produtos.sh gefs "" "" "" "" ""',
# #         dag=dag,
# #         ssh_conn_id='ssh_master',
# #         conn_timeout = TIME_OUT,
# #         cmd_timeout = TIME_OUT,
# #         execution_timeout = datetime.timedelta(hours=30),
# #         get_pty=True,
# #     )