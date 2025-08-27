import os
import requests
import pdb
import datetime
from typing import Optional
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")),'.env'))

__API_URL_AIRFLOW = os.getenv('API_URL_AIRFLOW') 
__USER_AIRFLOW = os.getenv('USER_AIRFLOW')  
__PASSWORD_AIRFLOW = os.getenv('PASSWORD_AIRFLOW') 

def get_airflow_auth():
    return requests.auth.HTTPBasicAuth(__USER_AIRFLOW, __PASSWORD_AIRFLOW)

def trigger_airflow_dag(
    dag_id: str,
    json_produtos: dict={},
    momento_req: Optional[datetime.datetime]=None,
    url_airflow: Optional[str] = __API_URL_AIRFLOW,
):

    trigger_dag_url = f"{url_airflow}/dags/{dag_id}/dagRuns"
    json = {"conf": json_produtos}
    if momento_req != None:
        json["dag_run_id"]=f"{json_produtos['modelos'][0][0]}{momento_req.strftime('_%d_%m_%Y_%H_%M_%S')}"

    # Faz a chamada para a API do Airflow com autenticação
    answer = requests.post(trigger_dag_url, json=json, auth=get_airflow_auth(),verify=False)
    return answer
    
def get_dag_run_state(dag_id:str, dag_run_id:str):
    get_dag_url = f"{__API_URL_AIRFLOW}/dags/{dag_id}/dagRuns/{dag_run_id}"

    # Faz a chamada para a API do Airflow com autenticação
    answer = requests.get(get_dag_url, auth=get_airflow_auth(),verify=False)
    return {"state":answer.json()['state'], "end_datetime":datetime.datetime.strptime(answer.json()['end_date'][0:19] , '%Y-%m-%dT%H:%M:%S')}

if __name__ == '__main__':
    trigger_airflow_dag('TESTE')
    pass