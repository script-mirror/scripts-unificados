import os
import sys
import requests

sys.path.insert(1,"/WX2TB/Documentos/fontes/PMO/scripts_unificados/")
from apps.gerarProdutos.config import ( URL_COGNITO, CONFIG_COGNITO )


def get_access_token() -> str:
    response = requests.post(
        URL_COGNITO,
        data=CONFIG_COGNITO,
        headers={'Content-Type': 'application/x-www-form-urlencoded'}
    )
    return response.json()['access_token']
 