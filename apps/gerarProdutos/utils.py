import os
import requests

from PMO.scripts_unificados.apps.gerarProdutos.config import (
    URL_COGNITO,
    CONFIG_COGNITO,
    )


def get_access_token() -> str:
    response = requests.post(
        URL_COGNITO,
        data=CONFIG_COGNITO,
        headers={'Content-Type': 'application/x-www-form-urlencoded'}
    )
    return response.json()['access_token']
 