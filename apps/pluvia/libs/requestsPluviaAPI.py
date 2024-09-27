# -*- coding: utf-8 -*-
"""
Created on Jun 2020 by Norus
"""

import os
import pdb
import pathlib
import requests
import datetime
from datetime import timezone

# -----------------------------------------------------------------------------
# Global variables | Variáveis globais
# -----------------------------------------------------------------------------

global token
global expires

basicURL ='https://api.pluvia.app'

verifyCertificate = True
username = ''
password = ''
token_file_path = os.path.join(pathlib.Path(__file__).parent.resolve(),'.pluvia')

# -----------------------------------------------------------------------------
# Get token | Obter token
# -----------------------------------------------------------------------------

def authenticatePluvia(_username, _password):
    global token, expires, username, password
    username = _username
    password = _password
    
    token, expires = getLocal()
    if token != '':
        expired = checkExpiredToken(expires)
        if expired:
            getNewToken()
    else:
        getNewToken()

# -----------------------------------------------------------------------------
# Funções Auxiliares
# -----------------------------------------------------------------------------

def getNewToken():
    global token, expires

    token, expires = getToken()
    saveToken(token, expires)

def saveToken(token, expires):
    with open(token_file_path, 'w') as f:
        f.write(token+'\n')
        f.write(expires)
        f.close()

def getLocal():
    if os.path.exists(token_file_path):
        with open(token_file_path, 'r') as f:
            
            token = f.readline().strip()
            expires = f.readline().strip()
    else:
        token = ''
        expires = ''
        
    return token, expires

def checkExpiredToken(expires):
    datetime_format = '%Y-%m-%dT%H:%M:%SZ'
    expires = datetime.datetime.strptime(expires, datetime_format).replace(tzinfo=timezone.utc)
    
    now = datetime.datetime.now(timezone.utc)

    return now > expires

# -----------------------------------------------------------------------------
# Get token | Obter token
# -----------------------------------------------------------------------------

def getToken():
    url = basicURL + '/v2/token'

    global token

    headers = {
        'content-type': 'application/json',
        'accept': '*/*'
    }

    data = {
        'username': username,
        'password': password
    }

    tokenResponse = requests.post(url, headers=headers, json=data,
                                  verify=verifyCertificate)
    
    token_json = tokenResponse.json()
    token = token_json["access_token"]
    limite_token = token_json["expires"]
    
    return token, limite_token

# -----------------------------------------------------------------------------
# Get JSON from REST API | Obter JSON via REST API
# -----------------------------------------------------------------------------

def getInfoFromAPI(apiFunction):
    global token
    global expires

    expired = checkExpiredToken(expires)
    if expired:
        getNewToken()
        
    # Specify URL | Especificar URL
    url = basicURL + apiFunction

    headers = {
        'Authorization': 'Bearer ' + token,
        "Content-Type": "application/json"
    }

    # Call REST API | Chamar Rest API
    response = requests.get(url, headers=headers, verify=verifyCertificate)

    if (response.status_code == 401):
        getNewToken()

        headers = {
            'Authorization': 'Bearer ' + token,
            "Content-Type": "application/json"
        }

        response = requests.get(url, headers=headers, verify=verifyCertificate)

    if (response.status_code == 200):
        return response.json()
    return ''

# -----------------------------------------------------------------------------
# Get file from REST API | Obter arquivo via REST API
# -----------------------------------------------------------------------------

def getFileFromAPI(apiFunction, fileName, pathToDownload = ''):
    global token
    global expires
    
    expired = checkExpiredToken(expires)

    if expired:
        getNewToken()
    
    # Specify URL | Especificar URL
    url = basicURL + apiFunction

    headers = {
        'Authorization': 'Bearer ' + token,
        "Content-Type": "application/json"
    }

    # Call REST API | Chamar REST API
    response = requests.get(url, headers=headers, stream=True,
                            verify=verifyCertificate)

    if (response.status_code == 401):
        getNewToken()

        headers = {
            'Authorization': 'Bearer ' + token,
            "Content-Type": "application/json"
        }

        response = requests.get(url, headers=headers, stream=True,
                                verify=verifyCertificate)

        print(response.status_code)

    if (response.status_code == 200):
        path_arquivo_saida = os.path.join(pathToDownload, fileName)
        try:
            with open(path_arquivo_saida, 'wb') as file:
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:  # filter out keep-alive new chunks
                        file.write(chunk)
        except:
            print('Não foi possível salvar o arquivo', str(path_arquivo_saida))

    return ''