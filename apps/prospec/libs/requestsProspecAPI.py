# -*- coding: utf-8 -*-
"""
Created on Wed Aug 15 11:03:44 2018
Plan4 
@author: Vitor
"""

import requests
import json

# -----------------------------------------------------------------------------
# Global variables | Variáveis globais
# -----------------------------------------------------------------------------

basicURL = 'https://api.prospec.app'
verifyCertificate = True
username = ''
password = ''

# -----------------------------------------------------------------------------
# Get token | Obter token
# -----------------------------------------------------------------------------


def getToken(username_temp, password_temp):
    url = basicURL + '/api/Token'

    global username
    username = username_temp
    global password
    password = password_temp

    headers = {
        'content-type': 'application/x-www-form-urlencoded'
    }

    data = {
        'grant_type': 'password',
        'username': username,
        'password': password
    }
    
    tokenResponse = requests.post(url, headers=headers, data=data,
                                  verify=verifyCertificate)
    token_json = tokenResponse.json()
    token = token_json["access_token"]

    return token

# -----------------------------------------------------------------------------
# Get JSON from REST API | Obter JSON via REST API
# -----------------------------------------------------------------------------


def getInfoFromAPI(*args):

    if len(args) < 2:
        print('Sao necessarios ao menos dois argumentos: token e apiFunction')
        return ''
    elif len(args) == 2:
        token = args[0]
        apiFunction = args[1]
    elif len(args) == 3:
        token = args[0]
        apiFunction = args[1]
        params = args[2]
    else:
        print('Sao necessarios ao menos dois argumentos: token, apiFunction e'
              ' parametros')
        return ''

    # Specify URL | Especificar URL
    url = basicURL + apiFunction

    headers = {
        'Authorization': 'Bearer ' + token,
        "Content-Type": "application/json"
    }

    # Call REST API | Chamar Rest API
    if 'params' in locals():
        response = requests.get(url, headers=headers, params=params,
                                verify=verifyCertificate)
    else:
        response = requests.get(url, headers=headers, verify=verifyCertificate)

    # print(response.status_code)

    if (response.status_code == 401):
        token = getToken(username, password)

        headers = {
            'Authorization': 'Bearer ' + token,
            "Content-Type": "application/json"
        }

        if 'params' in locals():
            response = requests.get(url, headers=headers, params=params,
                                    verify=verifyCertificate)
        else:
            response = requests.get(url, headers=headers,
                                    verify=verifyCertificate)

        print(response.status_code)

    if (response.status_code == 200):
        return response.json()
    return ''

# -----------------------------------------------------------------------------
# Get file from REST API | Obter arquivo via REST API
# -----------------------------------------------------------------------------


def getFileFromAPI(*args):

    if len(args) < 3:
        print('Sao necessarios ao menos tres argumentos: token e apiFunction')
        return ''
    elif len(args) == 3:
        token = args[0]
        apiFunction = args[1]
        fileName = args[2]
        pathToDownload = ''
    elif len(args) == 4:
        token = args[0]
        apiFunction = args[1]
        fileName = args[2]
        pathToDownload = args[3]
    else:
        print('Sao aceitos no máximo quatro argumentos: token, apiFunction e'
              ' parametros')
        return ''

    # Specify URL | Especificar URL
    url = basicURL + apiFunction

    headers = {
        'Authorization': 'Bearer ' + token,
        "Content-Type": "application/json"
    }

    # Call REST API | Chamar REST API
    response = requests.get(url, headers=headers, stream=True,
                            verify=verifyCertificate)

    # print(response.status_code)

    if (response.status_code == 401):
        token = getToken(username, password)

        headers = {
            'Authorization': 'Bearer ' + token,
            "Content-Type": "application/json"
        }

        response = requests.get(url, headers=headers, stream=True,
                                verify=verifyCertificate)

        print(response.status_code)
        print(response.text)

    if (response.status_code == 200):
        with open((pathToDownload + fileName), 'wb') as file:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:  # filter out keep-alive new chunks
                    file.write(chunk)

    return ''

# -----------------------------------------------------------------------------
# Get compilation from REST API | Obter compilação via REST API
# -----------------------------------------------------------------------------


def getCompilationFromAPI(*args):

    if len(args) < 3:
        print('Sao necessarios ao menos tres argumentos: token e apiFunction')
        return ''
    elif len(args) == 3:
        token = args[0]
        apiFunction = args[1]
        fileName = args[2]
        pathToDownload = ''
    elif len(args) == 4:
        token = args[0]
        apiFunction = args[1]
        fileName = args[2]
        pathToDownload = args[3]
    else:
        print('Sao aceitos no máximo quatro argumentos: token, apiFunction e'
              ' parametros')
        return ''

    # Specify URL | Especificar URL
    url = basicURL + apiFunction

    headers = {
        'Authorization': 'Bearer ' + token,
        "Content-Type": "application/json"
    }

    # Call REST API | Chamar REST API
    response = requests.post(url, headers=headers, stream=True,
                             verify=verifyCertificate)

    # print(response.status_code)

    if (response.status_code == 401):
        token = getToken(username, password)

        headers = {
            'Authorization': 'Bearer ' + token,
            "Content-Type": "application/json"
        }

        response = requests.post(url, headers=headers, stream=True,
                                 verify=verifyCertificate)

        print(response.status_code)

    # imprimir a mensagem de erro retornada do servidor
    if(response.status_code == 400):
        print(response.text)

    if (response.status_code == 200):
        with open((pathToDownload + fileName), 'wb') as file:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:  # filter out keep-alive new chunks
                    file.write(chunk)

    return ''

# -----------------------------------------------------------------------------
# Get file from S3 via REST API | Obter arquivos do S3 via REST API
# -----------------------------------------------------------------------------

def getFileFromS3viaAPI(*args):
    
    if len(args) < 3:
        print('Sao necessarios ao menos tres argumentos: token e apiFunction')
        return ''
    elif len(args) == 3:
        token = args[0]
        apiFunction = args[1]
        fileName = args[2]
        pathToDownload = ''
    elif len(args) == 4:
        token = args[0]
        apiFunction = args[1]
        fileName = args[2]
        pathToDownload = args[3]
    else:
        print('Sao aceitos no máximo quatro argumentos: token, apiFunction e'
                ' parametros')
        return ''
    
    #Specify URL | Especificar URL
    url = basicURL + apiFunction
    
    headers = {
        'Authorization': 'Bearer ' + token, 
        "Content-Type": "application/json"
        }
    
    #Call REST API | Chamar REST API
    response = requests.get(url, headers=headers, stream=True, 
                             verify=verifyCertificate, allow_redirects=True)
        
    print(response.status_code)
      
    if (response.status_code == 401):
        token = getToken(username, password)
        
        headers = {
            'Authorization': 'Bearer ' + token, 
            "Content-Type": "application/json"
            }
        
        response = requests.get(url, headers=headers, stream=True, 
                                 verify=verifyCertificate, allow_redirects=True)
        
        print(response.status_code)
    
    if (response.status_code == 200):
        with open((pathToDownload + fileName), 'wb') as file:
            for chunk in response.iter_content(chunk_size=1024): 
                if chunk: # filter out keep-alive new chunks
                    file.write(chunk)
        
    return ''

# -----------------------------------------------------------------------------
# Get file from S3 via REST API | Obter arquivos do S3 via REST API
# -----------------------------------------------------------------------------

def getFileFromS3viaAPIV2(*args):
    
    if len(args) < 5:
        print('Sao necessarios ao menos cinco argumentos: token e apiFunction')
        return ''
    elif len(args) == 5:
        token = args[0]
        apiFunction = args[1]
        fileNames = list(args[2])
        fileName = args[3]
        pathToDownload = args[4]
    else:
        print('Sao aceitos no máximo cinco argumentos: token, apiFunction, fileNames, fileName e'
                ' pathToDownload')
        return ''
    
    #Specify URL | Especificar URL
    url = basicURL + apiFunction
    
    headers = {
        'Authorization': 'Bearer ' + token, 
        "Content-Type": "application/json"
        }

    #Call REST API | Chamar REST API
    dado = {"FileNames": fileNames}

    response = requests.post(url, headers=headers, data=json.dumps(dado), verify=verifyCertificate)
        
    # print(response.status_code)
      
    if (response.status_code == 401):
        token = getToken(username, password)
        
        headers = {
            'Authorization': 'Bearer ' + token, 
            "Content-Type": "application/json"
            }
        
        response = requests.get(url, headers=headers, stream=True, 
                                 verify=verifyCertificate, allow_redirects=True)
        
        print(response.status_code)
    
    if (response.status_code == 200):
        with open((pathToDownload + fileName), 'wb') as file:
            for chunk in response.iter_content(chunk_size=1024): 
                if chunk: # filter out keep-alive new chunks
                    file.write(chunk)
        
    return ''


# -----------------------------------------------------------------------------
# Post in REST API | Postar via REST API
# -----------------------------------------------------------------------------


def postInAPI(*args):

    if len(args) < 4:
        print('Sao necessarios quatro argumentos: token, apiFunction, parametros'
              ' e dados')
        return ''
    elif len(args) >= 4:
        token = args[0]
        apiFunction = args[1]
        params = args[2]
        data = args[3]
    else:
        print('Sao necessarios quatro argumentos: token, apiFunction, parametros'
              ' e dados')
        return ''

    # Specify URL | especificar URL
    url = basicURL + apiFunction

    headers = {
        'Authorization': 'Bearer ' + token,
        "Content-Type": "application/json"
    }

    # Call REST API | Chamar REST API
    response = requests.post(url, headers=headers, params=params,
                             data=json.dumps(data), verify=verifyCertificate)

    # print(response.status_code)
    # print(response.text)

    if (response.status_code == 401):
        token = getToken(username, password)

        headers = {
            'Authorization': 'Bearer ' + token,
            "Content-Type": "application/json"
        }

        response = requests.post(url, headers=headers, params=params,
                                 data=json.dumps(data), verify=verifyCertificate)

        print(response.status_code)
        print(response.text)

    if (response.status_code == 200):
        return response.text
    elif (response.status_code == 201):
        return response.text

    return ''

# -----------------------------------------------------------------------------
# Patch in REST API | Patch via REST API
# -----------------------------------------------------------------------------

def patchInAPI(*args):

    if len(args) < 4:
        print('Sao necessarios quatro argumentos: token, apiFunction, parametros'
              ' e dados')
        return ''
    elif len(args) >= 4:
        token = args[0]
        apiFunction = args[1]
        params = args[2]
        data = args[3]
    else:
        print('Sao necessarios quatro argumentos: token, apiFunction, parametros'
              ' e dados')
        return ''

    # Specify URL | especificar URL
    url = basicURL + apiFunction

    headers = {
        'Authorization': 'Bearer ' + token,
        "Content-Type": "application/json"
    }

    # Call REST API | Chamar REST API
    response = requests.patch(url, headers=headers, params=params,
                             data=json.dumps(data), verify=verifyCertificate)

    if (response.status_code == 401):
        token = getToken(username, password)

        headers = {
            'Authorization': 'Bearer ' + token,
            "Content-Type": "application/json"
        }

        response = requests.patch(url, headers=headers, params=params,
                                 data=json.dumps(data), verify=verifyCertificate)

        print(response.status_code)
        print(response.text)

    if (response.status_code == 200):
        return response.text
    elif (response.status_code == 201):
        return response.text

    return ''

# -----------------------------------------------------------------------------
# Send files to REST API | Enviar arquivos para REST API
# -----------------------------------------------------------------------------


def sendFileToAPI(*args):

    if len(args) < 4:
        print('Sao necessarios quatro argumentos: token, apiFunction, '
              'pathToFile e nameToFile')
        return ''
    elif len(args) == 4:
        token = args[0]
        apiFunction = args[1]
        pathToFile = args[2]
        nameToFile = args[3]
    else:
        print('Sao necessarios quatro argumentos: token, apiFunction, '
              'pathToFile e nameToFile')
        return ''

    # Specify URL | Especificar URL
    url = basicURL + apiFunction

    headers = {
        'Authorization': 'Bearer ' + token
    }

    # Call REST API | Chamar via REST API
    files = {
        'file': (nameToFile, open(pathToFile, 'rb'),
                 'multipart/form-data', {'Expires': '0'})
    }
    response = requests.post(url, headers=headers, files=files,
                             verify=verifyCertificate)

    # print(response.status_code)
    # print(response.text)

    if (response.status_code == 401):
        token = getToken(username, password)

        headers = {
            'Authorization': 'Bearer ' + token,
            "Content-Type": "application/json"
        }

        response = requests.post(url, headers=headers, files=files,
                                 verify=verifyCertificate)

        print(response.status_code)
        print(response.text)

    if (response.status_code == 200):
        return response.json()
    elif (response.status_code == 201):
        return response

    return ''

# -----------------------------------------------------------------------------
# Send files | Enviar arquivos
# -----------------------------------------------------------------------------


def sendFiles(*args):

    if len(args) < 3:
        print('Sao necessarios tres argumentos: token, pathToFile e '
              'nameToFile')
        return ''
    elif len(args) == 3:
        token = args[0]
        apiFunction = args[1]
        files = args[2]
    else:
        print('Sao necessarios tres argumentos: token, pathToFile e '
              'nameToFile')
        return ''

    # Specify url | Especificar URL
    url = basicURL + apiFunction

    headers = {
        'Authorization': 'Bearer ' + token
    }

    pdb.set_trace()
    response = requests.post(url, headers=headers, files=files,
                             verify=verifyCertificate)

    print(response.status_code)
    print(response.text)

    if (response.status_code == 401):
        token = getToken(username, password)

        headers = {
            'Authorization': 'Bearer ' + token,
            "Content-Type": "application/json"
        }

        response = requests.post(url, headers=headers, files=files,
                                 verify=verifyCertificate)

        print(response.status_code)
        print(response.text)

    if (response.status_code == 200):
        return response.json()
    elif (response.status_code == 201):
        return response

    return ''


# -----------------------------------------------------------------------------
# Get file via REST API | Obter arquivos via REST API
# -----------------------------------------------------------------------------

def getFileviaAPI(*args): 
    if len(args) == 5:
        token = args[0]
        apiFunction = args[1]
        data = args[2]
        responseFileName = args[3]
        pathToDownload = args[4]
    else:
        print('São necessários cinco argumentos: token, apiFunction, fileName, responseFileName e'
                ' pathToDownload')
        return ''
    
    #Specify URL | Especificar URL
    url = basicURL + apiFunction
    
    headers = {
        'Authorization': 'Bearer ' + token, 
        "Content-Type": "application/json"
        }

    #Call REST API | Chamar REST API
    response = requests.post(url, headers=headers, data=json.dumps(data), verify=verifyCertificate)
        
    print(response.status_code)
      
    if (response.status_code == 401):
        token = getToken(username, password)
        
        headers = {
            'Authorization': 'Bearer ' + token, 
            "Content-Type": "application/json"
            }
        
        response = requests.get(url, headers=headers, stream=True, 
                                 verify=verifyCertificate, allow_redirects=True)
        
        print(response.status_code)
    
    if (response.status_code == 200):
        with open((pathToDownload + responseFileName), 'wb') as file:
            for chunk in response.iter_content(chunk_size=1024): 
                if chunk: # filter out keep-alive new chunks
                    file.write(chunk)
        
    return ''