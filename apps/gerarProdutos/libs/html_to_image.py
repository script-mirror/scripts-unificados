import os
import requests as req
from dotenv import load_dotenv
from random import randint
import uuid

load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")),'.env'))


URL_COGNITO = os.getenv('URL_COGNITO')
CONFIG_COGNITO = os.getenv('CONFIG_COGNITO')
URL_HTML_TO_IMAGE_WAZE = os.getenv('URL_HTML_TO_IMAGE_WAZE')

def get_access_token() -> str:
    response = req.post(
        URL_COGNITO,
        data=CONFIG_COGNITO,
        headers={'Content-Type': 'application/x-www-form-urlencoded'}
    )
    return response.json()['access_token']


def api_html_to_image_waze(html_str,path_save=f'output{uuid.uuid4().hex}.png'):
    headers = {
    'Authorization': f'Bearer {get_access_token()}'
    }
    payload = {
    "html": html_str,
    "options": {
        "type": "png",
        "quality": 100
        }
    }
    
    response = req.post(URL_HTML_TO_IMAGE_WAZE, headers=headers, json=payload)
    if response.status_code < 200 and response.status_code >= 300:
        raise Exception(f"Erro ao gerar imagem: {response.status_code}")
    with open(path_save, 'wb') as f:
        f.write(response.content)
    print(f"Salvo em: {os.path.abspath(path_save)}")
    return os.path.abspath(path_save)



def api_html_to_image(html_str,path_save='out_put.png'):
    
    return api_html_to_image_waze(html_str, path_save)

    __API_URL_HCTI = os.getenv('API_URL_HCTI') 
    __USER_HCTI = os.getenv('USER_HCTI') 
    __API_KEY_HCTI = os.getenv('API_KEY_HCTI') 

    __API_KEY_HCTI2 = os.getenv('API_KEY_HCTI2') 
    __USER_HCTI2 = os.getenv('USER_HCTI2') 

    __API_KEY_HCTI3 = os.getenv('API_KEY_HCTI3') 
    __USER_HCTI3 = os.getenv('USER_HCTI3') 

    __API_KEY_HCTI4 = os.getenv('API_KEY_HCTI4') 
    __USER_HCTI4 = os.getenv('USER_HCTI4') 

    __API_KEY_HCTI5 = os.getenv('API_KEY_HCTI5') 
    __USER_HCTI5 = os.getenv('USER_HCTI5') 


    data = { 'html': html_str,
            'google_fonts': "Roboto" 
            }
    
    imagem=None
    for user, token in [(__USER_HCTI,__API_KEY_HCTI),(__USER_HCTI2, __API_KEY_HCTI2),(__USER_HCTI3,__API_KEY_HCTI3),(__USER_HCTI4,__API_KEY_HCTI4),(__USER_HCTI5,__API_KEY_HCTI5)]:
        image = requests.post(url = __API_URL_HCTI, data = data, auth=(user, token))
        if image.status_code != 200:
            print(image.content)
            continue
        resposta = requests.get(image.json()['url'])
        imagem = Image.open(BytesIO(resposta.content))
        if image.status_code == 200:
            break

    if not imagem:
        print('Erro ao gerar imagem')
        return None
        
    imagem.save(path_save)
    print(f"Salvo em: {os.path.abspath(path_save)}")
    return os.path.abspath(path_save)