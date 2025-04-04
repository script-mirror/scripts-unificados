import os
import time
import logging
import datetime
from PIL import Image
import requests as req
from io import BytesIO
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO,
                    format='%(levelname)s:\t%(asctime)s\t %(name)s.py:%(lineno)d\t %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers=[
                        logging.StreamHandler()
                    ])
logger = logging.getLogger(__name__)

load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")),'.env'))


URL_COGNITO = os.getenv('URL_COGNITO')
CONFIG_COGNITO = os.getenv('CONFIG_COGNITO')
URL_HTML_TO_IMAGE = os.getenv('URL_HTML_TO_IMAGE')


headers = {
    'Authorization': None
}

def get_access_token() -> str:
    response = req.post(
        URL_COGNITO,
        data=CONFIG_COGNITO,
        headers={'Content-Type': 'application/x-www-form-urlencoded'}
    )
    return response.json()['access_token']


def get_auth_header():
    if not headers['Authorization']:
        headers['Authorization'] = f'Bearer {get_access_token()}'
    
    return headers

def check_image_generation_status(job_id):
    response = req.get(f"{URL_HTML_TO_IMAGE}/job/{job_id}", headers=get_auth_header)
    return response

def get_image(job_id):
    response = req.get(f"{URL_HTML_TO_IMAGE}/job/{job_id}/image", headers=get_auth_header)
    logger.info(f"get image {response.status_code}")
    if response.status_code < 200 or response.status_code > 299:
        return None
    return response.content

def api_html_to_image(html_str,path_save=f'output{datetime.datetime.now().strftime("%Y%m%d%H%M%s")}.png'):
    payload = {
    "html": html_str,
        "options": {
            "type": "png",
            "quality": 100,
            "trim": True,
            "deviceScaleFactor": 10,
            "background": True
        }
    }
    
    response = req.post(URL_HTML_TO_IMAGE, headers=headers, json=payload)
    job_id = response.json()['jobId']

    while check_image_generation_status(job_id).json()['status'] != 'completed':
        time.sleep(1)
        logger.info(f"Aguardando processamento da imagem {job_id}")
    
    if response.status_code < 200 and response.status_code >= 300:
        raise Exception(f"Erro ao gerar imagem: {response.status_code}")
    
    image = get_image(job_id)
    if not image:
        return None
    
    with open(path_save, 'wb') as f:
        f.write(image)
    print(f"Salvo em: {os.path.abspath(path_save)}")
    return os.path.abspath(path_save)



def _api_html_to_image(html_str,path_save='out_put.png'):
    
    # return api_html_to_image(html_str, path_save)

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
        image = req.post(url = __API_URL_HCTI, data = data, auth=(user, token))
        if image.status_code != 200:
            print(image.content)
            continue
        resposta = req.get(image.json()['url'])
        imagem = Image.open(BytesIO(resposta.content))
        if image.status_code == 200:
            break

    if not imagem:
        print('Erro ao gerar imagem')
        return None
        
    imagem.save(path_save)
    print(f"Salvo em: {os.path.abspath(path_save)}")
    return os.path.abspath(path_save)