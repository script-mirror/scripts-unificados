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


def get_access_token() -> str:
    response = req.post(
        URL_COGNITO,
        data=CONFIG_COGNITO,
        headers={'Content-Type': 'application/x-www-form-urlencoded'}
    )
    return response.json()['access_token']



def check_image_generation_status(job_id):
    response = req.get(f"{URL_HTML_TO_IMAGE}/job/{job_id}", headers={
    'Authorization': f'Bearer {get_access_token()}'
    })
    return response

def get_image(job_id):
    response = req.get(f"{URL_HTML_TO_IMAGE}/job/{job_id}/image", headers={
    'Authorization': f'Bearer {get_access_token()}'
    })
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
            "deviceScaleFactor": 1,
            "background": True
        }
    }
    
    response = req.post(URL_HTML_TO_IMAGE,headers={
    'Authorization': f'Bearer {get_access_token()}'
    }, json=payload)
    job_id = response.json()['jobId']

    while check_image_generation_status(job_id).json()['status'] != 'completed':
        logger.info(f"Aguardando processamento da imagem {job_id}")
        time.sleep(1)
    
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


if __name__ == "__main__":

    api_html_to_image(
                      """
<div style="display:flex">
    <div align="center">
        <table style="width:1080.0pt; border-collapse:collapse" width="1440" cellpadding="0" cellspacing="0" border="0"
            class="x_MsoNormalTable">
            <tbody>
                <tr style="height:15.0pt">
                    <td style="width:200.0pt; border:solid windowtext 1.0pt; background:#781E77; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt"
                        valign="bottom" nowrap="" width="267">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:white">Sensibilidade</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:100.0pt; border:solid windowtext 1.0pt; border-left:none; background:#781E77; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; background-attachment:scroll"
                        valign="bottom" nowrap="" width="133"></td>
                    <td style="width:120.0pt; border:solid windowtext 1.0pt; border-left:none; background:#781E77; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; background-attachment:scroll"
                        valign="bottom" nowrap="" width="160">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:white">jun/25
                                - RV0<br>SE-S-NE-N</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:120.0pt; border:solid windowtext 1.0pt; border-left:none; background:#781E77; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; background-attachment:scroll"
                        valign="bottom" nowrap="" width="160">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:white">jul/25
                                - RV0<br>SE-S-NE-N</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:120.0pt; border:solid windowtext 1.0pt; border-left:none; background:#781E77; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; background-attachment:scroll"
                        valign="bottom" nowrap="" width="160">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:white">ago/25
                                - RV0<br>SE-S-NE-N</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:120.0pt; border:solid windowtext 1.0pt; border-left:none; background:#781E77; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; background-attachment:scroll"
                        valign="bottom" nowrap="" width="160">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:white">set/25
                                - RV0<br>SE-S-NE-N</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:120.0pt; border:solid windowtext 1.0pt; border-left:none; background:#781E77; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; background-attachment:scroll"
                        valign="bottom" nowrap="" width="160">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:white">out/25
                                - RV0<br>SE-S-NE-N</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:120.0pt; border:solid windowtext 1.0pt; border-left:none; background:#781E77; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; background-attachment:scroll"
                        valign="bottom" nowrap="" width="160">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:white">nov/25
                                - RV0<br>SE-S-NE-N</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:120.0pt; border:solid windowtext 1.0pt; border-left:none; background:#781E77; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; background-attachment:scroll"
                        valign="bottom" nowrap="" width="160">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:white">dez/25
                                - RV0<br>SE-S-NE-N</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                </tr>
                <tr style="height:15.0pt">
                    <td style="width:49.0pt; border:solid windowtext 1.0pt; border-top:none; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;28/04-ENA:2001_id25103</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;ENA-Mês(%)<br>EAR(%)<br>ENA-S1(GW)<br>GAP-IT(NW/DC)<br>PLD
                                NW(R$)<br>PLD DC(R$)</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;81-97-39-87<br>73.2-34.4-71.1-98.0<br>29.2-7.7-2.4-11.8<br>0.028-41/0.001-8<br>267-275-254-254<br>277-277-274-274</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;81-132-41-101<br>72.8-48.3-66.7-95.9<br>24.3-16.1-1.3-7.2<br>0.013-30/0.001-9<br>282-282-282-282<br>275-275-275-275</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;82-83-49-95<br>71.8-64.2-63.1-94.2<br>20.1-18.0-1.6-4.2<br>0.024-30/0.0-8<br>261-261-261-261<br>283-283-283-283</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;88-72-61-92<br>68.1-64.5-58.2-89.4<br>17.9-8.2-1.7-2.4<br>0.053-30/0.001-20<br>227-227-227-227<br>241-241-241-241</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;91-115-75-109<br>64.1-50.9-53.2-79.9<br>20.8-25.3-1.9-1.7<br>0.025-30/0.001-25<br>254-254-254-254<br>210-210-210-210</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;85-68-67-78<br>62.1-73.7-47.5-66.1<br>18.4-5.1-2.5-2.3<br>0.043-30/0.0-14<br>146-146-146-146<br>228-228-228-228</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;94-89-74-75<br>60.5-55.5-49.0-55.5<br>31.5-10.1-8.6-5.4<br>0.028-30/0.001-33<br>217-217-217-217<br>229-229-229-229</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                </tr>
                <tr style="height:15.0pt">
                    <td style="width:49.0pt; border:solid windowtext 1.0pt; border-top:none; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;28/04-ENA:2002_id25104</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;ENA-Mês(%)<br>EAR(%)<br>ENA-S1(GW)<br>GAP-IT(NW/DC)<br>PLD
                                NW(R$)<br>PLD DC(R$)</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;80-112-35-85<br>73.2-34.4-71.1-98.0<br>30.2-6.2-1.7-11.8<br>0.028-41/0.001-7<br>267-275-254-254<br>282-283-282-282</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;79-66-41-91<br>72.5-56.2-66.4-95.9<br>22.2-10.0-1.5-6.5<br>0.018-30/0.001-7<br>269-269-269-269<br>311-311-311-311</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;84-123-48-87<br>70.3-56.9-62.7-94.2<br>19.3-7.6-1.5-3.8<br>0.026-30/0.001-21<br>323-323-323-323<br>314-314-314-314</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;87-157-58-89<br>66.9-64.4-57.7-89.2<br>16.8-10.0-1.6-2.3<br>0.04-30/0.001-16<br>244-244-244-244<br>210-210-210-210</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;64-139-60-64<br>64.2-79.3-53.3-80.1<br>17.3-17.1-2.1-1.8<br>0.026-30/0.001-29<br>163-162-163-163<br>227-227-227-227</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;77-167-56-54<br>59.1-88.5-47.1-66.7<br>22.5-20.8-2.0-2.0<br>0.062-30/0.001-11<br>188-188-188-188<br>189-189-189-189</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;71-191-43-42<br>57.0-87.2-47.3-56.8<br>25.2-20.3-2.2-1.8<br>0.032-30/0.001-35<br>149-149-149-149<br>233-233-233-233</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                </tr>
                <tr style="height:15.0pt">
                    <td style="width:49.0pt; border:solid windowtext 1.0pt; border-top:none; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;28/04-ENA:2003_id25105</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;ENA-Mês(%)<br>EAR(%)<br>ENA-S1(GW)<br>GAP-IT(NW/DC)<br>PLD
                                NW(R$)<br>PLD DC(R$)</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;74-74-38-78<br>73.2-34.4-71.1-98.0<br>25.7-6.2-1.9-11.1<br>0.028-41/0.001-7<br>267-275-254-254<br>312-314-311-311</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;75-66-40-85<br>71.5-47.3-66.5-95.6<br>20.5-4.3-1.5-5.9<br>0.01-30/0.001-8<br>307-307-306-306<br>336-336-336-336</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;74-37-49-90<br>69.3-48.8-62.8-93.7<br>17.2-5.2-1.6-3.9<br>0.024-30/0.001-16<br>324-324-324-324<br>395-396-395-395</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;69-26-56-88<br>63.2-43.0-56.7-88.8<br>13.7-2.8-1.6-2.5<br>0.099-30/0.0-25<br>306-306-306-306<br>385-386-384-384</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;69-36-47-68<br>57.2-36.2-51.4-79.7<br>14.0-3.4-1.6-1.7<br>0.04-30/0.001-16<br>378-378-378-378<br>516-517-516-516</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;64-61-46-57<br>51.2-36.7-43.3-65.2<br>17.7-4.2-2.3-1.8<br>0.033-30/0.001-26<br>491-491-491-491<br>583-583-583-583</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;67-191-32-47<br>47.2-37.7-42.6-54.9<br>31.8-4.3-1.8-2.9<br>0.077-43/0.001-30<br>453-453-441-441<br>475-475-475-475</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                </tr>
                <tr style="height:15.0pt">
                    <td style="width:49.0pt; border:solid windowtext 1.0pt; border-top:none; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;28/04-ENA:2004_id25107</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;ENA-Mês(%)<br>EAR(%)<br>ENA-S1(GW)<br>GAP-IT(NW/DC)<br>PLD
                                NW(R$)<br>PLD DC(R$)</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;96-75-35-84<br>73.2-34.4-71.1-98.0<br>38.7-7.4-1.7-12.1<br>0.028-41/0.001-10<br>267-275-254-254<br>258-260-257-257</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;97-100-43-88<br>74.5-47.2-66.4-95.6<br>25.7-5.9-1.6-6.2<br>0.022-30/0.001-16<br>258-258-257-257<br>251-251-251-251</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;89-49-49-88<br>74.1-60.4-63.0-93.8<br>22.6-8.0-1.7-3.9<br>0.019-30/0.001-36<br>247-247-247-247<br>293-293-293-293</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;78-58-56-84<br>69.5-54.3-57.8-88.8<br>16.5-4.4-1.6-2.4<br>0.012-30/0.001-51<br>250-250-250-250<br>312-312-312-312</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;91-98-48-86<br>64.5-49.7-52.3-79.2<br>14.2-11.2-1.6-1.6<br>0.02-30/0.001-25<br>276-276-276-276<br>271-271-271-271</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;80-113-38-65<br>61.6-64.2-44.7-65.3<br>22.3-14.1-2.0-2.2<br>0.026-30/0.001-11<br>239-239-239-239<br>309-309-309-309</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;86-59-42-61<br>60.2-62.1-42.5-55.8<br>30.2-5.4-3.0-4.5<br>0.027-30/0.001-33<br>216-216-216-216<br>307-307-307-307</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                </tr>
                <tr style="height:15.0pt">
                    <td style="width:49.0pt; border:solid windowtext 1.0pt; border-top:none; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;28/04-ENA:2005_id25108</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;ENA-Mês(%)<br>EAR(%)<br>ENA-S1(GW)<br>GAP-IT(NW/DC)<br>PLD
                                NW(R$)<br>PLD DC(R$)</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;96-195-48-92<br>73.2-34.4-71.1-98.0<br>40.3-12.5-2.2-13.4<br>0.028-41/0.001-9<br>267-275-254-254<br>181-181-173-173</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;84-88-42-95<br>75.9-82.4-67.4-95.6<br>25.0-9.6-1.5-6.7<br>0.023-30/0.001-7<br>173-173-173-173<br>224-224-224-224</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;83-90-52-90<br>74.9-79.8-63.7-93.7<br>20.5-10.4-1.7-4.0<br>0.025-30/0.001-14<br>246-246-246-246<br>246-246-246-246</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;94-237-58-89<br>71.5-57.8-59.0-88.8<br>17.3-39.5-1.7-2.3<br>0.087-30/0.0-7<br>221-221-221-221<br>143-143-143-143</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;94-172-53-64<br>69.1-91.9-55.1-80.1<br>21.4-13.8-1.7-2.0<br>0.021-30/0.001-19<br>93-93-93-93<br>98-63-98-98</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;87-111-70-53<br>67.5-93.7-48.8-67.0<br>28.7-17.0-1.9-1.5<br>0.036-30/0.001-16<br>77-77-77-77<br>87-87-87-87</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;117-58-134-91<br>65.9-82.6-50.3-56.3<br>48.6-5.5-6.0-4.2<br>0.023-30/0.001-18<br>82-82-82-82<br>24-24-24-24</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                </tr>
                <tr style="height:15.0pt">
                    <td style="width:49.0pt; border:solid windowtext 1.0pt; border-top:none; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;28/04-ENA:2006_id25109</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;ENA-Mês(%)<br>EAR(%)<br>ENA-S1(GW)<br>GAP-IT(NW/DC)<br>PLD
                                NW(R$)<br>PLD DC(R$)</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;67-24-35-110<br>73.2-34.4-71.1-98.0<br>24.4-1.6-1.7-15.4<br>0.028-41/0.001-21<br>267-275-254-254<br>326-620-189-189</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;69-37-40-113<br>70.8-31.9-66.3-96.0<br>19.8-3.5-1.5-8.2<br>0.014-30/0.001-27<br>343-345-341-341<br>378-666-376-376</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;68-77-47-103<br>67.2-29.3-62.2-93.9<br>16.0-10.3-1.5-4.6<br>0.018-31/0.001-12<br>371-375-370-370<br>407-408-404-404</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;73-56-58-100<br>61.3-37.4-55.7-88.4<br>14.8-7.6-1.5-2.7<br>0.044-44/0.001-40<br>316-316-316-316<br>335-335-332-333</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;85-37-78-101<br>57.0-39.5-51.1-79.3<br>15.1-5.8-2.0-2.1<br>0.051-30/0.001-15<br>334-334-334-334<br>372-372-371-372</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;76-87-95-100<br>53.4-39.0-44.8-65.4<br>21.6-5.8-3.7-3.2<br>0.053-30/0.0-13<br>387-387-387-387<br>413-413-413-413</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;87-87-76-71<br>51.7-40.2-47.6-55.2<br>30.4-7.8-3.7-4.6<br>0.033-41/0.001-25<br>308-309-298-298<br>332-333-331-332</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                </tr>
                <tr style="height:15.0pt">
                    <td style="width:49.0pt; border:solid windowtext 1.0pt; border-top:none; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;28/04-ENA:2007_id25110</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;ENA-Mês(%)<br>EAR(%)<br>ENA-S1(GW)<br>GAP-IT(NW/DC)<br>PLD
                                NW(R$)<br>PLD DC(R$)</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;77-77-35-88<br>73.2-34.4-71.1-98.0<br>28.6-13.4-1.6-12.3<br>0.028-41/0.001-7<br>267-275-254-254<br>301-303-292-292</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;86-122-40-96<br>71.9-49.3-66.3-95.7<br>21.7-4.8-1.5-6.7<br>0.007-30/0.001-7<br>263-263-263-263<br>266-266-266-266</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;80-73-48-96<br>70.5-57.0-62.7-93.8<br>25.9-14.7-1.5-4.2<br>0.036-30/0.001-10<br>276-276-276-276<br>318-318-318-318</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;65-64-54-98<br>66.9-57.2-57.6-89.0<br>13.9-6.3-1.5-2.6<br>0.057-50/0.001-43<br>245-245-245-245<br>303-303-303-303</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;55-61-43-67<br>60.9-54.4-52.9-79.8<br>12.0-8.3-1.5-1.8<br>0.031-35/0.001-19<br>290-290-290-290<br>400-400-400-400</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;63-133-31-36<br>53.8-55.4-44.4-65.4<br>18.4-17.4-1.3-1.2<br>0.043-38/0.001-13<br>346-346-346-346<br>412-412-412-412</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;57-92-30-34<br>50.5-63.3-41.3-55.0<br>18.0-5.3-2.6-2.8<br>0.053-42/0.001-15<br>317-317-317-317<br>475-475-475-475</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                </tr>
                <tr style="height:15.0pt">
                    <td style="width:49.0pt; border:solid windowtext 1.0pt; border-top:none; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;28/04-ENA:2008_id25111</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;ENA-Mês(%)<br>EAR(%)<br>ENA-S1(GW)<br>GAP-IT(NW/DC)<br>PLD
                                NW(R$)<br>PLD DC(R$)</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;73-63-34-95<br>73.2-34.4-71.1-98.0<br>27.0-5.6-1.6-13.5<br>0.028-41/0.001-6<br>267-275-254-254<br>312-314-263-263</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;70-53-40-101<br>71.6-40.5-66.3-96.0<br>20.4-10.6-1.5-7.1<br>0.022-30/0.001-20<br>309-309-309-309<br>340-341-340-340</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;83-65-47-99<br>68.6-43.1-62.6-94.2<br>17.1-4.7-1.6-4.5<br>0.044-30/0.001-45<br>355-355-355-355<br>380-380-380-380</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;68-47-54-94<br>64.2-41.1-57.5-89.3<br>14.2-4.4-1.5-2.5<br>0.048-30/0.001-22<br>272-272-272-272<br>332-332-332-332</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;59-83-50-70<br>58.8-38.6-53.0-80.1<br>13.7-4.2-1.6-1.9<br>0.07-30/0.001-22<br>318-318-318-318<br>386-386-386-386</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;53-122-44-55<br>53.1-48.3-44.7-65.9<br>14.0-20.5-1.3-1.1<br>0.069-32/0.001-9<br>355-355-355-355<br>465-465-465-465</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;56-46-47-60<br>48.9-51.5-44.1-55.8<br>17.3-4.6-4.9-5.0<br>0.038-41/0.001-28<br>359-359-357-357<br>544-545-420-544</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                </tr>
                <tr style="height:15.0pt">
                    <td style="width:49.0pt; border:solid windowtext 1.0pt; border-top:none; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;28/04-ENA:2009_id25112</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;ENA-Mês(%)<br>EAR(%)<br>ENA-S1(GW)<br>GAP-IT(NW/DC)<br>PLD
                                NW(R$)<br>PLD DC(R$)</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;80-55-42-152<br>73.2-34.4-71.1-98.0<br>29.7-8.0-2.1-19.9<br>0.028-41/0.001-10<br>267-275-254-254<br>281-285-34-34</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;104-155-45-168<br>72.7-41.1-67.1-95.9<br>24.0-5.8-1.7-12.4<br>0.011-30/0.001-39<br>253-253-252-252<br>0-0-0-0</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;113-206-51-139<br>72.8-74.2-62.2-93.3<br>31.5-15.9-1.6-6.4<br>0.031-30/0.001-17<br>129-129-129-129<br>11-11-11-11</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;133-245-68-127<br>71.3-91.8-57.5-89.5<br>20.4-12.1-1.9-3.4<br>0.047-30/0.0-10<br>18-18-18-18<br>0-0-0-0</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;127-166-89-132<br>70.8-93.3-54.1-81.0<br>24.5-39.6-2.2-2.5<br>0.087-30/0.001-9<br>2-2-2-2<br>0-0-0-0</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;99-122-105-100<br>69.5-93.2-52.1-68.1<br>31.4-9.5-7.0-4.6<br>0.049-30/0.001-10<br>0-0-0-0<br>0-0-0-0</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;110-120-58-80<br>69.2-91.0-54.4-59.1<br>38.0-12.7-2.7-4.0<br>0.016-33/0.001-62<br>1-1-1-1<br>0-0-0-0</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                </tr>
                <tr style="height:15.0pt">
                    <td style="width:49.0pt; border:solid windowtext 1.0pt; border-top:none; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;28/04-ENA:2010_id25113</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;ENA-Mês(%)<br>EAR(%)<br>ENA-S1(GW)<br>GAP-IT(NW/DC)<br>PLD
                                NW(R$)<br>PLD DC(R$)</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;72-87-37-78<br>73.2-34.4-71.1-98.0<br>26.0-8.8-1.9-11.2<br>0.028-41/0.0-9<br>267-275-254-254<br>310-311-309-309</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;75-100-41-91<br>71.3-49.4-66.4-95.7<br>20.1-6.4-1.5-6.1<br>0.012-30/0.001-13<br>295-295-294-294<br>315-315-315-315</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;72-80-49-98<br>69.3-53.0-62.8-94.2<br>17.3-12.2-1.6-4.3<br>0.064-30/0.0-18<br>314-314-314-314<br>342-342-342-342</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;71-64-54-94<br>64.7-54.6-57.6-89.5<br>13.1-4.7-1.6-2.6<br>0.011-50/0.001-66<br>264-264-264-264<br>300-300-300-300</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;84-41-60-85<br>59.1-50.0-52.3-80.3<br>25.0-7.7-1.5-1.7<br>0.029-30/0.001-18<br>298-298-298-298<br>335-335-335-335</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;84-56-89-89<br>56.1-48.3-45.1-65.9<br>23.9-5.9-3.1-3.2<br>0.017-30/0.001-20<br>383-383-383-383<br>387-387-387-387</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;89-160-67-80<br>54.4-40.1-47.9-55.8<br>32.0-9.2-5.3-4.4<br>0.046-30/0.001-11<br>298-299-295-295<br>253-253-253-253</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                </tr>
                <tr style="height:15.0pt">
                    <td style="width:49.0pt; border:solid windowtext 1.0pt; border-top:none; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;28/04-ENA:2011_id25114</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;ENA-Mês(%)<br>EAR(%)<br>ENA-S1(GW)<br>GAP-IT(NW/DC)<br>PLD
                                NW(R$)<br>PLD DC(R$)</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;76-73-35-105<br>73.2-34.4-71.1-98.0<br>25.2-2.7-1.7-15.2<br>0.028-41/0.0-8<br>267-275-254-254<br>300-302-205-205</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;75-200-40-103<br>72.3-38.2-66.3-95.6<br>21.4-19.7-1.5-7.5<br>0.024-30/0.001-15<br>308-308-307-307<br>269-269-269-269</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;94-276-47-94<br>71.0-76.2-62.7-94.0<br>18.1-22.2-1.5-4.2<br>0.029-30/0.001-51<br>250-250-250-250<br>0-0-0-0</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;70-201-54-89<br>68.7-82.8-57.7-89.4<br>16.0-32.4-1.5-2.4<br>0.071-30/0.001-3<br>82-82-82-82<br>130-130-130-130</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;92-75-69-92<br>64.6-94.6-53.6-80.9<br>12.6-9.6-1.5-1.7<br>0.019-30/0.001-13<br>146-145-146-146<br>154-154-154-154</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;76-63-55-111<br>61.9-80.6-48.2-66.9<br>18.2-7.7-3.5-3.0<br>0.026-30/0.001-6<br>162-162-162-162<br>201-201-201-201</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;84-43-77-146<br>58.6-68.8-48.2-56.1<br>31.5-3.7-4.0-9.3<br>0.052-30/0.001-18<br>143-143-143-143<br>137-137-136-136</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                </tr>
                <tr style="height:15.0pt">
                    <td style="width:49.0pt; border:solid windowtext 1.0pt; border-top:none; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;28/04-ENA:2012_id25115</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;ENA-Mês(%)<br>EAR(%)<br>ENA-S1(GW)<br>GAP-IT(NW/DC)<br>PLD
                                NW(R$)<br>PLD DC(R$)</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;130-145-40-78<br>73.2-34.4-71.1-98.0<br>33.3-11.0-1.8-11.2<br>0.028-41/0.001-34<br>267-275-254-254<br>0-0-0-0</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;112-98-42-84<br>76.7-74.5-67.1-95.8<br>38.3-9.9-1.8-5.7<br>0.005-30/0.001-28<br>100-100-100-100<br>12-12-12-12</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;96-82-50-86<br>77.2-77.2-61.9-94.2<br>23.7-17.3-1.6-3.8<br>0.012-30/0.0-9<br>178-178-178-178<br>194-194-194-194</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;86-51-56-82<br>74.3-72.3-57.1-89.4<br>17.3-3.8-1.6-2.2<br>0.015-30/0.001-32<br>163-163-163-163<br>236-236-236-236</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;71-46-46-64<br>69.3-61.8-51.8-79.8<br>18.2-5.1-1.6-1.6<br>0.012-30/0.0-21<br>229-229-229-229<br>345-345-345-345</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;76-43-84-78<br>64.0-51.3-43.7-65.7<br>17.7-6.2-1.6-1.5<br>0.039-30/0.0-40<br>322-322-322-322<br>391-391-391-391</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;67-53-54-89<br>61.1-40.8-45.8-55.3<br>27.6-2.9-7.2-6.0<br>0.015-30/0.001-24<br>273-274-272-272<br>426-432-425-425</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                </tr>
                <tr style="height:15.0pt">
                    <td style="width:49.0pt; border:solid windowtext 1.0pt; border-top:none; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;28/04-ENA:2013_id25116</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;ENA-Mês(%)<br>EAR(%)<br>ENA-S1(GW)<br>GAP-IT(NW/DC)<br>PLD
                                NW(R$)<br>PLD DC(R$)</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;108-166-42-98<br>73.2-34.4-71.1-98.0<br>41.2-10.6-1.7-14.1<br>0.028-41/0.001-75<br>267-275-254-254<br>0-0-0-0</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;109-151-41-102<br>75.0-59.5-67.2-96.0<br>44.0-30.3-1.6-7.3<br>0.013-30/0.001-39<br>178-178-178-178<br>0-0-0-0</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;92-197-50-97<br>75.4-79.8-63.4-94.2<br>25.1-14.7-1.7-4.2<br>0.011-30/0.001-8<br>136-136-136-136<br>80-80-80-80</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;86-156-57-95<br>72.3-79.1-58.6-89.5<br>16.9-13.7-1.6-2.6<br>0.015-30/0.001-11<br>98-98-98-98<br>103-103-103-103</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;86-95-59-77<br>68.7-76.2-53.8-81.0<br>19.4-24.2-1.7-1.8<br>0.027-30/0.001-15<br>92-92-92-92<br>116-116-116-116</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;77-70-40-66<br>65.3-81.8-48.8-66.8<br>20.9-6.9-1.8-2.1<br>0.039-30/0.001-10<br>95-95-95-95<br>173-173-173-173</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;82-71-88-108<br>61.5-68.9-46.3-56.7<br>34.9-5.6-3.3-4.0<br>0.02-30/0.001-19<br>141-141-141-141<br>170-170-170-170</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                </tr>
                <tr style="height:15.0pt">
                    <td style="width:49.0pt; border:solid windowtext 1.0pt; border-top:none; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;28/04-ENA:2014_id25117</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;ENA-Mês(%)<br>EAR(%)<br>ENA-S1(GW)<br>GAP-IT(NW/DC)<br>PLD
                                NW(R$)<br>PLD DC(R$)</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;96-285-35-88<br>73.2-34.4-71.1-98.0<br>30.2-16.5-1.7-12.6<br>0.028-41/0.001-37<br>267-275-254-254<br>0-0-0-0</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;90-151-41-91<br>73.6-68.8-66.5-95.9<br>24.2-51.8-1.5-6.6<br>0.024-30/0.001-15<br>101-101-101-101<br>78-78-78-78</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;88-73-52-87<br>72.1-92.6-62.9-94.2<br>26.8-10.4-1.6-3.8<br>0.013-30/0.001-11<br>197-197-197-197<br>241-241-241-241</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;85-114-56-83<br>68.6-82.4-58.2-89.5<br>16.1-10.1-1.6-2.3<br>0.023-30/0.001-17<br>196-196-196-196<br>164-164-164-164</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;70-122-46-68<br>64.9-67.0-54.1-80.8<br>24.9-34.1-1.5-1.6<br>0.017-30/0.001-18<br>157-156-157-157<br>186-186-186-186</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;67-85-48-70<br>60.6-84.0-47.1-67.9<br>17.2-8.4-1.7-1.8<br>0.023-30/0.001-10<br>147-146-147-147<br>210-210-210-210</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;75-122-60-62<br>56.3-72.3-45.8-57.8<br>35.9-6.6-3.4-2.8<br>0.022-30/0.001-23<br>221-221-220-220<br>269-269-269-269</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                </tr>
                <tr style="height:15.0pt">
                    <td style="width:49.0pt; border:solid windowtext 1.0pt; border-top:none; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;28/04-ENA:2015_id25118</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;ENA-Mês(%)<br>EAR(%)<br>ENA-S1(GW)<br>GAP-IT(NW/DC)<br>PLD
                                NW(R$)<br>PLD DC(R$)</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;88-119-39-93<br>73.2-34.4-71.1-98.0<br>32.7-7.7-2.0-13.4<br>0.028-41/0.001-5<br>267-275-254-254<br>248-249-220-220</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;126-253-43-95<br>73.3-59.1-66.8-95.8<br>25.8-9.0-1.6-6.7<br>0.018-30/0.001-19<br>214-214-213-213<br>0-0-0-0</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;97-80-51-91<br>73.3-81.4-63.2-94.1<br>25.7-19.3-1.7-4.0<br>0.03-30/0.001-8<br>66-66-66-66<br>138-138-138-138</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;127-115-66-86<br>69.5-86.5-58.5-89.5<br>17.7-6.7-1.7-2.4<br>0.017-30/0.001-8<br>106-106-106-106<br>60-60-60-60</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;89-208-52-61<br>68.9-79.5-55.4-80.6<br>21.3-26.3-1.9-1.6<br>0.031-30/0.001-21<br>53-52-53-53<br>30-26-30-30</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;101-151-43-36<br>66.3-93.1-50.5-67.9<br>29.4-13.1-1.9-1.8<br>0.035-30/0.001-19<br>17-17-17-17<br>11-3-11-11</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;77-230-36-18<br>66.0-91.0-48.4-58.6<br>40.8-18.1-4.3-1.6<br>0.017-30/0.001-24<br>19-19-19-19<br>112-17-112-112</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                </tr>
                <tr style="height:15.0pt">
                    <td style="width:49.0pt; border:solid windowtext 1.0pt; border-top:none; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;28/04-ENA:2016_id25119</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;ENA-Mês(%)<br>EAR(%)<br>ENA-S1(GW)<br>GAP-IT(NW/DC)<br>PLD
                                NW(R$)<br>PLD DC(R$)</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;115-81-38-73<br>73.2-34.4-71.1-98.0<br>43.5-11.9-1.7-10.6<br>0.028-41/0.0-7<br>267-275-254-254<br>239-239-239-239</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;90-91-41-80<br>77.2-47.1-66.7-95.8<br>26.6-6.6-1.6-5.5<br>0.019-30/0.001-13<br>232-232-232-232<br>267-267-267-267</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;100-120-49-78<br>76.0-58.8-63.0-94.0<br>20.8-6.5-1.6-3.4<br>0.033-30/0.001-13<br>256-256-256-256<br>245-245-245-245</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;89-79-56-82<br>73.4-61.6-58.1-89.4<br>20.4-12.4-1.6-2.0<br>0.013-30/0.001-24<br>179-179-179-179<br>236-236-236-236</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;82-103-68-65<br>69.8-61.5-53.4-80.4<br>15.5-4.9-1.7-2.1<br>0.018-30/0.0-27<br>206-206-206-206<br>240-240-240-240</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;81-69-56-50<br>65.6-66.0-47.5-65.8<br>19.4-9.7-1.7-1.1<br>0.021-30/0.001-22<br>211-211-211-211<br>307-307-307-307</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;67-85-57-34<br>63.8-53.2-47.4-55.7<br>26.7-6.6-6.3-2.0<br>0.014-30/0.001-20<br>248-248-248-248<br>416-416-416-416</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                </tr>
                <tr style="height:15.0pt">
                    <td style="width:49.0pt; border:solid windowtext 1.0pt; border-top:none; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;28/04-ENA:2017_id25120</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;ENA-Mês(%)<br>EAR(%)<br>ENA-S1(GW)<br>GAP-IT(NW/DC)<br>PLD
                                NW(R$)<br>PLD DC(R$)</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;104-282-42-73<br>73.2-34.4-71.1-98.0<br>34.7-58.6-2.5-10.5<br>0.028-41/0.001-21<br>267-275-254-254<br>29-29-29-29</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;87-44-40-79<br>75.7-92.4-67.0-95.7<br>25.9-7.0-1.5-5.5<br>0.011-30/0.001-10<br>68-68-68-68<br>213-213-213-213</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;91-68-49-79<br>73.8-79.4-63.1-94.2<br>19.9-3.5-1.6-3.4<br>0.03-30/0.0-22<br>264-264-264-264<br>292-292-292-292</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;76-34-55-74<br>69.7-70.0-58.2-89.1<br>16.5-4.6-1.6-2.0<br>0.072-30/0.001-61<br>235-235-235-235<br>319-319-319-319</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;80-90-50-52<br>64.2-58.6-52.8-79.5<br>16.7-4.9-1.5-1.4<br>0.017-30/0.001-19<br>301-301-301-301<br>325-325-325-325</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;105-122-48-69<br>60.1-67.9-44.8-65.7<br>34.9-16.1-1.7-2.0<br>0.038-30/0.001-15<br>287-287-287-287<br>223-222-223-223</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;96-84-67-88<br>61.0-71.1-43.6-55.5<br>44.9-6.6-3.2-3.4<br>0.023-30/0.001-23<br>154-154-154-154<br>181-181-181-181</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                </tr>
                <tr style="height:15.0pt">
                    <td style="width:49.0pt; border:solid windowtext 1.0pt; border-top:none; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;28/04-ENA:2018_id25121</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;ENA-Mês(%)<br>EAR(%)<br>ENA-S1(GW)<br>GAP-IT(NW/DC)<br>PLD
                                NW(R$)<br>PLD DC(R$)</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;71-49-35-84<br>73.2-34.4-71.1-98.0<br>25.4-3.0-1.8-11.9<br>0.028-41/0.001-22<br>267-275-254-254<br>319-323-318-318</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;69-63-40-91<br>70.8-34.3-66.3-95.8<br>20.1-10.1-1.5-6.4<br>0.013-30/0.001-12<br>324-325-323-323<br>362-363-361-361</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;80-64-49-93<br>67.2-42.2-62.3-94.2<br>16.5-6.5-1.5-4.0<br>0.052-30/0.001-20<br>366-366-366-366<br>397-397-396-396</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;77-108-56-89<br>62.4-32.2-57.2-88.9<br>14.0-21.4-1.6-2.4<br>0.047-43/0.001-23<br>316-316-316-316<br>323-323-323-323</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;94-113-57-81<br>59.0-50.6-52.3-79.9<br>15.5-8.7-1.7-1.6<br>0.049-30/0.001-13<br>293-293-293-293<br>277-277-277-277</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;119-106-83-85<br>57.9-68.8-45.2-65.6<br>23.9-16.8-2.5-1.9<br>0.023-30/0.0-12<br>267-267-267-267<br>156-156-156-156</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;89-84-78-161<br>61.5-62.0-49.8-55.5<br>52.4-8.0-6.4-8.9<br>0.013-30/0.001-16<br>119-119-119-119<br>164-164-164-164</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                </tr>
                <tr style="height:15.0pt">
                    <td style="width:49.0pt; border:solid windowtext 1.0pt; border-top:none; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;28/04-ENA:2019_id25122</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;ENA-Mês(%)<br>EAR(%)<br>ENA-S1(GW)<br>GAP-IT(NW/DC)<br>PLD
                                NW(R$)<br>PLD DC(R$)</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;87-167-37-93<br>73.2-34.4-71.1-98.0<br>36.7-39.4-2.0-13.2<br>0.028-41/0.001-6<br>267-275-254-254<br>136-136-136-136</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;83-67-41-100<br>74.0-75.1-66.7-96.0<br>22.4-12.4-1.4-7.0<br>0.018-30/0.001-13<br>170-170-170-170<br>245-245-245-245</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;78-36-49-103<br>72.6-74.1-62.9-94.2<br>18.0-4.8-1.6-4.4<br>0.02-30/0.001-28<br>260-260-260-260<br>319-319-319-319</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;72-32-55-97<br>66.7-60.0-57.8-89.5<br>15.9-3.5-1.6-2.6<br>0.079-30/0.0-19<br>265-265-265-265<br>332-332-332-332</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;58-48-52-80<br>60.9-52.6-52.4-80.0<br>15.4-3.8-1.5-2.0<br>0.034-30/0.0-19<br>296-296-296-296<br>412-412-412-412</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;56-98-37-54<br>54.1-45.7-44.4-65.6<br>13.3-13.3-1.6-1.8<br>0.052-30/0.001-9<br>402-402-402-402<br>505-505-505-505</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;79-62-65-50<br>49.6-43.3-42.5-55.6<br>31.3-5.8-4.0-3.4<br>0.041-41/0.001-25<br>400-400-395-395<br>477-479-477-477</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                </tr>
                <tr style="height:15.0pt">
                    <td style="width:49.0pt; border:solid windowtext 1.0pt; border-top:none; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;28/04-ENA:2020_id25123</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;ENA-Mês(%)<br>EAR(%)<br>ENA-S1(GW)<br>GAP-IT(NW/DC)<br>PLD
                                NW(R$)<br>PLD DC(R$)</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;77-135-37-91<br>73.2-34.4-71.1-98.0<br>26.7-5.6-1.8-12.7<br>0.028-41/0.001-11<br>267-275-254-254<br>262-262-260-260</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;80-163-41-103<br>72.3-58.8-66.7-96.0<br>29.1-17.3-1.6-7.3<br>0.016-30/0.001-7<br>251-251-251-251<br>244-244-244-244</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;92-113-48-99<br>71.5-81.0-63.0-94.2<br>17.6-7.7-1.6-4.3<br>0.027-30/0.001-11<br>239-239-239-239<br>230-230-230-230</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;72-57-54-96<br>68.1-82.7-58.1-89.5<br>16.3-7.5-1.6-2.7<br>0.043-36/0.001-22<br>143-143-143-143<br>239-239-239-239</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;59-27-51-74<br>62.6-75.2-53.4-80.2<br>13.1-6.0-1.5-1.7<br>0.035-30/0.001-29<br>236-235-236-236<br>357-357-357-357</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;55-26-77-80<br>55.7-52.7-47.5-65.8<br>16.0-2.2-3.5-3.0<br>0.041-38/0.001-15<br>332-332-332-332<br>476-476-476-476</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;63-55-34-40<br>49.8-38.6-46.2-56.1<br>13.5-5.3-3.8-2.9<br>0.052-42/0.001-30<br>452-453-432-432<br>679-688-614-679</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                </tr>
                <tr style="height:15.0pt">
                    <td style="width:49.0pt; border:solid windowtext 1.0pt; border-top:none; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;28/04-ENA:2021_id25124</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;ENA-Mês(%)<br>EAR(%)<br>ENA-S1(GW)<br>GAP-IT(NW/DC)<br>PLD
                                NW(R$)<br>PLD DC(R$)</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;71-75-35-86<br>73.2-34.4-71.1-98.0<br>26.4-5.4-1.7-12.5<br>0.028-41/0.001-8<br>267-275-254-254<br>312-313-308-308</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;69-54-41-89<br>71.4-41.0-66.3-95.8<br>19.9-12.0-1.5-6.1<br>0.029-30/0.001-33<br>305-305-304-304<br>339-339-339-339</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;69-38-48-91<br>68.1-44.6-62.6-94.2<br>16.6-4.8-1.5-3.9<br>0.02-30/0.001-38<br>367-367-367-367<br>458-459-457-457</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;62-77-54-86<br>61.6-39.0-56.1-89.0<br>13.1-3.0-1.6-2.3<br>0.09-38/0.001-20<br>328-328-328-328<br>367-367-366-366</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;108-93-70-83<br>56.4-39.6-49.0-79.5<br>12.3-9.7-1.5-1.7<br>0.056-30/0.001-15<br>355-355-355-355<br>269-269-269-269</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;82-58-82-116<br>56.1-50.2-46.6-65.6<br>25.9-7.1-3.4-3.0<br>0.046-30/0.001-11<br>225-225-225-225<br>334-334-333-334</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;73-34-72-124<br>55.5-47.2-46.5-56.2<br>27.2-3.6-5.4-5.9<br>0.047-47/0.001-28<br>251-251-250-250<br>374-392-349-370</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                </tr>
                <tr style="height:15.0pt">
                    <td style="width:49.0pt; border:solid windowtext 1.0pt; border-top:none; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;28/04-ENA:2022_id25125</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;ENA-Mês(%)<br>EAR(%)<br>ENA-S1(GW)<br>GAP-IT(NW/DC)<br>PLD
                                NW(R$)<br>PLD DC(R$)</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;81-280-36-73<br>73.2-34.4-71.1-98.0<br>29.3-36.5-1.8-10.5<br>0.028-41/0.001-24<br>267-275-254-254<br>28-28-28-28</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;73-77-41-79<br>73.5-88.2-66.6-96.0<br>21.4-15.0-1.5-5.5<br>0.018-30/0.0-7<br>128-128-128-128<br>219-219-219-219</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;82-124-48-81<br>71.3-85.9-61.4-94.2<br>17.1-5.6-1.6-3.5<br>0.019-30/0.001-12<br>264-264-264-264<br>267-267-267-267</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;82-76-55-77<br>67.6-86.7-56.5-89.6<br>14.8-7.3-1.6-2.1<br>0.032-30/0.0-29<br>180-180-180-180<br>216-216-216-216</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;85-126-50-62<br>62.8-78.6-52.2-80.5<br>23.8-9.5-1.7-1.6<br>0.025-30/0.001-25<br>210-209-210-210<br>207-207-207-207</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;62-68-54-118<br>60.0-87.1-45.4-66.9<br>22.7-10.3-2.1-4.5<br>0.025-30/0.001-12<br>141-141-141-141<br>204-204-204-204</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;72-66-56-141<br>55.3-72.9-45.8-56.2<br>21.1-5.4-4.2-11.7<br>0.081-30/0.001-16<br>174-174-174-174<br>211-211-211-211</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                </tr>
                <tr style="height:15.0pt">
                    <td style="width:49.0pt; border:solid windowtext 1.0pt; border-top:none; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;28/04-ENA:2023_id25126</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;ENA-Mês(%)<br>EAR(%)<br>ENA-S1(GW)<br>GAP-IT(NW/DC)<br>PLD
                                NW(R$)<br>PLD DC(R$)</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;78-60-35-80<br>73.2-34.4-71.1-98.0<br>27.3-2.9-1.7-11.4<br>0.028-41/0.001-8<br>267-275-254-254<br>311-314-308-308</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;72-117-41-82<br>72.0-40.1-66.3-95.7<br>20.9-6.2-1.5-5.8<br>0.023-30/0.0-10<br>311-311-310-310<br>328-328-328-328</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;74-75-48-79<br>69.9-60.1-62.6-94.0<br>16.8-6.3-1.5-3.5<br>0.047-30/0.001-36<br>316-316-316-316<br>346-346-346-346</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;70-104-56-77<br>65.0-54.4-57.6-88.8<br>14.7-12.6-1.6-2.2<br>0.074-44/0.001-35<br>278-278-278-278<br>302-302-302-302</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;82-249-54-54<br>60.6-57.2-51.0-79.3<br>14.8-10.8-1.5-1.4<br>0.032-30/0.0-15<br>275-275-275-275<br>213-213-213-213</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;61-376-43-38<br>58.6-93.0-45.9-66.0<br>27.7-48.2-2.0-1.2<br>0.081-30/0.001-21<br>164-164-164-164<br>236-235-236-236</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:white; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;35-130-17-16<br>54.9-96.1-44.0-56.1<br>18.8-17.7-1.6-1.3<br>0.051-30/0.001-19<br>165-165-165-165<br>493-493-493-493</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                </tr>
                <tr style="height:15.0pt">
                    <td style="width:49.0pt; border:solid windowtext 1.0pt; border-top:none; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;28/04-ENA:2024_id25127</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;ENA-Mês(%)<br>EAR(%)<br>ENA-S1(GW)<br>GAP-IT(NW/DC)<br>PLD
                                NW(R$)<br>PLD DC(R$)</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;68-125-34-76<br>73.2-34.4-71.1-98.0<br>25.4-11.4-1.6-10.8<br>0.028-41/0.001-7<br>267-275-254-254<br>305-306-305-305</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;69-149-40-80<br>71.3-56.2-66.2-95.5<br>19.1-13.7-1.5-5.6<br>0.053-30/0.001-11<br>288-288-288-288<br>292-292-292-292</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;69-62-47-78<br>69.4-79.1-62.6-93.8<br>15.8-10.1-1.5-3.4<br>0.038-30/0.001-28<br>277-277-277-277<br>331-331-331-331</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;61-59-53-74<br>64.2-68.6-57.3-88.7<br>12.6-4.2-1.5-2.0<br>0.067-50/0.001-22<br>271-271-271-271<br>332-332-332-332</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;64-66-56-61<br>59.1-59.5-51.5-79.4<br>11.1-8.0-1.5-1.4<br>0.089-30/0.001-13<br>291-291-291-291<br>345-345-345-345</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;87-53-59-48<br>54.1-56.3-44.9-65.0<br>20.5-5.4-2.7-1.4<br>0.068-32/0.001-31<br>348-348-348-348<br>400-400-400-400</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                    <td style="width:49.0pt; border-top:none; border-left:none; border-bottom:solid windowtext 1.0pt; border-right:solid windowtext 1.0pt; background:#DAC2DA; padding:0cm 3.5pt 0cm 3.5pt; height:15.0pt; border-color:currentcolor windowtext windowtext; background-attachment:scroll"
                        valign="bottom" nowrap="" width="65">
                        <p style="text-align:center" align="center" class="x_MsoNormal"><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif; color:black">&nbsp;77-144-34-36<br>53.5-44.1-45.1-54.6<br>25.6-4.4-4.4-2.0<br>0.043-42/0.001-16<br>376-376-375-375<br>422-422-422-422</span><span
                                style="font-size:11.0pt; font-family:&quot;Calibri&quot;,sans-serif"></span></p>
                    </td>
                </tr>
            </tbody>
        </table>
    </div>
    <p style="margin-bottom:12.0pt; text-align:center" align="center" class="x_MsoNormal"><br><br></p>
    <p style="margin-bottom:12.0pt; text-align:center" align="center" class="x_MsoNormal" aria-hidden="true">&nbsp;</p>
                      """
                      )