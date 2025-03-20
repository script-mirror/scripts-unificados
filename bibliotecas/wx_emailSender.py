# -*- coding: utf-8 -*-
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import formatdate
from email import encoders
from pathlib import Path
import uuid

import pdb
import os
import base64

import requests
from PIL import Image
from io import BytesIO


from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")),'.env'))

URL_COGNITO = os.getenv('URL_COGNITO')
CONFIG_COGNITO = os.getenv('CONFIG_COGNITO')
URL_HTML_TO_IMAGE_WAZE = os.getenv('URL_HTML_TO_IMAGE_WAZE')

class WxEmail:
    """Classe para envios de email"""
    def __init__(self):
        #DB MSSQL CONFIGURATION
        __IP_SMTP_EMAIL = os.getenv('IP_SMTP_EMAIL') 
        __PORT_EMAIL = os.getenv('PORT_EMAIL') 
        __USER_EMAIL_CLIME = os.getenv('USER_EMAIL_CLIME') 
        __PASSWORD_EMAIL_CLIME = os.getenv('PASSWORD_EMAIL_CLIME') 

        __USER_EMAIL_MIDDLE = os.getenv('USER_EMAIL_MIDDLE') 
        __USER_EMAIL_FRONT = os.getenv('USER_EMAIL_FRONT') 

        # Servidor pode ser utilizando tanto para emails do dominio da wxenergy quanto da climenergy
        self.server = __IP_SMTP_EMAIL
        self.port = __PORT_EMAIL
        self.isTls = True
        self.username = __USER_EMAIL_CLIME
        self.password = __PASSWORD_EMAIL_CLIME
        self.send_to = [__USER_EMAIL_MIDDLE,__USER_EMAIL_FRONT]

    def sendEmail(self, texto, assunto, anexos=[], send_to=[]):
        """ Funcao para envio de email. 
        :param texto: Mensagem (html) a ser enviada
        :param assunto: Assunto da msg a ser enviada
        :param anexos: [Opcional] Lista de path de arquivos a serem enviados em anexo.
        :param send_to: [Opcional] Parametro opcional, caso a lista de email nao for passada como parametro
        a lista default da classe sera utilizada
        :return None: """

        if send_to == []:
            send_to = self.send_to

        print("Enviando email: {}".format(assunto))
        print("De: {}".format(self.username))
        print("Destinatario(s): {}".format(str(send_to)[1:-1]))
        if len(anexos) > 0:
            for i, anexo in enumerate(anexos):
                print("Anexo {}: {}".format(i, anexo))

        msg = MIMEMultipart()

        msg['From'] = '{} <{}>'.format(self.username.split('@')[0].upper(), self.username)
        msg['To'] = ', '.join(send_to)
        msg['Date'] = formatdate(localtime = True)
        msg['Subject'] = assunto
        msg.attach(MIMEText(texto.encode('utf-8'),'html',_charset='utf-8'))

        for anexo in anexos:
            anexo = Path(anexo)
            part = MIMEBase('application', "octet-stream")
            part.set_payload(open(anexo, "rb").read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', 'attachment', filename=anexo.name)
            msg.attach(part)

        smtp = smtplib.SMTP(self.server, self.port)
        if self.isTls:
            smtp.starttls()

        answer = smtp.login(self.username,self.password)
        answer = smtp.sendmail(self.username, send_to, msg.as_string())
        print("Email enviado")
        smtp.quit()


def gerarTabela(body, header=[], widthColunas=[], nthColor=1):
    """ Gera uma tabela formatada no formato HTML que possa ser interpretada pelo GMAIL
    :param body: [lst] Uma lista no qual cada item da lista e uma linha. A linha e outra lista no qual cada item e uma celula
    :param header: [lst] Lista com as celulas do header
    :param widthColunas: [lst] Lista especificando os tamanhos das colunas
    :param nthColor: [int] Numero de linhas consecutivas da mesma cor
    :return html: [str] Tabela em forma de string formatada em html pronta a ser inserida no corpo do email 
    """

    tamanhoDefaultColunas = 61 #pt
    if widthColunas == [] and header != []:
        for i in range(len(header)+1):
            widthColunas.append(tamanhoDefaultColunas)

    tamanhoTabela = 0
    for tam in widthColunas:
        tamanhoTabela += tam

    html = '<table style="width:{}pt;border-collapse:collapse" width="{}" cellspacing="0" cellpadding="0" border="0"><tbody>\n'.format(int(tamanhoTabela*1.12), int(tamanhoTabela*1.12*1.33))
    if header != []:
        html += '\t<tr style="height: 15pt;">\n'
        for i, cell in enumerate(header):
            html += '\t\t<td style="width: {}pt; border: 1pt solid windowtext; background: rgb(120, 30, 119) none repeat scroll 0% 0%; padding: 0cm 3.5pt; height: 15pt;" width="{}" valign="bottom" nowrap="nowrap">\n'.format(widthColunas[i], int(widthColunas[i]*1.33))
            html += '\t\t<p class="MsoNormal" style="margin: 0cm 0cm 0.0001pt; text-align: center; line-height: normal; font-size: 11pt; font-family: &quot;Calibri&quot;, sans-serif;" align="center"><span style="color: white;">{}<span></span></span></p>\n'.format(cell)
            html += '\t\t</td>\n'
        html += '\t\t</tr>\n'

    corVerde = False
    if body != []:
        for index_linha, linha in enumerate(body):

            # a cada n iteracoes, a cor e alternada entre branco e verde
            if index_linha % nthColor == 0:
                corVerde = not corVerde

            html += '\t<tr style="height: 15pt;">\n'
            for cell in linha:
                # if index_linha%2 == 0:
                if corVerde:
                    html += '\t\t<td style="width:49pt;border-color:currentcolor windowtext windowtext;border-style:none solid solid;border-width:medium 1pt 1pt;background:white none repeat scroll 0% 0%;padding:0cm 3.5pt;height:15pt" width="65" valign="bottom" nowrap="">\n'
                    html += '\t\t<p class="MsoNormal" style="margin:0cm 0cm 0.0001pt;text-align:center;line-height:normal;font-size:11pt;font-family:&quot;Calibri&quot;,sans-serif" align="center"><span style="color:black"><span>&nbsp;</span>{}<span></span></span></p>\n'.format(cell)
                    html += '\t\t</td>\n'

                else:
                    html += '\t\t<td style="width:49pt;border-color:currentcolor windowtext windowtext;border-style:none solid solid;border-width:medium 1pt 1pt;background:rgb(218, 194, 218) none repeat scroll 0% 0%;padding:0cm 3.5pt;height:15pt" width="65" valign="bottom" nowrap="">\n'
                    html += '\t\t<p class="MsoNormal" style="margin:0cm 0cm 0.0001pt;text-align:center;line-height:normal;font-size:11pt;font-family:&quot;Calibri&quot;,sans-serif" align="center"><span style="color:black"><span>&nbsp;</span>{}<span></span></span></p>\n'.format(cell)
                    html += '\t\t</td>\n'

            html += '\t</tr>\n'
    html += '</tbody></table>\n'
    return html

def get_css_df_style(html):
    test2 = html.split('</style>')
    tb = test2[1]
    pp = test2[0].split('<style type="text/css">\n')[1]
    css_final = '''<style type="text/css">\n'''+'''table, th, td {
    border: 1px;
    color: white;
    text-align: center;
    }
    th {
            background-color: rgb(120, 30, 119);
            font-size: 12pt;
            padding:5px;
    }
    td {
            background-color: rgb(218, 194, 218);
            color: black;
            font-size: 15pt;
            padding: 2px;
    }
    caption{
            background-color: rgb(120, 30, 119);
            font-size: 15pt;
            border: 1px;
    }'''+'\n'+pp+'</style><br>'

    template = f"{css_final} {tb}"
    template.replace('\n','')
    return template

def apply_html_table_style(dfs, columns_to_apply=None, format = "{:.1f}"):

    from matplotlib.colors import LinearSegmentedColormap
    cmap=LinearSegmentedColormap.from_list('rg',['r','w','b'], N=256)
    html_string = ''
    for df in dfs:
        diff_styled = df.style.background_gradient(cmap=cmap,axis = None,vmin = -10, vmax = 10,subset=columns_to_apply)
        diff_styled = diff_styled.format(format)
        css = """ <style type="text/css">

                    caption{
                            background-color: rgb(120, 30, 119);
                            font-size: 15pt;
                            border: 1px;
                    }

                    table { 
                    margin: 10px;
                    border: 1px solid black;
                    border-collapse: separate;
                    border-spacing: 1px;
                    }
                    
                    tr {
                        display: table-row;
                        vertical-align: inherit;
                        border-color: inherit;
                    }
                    td {
                        background-color: #fefeff;
                    }
                    th, td {
                        padding: 5px 4px 6px 4px; 
                        text-align: center;
                        align-content: center;
                    }

                    th, td :nth-child(1){
                        background-color: rgb(120, 30, 119);
                        color: white;
                    }

                    .container {
                        display: flex;
                        justify-content: center; /* Centraliza as tabelas horizontalmente */
                        align-items: flex-start; /* Comeca no topo */
                    }

                </style>"""
        
        html_string += diff_styled.to_html()

    html_completo = (css+'<div class="container">'+html_string+"</div>").replace('\n', '').replace('\t', '')

    return html_completo


def get_image_base64(path):
    
    with open(path, "rb") as image_file:
        encoded_string = base64.b64encode(image_file.read()).decode('utf-8')
    return encoded_string



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


from random import randint
def api_html_to_image(html_str,path_save=f'output{randint(0, 9999)}.png'):
    
    # return api_html_to_image_waze(html_str, path_save)

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

if __name__ == '__main__':

    pass
    # Exemplo de como utilizar os
    # serv_email = WxEmail()
    # serv_email.username = 'thiago@wxe.com.br'
    # serv_email.password = 'mud@r123'
    # # serv_email.send_to = ['thiago.scher@gmail.com']
    # serv_email.send_to = ['thiago@wxe.com.br']
    # anexos = ['C:/Users/thiag/Desktop/tmp/20200508/ENA_GFS.PRELIMINAR_r06z.PNG', 'C:/Users/thiag/Desktop/tmp/20200508/smap_nat_GEFS.PRELIMINAR_r06z.txt']

    # html = '<html> <head> <style type="text/css"> table {font-family: arial, sans-serif;border-collapse: collapse;width: 1000px;} td, th {border: 1px solid #dddddd;text-align: center;padding: 8px;} tr:nth-child(4n + 2) td, tr:nth-child(4n + 3)td {background-color: #F2ECEC;} .umaLinha tr:nth-child(odd) td{ background-color: #F2ECEC;} .umaLinha tr:nth-child(even) td{ background-color: #ffffff;} </style> </head> <body> <b>ENERGIAS NATURAIS AFLUENTES PREVISTAS POR SUBSISTEMA (MWmed)</b><br>Previsao Semanal - Nao Consitido<br><br> <table class="umaLinha" class="dataframe"> <tbody> <tr> <td></td> <td>23/05/2020</td> <td></td> <td>Revisão da</td> <td></td> <td>Previsão</td> <td></td> </tr> <tr> <td></td> <td>a</td> <td></td> <td>Previsão Mensal</td> <td></td> <td>Mensal Inicial</td> <td></td> </tr> <tr> <td>REGIÕES</td> <td>29/05/2020</td> <td></td> <td>Incluindo Verificado</td> <td></td> <td>MAIO/2020</td> <td></td> </tr> <tr> <td></td> <td>(Mwmed)</td> <td>% MLT</td> <td>(Mwmed)</td> <td>MLT%</td> <td>(Mwmed)</td> <td>% MLT</td> </tr> <tr> <td>SUDESTE</td> <td>28222.4</td> <td>70.6074</td> <td>31129.2</td> <td>77.8797</td> <td>35249.7</td> <td>88.1886</td> </tr> <tr> <td>SUL</td> <td>1226.79</td> <td>14.3089</td> <td>1038.86</td> <td>12.117</td> <td>1587.7</td> <td>18.5185</td> </tr> <tr> <td>NORDESTE</td> <td>4009.53</td> <td>57.1074</td> <td>5583.8</td> <td>79.5295</td> <td>5526.6</td> <td>78.7149</td> </tr> <tr> <td>NORTE</td> <td>18976</td> <td>92.9322</td> <td>23549.8</td> <td>115.332</td> <td>24938.9</td> <td>122.134</td> </tr> </tbody> </table> <br><br>Envio por WX </body></html>'
    # # serv_email.sendEmail(texto=html, assunto='Teste', anexos=anexos)
    # serv_email.sendEmail(texto=html, assunto='Teste')



# Padrao para tabelas
# <html>
#    <head>
#       <style type="text/css">
# 		table {font-family: arial, sans-serif;border-collapse: collapse;width: 1000px;}
# 		td, th {border: 1px solid #dddddd;text-align: center;padding: 8px;}
#   		tr:nth-child(4n + 2) td, tr:nth-child(4n + 3) td {background-color: #F2ECEC;}
#   		.umaLinha tr:nth-child(odd) td{ background-color: #F2ECEC;}
#   		.umaLinha tr:nth-child(even) td{ background-color: #ffffff;}
#   	  </style>
#    </head>
#    <body>
#       <table class="umaLinha">
# 	      <tr>
# 	         <th style="font-size:80%;">
# 	            Submercado<br>
# 	            <div style="font-size:70%;"> mes anter | mes atual</div>
# 	         </th>
# 	         <th>21/05/2020</th>
# 	         <th>21/05/2020</th>
# 	         <th>21/05/2020</th>
# 	      </tr>

# 	      <tr> 
# 	      	<td>regiao1</td>
# 	      	<td style="color: #24c433">val1</td>
# 	      	<td style="color: #24c433">val2</td>
# 	      	<td style="color: #24c433">val3</td>
# 	      </tr> 

# 	      <tr> 
# 	      	<td>regiao2</td>
# 	      	<td style="color: #24c433">val1</td>
# 	      	<td style="color: #24c433">val2</td>
# 	      	<td style="color: #24c433">val3</td>
# 	      </tr>
#     </table>
# </body>
# </html>
