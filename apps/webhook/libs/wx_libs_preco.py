### -*- coding: utf-8 -*-

import sys
import pdb
import os 
import glob
import zipfile
from time import sleep
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.utils import formatdate
from email import encoders
import pandas as pd
import numpy as np

sys.path.insert(1, sys.path[0] + "/libs/")
# from wx_dbLib import WxDataB

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")),'.env'))


#EMAIL SERVER
__IP_SMTP_EMAIL = os.getenv('IP_SMTP_EMAIL')
__PORT_EMAIL = os.getenv('PORT_EMAIL')

#FROM
__USER_EMAIL_CLIME = os.getenv('USER_EMAIL_CLIME') 
__PASSWORD_EMAIL_CLIME = os.getenv('PASSWORD_EMAIL_CLIME') 

#TO
__USER_EMAIL_MIDDLE = os.getenv('USER_EMAIL_MIDDLE') 
__USER_EMAIL_FRONT = os.getenv('USER_EMAIL_FRONT')


## definicoes para email
username = __USER_EMAIL_CLIME
password = __PASSWORD_EMAIL_CLIME
send_from = __USER_EMAIL_CLIME
send_to = [__USER_EMAIL_MIDDLE,__USER_EMAIL_FRONT,'leticia@wxe.com.br', 'thiago@wxe.com.br']
server = __IP_SMTP_EMAIL
port = __PORT_EMAIL

def envia_email_wx(send_from,send_to,assunto,texto,anexo,server,port,username,password,isTls=True):

    msg = MIMEMultipart()
    msg['From'] = send_from
    msg['To'] = ', '.join(send_to)
    msg['Date'] = formatdate(localtime = True)
    msg['Subject'] = assunto
    msg.attach(MIMEText(texto,'html'))

    part = MIMEBase('application', "octet-stream")
    part.set_payload(open(anexo, "rb").read())
    encoders.encode_base64(part)
    #part.add_header('Content-Disposition', 'attachment', filename=assunto)
    part.add_header('Content-Disposition', 'attachment', filename=anexo)
    msg.attach(part)


    #context = ssl.SSLContext(ssl.PROTOCOL_SSLv3)
    #SSL connection only working on Python 3+
    smtp = smtplib.SMTP(server, port)
    if isTls:
        smtp.starttls()
    try:
        smtp.login(username,password)
        smtp.sendmail(send_from, send_to, msg.as_string())

        smtp.quit()
    except Exception as e:
        print("Erro ao enviar email: ", e)
        smtp.quit()


def monitora_cmo(arquivo):
    print(arquivo)
    path = '/WX2TB/Documentos/fontes/PMO/monitora_ONS/DECOMP/ultima_preliminar'

    # Primeira descompactacao do arquivo zipado
    try:
        with zipfile.ZipFile(arquivo, 'r') as zip_ref:
            sleep(1)
            zip_ref.extractall(path)
            
    except:
        print("Nao foi possivel dezipar arquivo " + arquivo)

    # Segunda descompactacao para mais 2 arquivos 

    for file in (glob.glob(path + '/*zip')):
        if 'RESULTADOS' not in file:
            assunto = file.split('/')[-1].split('.zip')[0]

        try:
            with zipfile.ZipFile(file, 'r') as zip_ref:
                sleep(1)
                zip_ref.extractall(path)
        except Exception as e:
            print(e)
    
    # Leitura do arquivo SUMARIO, para buscar informacao de interesse 

    try:
        for file in (glob.glob(path + '/SUMARIO.*')):
            f = open(file)
            lines = f.readlines()
            cmo = lines[204:223]

            html = '''<html><head>'''
            html = html + '<br>' + lines[200].split('\n')[0]
            html = html + '<br>' + lines[201].split('\n')[0]
            html = html + '<br>' + lines[202].split('\n')[0]
            html = html + '<br>' + lines[203].split('\n')[0]


            html = html + '''<table> <tr>'''

            for linha in cmo:
                html = html + '<tr>'
                for i,info in enumerate(linha.split()):
                    if i == 0:
                        html = html + '<th><div style="font-size:90%;text-align:left;">'+info+'</div></th>'
                    else:
                        html = html + '<th><div style="font-size:90%;text-align:right;">'+info+'</div></th>'

                    # html = html + ' <td style="color: #24c433">'+str(ena)+'</td>'
                html = html + '  </tr>'
            html = html + ' </table>'
            html = html + lines[224].split('\n')[0]
            html = html + '<br>' + lines[225].split('\n')[0]
            
            # for linha in cmo:
            # 	html = html + '<br>' + linha.split('\n')[0]

            html = html + '</body> </html>'

            # ENVIA E-MAIL com o CMO e anexo
            send_from = 'cmo@climenergy.com'
            send_to = ['middle@wxe.com.br','front@wxe.com.br']
            # send_to = ['edson@wxe.com.br']
            arq = 'PMO_deck_preliminar.zip'
            anexo = arquivo
            texto = "CMO ONS - envio por WX<br>"
            texto = texto + html
            assunto = '[CMO ONS] ' + assunto
            
            envia_email_wx(send_from, send_to, assunto, texto, anexo, server, port, username, password, isTls=True)
            
            for i in glob.glob(path + '/*'):
                os.remove(i)
            # os.remove(arquivo)

            # pdb.set_trace()
    except Exception as e:
        raise e

def nao_consistido_rv(arquivo):
    # print(arquivo)
    rev = arquivo.split('_')[-1].split('.')[0]
    assunto = arquivo.split('/')[-1].split('.')[0]
    path = '/WX2TB/Documentos/fontes/PMO/monitora_ONS/DECOMP/temporarios'

    # Primeira descompactacao do arquivo zipado
    try:
        with zipfile.ZipFile(arquivo, 'r') as zip_ref:
            sleep(1)
            zip_ref.extractall(path)
            
    except:
        print("Nao foi possivel dezipar arquivo " + arquivo)

    for i in glob.glob(path + '/Nao_Consistido/*' + rev + '*'):
        # print(i)
        if rev == 'PMO':
            sheet = 'Tab-5-6-7'
            rows = 10
            skip = 3
            x = 1
        else:
            sheet = 'REV-2'
            rows = 11
            skip = 4
            x = 2

        df = pd.read_excel(i, sheet_name=sheet,nrows = rows-skip+1,skiprows=skip,header=None)
        # pdb.set_trace()
        df = df.dropna(axis=1, how='all')
        df = df.replace(np.nan, '', regex=True)
        df.loc[0][x]= df.loc[0][x].strftime('%d/%m/%Y')
        df.loc[2][x]= df.loc[2][x].strftime('%d/%m/%Y')
        # df = df.style.set_properties(**{'text-align': 'center'})
        htmlfim = df.to_html(index=False,header=False)
        htmlfim = htmlfim.replace('border="1"','class="umaLinha"')

    html = '''<html><head>'''
    html = html + ''' <style type="text/css">'''
    html = html + '''table {font-family: arial, sans-serif;border-collapse: collapse;width: 1000px;}'''
    html = html + '''td, th {border: 1px solid #dddddd;text-align: center;padding: 8px;}'''
    html = html + '''tr:nth-child(4n + 2) td, tr:nth-child(4n + 3) td {background-color: #F2ECEC;}'''
    html = html + '''.umaLinha tr:nth-child(odd) td{ background-color: #F2ECEC;}'''
    html = html + '''.umaLinha tr:nth-child(even) td{ background-color: #ffffff;}'''
    html = html + '''</style></head><body>'''
    html += '<table class="umaLinha" class="dataframe"><tbody><tr>'
    html += '<th rowspan="2">REGIOES</th>'
    if rev != "PMO":
        # pdb.set_trace()

        html += '<th colspan="2">'+df.loc[0][2]+'<br> a <br>'+df.loc[2][2]+'</th>'
    else:
        html += '<th colspan="2">'+df.loc[0][1]+'<br> a <br>'+df.loc[2][1]+'</th>'


    if rev == 'PMO':
        html += '<th colspan="2">Previsao<br>Mensal Inicial<br>'+df.loc[2][3]+'</th></tr>'
        html += '<tr><td>(Mwmed)</td><td>% MLT</td><td>(Mwmed)</td><td>MLT%</td></tr>'
        
    else:
        html += '<th colspan="2">Revisao da<br>Previsao Mensal<br>Incluindo Verificado</th>'
        html += '<th colspan="2">Previsao<br>Mensal Inicial<br>'+df.loc[2][6]+'</th></tr>'
        html += '<tr><td>(Mwmed)</td><td>% MLT</td><td>(Mwmed)</td><td>MLT%</td><td>(Mwmed)</td><td>% MLT</td></tr>'
    

    for i,row in df.iterrows():
        if i >= 4:
            # pdb.set_trace()

            html += '<tr>'
            if rev != 'PMO':
                html += '<td>{}</td>'.format(row[1])
                html += '<td>{:6.0f}</td>'.format(row[2])
                html += '<td>{:6.0f}</td>'.format(row[3])
                html += '<td>{:6.0f}</td>'.format(row[4])
                html += '<td>{:6.0f}</td>'.format(row[5])
                html += '<td>{:6.0f}</td>'.format(row[6])
                html += '<td>{:6.0f}</td>'.format(row[7])
            else:
                html += '<td>{}</td>'.format(row[0])
                html += '<td>{:6.0f}</td>'.format(row[1])
                html += '<td>{:6.0f}</td>'.format(row[2])
                html += '<td>{:6.0f}</td>'.format(row[3])
                html += '<td>{:6.0f}</td>'.format(row[4])

            html += '</tr>'


    html = html + '</tbody></table></body> </html>'

    # ENVIA E-MAIL com o CMO e anexo
    username = 'rev_ena@climenergy.com'
    send_from = 'rev_ena@climenergy.com'

    send_to = ['middle@wxe.com.br','front@wxe.com.br']
    # send_to = ['edson@wxe.com.br']
    arq = i
    anexo = arquivo
    texto = "<b>ENERGIAS NATURAIS AFLUENTES PREVISTAS POR SUBSISTEMA (MWmed)</b><br>"
    texto = texto + "Previsao Semanal ONS - Nao Consistido<br><br>"
    texto = texto + html
    texto = texto + "<br><br>Envio por WX"
    assunto = '[ENAS] REVISAO ONS - ' + assunto
    # pdb.set_trace()
    
    envia_email_wx(send_from, send_to, assunto, texto, anexo, server, port, username, password, isTls=True)
    for i in glob.glob(path + '/Nao_Consistido/*'):
        try:
            os.remove(i)
        except:
            print('Nao foi possivel deletar o arquivo' + i)
    return [assunto, texto]

def dadvaz_pdp(arquivo,str_data):
    sheet = 'Diária_6'
    skip = 4
    lin_SE = 166

    # SUDESTE
    df = pd.read_excel(arquivo, sheet_name=sheet,skiprows = skip)
    colunas = df.columns.tolist()

    cp = df.loc[lin_SE:][colunas[1:]].copy()
    cp['SUB'] = 'SUDESTE'
    df_SE = cp.reset_index(drop=True)

    # OUTROS SUBS
    sheet = 'Diária_7'
    skip = 4
    lin_S  = 55
    lin_NE  = 69
    lin_N = 124
    df = pd.read_excel(arquivo, sheet_name=sheet,skiprows = skip)

    cp = df.loc[lin_S:lin_S+1][colunas[1:]].copy()
    cp['SUB'] = 'SUL'
    df_S = cp.reset_index(drop=True)
    cp = df.loc[lin_NE:lin_NE+1][colunas[1:]].copy()
    cp['SUB'] = 'NORDESTE'
    df_NE = cp.reset_index(drop=True)
    cp = df.loc[lin_N:lin_N+1][colunas[1:]].copy()
    cp['SUB'] = 'NORTE'
    df_N = cp.reset_index(drop=True)
    result = pd.concat([df_SE,df_S,df_NE,df_N])
    result = result.reset_index(drop=True)

    # montando HTML
    nome_sub = ['SUDESTE','SUL','NORDESTE','NORTE']
    html = '''<html><head>'''
    html = html + ''' <style type="text/css">'''
    html = html + '''table {font-family: arial, sans-serif;border-collapse: collapse;width: 1000px;}'''
    html = html + '''td, th {border: 1px solid #dddddd;text-align: center;padding: 8px;}'''
    html = html + '''tr:nth-child(4n + 2) td, tr:nth-child(4n + 3) td {background-color: #F2ECEC;}'''
    html = html + '''.umaLinha tr:nth-child(odd) td{ background-color: #F2ECEC;}'''
    html = html + '''.umaLinha tr:nth-child(even) td{ background-color: #ffffff;}'''
    html = html + '''</style></head><body><table> <tr><th style="font-size:100%;">Submercado<br>'''
    html = html + '''<div style="font-size:70%;"> % MLT </div></th> '''
    for data in colunas[2:]:
        html = html + '      <th style="color: #030303">'+data.strftime('%d/%m/%Y')+'</th>'
    html = html + '  </tr>'

    for i, sub in enumerate(nome_sub):
        # print(i)
        # print(' ')
        html = html + '<tr> <td rowspan="2" style="color: #030303">'+sub+' <br>'
        html = html + '<div style="font-size:70%;"> % MLT </div></td>'

        for ii,cols in enumerate(result):
            if ii > 0 and ii < 8:
                html = html + ' <td style="color: #030303">'+str(round(result.loc[i*2][ii]))+'</td>'
                # print(round(result.loc[i*2][ii]))
                # print(round(result.loc[i*2+1][ii],1))
        for ii,cols in enumerate(result):
            if ii > 0 and ii < 8:
                if ii == 1:
                    html = html + '</tr><tr><td style="color: #030303">' + str(round(result.loc[i*2+1][ii],1)) + '</td>'
                else:
                    html = html + '<td style="color: #030303">' + str(round(result.loc[i*2+1][ii],1)) + '</td>'
        html = html +' </tr>'
    html = html + ' </table>'

    

    # ENVIA E-MAIL com o CMO e anexo
    username = 'rev_ena@climenergy.com'
    send_from = 'rev_ena@climenergy.com'
    assunto = "Data: " + str_data

    send_to = ['middle@wxe.com.br','front@wxe.com.br','leticia@wxe.com.br']
    # send_to = ['edson@wxe.com.br']
    anexo = arquivo
    texto = "<b>Relatório de Previsão de Vazões Diárias</b><br>"
    texto = texto + "ENAS Diárias ONS - DESSEM<br><br>"
    texto = texto + html
    texto = texto + "<br><br>Envio por WX"
    assunto = '[ENAS] DESSEM ONS - ' + assunto
    # pdb.set_trace()
    
    envia_email_wx(send_from, send_to, assunto, texto, anexo, server, port, username, password, isTls=True)
    # pdb.set_trace()



if "__main__" == __name__:
    print('libs preco')
    # path = '/WX2TB/Documentos/fontes/PMO/monitora_ONS/DECOMP/downloads/PMO_deck_preliminar.zip'
    # monitora_cmo(path)
    # path = '/WX2TB/Documentos/fontes/PMO/monitora_ONS/DECOMP/downloads/Nao_Consistido_202006_PMO.zip'
    path = '/WX2TB/Documentos/fontes/PMO/monitora_ONS/DECOMP/downloads/Nao_Consistido_202005_REV4.zip'
    arquivo = '/WX2TB/Documentos/fontes/PMO/monitora_ONS/plan_acomph_rdh/Relatorio_previsao_diaria_03_01_2021_para_05_01_2021.xls'
    data = '03/01/2021'
    # dadvaz_pdp(arquivo,data)
    # nao_consistido_rv(path)

