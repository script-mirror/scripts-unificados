# -*- coding: utf-8 -*-
import os
import pdb
import sys
import locale
import datetime
import requests as req
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")),'.env'))
PATH_PROJETO = os.getenv("PATH_PROJETO", "/WX2TB/Documentos/fontes/PMO")
sys.path.insert(1,"/WX2TB/Documentos/fontes/")

sys.path.insert(1, f"{PATH_PROJETO}/scripts_unificados")
from apps.gerarProdutos.libs import rz_aux_libs ,rz_produtos_chuva
from apps.gerarProdutos.libs.html_to_image import api_html_to_image
from bibliotecas import  wx_dbLib
from apps.gerarProdutos.config import (WHATSAPP_API)
from middle.utils import Constants
constants = Constants()

diretorioApp = os.path.dirname(os.path.abspath(__file__))
pathArquivos = os.path.join(diretorioApp,'arquivos')

path_fontes = "/WX2TB/Documentos/fontes"
PATH_WEBHOOK_TMP = os.path.join(path_fontes,"PMO","scripts_unificados","apps","webhook","arquivos","tmp")
PATH_CV = os.path.abspath("/WX2TB/Documentos/chuva-vazao")

URL_COGNITO = os.getenv('URL_COGNITO')
CONFIG_COGNITO = os.getenv('CONFIG_COGNITO')

def get_access_token() -> str:
    response = req.post(
        URL_COGNITO,
        data=CONFIG_COGNITO,
        headers={'Content-Type': 'application/x-www-form-urlencoded'}
    )
    return response.json()['access_token']

def send_whatsapp_message(destinatario,msg,file=None):  
    fields={
        "destinatario": destinatario,
        "mensagem": msg,
    }
    files={}
    if file:
        files={
            "arquivo": (os.path.basename(file), open(file, "rb"))
        }
    response = req.post(
        WHATSAPP_API,
        data=fields,
        files=files,
        headers={
            'Authorization': f'Bearer {get_access_token()}'
            }
        )

    print("Status Code:", response.status_code)


try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
except:
    locale.setlocale(locale.LC_ALL, '')

class Resultado():

    def __init__(self, parametros):
        # Configurações de e-mail
        self.flagEmail = parametros.get('flagEmail', False)
        self.destinatarioEmail = parametros.get('destinatarioEmail')
        self.remetenteEmail = parametros.get('remetenteEmail')
        self.assuntoEmail = parametros.get('assuntoEmail')
        self.corpoEmail = parametros.get('corpoEmail')
        self.fileEmail = parametros.get('file')
        self.passwordEmail = None

        # Configurações de WhatsApp
        self.flagWhats = parametros.get('flagWhats', False)
        self.fileWhats = parametros.get('fileWhats')
        self.msgWhats = parametros.get('msgWhats')
        self.destinatarioWhats = parametros.get('destinatarioWhats')
        self.file =  None
        

def processar_produto_ACOMPH(parametros):

    resultado = Resultado(parametros)

    dtReferente = parametros["data"]
    path_file = parametros.get("path")

    if not parametros.get("path"):
        path_file = os.path.join(PATH_WEBHOOK_TMP,"Acomph","ACOMPH_{}.xls".format(dtReferente.strftime('%d.%m.%Y')))

    grafico_acomph = rz_aux_libs.gerarGraficoAcomph(dt=dtReferente)
    tabela_acomph = rz_aux_libs.gerarTabelasEmailAcomph(dtReferente)

    resultado.file = [grafico_acomph, path_file]
    resultado.corpoEmail = tabela_acomph
    resultado.remetenteEmail = 'acomph@climenergy.com'
    resultado.assuntoEmail = '[ACOMPH] Atualização do dia {}'.format(dtReferente.strftime('%d/%m/%Y'))
    resultado.flagEmail = True
    
    resultado.fileWhats = grafico_acomph
    resultado.msgWhats = 'Acomph ({})'.format(dtReferente.strftime('%d/%m/%Y'))
    resultado.flagWhats = True
    
    
    return resultado

def split_html_table(html_content):
    """Split HTML table content while preserving structure"""
    # Create BeautifulSoup object to parse HTML
    from bs4 import BeautifulSoup
    
    soup = BeautifulSoup(html_content, 'html.parser')
    tables = soup.find_all('table')
    
    html_chunks = []
    for table in tables:
        # Preserve the style
        style = soup.find('style')
        # Create new HTML document for each table
        new_html = f"""
        <!DOCTYPE html>
        <html>
        <head>
        {style if style else ''}
        </head>
        <body>
        {str(table)}
        </body>
        </html>
        """
        html_chunks.append(new_html)
    
    return html_chunks

def processar_produto_ACOMPH_tabelas_whatsapp(parametros):
    resultado = Resultado(parametros)
    
    dtReferente = parametros["data"]
    html_tabela_acomph = rz_aux_libs.gerarTabelasEmailAcomph(dtReferente)
    
    # Split the HTML content
    html_chunks = split_html_table(html_tabela_acomph)
    titles = ['Submercado', 'Bacias', 'Submercado (D-1)', 'Bacias (D-1)']
    
    # Process each chunk
    for i, html_chunk in enumerate(html_chunks):
        path_fig = api_html_to_image(
            html_chunk,
            path_save=os.path.join(PATH_WEBHOOK_TMP, f'acomph_{datetime.date.today()}_{i}.png')
        )
        
        msg = f'Acomph ({dtReferente.strftime("%d/%m/%Y")}) - {titles[i]}'
        send_whatsapp_message('Condicao Hidrica', msg, path_fig)
    
    return resultado
       
def processar_produto_RDH(parametros):
    resultado = Resultado(parametros)
    dtReferente = parametros["data"]
    resultado.file =  parametros.get("path")

    if not parametros.get("path"):
        path_file = os.path.join(PATH_WEBHOOK_TMP,"RDH","RDH_{}.xlsx".format(dtReferente.strftime('%d%b%Y')))
        resultado.file = path_file

    resultado.remetenteEmail = 'rdh@climenergy.com'
    resultado.assuntoEmail = '[RDH] Atualização do dia {}'.format(dtReferente.strftime('%d/%m/%Y'))
    resultado.flagEmail = True
    return resultado

def remover_primeira_div(html_string):
    inicio_div = html_string.find('<div>')
    fim_div = html_string.find('</div>') + 6  # +6 para incluir o </div>
    
    if inicio_div != -1 and fim_div != -1:
        return html_string[:inicio_div] + html_string[fim_div:], html_string[inicio_div:fim_div]
    else:
        return html_string, f"{datetime.datetime.now()}"
    
def processar_produto_RELATORIO_BBCE(parametros):
    resultado = Resultado(parametros)
    resultado.corpoEmail, resultado.file = rz_aux_libs.geraRelatorioBbce(parametros["data"])
    resultado.assuntoEmail = '[BBCE] Resumo das negociações do dia {}'.format(parametros["data"].strftime('%d/%m/%Y'))
    resultado.remetenteEmail = 'info_bbce@climenergy.com'
    resultado.flagEmail = True
    
    res = req.get(f"{constants.BASE_URL}/api/v2/bbce/produtos-interesse/html?data={parametros['data'].date()}&tipo_negociacao=Boleta Eletronica",
                  headers={
            'Authorization': f'Bearer {get_access_token()}'
            })
    html = remover_primeira_div(res.json()['html'])
    msg = html[1].replace("  ", "").replace("<h3>", "").replace("</h3>", "").replace("<div>", "").replace("</div>", "")[1:][:-1]
    send_whatsapp_message('bbce',msg, api_html_to_image(html[0]))
    
    res = req.get(f"{constants.BASE_URL}/api/v2/bbce/produtos-interesse/html?data={parametros['data'].date()}&tipo_negociacao=Mesa",
                  headers={
            'Authorization': f'Bearer {get_access_token()}'
            })
    html = remover_primeira_div(res.json()['html'])
    msg = html[1].replace("  ", "").replace("<h3>", "").replace("</h3>", "").replace("<div>", "").replace("</div>", "")[1:][:-1]
    send_whatsapp_message('bbce',msg, api_html_to_image(html[0]))
    
    return resultado

def processar_produto_RESULTADO_DESSEM(parametros):
    resultado = Resultado(parametros)
    resultado.corpoEmail, resultado.file = rz_aux_libs.gerarResultadoDessem(parametros["data"])
    resultado.assuntoEmail = '[DESSEM] Rodada DESSEM {}'.format(parametros["data"].strftime('%d/%m/%Y'))
    resultado.remetenteEmail = 'dessem@climenergy.com'
    resultado.destinatarioEmail = ['middle@wxe.com.br', 'front@wxe.com.br']
    resultado.flagEmail = True

    resultado.msgWhats = 'CMO {}'.format(parametros["data"].strftime('%d/%m/%Y'))
    resultado.fileWhats = resultado.file
    resultado.destinatarioWhats = 'RZ - DESSEM'
    resultado.flagWhats = True

    return resultado

def processar_produto_PREVISAO_CARGA_DESSEM(parametros):
    resultado = Resultado(parametros)
    pathFileOut = os.path.join(pathArquivos, 'tmp')
    file_aux, flag_preliminar = wx_dbLib.carga_dessem_plot(parametros["data"], pathFileOut)

    if flag_preliminar:
        resultado.msgWhats = 'Carga Horária Preliminar {}'.format(parametros["data"].strftime('%d/%m/%Y'))
    else:
        resultado.msgWhats = 'Carga Horária {}'.format(parametros["data"].strftime('%d/%m/%Y'))

    resultado.fileWhats = file_aux
    resultado.destinatarioWhats = 'RZ - DESSEM'
    resultado.flagWhats = True

    return resultado

def processar_produto_SITUACAO_RESERVATORIOS(parametros):
    resultado = Resultado(parametros)

    template,path_fig = rz_produtos_chuva.produto_situacao_reservatorios()

    dtAtualizacao = datetime.datetime.now() - datetime.timedelta(days=1)
		
    resultado.fileWhats = path_fig
    resultado.msgWhats = f"Situação dos Reservatórios ({dtAtualizacao.strftime('%d/%m/%Y')})"
    resultado.flagWhats = True

    return resultado

def processar_produto_FSARH(parametros):
    resultado = Resultado(parametros)
    data:datetime.date = parametros['data']
    titulo = parametros['titulo']
    html = parametros['html']
    
    path_fig = api_html_to_image(html,path_save=os.path.join(PATH_WEBHOOK_TMP,f'prev_ena_consistido_{data}.png'))
    
    resultado.fileWhats = path_fig

    resultado.msgWhats = titulo
    resultado.flagWhats = True
    resultado.destinatarioWhats = "FSARH"
    
    return resultado
    
if __name__ == '__main__':

    
    processar_produto_RELATORIO_BBCE({'data':datetime.datetime.now()})