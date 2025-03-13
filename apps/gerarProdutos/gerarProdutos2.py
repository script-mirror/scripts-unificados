# -*- coding: utf-8 -*-
import os
import pdb
import sys
import locale
import argparse
import datetime
import requests
from dotenv import load_dotenv
from smtplib import SMTPAuthenticationError
from pandas.plotting import register_matplotlib_converters
load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")),'.env'))

register_matplotlib_converters()

try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
except:
    locale.setlocale(locale.LC_ALL, '')


sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.apps.gerarProdutos import rz_processar_produtos
from PMO.scripts_unificados.bibliotecas import wx_emailSender,wx_opweek
from PMO.scripts_unificados.apps.gerarProdutos.utils import get_access_token
from PMO.scripts_unificados.apps.gerarProdutos.config import (
    PRODUTCS_MAPPING,
    WHATSAPP_API,
    NUM_GILSEU
    )


class GerardorProdutos():

    def __init__(self) -> None:
        self.num_whatsapp = NUM_GILSEU

        # pass

    def send_whatsapp_message(self, destinatarioWhats,msgWhats,fileWhats=None):  
        fields={
            "destinatario": destinatarioWhats,
            "mensagem": msgWhats,
        }
        files={}
        if fileWhats:
            files={
                "arquivo": (os.path.basename(fileWhats), open(fileWhats, "rb"))
            }
        response = requests.post(
            WHATSAPP_API,
            data=fields,
            files=files,
            headers={
                'Authorization': f'Bearer {get_access_token()}'
                }
            )
        print("Status Code:", response.status_code)


    def enviar(self,parametros):

        dt = datetime.datetime.now()
        print("Tentativa as {}".format(dt.strftime('%d/%m/%Y %H:%M:%S')))

        # Dicionário de produtos e funções correspondentes

        produto = parametros.get("produto")
        data = parametros.get("data")

        if not data:
            parametros['data'] = dt
            data = dt
            
        produto = produto.upper()
        data_str = data.strftime("%d/%m/%Y")

        funcao_processamento = PRODUTCS_MAPPING.get(produto,None)

        if funcao_processamento:
            execute_function = getattr(rz_processar_produtos,funcao_processamento)
            resultado = execute_function(parametros)

            #TESTES
            if parametros.get("teste_user") == 'joao':
                resultado.destinatarioWhats='11999029326'
                self.num_whatsapp = '11999029326'
                resultado.destinatarioEmail = ['joao.filho4@raizen.com']
            elif parametros.get("teste_user") == 'jose':
                resultado.destinatarioWhats='11968606707'
                resultado.destinatarioEmail = ['jose.flores@raizen.com']
            elif parametros.get("teste_user") == 'arthur':
                resultado.destinatarioWhats='11945892210'
                resultado.destinatarioEmail = ['arthur.moraes@raizen.com']
            elif parametros.get("teste_user") == 'tenorio':
                resultado.destinatarioWhats='21981155829'
                resultado.destinatarioEmail = ['rodrigo.tenorio@raizen.com']
                
            if resultado.flagEmail:

                if resultado.file and type(resultado.file) == str:
                    resultado.file = [resultado.file]


                serv_email = wx_emailSender.WxEmail()
                serv_email.username = resultado.remetenteEmail if resultado.remetenteEmail else serv_email.username
                serv_email.password = resultado.passwordEmail if resultado.passwordEmail else serv_email.password
                serv_email.send_to = resultado.destinatarioEmail if resultado.destinatarioEmail else serv_email.send_to

                try:
                    serv_email.sendEmail(
                        texto= resultado.corpoEmail if resultado.corpoEmail else f'<h3>{produto} referente a data {data_str}</h3>',
                        assunto=resultado.assuntoEmail if resultado.assuntoEmail else '',
                        anexos= resultado.file if resultado.file else [] 
                        )
                
                except SMTPAuthenticationError:
                    print("\033[91mErro de Autenticacao\033[0m")

                except Exception as e:
                    print(f"\033[91mErro nao mapeado \033[0m\n{e}\n")
                    
            if resultado.flagWhats:

                if not resultado.destinatarioWhats:
                    resultado.destinatarioWhats = ['WX - Meteorologia']

                if isinstance(resultado.destinatarioWhats,str):
                    resultado.destinatarioWhats= [resultado.destinatarioWhats]

                if not resultado.msgWhats:
                    resultado.msgWhats = f'{produto} ({data_str})'

                # esses produtos enviam para o whatsapp do gilseu, os arquivos enviados no email
                if (parametros['produto'] == 'REVISAO_CARGA_NW') or (parametros['produto'] == 'REVISAO_CARGA_NW_PRELIMINAR'):
                    msgWhats = " " 
                    if parametros['produto'] == 'REVISAO_CARGA_NW_PRELIMINAR': msgWhats="PRELIMINAR"

                    for file in resultado.file:
                        self.send_whatsapp_message(
                            destinatarioWhats=self.num_whatsapp,
                            msgWhats = msgWhats,
                            fileWhats = file
                        )

                for destino in resultado.destinatarioWhats:
                    print(f"Enviando mensagem para {destino}")
                    self.send_whatsapp_message(
                        destinatarioWhats=destino,
                        msgWhats = resultado.msgWhats,
                        fileWhats = resultado.fileWhats
                        )

        else:
            print(
                '''
                \nProduto não cadastrado!
                Cadastre o produto para que  
                ''')

    def printHelper(self):

        data = datetime.datetime.now()
        dt_proxRv = (wx_opweek.getLastSaturday(data) + datetime.timedelta(days=7)).strftime('%d/%m/%Y')
        proxRv = wx_opweek.ElecData(wx_opweek.getLastSaturday(data) + datetime.timedelta(days=7))
        mesRef = datetime.date(data.year, proxRv.mesReferente, 1).strftime('%B').capitalize()

        path_plan_acomph_rdh = os.path.abspath('/WX2TB/Documentos/fontes/PMO/monitora_ONS/plan_acomph_rdh')
        pathArquivosTmp = os.path.abspath('/WX2TB/Documentos/fontes/outros/webhook/arquivos/tmp')

        file_revisaoCargas = f'RV{proxRv.atualRevisao}_PMO_{mesRef}_{proxRv.anoReferente}_carga_semanal.zip'

        fileProspec1 = os.path.abspath('/WX2TB/Documentos/fontes/PMO/API_Prospec/DownloadResults/DC202105_rv1_rv5_MapaEC_Ext_Dia.zip')
        fileProspec2 = os.path.abspath('/WX2TB/Documentos/fontes/PMO/API_Prospec/DownloadResults/DC202107_rv1_rv5_MapaEC_Ext_Dia.zip')
        filesProspec = f'["{fileProspec1}","{fileProspec2}"]'

        deck_decomp = os.path.abspath('/WX2TB/Documentos/fontes/PMO/converte_dc/input')

        print(f'python {sys.argv[0]} --produto SST --data {data.strftime("%d/%m/%Y")}')
        print(f'python {sys.argv[0]} --produto IPDO --data {data.strftime("%d/%m/%Y")}')
        print(f'''python {sys.argv[0]} --produto ACOMPH --path {os.path.join(path_plan_acomph_rdh, f"ACOMPH_{data.strftime('%d.%m.%Y')}.xls")}''')
        print(f'python {sys.argv[0]} --produto RDH --data {data.strftime("%d/%m/%Y")}')
        print(f'python {sys.argv[0]} --produto preciptacao_modelo --data {data.strftime("%d/%m/%Y")} --modelo gefs --rodada 0')
        print(f'python {sys.argv[0]} --produto RELATORIO_BBCE --data {data.strftime("%d/%m/%Y")}')
        print(f'python {sys.argv[0]} --produto RESULTADO_DESSEM --data {(data + datetime.timedelta(days=1)).strftime("%d/%m/%Y")}')
        print(f'python {sys.argv[0]} --produto PREVISAO_ENA_SUBMERCADO --data {(data + datetime.timedelta(days=1)).strftime("%d/%m/%Y")}')
        print(f'python {sys.argv[0]} --produto PREVISAO_CARGA_DESSEM --data {(data + datetime.timedelta(days=1)).strftime("%d/%m/%Y")}')
        print(f'python {sys.argv[0]} --produto DIFERENCA_CV --data {data.strftime("%d/%m/%Y")}')
        print(f'python {sys.argv[0]} --produto RESULTADO_PREVS --data {data.strftime("%d/%m/%Y")} modelo PCONJUNTO rodada 0 dtrev {dt_proxRv} preliminar 0 pdp 0 psat 0')
        print(f'python {sys.argv[0]} --produto ACOMPH --data {data.strftime("%d/%m/%Y")}')
        print(f'python {sys.argv[0]} --produto RESULTADOS_PROSPEC --nomeRodadaOriginal \'["ecmwf_ens-wx1","ecmwf_ens-wx2"]\' --path \'{filesProspec}\' --destinatarioEmail \'["thiago@wxe.com.br","middle@wxe.com.br"]\' --assuntoEmail \'Assunto do email\' --corpoEmail \'<a1>Coloque aqui o corpo do email</a1>\' --considerarPrevs \'[0]\' --fazer_media 0 --enviar_whats 0')
        print(f'python {sys.argv[0]} --produto PREVISAO_GEADA --data {data.strftime("%d/%m/%Y")}')
        print(f'python {sys.argv[0]} --produto REVISAO_CARGA --path {os.path.join(path_plan_acomph_rdh, file_revisaoCargas)}')
        print(f'python {sys.argv[0]} --produto CMO_DC_PRELIMINAR --path {os.path.join(deck_decomp, "PMO_deck_preliminar.zip")}')
        print(f'python {sys.argv[0]} --produto GERA_DIFCHUVA --data {data.strftime("%d/%m/%Y")}')
        print(f'python {sys.argv[0]} --produto REVISAO_CARGA_NW --path {os.path.join(path_plan_acomph_rdh, "RV0_PMO_%B_%Y_carga_mensal.zip")}')
        print(f'python {sys.argv[0]} --produto REVISAO_CARGA_NW_PRELIMINAR --data {data.strftime("%d/%m/%Y")}')
        print(f'python {sys.argv[0]} --produto PSATH_DIFF --path {os.path.join(pathArquivosTmp, "psath_%d%m%Y.zip")}')
        print(f'python {sys.argv[0]} --produto TABELA_WEOL_MENSAL --data {data.strftime("%d/%m/%Y")}')
        print(f'python {sys.argv[0]} --produto TABELA_WEOL_SEMANAL --data {data.strftime("%d/%m/%Y")}')

        exit()


    def runWithParams(self):
        parser = argparse.ArgumentParser(description='Descrição do seu script.')
        parser.add_argument('--produto', type=str, help='Produtos enviados por email e/ou whatsapp')
        parser.add_argument('--data', type=lambda s: datetime.datetime.strptime(s, '%d/%m/%Y'), help='Data referente')
        parser.add_argument('--destinatarioEmail', type=str, help='Descrição do parâmetro')
        parser.add_argument('--assuntoemail', type=str, help='Descrição do parâmetro')
        parser.add_argument('--corpoemail', type=str, help='Descrição do parâmetro')
        parser.add_argument('--modelo', type=str, help='Descrição do parâmetro')
        parser.add_argument('--rodada', type=int, help='Descrição do parâmetro')
        parser.add_argument('--rev', type=int, help='Descrição do parâmetro')
        parser.add_argument('--dtrev', type=lambda s: datetime.datetime.strptime(s, '%d/%m/%Y'), help='Data da revisão')
        parser.add_argument('--preliminar', type=int, help='flag Preliminar')
        parser.add_argument('--pdp', type=int, help='flag pdp')
        parser.add_argument('--psat', type=int, help='flag psat')
        parser.add_argument('--url', type=str, help='Descrição do parâmetro')
        parser.add_argument('--path', type=str, help='Descrição do parâmetro')
        parser.add_argument('--nomeRodadaOriginal', type=str, help='Descrição do parâmetro')
        parser.add_argument('--considerarprevs', type=str, help='Descrição do parâmetro')
        parser.add_argument('--considerarrv', type=str, help='Descrição do parâmetro')
        parser.add_argument('--assuntoEmail', type=str, help='Descrição do parâmetro')
        parser.add_argument('--corpoEmail', type=str, help='Descrição do parâmetro')
        parser.add_argument('--gerarmatriz', type=int, help='Descrição do parâmetro')
        parser.add_argument('--fazer_media', type=float, help='Descrição do parâmetro')
        parser.add_argument('--enviar_whats', type=bool, help='Descrição do parâmetro')
        parser.add_argument('--teste_user', type=str, help='Coloque o nome \'thiago\' ou \'joao\'')
        parser.add_argument('--destinatarioWhats', type=lambda s: eval(s), help='Coloque a lista de destinatários')

        args = parser.parse_args()
        parametros = vars(args)

        print(parametros)

        if not any(parametros.values()):
            self.printHelper()
        else:
            # Lógica adicional de processamento, se necessário
            self.enviar(parametros)


if __name__ == '__main__':
    GerardorProdutos().runWithParams()




