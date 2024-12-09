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
from PMO.scripts_unificados.apps.bot_whatsapp.whatsapp_bot import WhatsappBot

WHATSAPP_API = os.getenv('WHATSAPP_API')


PATH_BOT_WHATS = "/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/bot_whatsapp"

PRODUTCS_MAPPING = {

        'SST': "processar_produto_SST",
        'PRECIPTACAO_MODELO': "processar_produto_PRECIPTACAO_MODELO",
        'IPDO': "processar_produto_IPDO",
        'ACOMPH': "processar_produto_ACOMPH",
        'RDH': "processar_produto_RDH",
        'RELATORIO_BBCE': "processar_produto_RELATORIO_BBCE",
        'RESULTADO_DESSEM': "processar_produto_RESULTADO_DESSEM",
        'PREVISAO_ENA_SUBMERCADO': "processar_produto_PREVISAO_ENA_SUBMERCADO",
        'PREVISAO_CARGA_DESSEM': "processar_produto_PREVISAO_CARGA_DESSEM",
        'RESULTADO_CV': "processar_produto_RESULTADO_CV",
        'RELATORIO_CV': "processar_produto_RELATORIO_CV",
        'DIFERENCA_CV': "processar_produto_DIFERENCA_CV",
        'RESULTADO_PREVS': "processar_produto_RESULTADO_PREVS",
        'RESULTADOS_PROSPEC': "processar_produto_RESULTADOS_PROSPEC",
        'PREVISAO_GEADA': "processar_produto_PREVISAO_GEADA",
        'REVISAO_CARGA': "processar_produto_REVISAO_CARGA",
        'CMO_DC_PRELIMINAR': "processar_produto_CMO_DC_PRELIMINAR",
        'GERA_DIFCHUVA': "processar_produto_GERA_DIFCHUVA",
        'REVISAO_CARGA_NW': "processar_produto_REVISAO_CARGA_NW",
        'PSATH_DIFF': "processar_produto_PSATH_DIFF",
        'SITUACAO_RESERVATORIOS': "processar_produto_SITUACAO_RESERVATORIOS",
        'REVISAO_CARGA_NW_PRELIMINAR': "processar_produto_REVISAO_CARGA_NW_preliminar",
    } 

class GerardorProdutos(WhatsappBot):

    def __init__(self) -> None:
        self.num_whatsapp = os.getenv('NUM_GILSEU')
        WhatsappBot.__init__(self)
        # pass

    def enviar(self,parametros):

        # confgs = configs.getConfiguration()


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
                    
            if parametros['produto'] == 'REVISAO_CARGA_NW':
                for file in resultado.file:
                    self.inserirMsgFila(self.num_whatsapp, "", file)
            elif parametros['produto'] == 'REVISAO_CARGA_NW_PRELIMINAR':
                for file in resultado.file:
                    self.inserirMsgFila(self.num_whatsapp, "PRELIMINAR", file)
                    
            if resultado.flagWhats:

                if not resultado.destinatarioWhats:
                    resultado.destinatarioWhats = 'WX - Meteorologia'

                if not resultado.msgWhats:
                    resultado.msgWhats = f'{produto} ({data_str})'
                    
                fields={
                    "destinatario": resultado.destinatarioWhats,
                    "mensagem": resultado.msgWhats,
                }
                files={}
                if resultado.fileWhats:
                    files={
                        "arquivo": (os.path.basename(resultado.fileWhats), open(resultado.fileWhats, "rb"))
                    }

                response = requests.post(WHATSAPP_API, data=fields, files=files)
                print("Status Code:", response.status_code)

                if response.status_code != 201:
                    self.inserirMsgFila(resultado.destinatarioWhats, resultado.msgWhats, resultado.fileWhats)

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
        parser.add_argument('--nomerodadaoriginal', type=str, help='Descrição do parâmetro')
        parser.add_argument('--considerarprevs', type=str, help='Descrição do parâmetro')
        parser.add_argument('--considerarrv', type=str, help='Descrição do parâmetro')
        parser.add_argument('--gerarmatriz', type=int, help='Descrição do parâmetro')
        parser.add_argument('--fazer_media', type=float, help='Descrição do parâmetro')
        parser.add_argument('--enviar_whats', type=bool, help='Descrição do parâmetro')
        parser.add_argument('--teste_user', type=str, help='Coloque o nome \'thiago\' ou \'joao\'')

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




