# -*- coding: utf-8 -*-
import os
import pdb
import sys
import time
import locale
import zipfile
import datetime
import tabulate
import pandas as pd
import requests as r

sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.apps.gerarProdutos.libs import wx_resultadosProspec ,wx_previsaoGeadaInmet ,rz_cargaPatamar ,rz_deck_dc_preliminar ,rz_aux_libs ,rz_produtos_chuva, html_to_image
from PMO.scripts_unificados.apps.gerarProdutos.libs.html_to_image import api_html_to_image
from PMO.scripts_unificados.bibliotecas import  wx_dbLib, wx_emailSender, wx_opweek
from PMO.scripts_unificados.apps.rodadas import rz_rodadasModelos
from PMO.scripts_unificados.apps.gerarProdutos.config import (
    __HOST_SERVIDOR,
    __USER_EMAIL_CV,
    __PASSWORD_EMAIL_CV
    )

diretorioApp = os.path.dirname(os.path.abspath(__file__))
pathArquivos = os.path.join(diretorioApp,'arquivos')

path_fontes = "/WX2TB/Documentos/fontes"
PATH_WEBHOOK_TMP = os.path.join(path_fontes,"PMO","scripts_unificados","apps","webhook","arquivos","tmp")
PATH_CV = os.path.abspath("/WX2TB/Documentos/chuva-vazao")



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
        

def processar_produto_SST(parametros):
    resultado = Resultado(parametros)
    for i in range(20):
        if parametros['file'] != '':
            break
        else:
            print("Nova tentativa em 5 minutos")
            time.sleep(60 * 15)
    resultado.flagEmail = True
    resultado.remetenteEmail = 'sst@climenergy.com'
    resultado.assuntoEmail = '[SST] Atualização do dia {}'.format(parametros["data"].strftime('%d/%m/%Y'))
    return resultado


def processar_produto_PRECIPTACAO_MODELO(parametros):
    resultado = Resultado(parametros)
    resultado.fileWhats = rz_aux_libs.gerarCompiladoPrevPrecipt(parametros["data"], parametros["modelo"], parametros["rodada"])

    resultado.destinatarioWhats = 'WX - Meteorologia'
    resultado.msgWhats = '{}_{:0>2}z ({})'.format(parametros["modelo"].upper(), parametros["rodada"], parametros["data"].strftime('%d/%m/%Y'))
    resultado.flagWhats = True
    return resultado


def processar_produto_IPDO(parametros):
    resultado = Resultado(parametros)
    dtReferente = parametros["data"]
    resultado.file = [parametros.get("path")]

    if not parametros.get("path"):
        path_file = os.path.join(PATH_WEBHOOK_TMP,"IPDO (Informativo Preliminar Diário da Operação)","IPDO-{}.pdf".format(dtReferente.strftime('%d-%m-%Y')))
        resultado.file = [path_file]
        
    # resultado.file = ["{}/IPDO-{}.pdf".format(PATH_PLAN_ACOMPH, dtReferente.strftime('%d-%m-%Y'))]
    resultado.remetenteEmail = 'ipdo@climenergy.com'
    resultado.assuntoEmail = '[IPDO] Atualização do dia {}'.format(dtReferente.strftime('%d/%m/%Y'))
    
    resultado.fileWhats = os.path.join(os.path.dirname(parametros["path"]), 'IPDO_{}.jpeg'.format(dtReferente.strftime('%Y%m%d')))
    rz_aux_libs.pdfToJpeg(resultado.file[0], 2, resultado.fileWhats)
    
    resultado.flagWhats = True
    resultado.flagEmail = True

    return resultado


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


def processar_produto_RELATORIO_BBCE(parametros):
    resultado = Resultado(parametros)
    resultado.corpoEmail, resultado.file = rz_aux_libs.geraRelatorioBbce(parametros["data"])
    resultado.assuntoEmail = '[BBCE] Resumo das negociações do dia {}'.format(parametros["data"].strftime('%d/%m/%Y'))
    resultado.remetenteEmail = 'info_bbce@climenergy.com'
    resultado.flagEmail = True
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


def processar_produto_PREVISAO_ENA_SUBMERCADO(parametros):
    resultado = Resultado(parametros)
    resultado.corpoEmail = rz_aux_libs.gerarPrevisaoEnaSubmercado(parametros["data"])
    resultado.assuntoEmail = '[ENAS] DESSEM ONS - {}'.format((parametros["data"] + datetime.timedelta(days=2)).strftime('%d/%m/%Y'))
    resultado.remetenteEmail = 'rev_ena@climenergy.com'
    resultado.destinatarioEmail = ['middle@wxe.com.br', 'front@wxe.com.br']
    resultado.flagEmail = True
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


# def processar_produto_RESULTADO_CV(parametros):
#     resultado = Resultado(parametros)
#     path = wx_resultadosCv.plotResultadosCv(parametros['data'], parametros['modelo'], parametros['rodada'], parametros['rev'], parametros['preliminar'], parametros['pdp'])

#     return resultado


def processar_produto_RELATORIO_CV(parametros):
    resultado = Resultado(parametros)
    pathSaida = rz_aux_libs.gerarRelatoriosResultCv(parametros['data'])
    print(pathSaida)

    return resultado


def processar_produto_DIFERENCA_CV(parametros):
    resultado = Resultado(parametros)
    corpoArquivos = rz_aux_libs.compare_fontes_cv(
        dt_rodada=parametros["data"],
        src_path=os.path.join(PATH_CV,parametros["data"].strftime('%Y%m%d'),'fontes')
        )
    
    resultado.corpoEmail = corpoArquivos[0]
    resultado.file = corpoArquivos[1]
    resultado.assuntoEmail = '[CV] Comparação de resultados {}'.format(parametros["data"].strftime('%d/%m/%Y'))
    resultado.destinatarioEmail = ['middle@wxe.com.br']
    resultado.flagEmail = True

    return resultado


# def processar_produto_RESULTADO_PREVS(parametros):
#     resultado = Resultado(parametros)
#     resultado.fileWhats = rz_aux_libs.gerarPlotPrevivaz(dt=parametros['data'], modelo=parametros['modelo'], hr_rodada=parametros['rodada'], dt_rev=parametros['dtrev'], flagPrel=parametros['preliminar'], flagpdp=parametros['pdp'], flagPsat=parametros['psat'])
#     resultado.msgWhats = 'Resultados de {} - modelo {}'.format(parametros["data"].strftime('%d/%m/%Y'), parametros['modelo'])
#     if parametros['preliminar']:
#         resultado.msgWhats += '.PRELIMINAR'
#     if parametros['pdp']:
#         resultado.msgWhats += '.PDP'
#     resultado.msgWhats += '_r{:0>2}z'.format(parametros['rodada'])
#     resultado.flagWhats = True
#     resultado.destinatarioWhats = 'WX - Meteorologia'

#     return resultado


def processar_produto_RESULTADOS_PROSPEC(parametros):
    resultado = Resultado(parametros)
    corpoE, assuntoE, file, msg_whats = wx_resultadosProspec.gerarEmailResultadosProspec(parametros['path'], parametros['nomerodadaoriginal'], parametros['considerarprevs'], parametros['considerarrv'], parametros['gerarmatriz'], parametros['fazer_media'])

    if corpoE != '':
        resultado.corpoEmail = corpoE
        resultado.remetenteEmail = 'rodadas@climenergy.com'
        resultado.assuntoEmail = assuntoE
        resultado.destinatarioEmail = ['front@wxe.com.br', 'middle@wxe.com.br']
        resultado.flagEmail = True
        
    if msg_whats != '' and parametros['enviar_whats']:
        
        resultado.msgWhats = msg_whats
        resultado.destinatarioWhats = 'PMO'
        resultado.flagWhats = True

    return resultado


def processar_produto_PREVISAO_GEADA(parametros):
    resultado = Resultado(parametros)
    if 'path' in parametros:
        pathSaida = parametros['path']
    else:
        pathSaida = os.path.join(pathArquivos, 'tmp')

    resultado.file = wx_previsaoGeadaInmet.getPrevisaoGeada(dataPrevisao=parametros['data'], pathSaida=pathSaida)

    return resultado


def processar_produto_REVISAO_CARGA(parametros):
    resultado = Resultado(parametros)
    
    diferencaCarga, dataRvAtual, dfCargaAtual_xlsx, dfCargaAtual_txt, difCarga = rz_cargaPatamar.cargaPatamar(parametros['path'])
    
    if not diferencaCarga.empty:
        diferencaCarga.columns = diferencaCarga.columns.str.replace(r'/\d{4}', '', regex=True)  # adicionando filtro para remover qualquer ano da coluna exemplo /2024 será removido


        diferencaCargaPMO = rz_cargaPatamar.cargaPatamarPMO(parametros['path'],diferencaCarga,dfCargaAtual_xlsx,dfCargaAtual_txt,difCarga)
        diferencaCargaPMO.columns = diferencaCargaPMO.columns.str.replace(r'/\d{4}', '', regex=True)  # adicionando filtro para remover qualquer ano da coluna exemplo /2024 será removido
        
        diferencaCarga.rename({'Sistema Interligado Nacional':'SIN'}, inplace=True)
        diferencaCarga.index.name = None
        diferencaCarga = diferencaCarga.loc[['SE','S','NE','N','SIN']]
        
        diferencaCargaPMO.rename({'Sistema Interligado Nacional':'SIN'}, inplace=True)
        diferencaCargaPMO.index.name = None
        diferencaCargaPMO = diferencaCargaPMO.loc[['SE','S','NE','N','SIN']]
        difCargaREV = diferencaCargaPMO - diferencaCarga

        diferencaCarga_estilizada = diferencaCarga.style.format('{:.0f}')
        diferencaCarga_estilizada_PMO = diferencaCargaPMO.style.format('{:.0f}')

        dataRvAnterior = wx_opweek.ElecData(dataRvAtual.data - datetime.timedelta(days=7))
        diferencaCarga_estilizada.set_caption(f"Atualizacao de carga DC (RV{dataRvAtual.atualRevisao} - RV{dataRvAnterior.atualRevisao})")
        diferencaCarga_estilizada_PMO.set_caption(f"Atualizacao de carga DC (RV{dataRvAtual.atualRevisao} - PMO)")

        
        css = '<style type="text/css">'
        css += 'caption {background-color: #781e77;color: white;}'
        css += 'th {background-color: #781e77;color: white;}'
        css += 'table {text-align: center;}'
        css += 'th {min-width: 50px;}'
        css += 'td.col'+str(dataRvAtual.atualRevisao)+' {background: #dac2da}'
        css += '</style>'

        if dataRvAtual.atualRevisao == 0:
            html = diferencaCarga_estilizada.to_html()
            print(f"Será enviado apenas a tabela: RV{dataRvAtual.atualRevisao} - RV{dataRvAnterior.atualRevisao}, a diferença da RV1 para RV0 é igual.")
            resultado.flagEmail = True
            resultado.destinatarioEmail = ['middle@wxe.com.br']
            
            resultado.flagWhats = True
            resultado.destinatarioWhats = 'Modelos'
       
        elif difCargaREV.isin([0, pd.np.nan]).all().all():
            html = diferencaCarga_estilizada.to_html()
            resultado.flagEmail = True
            resultado.destinatarioEmail = ['middle@wxe.com.br', 'front@wxe.com.br']
            resultado.flagWhats = True
            resultado.destinatarioWhats = 'PMO'
        
        else:
            html = diferencaCarga_estilizada.to_html()
            html += '<br>'
            html += diferencaCarga_estilizada_PMO.to_html()

            resultado.flagEmail = True
            resultado.destinatarioEmail = ['middle@wxe.com.br', 'front@wxe.com.br']
            resultado.flagWhats = True
            resultado.destinatarioWhats = 'PMO'

        
        html = html.replace('<style type="text/css">\n</style>\n', css)

        resultado.remetenteEmail = 'rev_carga@climenergy.com'
        resultado.corpoEmail = '<div>'+html+'</div>'
        resultado.assuntoEmail = f'Revisão carga - DC (RV{dataRvAtual.atualRevisao})'
        
        # path_file = os.path.join("/WX2TB/Documentos/fontes/outros/webhook/arquivos/tmp/","Carga por patamar - DECOMP")
        path_saida = os.path.join(PATH_WEBHOOK_TMP,f'revisaoCarga_{dataRvAtual.atualRevisao}.png')
        path_fig = api_html_to_image(html,path_save=os.path.join(PATH_WEBHOOK_TMP,f'revisaoCarga_{dataRvAtual.atualRevisao}.png'))
    
        print(path_saida)
        
        resultado.fileWhats = path_saida

    else:
        print("Email e whats não enviado. DataFrame vazio.")

    return resultado

def processar_produto_CMO_DC_PRELIMINAR(parametros):
    resultado = Resultado(parametros)
    
    assunto, cmo = rz_deck_dc_preliminar.sumario_cmo(parametros['path'])

    html = '<h1>'+assunto+'</h1>'
    html += '<p>Deck do ONS não comentado.</p>'
    resultado.msgWhats = '{}\nDeck do ONS não comentado\n{:->20}\n'.format(assunto,'')
    
    from tabulate import tabulate

    teste=[]
    header=[]
    for submercado in cmo:
        html += '<h3>{}</h3>'.format(submercado)
        html += wx_emailSender.gerarTabela(cmo[submercado][1:], cmo[submercado][0])
        teste.append([submercado,cmo[submercado][4][1]])
    
    resultado.msgWhats += tabulate(teste,tablefmt="plain")

    # resultado.flagEmail = True
    resultado.assuntoEmail = assunto
    resultado.corpoEmail = html
    resultado.remetenteEmail = 'cmo@climenergy.com'
    resultado.destinatarioEmail = ['middle@wxe.com.br', 'front@wxe.com.br']
    
    resultado.flagWhats= True
    resultado.destinatarioWhats = 'PMO'

    return resultado

def processar_produto_GERA_DIFCHUVA(parametros):
    resultado = Resultado(parametros)
    
    data = parametros.get("data")
    dados_modelo1 = parametros.get("dados_modelo1")
    dados_modelo2 = parametros.get("dados_modelo2")

    if not data:
        data = datetime.datetime.now()

    html_template = rz_produtos_chuva.produto_tbmaps_chuva_diff(data,dados_modelo1, dados_modelo2)

    date_today_str = data.strftime('%d/%m/%y')
    data_yesterday_str = (data - datetime.timedelta(1)).strftime('%d/%m/%y')
    
    resultado.flagEmail = True
    resultado.assuntoEmail = f'Dados da diferença das rodadas ({date_today_str} - {data_yesterday_str})'
    resultado.corpoEmail = html_template
    resultado.remetenteEmail = 'mapas@climenergy.com'
    resultado.destinatarioEmail = ['middle@wxe.com.br', 'front@wxe.com.br']
        
    return resultado

def processar_produto_GRAFICO_MODELOS(parametros):
    resultado = Resultado(parametros)
		
    rodada = rz_rodadasModelos.Rodadas(
        modelo=parametros.get('modelo'),
        dt_rodada=parametros.get("data").strftime('%Y-%m-%d'),
        hr_rodada=parametros.get('rodada'),
        flag_pdp=parametros.get('pdp'),
        flag_psat=parametros.get('psat'),
        flag_preliminar=parametros.get('preliminar')
        )
		
    file = rodada.gerar_plot_ena_modelos()

    resultado.fileWhats = file
    resultado.destinatarioWhats = 'WX - Meteorologia'
    resultado.msgWhats = 'ENA {}_{}z ({})'.format(parametros.get('modelo'),str(parametros.get('rodada')).zfill(2),parametros.get("data").strftime('%d/%m/%Y'))
    resultado.flagWhats = True

    return resultado



def processar_produto_REVISAO_CARGA_NW_preliminar(parametros):
    resultado = Resultado(parametros)
    
    data = parametros['data']
    dataAnoMesAtual = data
    print(data)
    
    resposta_cAdic_NW, resposta_Sistema_NW, arquivo_deckUtilizado = rz_cargaPatamar.gerador_arquivos_cAdic_Sistema_DAT(PATH_WEBHOOK_TMP,dataAnoMesAtual)
    print(resposta_cAdic_NW,resposta_Sistema_NW)
    if resposta_cAdic_NW is None or resposta_Sistema_NW is None:
        raise ValueError("Não foi possível gerar os arquivos cAdic e Sistema DAT.")
    
    resultado.file = [resposta_cAdic_NW, resposta_Sistema_NW]
    resultado.flagEmail = True
    resultado.remetenteEmail = 'rev_carga@climenergy.com'
    arquivo_deckUtilizado = f"Arquivos C_ADIC.DAT e SISTEMA.DAT gerados com a carga NEWAVE preliminar da ONS.<br><br>"
    resultado.destinatarioEmail = ['middle@wxe.com.br']

    resultado.flagWhats = True
    
    return resultado





def processar_produto_REVISAO_CARGA_NW(parametros):
    resultado = Resultado(parametros)
    
    path = parametros['path']
    zip_name = os.path.basename(path)
    dir_nomeMesAtual = datetime.datetime.strptime(zip_name, "RV0_PMO_%B_%Y_carga_mensal.zip").strftime('%B_%Y').capitalize()
    dataAnoMesAtual = pd.to_datetime(dir_nomeMesAtual, format="%B_%Y")
    nomeMesAtual = dataAnoMesAtual.strftime("%b%Y").capitalize()

    archive = zipfile.ZipFile(path, 'r')
    xlfile = archive.open(archive.filelist[0].filename)
    columns_to_skip = ['WEEK', 'GAUGE', 'REVISION', 'Base_CGH', 'Base_EOL', 'Base_UFV', 'Base_UTE', 'Base_MMGD', 'LOAD_cMMGD']

    try:
        pmo_mes = pd.read_excel(xlfile, usecols=lambda x: x not in columns_to_skip, sheet_name=1)
    except:
        pmo_mes = pd.read_excel(xlfile, usecols=lambda x: x not in columns_to_skip)

    datas_list = pmo_mes[pmo_mes.DATE >= dataAnoMesAtual].DATE.unique()[:2]

    html_completo_sMMGD = ""
    html_completo_cMMGD = ""
    title = ['', 'DIFF', '%']
    submarket = ['SE', 'S', 'NE', 'N', 'SIN']
    
    resultado.msgWhats = "```\n\n"

    resposta_cAdic_NW,resposta_Sistema_NW,arquivo_deckUtilizado = rz_cargaPatamar.gerador_arquivos_cAdic_Sistema_DAT(PATH_WEBHOOK_TMP,dataAnoMesAtual)
    resultado.file = [resposta_cAdic_NW, resposta_Sistema_NW]
    
    for data in datas_list:
        html_completo_sMMGD += rz_cargaPatamar.gera_tb_pmo_nw(path, data)[0]
        html_completo_cMMGD += rz_cargaPatamar.gera_tb_pmo_nw(path, data)[2]
        lista_sMMGD = rz_cargaPatamar.gera_tb_pmo_nw(path, data)[1][['DIFF', 'PORCENTAGEM']].values.tolist()
        lista_cMMGD = rz_cargaPatamar.gera_tb_pmo_nw(path, data)[3][['DIFF', 'PORCENTAGEM']].values.tolist()

        cmo_cMMGD = [title]
        cmo_sMMGD = [title]

        for i, values in enumerate(lista_sMMGD):
            cmo_sMMGD.append([submarket[i]] + values)

        for i, values in enumerate(lista_cMMGD):
            cmo_cMMGD.append([submarket[i]] + values)

        table_sMMGD = cmo_sMMGD[0], cmo_sMMGD[1], cmo_sMMGD[2], cmo_sMMGD[3], cmo_sMMGD[4], cmo_sMMGD[5]
        table_sMMGD = tabulate.tabulate(table_sMMGD, tablefmt="pretty")

        table_cMMGD = cmo_cMMGD[0], cmo_cMMGD[1], cmo_cMMGD[2], cmo_cMMGD[3], cmo_cMMGD[4], cmo_cMMGD[5]
        table_cMMGD = tabulate.tabulate(table_cMMGD, tablefmt="pretty")

        resultado.msgWhats += f'\n{pd.to_datetime(data).strftime("%b%Y").capitalize()} - carga sem MMGD\n'
        resultado.msgWhats += table_sMMGD

        resultado.msgWhats += f'\n{pd.to_datetime(data).strftime("%b%Y").capitalize()} - carga com MMGD\n'
        resultado.msgWhats += table_cMMGD

    resultado.msgWhats += "```"
    resultado.flagEmail = True
    
    # fazendo o split pois cada html vem com mes atual e antigo e queremos separar cada mes em sua string
    html_completo_sMMGD_split = html_completo_sMMGD.split('<style type')
    html_completo_sMMGD_parte1 = '<style type' + html_completo_sMMGD_split[1]
    html_completo_sMMGD_parte2 = '<style type' + html_completo_sMMGD_split[2]

    html_completo_cMMGD_split = html_completo_cMMGD.split('<style type')
    html_completo_cMMGD_parte1 = '<style type' + html_completo_cMMGD_split[1]
    html_completo_cMMGD_parte2 = '<style type' + html_completo_cMMGD_split[2]
    
    resultado.assuntoEmail = f'Revisão carga PMO-NW-{nomeMesAtual}'
    resultado.corpoEmail = arquivo_deckUtilizado + html_completo_sMMGD_parte1 + html_completo_cMMGD_parte1 + html_completo_sMMGD_parte2 + html_completo_cMMGD_parte2
    resultado.remetenteEmail = 'rev_carga@climenergy.com'
    
    resultado.flagWhats = True
    resultado.destinatarioWhats = 'PMO'
    
    return resultado



def processar_produto_PSATH_DIFF(parametros):
    resultado = Resultado(parametros)

    path_arq = parametros['path']
    templates_unificados = rz_produtos_chuva.produto_psath_diff(path_arq,limiar=1)

    if not templates_unificados:
        resultado.flagEmail = False
        print('Não será enviado o e-mail')
    else:
        resultado.flagEmail = True
        resultado.remetenteEmail = __USER_EMAIL_CV
        resultado.passwordEmail = __PASSWORD_EMAIL_CV

        resultado.corpoEmail = templates_unificados
        data_atual_str = datetime.datetime.today().strftime("%d/%m/%Y")
        data_anterior_str = pd.to_datetime(datetime.datetime.today() - datetime.timedelta(days=1)).strftime("%d/%m/%Y")
        resultado.assuntoEmail = f'Diferença dos arquivos de histórico de precipitação por satélite ({data_atual_str} - {data_anterior_str})'

    return resultado

def processar_produto_SITUACAO_RESERVATORIOS(parametros):
    resultado = Resultado(parametros)

    template,path_fig = rz_produtos_chuva.produto_situacao_reservatorios()

    dtAtualizacao = datetime.datetime.now() - datetime.timedelta(days=1)
		
    resultado.fileWhats = path_fig
    resultado.msgWhats = f"Situação dos Reservatórios ({dtAtualizacao.strftime('%d/%m/%Y')})"
    resultado.flagWhats = True

    return resultado

def processar_produto_TABELA_WEOL_MENSAL(parametros):
    resultado = Resultado(parametros)
    data:datetime.date = parametros['data']
    
    res = r.get(f"http://{__HOST_SERVIDOR}:8000/api/v2/decks/patamares/weighted-average/month/table", params={"dataProduto":str(data), "quantidadeProdutos":15})
    html = res.json()["html"]
    path_fig = api_html_to_image(html,path_save=os.path.join(PATH_WEBHOOK_TMP,f'weol_mensal_{data}.png'))

    resultado.fileWhats = path_fig
    resultado.msgWhats = f"WEOL Mensal ({(data + datetime.timedelta(days=1)).strftime('%d/%m/%Y')})"
    resultado.flagWhats = True
    resultado.destinatarioWhats = "Premissas Preco"

    return resultado


def processar_produto_TABELA_WEOL_SEMANAL(parametros):
    resultado = Resultado(parametros)
    data:datetime.date = parametros['data']
    
    res = r.get(f"http://{__HOST_SERVIDOR}:8000/api/v2/decks/patamares/weighted-average/week/table", params={"dataProduto":str(data), "quantidadeProdutos":15})
    html = res.json()["html"]
    path_fig = api_html_to_image(html,path_save=os.path.join(PATH_WEBHOOK_TMP,f'weol_mensal_{data}.png'))

    resultado.fileWhats = path_fig
    resultado.msgWhats = f"WEOL Semanal ({(data + datetime.timedelta(days=1)).strftime('%d/%m/%Y')})"
    resultado.flagWhats = True
    resultado.destinatarioWhats = "Premissas Preco"

    return resultado

def processar_produto_prev_ena_consistido(parametros):
    resultado = Resultado(parametros)
    data:datetime.date = parametros['data']
    titulo = parametros['titulo']
    html = parametros['html']
    
    path_fig = api_html_to_image(html,path_save=os.path.join(PATH_WEBHOOK_TMP,f'prev_ena_consistido_{data}.png'))
    resultado.fileWhats = path_fig

    resultado.msgWhats = titulo
    resultado.flagWhats = True
    resultado.destinatarioWhats = "Condicao Hidrica"

    return resultado

def processar_produto_MAPA_PSAT(parametros):
    resultado = Resultado(parametros)
    data:datetime.date = parametros['data']

    
    path_fig = f"/WX2TB/Documentos/dados/psat/zgifs/psat_{data.strftime('%Y%m%d')}12z_smap.png"
    resultado.fileWhats = path_fig
    resultado.msgWhats = f"PSAT ({data.strftime('%d/%m/%Y')})"
    resultado.flagWhats = True
    resultado.destinatarioWhats = "Condicao Hidrica"

    return resultado
