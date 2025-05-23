# -*- coding: utf-8 -*-
import os
import re
import sys
import pdb
import time
import psutil
import datetime
import numpy as np
import pandas as pd
import sqlalchemy as db
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By


sys.path.insert(1,"/WX2TB/Documentos/fontes")
from PMO.scripts_unificados.bibliotecas import wx_dbClass,wx_emailSender,rz_dir_tools
from PMO.scripts_unificados.apps.verificadores.ons import rz_ons
from PMO.scripts_unificados.apps.verificadores import rz_selenium
from PMO.scripts_unificados.apps.gerarProdutos import gerarProdutos2 

GERAR_PRODUTO = gerarProdutos2.GeradorProdutos()

file_path = os.path.dirname(os.path.abspath(__file__))
path_app = os.path.dirname(file_path)
path_local_tmp = os.path.join(path_app,'tmp')

DIR_TOOLS = rz_dir_tools.DirTools()
PATH_TMP_DOWNLOAD = os.path.join(path_local_tmp,str(time.time()))


timestampFormat = '%Y-%m-%d %H:%M'



def checkAtualizFSARH(selenium):
    """ Faz o check das atualizações de FSARH e faz a insercao no DB
    :param None: 
    :return None: 
    """

    global timestampFormat

    db_ons = wx_dbClass.db_mysql_master('db_ons',connect=True)
    tb_fsarh = db_ons.db_schemas['tb_fsarh']


    subquery_max_modified = db.select(db.func.max(tb_fsarh.c.modified))
    max_modified = db_ons.db_execute(subquery_max_modified).scalar()

    query = db.select(
        tb_fsarh.c.id,
        tb_fsarh.c.modified
    ).where(
        tb_fsarh.c.modified == max_modified
    )
    answer = db_ons.db_execute(query).fetchall()
    ultimaModificacao = answer[0][1]

    sair_loop = 0

    
    url ='https://integracaoagentes.ons.org.br/FSAR-H/SitePages/Exibir_Forms_FSARH.aspx'	
    selenium.get(url)
    time.sleep(20)

    while 'display' not in selenium.find_element(By.XPATH, "//div[@id='loading_loadExibir']").get_attribute('style'):
        time.sleep(1)

    selenium.find_element(By.XPATH, "//input[@id='chkAtivas']").click()
    while 'display' not in selenium.find_element(By.XPATH, "//div[@id='loading_loadExibir']").get_attribute('style'):
        time.sleep(1)

    # Colocar 50 resultados por pagina
    selenium.find_element(By.XPATH, "//select[@name='tblMain_length']/option[@value=50]").click()
    while 'display' not in selenium.find_element(By.XPATH, "//div[@id='loading_loadExibir']").get_attribute('style'):
        time.sleep(1)

    tabela = selenium.find_elements(By.XPATH, "//table[@id='tblMain']/tbody/tr")
    
    # Armazenamento de algumas informacoes de todas alteracoes nao cadastrada no DB
    updates = []
    for linha in tabela:
        if sair_loop:
            break

        colunas = linha.find_elements(By.TAG_NAME, 'td')
        hrModific = colunas[0].get_attribute('innerText')
        hrModific = datetime.datetime.strptime(hrModific,'%d/%m/%Y %H:%M')
        
        if hrModific <= ultimaModificacao:
            sair_loop = 1
            continue

        # idCompleto = colunas[1].get_attribute('innerText')
        idCompleto = colunas[2].get_attribute('innerText')
        tpAtualizacao = colunas[7].get_attribute('innerText')

        idNum = int(re.findall('\d+', idCompleto)[0])

        updates.append([hrModific, idNum, tpAtualizacao])

    # Verificacao de informacoes faltantes das modificacoes
    df_fsarh = pd.DataFrame()
    for update in updates:
        id_fsarh = update[1]
        url = 'https://integracaoagentes.ons.org.br/FSAR-H/Lists/FSARH/DispForm.aspx?ID={}'.format(id_fsarh)
        selenium.get(url)

        df = pd.read_html(selenium.page_source, decimal=',', thousands='.')
        df_tmp = pd.DataFrame(df[0].loc[3:40])
        df_tmp = pd.concat([df_tmp,df[0].loc[[59,65,66]]])

        if df_fsarh.empty:
            df_fsarh = pd.DataFrame(columns=df_tmp[0].to_list())

        df_fsarh.loc[id_fsarh] = df_tmp[1].to_list()
    selenium.quit()
    html = ''
    # Insere no banco e gera a tabela que sera enviada por email das novas informacoes
    if len(df_fsarh)>0:

        ordem = ['TemporalidadeRestricao', 'Agente', 'Bacia', 'Rio', 'ReservatorioRelacionado', 'RestricoesMontante', 'RestricoesJusante']
        ordem += ['CaracteristicaRestricao', 'ClasseRestricao', 'NovoValor', 'ValorVigente', 'DescricaoJustificativa', 'Status']
        ordem += ['CaracteristicaRestricaoPontoControle', 'DataCalculadaInicio', 'DataCalculadaTermino']

        df_fsarh['NovoValor'] = df_fsarh['NovoValor'].apply(pd.to_numeric, errors='coerce')
        df_fsarh['ValorVigente'] = df_fsarh['ValorVigente'].apply(pd.to_numeric, errors='coerce')
        # Formatacao e insercao no banco de dados
        index = 0
        
        val = []
        for i,row in df_fsarh.iterrows():
            row = row.replace({np.nan: None})

            row['DataCalculadaInicio'] = datetime.datetime.strptime(row['DataCalculadaInicio'],'%d/%m/%Y %H:%M')
            row['DataCalculadaInicio'] = row['DataCalculadaInicio'].strftime(timestampFormat)

            row['DataCalculadaTermino'] = datetime.datetime.strptime(row['DataCalculadaTermino'],'%d/%m/%Y %H:%M')
            row['DataCalculadaTermino'] = row['DataCalculadaTermino'].strftime(timestampFormat)


            dt_modifc = updates[index][0] #datetime.datetime.strptime(updates[index][0],'%d/%m/%Y %H:%M')
            dt_modifc = dt_modifc.strftime(timestampFormat)

            values = [row.name, dt_modifc]
            values += list(row[ordem].values)

            val.append(tuple(values))

            print('Tentativa de inserir linha {}'.format(i))
            index += 1

        df_values = pd.DataFrame(val,
                columns = [
                    "id", "modified", "temporalidade_restricao", "agente", "bacia",
                    "rio", "reservatorio_relacionado", "restricoes_montante", "restricoes_jusante",
                    "caracteristica_restricao", "classe_restricao", "novo_valor", "valor_vigente",
                    "justificativa", "status", "caracteristica_restricao_ponto_cont",
                    "data_inic", "data_fim"
                ]
                )

        # datab.executemany(sql, val)
        sql_insert = tb_fsarh.insert().values(df_values.replace({float('nan'): None}).to_dict("records"))
        n_row = db_ons.db_execute(sql_insert).rowcount
        print(f"{n_row} Linhas inseridas na tb_fsarh")

        # Formatacao e geracao da tabela 
        hoje = datetime.datetime.now()
        index = 0
        header = ['ID', 'Data Modificação', 'Data Inicio', 'Data Final', 'Agente', 'Região Hidrográfica', 'Reservatório', 'Restrição', 'Valor', 'Atualização', 'Temporarialidade', 'Status']
        body = []
        for i,row in df_fsarh.iterrows():
            row = row.replace({np.nan: ''})
            cells =['<a href="https://integracaoagentes.ons.org.br/FSAR-H/Lists/FSARH/DispForm.aspx?ID={}">{}</a>'.format(row.name, row.name)]
            cells.append((updates[index][0]).strftime('%d/%m/%Y %H:%M'))
            cells.append(row['DataCalculadaInicio'])
            cells.append(row['DataCalculadaTermino'])
            cells.append(row['Agente'])
            cells.append(row['RegiaoHidrografica'])
            cells.append(row['ReservatorioRelacionado'])
            cells.append(row['Variavel_Restricao_Email'])
            try:
                cells.append(str(round(float(row['NovoValor']),1)).replace('.',','))
            except:
                cells.append(row['NovoValor'])
            cells.append(', '.join([item.split('-')[0].strip() for item in updates[index][2].split(',')]))
            cells.append(row['TemporalidadeRestricao'])
            cells.append(row['Status'])
            body.append(cells)
            index += 1
        
        if len(body)>0:
            html = '<h3>Mudanças FSARH {}  </h3><h4>Sistema de Gestão da Atualização de Restrições Hidráulicas </h4>'.format(hoje.strftime('%d/%m/%Y'))
            html += wx_emailSender.gerarTabela(body, header)
        
    else:
        print('Não há atualizações FSARH!')
    return html


def checkUpdateIntervernc(selenium):
    """ Faz o check das atualizações de intervencoes de usinas no site do ONS criando a tabela 
        HTLM que sera enviada por email
    :param None: 
    :return None: 
    """

    # Remove o arquivo xml caso ele exista
    file_path = os.path.join(PATH_TMP_DOWNLOAD,'T_Intervenções(P).xlsx')
    if (os.path.exists(file_path)):
        os.remove(file_path)

    time.sleep(5)
    url ='https://tableau.ons.org.br/t/ONS_Publico/views/ConsultaIntervenes/Intervenes?%3Aiid=1&%3AisGuestRedirectFromVizportal=y&%3Aembed=y'
    selenium.get(url)


    # Espera que o element de "loading" se encerre para continuar o script
    xpath_loadingGlass = '//div[@id="loadingGlassPane"]'
    elmnt = selenium.find_element(By.XPATH, xpath_loadingGlass)
    while 1:
        if 'display: none;' in elmnt.get_attribute("style"):
            break
        time.sleep(1)

    btn_pauseAtualizacao = selenium.find_element(By.ID, 'updates')

    btn_pauseAtualizacao.click()
    time.sleep(2)

    # Mudanca da data inicial para o dia atual
    hoje = datetime.datetime.now()
    xpath_dt_inicial = '//div[@id="typein_[Parameters].[Parâmetro 1]"]/span/input'
    for i in range(30):
        try:
            # elmnt = selenium.find_element_by_xpath(xpath_dt_inicial)
            elmnt = selenium.find_element(By.XPATH, xpath_dt_inicial)
            break
        except Exception as e:
            time.sleep(2)
    # Remove todos os caracteres (nao funciona a funcao clear do selenium)
    for i in range(23):
        elmnt.send_keys(Keys.BACKSPACE)
    txtInsert = (hoje.strftime("%d/%m/%Y")+" {}").format('00:00:00')
    elmnt.send_keys(txtInsert)
    elmnt.send_keys(Keys.TAB)

    # Espera que o element de "loading" se encerre para continuar o script
    xpath_loadingGlass = '//div[@id="loadingGlassPane"]'
    elmnt = selenium.find_element(By.XPATH, xpath_loadingGlass)
    while 1:
        if 'display: none;' in elmnt.get_attribute("style"):
            break
        time.sleep(1)

    # Mudanca da data Final para uma data muito distante (ano de 2100)
    xpath_dt_final = '//div[@id="typein_[Parameters].[Parâmetro 2]"]/span/input'
    elmnt = selenium.find_element(By.XPATH, xpath_dt_final)
    for i in range(23):
        elmnt.send_keys(Keys.BACKSPACE)
    txtInsert = '31/12/{} 23:59:59 '.format(hoje.year+100)
    elmnt.send_keys(txtInsert)
    elmnt.send_keys(Keys.TAB)

    time.sleep(3)
    btn_atualizar = selenium.find_element(By.ID, 'refresh')
    btn_atualizar.click()

    time.sleep(3)
    # Espera que o element de "loading" se encerre para continuar o script
    xpath_loadingGlass = '//div[@id="loadingGlassPane"]'
    elmnt = selenium.find_element(By.XPATH, xpath_loadingGlass)
    while 1:
        if 'display: none;' in elmnt.get_attribute("style"):
            break
        time.sleep(1)

    # Click na tabela selecionando o que se deseja fazer download 
    xpath_elmt = '//div[@class="tab-tvYLabel tvimagesNS"]/div[@class="tvimagesContainer"]/img'
    elmnt = selenium.find_element(By.XPATH, xpath_elmt)
    elmnt.click()

    time.sleep(10)
    print('Tentativa de clicar no botão de download')
    for i in range(10) :
        try:
            # Click no botao de download no lado inferior esquerdo 
            xpath_download_btn = '//button[@id="download"]'
            # elmnt = selenium.find_element_by_xpath(xpath_download_btn)
            elmnt = selenium.find_element(By.XPATH, xpath_download_btn)
            elmnt.click()
            break
        except:
            time.sleep(10)


    # 10 tentativas com intervalos de 1 segundo entre eles parar clicar no link de download dos dados cruzados
    for i in range(10) :
        try:
            xpath_elmt = '//div[@data-tb-test-id="download-flyout-download-crosstab-MenuItem"]'
            # elmnt = selenium.find_element_by_xpath(xpath_elmt)
            elmnt = selenium.find_element(By.XPATH, xpath_elmt)
            elmnt.click()
            break
        except:
            time.sleep(1)

    for i in range(10) :
        try:
            xpath_elmt = '//button[@data-tb-test-id="export-crosstab-export-Button"]'
            # xpath_elmt = '//div[@data-tb-test-id="DownloadCrosstab-Button"]'
            # elmnt = selenium.find_element_by_xpath(xpath_elmt)
            elmnt = selenium.find_element(By.XPATH, xpath_elmt)
            elmnt.click()
            break
        except:
            time.sleep(1)

    fikeTmp_path = str(file_path).replace(".xlsx",".xlsx.part")
    # Sleep para finalizar o download
    for i in range (30) :
        if rz_selenium.is_file_downloaded(file_path)  and not os.path.exists(fikeTmp_path):
            print(f'Download Completo! {file_path}')
            break
        else:
            print('...')
            time.sleep(5)

    # necessario esse timer para terminar de baixar o arquivo
    time.sleep(10)

    db_ons = wx_dbClass.db_mysql_master('db_ons',connect=True)
    tb_intervencoes = db_ons.db_schemas['tb_intervencoes']

    sql_select = db.select(db.func.max(tb_intervencoes.c.id))
    max_id = db_ons.db_execute(sql_select).scalar()

    for i in range (30) :
        try:
            # Abre a planilha baixada anteriormente
            df_input = pd.read_excel(file_path, header=0)
            DIR_TOOLS.remove_src(PATH_TMP_DOWNLOAD)
            break

        except:
            if i == 29:
                return ''
            time.sleep(1)


    intervComDesligamento = df_input[df_input['Caracterização'] != "Com Desligamento"].index
    df_input.drop(intervComDesligamento , inplace=True)

    df_input = df_input[df_input['Estado Intervenção'].isin(["Aprovada", "Em Execução", "Em Análise", "Concluída"])]

    # Retirada das colunas nao desejadas
    df_input.drop(['INDEX()', 'DatasIndex (P)', 'Pedido SGI'], inplace=True, axis=1)

    # converte coluna (ID) para inteiro
    df_input['id_intervencao'] = df_input['id_intervencao'].astype(int)

    # Mascara para a selecao de todos os dados com ID superior ao ultimo inserido no banco de dados
    mask = df_input['id_intervencao'] > max_id
    updates = df_input[mask].copy(deep=True)

    html = ""
    if len(updates):

        html = '<h3>Intervenções</h3>'
        updates.loc[:, 'Tipo'] = np.nan
        updates.loc[:, 'Potencia'] = np.nan
        updates.loc[:, 'pot'] = np.nan

        val = []
        # Formatacao e insercao dos dados na tabela
        for i,row in updates.iterrows():

            if row['Equipamento'][0:2] == 'UG':
                pos = row['Equipamento'].find('W ')
            elif row['Equipamento'][0:2] == 'LT':
                pos = row['Equipamento'].find('V ')
            else:
                # print(row['Equipamento'])
                continue

            updates.loc[i, 'Tipo'] = row['Equipamento'][0:2]
            try:
                potencia = row['Equipamento'][4:pos+1]
                updates.loc[i, 'Potencia'] = potencia
                potencia = potencia.replace('V', '').replace('W', '')
                if 'k' in potencia:
                    potencia = potencia.replace('k', '')
                    potencia = int(potencia)*1000
                elif 'M' in potencia:
                    potencia = potencia.replace('M', '')
                    potencia = int(potencia)*1000000
                elif 'G' in potencia:
                    potencia = potencia.replace('M', '')
                    potencia = int(potencia)*1000000000
                updates.loc[i, 'pot'] = potencia
            except:
                pass

            
            # replace todos os campos NaN para none
            row = row.replace({np.nan: None})

            try:
                row['Tipo Intervenção'] = int(row['Tipo Intervenção'].replace('Tipo ',''))
            except Exception as e:
                print(e)

            try:

                # Formatacao para timestamp no mssql
                row['Início'] = datetime.datetime.strptime( row['Início'],'%d/%m/%Y %H:%M:%S')
                row['Início'] = row['Início'].strftime(timestampFormat)

                # Formatacao para timestamp no mssql
                row['Fim'] = datetime.datetime.strptime( row['Fim'],'%d/%m/%Y %H:%M:%S')
                row['Fim'] = row['Fim'].strftime(timestampFormat)
            except Exception as e:
                # Utilizando AM e PM

                # Formatacao para timestamp no mssql
                row['Início'] = datetime.datetime.strptime( row['Início'],'%m/%d/%Y %H:%M:%S %p')
                row['Início'] = row['Início'].strftime(timestampFormat)

                # Formatacao para timestamp no mssql
                row['Fim'] = datetime.datetime.strptime( row['Fim'],'%m/%d/%Y %H:%M:%S %p')
                row['Fim'] = row['Fim'].strftime(timestampFormat)


            try:
                print("Tentativa de importar linha com ID {}".format(row['id_intervencao']))
                val.append(tuple(row.values[0:13]))
            except:
                print(e)
                quit()


        # Criando o DataFrame vazio com essas colunas
        df_values = pd.DataFrame(val,
                        columns = [
                        'id', 'num_interv', 'estado_interv', 'nome_equipamento',
                        'orgao_solic', 'data_inicio', 'data_fim', 'periodo',
                        'nome_categ', 'tipo', 'nome_natureza', 
                        'risco_postergacao', 'recomendacao'
                    ])
        sql_insert = tb_intervencoes.insert().values(df_values.replace({float('nan'): None}).to_dict("records"))
        n_row = db_ons.db_execute(sql_insert).rowcount
        print(f"{n_row} Linhas inseridas na tb_intervencoes")

        # datab.executemany(sql, val)
        mask_ug = updates['Tipo'] == 'UG'
        mask_lt = updates['Tipo'] == 'LT'

        header=['Tipo', 'Potência', 'Numero da intervenção', 'Status', 'Orgão solicitante', 'Equipamento', 'Data Inicio', 'Data Fim', 'Período', 'Caracterização', 'Natureza', 'Risco de postergação']
        body_ug = []
        for i,row in (updates[mask_ug].sort_values('pot', ascending=False)).iterrows():
            body_ug.append([row['Tipo'], row['Potencia'], row['Nº Intervenção'], row['Estado Intervenção'], row['Órgão Solicitante'], row['Equipamento'], row['Início'], row['Fim'], row['Tipo Período'], row['Caracterização'], row['Natureza'], row['Risco Postergação']])
            # body_ug.append([row['Tipo'], row['Potencia'], row['Unidade'], row[3], row[4], row[5], row[6], row[7], row[8], row[10], row[11], row[15], row[14]])
        
        if len(body_ug)>0:
            html += '<h4>Unidade Geradora</h4>' 
            html += wx_emailSender.gerarTabela(body_ug, header)

        header=['Tipo', 'Tensão', 'Numero da intervenção', 'Status', 'Orgão solicitante', 'Equipamento', 'Data Inicio', 'Data Fim', 'Período', 'Caracterização', 'Natureza', 'Risco de postergação']
        body_lt = []
        for i,row in (updates[mask_lt].sort_values('pot', ascending=False)).iterrows():
            body_lt.append([row['Tipo'], row['Potencia'], row['Nº Intervenção'], row['Estado Intervenção'], row['Órgão Solicitante'], row['Equipamento'], row['Início'], row['Fim'], row['Tipo Período'], row['Caracterização'], row['Natureza'], row['Risco Postergação']])
            # body_lt.append([row['Tipo'], row['Potencia'], row['Unidade'], row[3], row[4], row[5], row[6], row[7], row[8], row[10], row[11], row[15], row[14]])
        
        if len(body_lt)>0:
            html += '<h4>Linha de Transmissão</h4>' 
            html += wx_emailSender.gerarTabela(body_lt, header)
        
    else:
        print('Nã há novos novas intervenções')

    if html == '<h3>Intervenções</h3>':
        html = ''
    return html



def main():

    hoje = datetime.datetime.now()

    # Funcoes que 
    # recoperarHistorico()
    table_fsarh = ''
    table_intervencoes = ''
    
    print('Verificacao de atualizacoes no FSARH.')
    selenium_driver = rz_selenium.abrir_undetected_chrome(path_tmp_download=PATH_TMP_DOWNLOAD)
    rz_ons.login_ons(selenium_driver)
    try:
        table_fsarh = checkAtualizFSARH(selenium_driver)
        
        titulo = 'Atualizações FSARH - {}'.format(hoje.strftime('%d/%m/%Y'))
            
        print('Enviando tabela no whatsapp...')
        GERAR_PRODUTO.enviar({
            "produto":"FSARH",
            "data": hoje,
            "titulo":titulo,
            "html": table_fsarh
        })
    except Exception as e:
        print(e)

    finally:
        selenium_driver.quit()

    time.sleep(5)
    print('\n\nVerificacao de atualizacoes das intervencoes.')
    selenium_driver = rz_selenium.abrir_undetected_chrome(path_tmp_download=PATH_TMP_DOWNLOAD)
    rz_ons.login_ons(selenium_driver)

    try:
        table_intervencoes = checkUpdateIntervernc(selenium_driver)
    except Exception as e:
        print(e)
    finally:
        selenium_driver.quit()


    if table_fsarh != '' or table_intervencoes != '':
        html = '<html> <head> <style type="text/css">table {font-family: arial, sans-serif;border-collapse: '
        html += 'collapse;width: 1500px;}td, th {border: 1px solid #dddddd;text-align: center;padding: 8px;} '
        html += 'tr:nth-child(4n + 2) td, tr:nth-child(4n + 3) td {background-color: #F2ECEC;} .umaLinha '
        html += 'tr:nth-child(odd) td{ background-color: #F2ECEC;} .umaLinha tr:nth-child(even) td{ '
        html += 'background-color: #ffffff;} </style> </head> <body> '

        if table_fsarh != '':
            html += table_fsarh
        html += '<br><br>'
        if table_intervencoes != '':
            html += table_intervencoes

        html += '</body> </html>'

        print('Enviando Email...')
        serv_email = wx_emailSender.WxEmail()
        serv_email.username = 'intervencoes@climenergy.com'
        serv_email.password = 'clime2sam'

        # 10 tentativas de mandar email
        for i in range(10):
            try:
                serv_email.sendEmail(texto=html, assunto='[RESTRICOES] Atualizações FSARH e/ou restrições ({})'.format(hoje.strftime('%d/%m/%Y')))
                break
            except:
                pass
            
        
        

if __name__ == '__main__':

    # try:
    main()
    # except Exception as e:
    #     print(e)
    #     hoje = datetime.datetime.now()
    #     serv_email = wx_emailSender.WxEmail()
    #     serv_email.send_to = ['joao.Filho4@raizen.com']
    #     serv_email.sendEmail(texto=str(e), assunto='Erro no resticoes_verifier ({})'.format(hoje.strftime('%d/%m/%Y - %H:%M')))

