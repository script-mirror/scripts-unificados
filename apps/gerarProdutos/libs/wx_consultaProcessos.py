
import os
import re
import bs4
import sys
import pdb
import json
import time
import datetime
import requests

import pandas as pd

from selenium import webdriver

try:
	import wx_emailSender
except:
	pass

def fechar_possivel_popup(driver):
    try:
        # Tratamento de um possivel alerta
        driver.switch_to.alert.accept()
    except Exception as e:
        pass


def abrir_undetected_chome(path_profile = ''):
    import undetected_chromedriver.v2 as uc
    from selenium.webdriver.common.by import By

    options = uc.ChromeOptions()

    if path_profile != '':
        options.user_data_dir = path_profile
    # options.add_argument('headless')
    # options.add_argument("start-maximized")

    driver = uc.Chrome(options=options, version_main=94)

    return driver

def fazer_login_aneel(driver):
    from selenium.webdriver.common.by import By
    emailUsuario = 'thiago.scher@gmail.com'
    passUsuario = 'ex$KL6vs'

    driver.get('https://antigo.aneel.gov.br/web/guest/consulta-processual')

    usuarioLogado = driver.find_elements_by_class_name('user-full-name')
    if usuarioLogado == []:
        user = driver.find_element(By.ID, "_58_login")
        user.clear()
        user.send_keys(emailUsuario)
        password = driver.find_element(By.ID, "_58_password")
        password.send_keys(passUsuario)
        print('Usuário não está logado, favor logar. É necessário apenas que digite a captch')
        input('Após o login aperte enter no cmd/terminal')




def verificar_documentos_Aneel_numero_processo():
    from selenium.webdriver.common.by import By

    processos = {}


    path_libs = os.path.dirname(os.path.abspath(__file__))
    path_local = os.path.dirname(path_libs)
    path_profile = os.path.join(path_local, 'profile')
    pathRegProcessosDia = os.path.join(path_local, 'arquivos', 'tmp', 'consultaProcessosAneel_new.txt')


    if os.path.exists(pathRegProcessosDia):
        with open(pathRegProcessosDia) as json_file:
            processos = json.load(json_file)

    driver = abrir_undetected_chome(path_profile)
    fazer_login_aneel(driver)

    atualizacoes = {}

    html = ''
    for i, processo in enumerate(processos):
        print('Processo: {} ({}/{})'.format(processo, i+1, len(processos)))

        ultimo_processo = processos[processo]['ultimo_documento_enviado']

        url = 'https://sicnet2.aneel.gov.br/sicnetweb/pesquisa.asp'
        driver.get(url)
        time.sleep(1)

        driver.execute_script("document.getElementById('btn_pesquisar').onclick = function() { return true; };")

        match = re.match(r'([0-9]{5}).([0-9]{6})/([0-9]{4})', processo)
        num_orgao = match.group(1)
        num_sequencial = match.group(2)
        num_ano = match.group(3)
        driver.execute_script("$('#txt_numero_orgao').val('{}')".format(num_orgao))
        driver.execute_script("$('#txt_numero_sequencial').val('{}')".format(num_sequencial))
        driver.execute_script("$('#txt_numero_ano').val('{}')".format(num_ano))

        driver.find_element(By.ID,'btn_pesquisar').click()

        fechar_possivel_popup(driver)

        page_source=driver.page_source
        lista_df = pd.read_html(page_source)

        if len(lista_df) > 1:
            df_processos = lista_df[1]

            row = df_processos.iloc[0]

            driver.find_element(By.XPATH,'//a[contains(text(), "{}")]'.format(row['NÚMERO'])).click()
            time.sleep(0.5)
            chld = driver.window_handles[1]
            driver.switch_to.window(chld)
            fechar_possivel_popup(driver)

            # driver.find_element(By.XPATH,'//a[contains(text(), "DOCUMENTOS JUNTADOS")]').click()

            df_documentos = pd.read_html(driver.find_element(By.XPATH,'//*[@id="aba-3"]').get_attribute('innerHTML'))
            links_documentos = driver.find_elements(By.XPATH,'//*[@id="aba-3"]/table/tbody/tr/td/a[1]')
            links_downloads = driver.find_elements(By.XPATH,'//*[@id="aba-3"]/table/tbody/tr/td/a[2]')
            if len(df_documentos):
                df_documentos = df_documentos[0]
    
                for index, doc in df_documentos.iterrows():

                    if doc['NÚMERO DO DOCUMENTO/PROCESSO:'][:-3] == ultimo_processo:
                        break

                    if index == 0:
                        processos[processo]['ultimo_documento_enviado'] = doc['NÚMERO DO DOCUMENTO/PROCESSO:'][:-3]

                    try:
                        doc['NÚMERO DO DOCUMENTO/PROCESSO:'] = '<a href="{}">{}</a>'.format(links_downloads[index].get_attribute('href'), doc['NÚMERO DO DOCUMENTO/PROCESSO:'])
                    except:
                        pass

                    informacoes = doc.to_list()
                    informacoes.insert(1, links_documentos[index].get_attribute('title'))

                    if processo in atualizacoes:
                        atualizacoes[processo].append(informacoes)
                    else:
                        atualizacoes[processo] = [informacoes]
            
            driver.close();
            parent = driver.window_handles[0]
            driver.switch_to.window(parent)


    for i_proc, proc in enumerate(atualizacoes):
        html += '<br><h3>Processo: {} ({})</h3><br>{}'.format(proc, processos[proc]['efeito'], processos[proc]['assunto'])
        html += wx_emailSender.gerarTabela(body=atualizacoes[proc] , header=['NÚMERO DO DOCUMENTO/PROCESSO', 'DESCRIÇÃO', 'DATA', 'Nº DE ORIGEM', 'PROCEDÊNCIA', 'TIPO DE DOCUMENTO'], widthColunas=[61,300,61,61,61,61])


    driver.quit()
    with open(pathRegProcessosDia, 'w') as outfile:
        json.dump(processos, outfile, indent=4)

    # with open('saida.html', 'w', encoding="utf-8") as outfile:
    #     outfile.write(html)

    return html




def verificarProcessosAneelComSelenium(dt='', tipo_consulta='assuntos'):
    import undetected_chromedriver.v2 as uc
    from selenium.webdriver.common.by import By

    if dt == '':
        dt = datetime.datetime.now()

    if tipo_consulta == 'assuntos':
        assuntos = ['FURNAS', 'Sobradinho', 'CHESF', 'ONS', 'Operador Nacional do Sistema', 'CESP', 'CTG', 'Jupiá', 'Porto Primavera', 'Belo Monte', 'Norte Energia', 'BMTE', 'XRTE']
        assuntos += ['eletronorte', 'hidrovia', 'petrobras', 'mexilhão', 'cvu', 'custo variável', 'nortefluminense', 'araucária', 'termopernambuco', 'Raízen', 'GNA', 'CPAMP']
        assuntos += ['termoceara', 'ilha solteira', 'itaipu', 'creg', 'Procedimento Competitivo Simplificado', 'Leilão de Reserva de Capacidade', 'ppt', 'âmbar', 'EPP', 'EDLUX']

    elif tipo_consulta == 'numero_processos':
        num_processos = ['48581.001541/2022', '48581.001531/2022']

    path_libs = os.path.dirname(os.path.abspath(__file__))
    path_local = os.path.dirname(path_libs)
    path_profile = os.path.join(path_local, 'profile')
    pathRegProcessosDia = os.path.join(path_local, 'arquivos', 'tmp', 'consultaProcessosAneel.txt')

    controle_processos = {}
    controle_processos_atualizado = {}
    if os.path.exists(pathRegProcessosDia):
        try:
            with open(pathRegProcessosDia) as json_file:
                controle_processos = json.load(json_file)
        except:
            pass

    options = uc.ChromeOptions()

    options.user_data_dir = path_profile
    # options.add_argument('headless')
    # options.add_argument("start-maximized")

    emailUsuario = 'thiago.scher@gmail.com'
    passUsuario = 'ex$KL6vs'

    # start chrome browser
    driver = uc.Chrome(options=options, version_main=94)
    driver.get('https://antigo.aneel.gov.br/web/guest/consulta-processual')

    usuarioLogado = driver.find_elements_by_class_name('user-full-name')
    if usuarioLogado == []:
        user = driver.find_element(By.ID, "_58_login")
        user.clear()
        user.send_keys(emailUsuario)
        password = driver.find_element(By.ID, "_58_password")
        password.send_keys(passUsuario)
        print('Usuário não está logado, favor logar. É necessário apenas que digite a captch')
        input('Após o login aperte enter no cmd/terminal')

    html = ''
    for i, assunto in enumerate(assuntos):
        print('Assunto: {} ({}/{})'.format(assunto, i+1, len(assuntos)))

        url = 'https://sicnet2.aneel.gov.br/sicnetweb/pesquisa.asp'
        driver.get(url)
        time.sleep(1)

        driver.execute_script("document.getElementById('btn_pesquisar').onclick = function() { return true; };")

        campoLivre = driver.find_element(By.ID,'txt_campo_livre')
        campoLivre.clear()
        driver.execute_script("$('#txt_campo_livre').val('{}')".format(assunto))

        driver.find_element(By.ID,'cod_protocolo_tipo_2').click()
        
        driver.execute_script("$('#dt_protocolo_inicial').val('{}')".format((dt-datetime.timedelta(days=3)).strftime('%d/%m/%Y')))
        driver.execute_script("$('#dt_protocolo_final').val('{}')".format(dt.strftime('%d/%m/%Y')))
        
        driver.find_element(By.ID,'btn_pesquisar').click()

        processos_assunto_especifico = []
        fechar_possivel_popup(driver)

        page_source=driver.page_source
        lista_df = pd.read_html(page_source)

        if len(lista_df) > 1:
            df_processos = lista_df[1]

            df_processos['TÍTULO'] = ''
            df_processos = df_processos[['NÚMERO', 'TÍTULO', 'UNIDADE ATUAL', 'PROCEDÊNCIA/INTERESSADO', 'TIPO', 'Nº ORIGEM', 'DATA']]
            for index, row in df_processos.iterrows():

                if index == 0:
                    controle_processos_atualizado[assunto] = row['NÚMERO']
                
                if assunto in controle_processos:
                    if row['NÚMERO'] == controle_processos[assunto]:
                        break
            
                driver.find_element(By.XPATH,'//a[contains(text(), "{}")]'.format(row['NÚMERO'] )).click()
                chld = driver.window_handles[1]
                driver.switch_to.window(chld)
                fechar_possivel_popup(driver)

                try:
                    row['TÍTULO'] = driver.find_element(By.XPATH,'//table/tbody/tr[10]/td[1]').text
                except:
                    row['TÍTULO'] = "Sem acesso"
 
                processos_assunto_especifico.append(row.to_list())

                driver.close();
                parent = driver.window_handles[0]
                driver.switch_to.window(parent)

            if len(processos_assunto_especifico):
                html += '<br><h3>Assunto: {}</h3>'.format(assunto)
                html += wx_emailSender.gerarTabela(body=processos_assunto_especifico , header=['NÚMERO', 'TÍTULO', 'UNIDADE ATUAL', 'PROCEDÊNCIA/INTERESSADO', 'TIPO', 'Nº ORIGEM', 'DATA'], widthColunas=[61,300,61,61,61,61,61])

    driver.quit()
    with open(pathRegProcessosDia, 'w') as outfile:
        json.dump(controle_processos_atualizado, outfile, indent=4)

    return html



def verificarProcessosAna(dt=''):

	assuntos = ['FURNAS', 'Sobradinho', 'CHESF', 'ONS', 'Operador Nacional do Sistema', 'CESP', 'CTG', 'Jupiá', 'Porto Primavera', 'Belo Monte', 'Norte Energia', 'eletronorte', 'hidrovia']

	pathLocal = os.path.abspath('.')
	pathRegProcessosEnv = os.path.join(pathLocal, 'arquivos', 'tmp', 'consultaProcessosAna.txt')
	
	processosEnv = {}
	if os.path.exists(pathRegProcessosEnv):
		try:
			with open(pathRegProcessosEnv) as json_file:
				processosEnv = json.load(json_file)
		except:
			pass

	if dt == '':
		dt = datetime.datetime.now()

	pathLocal = os.path.abspath('.')
	userDataPath = os.path.join(pathLocal, 'chrome-data')

	options = webdriver.ChromeOptions()
	options.add_argument('lang=pt-br')
	# options.add_argument('--user-data-dir="{}"'.format(userDataPath))
	options.add_argument('--user-data-dir={}'.format(userDataPath))
	driver = webdriver.Chrome(options=options)

	html = ''
	for i, assunto in enumerate(assuntos):
		print('{} {}/{}'.format(assunto, i+1, len(assuntos)))
		url = 'https://www.ana.gov.br/www/AcoesAdministrativas/CDOC/consultaExterna.asp'
		driver.get(url)
		time.sleep(1)

		driver.find_element_by_xpath('//input[@type="text" and @name="txt_interessado"]').send_keys(assunto)
		driver.find_element_by_xpath('//input[@type="submit" and @name="acao"]').click()

		page_source=driver.page_source
		df = pd.read_html(page_source)	

		if len(df)>1:

			df = df[1]
			links = []
			for e in driver.find_elements_by_xpath('//td[@class="td_primeira"]/a'):
				links.append('<a href=\'{}\'>{}</a>'.format(e.get_attribute('href'), e.get_attribute('text')))

			df['LINKS'] = links

			idx_ultimoProc = 5
			if assunto in processosEnv:
				ultimoProcessoDoAssunto = processosEnv[assunto]
				if df['NÚMERO'].str.contains(ultimoProcessoDoAssunto).any():
					idx_ultimoProc = df[df['NÚMERO'] == ultimoProcessoDoAssunto].index.tolist()[0]

			df = df[df.index < idx_ultimoProc]
			if df.shape[0] > 0:

				processosEnv[assunto] = df.iloc[0,0]
				df = df[['LINKS', 'UORG ATUAL', 'INTERESSADO', 'ASSUNTO']]

				html += '<br><h3>Assunto: {}</h3>'.format(assunto)
				html += wx_emailSender.gerarTabela(body=df.values.tolist() , header=['NÚMERO', 'UORG ATUAL', 'INTERESSADO', 'ASSUNTO'], widthColunas=[61,61,61,300])

		if len(assuntos) == i+1:
			break

	driver.quit()

	with open(pathRegProcessosEnv, 'w') as outfile:
		json.dump(processosEnv, outfile, indent=4)

	return html

if __name__ == '__main__':

    diretorioRaiz = os.path.abspath('../../../')
    pathLibUniversal = os.path.join(diretorioRaiz,'bibliotecas')
    sys.path.insert(1, pathLibUniversal)
    import wx_emailSender

    # verificarProcessosAneel()
    verificarProcessosAneelComSelenium(dt='')


