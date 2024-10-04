
import pdb
import time
import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from dotenv import load_dotenv
import os
load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")),'.env'))

__USER_SINTEGRE = os.getenv('USER_SINTEGRE') 
__PASSWORD_SINTEGRE = os.getenv('PASSWORD_SINTEGRE')

def abrir_selenium():

	user_ = __USER_SINTEGRE
	pass_ = __PASSWORD_SINTEGRE
	pdb.set_trace()
	driver = webdriver.Chrome()

	url_sintegre = 'https://sintegre.ons.org.br/'
	driver.get(url_sintegre)

	btn_avancar = driver.find_element(By.XPATH, "//input[@type='submit']")
	
	if btn_avancar.get_attribute("value") == 'Avançar':
		username_input = driver.find_element(By.ID, "username")
		username_input.send_keys(user_)
		btn_avancar.click()

	password_input = WebDriverWait(driver, 60).until(
			EC.presence_of_element_located((By.ID, "password"))
		)

	password_input.send_keys(pass_)
	# desabilitado p passar manualmente
	# btn_avancar = driver.find_element(By.XPATH, "//input[@type='submit']")
	# btn_avancar.click()
	pdb.set_trace()

	WebDriverWait(driver, 60).until(
			EC.presence_of_element_located((By.ID, "user-foto"))
		)

	url = "https://integracaoagentes.ons.org.br/FSAR-H/SitePages/Exibir_Forms_FSARH.aspx"
	driver.get(url)


	checkbox_ativas = WebDriverWait(driver, 60).until(
			EC.presence_of_element_located((By.XPATH, "//input[@id='chkAtivas']"))
		)
	checkbox_ativas.click()

	WebDriverWait(driver, 60).until(
			EC.presence_of_element_located((By.NAME, "tblMain_length"))
		)
	select = Select(driver.find_element(By.NAME, 'tblMain_length'))
	select.select_by_value('100')


	coluna_status = driver.find_element(By.XPATH, "//th[@aria-label='Status: Ordenar colunas de forma ascendente']")
	coluna_status.click()
	
	# for nome_usina in usinas_fsarh:
	usinas_fsarh = {}
	usinas_fsarh['Nova Ponte'] = {'restricao':'DEFLUENTE'}
	usinas_fsarh['Três Marias'] = {'restricao':'DEFLUENTE'}
	usinas_fsarh['Queimado'] = {'restricao':'DEFLUENTE'}
	usinas_fsarh['Caconde'] = {'restricao':'DEFLUENTE'}
	usinas_fsarh['Limoeiro'] = {'restricao':'DEFLUENTE'}
	usinas_fsarh['Piraju'] = {'restricao':'DEFLUENTE'}
	usinas_fsarh['Jurumirim'] = {'restricao':'DEFLUENTE'}
	usinas_fsarh['Chavantes'] = {'restricao':'DEFLUENTE'}
	usinas_fsarh['Primavera'] = {'restricao':'DEFLUENTE'}
	usinas_fsarh['Jupiá'] = {'restricao':'DEFLUENTE'}
	usinas_fsarh['Furnas'] = {'restricao':'DEFLUENTE'}
	usinas_fsarh['Mascarenhas de Moraes'] = {'restricao':'DEFLUENTE'}
	usinas_fsarh['Serra da Mesa'] = {'restricao':'DEFLUENTE'}
	usinas_fsarh['Manso'] = {'restricao':'DEFLUENTE'}
	usinas_fsarh['Batalha'] = {'restricao':'DEFLUENTE'}
	usinas_fsarh['Peixe Angical'] = {'restricao':'DEFLUENTE'}
	usinas_fsarh['Salto Osório'] = {'restricao':'DEFLUENTE'}
	usinas_fsarh['Miranda'] = {'restricao':'DEFLUENTE'}
	usinas_fsarh['Dona Francisca'] = {'restricao':'DEFLUENTE'}
	usinas_fsarh['Xingó'] = {'restricao':'DEFLUENTE'}
	usinas_fsarh['Emborcação'] = {'restricao':'DEFLUENTE'}
	usinas_fsarh['Itumbiara'] = {'restricao':'DEFLUENTE'}
	usinas_fsarh['Emborcação'] = {'restricao':'DEFLUENTE'}
	usinas_fsarh['Marimbondo'] = {'restricao':'TURBINADA'}
	usinas_fsarh['Pimental'] = {'restricao':'DEFLUENTE'}



	for nome_usina in usinas_fsarh:
		print(f'\n### {nome_usina} ###')
		if nome_usina == 'Pimental':
			pdb.set_trace()
		tipo_restricao = usinas_fsarh[nome_usina]['restricao']
		input_pesquisa = driver.find_element(By.XPATH, "//input[@type='search']")
		input_pesquisa.clear()
		input_pesquisa.send_keys(nome_usina)

		table_element = driver.find_element(By.XPATH, "//table[@id='tblMain']")
		table_html = table_element.get_attribute('outerHTML')
		table_df = pd.read_html(table_html)[0]

		filtro1 = table_df['Status']=='Aceito'
		filtro2 = table_df['Evento'].str.upper().str.contains('PMO')
		filtro3 = table_df['Restrição'].str.upper().str.contains(tipo_restricao.upper())
		filtro4 = table_df['Reservatório'].str.upper().str.contains(nome_usina.upper())
		fsarh_filtrados = table_df.loc[filtro1&filtro2&filtro3&filtro4]

		for idx, row in fsarh_filtrados.iterrows():

			visualizadores_fsarh = driver.find_elements(By.XPATH, "//img[@src='/FSAR-H/SiteAssets/img/icones/icone-visualizar.png']")
			fsarh_x = visualizadores_fsarh[fsarh_filtrados.loc[idx].name]
			fsarh_x.click()

			tipo_rest = row['Restrição']


			# if row['Restrição'] not in restricoes:
			# 	if row['Restrição']  == 'Jusante - Vazão Defluente Mínima':
			# 		restricoes[row['Restrição']] = [0 for x in range(12)]
			# 	else:
			# 		pdb.set_trace()
			

			novo_frame = WebDriverWait(driver, 60).until(
					EC.presence_of_element_located((By.ID, "myIframe"))
				)

			driver.switch_to.frame(novo_frame)
			
			WebDriverWait(driver, 60).until(
					EC.presence_of_element_located((By.XPATH, "//table[@class='tblDadosPeriodo']"))
				)

			rows = driver.find_elements(By.XPATH, "//table[@class='tblDadosPeriodo']//tr")
			if len(rows) >= 4:
				for i_row in range(3, len(rows)):
					

					# novoValor = driver.find_element(By.XPATH, "//input[contains(@class, 'txtNovoValor')]").get_attribute("value")
					# print(f'Novo valor:{novoValor}')
					# pdb.set_trace()
					time.sleep(1)
					novoValor = driver.find_element(By.XPATH, "//input[contains(@class, 'txtNovoValor')]").get_attribute("value")

					diaInicio = driver.find_element(By.ID, "txtDiaInicioNovo").get_attribute("value")
					mesInicio = driver.find_element(By.ID, "txtMesInicioNovo").get_attribute("value")
					anoInicio = driver.find_element(By.ID, "txtAnoInicioNovo").get_attribute("value")
					
					diaTermino = driver.find_element(By.ID, "txtDiaTerminoNovo").get_attribute("value")
					mesTermino = driver.find_element(By.ID, "txtMesTerminoNovo").get_attribute("value")
					anoTermino = driver.find_element(By.ID, "txtAnoTerminoNovo").get_attribute("value")

					print(f'[{tipo_rest}] {diaInicio}/{mesInicio}/{anoInicio} -> {diaTermino}/{mesTermino}/{anoTermino}: {novoValor}')
					
					
					# # driver.find_element(By.ID, "txtHoraInicioNovo").get_attribute("value")
					# print(driver.find_element(By.ID, "txtDiaInicioNovo").get_attribute("value"))
					# print(driver.find_element(By.ID, "txtMesInicioNovo").get_attribute("value"))
					# print(driver.find_element(By.ID, "txtAnoInicioNovo").get_attribute("value"))

					# # driver.find_element(By.ID, "txtHoraTerminoNovo").get_attribute("value")
					# print(driver.find_element(By.ID, "txtDiaTerminoNovo").get_attribute("value"))
					# print(driver.find_element(By.ID, "txtMesTerminoNovo").get_attribute("value"))
					# print(driver.find_element(By.ID, "txtAnoTerminoNovo").get_attribute("value"))

			driver.switch_to.default_content()
			button_close = driver.find_elements(By.XPATH, "//button[contains(@class, 'ui-dialog-titlebar-close')]")[0]
			button_close.click()

	# Fechar o navegador
	driver.quit()


if '__main__' == __name__:
	abrir_selenium()