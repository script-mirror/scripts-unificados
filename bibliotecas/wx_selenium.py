#######################################################################################
# 										WX Energy
#
# Description: Selenium lib
#
# Author: Thiago Scher (thiago.scher@gmail.com)
# Date: 07/09/2019
#
#######################################################################################
import os
import sys
import pdb
import psutil

from pandas import read_html

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from pathlib import Path

sys.path.insert(1,"/WX2TB/Documentos/fontes/PMO/scripts_unificados/")
from bibliotecas.wx_configs import getConfiguration




def killpid(pid):	
	try:
		p = psutil.Process(pid)
	except:
		return 

	children = p.children(recursive=True)
	while len(children) > 1:
		os.system(f'kill -9 {children[0].pid}')
		children = p.children(recursive=True)
	os.system(f"kill -9 {p.pid}")



def openSelenium():
	""" Open chrome  with Selenium
	:param downloadDirectory: Directory where the downloads will be save
	:return: The driver object
	"""

	configs = getConfiguration()

	downloadDirectory = configs['selenium']['downloadDirectory']
	if downloadDirectory == '':
		downloadDirectory = os.getcwd()+'/arquivos/temporarios/'

	downloadDirectory = Path(downloadDirectory)
	if not downloadDirectory.exists():
		os.makedirs(downloadDirectory)

	seleniumDriver = str(configs['selenium']['seleniumDriverPath'])
	if configs['selenium']['brownser'] == 'chrome':
		# Configuracoes para o selenium 
		chrome_options = webdriver.ChromeOptions()

		prefs = {}
		# prefs["download.default_directory"] = str(downloadDirectory)
		prefs["download.default_directory"] = "/dev/null"
		prefs["download.prompt_for_download"] = False
		prefs["download.directory_upgrade"] = True
		chrome_options.add_experimental_option("prefs", prefs)

		if 'dataDir' in configs['selenium']:
			chrome_options.add_argument('--user-data-dir="{}"'.format(configs['selenium']['dataDir']))

		if seleniumDriver == '':
			return webdriver.Chrome(options=chrome_options)
		else:
			return webdriver.Chrome(seleniumDriver, options=chrome_options)

	elif configs['selenium']['brownser'] == 'firefox':
		from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

		cap = DesiredCapabilities.FIREFOX
		cap["marionette"] = False

		firefox_profile_path = "/home/admin/.mozilla/firefox/l82fbozl.default"
		print(f"Camino do perfil fornecido: {firefox_profile_path}")

		fp = webdriver.FirefoxProfile(firefox_profile_path)#"/home/admin/.mozilla/firefox/1eqgb9y1.Raizen"
		print(f"Camino do perfil após a criação: {fp.path}")
  
		fp.set_preference("browser.preferences.instantApply",True)
		fp.set_preference("browser.helperApps.neverAsk.saveToDisk", "text/plain, application/octet-stream, application/binary, text/csv, application/csv, application/excel, text/comma-separated-values, text/xml, application/xml, application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
		fp.set_preference("browser.helperApps.alwaysAsk.force",False)
		fp.set_preference("browser.download.manager.showWhenStarting",False)
		fp.set_preference("webdriver.firefox.marionette", False)
		fp.set_preference("browser.download.folderList",2)
		# fp.set_preference("browser.download.dir", str(downloadDirectory))
		fp.set_preference("browser.download.dir", "/dev/null")
		print(fp.profile_dir)
		fox_opt = webdriver.FirefoxOptions()
		fox_opt.binary = "/usr/bin/firefox"
		fox_opt.add_argument(f'--profile={fp.path}')

		

		if os.name == 'nt':
			cap["marionette"] = False
			fox_opt.binary = "C:/Program Files/Mozilla Firefox/firefox.exe"

		return webdriver.Firefox(executable_path='/usr/bin/geckodriver', firefox_binary='/usr/bin/firefox', options=fox_opt, capabilities=cap,firefox_profile=fp)



def insertInElement(driver, element ,txtInsert):
	""" Insert a text in a specific element
	:param driver: Selenium driver where the page is opened
	:param element: Element where the text will be inserted
	:param txtInsert: Text to be inserted
	:return: 
	"""
	elmnt = driver.find_element_by_name(element)
	elmnt.clear()
	elmnt.send_keys(txtInsert)
	elmnt.send_keys(Keys.TAB)

def clickElement(driver, element):
	""" Click in a specific element
	:param driver: Selenium driver where the page is opened
	:param element: Element to be clicked
	:param txtInsert: Text to be inserted
	:return: 
	"""
	elmnt = driver.find_element_by_name(element)
	driver.execute_script("return arguments[0].scrollIntoView();", elmnt)
	elmnt.click();

def get_table_from_page(driver):
	return read_html(driver.page_source)[-1].T

def get_cookies(driver):
	return driver.get_cookies()


if __name__ == '__main__':

	driver = openSelenium()
	driver.quit()