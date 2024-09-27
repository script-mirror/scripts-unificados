#######################################################################################
#										WX Energy
#
# Description: 
#
# Author: Thiago Scher (thiago@wxe.com.br)
# Date: 12/11/2019
#
#######################################################################################

# importing the requests library 
import requests 
import json
import pdb
from bs4 import BeautifulSoup

class Wxbbce:

	def __init__(self):
		""" Class constructor
		:param :
		:return: 
		"""
		self.products = []
		self.productsActive = {}
		self.authentication = self.getToken()
		self.loginData = self.login()


	def getToken(self):
		url = 'https://wso2.bbce.com.br/token'
		body = {'grant_type': 'client_credentials'}
		headers = {'Content-Type':'application/x-www-form-urlencoded', 'Authorization': 'Basic YUhaOGpmY0Y5cGdsZjY4dGpZaHRfbUxTal9VYTppOW1saDRpNVFlMUtJSnE0NG93RmROOUNHVE1h'}

		r = requests.post(url, data=body, headers=headers)

		if ('http://wso2.org/apimanager/throttling' in r.text):
			return ''

		# Transform the JSON retorned in a dictionary
		attributs = eval(r.content)
		print(attributs)
		return attributs


	def login(self):
		url = 'https://wso2.bbce.com.br/login-api/v1/login'

		body = {'login': 'thiago@wxe.com.br', 'Senha':'08xQR0q7'}
		headers = {'Authorization': 'Bearer '+self.authentication['access_token']}

		r = requests.post(url, data=body, headers=headers)

		if ('http://wso2.org/apimanager/throttling' in r.text):
			return ''
		print(r.text)
		# Transform the JSON retorned in a dictionary
		attributs = eval(r.content)
		return attributs

	def getProducts(self):
		url = 'https://wso2.bbce.com.br/produtos-api/v1/produtos'
		headers = {'Authorization':'Bearer '+self.authentication['access_token'], 'BBCE-BUSTOKEN': self.loginData['token']}
		r = requests.get(url, headers=headers)
		
		# these variables are set to not broke the eval 	
		false = False
		true = True
		null = None
		products = eval(r.content)

		return products

	def listarContratos(self, dataInicial, dataFinal):
		""" Consultar os contratos da empresa 
		:param None: 
		:return None: 
		"""
		url = 'https://wso2.bbce.com.br/contratos-api/v1/listar?de={}&ate={}&produtoId=&contraparteId=&prazo=&fonte=&submercado=&tipo_preco='.format(dataInicial.strftime('%Y-%m-%d'), dataFinal.strftime('%Y-%m-%d'))
		headers = {'Authorization':'Bearer '+self.authentication['access_token'], 'BBCE-BUSTOKEN': self.loginData['token']}
		r = requests.get(url, headers=headers)
		
		# these variables are set to not broke the eval 	
		false = False
		true = True
		null = None

		contratos = eval(r.content)
		return contratos

	def getCurvaPrecos(self):
		url = 'https://wso2.bbce.com.br/produtos-api/v1/produtos'
		headers = {'Authorization':'Bearer '+self.authentication['access_token'], 'BBCE-BUSTOKEN': self.loginData['token']}
		r = requests.get(url, headers=headers)

		false = False
		true = True
		null = None

		return eval(r.content)

	def getNegocios(self, dataInicial, dataFinal, pagina=1, tamanho_pagina=100):
		url = 'https://wso2.bbce.com.br/negociacoes-api/v1/plataforma?de={}&ate={}&tamanho_pagina={}&pagina={}'.format(dataInicial.strftime('%Y-%m-%d'), dataFinal.strftime('%Y-%m-%d'), tamanho_pagina, pagina)
		headers = {'Authorization':'Bearer '+self.authentication['access_token'], 'BBCE-BUSTOKEN': self.loginData['token']}
		r = requests.get(url, headers=headers)

		false = False
		true = True
		null = None

		return eval(r.content)


	def getOfertas(self, idProd):

		if self.authentication == '' or self.loginData == '':
			return 'O maximo de request excedeu o permitido, sera feita uma nova tentativa em 30 segundos!'

		url = 'https://wso2.bbce.com.br/ofertas-api/v1/ofertas?produtoId='+str(idProd)
		headers = {'Authorization':'Bearer '+self.authentication['access_token'], 'BBCE-BUSTOKEN': self.loginData['token']}
		r = requests.get(url, headers=headers)
	
		# these variables are set to not broke the eval 	
		null = None

		if ('http://wso2.org/apimanager/throttling' in r.text):
			return 'O maximo de request excedeu o permitido, sera feita uma nova tentativa em 30 segundos!'

		return eval(r.content)

	def getAllActive(self):
		self.products = self.getProducts()

		for i in range(len(self.products['model'])):
			if self.products['model'][i]['quantidade'] > 0:
				self.productsActive[self.products['model'][i]['descricao']] = self.getOfertas(self.products['model'][i]['id'])
				# print(self.products['model'][i]['id'], self.products['model'][i]['quantidade'])
		
		print(self.productsActive)
		return self.productsActive

	def getNegotiations(self, dtFrom, dtTo='', page='', lengthPage=''):
		url = 'https://wso2.bbce.com.br/negociacoes-api/v1/plataforma?de='+str(dtFrom)+'&ate='+str(dtTo)
		if page != "":
			url = url + '&pagina='+str(page)
		if lengthPage != "":
			url = url + '&tamanho_pagina='+str(lengthPage)


		headers = {'Authorization':'Bearer '+self.authentication['access_token'], 'BBCE-BUSTOKEN': self.loginData['token']}
		r = requests.get(url, headers=headers)
		null = None
		false = False
		true = True
		return eval(r.content)


if __name__ == "__main__":
	wxBbce = Wxbbce()

	# products = wxBbce.getProducts()
	

	# print(list(products.keys()))

	# Todos os produtos na "mesa"
	# print(products['model'])

	# Printa todas as informacoes do produdo na posicao X
	# print(products['model'][0])

	# Product id usef to get the offers
	# print(products['model'][0]['id'])


	# print(wxBbce.getOfertas('1282'))

	# activeProduct = wxBbce.getAllActive()

	negotiations = wxBbce.getNegotiations(dtFrom='2019-11-12',dtTo='2019-11-14',page=1, lengthPage=10)
	print(negotiations)

	# pdb.set_trace()

