# -*- coding: UTF-8 -*-

import os
import sys
import pdb
import time
import datetime

sys.path.insert(1,"/WX2TB/Documentos/fontes")
from PMO.scripts_unificados.apps.bot_whatsapp.lib import wx_dbWhats

class DbMensagens:
	def __init__(self, dbPath='lib/db_whats.db'):


		self.dbPath = dbPath

		self.datetimeFormat = '%Y-%m-%d %H:%M:%S'
		self.dateFormat = '%Y-%m-%d'

		self.delay = 30

		self.db = wx_dbWhats.db_sql_lite()

		if not os.path.exists(self.dbPath):
			self.db.create_table()

		self.tb_mensagens = self.db.getSchema()
		
	def querySelect(self, sql):
		self.db.connect()
		result = self.db.conn.execute(sql).fetchall()
		return result

	def query(self, sql):
		self.db.connect()
		self.db.conn.execute(sql)
		


	def inserirMsg(self, destinatario, msg, arquivo):

		horario = datetime.datetime.now()

		sql = self.tb_mensagens.insert().values(HR_INSERT=horario,TX_DESTINATARIO=destinatario,TX_MENSAGEM=msg,TX_ARQUIVO=arquivo)

		while 1:
			try:
				self.query(sql)
				break
			except Exception as e:
				print(e)
				print("Nova tentativa em {} segundos!".format(self.delay))
				time.sleep(self.delay)


	def verificarMsgs(self, imprimirMsgs=False):

		sql = self.tb_mensagens.select()

		while 1:
			try:
				msgs = self.querySelect(sql)
				break
			except Exception as e:
				print(e)
				print("Nova tentativa em {} segundos!".format(self.delay))
				time.sleep(self.delay)
		
		if imprimirMsgs:
			for msg in msgs:
				print(msg)

		return msgs


class WhatsappBot:
	def __init__(self):

		# Carrega o banco de dados default
		self.db_msg = DbMensagens()


	def inserirMsgFila(self, dst, msg, file):
		self.db_msg.inserirMsg(dst, msg, file)
		self.db_msg.verificarMsgs(imprimirMsgs=True)


def printHelper():

	print('Help:')
	print("python {} inserirMsgFila dst '1199999999' msg 'Msg teste' file '' ".format(sys.argv[0]))


if __name__ == '__main__':

	if len(sys.argv) > 1:

		# passar o print do help pra quando nao entrar com nenhum parametro
		if 'help' in sys.argv or '--h' in sys.argv or '-help' in sys.argv:
			printHelper()
			quit()

		# sys.argv[0] e o nome do arquivo python
		for i in range(1, len(sys.argv[1:])):
			# sempre transformar os parametros pra lowercase
			argumento = sys.argv[i].lower()

			if argumento == 'inserirMsgFila':
				wz = WhatsappBot()

			if argumento == 'dst':
				dst = sys.argv[i+1]

			elif argumento == 'msg':
				msg = sys.argv[i+1]

			elif argumento == 'file':
				file = sys.argv[i+1]


		wz = WhatsappBot()
		if 'inserirMsgFila' in sys.argv:
			wz.inserirMsgFila(dst, msg, file)

	else:
		printHelper()






