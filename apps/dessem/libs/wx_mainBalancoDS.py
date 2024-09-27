import os
import sys
from datetime import datetime, date, timedelta

sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.apps.dessem.libs.wx_relatorioIntercambio import readIntercambios,getDataDeck
from PMO.scripts_unificados.apps.dessem.libs.wx_pdoSist import readPdoSist

import pdb

def main(pastaDeck=None):
  #pastaDeck ='20220119'
	#for dia in range (140):
	#pastaDeck =  (datetime(2021,9,1) + timedelta(days = dia)).strftime("%Y%m%d")
	
	if pastaDeck is None:
		pastaDeck =  (datetime.today() + timedelta(days = 1)).strftime("%Y%m%d")
		print ('\nLendo deck: ', pastaDeck)
	else:
		pastaDeck
		print ('\nLendo deck: ', pastaDeck)
	try:
		pathEntrada = '/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/dessem/arquivos/'+ pastaDeck + '/entrada/ccee_entrada'
		patSaida    = '/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/dessem/arquivos/'+ pastaDeck + '/entrada/ccee_saida'
		#pathOut     = '/WX2TB/Documentos/fontes/PMO/Gilseu_testes/LeituraDecksDessem/Output'
		pathOutInt  = '/home/admin/Dropbox/WX - Middle/Programas/Dashboards_pbi/Balanco_DS_Oficial/dados/intercambio'
		pathOutPdo  = '/home/admin/Dropbox/WX - Middle/Programas/Dashboards_pbi/Balanco_DS_Oficial/dados/balanco'
		pathConfigRE = '/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/dessem/config_RE'
		dataDeck = getDataDeck(pathEntrada)
		try:
			readIntercambios(pathEntrada, patSaida, pathConfigRE, dataDeck, pathOutInt)
		except:
			print ('Erro na leitura dos INTERCAMBIOS do deck: ', pastaDeck)

		try:
			readPdoSist(patSaida, dataDeck, pathOutPdo)
		except:
			print ('Erro na leitura do PDO SIST do deck: ', pastaDeck)					
		
	except:
		print ('Erro na leitura do deck: ', pastaDeck)

if '__main__' == __name__:
	if len(sys.argv) > 1:
		pastaDeck = sys.argv[1]
		main(pastaDeck)
	else:
		main()