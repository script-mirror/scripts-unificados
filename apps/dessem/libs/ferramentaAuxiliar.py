import os
import pdb
import sys
import datetime

diretorioRaiz = os.path.abspath('../../../')
pathLibUniversal = os.path.join(diretorioRaiz,'bibliotecas')
sys.path.insert(1, pathLibUniversal)

sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.bibliotecas import wx_dbLib

def regex():
	mnemonico = 'operlpp'
	formato = 'XX  XXX XXX  xx xx x xx xx x x'
	columns = 'mnemonico ute  ug  di hi m df hf m F'
	# para os arquivos do PDO
	formato = 'xxxxx;xxxxxxx;xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx;xxxxxxx;xxxxxxxxx;xxxxxxxxxxxx;xxxxxxxxxxxxx;xxxxxxxxxxxxx;xxxxxxx;xxxxxxx;xxxxxxxxxxxx;xxxxxxxxxxxxxxxxxxxx;'
	columns = 'iper num nome resp numre somflux limitetab limiteuti tippar numpar valparam multip'

	output1 = ''
	output2 = ''
	letraAnterior = ''
	tamanho = 0
	for letra in formato:
		if letraAnterior not in ['', ' '] and letraAnterior != letra:
			output1 += '(.{' + str(tamanho) + '})'
			output2 += '{:>' + str(tamanho) + '}'
			tamanho = 1
		else:
			tamanho += 1

		if letra == ' ':
			tamanho = 0
			output1 += ' '
			output2 += ' '

		letraAnterior = letra

	output1 += '(.{' + str(tamanho) + '})(.*)'
	output2 += '{:>' + str(tamanho) + '}'


	print("\tblocos['"+mnemonico+"'] = {'campos':[")
	for name in columns.split():
		print(f"\t\t\t\t\t\t'{name}',")
	print('\t\t\t\t],')
	print(f"\t\t\t\t'regex':'{output1}',")
	print(f"\t\t\t\t'formatacao':'{output2}'" + '}')


def code(mnemonico):

	code = '# Bloco {}\n'.format(mnemonico)
	code += "bloco{} = extrairInfoBloco(entdados, '{}', infoBlocos['{}']['regex'])\n".format(mnemonico, mnemonico, mnemonico)
	code += "df_bloco{} = pd.DataFrame(bloco{}, columns=infoBlocos['{}']['campos'])\n".format(mnemonico, mnemonico, mnemonico)
	code += "bloco_{} = getCabecalhos('{}')\n".format(mnemonico, mnemonico)
	code += "bloco_{} += formatarBloco(df_bloco{}, infoBlocos['{}']['formatacao'])\n".format(mnemonico, mnemonico, mnemonico)
	code += "bloco_{} += getRodapes('{}')\n".format(mnemonico, mnemonico)
	code += "gravarArquivo(pathEntdados, bloco_{})\n".format(mnemonico)

	print(code)
	print()


def verificarDatasFaltantes(dtInicial,ftFinal):

	wxdb = wx_dbLib.WxDataB()
	dtFormat = wxdb.getDateFormat()
	
	if ftFinal > datetime.datetime.now():
		ftFinal = datetime.datetime.combine(datetime.date.today(), datetime.datetime.min.time())

	sql = '''
			SELECT
				DT_REFERENTE
			from
				TB_CADASTRO_DESSEM
			WHERE
				DT_REFERENTE >= \'{}\'
				and DT_REFERENTE <= \'{}\'
				AND TX_FONTE = 'ccee'
			GROUP BY
				DT_REFERENTE'''.format(dtInicial.strftime(dtFormat), ftFinal.strftime(dtFormat))
	
	answer = wxdb.requestServer(sql)
	diasInseridos = [r[0] for r in answer]

	diasFaltantes = []
	dt = dtInicial.date()
	while dt <= ftFinal.date():
		if dt not in diasInseridos:
			diasFaltantes.append(dt)
		dt += datetime.timedelta(days=1)

	return diasFaltantes


if __name__ == '__main__':
	# regex()

	mesDesejado = datetime.datetime(2022,1,1)
	verificarDatasFaltantes(mesDesejado)


	# for mnemonico in ['MT']:
	# 	code(mnemonico)



