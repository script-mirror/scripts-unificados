import os
import pdb
import shutil
import datetime

def gerarRespot(data, pathArquivos):

	pathArqEntrada = os.path.join(pathArquivos, data.strftime('%Y%m%d'), 'entrada')
	pathArqSaida = os.path.join(pathArquivos, data.strftime('%Y%m%d'), 'saida')
	if not os.path.exists(pathArqSaida):
		os.makedirs(pathArqSaida)

	pathEntdadosOut = os.path.join(pathArqSaida,'respot.dat')
	# fileOut = open(pathEntdadosOut, 'w')
	# fileOut.close()

	pathEntdadosIn = os.path.join(pathArqEntrada, 'blocos', 'RESPOT.DAT')
	# entdados = leituraArquivo(pathEntdadosIn)

	shutil.copyfile(pathEntdadosIn, pathEntdadosOut)
	print('respot.dat: {}'.format(pathEntdadosOut))
	return pathEntdadosOut


def gerarRespotDef(data, pathArquivos):
	dataRodada = data
	data = data + datetime.timedelta(days=1)

	pathArqEntrada = os.path.join(pathArquivos, data.strftime('%Y%m%d'), 'entrada')

	pathArqSaida = os.path.join(pathArquivos, dataRodada.strftime('%Y%m%d'), 'definitiva')
	if not os.path.exists(pathArqSaida):
		os.makedirs(pathArqSaida)

	src = os.path.join(pathArqEntrada, 'ons_entrada_saida', 'respot.dat')
	dst = os.path.join(pathArqSaida,'respot.dat')

	shutil.copy(src, dst)
	print('respot.dat: {}'.format(dst))