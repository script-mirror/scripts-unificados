import os
import re
import pdb
import sys
import datetime 

from PIL import Image

# pip install pillow
def imagemColorida(img_path):
	img = Image.open(img_path).convert('RGB')
	w, h = img.size
	for i in range(w):
		for j in range(h):
			# tentativa de pular o logo da raizen
			if i >= 490 and j >= 510:
				return False
			r, g, b = img.getpixel((i,j))
			if r != g != b:
				return True
	return False


def verificarArquivPath(pathImgs, regex):
	if not os.path.exists(pathImgs):
		print('Diretorio {} nao existe!'.format(pathImgs))
		quit()

	print('Verificando arquivos na pasta:')
	print('{}'.format(pathImgs))
	
	# Condicao caso for uma lista de regex
	if type(regex) == type([]):
		nomeImgs = []
		for i_regex in regex:
			nomeImgs += [f for f in os.listdir(pathImgs) if re.search(i_regex, f)]
	else:
		nomeImgs = [f for f in os.listdir(pathImgs) if re.search(regex, f)]

	nomeImgs.sort()

	if len(nomeImgs) > 0 :
		verificacao = True
	else:
		print("Nenhum arquivo encontrado com esse padrao de nome!")
		verificacao = False


	for nome in nomeImgs:
		pathImg = os.path.join(pathImgs, nome)

		if not imagemColorida(pathImg):
			print('(Erro) {}'.format(nome))
			verificacao = False
		else:
			print('(OK) {}'.format(nome))

	return verificacao


def printHelper():
	dt_hoje = datetime.datetime.now() 
	pathExemple = '/WX2TB/Documentos/saidas-modelos/gefs/2021020400/semana-energ'
	print('\nExemplos de como utilizar:')
	print("python {} path \'{}\' regex \'.gif\'".format(sys.argv[0], pathExemple))
	print('')
	quit()

if __name__ == '__main__':

	if len(sys.argv) > 1:
		if 'help' in sys.argv or '-h' in sys.argv or '-help' in sys.argv:
			printHelper()

		for i, argumento in enumerate(sys.argv):
			if argumento == 'path':
				pathImgs = os.path.abspath(sys.argv[i+1].replace('\'', ''))
			
			if argumento == 'regex':
				regex = sys.argv[i+1].replace('\'', '')

	else:
		printHelper()

	if not 'regex' in sys.argv:
		print('Especificar a regex')
		quit()

	verificarArquivPath(pathImgs, regex)
