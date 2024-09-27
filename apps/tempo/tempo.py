import os
import pdb
import sys
import datetime

home = os.path.expanduser('~')

path_fontes = "/WX2TB/Documentos/fontes/"
sys.path.insert(1,path_fontes)
from PMO.scripts_unificados.apps.tempo.libs import conversorArquivos,plot

def printHelper():
	hoje = datetime.datetime.now()
	trintaDiasAtras = hoje - datetime.timedelta(days=30)
	dst1 = os.path.join('/WX2TB/Documentos/chuva-vazao/{}/ECMWF-ens/'.format(hoje.strftime('%Y%m%d')))
	dst2 = os.path.join(home,'WX - Middle','NovoSMAP',hoje.strftime('%Y%m%d'),'rodada00z')
	print("python {} gerarArqEntradaSmap data {} modelo 'ecmwf-ens-ext' rodada 0 destino '{}'".format(sys.argv[0],hoje.strftime("%d/%m/%Y"),dst1))
	print("python {} gerarArqEntradaSmapObservado dataInicial {} dataFinal {} destino '{}'".format(sys.argv[0],trintaDiasAtras.strftime("%d/%m/%Y"),hoje.strftime("%d/%m/%Y"),dst1))
	print("python {} gerarArqEntradaPconj data {} modelo 'GEFS' rodada 0 destino '{}'".format(sys.argv[0],hoje.strftime("%d/%m/%Y"),dst2))
	print("python {} plotarMapasPrevisao data {} modelo 'GEFS' rodada 0".format(sys.argv[0],hoje.strftime("%d/%m/%Y"),dst2))
	print("python {} plot_diff_acumul_subbacia modelo ['{}','PCONJUNTO','0'] modelo2 ['{}','PCONJUNTO','0']".format(sys.argv[0],hoje.strftime("%d/%m/%Y"),(hoje-datetime.timedelta(1)).strftime("%d/%m/%Y")))
	
	print('\n\n Para os modelos com use \'gefs_membro\' ou \'ecmwf-ens_membro\' ')
	quit()


if __name__ == '__main__':

	if len(sys.argv) > 1:
		if 'help' in sys.argv or '--h' in sys.argv or '-help' in sys.argv:
			printHelper()

		p_dataReferente = datetime.datetime.now()

		for i in range(len(sys.argv)):
			argumento = sys.argv[i].lower()
			if argumento == 'data':
				p_dataReferente = sys.argv[i+1]
				p_dataReferente = datetime.datetime.strptime(p_dataReferente, "%d/%m/%Y")

			elif argumento == 'datainicial':
				p_dataInicial = sys.argv[i+1]
				p_dataInicial = datetime.datetime.strptime(p_dataInicial, "%d/%m/%Y")

			elif argumento == 'datafinal':
				p_dataFinal = sys.argv[i+1]
				p_dataFinal = datetime.datetime.strptime(p_dataFinal, "%d/%m/%Y")

			elif argumento == 'modelo':
				p_modelo = sys.argv[i+1]

			elif argumento == 'rodada':
				p_rodada = int(sys.argv[i+1])

			elif argumento == 'destino':
				p_dst = os.path.abspath(sys.argv[i+1])
			
			elif argumento == 'modelo2':
				p_modelo2 = sys.argv[i+1]

				

		if sys.argv[1] == 'gerarArqEntradaSmap':
			if p_modelo.lower() == 'gefs_membro':
				membros = [nMembro for nMembro in range(31)]
				for nMembro in membros:
					print('\nGerarando arquivos de entrada para o SMAP para o membro {:0>2}'.format(nMembro))
					conversorArquivos.converterFormatoSmap(modelo=p_modelo, dataRodada=p_dataReferente, rodada=p_rodada, destino=p_dst, membro=nMembro)
			elif p_modelo.lower() == 'ecmwf-ens_membro':
				# Colocando o membro 51 primeiro pois ele sera usado no PCONJUNTO
				membros = [101] #[51] + [nMembro for nMembro in range(51)]
				
				for nMembro in membros:
					print('\nGerarando arquivos de entrada para o SMAP para o membro {:0>2}'.format(nMembro))
					conversorArquivos.converterFormatoSmap(modelo=p_modelo, dataRodada=p_dataReferente, rodada=p_rodada, destino=p_dst, membro=nMembro)
			elif p_modelo.lower() == 'ecmwf-ens-ext':
				membros = [nMembro for nMembro in range(52)]
				for nMembro in membros:
					print('\nGerarando arquivos de entrada para o SMAP para o membro {:0>2}'.format(nMembro))
					conversorArquivos.converterFormatoSmap(modelo=p_modelo, dataRodada=p_dataReferente, rodada=p_rodada, destino=p_dst, membro=nMembro)
			elif p_modelo.lower() == 'gefs_membro-ext':
				membros = [nMembro for nMembro in range(31)]
				for nMembro in membros:
					print('\nGerarando arquivos de entrada para o SMAP para o membro {:0>2}'.format(nMembro))
					conversorArquivos.converterFormatoSmap(modelo=p_modelo, dataRodada=p_dataReferente, rodada=p_rodada, destino=p_dst, membro=nMembro)

			else:
				conversorArquivos.converterFormatoSmap(modelo=p_modelo, dataRodada=p_dataReferente, rodada=p_rodada, destino=p_dst)
		elif sys.argv[1] == 'gerarArqEntradaPconj':
			conversorArquivos.converterFormatoPconjunto(modelo=p_modelo, dataRodada=p_dataReferente, rodada=p_rodada, destino=p_dst)

		elif sys.argv[1] == 'gerarArqEntradaSmapObservado':
			dataRef = p_dataInicial
			while dataRef <= p_dataFinal:
				if dataRef < datetime.datetime(2020,5,28):
					p_modelo = 'merge'
				else:
					p_modelo = 'pobs'

				try:
					conversorArquivos.converterFormatoSmap(modelo=p_modelo, dataRodada=dataRef, destino=p_dst,)
				except:
					print("Problema na geracao do arquivo de entrada para o dia {}".format(dataRef.strftime('%d/%m/%Y')))
				dataRef = dataRef + datetime.timedelta(days=1)

		elif sys.argv[1] == 'plotarMapasPrevisao':

			if p_modelo.lower() == 'ecmwf-ens':
				for nMembro in range(52):
					plot.plotAcumuladosSubbacias(modelo=p_modelo,dataPrevisao=p_dataReferente,rodada=p_rodada,membro=nMembro)
				plot.plotPrevDiaria(modelo=p_modelo,dataPrevisao=p_dataReferente,rodada=p_rodada,membro=51)
			elif p_modelo.lower() == 'ecmwf-ens-ext':
				for nMembro in range(52):
					plot.plotAcumuladosSubbacias(modelo=p_modelo,dataPrevisao=p_dataReferente,rodada=p_rodada,membro=nMembro)
			elif p_modelo.lower() == 'gefs-membros':
				for nMembro in range(31):
					plot.plotAcumuladosSubbacias(modelo=p_modelo,dataPrevisao=p_dataReferente,rodada=p_rodada,membro=nMembro)
			elif p_modelo.lower() == 'gefs-membros-ext':
				for nMembro in range(31):
					plot.plotAcumuladosSubbacias(modelo=p_modelo,dataPrevisao=p_dataReferente,rodada=p_rodada,membro=nMembro)
			elif p_modelo.lower() == 'gefs-ext':
				plot.plotAcumuladosSubbacias(modelo=p_modelo,dataPrevisao=p_dataReferente,rodada=p_rodada)
			elif p_modelo.lower() == 'psat':
				plot.plotPrevDiaria(modelo=p_modelo,dataPrevisao=p_dataReferente,rodada=p_rodada)
			else:
				plot.plotAcumuladosSubbacias(modelo=p_modelo,dataPrevisao=p_dataReferente,rodada=p_rodada)
				plot.plotPrevDiaria(modelo=p_modelo,dataPrevisao=p_dataReferente,rodada=p_rodada)
		
		elif sys.argv[1] == 'plot_diff_acumul_subbacia':

			html_tb_maps = plot.plot_diff_acumul_subbacia(eval(p_modelo), eval(p_modelo2))


	else:
		printHelper()


