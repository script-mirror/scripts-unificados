

import os
import pdb
import datetime

dataInicial = datetime.datetime(2021, 5, 3)
numdias = 30

pdb.set_trace()
for i in range(numdias):
	dt = dataInicial - datetime.timedelta(days=i)
	print(dt.strftime('%d/%m/%Y'))

	os.system('python pluvia.py atualizacaoDiaria data {} preliminar 1'.format(dt.strftime('%d/%m/%Y')))
	os.system('python pluvia.py atualizacaoDiaria data {} preliminar 0'.format(dt.strftime('%d/%m/%Y')))

	# python pluvia.py atualizacaoDiaria data 04/05/2021 preliminar 1
