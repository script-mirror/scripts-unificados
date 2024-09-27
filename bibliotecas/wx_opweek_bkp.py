import datetime
import pdb

#|     days  | weekday |
#|    Monday |    0    |
#|   Tuesday |    1    |
#| Wednesday |    2    |
#|  Thursday |    3    |
#|    Friday |    4    |
#|  Saturday |    5    |
#|    Sunday |    6    |

def getLastSaturday(data):
	if data.weekday() != 5:
		if data.weekday() < 5:
			data = data - datetime.timedelta(days=(data.weekday()+2))
		else:
			data = data - datetime.timedelta(days=1)
	return data

def getLastFriday(data):
	if data.weekday() != 4:
		if data.weekday() < 4:
			data = data - datetime.timedelta(days=(data.weekday()+3))
		else:
			data = data - datetime.timedelta(days=(data.weekday()-4))
	return data

def getLastThursday(data):
	if data.weekday() != 3:
		if data.weekday() < 3:
			data = data - datetime.timedelta(days=(data.weekday()+4))
		else:
			data = data - datetime.timedelta(days=(data.weekday()-3))
	return data


def diffWeek(data1, data2):
	if data1 > data2:
		return (data1 - data2).days/7.0
	else:
		return (data2 - data1).days/7.0

def countElecWeek(data1, data2):
	return diffWeek(data1, data2)

def getLstRound(data):
	return getLastThursday(data)

def getRevAtual(primeiroDiaMes, data):
	return diffWeek(primeiroDiaMes, getLastSaturday(data))

def getPesoSemanas(primeiroDiaMes):
	vetor_pesos = []
	vetor_pesos.append((primeiroDiaMes + datetime.timedelta(days=6)).day)
	mes_pmo = (primeiroDiaMes + datetime.timedelta(days=6)).month
	j=1
	while (primeiroDiaMes + datetime.timedelta(days=7*j)).month == mes_pmo:
		vetor_pesos.append(7)
		j += 1
	vetor_pesos.pop(-1)
	vetor_pesos.append(8 - (primeiroDiaMes + datetime.timedelta(days=7*j)).day)

	while len(vetor_pesos) < 6:
		vetor_pesos.append(0)
	return vetor_pesos



class ElecData:

	# Entrar apenas com data referente ao sabado (semanas eletricas)
	def __init__(self, data):
		
		self.data = data

		self.primeiroDiaMes = getLastSaturday(datetime.date(data.year, data.month, 1))
		if data > self.primeiroDiaMes:
			data_aux = self.data + datetime.timedelta(days=6)
			# Primeiro dia do ano eletrico
			self.primeiroDiaAno = getLastSaturday(datetime.date(data_aux.year, 1, 1))

			# Ultimo dia do ano eletrico
			self.ultimoDiaAno = getLastFriday(datetime.date(data_aux.year, 12, 31))

			# primeiro dia do mes eletrico
			self.primeiroDiaMes = getLastSaturday(datetime.date(data_aux.year, data_aux.month, 1))
		else:
			self.primeiroDiaAno = getLastSaturday(datetime.date(self.data.year, 1, 1))
			self.ultimoDiaAno = getLastFriday(datetime.date(data.year, 12, 31))

		# Revisao atual em da data passada por parametro
		self.atualRevisao = getRevAtual(self.primeiroDiaMes, data)

		# Inicio da semana Eletrica
		self.inicioSemana = self.primeiroDiaMes + datetime.timedelta(days=7*self.atualRevisao)
		
		# Numero de semanas ate a data passada por parametro
		self.numSemanas = countElecWeek(self.primeiroDiaAno, getLastSaturday(self.data)) + 1	# adicionado 1 para incluir a semana atual

		# Numero de semanas ate o primeiro dia do mes eletrico
		self.numSemanasPrimeiroDiaMes = countElecWeek(self.primeiroDiaAno, self.primeiroDiaMes) + 1

		# Numero de semanas que o ano eletrico possui
		self.numSemanasAno = countElecWeek(self.primeiroDiaAno, self.ultimoDiaAno+datetime.timedelta(days=1))

		# Mes eletrico da data passada por parametro
		self.mesRefente = (self.primeiroDiaMes + datetime.timedelta(days=6)).month

	def getPesoSemanas(self):
		return getPesoSemanas(self.primeiroDiaMes)


if __name__ == '__main__':

	
	anoOperacional = ElecData(datetime.date(2020, 12, 2))

	print('primeiroDiaAno', anoOperacional.primeiroDiaAno)
	print('ultimoDiaAno', anoOperacional.ultimoDiaAno)
	print('primeiroDiaMes', anoOperacional.primeiroDiaMes)
	print('inicioSemana', anoOperacional.inicioSemana)
	print('atualRevisao', anoOperacional.atualRevisao)
	print('numSemanas', anoOperacional.numSemanas)
	print('numSemanasAno',anoOperacional.numSemanasAno)
	print('mesRefente',anoOperacional.mesRefente)
	print('numSemanasPrimeiroDiaMes',anoOperacional.numSemanasPrimeiroDiaMes)



