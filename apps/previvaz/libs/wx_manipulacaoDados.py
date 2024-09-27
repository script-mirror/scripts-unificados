import pdb
import datetime

import numpy as np
import pandas as pd

def fechamentoSemanas(vazao, dtUltimaRev, dtRevInteresse, tipoFechamento):

	dt = pd.Timestamp(dtUltimaRev-datetime.timedelta(days=1))
	dtFinal = pd.Timestamp(dtRevInteresse + datetime.timedelta(days=2*7))

	while dt < dtFinal:
		if dt not in vazao.columns:
			vazao[dt] = np.nan
		idx_nan = vazao[dt].isna()

		if idx_nan.any():
			if tipoFechamento == 5:
				vazao.loc[idx_nan, dt] = vazao.loc[idx_nan, dt-datetime.timedelta(days=1)] + 0.8*(vazao.loc[idx_nan, dt-datetime.timedelta(days=1)]-vazao.loc[idx_nan, dt-datetime.timedelta(days=2)])
				
				# Evitar que alguma vazão tenha valor igual ou inferior a 0
				vazao[vazao <= 0 ] = 1

		dt = dt + datetime.timedelta(days=1)

	# Retorna apenas até a data final do arquivo str (1 semana a frente do )
	return vazao[pd.date_range(min(vazao.columns), dtFinal-datetime.timedelta(days=1))]


def aplicarTempoViagem(vazao_nat, vazao_inc, dtInicial):

	tempoViagem = {18:18, 33:30, 99:46, 241:28, 261:28, 34:5, 243:7, 245:48, 154:32, 285:23, 246:56, 63:0}
	vaz_aux = {}

	for posto in tempoViagem:
		dt = pd.Timestamp(dtInicial + datetime.timedelta(days=1))
		vaz_aux[posto] = {}
		tv = tempoViagem[posto]

		while dt <= max(vazao_nat.columns):
			vaz_aux[posto][dt] = (((int(tv/24)+1)*24-tv)*vazao_nat.loc[posto, dt-datetime.timedelta(days=int(tv/24))] + (tv-(int(tv/24)*24))*vazao_nat.loc[posto, dt-datetime.timedelta(days=(int(tv/24)+1))])/24
			dt = dt + datetime.timedelta(days=1)

	propagacaoPostos = {}
	propagacaoPostos[34] = [18, 33, 241, 261, 99]
	propagacaoPostos[245] = [34,243]
	propagacaoPostos[246] = [245,154]
	propagacaoPostos[266] = [246,63]

	for posto in propagacaoPostos:
		dt = pd.Timestamp(dtInicial + datetime.timedelta(days=1))
		while dt <= max(vazao_nat.columns):
			vazao_nat.loc[posto, dt] = vazao_inc.loc[posto, dt]

			for p in propagacaoPostos[posto]:
				vazao_nat.loc[posto, dt] += vaz_aux[p][dt]
			
			dt = dt + datetime.timedelta(days=1)

	return vazao_nat


def propagarPostosAcomph(vazao_nat, vazao_inc, dtInicial):

	tempoViagem = {18:18, 33:30, 99:46, 241:28, 261:28, 34:5, 243:7, 245:48, 154:32, 285:23, 246:56, 63:0}
	vazaoPropagada = {}

	for posto in tempoViagem:
		dt = pd.Timestamp(dtInicial)
		vazaoPropagada[posto] = {}
		tv = tempoViagem[posto]

		while dt <= max(vazao_nat.columns):
			vazaoPropagada[posto][dt] = (((int(tv/24)+1)*24-tv)*vazao_nat.loc[posto, dt-datetime.timedelta(days=int(tv/24))] + (tv-(int(tv/24)*24))*vazao_nat.loc[posto, dt-datetime.timedelta(days=(int(tv/24)+1))])/24
			dt = dt + datetime.timedelta(days=1)

	propagacaoPostos = {}
	propagacaoPostos[34] = [18, 33, 241, 261, 99]
	propagacaoPostos[245] = [34,243]
	propagacaoPostos[246] = [245,154]
	propagacaoPostos[266] = [246,63]

	for posto in propagacaoPostos:
		dt = pd.Timestamp(dtInicial)
		while dt.weekday() != 5:
			vazao_nat.loc[posto, dt] = vazao_inc.loc[posto, dt]

			for p in propagacaoPostos[posto]:
				vazao_nat.loc[posto, dt] += vazaoPropagada[p][dt]
			
			dt = dt + datetime.timedelta(days=1)

	return vazao_nat

