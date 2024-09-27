import pdb
import datetime
import pandas as pd

def importarSaidaSmap(smapPath, dataInicialSmap, postosIncSmap):

	vazao = pd.read_csv(smapPath, delim_whitespace=True, header=None)
	vazao.set_index(0, inplace=True)

	idx = pd.date_range(dataInicialSmap, dataInicialSmap+datetime.timedelta(days=len(vazao.columns)-1))
	vazao.columns = idx

	vazaoInc = vazao.loc[vazao.index.isin(postosIncSmap)]
	vazaoNat = vazao.loc[~vazao.index.isin(postosIncSmap)]

	return vazaoNat, vazaoInc