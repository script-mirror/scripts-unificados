import pandas as pd
import pdb

def getCpins(filePath, primeiroDia, ultimoDia):

	cpins_df = pd.read_excel(filePath, sheet_name='PlanilhaUSB.xls',usecols=["DATA", "INC.", "NAT."], index_col="DATA")

	mask1 = cpins_df.index >= primeiroDia
	mask2 = cpins_df.index <= ultimoDia

	cpins_df = cpins_df[mask1 & mask2]
	cpins_df = cpins_df.rename(columns={'INC.':168, 'NAT.':169})

	return cpins_df

def get_all_cpins(filePath):
	cpins_df = pd.read_excel(filePath, sheet_name='PlanilhaUSB.xls', index_col="Unnamed: 0")
	return cpins_df

if __name__ == '__main__':

	import datetime
	import pdb
	primeiroDia = datetime.datetime(2022,5,1)
	ultimoDia = datetime.datetime(2022,5,25)
	filePath = 'C:/Users/cs341052/Downloads/Modelos_Chuva_Vazao_20220526/Modelos_Chuva_Vazao/MPV/SSARR/Sao_Francisco/PlanilhaUSB.xls'
	getCpins(filePath, primeiroDia, ultimoDia)
