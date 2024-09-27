import os
import re
import pdb
import sys
import glob
import math
import shutil
import datetime

import shapely.geometry
import matplotlib.pyplot as plt
import pandas as pd
import xarray as xr

dirArqCoordenadasPsat = os.path.abspath('/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/tempo/arquivos/auxiliares/CV')
dirPrevisoesModelosRaizen = os.path.abspath('/WX4TB/Documentos/saidas-modelos')
dirpreciptacaoObservada = os.path.abspath('/WX2TB/Documentos/dados/gpm-1h')
dirPreciptacaoImerge = os.path.abspath('/WX2TB/Documentos/dados/imerg_diario')
dirPreciptacaoMerge = os.path.abspath('/WX2TB/Documentos/dados/merge')


bacias = {}
bacias['Doce'] = {"Candonga":"PSATCAN","Sa Carvalho":"PSATSAC","Porto Estrela":"PSATPES","VIA Mascarenhas":"PSATMAS"}
bacias['Grande'] = {"Inc_AV_Marimbondo":"PSATAGV", "Camargos":"PSATCMG","Fazenda_Capao_Escuro":"PSATCES","E_cunha":"PSATELC","Funil":"PSATFUN","Furnas":"PSATFUR","Marimbondo":"PSATMRB","Paraguacu":"PSATPRG","Passagem":"PSATPAS","Porto_dos_Buenos":"PSATPTB","Porto_Colombia":"PSATPTC"}
bacias['Iguacu'] = {"foz_do_areia":"PSATFZA","jordao_segredo":"PSATJSG","salto_caxias":"PSATSCX","sta_clara":"PSATSCL","uniao_da_vitoria":"PSATUVT","BIguacu_VIA":"PSATBIGU"}
bacias['Itaipu'] = {"balsa_santa_maria":"PSATBSM","florida_estrada":"PSATFLE","itaipu":"PSATITP","ivinhema":"PSATIVM","porto_taquara":"PSATPTQ"}
bacias['Madeira'] = {"Amaru_Mayu":"PSATAMY","Guajara_Mirim":"PSATGJR","Principe_da_Beira":"PSATPRNC","Sto_Antonio":"PSATSANT","UHE_Dardanelos":"PSATDARD","UHE_Guapore":"PSATGUAP","UHE_Jirau":"PSATJIRA","UHE_Rondon_II":"PSATROND","UHE_Samuel":"PSATSAMU"}
bacias['Outras Sul'] = {"UHE_14_Julho_INC":"PSATXIVJ","UHE_D_Francisca_INC":"PSATDFRC","UHE_Passo_Real_INC":"PSATREAL","UHE_Castro_Alves":"PSATCTAV","UHE_Ernestina_ajust":"PSATERNT","SaltoPilao":"PSATSTPL","GPSouza_ajust":"PSATCVCH"}
bacias['Outras_NNE'] = {"Balbina":"PSATBALB","CuruaUna":"PSATCUNA","Santo Antônio do Jari":"PSATJARI","FerreiraGomes":"PSATFGOM","Irape":"PSATIRAP","Itapebi":"PSATITBI","UHE_BoaEsperanca":"PSATUBE","Pedra do Cavalo":"PSATPDC"}
bacias['Outras_SE'] = {"Sta Clara MG":"PSATSTCL","Rosal":"PSATROSA"}
bacias['Paraguai'] = {"Itiquira":"PSATITQ","Jauru":"PSATJAU","Manso":"PSATMAN","PPedra":"PSATPTP"}
bacias['Parana'] = {"Espora":"PSATESP","Salto_Verdinho":"PSATSRC","Foz_do_Rio_Claro":"PSATFRCL","Ilha_Solteira_Tres_Irmaos":"PSATISOT","Jupia":"PSATJUP","Sao_Domingos":"PSATSDG","Fazenda_Buriti":"PSATFZBT","Porto_Primavera":"PSATPPRA"}
bacias['Paranaiba'] = {"corumba1":"PSATCBI","corumba4":"PSATCBIV","emborcacao":"PSATEMB","itumbiara":"PSATIMBR","nova_ponte":"PSATNPTE","Abaixo_da_Barra_do_Rio_Verde":"PSATARV","serra_do_facao":"PSATSFC","Sao_Simao":"PSATSSM"}
bacias['Paranapanema'] = {"CanoasI_SMAP2.0":"PSATCNI","Capivara_SMAP2.0":"PSATCPV","Chavantes_SMAP2.0":"PSATCHT","Jurumirim_SMAP2.0":"PSATJUR","Maua":"PSATMAU","Rosana_SMAP2.0":"PSATROS"}
bacias['Sao Francisco'] = {"Queimado":"PSATQMD","RetiroBaixo":"PSATRBX","Inc_SaoFrancisco_SaoRomao":"PSATSFR","SaoRomao":"PSATSRM","IncrementalTresMarias":"PSATTMR","Boqueirao":"PSATBOQ"}
bacias['Teles Pires'] = {"Colider":"PSATCOLI","SManoel":"PSATSAEL"}
bacias['Tiete'] = {"Barra_Bonita":"PSATBBO","Edgard_Souza":"PSATESZ","Ibitinga":"PSATIBT","Nova_Avanhandava":"PSATNAV"}
bacias['Tocantins'] = {"bandeirantes":"PSATBTE","Conceicao_do_Araguaia":"PSATARAG","Lajeado":"PSATLAJ","Porto_Real":"PSATPTRL","Estreito":"PSATLJET","Tucurui":"PSATUCR","Serra_da_Mesa":"PSATSME"}
bacias['Uruguai'] = {"Barra_Grande":"PSATBGR","Campos_Novos":"PSATCNV","Foz_do_Chapeco":"PSATFCH","Ita":"PSATITA","Machadinho":"PSATMCD","Monjolinho":"PSATMOJ","Quebra_Queixo":"PSATQQX","Passo_Sao_Joao":"PSATPSJ"}
bacias['Xingu'] = {"Boa Sorte":"PSATBSOR","Boa Esperança":"PSATBESP","Pimental":"PSATPIME"}
bacias['Paraiba do Sul'] = {"VIA_SantaBranca":"PSATSBR","Jaguari":"PSATJAG","INC_Funil":"PSATFUNI","INC_SantaCecilia":"PSATSACE","Picada":"PSATPIC","Sobragi":"PSATSOGI","Ilha dos Pombos_Simplicio":"PSATIPSI","VIA_Tocos_Lajes":"PSATTCLJ","BarradoBrauna":"PSATBRAU"}
bacias['Santa Maria da Vitoria'] = {"UHE_Suica_ajusteGEFS_rev" : "PSATSUIC"}

pwd = os.path.abspath(sys.path[0])

if os.path.basename(pwd) == 'libs':
	tempoAppDir = os.path.dirname(pwd)
else:
	tempoAppDir = pwd

arquivos = os.path.join(tempoAppDir,'arquivos')
dir_bnl = os.path.join(arquivos, 'auxiliares', 'contornos_bacias')

def plotBacia():

	# ncModelo = r'C:/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/tempo/arquivos/auxiliares/CV/arquivosNetcf/ec-ens-ext-br_2022010600_00.nc'
	arquivoNc = os.path.abspath('/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/tempo/arquivos/auxiliares/CV/arquivosNetcf')
	ncModelo = os.path.join(arquivoNc,'20220210000000-d01-oper-fc_12z-12z.nc') 

	contornosTodasBacias = {}
	for bac in bacias:
		for num, arq_bnl in enumerate(glob.glob(os.path.join(dir_bnl, bac, '*'))):
			arq = os.path.basename(arq_bnl)
			subBacia = arq.replace('.bln','')

			if subBacia not in bacias[bac]:
				continue

			try:
				df_coordenadas = pd.read_csv(arq_bnl, parse_dates=True, skiprows=1, delimiter=',', names=['lon', 'lat', 'aa'])
			except:
				df_coordenadas = pd.read_csv(arq_bnl, parse_dates=True, skiprows=1, delimiter=',', names=['lon', 'lat'])


			contornoBacia = [tuple(l) for l in df_coordenadas[['lon','lat']].values]
			contornosTodasBacias[bacias[bac][subBacia]] = shapely.geometry.Polygon(contornoBacia)
	
	x,y = contornosTodasBacias['PSATQQX'].exterior.xy
	xMax = max(x)
	xMin = min(x)
	yMax = max(y)
	yMin = min(y)

	modelo = xr.open_dataset(ncModelo)

	f1 = modelo.longitude.values < xMax + 0.5
	f2 = modelo.longitude.values > xMin - 0.5
	
	f3 = modelo.latitude.values < yMax + 0.5
	f4 = modelo.latitude.values > yMin - 0.5

	lonFiltrada = modelo.longitude[(f1) & (f2)]
	latFiltrada = modelo.latitude[(f3) & (f4)]

	for lo in lonFiltrada:
		for la in latFiltrada:
			plt.scatter(float(lo.values),float(la.values),color='yellow')

	plt.plot(x,y)
	plt.show()


def atualizarCoordenadasBacias(modelo=''):

	if modelo == '':
		modelo = ['merge','gpm','imerg','gefs','gefs-ext','gefs_membro','gefs_membro-ext','gfs','eta','ecmwf','ecmwf-ens','ecmwf-ens_membro','ecmwf-ens-ext','ecmwf-orig']
	else:
		modelo = [modelo]

	# Arquivos vindos do sintegre
	pathArquivosSaida = os.path.abspath('/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/tempo/arquivos/auxiliares/CV')
	arquivoNc = os.path.abspath('/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/tempo/arquivos/auxiliares/CV/arquivosNetcf')

	path_r_prevGefs = os.path.join(arquivoNc,'gefs.acumul12z-12z.pgrb2a.0p50f15.nc')
	path_r_prevGefsExt = os.path.join(arquivoNc,'gefs.acumul12z-12z.pgrb2af_estendido.nc')
	path_r_prevGefsMembroExt = os.path.join(arquivoNc,'gefs_membro00.acumul12z-12z.pgrb2af_estendido.nc')
	path_r_prevGefsMembro = os.path.join(arquivoNc,'gefs_membro00.acumul12z-12z.pgrb2af.nc')
	path_r_prevGfs = os.path.join(arquivoNc,'gfs-regrid.acumul12z-12z.pgrb2f15_regrided.nc')
	path_r_prevEta = os.path.join(arquivoNc,'pp20200102_0036.ctl.nc')
	path_r_prevEC = os.path.join(arquivoNc,'ecm-br_2022010500_12z-12z.nc')
	path_r_prevEC_ens = os.path.join(arquivoNc,'D1D12270000_00.nc') # Modelo que compoe o PCONJUNTO
	path_r_prevEC_ensMembro = os.path.join(arquivoNc,'A1F02150000_101.nc')

		# PSATGUAP;-58.8,14.8;-59.2,14.8
		# PSATCVCH;-49.2,-25.2;-48.8,-25.2
		# PSATROSA;-42.0,-20.8;-41.6,-20.8s
		# PSATJAU;-58.8,-15.2;-58.8,-14.8;-58.4,-14.8
		# PSATQQX;-52.4,-26.4;-52.0,-26.8;-51.6,-26.8
		
	path_r_prevEC_ensMembroExt = os.path.join(arquivoNc,'A1EF_all_01160000_00_regrid.nc')
	path_r_prevEC_orig = os.path.join(arquivoNc,'20220210000000-d01-oper-fc_12z-12z.nc') # modelo que a raizen roda diariamente
	path_r_prevGpm = os.path.join(arquivoNc,'MERGE_CPTEC_2021121507.acumul12z.nc')
	path_r_histImerg = os.path.join(arquivoNc,'imerg_daily_20220427.nc')
	path_r_histmerg = os.path.join(arquivoNc,'prec_2020.05.13.nc')
 

	coordenadasModelos = {}

	if 'imerg' in modelo:
		print('Importando coordenadas do modelo imerg')
		coordenadasModelos['imerg'] = []
		imerge = xr.open_dataarray(path_r_histImerg)
		for ilat, lat in enumerate(imerge.lat.values):
			if lat < -56.95 or lat > 13.05:
				continue
			for ilon, lon in enumerate(imerge.lon.values):
				if lon < -82.15 or lon > -32.95:
					continue
				coordenadasModelos['imerg'].append(shapely.geometry.Point(lon, lat))
	
	elif 'merge' in modelo:
		print('Importando coordenadas do modelo merge')
		coordenadasModelos['merge'] = []
		merge = xr.open_dataarray(path_r_histmerg)
		for ilat, lat in enumerate(merge.lat.values):
			for ilon, lon in enumerate(merge.lon.values):
				coordenadasModelos['merge'].append(shapely.geometry.Point(lon, lat))

	elif 'gpm' in modelo:
		print('Importando coordenadas do modelo gpm')
		coordenadasModelos['gpm'] = []
		gpm = xr.open_dataarray(path_r_prevGpm)
		for ilat, lat in enumerate(gpm.lat.values):
			for ilon, lon in enumerate(gpm.lon.values):
				coordenadasModelos['gpm'].append(shapely.geometry.Point(lon, lat))

	elif 'gefs' in modelo:
		print('Importando coordenadas do modelo gefs')
		coordenadasModelos['gefs'] = []
		gefs = xr.open_dataarray(path_r_prevGefs)
		for ilat, lat in enumerate(gefs.latitude.values):
			for ilon, lon in enumerate(gefs.longitude.values):
				lon = lon-360
				coordenadasModelos['gefs'].append(shapely.geometry.Point(lon, lat))

	elif 'gefs-ext' in modelo:
		print('Importando coordenadas do modelo gefs-ext')
		coordenadasModelos['gefs-ext'] = []
		gefs = xr.open_dataarray(path_r_prevGefsExt)
		for ilat, lat in enumerate(gefs.lat.values):
			for ilon, lon in enumerate(gefs.lon.values):
				lon = lon-360
				coordenadasModelos['gefs-ext'].append(shapely.geometry.Point(lon, lat))

	elif 'gefs_membro' in modelo:
		print('Importando coordenadas do modelo gefs_membro')
		coordenadasModelos['gefs_membro'] = []
		gefsMembro = xr.open_dataarray(path_r_prevGefsMembro)
		for ilat, lat in enumerate(gefsMembro.lat.values):
			for ilon, lon in enumerate(gefsMembro.lon.values):
				lon = lon-360
				coordenadasModelos['gefs_membro'].append(shapely.geometry.Point(lon, lat))

	elif 'gefs_membro-ext' in modelo:
		print('Importando coordenadas do modelo gefs_membro-ext')
		coordenadasModelos['gefs_membro-ext'] = []
		gefsMembroExt = xr.open_dataarray(path_r_prevGefsMembroExt)
		for ilat, lat in enumerate(gefsMembroExt.lat.values):
			for ilon, lon in enumerate(gefsMembroExt.lon.values):
				lon = lon-360
				coordenadasModelos['gefs_membro-ext'].append(shapely.geometry.Point(lon, lat))

	elif 'gfs' in modelo:
		print('Importando coordenadas do modelo gfs')
		coordenadasModelos['gfs'] = []
		gfs = xr.open_dataarray(path_r_prevGfs)
		for ilat, lat in enumerate(gfs.lat.values):
			for ilon, lon in enumerate(gfs.lon.values):
				coordenadasModelos['gfs'].append(shapely.geometry.Point(lon, lat))

	elif 'eta' in modelo:
		print('Importando coordenadas do modelo eta')
		coordenadasModelos['eta'] = []
		eta = xr.open_dataarray(path_r_prevEta)
		for ilat, lat in enumerate(eta.lat.values):
			for ilon, lon in enumerate(eta.lon.values):
				coordenadasModelos['eta'].append(shapely.geometry.Point(lon, lat))

	elif 'ecmwf' in modelo:
		print('Importando coordenadas do modelo ecmwf')
		coordenadasModelos['ecmwf'] = []
		ecmwf = xr.open_dataarray(path_r_prevEC,decode_times=False)
		for ilat, lat in enumerate(ecmwf.g0_lat_1):
			for ilon, lon in enumerate(ecmwf.g0_lon_2):
				coordenadasModelos['ecmwf'].append(shapely.geometry.Point(lon, lat))
		
	elif 'ecmwf-ens' in modelo:
		print('Importando coordenadas do modelo ecmwf-ens')
		coordenadasModelos['ecmwf-ens'] = []
		ecmwf = xr.open_dataset(path_r_prevEC_ens).TP_GDS0_SFC
		for ilat, lat in enumerate(ecmwf.g0_lat_0):
			for ilon, lon in enumerate(ecmwf.g0_lon_1):
				coordenadasModelos['ecmwf-ens'].append(shapely.geometry.Point(lon, lat))

	elif 'ecmwf-ens_membro' in modelo:
		print('Importando coordenadas do modelo ecmwf-ens_membro')
		coordenadasModelos['ecmwf-ens_membro'] = []
		ecmwf_membro = xr.open_dataset(path_r_prevEC_ensMembro).var228
		for ilat, lat in enumerate(ecmwf_membro.latitude):
			for ilon, lon in enumerate(ecmwf_membro.longitude):
				coordenadasModelos['ecmwf-ens_membro'].append(shapely.geometry.Point(lon, lat))

	elif 'ecmwf-ens-ext' in modelo:
		print('Importando coordenadas do modelo ecmwf-ens-ext')
		coordenadasModelos['ecmwf-ens-ext'] = []
		ecmwf_membro = xr.open_dataset(path_r_prevEC_ensMembroExt).var228
		for ilat, lat in enumerate(ecmwf_membro.lat):
			for ilon, lon in enumerate(ecmwf_membro.lon):
				coordenadasModelos['ecmwf-ens-ext'].append(shapely.geometry.Point(lon, lat))

	elif 'ecmwf-orig' in modelo:
		print('Importando coordenadas do modelo ecmwf-orig')
		coordenadasModelos['ecmwf-orig'] = []
		ecmwf = xr.open_dataset(path_r_prevEC_orig)
		for ilat, lat in enumerate(ecmwf.lat_0):
			for ilon, lon in enumerate(ecmwf.lon_0):
				lon = lon-360
				coordenadasModelos['ecmwf-orig'].append(shapely.geometry.Point(lon, lat))

	print('Importacao da base de modelos finalizada')
	print('\nIniciando a importacao dos contornos')
	contornosTodasBacias = {}
	for bacia in bacias:
		print(bacia)
		for num, arq_bnl in enumerate(glob.glob(os.path.join(dir_bnl, bacia, '*'))):
			arq = os.path.basename(arq_bnl)
			subbacia = arq.replace('.bln','')

			if subbacia not in bacias[bacia]:
				continue

			try:
				df_coordenadas = pd.read_csv(arq_bnl, parse_dates=True, skiprows=1, delimiter=',', names=['lon', 'lat', 'aa'])
			except:
				df_coordenadas = pd.read_csv(arq_bnl, parse_dates=True, skiprows=1, delimiter=',', names=['lon', 'lat'])

			contornoBacia = [tuple(l) for l in df_coordenadas[['lon','lat']].values]
			contornosTodasBacias[subbacia] = shapely.geometry.Polygon(contornoBacia)

	if not os.path.exists(pathArquivosSaida):
		os.makedirs(pathArquivosSaida)

	print('Importacao dos contornos finalizada')
	print('\nIniciando a verificacao de cada um das coordenadas')
	
	composicaoSubbacia = {}
	for modelo in coordenadasModelos:
		composicaoSubbacia[modelo] = {}
		for coord in coordenadasModelos[modelo]:
			for bacia in bacias:
				for subbacia in bacias[bacia]:
					if contornosTodasBacias[subbacia].contains(coord):
						codigoPsat = bacias[bacia][subbacia]
						if codigoPsat not in composicaoSubbacia[modelo]:
							composicaoSubbacia[modelo][codigoPsat] = []
						composicaoSubbacia[modelo][codigoPsat].append((coord.x,coord.y))

		pathFileSaida = os.path.join(pathArquivosSaida, 'coordenadas_{}.txt'.format(modelo))
		fileSaida = open(pathFileSaida, 'w')

		for bacia in bacias:
			for subbacia in bacias[bacia]:
				codigoPsat = bacias[bacia][subbacia]
				linha = '{};'.format(codigoPsat)
				if codigoPsat not in composicaoSubbacia[modelo]:
					print('Problema no posto {}, verificar as coordenadas manualmente com a funcao "plotBacia"'.format(codigoPsat))
					continue
				for coord in composicaoSubbacia[modelo][codigoPsat]:
					linha += '{},{};'.format(round(coord[0],2),round(coord[1],2))
				fileSaida.write('{}\n'.format(linha))

		if modelo == 'gpm':
			src = pathFileSaida
			dst = pathFileSaida.replace('gpm','pobs')
			shutil.copyfile(src, dst)

		print(pathFileSaida)
		print()


def converterFormatoSmap(modelo, dataRodada, rodada=0, destino='', membro=None):
	
	print('Gerando previsao para o modelo:'+modelo)
	modelo = modelo.lower()
	destino = os.path.abspath(destino)
	arquivoCoordenadasModelo = os.path.join(dirArqCoordenadasPsat,'coordenadas_{}.txt'.format(modelo))

	if modelo == 'gefs':
		padraoNome = os.path.join(dirPrevisoesModelosRaizen, 'gefs', '{}{:0>2}'.format(dataRodada.strftime('%Y%m%d'), rodada), 'data', '{}.acumul12z-12z.pgrb2a.0p50f*.nc'.format(modelo))
	elif modelo == 'gefs-ext':
		padraoNome = os.path.join(dirPrevisoesModelosRaizen, 'gefs', '{}{:0>2}'.format(dataRodada.strftime('%Y%m%d'), rodada), 'data', 'gefs.acumul12z-12z.pgrb2af_estendido.nc')
	elif modelo == 'gefs_membro':
		padraoNome = os.path.join(dirPrevisoesModelosRaizen, 'gefs', '{}{:0>2}'.format(dataRodada.strftime('%Y%m%d'), rodada), 'data','gefs_membro{:0>2}.acumul12z-12z.pgrb2af.nc'.format(membro))
	elif modelo == 'gefs_membro-ext':
		padraoNome = os.path.join(dirPrevisoesModelosRaizen, 'gefs', '{}{:0>2}'.format(dataRodada.strftime('%Y%m%d'), rodada), 'data','gefs_membro{:0>2}.acumul12z-12z.pgrb2af_estendido.nc'.format(membro))
	elif modelo == 'gfs':
		padraoNome = os.path.join(dirPrevisoesModelosRaizen, 'gfs', '{}{:0>2}'.format(dataRodada.strftime('%Y%m%d'), rodada), 'gfs-regrid.acumul12z-12z.pgrb2f*_regrided.nc')	
	elif modelo == 'ecmwf':
		padraoNome = os.path.join(dirPrevisoesModelosRaizen, 'ecmwf', '{}{:0>2}'.format(dataRodada.strftime('%Y%m%d'), rodada), 'data','ecm-br_{}{:0>2}_12z-12z.nc'.format(dataRodada.strftime('%Y%m%d'), rodada))	
	elif modelo == 'ecmwf-ens': # ec usado no pconjunto
		padraoNome = os.path.join(dirPrevisoesModelosRaizen, 'ecmwf-ens', '{}{:0>2}'.format(dataRodada.strftime('%Y%m%d'), rodada), 'data','ec-ens-br_{}{:0>2}_ens_AVG-12z-12z.nc'.format(dataRodada.strftime('%Y%m%d'), rodada))	
	elif modelo == 'ecmwf-ens_membro':
		dt_anterior_rodada = dataRodada - datetime.timedelta(days=1)
		padraoNome = os.path.join(dirPrevisoesModelosRaizen, 'ecmwf-ens-orig', '{}{:0>2}'.format(dt_anterior_rodada.strftime('%Y%m%d'), rodada), 'data','A1F{}0000_{:0>2}.nc'.format(dt_anterior_rodada.strftime('%m%d'), membro))
	elif modelo == 'ecmwf-ens-ext':
		padraoNome = os.path.join(dirPrevisoesModelosRaizen, 'ecmwf-ens-orig', '{}{:0>2}'.format(dataRodada.strftime('%Y%m%d'), rodada), 'data','A1EF_all_{}0000_{:0>2}_regrid.nc'.format(dataRodada.strftime('%m%d'), membro))	
	elif modelo == 'ecmwf-orig':
		padraoNome = os.path.join(dirPrevisoesModelosRaizen, 'ecmwf-orig', '{}{:0>2}'.format(dataRodada.strftime('%Y%m%d'), rodada),'{}{:0>2}0000-d*-oper-fc_12z-12z.nc'.format(dataRodada.strftime('%Y%m%d'), rodada))	
	elif modelo == 'eta':
		padraoNome = os.path.join(dirPrevisoesModelosRaizen, 'ons-eta', '{}{:0>2}'.format(dataRodada.strftime('%Y%m%d'), rodada), 'data','pp{}_*.ctl.nc'.format(dataRodada.strftime('%Y%m%d')))
	elif modelo == 'pobs':
		padraoNome = os.path.join(dirpreciptacaoObservada, 'MERGE_CPTEC_{}*.acumul12z.nc'.format(dataRodada.strftime('%Y%m%d')))
	elif modelo == 'imerg':
		padraoNome = os.path.join(dirPreciptacaoImerge, 'imerg_daily_{}.nc'.format(dataRodada.strftime('%Y%m%d')))
	elif modelo == 'merge':
		padraoNome = os.path.join(dirPreciptacaoMerge, 'prec_{}.nc'.format(dataRodada.strftime('%Y.%m.%d')))

	arquivosPrevisoesModelo = glob.glob(padraoNome)

	# Como todos os dias do EC sao armazenados em apenas um arquivo, o arquivo foi repetido 14 vezes
	if modelo == 'ecmwf':
		arquivosPrevisoesModelo = [padraoNome for i in range(9)]
	elif modelo == 'ecmwf-ens':
		arquivosPrevisoesModelo = [padraoNome for i in range(14)]
	elif modelo == 'ecmwf-ens_membro':
		arquivosPrevisoesModelo = [padraoNome for i in range(14)]
	elif modelo == 'ecmwf-ens-ext':
		arquivosPrevisoesModelo = [padraoNome for i in range(31)]
	elif modelo == 'gefs_membro':
		arquivosPrevisoesModelo = [padraoNome for i in range(15)]
	elif modelo == 'gefs-ext':
		arquivosPrevisoesModelo = [padraoNome for i in range(34)]
	elif modelo == 'gefs_membro-ext':
		arquivosPrevisoesModelo = [padraoNome for i in range(34)]


	print('Padrao: {}'.format(padraoNome))

	if len(arquivosPrevisoesModelo) == 0:
		raise('Nenhum arquivo de previsão encontrado')

	coordenadasPostos = open(arquivoCoordenadasModelo, 'r').readlines()
	print('\nUtilizando o arquivo de previsao: {}'.format(os.path.basename(arquivoCoordenadasModelo)))

	previsaoCoordenadas = {}
	previsaoSubBacia = pd.DataFrame()
	for i, arquivoPrev in enumerate(arquivosPrevisoesModelo):

		nomeArquivo = os.path.basename(arquivoPrev)

		if modelo == 'gefs':
			ndia = int(re.match(modelo+'.acumul12z-12z.pgrb2a.0p50f([0-9]{1,}).nc', nomeArquivo).group(1))
			if rodada == 18:
				ndia += 1
			dtReferente = dataRodada + datetime.timedelta(days=ndia)
			precModelo = xr.open_dataarray(arquivoPrev)

		elif modelo == 'gefs_membro':
			ndia = i+1
			dtReferente = dataRodada + datetime.timedelta(days=ndia)
			dtime = datetime.datetime(2017,1,1,12) + datetime.timedelta(hours=ndia)
			precModelo = xr.open_dataarray(arquivoPrev).sel(time=dtime)
		
		elif modelo == 'gefs-ext':
			ndia = i+1
			dtReferente = dataRodada + datetime.timedelta(days=ndia)
			dtime = datetime.datetime(2017,1,1,12) + datetime.timedelta(hours=ndia)
			precModelo = xr.open_dataarray(arquivoPrev).sel(time=dtime)

		elif modelo == 'gefs_membro-ext':
			ndia = i+1
			dtReferente = dataRodada + datetime.timedelta(days=ndia)
			dtime = datetime.datetime(2017,1,1,12) + datetime.timedelta(hours=ndia)
			precModelo = xr.open_dataarray(arquivoPrev).sel(time=dtime)

		elif modelo == 'gfs':
			ndia = int(re.match(modelo+'-regrid.acumul12z-12z.pgrb2f([0-9]{1,})_regrided.nc', nomeArquivo).group(1))
			dtReferente = dataRodada + datetime.timedelta(days=ndia)
			precModelo = xr.open_dataarray(arquivoPrev)

		elif modelo == 'ecmwf':
			ndia = (i+1)
			dtReferente = dataRodada + datetime.timedelta(days=ndia)
			horas = i*24
			precModelo = xr.open_dataset(arquivoPrev,decode_times=False).TP_GDS0_SFC.sel(forecast_time0=horas)

		elif modelo == 'ecmwf-ens':
			ndia = i+1
			dtReferente = dataRodada + datetime.timedelta(days=ndia)
			dtime = dtReferente + datetime.timedelta(hours=12)
			precModelo = xr.open_dataset(arquivoPrev).var228.sel(time=dtime)

		elif modelo == 'ecmwf-ens_membro':
			ndia = i+1
			dtReferente = dataRodada + datetime.timedelta(days=ndia)
			dtime = dtReferente + datetime.timedelta(hours=12)
			precModelo = xr.open_dataset(arquivoPrev).var228.sel(time=dtime)
			dtimeAnt = dtime - datetime.timedelta(days=1)
			precModeloAnt = xr.open_dataset(arquivoPrev).var228.sel(time=dtimeAnt)

		elif modelo == 'ecmwf-ens-ext':
			ndia = i+15
			dtReferente = dataRodada + datetime.timedelta(days=ndia)
			dtime = dtReferente + datetime.timedelta(hours=12)
			precModelo = xr.open_dataset(arquivoPrev).var228.sel(time=dtime)
			dtimeAnt = dtime - datetime.timedelta(days=1)
			precModeloAnt = xr.open_dataset(arquivoPrev).var228.sel(time=dtimeAnt)
		
		elif modelo == 'ecmwf-orig':
			ndia = int(re.match('[0-9]{10}0000-d([0-9]{1,})-oper-fc_12z-12z.nc', nomeArquivo).group(1))
			dtReferente = dataRodada + datetime.timedelta(days=ndia)
			precModelo = xr.open_dataarray(arquivoPrev)
		 
		elif modelo == 'eta':
			nhoras = int(re.match('pp[0-9]{8}_([0-9]{1,}).ctl.nc', nomeArquivo).group(1))
			dtReferente = dataRodada + datetime.timedelta(hours=nhoras-12)
			precModelo = xr.open_dataarray(arquivoPrev)

		elif modelo in ['gpm','pobs','imerg','merge']:
			dtReferente = dataRodada
			precModelo = xr.open_dataarray(arquivoPrev)

		previsaoCoordenadas[dtReferente] = {}

		print(dtReferente.strftime('%d/%m/%Y'))
		for i, row in enumerate(coordenadasPostos):
			row = row.split(';')
			codigoPsat = row[0]
			coords = row[1:-1]

			precBacia = []
			for c in coords:
				lon, lat = c.split(',')
				lon = float(lon)
				lat = float(lat)
				if modelo == 'gefs':
					p = float(precModelo.sel(longitude=lon+360,latitude=lat,method="nearest").values)
				elif modelo == 'gefs_membro':
					p = float(precModelo.sel(lon=lon+360,lat=lat,method="nearest").values)
				elif modelo == 'gefs-ext':
					p = float(precModelo.sel(lon=lon+360,lat=lat,method="nearest").values)
				elif modelo == 'gefs_membro-ext':
					p = float(precModelo.sel(lon=lon+360,lat=lat,method="nearest").values)
				elif modelo == 'gfs':
					p = float(precModelo.sel(lon=lon,lat=lat,method="nearest").values)
				elif modelo == 'ecmwf':
					p = float(precModelo.sel(g0_lon_2=lon,g0_lat_1=lat,method="nearest").values)
				elif modelo == 'ecmwf-ens':
					p = float(precModelo.sel(longitude=lon,latitude=lat,method="nearest").values)
				elif modelo == 'ecmwf-ens_membro':
					p1 = float(precModeloAnt.sel(longitude=lon,latitude=lat,method="nearest"))
					p2 = float(precModelo.sel(longitude=lon,latitude=lat,method="nearest"))
					# o valor e cumulativo, entao e necessario subtrarir o valor anterior e esta em metros quadrados
					p = (p2-p1)*1000 
				elif modelo == 'ecmwf-ens-ext':
					p1 = float(precModeloAnt.sel(lon=lon,lat=lat,method="nearest"))
					p2 = float(precModelo.sel(lon=lon,lat=lat,method="nearest"))
					# o valor e cumulativo, entao e necessario subtrarir o valor anterior e esta em metros quadrados
					p = (p2-p1)*1000 
				elif modelo == 'ecmwf-orig':
					p = float(precModelo.sel(longitude=lon,latitude=lat,method="nearest").values)
				elif modelo == 'eta':
					p = float(precModelo.sel(lon=lon,lat=lat,method="nearest").values)
				elif modelo in ['gpm','pobs','imerg','merge']:
					p = float(precModelo.sel(lon=lon,lat=lat,method="nearest").values)

				previsaoCoordenadas[dtReferente][(lon, lat)] = round(p,2)
				precBacia.append(p)
			precMedia = round(sum(precBacia)/len(precBacia),2)
			if precMedia < 0:
				precMedia = 0
			previsaoSubBacia.loc[codigoPsat, dtReferente] = precMedia

	previsaoCoordenadas = pd.DataFrame(previsaoCoordenadas)

	previsaoSubBacia = previsaoSubBacia.reindex(sorted(previsaoSubBacia.columns), axis=1)

	# Atualizar esse arquivo sempre que entrar novos postos
	arquivoModelo = glob.glob(os.path.join(dirArqCoordenadasPsat, 'ETA40_m_*.dat'))[-1]

	df_saida = pd.read_csv(arquivoModelo, header=None, sep=' ', skipinitialspace=True)
	df_saida = df_saida.set_index(0)

	nomeSaida = '{}_p{}a{}.dat'

	if not os.path.exists(destino):
		os.makedirs(destino)

	nomeModeloSaida = modelo.upper()
	if modelo == 'ecmwf-orig':
		nomeModeloSaida = 'EC'
	elif modelo == 'ecmwf-ens_membro':
		nomeModeloSaida = 'EC-ens{:0>2}'.format(membro)
	elif modelo == 'ecmwf-ens-ext':
		nomeModeloSaida = 'EC-ens{:0>2}-ext'.format(membro)
	elif modelo in ['pobs','imerg']:
		dataRodada = dataRodada - datetime.timedelta(days=1)
	elif modelo in ['merge']:
		dataRodada = datetime.datetime(1998,1,1)
	elif modelo == 'gefs_membro':
		nomeModeloSaida = 'GEFS{:0>2}'.format(membro)
	elif modelo == 'gefs-ext':
		nomeModeloSaida = 'GEFS-ext'
	elif modelo == 'gefs_membro-ext':
		nomeModeloSaida = 'GEFS{:0>2}-ext'.format(membro)


	subBaciasCalculadas = {}
	subBaciasCalculadas[(-64.66, -09.26)] = [{'cod':'PSATJIRA', 'fator':0.13},{'cod':'PSATAMY', 'fator':0.87}]
	subBaciasCalculadas[(-51.77, -03.13)] = [{'cod':'PSATPIME', 'fator':0.699},{'cod':'PSATBSOR', 'fator':0.264},{'cod':'PSATBESP', 'fator':0.037}]

	for dataPrev in previsaoSubBacia.columns:
		pathArqSaida = os.path.join(destino, nomeSaida.format(nomeModeloSaida, dataRodada.strftime('%d%m%y'), dataPrev.strftime('%d%m%y')))
		arquivoSaida = open(pathArqSaida, 'w')
		
		for codPsat, row in df_saida.iterrows():
			lat = '{:>.2f}'.format(row[1])
			lon = '{:>.2f}'.format(row[2])


			lat = '{:>6}'.format(lat.zfill(6))
			lon = '{:>6}'.format(lon.zfill(6))

			if 	row[1] > 0:
				lat = '{:>.2f}'.format(row[1])
				lat = '{:>6}'.format(lat.zfill(5))
			
			prec ='{:>.2f}'.format(previsaoSubBacia.loc[codPsat,dataPrev])
			arquivoSaida.write('{} {} {}\n'.format(lon,lat,prec))

		for coord in subBaciasCalculadas:
			subBac_calc = subBaciasCalculadas[coord]

			lat = '{:>6}'.format(str(round(coord[1],2)).zfill(6))
			lon = '{:>6}'.format(str(round(coord[0],2)).zfill(6))

			if 	coord[1] > 0:
				lat = '{:>6}'.format(str(round(coord[1],2)).zfill(5))

			prec = 0
			for sbac in subBac_calc:
				prec += previsaoSubBacia.loc[sbac['cod'],dataPrev]*sbac['fator']

			arquivoSaida.write('{} {} {:>.2f}\n'.format(lon,lat,prec))

		print(pathArqSaida)
		arquivoSaida.close()

	if rodada == 18:
		# A rodada das 18z nao possui a previsao do dia seguinte, portanto e copiado da rodada das 00z
		pathSrc = destino.replace('_{:0>2}'.format(rodada),'')
		diaSeguinte = dataRodada + datetime.timedelta(days=1)
		src = os.path.join(pathSrc, nomeSaida.format(nomeModeloSaida, dataRodada.strftime('%d%m%y'), diaSeguinte.strftime('%d%m%y')))
		dst = os.path.join(destino, nomeSaida.format(nomeModeloSaida, dataRodada.strftime('%d%m%y'), diaSeguinte.strftime('%d%m%y')))
		print('\nCopiando o primeiro dia da rodada das 00z:\n{} -> {}'.format(src,dst))
		shutil.copyfile(src, dst)
	return previsaoSubBacia

def converterFormatoPconjunto(modelo, dataRodada, rodada=0, destino=''):

	dataRodada = datetime.datetime.combine(dataRodada.date(), datetime.datetime.min.time())
	dt_anterior_rodada = dataRodada - datetime.timedelta(days=1)

	print('Gerando previsao para o modelo:'+modelo)
	modelo = modelo.lower()
	arquivoCoordenadasModelo = os.path.join(dirArqCoordenadasPsat,'coordenadas_{}.txt'.format(modelo))

	if modelo == 'gefs':
		padraoNome = os.path.join(dirPrevisoesModelosRaizen, 'gefs', '{}{:0>2}'.format(dataRodada.strftime('%Y%m%d'), rodada), 'data', '{}.acumul12z-12z.pgrb2a.0p50f*.nc'.format(modelo))
	elif modelo == 'ecmwf':
		padraoNome = os.path.join(dirPrevisoesModelosRaizen, 'ecmwf-ens-orig', '{}{:0>2}'.format(dt_anterior_rodada.strftime('%Y%m%d'), rodada), 'data','A1F{}0000_{:0>2}.nc'.format(dt_anterior_rodada.strftime('%m%d'), 101))	
	elif modelo == 'eta':
		padraoNome = os.path.join(dirPrevisoesModelosRaizen, 'ons-eta', '{}{:0>2}'.format(dataRodada.strftime('%Y%m%d'), rodada), 'data','pp{}_*.ctl.nc'.format(dataRodada.strftime('%Y%m%d')))
	elif modelo == 'gpm':
		padraoNome = os.path.join(dirpreciptacaoObservada, 'MERGE_CPTEC_{}*.acumul12z.nc'.format(dataRodada.strftime('%Y%m%d')))

	arquivosPrevisoesModelo = glob.glob(padraoNome)

	if len(arquivosPrevisoesModelo) == 0:
		raise('Nenhum previsão com o padrao acima.')
	elif len(arquivosPrevisoesModelo) == 1 and modelo != 'gpm':
		arquivosPrevisoesModelo = [arquivosPrevisoesModelo[0] for i in range(14)]

	print(padraoNome)

	coordenadasPostos = open(arquivoCoordenadasModelo, 'r').readlines()

	## Para utilizar 
	previsaoCoordenadas = {}
	previsaoSubBacia = pd.DataFrame()
	for i, arquivoPrev in enumerate(arquivosPrevisoesModelo):

		nomeArquivo = os.path.basename(arquivoPrev)

		if modelo == 'gefs':
			ndia = int(re.match(modelo+'.acumul12z-12z.pgrb2a.0p50f([0-9]{1,}).nc', nomeArquivo).group(1))
			dtReferente = dataRodada + datetime.timedelta(days=ndia)
			precModelo = xr.open_dataarray(arquivoPrev)

		elif modelo == 'ecmwf':
			ndia = i+1
			dtReferente = dataRodada + datetime.timedelta(days=ndia)
			dtime = dtReferente + datetime.timedelta(hours=12)
			ecmwf_membro_101 = xr.open_dataset(arquivoPrev).diff('time')*1000
			precModelo = ecmwf_membro_101.var228.sel(time=dtime)
			

		elif modelo == 'eta':
			nhoras = int(re.match('pp[0-9]{8}_([0-9]{1,}).ctl.nc', nomeArquivo).group(1))
			dtReferente = dataRodada + datetime.timedelta(hours=nhoras-12)
			precModelo = xr.open_dataarray(arquivoPrev)

		elif modelo == 'gpm':
			dtReferente = dataRodada
			precModelo = xr.open_dataarray(arquivoPrev)

		previsaoCoordenadas[dtReferente] = {}

		print(dtReferente.strftime('%d/%m/%Y'))
		for i, row in enumerate(coordenadasPostos):
			row = row.split(';')
			codigoPsat = row[0]
			coords = row[1:-1]

			precBacia = []
			for c in coords:
				lon, lat = c.split(',')
				lon = float(lon)
				lat = float(lat)

				if modelo == 'gefs':
					p = float(precModelo.sel(longitude=lon+360,latitude=lat,method="nearest").values)
				elif modelo == 'ecmwf':
					p = float(precModelo.sel(longitude=lon,latitude=lat,method="nearest").values)
				elif modelo == 'eta':
					p = float(precModelo.sel(lon=lon,lat=lat,method="nearest").values)
				elif modelo == 'gpm':
					p = float(precModelo.sel(lon=lon,lat=lat,method="nearest").values)

				previsaoCoordenadas[dtReferente][(lon, lat)] = round(p,2)
				# precBacia.append(p)

				if not math.isnan(p):
					precBacia.append(p)
				elif len(precBacia):
					precBacia.append(0) # se for o valor for NaN e nao tiver nenhum valor para a subbacia, sera adicionado 0 

			previsaoSubBacia.loc[codigoPsat, dtReferente] = round(sum(precBacia)/len(precBacia),2)

	previsaoCoordenadas = pd.DataFrame(previsaoCoordenadas)

	previsaoSubBacia = previsaoSubBacia.reindex(sorted(previsaoSubBacia.columns), axis=1)
	numeroDiasPrevistos = 14
	if modelo.lower() == 'eta':
		numeroDiasPrevistos = 9
	if modelo.lower() == 'gpm':
		numeroDiasPrevistos = 1

	# Atualizar esse arquivo sempre que entrar novos postos (!!! Mudar p banco de dados)
	arquivoModelo = glob.glob(os.path.join(dirArqCoordenadasPsat, 'ETA40_m_*.dat'))[-1]

	df_saida = pd.read_csv(arquivoModelo, header=None, sep=' ', skipinitialspace=True)
	df_saida = df_saida.set_index(0)

	nomeSaida = modelo.upper()
	if modelo.lower() == 'eta':
		nomeSaida = modelo.upper() + '40'

	pathArqSaida = os.path.join(destino, '{}_m_{}.dat'.format(nomeSaida.upper(), dataRodada.strftime('%d%m%y')))
	arquivoSaida = open(pathArqSaida, 'w')

	for codPsat, row in df_saida.iterrows():
		lon = '{:>7.2f}'.format(row[1])
		lat = '{:>7.2f}'.format(row[2])
		linha = '{:<9} {} {}'.format(codPsat, lon, lat)
		for i in range(numeroDiasPrevistos):
			linha += ' {:>6.2f}'.format(round(previsaoSubBacia.loc[codPsat][i],2))
		arquivoSaida.write('{}\n'.format(linha))

	arquivoSaida.close()


	print('Previsao gerado com sucesso:\n{}'.format(os.path.join(os.path.abspath('.'), pathArqSaida)))

	return previsaoSubBacia

if __name__ == '__main__':
	tempoAppDir = os.path.dirname(sys.path[0])

	modelo = 'imerg'
	modelo = 'pobs'
	membro = 51
	dataRodada = datetime.datetime(2022,4,28)
	rodada = 0
	# destino = r'C:/Users/cs341052/Dropbox/WX - Middle/NovoSMAP/{}/rodada00z'.format(dataRodada.strftime('%Y%m%d'))

	if rodada:
		destino = r'/WX2TB/Documentos/chuva-vazao/{}/ECMWF-orig_{:0>2}/'.format(dataRodada.strftime('%Y%m%d'),rodada)
	else:
		destino = r'/WX2TB/Documentos/chuva-vazao/{}/ECMWF-orig/'.format(dataRodada.strftime('%Y%m%d'))

	# plotBacia()
	# atualizarCoordenadasBacias(modelo=modelo)
	# atualizarCoordenadasBacias()
	converterFormatoSmap(modelo=modelo, dataRodada=dataRodada, rodada=rodada, destino=destino, membro=membro)
	# converterFormatoPconjunto(modelo=modelo, dataRodada=dataRodada, rodada=rodada, destino=destino)


