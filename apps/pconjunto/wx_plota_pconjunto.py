
import os
import pdb
import sys
import datetime

import numpy as np
import pandas as pd
import xarray as xr
import matplotlib as mpl
import cartopy.crs as ccrs
import matplotlib.pyplot as plt

from cartopy.feature import ShapelyFeature
from cartopy.io.shapereader import Reader
from cartopy.mpl.gridliner import LONGITUDE_FORMATTER, LATITUDE_FORMATTER

sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.apps.tempo.arquivos.auxiliares.configuracaoSmap import get_smap_plot_structure

composicaoPconjunto = ['ETA40','GEFS']
PATH_ARQUIVOS_AUXILIARES = os.path.abspath('/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/tempo/arquivos/auxiliares')
PATH_PCONJUNTO = os.path.dirname(os.path.abspath(__file__))
PATH_ROTINA_CONJUNTO = os.path.join(PATH_PCONJUNTO, "pconjunto-ONS")
PATH_SAIDA_GEFS_ETA = os.path.abspath('/WX2TB/Documentos/saidas-modelos/gefs-eta')

def cria_xarray_etabase(df):
	ncFile_eta = os.path.join(PATH_ARQUIVOS_AUXILIARES, 'CV','arquivosNetcf','pp20200102_0036.ctl.nc')
	etanc = xr.open_dataarray(ncFile_eta)
	etanc = etanc.assign_coords(lat=np.round(etanc.lat,2));etanc = etanc.assign_coords(lon=np.round(etanc.lon,2))
	ds = xr.DataArray(dims=['lat','lon','time'],coords={'lat':etanc.coords['lat'].values,'lon':etanc.coords['lon'].values,'time':df.columns.values[2:]})

	composicaoSubbaciasSmapEta = os.path.join(PATH_ARQUIVOS_AUXILIARES,'CV','coordenadas_eta.txt')
	compSubBac = open(composicaoSubbaciasSmapEta, 'r').readlines()
	
	for row in compSubBac:
		infoRow = row.split(';')
		subBac = infoRow[0]
		coordenadasSubBac = infoRow[1:]

		for dt in df.columns.values[2:]:
			for coord in coordenadasSubBac:
				if coord in ['','\n']:
					continue
				coord = coord.split(',')
				lon = float(coord[0])
				lat = float(coord[1])
				try:
					ds.loc[dict(time=dt,lon=lon,lat=lat)] = df.loc[subBac,dt]
				except:
					print(f"subbacia {subBac} nao mapeada")

	return ds

def plota_fig(ds,path_fig,str_titulo,levels=None,extent=None,DPI=None,cores=None):
	
	if levels == None:
		levels = [0,1,5,10,15,20,25,30,40,50,75,100,150,200,400]
	if cores == None:
		cores = "ONS"
	if extent == None:
		extent = [-76, -34, -32, 6]
	if DPI == None:
		DPI = 100

	shp1 = os.path.join(PATH_ARQUIVOS_AUXILIARES,'shapefiles','Contorno_Bacias_rev2.shp')
	shape_feature1 = ShapelyFeature(Reader(shp1).geometries(),ccrs.PlateCarree(),edgecolor='purple',facecolor='none',linewidth=1)
	
	# shp2 = '/WX2TB/Documentos/fontes/tempo/gefs-eta-plot/shapefiles/BRA_adm1.shp'
	shp2 = os.path.join(PATH_ARQUIVOS_AUXILIARES,'shapefiles','brasil.shp')
	shape_feature2 = ShapelyFeature(Reader(shp2).geometries(),ccrs.PlateCarree(),edgecolor='black',facecolor='none',linewidth=0.3)    


	fig = plt.figure(figsize=(9.5, 9))
	ax = plt.axes(projection=ccrs.PlateCarree())
	ax.set_extent(extent)
	# ax.coastlines()
	gl = ax.gridlines(crs=ccrs.PlateCarree(), draw_labels=True,
              linewidth=0.1, color='gray', alpha=0.9, linestyle='--')
	gl.top_labels = False
	gl.right_labels = False
	gl.xformatter = LONGITUDE_FORMATTER
	gl.yformatter = LATITUDE_FORMATTER

	ax.add_feature(shape_feature1,facecolor='none',linewidth=1)
	ax.add_feature(shape_feature2,facecolor='none',linewidth=1)
# Add logos / images to the plot
	logo = plt.imread('/WX2TB/Documentos/fontes/tempo/imagens-satelite/logo_completo_teste.png')
	fig.figimage(logo, 430,100, zorder=0.7, alpha = 1, origin = 'upper')

# criando paleta de cores ONS
	REAL = [[225,255,255],[180,240,250],[150,210,250],[40,130,240],[20,100,210],[103,254,133],[24,215,6],[30,180,30],[255,232,120],[255,192,60],[255,96,0],[225,20,0],[251,94,107],[168,168,168]] 
	paleta_ons = mpl.colors.ListedColormap(np.divide(REAL,255.0))

	if 'Dif.' not in str_titulo:
		paleta = paleta_ons
		level = levels

	else:
		paleta = 'RdBu'
		level = [-50,-30,-20,-15,-10,-8,-4,-2,0,2,4,8,10,15,20,30,50]

	ds.plot(cmap=paleta,levels=level,cbar_kwargs={"ticks": level, "label":'Precipitacao [mm]'})
	
	plt.title(str_titulo)
	plt.savefig(path_fig, dpi=DPI, bbox_inches='tight', pad_inches=0)
	print(path_fig)
	plt.close()

def getDivisaoSemanas(datas):
	sem = 0
	diasAcumulados = {sem:[]}
	for i_dt, dt in enumerate(datas):
		if dt.weekday() == 6 and i_dt!=0:
			sem += 1
			diasAcumulados[sem] = []
		diasAcumulados[sem].append(dt)
	return diasAcumulados

def plota_saida_pconjunto(data,path,path_fig):
	
	subbacias_smap = get_smap_plot_structure()
	configSmap = subbacias_smap

	data = datetime.datetime.combine(data, datetime.datetime.min.time())
	path_data = '/WX4TB/Documentos/saidas-modelos/gefs-eta/' + data.strftime('%Y%m%d') + '00/data'

	prevModelos = {}
	prevModelos_r = {}
	cols = ['cod','lat','lon'] + [data.date()+datetime.timedelta(days=i+1) for i in range(15)]
	cols_r = ['cod','lon','lat'] + [data.date()+datetime.timedelta(days=i+1) for i in range(15)]
	for model in composicaoPconjunto:
		prevModelos[model] = pd.read_csv(os.path.join(path,'Arq_Entrada',model,'{}_m_{}.dat'.format(model,data.strftime('%d%m%y'))),delimiter=r'\s+',header=None)
		prevModelos[model].columns = cols[:len(prevModelos[model].columns)]
		prevModelos[model].set_index('cod', inplace=True)
		
		prevModelos_r[model] = pd.read_csv(os.path.join(path_data,'{}_rem_vies.dat'.format(model)),delimiter=r'\s+',header=None)
		prevModelos_r[model].columns = cols_r[:len(prevModelos_r[model].columns)]
		prevModelos_r[model].set_index('cod', inplace=True)

	for i in range(1,15):
		datapos = data + datetime.timedelta(days=i)
		filePath = os.path.join(path,'Arq_Saida','PMEDIA_p{}a{}.dat'.format(data.strftime('%d%m%y'),datapos.strftime('%d%m%y')))
		if i == 1:
			pmedia = pd.read_csv(filePath,delimiter=r'\s+',index_col=False,header=None)
		else:
			pmedia[i+1] = pd.read_csv(filePath,delimiter=r'\s+',index_col=False,header=None,usecols=[2])

	pmedia.columns = cols_r[1:len(pmedia.columns)+1]
	for subBac in configSmap:
		coord = configSmap[subBac]['coordenadas']
		pmedia.loc[(pmedia['lon']==coord['lon']) & (pmedia['lat']==coord['lat']),'cod'] = subBac

	pmedia = pmedia.dropna()
	pmedia.set_index('cod', inplace=True)
	prevModelos['PMEDIA'] = pmedia

	diasSemana = 'DSTQQSS'

	print('plota figuras\n')
	for model in prevModelos:

		ds = cria_xarray_etabase(prevModelos[model])
		
		# Plot diarios
		for dt in ds.time.values:
			tituloGrafico = '{} (Bacias SMAP ONS) - PREC24\n{}12Z (ini: {}00Z)'.format(model, dt.strftime('%d%m%Y'), data.strftime('%d%m%Y'))
			pathSaida = os.path.join(path_fig, data.strftime('%Y%m%d00'), '24-em-24-gifs', model +'_rain_'+data.strftime('%d%m%Y')+'00z_f'+dt.strftime('%Y%m%d')+'12z.png')
			plota_fig(ds.sel(time=dt),pathSaida,tituloGrafico)

		# Plot acumulados
		diasAcumulados = getDivisaoSemanas(list(ds.time.values))
		ds_acumulado = ds.assign_coords(time=pd.to_datetime(ds.time.values))
		ds_acumulado = ds_acumulado.resample(time='W-Sat').sum('time',skipna=False)
		for i_sem, sem in enumerate(ds_acumulado.time.values):
			if i_sem == 0:
				sem_label = diasSemana[-len(diasAcumulados[i_sem]):]
			else:
				sem_label = diasSemana[:len(diasAcumulados[i_sem])]

			pathSaida = os.path.join(path_fig, data.strftime('%Y%m%d00'),'semana-energ', '{}semana_energ_{}-r{}00.png'.format(i_sem+1, model,data.strftime('%d%m%Y')))
			tituloGrafico = '{} (Bacias SMAP ONS) \n Sem. {} - {} (ini: {}00Z - {})'.format(model, i_sem+1, sem_label, data.strftime('%d%m%Y'), data.strftime('%a').capitalize())
			plota_fig(ds_acumulado.sel(time=sem),pathSaida,tituloGrafico)

		if model == 'PMEDIA':
			continue

		prevModelos_r[model] = prevModelos_r[model].dropna(axis=1)
		# Plot diarios (removedor de vies)
		ds_r = cria_xarray_etabase(prevModelos_r[model])
		for tempo in ds_r.time.values:
			tituloGrafico = '{}-rem-vies (Bacias SMAP ONS) - PREC24\n{}12Z (ini: {}00Z)'.format(model, tempo.strftime('%d%m%Y'), data.strftime('%d%m%Y'))
			pathSaida = os.path.join(path_fig, data.strftime('%Y%m%d00'), '24-em-24-gifs', model +'-rem-vies_rain_'+data.strftime('%d%m%Y')+'00z_f'+tempo.strftime('%Y%m%d')+'12z.png')
			plota_fig(ds_r.sel(time=tempo),pathSaida,tituloGrafico)

		# Plot acumulados (removedor de vies)
		diasAcumulados = getDivisaoSemanas(list(ds_r.time.values))
		ds_r_acumulado = ds_r.assign_coords(time=pd.to_datetime(ds_r.time.values))
		ds_r_acumulado = ds_r_acumulado.resample(time='W-Sat').sum('time',skipna=False)
		for i_sem, sem in enumerate(ds_r_acumulado.time.values):
			if i_sem == 0:
				sem_label = diasSemana[-len(diasAcumulados[i_sem]):]
			else:
				sem_label = diasSemana[:len(diasAcumulados[i_sem])]

			pathSaida = os.path.join(path_fig, data.strftime('%Y%m%d00'),'semana-energ', '{}semana_energ_{}-rem-vies-r{}00.png'.format(i_sem+1, model,data.strftime('%d%m%Y')))
			tituloGrafico = '{}-rem-vies (Bacias SMAP ONS) \n Sem. {} - {} (ini: {}00Z - {})'.format(model, i_sem+1, sem_label, data.strftime('%d%m%Y'), data.strftime('%a').capitalize())
			plota_fig(ds_r_acumulado.sel(time=sem),pathSaida,tituloGrafico)

		# Plot diarios (Diferenças)
		ds_diff = ds_r - ds
		for tempo in ds_diff.time.values:
			tituloGrafico = 'Dif. entre {} com e sem remocao vies (Bacias SMAP ONS) - PREC24\n{}12Z (ini: {}00Z)'.format(model, tempo.strftime('%d%m%Y'), data.strftime('%d%m%Y'))
			pathSaida = os.path.join(path_fig, data.strftime('%Y%m%d00'), '24-em-24-gifs', 'DIF-'+model +'_rain_'+data.strftime('%d%m%Y')+'00z_f'+tempo.strftime('%Y%m%d')+'12z.png')
			plota_fig(ds_diff.sel(time=tempo),pathSaida,tituloGrafico)
		
		# Plot acumulados (Diferenças)
		diasAcumulados = getDivisaoSemanas(list(ds_diff.time.values))
		ds_diff = ds_diff.assign_coords(time=pd.to_datetime(ds_diff.time.values))
		ds_acumulado_acumulado = ds_diff.resample(time='W-Sat').sum('time',skipna=False)
		for i_sem, sem in enumerate(ds_acumulado_acumulado.time.values):
			if i_sem == 0:
				sem_label = diasSemana[-len(diasAcumulados[i_sem]):]
			else:
				sem_label = diasSemana[:len(diasAcumulados[i_sem])]

			pathSaida = os.path.join(path_fig, data.strftime('%Y%m%d00'),'semana-energ', '{}semana_energ_DIF-{}-r{}00.png'.format(i_sem+1, model,data.strftime('%d%m%Y')))
			tituloGrafico = '{}-rem-vies (Bacias SMAP ONS) \n Sem. {} - {} (ini: {}00Z - {})'.format(model, i_sem+1, sem_label, data.strftime('%d%m%Y'), data.strftime('%a').capitalize())
			plota_fig(ds_acumulado_acumulado.sel(time=sem),pathSaida,tituloGrafico)


def plota_psat(data):

	pathPsat = os.path.join(PATH_ROTINA_CONJUNTO,'Arq_Entrada','Observado','psat_' + data.strftime('%d%m%Y') + '.txt')
	df_psat = pd.read_csv(pathPsat,delimiter=r'\s+',header=None)
	dataRef = data + datetime.timedelta(days=1)
	cols = ['cod','lat','lon'] + [dataRef]
	df_psat.columns = cols
	df_psat.set_index('cod', inplace=True)
	ds_psat = cria_xarray_etabase(df_psat)
	pathSaida = os.path.abspath('/WX2TB/Documentos/dados/psat/zgifs/psat_{}12z.png'.format(dataRef.strftime('%Y%m%d')))
	tituloGrafico = 'Precipitacao Satelite (PSAT) - (Bacias SMAP ONS)\n PREC24 - {} - 12Z'.format(dataRef.strftime('%d%m%y'))
	plota_fig(ds_psat.sel(time=dataRef),pathSaida,tituloGrafico)



def printHelper():

	dt = datetime.datetime.now()
	print('Help: Entre com o argumento data para o plot do PSAT e/ou import outras libs aqui')
	print("python {} plot_psat data {}".format(sys.argv[0], dt.strftime('%d/%m/%Y')))
	print("python {} plot_pconjunto data {}".format(sys.argv[0], dt.strftime('%d/%m/%Y')))


if __name__ == '__main__':

	if len(sys.argv) > 1:
		if 'help' in sys.argv or '--h' in sys.argv or '-help' in sys.argv:
			printHelper()
			quit()

		for i in range(0, len(sys.argv)):

			argumento = sys.argv[i].lower()
			if argumento == 'data':
				data = datetime.datetime.strptime(sys.argv[i+1], '%d/%m/%Y')


		if 'plot_psat' in sys.argv:
			plota_psat(data)
			quit()

		if 'plot_pconjunto' in sys.argv:
			plota_saida_pconjunto(data,PATH_ROTINA_CONJUNTO,PATH_SAIDA_GEFS_ETA)
			quit()

	else:
		printHelper()
		quit()