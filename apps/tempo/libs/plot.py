import os
import re
import sys
import pdb
import glob
import datetime
import pandas as pd
import shapefile as shp

sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.apps.web_modelos.server.libs import rz_chuva
from PMO.scripts_unificados.apps.tempo.arquivos.auxiliares.configuracaoSmap import get_smap_plot_structure

PATH_ARQUIVOS_AUXILIARES = os.path.abspath('/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/tempo/arquivos/auxiliares')
PATH_CV = os.path.abspath('/WX2TB/Documentos/chuva-vazao')
PATH_PCONJUNTO = os.path.abspath('/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/pconjunto')
PATH_SAIDA_MODELOS = os.path.abspath('/WX2TB/Documentos/saidas-modelos')
PATH_DADOS = os.path.abspath('/WX2TB/Documentos/dados')
PATH_ROTINA_CONJUNTO = os.path.join(PATH_PCONJUNTO, "pconjunto-ONS")

paletaCores = [[255,255,255],[225,255,255],[180,240,250],[150,210,250],[40,130,240],[20,100,210],[103,254,133],[24,215,6],[30,180,30],[255,232,120],[255,192,60],[255,96,0],[225,20,0],[251,94,107],[168,168,168]]
coresHex = ['#ffffff','#e1ffff','#b4f0fa','#96d2fa','#2882f0','#1464d2','#67fe85','#18d706','#1eb41e','#ffe878','#ffc03c','#ff6000','#e11400','#fb5e6b','#a8a8a8']
bounds = [0,1,5,10,15,20,25,30,40,50,75,100,150,200]


locPrec = {}
locPrec['Parnaíba']=(-43.7,-7.8)
locPrec['Itajaí-Açu']=(-49.7,-27.4)
locPrec['Xingu']=(-52.8,-8.7)
locPrec['Grande']=(-46.5,-22)
locPrec['Iguaçu']=(-53,-26.2)
locPrec['Madeira']=(-63.6,-13.1)
locPrec['Paraná']=(-54,-21.5)
locPrec['Paranaíba']=(-49.2,-17.7)
locPrec['Paranapanema']=(-50.2,-24.0)
locPrec['São Francisco']=(-44,-12)
locPrec['Tietê']=(-48.2,-23.2)
locPrec['Tocantins']=(-49.9,-11.2)
locPrec['Uruguai']=(-54.5,-28.3)
locPrec['Antas']=(-49.9,-29.7)
locPrec['Capivari']=(-48.5,-25.4)
locPrec['Araguari']=(-52.3,1.3)
locPrec['Curuá-Una']=(-55.2,-2.3)
locPrec['Doce']=(-42.2,-19.5)
locPrec['Jacuí']=(-53.4,-30.3)
locPrec['Itabapoana']=(-41.6,-21.0)
locPrec['Paraguaçu']=(-40.4,-12.5)
locPrec['Jequitinhonha']=(-41.7,-16.7)
locPrec['Jari']=(-54.3,0.5)
locPrec['Correntes']=(-55.7,-18.8)
locPrec['Itiquira']=(-55.5,-16.9)
locPrec['Manso']=(-57.5,-15.7)
locPrec['Tapajós']=(-57,-9.2)
locPrec['Jauru']=(-58.2,-14.2)
locPrec['Uatuamã']=(-61,-3.1)
locPrec['Paraíba do Sul']=(-43.7,-22.1)

def leituraPrevEntradaSmap(arquivoPrev,datas):

    subbacias_smap = get_smap_plot_structure()

    if os.path.basename(arquivoPrev)[:5] == 'ETA40':
        prec = pd.read_csv(arquivoPrev, sep='\t', skipinitialspace=True, header=None)
    else:
        prec = pd.read_csv(arquivoPrev, sep=' ', skipinitialspace=True, header=None)

    prec.columns = ['lon','lat']+datas

    for subBacia in subbacias_smap:
        coordenadas = subbacias_smap[subBacia]['coordenadas']
        prec.loc[(prec['lon']==coordenadas['lon']) & (prec['lat']==coordenadas['lat']),'cod'] = subBacia

    # se nao dropar, vai ter valores dobrados para os postos a seguir
    subBaciasCalculadas = {}
    subBaciasCalculadas[(-64.66, -09.26)] = ['PSATJIRA','PSATAMY']
    subBaciasCalculadas[(-51.77, -03.13)] = ['PSATPIME','PSATBSOR','PSATBESP']

    for subBac in subBaciasCalculadas:
        # prec.drop(prec.loc[prec['cod'].isin(subBaciasCalculadas[subBac])].index, inplace=True)
        # prec.loc[(prec['lon']==subBac[0]) & (prec['lat']==subBac[1]),'cod'] = subBaciasCalculadas[subBac][0]
        prec.drop(prec.loc[(prec['lon']==subBac[0]) & (prec['lat']==subBac[1])].index, inplace=True)

    prec = prec.set_index('cod')
    return prec


def leituraPrevModelo(modelo,dataPrevisao,rodada,membro=None):

    rodada = int(rodada)
    modelo = modelo.lower()

    if membro != None:
        membro = int(membro)

    pathArqPrev = os.path.join(PATH_CV,dataPrevisao.strftime('%Y%m%d'))

    if modelo == 'gefs':
        pathModelo = os.path.join(pathArqPrev,'GEFS')
        padraoNomeArquivos = 'GEFS_p{}a*.dat'.format(dataPrevisao.strftime('%d%m%y'))
    elif modelo == 'gefs-ext':
        pathModelo = os.path.join(pathArqPrev,'GEFS')
        padraoNomeArquivos = 'GEFS-ext_p{}a*.dat'.format(dataPrevisao.strftime('%d%m%y'))
    elif modelo == 'gefs-membros':
        pathModelo = os.path.join(pathArqPrev,'GEFS')
        padraoNomeArquivos = 'GEFS{:0>2}_p{}a*.dat'.format(membro,dataPrevisao.strftime('%d%m%y'))
    elif modelo == 'gefs-membros-ext':
        pathModelo = os.path.join(pathArqPrev,'GEFS')
        padraoNomeArquivos = 'GEFS{:0>2}-ext_p{}a*.dat'.format(membro,dataPrevisao.strftime('%d%m%y'))
    elif modelo == 'gfs':
        pathModelo = os.path.join(pathArqPrev,'GFS')
        padraoNomeArquivos = 'GFS_p{}a*.dat'.format(dataPrevisao.strftime('%d%m%y'))
    elif modelo == 'ecmwf':
        pathModelo = os.path.join(pathArqPrev,'ECMWF')
        padraoNomeArquivos = 'ECMWF_p{}a*.dat'.format(dataPrevisao.strftime('%d%m%y'))
    elif modelo == 'ecmwf-orig':
        pathModelo = os.path.join(pathArqPrev,'ECMWF-orig')
        padraoNomeArquivos = 'EC_p{}a*.dat'.format(dataPrevisao.strftime('%d%m%y'))
    elif modelo == 'pmedia':
        pathModelo = os.path.join(pathArqPrev,'CONJUNTO_ONS_ORIG')
        padraoNomeArquivos = 'PMEDIA_p{}a*.dat'.format(dataPrevisao.strftime('%d%m%y'))
    elif modelo == 'pconjunto':
        pathModelo = os.path.join(pathArqPrev,'CONJUNTO_ONS_WX')
        padraoNomeArquivos = 'PCONJUNTO_p{}a*.dat'.format(dataPrevisao.strftime('%d%m%y'))
    elif modelo == 'eta40':
        pathModelo = os.path.join(pathArqPrev,'ETA40')
        padraoNomeArquivos = 'ETA40_p{}a*.dat'.format(dataPrevisao.strftime('%d%m%y'))
    elif modelo == 'ecmwf-ens':
        pathModelo = os.path.join(pathArqPrev,'ECMWF-ens')
        padraoNomeArquivos = 'EC-ens{:0>2}_p{}a*.dat'.format(membro,dataPrevisao.strftime('%d%m%y'))
    elif modelo == 'ecmwf-ens-ext':
        pathModelo = os.path.join(pathArqPrev,'ECMWF-ens')
        padraoNomeArquivos = 'EC-ens{:0>2}-ext_p{}a*.dat'.format(membro,dataPrevisao.strftime('%d%m%y'))
    # elif modelo == 'psat':
    #     pathModelo = os.path.join(PATH_ROTINA_CONJUNTO,'Arq_Entrada','Observado')
    #     padraoNomeArquivos = 'psat_{}.txt'.format(dataPrevisao.strftime('%d%m%Y'))

    if rodada:
        pathModelo = pathModelo+'_{:0>2}'.format(rodada)

    patternFiles = os.path.join(pathModelo,padraoNomeArquivos)
    arquivos = glob.glob(patternFiles)
    
    if not len(arquivos):
        raise Exception('Nao foi encontrado nenhum arquivo como seguinte formato: %s' % patternFiles)

    preciptacao = pd.DataFrame()
    for f in arquivos:
            
        match = re.match(r'.*_p[0-9]{6}a([0-9]{6}).dat',f)
        dataPrevista = match.group(1)
        dtPrevista = datetime.datetime.strptime(dataPrevista,'%d%m%y').date()
        prec = leituraPrevEntradaSmap(f,[dtPrevista])

        if preciptacao.empty:
            preciptacao = prec
        else:
            preciptacao[dtPrevista] = prec[dtPrevista]

    return preciptacao


def calcAcumuladosSemanais(modelo, dataPrevisao, rodada, membro=None):

    preciptacao = leituraPrevModelo(modelo, dataPrevisao, rodada, membro)

    precAcumulada = pd.DataFrame()
    if modelo in ['ecmwf-ens-ext']:
        sem = 3
        dtAcumu = {3: []}
    else:
        sem = 1
        dtAcumu = {1: []}

    for dia in range(len(preciptacao.columns)-2):
        if rodada == 18 and dia == 0:
            continue
        if modelo in ['ecmwf-ens-ext']:
            dt = (dataPrevisao + datetime.timedelta(days=15+dia)).date()
        else:
            dt = (dataPrevisao + datetime.timedelta(days=1+dia)).date()

        if dt.weekday() == 6 and len(dtAcumu[sem]) > 0:
            precAcumulada[sem] = preciptacao[dtAcumu[sem]].sum(axis=1)
            sem += 1
            dtAcumu[sem] = [dt]
            continue

        dtAcumu[sem].append(dt)

    precAcumulada[sem] = preciptacao[dtAcumu[sem]].sum(axis=1)

    return precAcumulada, dtAcumu


def plot(preciptacao,titulo=None,pathSave=None):

    import matplotlib.pyplot as plt
    import matplotlib as mpl
    import numpy as np

    figsize = (8,6.18)
    # figsize = (11,9)
    x_lim = None
    y_lim = None

    #shp_path -> ONS/Acompanhamento e Previsão Hidrológica/Dados Gerais do Processo de Previsão de Vazões/Contornos/SIN/Contorno_Bacias_rev2.shp
    shp_path = os.path.join(PATH_ARQUIVOS_AUXILIARES,'shapefiles','Contorno_Bacias_rev2.shp')
    shp_pathBrasil = os.path.join(PATH_ARQUIVOS_AUXILIARES,'shapefiles','brasil.shp')

    sf_bacias = shp.Reader(shp_path, encoding = "latin1")
    sf_brasil = shp.Reader(shp_pathBrasil, encoding = "latin1")

    fig, ax = plt.subplots(figsize = figsize)

    # df = read_shapefile(sf_ba'cias)

    paletaCores = [[255,255,255],[225,255,255],[180,240,250],[150,210,250],[40,130,240],[20,100,210],[103,254,133],[24,215,6],[30,180,30],[255,232,120],[255,192,60],[255,96,0],[225,20,0],[251,94,107],[168,168,168]]
    coresHex = ['#ffffff','#e1ffff','#b4f0fa','#96d2fa','#2882f0','#1464d2','#67fe85','#18d706','#1eb41e','#ffe878','#ffc03c','#ff6000','#e11400','#fb5e6b','#a8a8a8']
    
    bounds = [0,1,5,10,15,20,25,30,40,50,75,100,150,200]

    paleta = mpl.colors.ListedColormap(np.divide(paletaCores,255.0))

    norm = mpl.colors.BoundaryNorm(bounds, len(paletaCores), extend='both')
    sm = plt.cm.ScalarMappable(cmap=paleta,norm=norm)

    for i, shape in enumerate(sf_brasil.shapeRecords()):
        x = [i[0] for i in shape.shape.points[:]]
        y = [i[1] for i in shape.shape.points[:]]
        ax.plot(x, y, color='red', linewidth=0.3)

    bounds.reverse()
    coresHex.reverse()

    colTabela = []
    valTabela = []
    for shape in sf_bacias.shapeRecords():

        nomeBacia = shape.record[9]

        if nomeBacia in ['Correntes','Tapajós','Antas','Manso','Uatuamã','Itiquira','Jauru','Mucuri', 'Paraguaçu', 'Paraíba do Sul']: #Jauru, Paraguacu
            continue

        prec = int(preciptacao[nomeBacia])
        colTabela.append(nomeBacia)
        valTabela.append([prec])

        x = [i[0] for i in shape.shape.points[:]]
        y = [i[1] for i in shape.shape.points[:]]
        ax.plot(x, y, color='black', linewidth=0.5)

        color = '#ffffff'
        for iNivel, nivel in enumerate(bounds):
            if prec >= nivel:
                color = coresHex[iNivel]
                break

        id = shape.record[6]
        shape_ex = sf_bacias.shape(id)
        x_lon = np.zeros((len(shape_ex.points),1))
        y_lat = np.zeros((len(shape_ex.points),1))
        for ip in range(len(shape_ex.points)):
            x_lon[ip] = shape_ex.points[ip][0]
            y_lat[ip] = shape_ex.points[ip][1]

        x0 = np.mean(x_lon)
        y0 = np.mean(y_lat)
        plt.text(x0, y0, prec, fontsize=12)
        ax.fill(x_lon,y_lat, color)
    
    # # t_values = [[int(p)] for p in list(preciptacao.values())]
    # tabela = plt.table(cellText=valTabela,
    # 				  # cellText=t_values,
    #                   colWidths = [0.05],
    #                   rowLabels=colTabela,
    #                   # rowLabels=preciptacao.keys(),
    #                   loc='center right')
    # tabela.set_fontsize(8)
    # x_lim = (-76,-25) 
    # y_lim = (-40,7)

    if (x_lim != None) & (y_lim != None):     
        plt.xlim(x_lim)
        plt.ylim(y_lim)

    if titulo != None:
        plt.title(titulo)

    bounds.reverse()
    sm._A = []
    fig.colorbar(sm,ticks=bounds,label="Preciptação [mm]")

    if pathSave != None:
        fig.savefig(pathSave, dpi=100)
        print('Salvo:', pathSave)
    plt.clf()
    # plt.show()



def plotAcumulados(modelo,dataPrevisao,rodada,membro=None):

    subbacias_smap = get_smap_plot_structure()

    modelo = modelo.lower()
    acumulado, dtsSemanas = calcAcumuladosSemanais(modelo,dataPrevisao,rodada,membro)

    infoSubBacias = pd.DataFrame(subbacias_smap).T
    
    acumulado['bacia'] = infoSubBacias['bacia']
    acumuladoBacia = acumulado.groupby('bacia').mean()

    nomeArquivo = '{{}}semana_energ-r{}{:0>2}_smap.jpeg'.format(dataPrevisao.strftime('%Y%m%d'),rodada)
    if modelo in ['ecmwf-orig','gefs','gfs','ecmwf']:
        pathPlot = os.path.join(PATH_SAIDA_MODELOS,modelo,dataPrevisao.strftime('%Y%m%d')+'{:0>2}'.format(rodada),'semana-energ')
    elif modelo == 'pconjunto':
        pathPlot = os.path.join(PATH_SAIDA_MODELOS,'gefs-eta',dataPrevisao.strftime('%Y%m%d')+'{:0>2}'.format(rodada),'semana-energ')
    elif modelo == 'eta40':
        pathPlot = os.path.join(PATH_SAIDA_MODELOS,'ons-eta',dataPrevisao.strftime('%Y%m%d')+'{:0>2}'.format(rodada),'semana-energ')
    elif modelo == 'ecmwf-ens':
        nomeArquivo = '{{}}semana_energ-r{}{:0>2}_m{:0>2}_smap.jpeg'.format(dataPrevisao.strftime('%Y%m%d'),rodada,membro)
        pathPlot = os.path.join(PATH_SAIDA_MODELOS,'ecmwf-ens',dataPrevisao.strftime('%Y%m%d')+'{:0>2}'.format(rodada),'semana-energ')
    elif modelo == 'ecmwf-ens-ext':
        nomeArquivo = '{{}}semana_energ-r{}{:0>2}_m{:0>2}-ext_smap.jpeg'.format(dataPrevisao.strftime('%Y%m%d'),rodada,membro)
        pathPlot = os.path.join(PATH_SAIDA_MODELOS,'ecmwf-ens',dataPrevisao.strftime('%Y%m%d')+'{:0>2}'.format(rodada),'semana-energ')
    elif modelo == 'gefs-membros':
        nomeArquivo = '{{}}semana_energ-r{}{:0>2}_m{:0>2}_smap.jpeg'.format(dataPrevisao.strftime('%Y%m%d'),rodada,membro)
        pathPlot = os.path.join(PATH_SAIDA_MODELOS,'gefs',dataPrevisao.strftime('%Y%m%d')+'{:0>2}'.format(rodada),'semana-energ')
    elif modelo == 'gefs-membros-ext':
        nomeArquivo = '{{}}semana_energ-r{}{:0>2}_m{:0>2}-ext_smap.jpeg'.format(dataPrevisao.strftime('%Y%m%d'),rodada,membro)
        pathPlot = os.path.join(PATH_SAIDA_MODELOS,'gefs',dataPrevisao.strftime('%Y%m%d')+'{:0>2}'.format(rodada),'semana-energ')
    elif modelo == 'gefs-ext':
        nomeArquivo = '{{}}semana_energ-r{}{:0>2}-ext_smap.jpeg'.format(dataPrevisao.strftime('%Y%m%d'),rodada,membro)
        pathPlot = os.path.join(PATH_SAIDA_MODELOS,'gefs',dataPrevisao.strftime('%Y%m%d')+'{:0>2}'.format(rodada),'semana-energ')

    if not os.path.exists(pathPlot):
        os.makedirs(pathPlot)
    
    semanaTitulo = 'DSTQQSS'
    diaSemana = ['Seg','Ter','Qua','Qui','Sex','Sab','Dom']
    for sem in acumuladoBacia:
        nomeArquivoFinal = nomeArquivo.format(sem)
        if sem == 1:
            titulo = '{} Sem{} - {} (ini: {}{:0>2}z - {})'.format(modelo.upper(),sem,semanaTitulo[-len(dtsSemanas[sem]):],dataPrevisao.strftime('%d%m%y'),rodada,diaSemana[dataPrevisao.weekday()])
        else:
            titulo = '{} Sem{} - {} (ini: {}{:0>2}z - {})'.format(modelo.upper(),sem,semanaTitulo[:len(dtsSemanas[sem])],dataPrevisao.strftime('%d%m%y'),rodada,diaSemana[dataPrevisao.weekday()])
        plot(acumuladoBacia[sem].to_dict(),titulo=titulo,pathSave=os.path.join(pathPlot,nomeArquivoFinal))

def plotPrevDiaria(modelo,dataPrevisao,rodada,membro=None):
    modelo = modelo.lower()
    preciptacao = leituraPrevModelo(modelo,dataPrevisao,rodada,membro)
    preciptacao.drop(['lon','lat'],axis=1,inplace=True)

    # infoSubBacias = pd.DataFrame(subbacias_smap).T
    # preciptacao['bacia'] = infoSubBacias['bacia']
    # preciptacaoBacia = preciptacao.groupby('bacia').mean()

    pathPlot = os.path.join(PATH_SAIDA_MODELOS,modelo,dataPrevisao.strftime('%Y%m%d')+'{:0>2}'.format(rodada),'24-em-24-gifs')
    nomeArquivo = '{}_rain_{}{:0>2}z_f{{}}12z_smap.jpeg'.format(modelo,dataPrevisao.strftime('%Y%m%d'),rodada,rodada)
    titulo = '{} - PREC24 - {{}}12z (ini: {}{:0>2}z)'.format(modelo.upper(), dataPrevisao.strftime('%d%m%y'),rodada)
    
    if modelo in ['gefs','gfs','ecmwf-orig']:
        pass
    elif modelo == 'ecmwf':
        pathPlot = os.path.join(PATH_SAIDA_MODELOS,modelo,dataPrevisao.strftime('%Y%m%d')+'{:0>2}'.format(rodada),'gifs')
    elif modelo == 'pconjunto':
        pathPlot = os.path.join(PATH_SAIDA_MODELOS,'gefs-eta',dataPrevisao.strftime('%Y%m%d')+'{:0>2}'.format(rodada),'24-em-24-gifs')
    elif modelo == 'eta40':
        pathPlot = os.path.join(PATH_SAIDA_MODELOS,'ons-eta',dataPrevisao.strftime('%Y%m%d')+'{:0>2}'.format(rodada),'24-em-24-gifs')
    elif modelo == 'ecmwf-ens':
        nomeArquivo = 'ecmwf{:0>2}_rain_{}{:0>2}z_f{{}}12z_smap.jpeg'.format(membro,dataPrevisao.strftime('%Y%m%d'),rodada,rodada)
        pathPlot = os.path.join(PATH_SAIDA_MODELOS,'ecmwf-ens-orig',dataPrevisao.strftime('%Y%m%d')+'{:0>2}'.format(rodada),'gifs')
    elif modelo == 'psat':
        pathPlot = os.path.join(PATH_DADOS,'psat','zgifs')
        titulo = '{} - PREC24 - {{}} - 12z'.format(modelo.upper(),rodada, dataPrevisao.strftime('%d%m%y'),rodada)
        nomeArquivo = 'psat_{{}}12z_smap.png'.format(rodada)
    if not os.path.exists(pathPlot):
        os.makedirs(pathPlot)

    for dt in preciptacao.columns:
        if (dataPrevisao+datetime.timedelta(days=1)).date() == dt and rodada == 18:
            continue
        nomeArquivoFinal = nomeArquivo.format(dt.strftime('%Y%m%d'))
        tituloFinal = titulo.format(dt.strftime('%d%m%y'))
        plotSubbacias(preciptacao[dt].to_dict(),titulo=tituloFinal,pathSave=os.path.join(pathPlot,nomeArquivoFinal))



def plotSubbacias(preciptacao,titulo=None,pathSave=None,paletaCores= paletaCores, coresHex= coresHex, bounds = bounds ):
    dir_bnl = os.path.join(PATH_ARQUIVOS_AUXILIARES, 'contornos_bacias')

    subbacias_smap = get_smap_plot_structure()

    import matplotlib.pyplot as plt
    import matplotlib as mpl
    import numpy as np

    figsize = (8,6.18)
    # figsize = (11,9)
    x_lim = None
    y_lim = None

    infoSubBacias = pd.DataFrame(subbacias_smap).T
    shp_pathBrasil = os.path.join(PATH_ARQUIVOS_AUXILIARES,'shapefiles','brasil.shp')
    shp_pathBacias = os.path.join(PATH_ARQUIVOS_AUXILIARES,'shapefiles','Contorno_Bacias_rev2.shp')
    
    sf_bacias = shp.Reader(shp_pathBacias, encoding = "latin1")
    sf_brasil = shp.Reader(shp_pathBrasil, encoding = "latin1")

    fig, ax = plt.subplots(figsize = figsize)

    paleta = mpl.colors.ListedColormap(np.divide(paletaCores,255.0))

    norm = mpl.colors.BoundaryNorm(bounds, len(paletaCores), extend='both')
    sm = plt.cm.ScalarMappable(cmap=paleta,norm=norm)

    for i, shape in enumerate(sf_brasil.shapeRecords()):
        x = [i[0] for i in shape.shape.points[:]]
        y = [i[1] for i in shape.shape.points[:]]
        ax.plot(x, y, color='red', linewidth=0.3)

    bounds.reverse()
    coresHex.reverse()

    baciasPlotada = {}
    for subbac in preciptacao:

        bac = infoSubBacias.loc[subbac]['bacia']

        prec = preciptacao[subbac]
        color = '#ffffff'
        for iNivel, nivel in enumerate(bounds):
            if prec >= nivel:
                color = coresHex[iNivel]
                break
        
        try:
            arq_bnl = os.path.join(dir_bnl, infoSubBacias.loc[subbac]['pastaContorno'], infoSubBacias.loc[subbac]['nomeSmap']+'.bln')
        except:
            pdb.set_trace()
        try:
            df_coordenadas = pd.read_csv(arq_bnl, parse_dates=True, skiprows=1, delimiter=',', names=['lon', 'lat', 'aa'])
        except:
            df_coordenadas = pd.read_csv(arq_bnl, parse_dates=True, skiprows=1, delimiter=',', names=['lon', 'lat'])

        ax.fill(df_coordenadas['lon'],df_coordenadas['lat'], color=color)

        if bac not in baciasPlotada:
            baciasPlotada[bac] = [prec]
        else:
            baciasPlotada[bac].append(prec)
    
    for shape in sf_bacias.shapeRecords():
        bac = shape.record[9]
        if bac in baciasPlotada:
            x = [i[0] for i in shape.shape.points[:]]
            y = [i[1] for i in shape.shape.points[:]]
            ax.plot(x, y, color='black', linewidth=0.5)

            if bac in locPrec:
                x0 = locPrec[bac][0]
                y0 = locPrec[bac][1]
            else:

                x0 = round(sum(x) / len(x),1)
                y0 = round(sum(y) / len(y),1)
                print('locPrec[{}]=({},{})'.format(bac,x0,y0))
            prec = round(sum(baciasPlotada[bac]) / len(baciasPlotada[bac]))
            plt.text(x0, y0, prec, fontsize=9)

    bounds.reverse()
    coresHex.reverse()

    sm._A = []
    fig.colorbar(sm,ticks=bounds,label="Preciptação [mm]")

    if titulo != None:
        plt.title(titulo)

    if pathSave != None:
        fig.savefig(pathSave, dpi=100)
        print('Salvo:', pathSave)
    else:
        plt.show()

    plt.clf()
    # plt.show()



def plotAcumuladosSubbacias(modelo,dataPrevisao,rodada,membro=None):

    modelo = modelo.lower()
    acumulado, dtsSemanas = calcAcumuladosSemanais(modelo,dataPrevisao,rodada,membro)
    diaSemana = ['Seg','Ter','Qua','Qui','Sex','Sab','Dom']
    nomeArquivo = '{{}}semana_energ-r{}{:0>2}_smap.jpeg'.format(dataPrevisao.strftime('%Y%m%d'),rodada)
    titulo = '{} Sem{{}} - {{}} (ini: {}{:0>2}z - {})'.format(modelo.upper(),dataPrevisao.strftime('%d%m%y'),rodada,diaSemana[dataPrevisao.weekday()])
    if modelo in ['ecmwf-orig','gefs','gfs','ecmwf']:
        pathPlot = os.path.join(PATH_SAIDA_MODELOS,modelo,dataPrevisao.strftime('%Y%m%d')+'{:0>2}'.format(rodada),'semana-energ')
    elif modelo == 'pconjunto':
        pathPlot = os.path.join(PATH_SAIDA_MODELOS,'gefs-eta',dataPrevisao.strftime('%Y%m%d')+'{:0>2}'.format(rodada),'semana-energ')
    elif modelo == 'eta40':
        pathPlot = os.path.join(PATH_SAIDA_MODELOS,'ons-eta',dataPrevisao.strftime('%Y%m%d')+'{:0>2}'.format(rodada),'semana-energ')
    elif modelo == 'ecmwf-ens':
        nomeArquivo = '{{}}semana_energ-r{}{:0>2}_m{:0>2}_smap.jpeg'.format(dataPrevisao.strftime('%Y%m%d'),rodada,membro)
        titulo = '{}-{:0>2} Sem{{}} - {{}} (ini: {}{:0>2}z - {})'.format(modelo.upper(),membro,dataPrevisao.strftime('%d%m%y'),rodada,diaSemana[dataPrevisao.weekday()])
        pathPlot = os.path.join(PATH_SAIDA_MODELOS,'ecmwf-ens-orig',dataPrevisao.strftime('%Y%m%d')+'{:0>2}'.format(rodada),'semana-energ')
    elif modelo == 'ecmwf-ens-ext':
        nomeArquivo = '{{}}semana_energ-r{}{:0>2}_m{:0>2}-ext_smap.jpeg'.format(dataPrevisao.strftime('%Y%m%d'),rodada,membro)
        titulo = '{}-{:0>2} Sem{{}} - {{}} (ini: {}{:0>2}z - {})'.format(modelo.upper(),membro,dataPrevisao.strftime('%d%m%y'),rodada,diaSemana[dataPrevisao.weekday()])
        pathPlot = os.path.join(PATH_SAIDA_MODELOS,'ecmwf-ens-orig',dataPrevisao.strftime('%Y%m%d')+'{:0>2}'.format(rodada),'semana-energ')
    elif modelo == 'gefs-membros':
        nomeArquivo = '{{}}semana_energ-r{}{:0>2}_m{:0>2}_smap.jpeg'.format(dataPrevisao.strftime('%Y%m%d'),rodada,membro)
        titulo = '{}-{:0>2} Sem{{}} - {{}} (ini: {}{:0>2}z - {})'.format(modelo.upper(),membro,dataPrevisao.strftime('%d%m%y'),rodada,diaSemana[dataPrevisao.weekday()])
        pathPlot = os.path.join(PATH_SAIDA_MODELOS,'gefs',dataPrevisao.strftime('%Y%m%d')+'{:0>2}'.format(rodada),'semana-energ')
    elif modelo == 'gefs-membros-ext':
        nomeArquivo = '{{}}semana_energ-r{}{:0>2}_m{:0>2}-ext_smap.jpeg'.format(dataPrevisao.strftime('%Y%m%d'),rodada,membro)
        titulo = '{}-{:0>2} Sem{{}} - {{}} (ini: {}{:0>2}z - {})'.format(modelo.upper(),membro,dataPrevisao.strftime('%d%m%y'),rodada,diaSemana[dataPrevisao.weekday()])
        pathPlot = os.path.join(PATH_SAIDA_MODELOS,'gefs',dataPrevisao.strftime('%Y%m%d')+'{:0>2}'.format(rodada),'semana-energ')
    elif modelo == 'gefs-ext':
        nomeArquivo = '{{}}semana_energ-r{}{:0>2}-ext_smap.jpeg'.format(dataPrevisao.strftime('%Y%m%d'),rodada,membro)
        titulo = '{}-{:0>2} Sem{{}} - {{}} (ini: {}{:0>2}z - {})'.format(modelo.upper(),membro,dataPrevisao.strftime('%d%m%y'),rodada,diaSemana[dataPrevisao.weekday()])
        pathPlot = os.path.join(PATH_SAIDA_MODELOS,'gefs',dataPrevisao.strftime('%Y%m%d')+'{:0>2}'.format(rodada),'semana-energ')

    if not os.path.exists(pathPlot):
        os.makedirs(pathPlot)
    
    semanaTitulo = 'DSTQQSS'
    for sem in acumulado:
        nomeArquivoFinal = nomeArquivo.format(sem)
        if sem == 1:
            tituloFinal = titulo.format(sem,semanaTitulo[-len(dtsSemanas[sem]):])
        else:
            tituloFinal = titulo.format(sem,semanaTitulo[:len(dtsSemanas[sem])])

        plotSubbacias(acumulado[sem].to_dict(),titulo=tituloFinal,pathSave=os.path.join(pathPlot,nomeArquivoFinal))


def plot_diff_acumul_subbacia(dados_modelo1, dados_modelo2, path_file=None):

    bounds=[-30, -20, -10, -5, -2, 0 , 2, 5 ,10, 20, 30 ]
    coresHex=['#e11400','#ff6000','#ff8c00','#ffc03c','#ffe878','#ffffff','#ffffff','#e1ffff','#b4f0fa','#96d2fa','#2882f0','#1464d2']
    paletaCores=[[225,20,0],[255,96,0],[255,140,0],[255,192,60],[255,232,120],[255,255,255],[255,255,255],[225,255,255],[180,240,250],[150,210,250],[40,130,240],[20,100,210]]

    data_rodada1, modelo_name1, hr_rodada1 = dados_modelo1
    data_rodada2, modelo_name2, hr_rodada2 = dados_modelo2

    #tranformando para datetime
    data_rodada1 = datetime.datetime.strptime(data_rodada1,"%d/%m/%Y")
    data_rodada2 = datetime.datetime.strptime(data_rodada2,"%d/%m/%Y")
    
    #padrao nome no mapa
    dt_base = data_rodada1.strftime('%d/%m/%y')
    dt_subtrai = data_rodada2.strftime('%d/%m/%y')
    name = f"{modelo_name1} ({dt_base} {str(hr_rodada1).zfill(2)}z) - {modelo_name2} ({dt_subtrai} {str(hr_rodada2).zfill(2)}z)"

    if not path_file:
        path_file = f"/WX2TB/Documentos/saidas-modelos/gefs-eta/{data_rodada1.strftime('%Y%m%d')}00/semana-energ/"

    os.makedirs(path_file, exist_ok=True)

    df_diff_diario, df_diff_semanal = rz_chuva.dif_tb_chuva(dados_modelo1, dados_modelo2,granularidade='subbacia')
    padrao_name = data_rodada1.strftime('%d%m%y') + '00z'

    for i,semana in enumerate(df_diff_semanal.columns):
        preciptacao = df_diff_semanal[semana].reset_index('submercado',drop=True).to_dict()
        titulo = f"Sem{i+1} - {semana} ({name}) "
        file_name = f"{i+1}semana_energ_DIF_{modelo_name1}-{modelo_name2}_{padrao_name}.png"
        pathSave = os.path.join(path_file,file_name)
        plotSubbacias(preciptacao,titulo=titulo,bounds=bounds,coresHex=coresHex,paletaCores=paletaCores,pathSave=pathSave)


if __name__ == '__main__':

    modelo = 'GFS'
    dataPrevisao = datetime.datetime(2022,1,23)
    rodada = 12
    # plotAcumulados(modelo,dataPrevisao,rodada)
    # plotPrevDiaria(modelo,dataPrevisao,rodada)

    plotAcumuladosSubbacias(modelo,dataPrevisao,rodada)


