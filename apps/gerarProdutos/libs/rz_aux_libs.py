


import os
import re
import sys
import glob
import locale
import datetime
import requests as req
import sqlalchemy as db
from dateutil import relativedelta
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from PIL import Image
from pandas.plotting import register_matplotlib_converters
from matplotlib import gridspec
register_matplotlib_converters()

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")),'.env'))
from middle.utils import Constants
constants = Constants()

URL_COGNITO = os.getenv('URL_COGNITO')
CONFIG_COGNITO = os.getenv('CONFIG_COGNITO')
try:
	locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
except:
	locale.setlocale(locale.LC_ALL, '')


dirLibs = os.path.dirname(os.path.abspath(__file__))
dirApp = os.path.dirname(dirLibs)
pathArquivos = os.path.join(dirApp,'arquivos')


path_fontes = "/WX2TB/Documentos/fontes/"
sys.path.insert(1,path_fontes)
from PMO.scripts_unificados.bibliotecas import wx_dbLib,wx_emailSender,wx_verificaCorImg,wx_dbClass
from PMO.scripts_unificados.apps.gerarProdutos.libs import rz_relatorio_bbce,configs
from PMO.scripts_unificados.apps.web_modelos.server.libs import wx_calcEna 


def get_access_token() -> str:
    response = req.post(
        URL_COGNITO,
        data=CONFIG_COGNITO,
        headers={'Content-Type': 'application/x-www-form-urlencoded'}
    )
    return response.json()['access_token']


    confgs = configs.getConfiguration()

    pastaTmp = confgs['paths']['pastaTmp']

    if not os.path.exists(pastaTmp):
        os.makedirs(pastaTmp)

    dtRodada = '{}{:0>2}'.format(data.strftime('%Y%m%d'), rodada)

    
    sufixosDesconnsiderar = ["_pn.gif","_lim.gif","-periodo_ec9d.gif","_est.gif","_est_anom.gif"]
    path = os.path.abspath('/WX2TB/Documentos/saidas-modelos/NOVAS_FIGURAS')

    if modelo in ['ecmwf-orig']: # ,'ecmwf-ens'
        pattern = os.path.join(path,modelo,dtRodada,'semana-energ','[0-9]semana_energ-*{}*'.format(dtRodada))

    elif modelo == 'cfsv2':
        path = os.path.abspath('/WX2TB/Documentos/saidas-modelos')
        pattern = os.path.join(path,modelo,data.strftime('%Y%m%d'),dtRodada,'semana-energ','[0-9]semana_energ-{}_ensemble*dias*{}'.format(modelo,data.strftime('%Y%m%d')))
        sufixosDesconnsiderar = ["_pn.gif","_lim.gif","-periodo_ec9d.gif","_est.gif","_est_anom.gif"]
    elif modelo == 'gefs':
        pattern = os.path.join(path,modelo,dtRodada,'semana-energ','[0-9]semana_energ-r{}*'.format(dtRodada))
        sufixosDesconnsiderar = ["_pn.gif","_lim.gif","-periodo_ec9d.gif","_est.gif","_est_anom.gif","_m[0-9]{2}-ext_smap.jpeg","_m[0-9]{2}_smap.jpeg"]
    
    elif modelo == 'gefs-eta':
            pattern = os.path.join(path,modelo,dtRodada,'semana-energ','[0-9]semana_energ-r{}_smap*'.format(dtRodada))
            pattern2 = os.path.join(path,modelo,dtRodada,'semana-energ','[0-9]semana_energ_DIF_PCONJUNTO-PCONJUNTO_{}{:0>2}*'.format(data.strftime('%d%m%y'),rodada))
    
    elif modelo.lower() == 'ecmwf-ens':
        pattern = os.path.join(path,modelo,dtRodada,'semana-energ','EC-ENS_[0-9]semana_energ-r{}*'.format(dtRodada))

    elif modelo.lower() == 'ecmwf-ens-estendido':
        pattern = os.path.join(path,modelo,dtRodada,'semana-energ','EC-ENS-ESTENDIDO_[0-9]semana_energ-r{}*'.format(dtRodada))

    elif modelo.lower() == 'diferença-ecmwf-estendido':
        pattern = os.path.join(path,'ecmwf-ens-estendido',dtRodada,'dif_prec','*_dif_acumulado_*-r{}*'.format(dtRodada))

    else:
        pattern = os.path.join(path,modelo,dtRodada,'semana-energ','[0-9]semana_energ-r{}*'.format(dtRodada))
        
    print('Formato:',pattern)

    files = []
    if modelo =='gefs-eta':
        files +=glob.glob(pattern+'.jpeg')
        files += glob.glob(pattern2+'.png')
    
    else:
        for extensao in ['.gif','.jpeg','.png']:
            files += glob.glob(pattern+extensao)

    arquivosDesconsiderar = []
    for i,f in enumerate(files):

        for sufix in sufixosDesconnsiderar:
            if re.search(sufix, f):
                arquivosDesconsiderar.append(f)
                break
  
            if sufix in f:
                arquivosDesconsiderar.append(f)
                break

        if not wx_verificaCorImg.imagemColorida(f) and "semana_energ_DIF_" not in f:
            arquivosDesconsiderar.append(f)
            continue

    files = [f for f in files if f not in arquivosDesconsiderar]

    files.sort()

    numImgs_v = len(files) if len(files) <=3 else 2
    numImgs_h = int(len(files)/numImgs_v)

    if len(files)%numImgs_v:
        numImgs_h += 1

    im = Image.open(files[0])
    imgWidth = im.width
    imgHeight = im.height

    dst = Image.new('RGB', (imgWidth * numImgs_v, imgHeight*numImgs_h))
    for i, img in enumerate(files):
        # print(img)
        col = int(i % numImgs_v)
        row = int(i / numImgs_v)
        im = Image.open(img)
        dst.paste(im, (imgWidth*col, imgHeight*row))


    arquivoSaida = '{}/{}_{}z.jpg'.format(pastaTmp, modelo, dtRodada)
    dst.save(arquivoSaida)
    print('Arquivo salvo em :\n{}'.format(arquivoSaida))

    return arquivoSaida

def is_first_bussines_day(data:datetime.date):
    return (data.day == 1 and (data.weekday() != 5 and data.weekday() != 6)) or ((data.day == 2 or data.day == 3) and data.weekday() == 0)

def geraRelatorioBbce(data = datetime.datetime.now()):

    if is_first_bussines_day(data.date()):
        
        quantidade_produtos = {}
        quantidade_produtos['mensal'] = 6
        quantidade_produtos['trimestral'] = 4
        quantidade_produtos['semestral'] = 4
        quantidade_produtos['anual'] = 6
        
        lista_produtos = []
        for graniularidade in quantidade_produtos:
            for i_produtos in range(quantidade_produtos[graniularidade]):
                if graniularidade == 'mensal':
                    dt_referente = data + relativedelta.relativedelta(months=i_produtos)
                    produto = f"SE CON MEN {dt_referente.strftime('%B').upper()[:3]}/{dt_referente.strftime('%y')} - Preço Fixo"

                elif graniularidade == 'trimestral':
                    dt_inicial = data
                    while dt_inicial.month%3 != 1:
                        dt_inicial = dt_inicial + relativedelta.relativedelta(months=1)
                    dt_inicial = dt_inicial + relativedelta.relativedelta(months=i_produtos*3)
                    dt_final = dt_inicial + relativedelta.relativedelta(months=2)
                    produto = f"SE CON TRI {dt_inicial.strftime('%B').upper()[:3]}/{dt_inicial.strftime('%y')} {dt_final.strftime('%B').upper()[:3]}/{dt_final.strftime('%y')} - Preço Fixo"
                elif graniularidade == 'semestral':
                    dt_inicial = data
                    while dt_inicial.month%6 != 0:
                        dt_inicial = dt_inicial + relativedelta.relativedelta(months=1)
                    dt_inicial = dt_inicial + relativedelta.relativedelta(months=1+i_produtos*6)
                    dt_final = dt_inicial + relativedelta.relativedelta(months=5)
                    produto = f"SE CON SEM {dt_inicial.strftime('%B').upper()[:3]}/{dt_inicial.strftime('%y')} {dt_final.strftime('%B').upper()[:3]}/{dt_final.strftime('%y')} - Preço Fixo"
                elif graniularidade == 'anual':
                    dt_inicial = data
                    while dt_inicial.month%12 != 0:
                        dt_inicial = dt_inicial + relativedelta.relativedelta(months=1)
                    dt_inicial = dt_inicial + relativedelta.relativedelta(months=1+i_produtos*12)
                    dt_final = dt_inicial + relativedelta.relativedelta(months=11)
                    produto = f"SE CON ANU {dt_inicial.strftime('%B').upper()[:3]}/{dt_inicial.strftime('%y')} {dt_final.strftime('%B').upper()[:3]}/{dt_final.strftime('%y')} - Preço Fixo"

                lista_produtos.append(produto)

        dados_a_inserir = [{"str_produto": produto, "ordem":i} for i, produto in enumerate(lista_produtos)]
        
        req.post(f"{constants.BASE_URL}/api/v2/bbce/produtos-interesse",
                      json=dados_a_inserir,
                      headers={
                'Authorization': f'Bearer {get_access_token()}'
            })
        # insert_prod_interesse = 
        
  

    body, path_graficos = rz_relatorio_bbce.get_tabela_bbce(data)

    
    return body, path_graficos

def autolabelMediaPld(ax, linhas, pontoInicio=0, size=6, cor='black'):
    """Attach a text label above each bar in *rects*, displaying its height."""
    eixo_x = linhas.get_data()[0]
    eixo_y = linhas.get_data()[1]

    y = eixo_y[0] # altura sera a vazao
    ax.annotate('R${:.2f}'.format(y),
                xy=(pontoInicio, y),
                xytext=(0, 2),  # 3 points vertical offset
                textcoords="offset points",
                ha='center', va='bottom', size=size, color=cor)

def gerarResultadoDessem(data):

    idSubmercados = {1:'SE', 2:'S', 3:'NE', 4:'N'}
    siglaSubmercados = ['SE', 'S', 'NE', 'N']

    corpoEmail = '<h3>Resultado da rodada WX do DESSEM referente ao dia {}</h3>'.format(data.strftime('%d/%m/%Y'))

    corpoEmail += '<small> <ul>'
    corpoEmail += '<li><b>wx_p (preliminar):</b> Utiliza o deck do dia anterior como base dos arquivos de entrada</li>'
    corpoEmail += '<li><b>wx_d (definitivo):</b> Utiliza o deck do ONS do dia atual como base dos arquivos de entrada</li>'
    corpoEmail += '</ul> </small>'

    db_decks = wx_dbClass.db_mysql_master('db_decks')
    db_decks.connect()
    tb_cadastro_dessem = db_decks.getSchema('tb_cadastro_dessem')
    tb_ds_pdo_cmosist = db_decks.getSchema('tb_ds_pdo_cmosist')
    tb_fontes = db_decks.getSchema('tb_fontes')

    query_dessem = db.select([
        tb_ds_pdo_cmosist.c.cd_submercado, 
        tb_ds_pdo_cmosist.c.vl_horario,
        tb_ds_pdo_cmosist.c.vl_preco,
        tb_fontes.c.str_fonte,
        ]
        ).where(
            tb_cadastro_dessem.c.dt_referente == data.strftime("%Y-%m-%d"),
        ).join(
            tb_cadastro_dessem,
            tb_cadastro_dessem.c.id_fonte == tb_fontes.c.cd_fonte,
        ).join(
            tb_ds_pdo_cmosist,
            tb_ds_pdo_cmosist.c.id_deck == tb_cadastro_dessem.c.id,
        )
    
    answer = db_decks.db_execute(query_dessem).fetchall()
    precos = pd.DataFrame(answer,columns=['SUBMERCADO', 'HORARIO', 'CMO','FONTE'])

    db_ons = wx_dbClass.db_mysql_master('db_ons')
    db_ons.connect()
    tb_pld = db_ons.getSchema('tb_pld')

    query_pld = db.select(
      tb_pld.c.str_ano,
      tb_pld.c.vl_PLDmin,
      tb_pld.c.vl_PLDmax_hora,
      tb_pld.c.vl_PLDmax_estr,

      ).where(
          tb_pld.c.str_ano == data.strftime('%Y'))

    answer = db_ons.db_execute(query_pld).fetchall()
    pld_min = answer[0][1]
    pld_max = answer[0][2]
    
    precos['SUBMERCADO'] = precos['SUBMERCADO'].replace(idSubmercados)
    precos['HORARIO'] = precos['HORARIO'].astype(str).str[0:5]
    precos['FONTE'] = precos['FONTE'].str.lower()

    precos['PLD'] = precos['CMO'] 
    precos.loc[precos['PLD'] < pld_min, 'PLD'] = pld_min
    precos.loc[precos['PLD'] > pld_max, 'PLD'] = pld_max

    mmm = precos.groupby(['FONTE', 'SUBMERCADO']).agg({'PLD': ['mean', 'min', 'max']})

    pld_diario_max = answer[0][3]
    for fonte in precos['FONTE'].unique():
        for sub in precos['SUBMERCADO'].unique():
            media = mmm.loc[fonte, 'PLD'].loc[sub, 'mean']
            if media > pld_diario_max:
                fator = media/pld_diario_max
                precos.loc[(precos['SUBMERCADO']==sub) & (precos['FONTE']==fonte), 'PLD'] = precos.loc[(precos['SUBMERCADO']==sub) & (precos['FONTE']==fonte),'PLD']/fator

    mmm = precos.groupby(['FONTE', 'SUBMERCADO']).agg({'PLD': ['mean', 'min', 'max']})
    mmm = mmm.reset_index()
    mmm = mmm.sort_values('SUBMERCADO', ascending=False)

    
    fig = plt.figure(figsize=(20, 10))
    gs = gridspec.GridSpec(2, 3)
    
    fig.suptitle('CMO horário ({})'.format(data.strftime('%d/%m/%Y')), fontsize=16)

    table_image = [['Submercado', 'Fonte', 'PLD']]
    tabela = []
    for index, row in mmm.iterrows():
        valMedia = ('R${:.2f}'.format(row['PLD']['mean'])).replace('.', ',')
        valMin = ('R${:.2f}'.format(row['PLD']['min'])).replace('.', ',')
        valMax = ('R${:.2f}'.format(row['PLD']['max'])).replace('.', ',')
        linhaTabela = [row.values[1], row.values[0], valMedia, valMin, valMax]

        table_image.append([row.values[1], row.values[0], valMedia])
        tabela.append(linhaTabela)


    ax = plt.subplot(gs[:, 2])
    ax.axis('off')
    table = ax.table(cellText=table_image, loc='upper center')

    table.set_fontsize(12)
    table.scale(0.7, 1.3)

    corpoEmail += '<p>Média diária</p>'
    header = ['Submercado', 'Fonte', 'Média', 'Mínima', 'Máxima']
    corpoEmail += wx_emailSender.gerarTabela(tabela, header, nthColor=len(precos['FONTE'].unique()))

    precos = pd.pivot_table(precos, index=['HORARIO', 'FONTE'], columns='SUBMERCADO', values='CMO', aggfunc='first')

    precos = precos[siglaSubmercados]
    precos = precos.reset_index()

    
    cores = {'ccee':'blue', 'ons':'coral', 'rz_p':'cadetblue', 'rz_d':'darkslategray', 'rz_c':'darkorange'}
    ordem = {'ccee':5, 'ons':1, 'rz_p':3, 'rz_d':4, 'rz_c':2}

    axes_list = [plt.subplot(gs[0,0]),plt.subplot(gs[0,1]), plt.subplot(gs[1,0]) ,plt.subplot(gs[1,1])]

    for i, sub in enumerate(siglaSubmercados):
        
        ax =axes_list[i]
        ax.set_title(sub)
        
        for index, simulacao in precos.groupby('FONTE').__iter__():
            x = simulacao['HORARIO']
            y = simulacao[sub]



            ax.plot(x, y, label=index, color=cores[index], zorder=ordem[index])

            x_media = [y.mean()]*len(x)
            media = ax.plot(x, x_media, color=cores[index], linestyle='--', linewidth=.7)
            autolabelMediaPld(ax, media[0], 0, size=8, cor=cores[index])
            

        ax.legend()
        ax.tick_params(axis='x', labelrotation=-40)
        ax.set_ylabel('Preço [R$]')
        ax.grid(color = 'gray', linestyle = '--', linewidth = 0.2)

    pathFileOut = os.path.join(pathArquivos, 'tmp', 'CMO_HORARIO.PNG')
    plt.subplots_adjust(hspace=0.5, wspace=0.2)
    plt.savefig(pathFileOut)

    tabela = []
    for index, row in precos.iterrows():
        linhaTabela = [row['HORARIO'], row['FONTE']]
        for sub in ['SE', 'S', 'NE', 'N']:
            linhaTabela.append(('R${:.2f}'.format(row[sub])).replace('.', ','))
        tabela.append(linhaTabela)

    corpoEmail += '<p>Preço horário</p>'
    header = ['Horario', 'Fonte', 'Sudeste', 'Sul', 'Nordeste', 'Norte']
    corpoEmail += wx_emailSender.gerarTabela(tabela, header, nthColor=len(precos['FONTE'].unique()))

    return corpoEmail, pathFileOut

def autolabel(ax, linhas, pontoInicio=0, size=6):
    """Attach a text label above each bar in *rects*, displaying its height."""
    eixo_x = linhas.get_data()[0]
    eixo_y = linhas.get_data()[1]

    for i, x in enumerate(eixo_x[:-1]):
        y = eixo_y[i] # altura sera a vazao
        ax.annotate('{}'.format(int(round(y))),
                    xy=(pontoInicio + datetime.timedelta(days=i*7 + 4), y),
                    xytext=(0, 2),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom', size=size)

def autolabelAcomph(ax, linhas, cor='black'):
    """Attach a text label above each bar in *rects*, displaying its height."""
    eixo_x = linhas.get_data()[0]
    eixo_y = linhas.get_data()[1]

    for i, x in enumerate(eixo_x):
        if (i+1)%3 != 0:
            continue
        y = eixo_y[i] # altura sera a vazao
        ax.annotate('{}'.format(int(round(y))),
                    xy=(x, y),
                    xytext=(0, 2),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom', size=10, color=cor)

def autolabelMlt(ax, linhas, cor='black'):
    """Attach a text label above each bar in *rects*, displaying its height."""
    eixo_x = linhas.get_data()[0]
    eixo_y = linhas.get_data()[1]

    for i, x in enumerate(eixo_x[:-1]):
        y = eixo_y[i] # altura sera a vazao
        localizacao = eixo_x[i] + (eixo_x[i+1] - eixo_x[i])/2
        ax.annotate('{}'.format(int(round(y))),
                    xy=(localizacao, y),
                    xytext=(0, 2),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom', size=10, color=cor)

def autolabelMedia(ax, linhas, percentuais, cor='black'):
    """Attach a text label above each bar in *rects*, displaying its height."""
    eixo_x = linhas.get_data()[0]
    eixo_y = linhas.get_data()[1]

    for i, x in enumerate(eixo_x[:-1]):
        y = eixo_y[i] # altura sera a vazao
        localizacao = eixo_x[i] + (eixo_x[i+1] - eixo_x[i])/2
        ax.annotate('{} ({}%)'.format(int(round(y)), int(round(percentuais[i]))),
                    xy=(localizacao, y),
                    xytext=(0, 2),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom', size=11, color=cor)

def gerarGraficoAcomph(dt):

    pathLocal = os.path.dirname(os.path.abspath(__file__))
    dir_saida = os.path.join(pathLocal, 'arquivos', 'tmp')
    
    if not os.path.exists(dir_saida):
        os.makedirs(dir_saida)

    iMesAtual = (dt-datetime.timedelta(days=1)).replace(day=1)
    fMesAnterior = iMesAtual-datetime.timedelta(days=1)
    iMesAnterior = fMesAnterior.replace(day=1)


    acomph = wx_dbLib.get_acomph_cached(dataInicial=iMesAnterior, granularidade='submercado')
    df_acomph_ena = pd.DataFrame(acomph)
    df_acomph_ena.index = pd.to_datetime(df_acomph_ena.index)
    ultimosTrintaDias = df_acomph_ena.loc[(dt-datetime.timedelta(days=30)).strftime('%Y-%m-%d'):]

    mediaMesAnterior = df_acomph_ena.loc[iMesAnterior.strftime('%Y-%m-%d'):fMesAnterior.strftime('%Y-%m-%d')].mean()
    mediaMesAtual = df_acomph_ena.loc[iMesAtual.strftime('%Y-%m-%d'):].mean()

    media = pd.DataFrame()
    media[dt-datetime.timedelta(days=31)] = mediaMesAnterior
    media[iMesAtual] = mediaMesAtual
    media[max(df_acomph_ena.index)] = np.nan

    mlt = wx_dbLib.getMlt()
    df_mlt = pd.DataFrame(mlt, columns=['SUBMERCADO', 'MES', 'MLT'])
    # mesesMlt = list(ultimosTrintaDias.index.to_series().dt.month.unique())
    dtsMlt = [dt-datetime.timedelta(days=31), iMesAtual]
    mesesMlt = [d.month for d in dtsMlt]
    df_mlt = (df_mlt[df_mlt['MES'].isin(mesesMlt)]).copy()
    df_mlt['DT_REFERENTE'] = df_mlt['MES'].replace(dict(zip(mesesMlt, dtsMlt)))
    df_mlt['SUBMERCADO'].replace({1:'N', 2:'NE', 3:'SE', 4:'S'}, inplace=True)
    df_mlt = df_mlt.pivot(index='SUBMERCADO', columns='DT_REFERENTE', values='MLT')
    if max(df_acomph_ena.index) != iMesAtual:
        df_mlt[max(df_acomph_ena.index)] = np.nan

    # A data referente do Acomph e d-1. Para pegar o acomph do dia anterior e necessario pegar do d-2
    acomphDiaAnterior = wx_dbLib.getAcomphEspecifico(dtAcomph=dt-datetime.timedelta(days=2))
    df_acomphDiaAnterior = pd.DataFrame(acomphDiaAnterior, columns=['DT_REFERENTE', 'CD_POSTO', 'VL_EAR_LIDO', 'VL_EAR_CONSO', 'VL_VAZ_DEF_LIDO', 'VL_VAZ_DEF_CONSO', 'VL_VAZ_AFL_LIDO', 'VL_VAZ_AFL_CONSO', 'VL_VAZ_INC_CONSO', 'VL_VAZ_NAT_CONSO', 'DT_ACOMPH'])
    df_acomphDiaAnterior_vazNat = df_acomphDiaAnterior.pivot(index='CD_POSTO', columns='DT_REFERENTE', values='VL_VAZ_NAT_CONSO')
    df_acomphDiaAnterior_vazNat = wx_calcEna.calcPostosArtificiais_df(df_acomphDiaAnterior_vazNat, ignorar_erros=True)
    df_acomphDiaAnterior_ena = wx_calcEna.gera_ena_df(df_acomphDiaAnterior_vazNat)
    df_acomphDiaAnterior_ena = df_acomphDiaAnterior_ena.T

    tituloGrafico = 'Acomph ({})'.format(dt.strftime('%d/%m/%Y'))
    fig, ax =  plt.subplots(2,2)
    plt.gcf().set_size_inches(20, 10)
    plt.rcParams['axes.grid'] = True
    fig.suptitle('{}'.format(tituloGrafico), fontsize=16)

    submercados = ['SE', 'S', 'NE', 'N']
    for i, sub in enumerate(submercados):
        ax[int(i/2), i%2].set_title(sub)
        
        curvaAcomph = ax[int(i/2), i%2].plot(ultimosTrintaDias[sub].index, ultimosTrintaDias[sub].values, label="ACOMPH", color="darkturquoise", zorder=3)
        autolabelAcomph(ax[int(i/2), i%2], curvaAcomph[0], cor="darkturquoise")

        curvaAcomphAnterior = ax[int(i/2), i%2].plot(df_acomphDiaAnterior_ena[sub].index, df_acomphDiaAnterior_ena[sub].values, label="ACOMPH (Ant)", color="slategrey", linestyle = '--', zorder=2)

        curvaMediaMensal = ax[int(i/2), i%2].step(media.loc[sub].index, media.loc[sub].values, where='post', label="Média mensal", color="midnightblue", linestyle = '--', linewidth = 1, zorder=4)
        percentuais = (media.loc[sub].values/df_mlt.loc[sub].values)*100
        autolabelMedia(ax[int(i/2), i%2], curvaMediaMensal[0], percentuais, cor="midnightblue")

        curvaMlt = ax[int(i/2), i%2].step(df_mlt.loc[sub].index, df_mlt.loc[sub].values, where='post', label="MLT", color="black", linewidth = .5, zorder=1)
        autolabelMlt(ax[int(i/2), i%2], curvaMlt[0])

        ax[int(i/2), i%2].legend()
        ax[int(i/2), i%2].tick_params(axis='x', labelrotation=-20)
        ax[int(i/2), i%2].set_ylabel('ENA (MW)')
        ax[int(i/2), i%2].grid(color = 'gray', linestyle = '--', linewidth = 0.2)

    pathFileOut = os.path.join(dir_saida, 'ACOMPH.PNG')
    plt.savefig(pathFileOut)
    print(pathFileOut)
    return pathFileOut

def gerarTabelasEmailAcomph(data):

    # corHexSubida = '#24c433'
    # corHexDecida = '#d44425'

    corHexSubida = 'blue'
    corHexDecida = 'red'

    numDiasNaLinha = 7
    submercados = {'SE':'SUDESTE','S':'SUL','NE':'NORDESTE','N':'NORTE'}
    bacias = ['GRANDE','PARANAÍBA','TIETÊ','PARANAPANEMA','ALTO PARANÁ','BAIXO PARANÁ','ALTO TIETÊ','PARAÍBA DO SUL']
    bacias += ['ITABAPOANA','MUCURI','DOCE','PARAGUAI','IGUAÇU','JACUÍ','URUGUAI','CAPIVARI','ITAJAÍ-AÇU']
    bacias += ['SÃO FRANCISCO','PARNAÍBA','PARAGUAÇU','JEQUITINHONHA','TOCANTINS','AMAZONAS','ARAGUARI','XINGU']

    iMesAtual = (data).replace(day=1)
    fMesAnterior = iMesAtual-datetime.timedelta(days=1)
    iMesAnterior = fMesAnterior.replace(day=1)

    df_acomph_ena_sub = wx_dbLib.get_acomph_cached(dataInicial=iMesAnterior, granularidade='submercado')
    df_acomph_ena_sub = pd.DataFrame(df_acomph_ena_sub)
    df_acomph_ena_sub.index = pd.to_datetime(df_acomph_ena_sub.index)

    enaSubmercados = df_acomph_ena_sub.loc[(data-datetime.timedelta(days=numDiasNaLinha)).strftime('%Y-%m-%d'):].copy()

    mediaMesAnteriorSubm = df_acomph_ena_sub.loc[iMesAnterior.strftime('%Y-%m-%d'):fMesAnterior.strftime('%Y-%m-%d')].mean()
    mediaMesAtualSubm = df_acomph_ena_sub.loc[iMesAtual.strftime('%Y-%m-%d'):].mean()

    
    # A data referente do Acomph e d-1. Para pegar o acomph do dia anterior e necessario pegar do d-2
    acomphDiaAnterior = wx_dbLib.getAcomphEspecifico(dtAcomph=data-datetime.timedelta(days=2))
    df_acomphDiaAnterior = pd.DataFrame(acomphDiaAnterior, columns=['DT_REFERENTE', 'CD_POSTO', 'VL_EAR_LIDO', 'VL_EAR_CONSO', 'VL_VAZ_DEF_LIDO', 'VL_VAZ_DEF_CONSO', 'VL_VAZ_AFL_LIDO', 'VL_VAZ_AFL_CONSO', 'VL_VAZ_INC_CONSO', 'VL_VAZ_NAT_CONSO', 'DT_ACOMPH'])
    df_acomphDiaAnterior_vazNat = df_acomphDiaAnterior.pivot(index='CD_POSTO', columns='DT_REFERENTE', values='VL_VAZ_NAT_CONSO')
    df_acomphDiaAnterior_vazNat = wx_calcEna.calcPostosArtificiais_df(df_acomphDiaAnterior_vazNat, ignorar_erros=True)
    
    df_acomph_ena_diaAnterior_sub = wx_calcEna.gera_ena_df(df_acomphDiaAnterior_vazNat)
    df_acomph_ena_diaAnterior_sub = df_acomph_ena_diaAnterior_sub.T
    enaSubmercadosDiaAnterior = df_acomph_ena_diaAnterior_sub.loc[(data-datetime.timedelta(days=numDiasNaLinha)).strftime('%Y-%m-%d'):].copy()

    difEnaSubmercadoDiaAnterior = (enaSubmercados - enaSubmercadosDiaAnterior).dropna()

    enaSubmercadoFormatado = pd.DataFrame()
    diferencaSubmercadoEnaFormatado = pd.DataFrame()
    for siglaSub in submercados:
        enaSubmercadoFormatado = enaSubmercadoFormatado._append(enaSubmercados[siglaSub])
        diferencaSubmercadoEnaFormatado = diferencaSubmercadoEnaFormatado._append(difEnaSubmercadoDiaAnterior[siglaSub])


    # Bacias
    df_acomph_ena_bacia = wx_dbLib.get_acomph_cached(dataInicial=iMesAnterior, granularidade='bacia')
    df_acomph_ena_bacia = pd.DataFrame(df_acomph_ena_bacia)
    df_acomph_ena_bacia.index = pd.to_datetime(df_acomph_ena_bacia.index)
    enaBacias = df_acomph_ena_bacia.loc[(data-datetime.timedelta(days=numDiasNaLinha)).strftime('%Y-%m-%d'):].copy()
    
    df_acomphDiaAnterior_ena = wx_calcEna.gera_ena_df(df_acomphDiaAnterior_vazNat, divisao='bacia')
    df_acomph_ena_bacia = df_acomphDiaAnterior_ena.T
    enaBaciasDiaAnterior = df_acomph_ena_bacia.loc[(data-datetime.timedelta(days=numDiasNaLinha)).strftime('%Y-%m-%d'):].copy()

    mediaMesAnteriorBacias = df_acomph_ena_bacia.loc[iMesAnterior.strftime('%Y-%m-%d'):fMesAnterior.strftime('%Y-%m-%d')].mean()
    mediaMesAtualBacias = df_acomph_ena_bacia.loc[iMesAtual.strftime('%Y-%m-%d'):].mean()

    diferencaEnaBaciasDiaAnterior = (enaBacias - enaBaciasDiaAnterior).dropna(how='all')

    enaBaciasFormatado = pd.DataFrame()
    diferencaBaciasEnaFormatado = pd.DataFrame()
    for bac in bacias:
        if bac == 'JEQUITINHONHA':
            mediaMesAnteriorBacias[bac] = mediaMesAnteriorBacias['JEQUITINHONHA (SE)'] + mediaMesAnteriorBacias['JEQUITINHONHA (NE)']
            mediaMesAtualBacias[bac] = mediaMesAtualBacias['JEQUITINHONHA (SE)'] + mediaMesAtualBacias['JEQUITINHONHA (NE)']
            
            enaBac = enaBacias['JEQUITINHONHA (SE)'] + enaBacias['JEQUITINHONHA (NE)']
            difEnaBac = diferencaEnaBaciasDiaAnterior['JEQUITINHONHA (SE)'] + diferencaEnaBaciasDiaAnterior['JEQUITINHONHA (NE)']

        elif bac == 'SÃO FRANCISCO':
            mediaMesAnteriorBacias[bac] = mediaMesAnteriorBacias['SÃO FRANCISCO (SE)'] + mediaMesAnteriorBacias['SÃO FRANCISCO (NE)']
            mediaMesAtualBacias[bac] = mediaMesAtualBacias['SÃO FRANCISCO (SE)'] + mediaMesAtualBacias['SÃO FRANCISCO (NE)']
            enaBac = enaBacias['SÃO FRANCISCO (SE)'] + enaBacias['SÃO FRANCISCO (NE)']
            difEnaBac = diferencaEnaBaciasDiaAnterior['SÃO FRANCISCO (SE)'] + diferencaEnaBaciasDiaAnterior['SÃO FRANCISCO (NE)']

        elif bac == 'TOCANTINS':
            mediaMesAnteriorBacias[bac] = mediaMesAnteriorBacias['TOCANTINS (N)'] + mediaMesAnteriorBacias['TOCANTINS (SE)']
            mediaMesAtualBacias[bac] = mediaMesAtualBacias['TOCANTINS (N)'] + mediaMesAtualBacias['TOCANTINS (SE)']
            enaBac = enaBacias['TOCANTINS (N)'] + enaBacias['TOCANTINS (SE)']
            difEnaBac = diferencaEnaBaciasDiaAnterior['TOCANTINS (N)'] + diferencaEnaBaciasDiaAnterior['TOCANTINS (SE)']

        elif bac == 'AMAZONAS':
            mediaMesAnteriorBacias[bac] = mediaMesAnteriorBacias['AMAZONAS (N)'] + mediaMesAnteriorBacias['AMAZONAS (SE)']
            mediaMesAtualBacias[bac] = mediaMesAtualBacias['AMAZONAS (N)'] + mediaMesAtualBacias['AMAZONAS (SE)']
            enaBac = enaBacias['AMAZONAS (N)'] + enaBacias['AMAZONAS (SE)']
            difEnaBac = diferencaEnaBaciasDiaAnterior['AMAZONAS (N)'] + diferencaEnaBaciasDiaAnterior['AMAZONAS (SE)']

        else:
            enaBac = enaBacias[bac]
            difEnaBac = diferencaEnaBaciasDiaAnterior[bac]

        enaBac.name = bac
        difEnaBac.name = bac

        enaBaciasFormatado = enaBaciasFormatado._append(enaBac)
        diferencaBaciasEnaFormatado = diferencaBaciasEnaFormatado._append(difEnaBac)

    mlt = wx_dbLib.getMlt()
    df_mlt = pd.DataFrame(mlt, columns=['SUBMERCADO', 'MES', 'MLT'])
    mesesMlt = [iMesAnterior.month, iMesAtual.month]
    df_mlt = (df_mlt[df_mlt['MES'].isin(mesesMlt)]).copy()
    df_mlt['SUBMERCADO'].replace({1:'N', 2:'NE', 3:'SE', 4:'S'}, inplace=True)
    df_mlt = df_mlt.pivot(index='SUBMERCADO', columns='MES', values='MLT')

    mltBaciasPath = os.path.join(pathArquivos, 'ENAS_MLT_bacias.txt')
    mltBacias = pd.read_csv(mltBaciasPath, sep='\t', index_col='BACIA')
    mltBacias.columns = mltBacias.columns.astype(int)

    inicioSemanaEletrica = data
    while inicioSemanaEletrica.weekday() != 5:
        inicioSemanaEletrica = inicioSemanaEletrica-datetime.timedelta(days=1)

    tabelaEna = []
    for index, enaSub in enaSubmercadoFormatado.iterrows():
        enaAux = []
        for i, dt in enumerate(enaSub.index):
            if i == 0:
                enaAux.append('{}<br><small>{:.0f}% | {:.0f}%</small>'.format(submercados[index], 100*(mediaMesAnteriorSubm[index]/df_mlt.loc[index, iMesAnterior.month]), 100*(mediaMesAtualSubm[index]/df_mlt.loc[index, iMesAtual.month])))
                cor = corHexSubida
            elif enaSub.iloc[i-1] < enaSub.iloc[i]:
                cor = corHexSubida
            else:
                cor = corHexDecida

            enaAux.append('<span style="color: {}">{:.0f}<br><small>{:.0f}%</small></span>'.format(cor, enaSub[dt], 100*(enaSub[dt]/df_mlt.loc[index, dt.month])))
        
        enaAux.append('<span style="color: {}">{:.0f}<br><small>{:.0f}%</small></span>'.format("black", enaSub[inicioSemanaEletrica:].mean(), 100*(enaSub[inicioSemanaEletrica:].mean()/df_mlt.loc[index, dt.month])))
        tabelaEna.append(enaAux)

    html = "<h4>Submercado</h4>"
    header = ['Submercado <br><small>mês anter | mês atual</small>']+list(enaSubmercadoFormatado.columns.strftime('%d/%m/%Y')) + ['<span style="font-weight: bold;">Média</span><br><smal>(semana)</smal>']
    tamanhoColunas = [85] + [61 for i in range(len(header))]
    html += wx_emailSender.gerarTabela(body=tabelaEna, header=header, widthColunas=tamanhoColunas)

    
    tabelaEnaBacia = []
    for index, enaBac in enaBaciasFormatado.iterrows():
        enaAux = []
        for i, dt in enumerate(enaBac.index):
            if i == 0:
                enaAux.append('{}<br><small>{:.0f}% | {:.0f}%</small>'.format(index, 100*(mediaMesAnteriorBacias[index]/mltBacias.loc[index, iMesAnterior.month]), 100*(mediaMesAtualBacias[index]/mltBacias.loc[index, iMesAtual.month])))
                cor = corHexSubida
            elif enaBac.iloc[i-1] < enaBac.iloc[i]:
                cor = corHexSubida
            else:
                cor = corHexDecida

            enaAux.append('<span style="color: {}">{:.0f}<br><small>{:.0f}%</small></span>'.format(cor, enaBac[dt], 100*(enaBac[dt]/mltBacias.loc[index, dt.month])))
        
        enaAux.append('<span style="color: {}">{:.0f}<br><small>{:.0f}%</small></span>'.format("black", enaBac[inicioSemanaEletrica:].mean(), 100*(enaBac[inicioSemanaEletrica:].mean()/mltBacias.loc[index, dt.month])))
        tabelaEnaBacia.append(enaAux)

    html += "<h4>Bacias</h4>"
    header = ['Bacia <br><small>mês anter | mês atual</small>']+list(enaBaciasFormatado.columns.strftime('%d/%m/%Y')) + ['<span style="font-weight: bold;">Média</span><br><smal>(semana)</smal>']
    tamanhoColunas = [85] + [61 for i in range(len(header))]
    html += wx_emailSender.gerarTabela(body=tabelaEnaBacia, header=header, widthColunas=tamanhoColunas)


    tabelaDifEnaSub = []
    for index, difEnaSub in diferencaSubmercadoEnaFormatado.iterrows():
        enaAux = []
        for i, dt in enumerate(difEnaSub.index):
            if i == 0:
                enaAux.append('{}'.format(submercados[index]))
            if difEnaSub.iloc[i] >= 0:
                cor = corHexSubida
            else:
                cor = corHexDecida
            
            enaAux.append('<span style="color: {}">{:.0f}<br></span>'.format(cor, difEnaSub[dt]))
        tabelaDifEnaSub.append(enaAux)

    header = ['Submercado']+list(diferencaSubmercadoEnaFormatado.columns.strftime('%d/%m/%Y'))
    html += "<h3>Diferenças ACOMPH do dia {} com o acomph do dia {}</h3>".format(data.strftime('%d/%m/%Y'), (data-datetime.timedelta(days=1)).strftime('%d/%m/%Y'))
    html += "<h4>Submercados</h4>"
    html += wx_emailSender.gerarTabela(body=tabelaDifEnaSub, header=header)

    tabelaDifEnaBac = []
    for index, difEnaBac in diferencaBaciasEnaFormatado.iterrows():
        enaAux = []
        for i, dt in enumerate(difEnaBac.index):
            if i == 0:
                enaAux.append('{}'.format(index))
            if difEnaBac.iloc[i] >= 0:
                cor = corHexSubida
            else:
                cor = corHexDecida
            
            enaAux.append('<span style="color: {}">{:.0f}<br></span>'.format(cor, difEnaBac[dt]))
        tabelaDifEnaBac.append(enaAux)

    header = ['Bacia']+list(diferencaBaciasEnaFormatado.columns.strftime('%d/%m/%Y'))
    html += "<h4>Bacias</h4>"
    html += wx_emailSender.gerarTabela(body=tabelaDifEnaBac, header=header)

    return html

if __name__ == "__main__":
    geraRelatorioBbce(datetime.datetime(2024, 9, 2))
