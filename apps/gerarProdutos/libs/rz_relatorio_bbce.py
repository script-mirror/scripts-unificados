import os
import re
import sys
import pdb
import locale 
import datetime
import pandas as pd
import requests as r
import sqlalchemy as db
import mplfinance as fplt
from dotenv import load_dotenv
from middle.utils import Constants
load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")),'.env'))
constants = Constants()

__HOST_SERVER = os.getenv('HOST_SERVIDOR') 
sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.bibliotecas import wx_dbClass

locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')

URL_COGNITO = os.getenv('URL_COGNITO')
CONFIG_COGNITO = os.getenv('CONFIG_COGNITO')
def get_access_token() -> str:
    response = r.post(
        URL_COGNITO,
        data=CONFIG_COGNITO,
        headers={'Content-Type': 'application/x-www-form-urlencoded'}
    )
    return response.json()['access_token']


def save():
    fplt.screenshot(open('screenshot.png', 'wb'))

def plot_using_finplot(dados_plot, titulo, path_arquivo_saida):

    ax,ax2 = fplt.create_plot(titulo, rows=2)
    candles = dados_plot[['Open', 'Close', 'High', 'Low']]
    fplt.candlestick_ochl(candles, ax=ax)
    ax.setLabel('left', 'Preço [R$]')
    ax.setTitle(titulo)
    ax.showGrid(x=True, y=True, alpha=.4)
    # fplt.plot(dados_plot['close'].dropna().rolling(2).mean(), ax=ax, legend='ma-25')
    
    volumes = dados_plot[['Open','Close','Volume']]
    fplt.volume_ocv(volumes, ax=ax2)
    ax2.setLabel('left', 'Volume [MWm]')
    ax2.showGrid(x=True, y=True, alpha=.2)
    
    # https://pyqtgraph.readthedocs.io/en/latest/graphicsItems/plotitem.html
       
    fplt.show(True)
    
    # time.sleep(2)
    
    # shutil.move('screenshot.png', path_arquivo_saida)
    print(path_arquivo_saida)

def plot_using_mplfinance(dados_plot, titulo, path_arquivo_saida):
        
    m = fplt.make_marketcolors(base_mpf_style='charles',vcdopcod=True)
    s = fplt.make_mpf_style(base_mpf_style='charles',marketcolors=m)
    
    fplt.plot(dados_plot, 
            #   style='charles',
              show_nontrading=False,
              type='candle',
              title=titulo,
              ylabel='R$', 
              ylabel_lower='MWm',
              volume=True,
              figratio=(12,6),
            #   mav=(2,4,6),
              style=s,
              savefig=dict(fname=path_arquivo_saida,dpi=200))
    
    print(path_arquivo_saida)
        
def gerar_grafico(produtosOrdenados, df_negociacoes, produtos_bbce):
	numero_dias_historico = 31*4
	hoje = datetime.datetime.now()
	dt_inicio_grafico = hoje - datetime.timedelta(days=numero_dias_historico)

	path_plots = []
	for prod in sorted(produtosOrdenados):
		prod = produtosOrdenados[prod]
		df_negociacoesProduto = df_negociacoes[df_negociacoes['id_produto'] == prod]

		# df_negociacoesProduto_sem_outliers = df_negociacoesProduto[(np.abs(stats.zscore(df_negociacoesProduto['preco'])) < 3)]
		
		ohlc = df_negociacoesProduto['preco'].resample('1D', convention='start').ohlc(_method='ohlc')
		volume = df_negociacoesProduto['quantidade_media'].resample('1D', convention='start').sum()
		
		dados_plot = pd.concat([ohlc, volume], axis=1)
		
		dados_plot = dados_plot.loc[dados_plot.index >= dt_inicio_grafico]
		if dados_plot.shape[0] == 0:
			continue
  
		dados_plot.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
		
		# https://coderzcolumn.com/tutorials/data-science/candlestick-chart-in-python-mplfinance-plotly-bokeh
		
		path_saida = os.path.abspath(r'/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/gerarProdutos/arquivos/tmp')
		nome_imagem = ('{}.png'.format(produtos_bbce[prod])).replace('/', '')
		
		path_arquivo_saida = os.path.join(path_saida, nome_imagem)
		
		# plot_using_finplot(dados_plot=dados_plot, titulo=produtos_bbce[prod], path_arquivo_saida=path_arquivo_saida)
		plot_using_mplfinance(dados_plot=dados_plot, titulo=produtos_bbce[prod], path_arquivo_saida=path_arquivo_saida)
  
		path_plots.append(path_arquivo_saida)
  
	return path_plots
def get_tabela_bbce(data:datetime.datetime):
	database = wx_dbClass.db_mysql_master('bbce')

	tb_produtos = database.getSchema('tb_produtos')
	tb_negociacoes = database.getSchema('tb_negociacoes')
	produtos_bbce = tuple(pd.DataFrame(	r.get(f"{constants.BASE_URL}/api/v2/bbce/produtos-interesse",
                                           headers={
                'Authorization': f'Bearer {get_access_token()}'
            }).json())["str_produto"].to_list())
	
	query_prod = db.select(
		tb_produtos.c["id_produto"],
  		tb_produtos.c["str_produto"]
	).where(
      	tb_produtos.c['str_produto'].in_(produtos_bbce)
    )
	answer = database.db_execute(query_prod).fetchall()

 
	produtos_bbce = {}
	for prod in answer: 
		produtos_bbce[prod[0]] = prod[1]

	produtos_id = ''
	for prod in produtos_bbce: 
		produtos_id += '{}, '.format(prod)
	produtos_id = produtos_id[:-2]
 
	try:
		query_master = db.select(
			tb_negociacoes.c["id_negociacao"],
			tb_negociacoes.c["id_produto"],
			tb_negociacoes.c["vl_quantidade"],
			tb_negociacoes.c["vl_preco"],
			tb_negociacoes.c["dt_criacao"],
    		tb_produtos.c["id_produto"],
    		tb_produtos.c["str_produto"],
    		tb_produtos.c["dt_cria"],
      
		).join(
      	tb_negociacoes, tb_negociacoes.c["id_produto"] == tb_produtos.c["id_produto"]
       	).where(
            tb_produtos.c["id_produto"].in_(eval(produtos_id))
            )
		answer = database.db_execute(query_master)
	except:
		print('nao encontrado')

	list_complete = ()
	for date in answer:
		try:
			prod_str = date[6].replace("–","-")
			date = tuple(date)
			pattern = r"\b[a-zA-Z]{3}/\d{2}\b"
			dates = re.findall(pattern, prod_str)
			data1 = dates[0]

			if (len(dates) > 1):
				data2 = dates[-1]
				days = abs(pd.to_datetime(data1, format = "%b/%y") - pd.to_datetime((pd.to_datetime(data2, format = "%b/%y")+datetime.timedelta(weeks=5)).strftime("%b/%y"), format="%b/%y"))
			else:
				days = abs(pd.to_datetime(data1, format = "%b/%y") - pd.to_datetime((pd.to_datetime(data1, format = "%b/%y")+datetime.timedelta(weeks=5)).strftime("%b/%y"), format="%b/%y"))
			date += (days,) + (prod_str.strip(),)
			list_complete += date,
		except: 
			print(f"Erro no produto: {prod_str}")
			quit()
	df = pd.DataFrame(list_complete, columns=['id','id_produto','quantidade_media','preco','data_alteracao','id_prod','produto','dt_cria','days','unidade_valor'])
	df['days'] = df['days'].dt.days.astype('int')*24
	df['quantidade'] = df['days']  * df['quantidade_media']
	
	df_negociacoes = df[['id', 'quantidade', 'preco', 'data_alteracao', 'quantidade_media', 'id_produto', 'unidade_valor']]

	df_negociacoes.set_index('data_alteracao', inplace=True)
	
	ordemPeriodo = {'MEN':1, 'TRI':2, 'SEM':3, 'ANU':4}
	ordemSubmercados = {'SE':1, 'SU':2, 'NE':3, 'NO':4}
	produtosOrdenados = {}
	
	for prod in produtos_bbce.items():
		try:
			regex = "^(.{1,2}) (.{1,3}) (.{3}) (.{1,13}) - (.{1,})(.*)"
			infosProduto = re.split(regex, prod[1])
			periodoFornecimento = infosProduto[3].strip()
		except:
			regex = "^(.{1,2}) (.{1,3}) (.{3}) (.{1,13}) – (.{1,})(.*)"
			infosProduto = re.split(regex, prod[1])
			periodoFornecimento = infosProduto[3].strip()
		
		submercado = infosProduto[1].strip()
		tipo = infosProduto[2].strip()
		inicio = infosProduto[4].strip().split()[0]
		modalidade = infosProduto[5].strip()

		key = ''
		if periodoFornecimento in ordemPeriodo:
			key = '{}'.format(ordemPeriodo[periodoFornecimento])
		else:
			key = '{}'.format(len(ordemPeriodo)+1)

		if submercado in ordemSubmercados:
			key += '{}'.format(ordemSubmercados[submercado])
		else:
			key += '{}'.format(len(ordemSubmercados)+1)

		mesInicial = datetime.datetime.strptime(inicio.split('/')[0], '%b').month
		anoInicial = inicio.split('/')[1]

		key += '{}{:0>2}{}{}'.format(anoInicial,mesInicial,tipo,modalidade)

	
		produtosOrdenados[key] = prod[0]
  
	path_graficos = []
	if data.weekday() == 4:
		path_graficos = gerar_grafico(produtosOrdenados, df_negociacoes, produtos_bbce)
	html = r.get(f"{constants.BASE_URL}/api/v2/bbce/produtos-interesse/html?data={data.strftime('%Y-%m-%d')}",
              headers={
                'Authorization': f'Bearer {get_access_token()}'
            }).json()["html"]
	return html, path_graficos

if __name__ == '__main__':
    teste = get_tabela_bbce(datetime.datetime.now())
    pass