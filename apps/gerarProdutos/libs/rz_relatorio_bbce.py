import os
import re
import sys
import pdb
import locale 
import datetime
import pandas as pd
import sqlalchemy as db
import mplfinance as fplt

sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.bibliotecas import wx_dbClass

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
    pdb.set_trace()
    
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
        

def gerar_grafico(produtosOrdenados, df_negociacoes, produtosBbce):
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
		nome_imagem = ('{}.png'.format(produtosBbce[prod])).replace('/', '')
		
		path_arquivo_saida = os.path.join(path_saida, nome_imagem)
		
		# plot_using_finplot(dados_plot=dados_plot, titulo=produtosBbce[prod], path_arquivo_saida=path_arquivo_saida)
		plot_using_mplfinance(dados_plot=dados_plot, titulo=produtosBbce[prod], path_arquivo_saida=path_arquivo_saida)
  
		path_plots.append(path_arquivo_saida)
  
	return path_plots


def get_tabela_bbce(data, produtosInteressesBbce):

	locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')

	database = wx_dbClass.db_mysql_master('bbce')

	database.connect()
	tb_produtos = database.getSchema('tb_produtos')
	tb_negociacoes = database.getSchema('tb_negociacoes')
	
	produtosBbce = ''
	for prod in produtosInteressesBbce:
		produtosBbce += '\'{}\', '.format(prod)
	produtosBbce = produtosBbce[:-2]

	produtosBbce = eval(produtosBbce)

	try:
		query_prod = db.select([tb_produtos.c.id_produto,tb_produtos.c.str_produto]).where(tb_produtos.c['str_produto'].in_(produtosBbce))
		answer = database.conn.execute(query_prod).fetchall()
	except:
		print('nao encontrado')


	produtosBbce = {}
	for prod in answer: 
		produtosBbce[prod[0]] = prod[1]

	produtosId = ''
	for prod in produtosBbce: 
		produtosId += '{}, '.format(prod)
	produtosId = produtosId[:-2]
	try:
		query_master = db.select([tb_negociacoes,tb_produtos]).join(tb_negociacoes, tb_negociacoes.c.id_produto == tb_produtos.c.id_produto).where(tb_produtos.c.id_produto.in_(eval(produtosId)))
		answer = database.conn.execute(query_master).fetchall()
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

	
	for prod in produtosBbce.items():
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
		path_graficos = gerar_grafico(produtosOrdenados, df_negociacoes, produtosBbce)

	body = []
	
	for prod in sorted(produtosOrdenados):
		prod = produtosOrdenados[prod]
		df_negociacoesProduto = df_negociacoes[df_negociacoes['id_produto'] == prod]
		row = [produtosBbce[prod]]

		precoMedio_f ='-'
		quantidadeMedia_f = '-'
		precoAbertura_f = '-'
		precoFechamento_f = '-'
		precoMaximo_f = '-'
		precoMinimo_f = '-'
		precoFechamentoAnterior_f = '-'


		if len(df_negociacoesProduto) > 0:

			try:
				ultimoDiaNegociacao = max(df_negociacoesProduto[:(data-datetime.timedelta(days=1)).strftime('%Y-%m-%d')].index)
				negociacoesDiaAnterior = df_negociacoesProduto.sort_index().loc[ultimoDiaNegociacao.strftime('%Y-%m-%d'):ultimoDiaNegociacao.strftime('%Y-%m-%d'),:]
				if negociacoesDiaAnterior.shape[0] > 0:

					ultimaNegociacaoDiaAnterior = negociacoesDiaAnterior.loc[max(negociacoesDiaAnterior.index)]
					
					if type(ultimaNegociacaoDiaAnterior) == type(pd.DataFrame()) and len(ultimaNegociacaoDiaAnterior)>0:
						precoFechamentoAnterior = ultimaNegociacaoDiaAnterior.iloc[-1]['preco']
					
					elif type(ultimaNegociacaoDiaAnterior) == type(pd.Series([],dtype=pd.StringDtype())):
						precoFechamentoAnterior = ultimaNegociacaoDiaAnterior['preco']

					precoFechamentoAnterior_f = ''
					# if (data.date()-ultimoDiaNegociacao.date()).days > 1:
					cor = 'black'
					precoFechamentoAnterior_f = '<small style="color:{}">({})</small><br>'.format(cor, ultimoDiaNegociacao.strftime('%d/%m/%y'))
					precoFechamentoAnterior_f = '{}R$ {:.2f}'.format(precoFechamentoAnterior_f, precoFechamentoAnterior)
			except:
				pass

			negociacoesDia = df_negociacoesProduto.sort_index().loc[data.strftime('%Y-%m-%d'):(data+datetime.timedelta(hours=23, seconds=59)).strftime('%Y-%m-%d'),:]

			if len(negociacoesDia) > 0:

				precoMedio = ((negociacoesDia['preco']*negociacoesDia['quantidade_media'])/negociacoesDia['quantidade_media'].sum()).sum()
				precoMedio_f = 'R$ {:.2f}'.format(precoMedio)

				quantidadeMedia = negociacoesDia['quantidade_media'].sum()
				quantidadeMedia_f = '{:.2f}'.format(quantidadeMedia)

				precoMaximo = negociacoesDia['preco'].max()
				precoMaximo_f = 'R$ {:.2f}'.format(precoMaximo)

				precoMinimo = negociacoesDia['preco'].min()
				precoMinimo_f = 'R$ {:.2f}'.format(precoMinimo)
				
				primeirraNegociacao = negociacoesDia.loc[min(negociacoesDia.index)]
				if type(primeirraNegociacao) == type(pd.DataFrame()):
					precoAbertura = primeirraNegociacao.iloc[0]['preco']
				else:
					precoAbertura = primeirraNegociacao['preco']
				precoAbertura_f = 'R$ {:.2f}'.format(precoAbertura)
				if precoFechamentoAnterior_f != '-':
					percent = ((precoAbertura/precoFechamentoAnterior)-1)*100
					cor = 'blue'
					if percent < 0:
						cor = 'red'
					precoAbertura_f = '<span style="color:{}"> {:.1f}% </span><br>{}'.format(cor, percent, precoAbertura_f)

				ultimaNegociacao = negociacoesDia.loc[max(negociacoesDia.index)]
				if type(ultimaNegociacao) == type(pd.DataFrame()):
					precoFechamento = ultimaNegociacao.iloc[-1]['preco']
				else:
					precoFechamento =ultimaNegociacao['preco']
				precoFechamento_f = 'R$ {:.2f}'.format(precoFechamento)
				if precoFechamentoAnterior_f != '-':
					percent = ((precoFechamento/precoFechamentoAnterior)-1)*100
					cor = 'blue'
					if percent < 0:
						cor = 'red'
					precoFechamento_f = '<span style="color:{}"> {:.1f}% </span><br>{}'.format(cor, percent, precoFechamento_f)

			row.append( precoMedio_f.replace('.', ',') )
			row.append( quantidadeMedia_f.replace('.', ',') )
			row.append( precoFechamentoAnterior_f.replace('.', ',') )
			row.append( precoAbertura_f.replace('.', ',') )
			row.append( precoFechamento_f.replace('.', ',') )
			row.append( precoMaximo_f.replace('.', ',') )
			row.append( precoMinimo_f.replace('.', ',') )

		else:
			row = [produtosBbce[prod], '-', '-', '-', '-', '-', '-', '-']

		body.append(row)
	
	return body, path_graficos

def get_tabela_teste_bbce(data, produtosInteressesBbce):
	pass
if __name__ == '__main__':
    teste = get_tabela_bbce(datetime.datetime.now(), ['SE CON MEN JUL/24 - Preço Fixo', 'SE CON MEN AGO/24 - Preço Fixo', 'SE CON MEN SET/24 - Preço Fixo', 'SE CON MEN OUT/24 - Preço Fixo', 'SE CON MEN NOV/24 - Preço Fixo', 'SE CON MEN DEZ/24 - Preço Fixo', 'SE CON TRI JUL/24 SET/24 - Preço Fixo', 'SE CON TRI OUT/24 DEZ/24 - Preço Fixo', 'SE CON TRI JAN/25 MAR/25 - Preço Fixo', 'SE CON TRI ABR/25 JUN/25 - Preço Fixo', 'SE CON SEM JAN/25 JUN/25 - Preço Fixo', 'SE CON SEM JUL/25 DEZ/25 - Preço Fixo', 'SE CON SEM JAN/26 JUN/26 - Preço Fixo', 'SE CON SEM JUL/26 DEZ/26 - Preço Fixo', 'SE CON ANU JAN/25 DEZ/25 - Preço Fixo', 'SE CON ANU JAN/26 DEZ/26 - Preço Fixo', 'SE CON ANU JAN/27 DEZ/27 - Preço Fixo', 'SE CON ANU JAN/28 DEZ/28 - Preço Fixo', 'SE CON ANU JAN/29 DEZ/29 - Preço Fixo', 'SE CON ANU JAN/30 DEZ/30 - Preço Fixo'])
    pass