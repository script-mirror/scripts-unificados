import os
import re
import pdb
import sys
import glob
import zipfile
import requests
import datetime
import pandas as pd
import sqlalchemy as db
from bs4 import BeautifulSoup

sys.path.insert(1,"/WX2TB/Documentos/fontes/PMO/scripts_unificados/")
from bibliotecas import wx_dbClass , wx_opweek , rz_dir_tools

path_libs = os.path.dirname(os.path.abspath(__file__))
path_db_updater = os.path.dirname(path_libs)

DIR_TOOLS = rz_dir_tools.DirTools()
PATH_DB_UPDATER_TMP = os.path.join(path_db_updater,'tmp', datetime.datetime.now().strftime("%d-%m-%Y %H%M%S"))

def obter_datas_por_ano(ano):
    datas_por_ano = {}
    nome_meses = [
        'janeiro', 'fevereiro', 'marco', 'abril',
        'maio', 'junho', 'julho', 'agosto',
        'setembro', 'outubro', 'novembro', 'dezembro'
    ]

    # Mapeando os dias da semana para o formato especificado (1 = domingo, 2 = segunda, ...)
    mapeamento_dias_semana = {0: 2, 1: 3, 2: 4, 3: 5, 4: 6, 5: 7, 6: 1}

    # Itera pelos meses de janeiro a dezembro
    for mes in range(1, 13):
        primeiro_dia = datetime.datetime(ano, mes, 1)
        proximo_mes = primeiro_dia.replace(day=28) + datetime.timedelta(days=4)  # Aproximação para o último dia do mês
        ultimo_dia = (proximo_mes - datetime.timedelta(days=proximo_mes.day)).date()

        datas_mes = []

        # Itera pelos dias do mês
        while primeiro_dia.date() <= ultimo_dia:
            dia_da_semana = mapeamento_dias_semana[primeiro_dia.weekday()]  # Mapeamento para o formato desejado
            data_formatada = primeiro_dia.strftime("%Y-%m-%d")
            datas_mes.append((dia_da_semana, data_formatada))
            primeiro_dia += datetime.timedelta(days=1)

        nome_mes = nome_meses[mes - 1]
        datas_por_ano[nome_mes] = datas_mes

    return datas_por_ano


def atualizar_patamar_ano_atual(pathZip, dataReferente):

    db_decks = wx_dbClass.db_mysql_master("db_decks")
    db_decks.connect()
    tb_ds_bloco_tm = db_decks.getSchema('tb_ds_bloco_tm')

    dia = dataReferente.day
    mes = dataReferente.month
    ano = dataReferente.year
    arquivoDeckSE_zip = f'Deck_SECO_{ano}-01-01.zip'
    nome_arquivo_csv = f'SECO_{ano}-01-01_PATAMARES.csv'
    datas_ano = obter_datas_por_ano(ano)

    mesesNomes = [datas_ano['janeiro'], datas_ano['fevereiro'], datas_ano['marco'], datas_ano['abril'],
                  datas_ano['maio'], datas_ano['junho'], datas_ano['julho'], datas_ano['agosto'],
                  datas_ano['setembro'], datas_ano['outubro'], datas_ano['novembro'], datas_ano['dezembro']]
    mesesNumeros = list(range(1, 13))

    cod_patamar = {'Leve': 1, 'Media': 2, 'Pesada': 3}

    with zipfile.ZipFile(pathZip, 'r') as zip_file_externo:
        with zip_file_externo.open(arquivoDeckSE_zip) as zip_file_interno:
            with zipfile.ZipFile(zip_file_interno, 'r') as zip_file_interno_real:
                with zip_file_interno_real.open(nome_arquivo_csv) as csv_file:
                    dataframe = pd.read_csv(csv_file, delimiter=';')
                    dataframe = dataframe.drop(columns=["HoraFim"])
                    listaDataframe = []
                    ano_atual = ano

                    url = f"https://www.anbima.com.br/feriados/fer_nacionais/{ano_atual}.asp"

                    response = requests.get(url)
                    lista_feriados = []

                    if response.status_code == 200:
                        soup = BeautifulSoup(response.text, 'html.parser')
                        elementos_td = soup.find_all('td', class_='tabela')

                        for td in elementos_td:
                            valor = td.get_text(strip=True)
                            # Verifica se o valor é uma data no formato '1/1/23' usando expressão regular
                            if re.match(r'\d{1,2}/\d{1,2}/\d{2}', valor):
                                lista_feriados.append(valor)
                    else:
                        print("Falha ao acessar a página. Código de status:", response.status_code)

                    lista_feriados_formatada = []
                    for i in range(0, len(lista_feriados)):
                        data = lista_feriados[i]
                        data_obj = datetime.datetime.strptime(data, '%d/%m/%y')
                        data_formatada = data_obj.strftime('%Y-%m-%d')
                        lista_feriados_formatada.append((1, data_formatada))
                        
                    # capturando ultimo dia de fevereiro para conseguir saber o dia da quarta feira de cinzas.
                    ultimo_dia_fevereiro = None
                    for feriado in lista_feriados_formatada:
                        if feriado[1].startswith(f'{ano}-02'):
                            ultimo_dia_fevereiro = feriado
                            
                    if ultimo_dia_fevereiro:
                        data_formatada = datetime.datetime.strptime(ultimo_dia_fevereiro[1], '%Y-%m-%d')
                        # Adicione um dia à data para achar quarta feira de cinzas
                        nova_data = data_formatada + datetime.timedelta(days=1)
                        nova_data_formatada = nova_data.strftime('%Y-%m-%d')
                        lista_feriados_formatada.append((1, nova_data_formatada))
                    
                    for mes in mesesNomes:
                        for i in range(len(mes)):
                            for feriado in lista_feriados_formatada:
                                if mes[i][1] == feriado[1]:
                                    mes[i] = (feriado[0], feriado[1])

                    for indice in range(len(mesesNumeros)):
                        dataframeMes = dataframe[dataframe['Mes'] == mesesNumeros[indice]]
                        dataframeMes = pd.concat([dataframeMes] * 4, ignore_index=True)
                        df = pd.DataFrame(mesesNomes[indice], columns=['DiaSemana', 'Data'])
                        df_merged_ = pd.merge(dataframe, df, on='DiaSemana', how='inner')
                        df_ordenado = df_merged_.sort_values(by=['Data', 'HoraIni'], ascending=[True, True])
                        df_finalOrdenado = df_ordenado.drop_duplicates()
                        mascara = df_finalOrdenado['Patamar'].shift() == df_finalOrdenado['Patamar']
                        df_finalResultado = df_finalOrdenado[~mascara]
                        listaDataframe.append(df_finalResultado)

                    dataframe_final = pd.concat(listaDataframe, ignore_index=True)
                    dataframe_final['dt_inicio_patamar'] = dataframe_final['Data'] + ' ' + dataframe_final['HoraIni']
                    dataframe_final['dt_inicio_patamar'] = pd.to_datetime.datetime(dataframe_final['dt_inicio_patamar'])
                    dataframe_final = dataframe_final.sort_values(by='dt_inicio_patamar', ascending=True)
                    dataframe_final = dataframe_final.drop(columns=['Intervalo', 'DiaSemana', 'Mes', 'HoraIni', 'Data'])
                    dataframe_final = dataframe_final[['dt_inicio_patamar', 'Patamar']]
                    dataframe_final['vl_patamar'] = dataframe_final['Patamar'].map(cod_patamar)
                    
                    mascara = dataframe_final['vl_patamar'].shift() == dataframe_final['vl_patamar']
                    novo_df_patamar = dataframe_final[~mascara]
                    novo_df_patamar = novo_df_patamar.drop(columns=["Patamar"])
                    
                    listaFinal = novo_df_patamar.values.tolist()
                    for dt_inicio_patamar, vl_patamar in listaFinal:
                      insere_query = tb_ds_bloco_tm.insert().values(
                          dt_inicio_patamar=dt_inicio_patamar,
                          vl_patamar=vl_patamar
                      )
                      db_decks.conn.execute(insere_query)
                    print(f"Banco de dados atualizado com sucesso, para o ano de {ano_atual}")




# def importar_rev_consistido(pathZip):
#     """ Importa para o banco os valores de previsao de REV
#     :param path: Path da planilha
#     :return None: 
#     """
#     try:
#         db_ons = wx_dbClass.db_mysql_master('db_ons')

#         db_ons.connect()
        
#         tb_ve_bacias = db_ons.getSchema('tb_ve_bacias')
#         tb_ve = db_ons.getSchema('tb_ve')

#         tb_submercado = db_ons.getSchema('tb_submercado')
#         tb_bacias_segmentadas = db_ons.getSchema('tb_bacias_segmentadas')
#         real_path = DIR_TOOLS.get_name_insentive_name(pathZip)
        
#         DIR_TOOLS.extract(zipFile=real_path,path= PATH_DB_UPDATER_TMP)
#         file_name = os.path.join(PATH_DB_UPDATER_TMP, 'Consistido','Relatório*-preliminar.xls')

#         file_name_match = glob.glob(file_name, recursive=True)
#         if not file_name_match:
#             raise f"Arquivo não encontrado com padrao {file_name}"

#         print('Inserindo valores da tabela:\n{}'.format(file_name_match[0]))
#         df_excel = pd.ExcelFile(file_name_match[0])
#         abas = df_excel.sheet_names

#         submercados = ['SUDESTE', 'SUL', 'NORDESTE', 'NORTE']

#         if abas[0] == 'Capa':
#             aba_data = 'Capa'
#             aba_NE = 'Tab-12'
#             aba_S_NE_N = 'Tab-13-14-15'
#             linha_data = 4
#         else:
#             aba_data = 'REV-1'
#             aba_NE = 'REV-5'
#             aba_S_NE_N = 'REV-6'
#             linha_data = 5

#         df_dt = df_excel.parse(aba_data, header=None)
#         dt = df_dt[~df_dt[0].isna()].values.tolist()[-2][0]
#         while dt.weekday() != 5:
#             dt = dt + datetime.timedelta(days=1)
        
#         dadosRev = wx_opweek.ElecData(dt.date())

#         print('{} (REV{})'.format(dadosRev.data.strftime('%m/%Y'), dadosRev.atualRevisao))

#         df_SE = df_excel.parse(aba_NE, header=None)
#         df_S_NE_N = df_excel.parse(aba_S_NE_N, header=None)

#         df_SE[0] = df_SE[0].str.upper()
#         df_S_NE_N[0] = df_S_NE_N[0].str.upper()

#         bacias_NE = df_SE[0].unique().tolist()
#         bacias_S_NE_N = df_S_NE_N[0].unique().tolist()

#         baciasPercent = [bacia for bacia in bacias_NE if isinstance(bacia, str) and bacia[-1] == '%']
#         baciasPercent += [bacia for bacia in bacias_S_NE_N if isinstance(bacia, str) and bacia[-1] == '%']

#         for sub in submercados:
#             baciasPercent.remove('{} %'.format(sub))

#         bacias = [bacia[:-2] for bacia in baciasPercent]

#         df_enaSubmercado = df_SE[df_SE[0].isin(submercados)].copy()
#         df_enaSubmercado = pd.concat([df_enaSubmercado,df_S_NE_N[df_S_NE_N[0].isin(submercados)].copy()])
        
#         df_enaBacia = df_SE[df_SE[0].isin(bacias)].copy()
#         df_enaBacia = pd.concat([df_enaBacia,df_S_NE_N[df_S_NE_N[0].isin(bacias)].copy()])

#         df_enaBaciaPercent = df_SE[df_SE[0].isin(baciasPercent)].copy()
#         df_enaBaciaPercent = pd.concat([df_enaBaciaPercent,df_S_NE_N[df_S_NE_N[0].isin(baciasPercent)].copy()])

#         df_enaBaciaPercent[0] = df_enaBaciaPercent[0].str[:-2]

#         dataFormat = "%Y-%m-%d"

#         query_submercado = tb_submercado.select()
#         answer_tb_submercado = db_ons.conn.execute(query_submercado).fetchall()

#         codSubmercados = {}
#         for row in answer_tb_submercado:
#             codSubmercados[row[1]] = row[0]

#         df_enaSubmercado[0] = df_enaSubmercado[0].replace(codSubmercados)

#         select_tb_bacias_segmentadas = tb_bacias_segmentadas.select()
#         answer_tb_bacias_segmentadas = db_ons.conn.execute(select_tb_bacias_segmentadas).fetchall()

#         bacias = {}
#         for row in answer_tb_bacias_segmentadas:
#             bacias[row[1]] = row[0]
#         bacias['PARANAPANEMA (SE)'] = bacias['PARANAPANEMA']

#         df_enaBacia[0] = df_enaBacia[0].replace(bacias)
#         df_enaBaciaPercent[0] = df_enaBaciaPercent[0].replace(bacias)

#         numSemanas = 6
#         colunaInicial = 3


#         #TB_VE
#         sql_delete_tb_ve = tb_ve.delete()\
#                     .where(db.and_(tb_ve.c.vl_ano == dadosRev.anoReferente,tb_ve.c.vl_mes == dadosRev.mesReferente,tb_ve.c.cd_revisao == dadosRev.atualRevisao))
#         execute_delete_tb_ve = db_ons.conn.execute(sql_delete_tb_ve)
#         num_linhas_deletadas = execute_delete_tb_ve.rowcount
#         print(f"{num_linhas_deletadas} Linhas deletadas na tabela tb_ve")

#         valores = []
#         for index, row in df_enaSubmercado.iterrows():
#             for sem in range(0, numSemanas):
#                 dt = dadosRev.primeiroDiaMes + datetime.timedelta(days=sem*7)
#                 valores.append((dadosRev.anoReferente, dadosRev.mesReferente, dadosRev.atualRevisao, row[0], dt.strftime(dataFormat), round(row[sem+colunaInicial])))

#         sql_insert_tb_ve = tb_ve.insert(valores)
#         execute_insert_tb_ve = db_ons.conn.execute(sql_insert_tb_ve)
#         num_linhas_inseridas = execute_insert_tb_ve.rowcount
#         print(f"{num_linhas_inseridas} Linhas inseridas na tabela tb_ve")
        
#         #TB_VE_BACIAS
#         sql_delete_tb_ve_bacias = tb_ve_bacias.delete()\
#                     .where(db.and_(tb_ve_bacias.c.vl_ano == dadosRev.anoReferente,tb_ve_bacias.c.vl_mes == dadosRev.mesReferente,tb_ve_bacias.c.cd_revisao == dadosRev.atualRevisao))
#         execute_delete_tb_ve_bacias = db_ons.conn.execute(sql_delete_tb_ve_bacias)
#         num_linhas_deletadas = execute_delete_tb_ve_bacias.rowcount
#         print(f"{num_linhas_deletadas} Linhas deletadas na tabela tb_ve_bacias")

#         valores = []
#         for index, row in df_enaBacia.iterrows():
#             for sem in range(0, numSemanas):
#                 dt = dadosRev.primeiroDiaMes + datetime.timedelta(days=sem*7)
#                 valorNumerico = round(row[sem+colunaInicial])
#                 valorPercent = round(df_enaBaciaPercent[df_enaBaciaPercent[0] == row[0]][sem+colunaInicial].item(),2)
#                 valores.append((dadosRev.anoReferente, dadosRev.mesReferente, dadosRev.atualRevisao, row[0], dt.strftime(dataFormat), valorNumerico, valorPercent))
        
#         sql_insert_tb_ve_bacias = tb_ve_bacias.insert(valores)
#         execute_insert_tb_ve_bacias = db_ons.conn.execute(sql_insert_tb_ve_bacias)
#         num_linhas_inseridas = execute_insert_tb_ve_bacias.rowcount
#         print(f"{num_linhas_inseridas} Linhas inseridas na tabela tb_ve_bacias")
#     except Exception as e:
#         print(e)

#     df_excel.close()
#     DIR_TOOLS.remove_src(PATH_DB_UPDATER_TMP)


# def importar_prev_ena_consistido(path, dtPrevisao):
#     """ Importa a previsao de vazao do DESSEM" disponibilizado pelo ONS para o banco de dados
#     :param path: Caminho do arquivo
#     :return answer: Numero de linhas inseridas no banco de dados
#     """

#     numeroLinhasCabecalho = 4

#     infoPlanilha = {}
#     infoPlanilha['SUDESTE'] = {'sheet_name':'Diária_6', 'posicaoInfo':[168, 169]}
#     infoPlanilha['SUL'] = {'sheet_name':'Diária_7', 'posicaoInfo':[56, 57]}
#     infoPlanilha['NORDESTE'] = {'sheet_name':'Diária_7', 'posicaoInfo':[70,71]}
#     infoPlanilha['NORTE'] = {'sheet_name':'Diária_7', 'posicaoInfo':[125,126]}

#     codSubmercado = {'SUDESTE':1, 'SUL':2, 'NORDESTE':3, 'NORTE':4}

#     sheet_name = ''

#     vaues_to_insert = []
#     for sub in infoPlanilha:
#         if sheet_name != infoPlanilha[sub]['sheet_name']:

#             df = pd.read_excel(path, sheet_name=infoPlanilha[sub]['sheet_name'], skiprows=numeroLinhasCabecalho)
#             colunas = df.columns.tolist()
#             index_submercado = df[df[colunas[4]] ==sub].index[0]

#             df_values = df.loc[index_submercado+1:index_submercado+2][colunas[2:]]

#             df_values.columns = pd.to_datetime(df_values.columns).strftime("%Y-%m-%d")
#             df_values = df_values.T
#             df_values['submercado']  = codSubmercado[sub]
#             df_values['dtPrevisao'] =  dtPrevisao.strftime("%Y-%m-%d")
#             vaues_to_insert += pd.concat([df_values.reset_index().iloc[:,-2:],df_values.reset_index().iloc[:,:-2]],axis=1).values.tolist()
#             pdb.set_trace()

#     db_ons = wx_dbClass.db_mysql_master('db_ons')
#     db_ons.connect()
#     tb_prev_ena_submercado = db_ons.getSchema('tb_prev_ena_submercado')

#     query_delete = tb_prev_ena_submercado.delete().where(tb_prev_ena_submercado.c.dt_previsao == dtPrevisao)
#     n_values = db_ons.conn.execute(query_delete).rowcount
#     print(f"{n_values} Linhas deletada na tb_prev_ena_submercado!")
    
#     query_insert = tb_prev_ena_submercado.insert().values(vaues_to_insert)
#     n_values = db_ons.conn.execute(query_insert).rowcount
#     print(f"{n_values} Linhas Inseridas na tb_prev_ena_submercado!")


if __name__ == '__main__':

    pass