import os
import sys
import warnings
import datetime
import pandas as pd
import sqlalchemy as db
import logging
import numpy as np

logging.basicConfig(
    level=logging.INFO,  
    format='%(asctime)s - %(levelname)s - %(message)s' 
)

logger = logging.getLogger()
warnings.filterwarnings("ignore")

sys.path.insert(1, "/WX2TB/Documentos/fontes/PMO/scripts_unificados/")
from bibliotecas import wx_dbClass

def read_pdo_sist( path: str, deck_date) -> pd.DataFrame:
          
    logger.info("Reading load data from pdo_sist.dat")
    pdo_file = read_file(path, 'pdo_sist.dat')
    data, load = [], False
    for line in pdo_file:
        parts = line.split(';') 
            
        if parts[0].strip().lower() == 'iper':
            load = True
            columns = line.split(';')[0:3] + last_line.split(';')[3:-1]
            columns = [col.strip().lower() for col in columns]
            continue

        if load and '-' not in parts[0]:
            minuto = (int(parts[0].strip()) - 1) * 30
            date = deck_date + datetime.timedelta(minutes=minuto)
            if int(parts[0].strip()) < 49:
                parts[0] = date.strftime('%Y-%m-%d %H:%M:%S')
                data.append(dict(zip(columns, [valor.strip() for valor in parts])))
        last_line = line
    df = pd.DataFrame(data)
    df = df[df['sist'] != 'FC']
    df['sist'] = df['sist'].replace({'SE': 1, 'S': 2, 'NE': 3, 'N': 4})
    df['iper'] = pd.to_datetime(df['iper'])
    colums_drop = ['perdas', 'gpqusi', 'gfixbar', 'import.', 'export.', 'cortcarg.', 'saldo', 'recebimento','earm', 'pat']
    df = df.drop(colums_drop, axis=1)
    df = df.set_index('iper')
    df = df.replace('--', np.nan)
    cols_to_convert = [col for col in df.columns if col != 'sist']
    df[cols_to_convert] = df[cols_to_convert].astype(float)
    df = df.groupby([df.index.date, df.index.hour, 'sist']).mean().reset_index()
    df['dataHora'] = pd.to_datetime(df['level_0'].astype(str) + ' ' + df['iper'].astype(str).str.zfill(2) + ':00')
    df = df.drop(['level_0','iper'], axis=1)
    df['sist'] = df['sist'].replace({ 1:'SE', 2:'S',3:'NE', 4:'N'})
    df['ande'] = (df['demanda']- df['grenova']- df['somatgh'] - df['somatgt'] + df['conseleva'])
    df['somatgh'] = ( df['somatgh'] - df['ande']) 
    df = df.drop(['ande'], axis=1)
    df['intercambio'] = ( df['demanda'] - df['grenova'] - df['somatgh'] - df['somatgt'] + df['conseleva']) 
    return df

def read_file( directory: str, file_find: str):
    logger.info(f"Searching for file with prefix '{file_find}' in: {directory}")
    for file in os.listdir(directory):
        if file.lower().startswith(file_find.lower()):
            file_path = os.path.join(directory, file)
            logger.info(f"File found: {file}")
            with open(file_path, 'r', encoding='latin-1', errors='ignore') as f:
                lines = f.readlines()
            logger.debug(f"File read: {len(lines)} lines")
            return lines


def calculo_pld(lista_input, PLD_min, PLDmax_h, PLDmax_estr):
    
	# ajusta PLDs para piso e teto
	for i in range(len(lista_input)):
		if float(lista_input[i]) > PLDmax_h:
			lista_input[i] = PLDmax_h
		if float(lista_input[i]) < PLD_min:
			lista_input[i] = PLD_min
    
	# método iterativo para adequação a PLDmax estrutural
	PLD_md0 = sum(lista_input) /len(lista_input)
	PLD_md = PLD_md0
    
	f_est0 = PLDmax_estr / PLD_md0
	f_est = f_est0
	contCalc = 0
	dif = (PLD_md - PLDmax_estr)

	while dif > 0.01:	
		lista_aux = [round(element * f_est, 2) for element in lista_input]
		lista_input = lista_aux
		PLD_md = sum(lista_input) / len(lista_input)
		f_est = PLDmax_estr / PLD_md
		dif = (PLD_md - PLDmax_estr)
		contCalc = contCalc + 1
		if contCalc > 20:
			break
	lista_output = lista_input
    
	return lista_output

def calculaPLD(df_sist, data):
    db_ons = wx_dbClass.db_mysql_master('db_ons')
    db_ons.connect()
    tb_pld = db_ons.getSchema('tb_pld')
    ano = data.year
    query_get_ano_pld = db.select(tb_pld).where(db.and_(tb_pld.c.str_ano == ano))

    colunas = db_ons.conn.execute(query_get_ano_pld)

    for valor in colunas:
        PLDmax_hora = valor.vl_PLDmax_hora
        PLDmax_estr = valor.vl_PLDmax_estr
        PLDmin = valor.vl_PLDmin
  
    """PLDmax_hora = 1542.23 
    PLDmax_estr = 751.73
    PLDmin = 58.6"""


    listPldSE = calculo_pld(df_sist[df_sist['sist'] == 'SE']['cmo'].tolist(), PLDmin, PLDmax_hora, PLDmax_estr) 
    listPldS  = calculo_pld(df_sist[df_sist['sist'] == 'S']['cmo'].tolist(), PLDmin, PLDmax_hora, PLDmax_estr)
    listPldNE = calculo_pld(df_sist[df_sist['sist'] == 'NE']['cmo'].tolist(), PLDmin, PLDmax_hora, PLDmax_estr)
    listPldN  = calculo_pld(df_sist[df_sist['sist'] == 'N']['cmo'].tolist(), PLDmin, PLDmax_hora, PLDmax_estr)

    listPld = []
    for i in range(len(listPldSE)):
        listPld.append(listPldSE[i])
        listPld.append(listPldS[i])
        listPld.append(listPldNE[i])
        listPld.append(listPldN[i])

    df_sist['pld'] = listPld
    return df_sist

def readPdoSist(path, data, pathOut):
    logger.info(f"=== INÍCIO DO PROCESSAMENTO ===")
    logger.info(f"Data de referência: {data}")
    logger.info(f"Pasta de entrada: {path}")

    # Leitura do arquivo
    df_sist = read_pdo_sist(path, datetime.datetime.combine(data, datetime.datetime.min.time()))

    logger.info(f"DataFrame bruto carregado: {df_sist.shape}")

    numeric_cols = ['demanda', 'grenova', 'somatgh', 'conseleva', 'somatgt',
                    'somagtmin', 'somatgtmax', 'earm', 'intercambio']
    for col in numeric_cols:
        if col in df_sist.columns:
            df_sist[col] = pd.to_numeric(df_sist[col], errors='coerce').fillna(0).astype(int)


    # Cálculos
    df_sist = calculaPLD(df_sist, data)
    df_sist['dataHora'] = df_sist['dataHora'].dt.strftime('%d/%m/%Y %H:%M')
    
    comlumns_order = ['dataHora', 'sist','cmo', 'demanda', 'grenova', 'somatgh', 'somatgt',
                     'somagtmin', 'somatgtmax', 'earm', 'intercambio',  'pld']
    df_sist = df_sist[comlumns_order]
       

    # Converte string de data para datetime
    registros = []
    for reg in df_sist.itertuples(index=False):
        data_str = reg.dataHora
        dt = datetime.datetime.strptime(data_str, '%d/%m/%Y %H:%M')
        linha = list(reg)
        linha[0] = dt
        registros.append(linha)

    # Delete + Insert
    inicio_dia = datetime.datetime.combine(data, datetime.time.min)
    fim_dia = datetime.datetime.combine(data, datetime.time.max)

    logger.info(f"Apagando registros existentes entre {inicio_dia} e {fim_dia}")
       
    # Persistência no banco
    db_decks = wx_dbClass.db_mysql_master('db_decks')
    db_decks.connect()
    tb_balanco = db_decks.getSchema('tb_balanco_dessem')

    
    delete_stmt = tb_balanco.delete().where(
        tb_balanco.c.dt_data_hora.between(inicio_dia, fim_dia)
    )
    db_decks.conn.execute(delete_stmt)

    logger.info(f"Inserindo {len(registros)} novos registros (IGNORE)")
    insert_stmt = tb_balanco.insert().values(registros).prefix_with('IGNORE')
    db_decks.conn.execute(insert_stmt)

    logger.info("=== PROCESSAMENTO CONCLUÍDO COM SUCESSO ===\n")


if __name__ == '__main__':
    logger.info("Script iniciado diretamente")
    path = "C:/Users/cs341053/Downloads/DES_202511/Resultado_DS_CCEE_112025_SEMREDE_RV3D24"
    data = datetime.date(2025, 11, 24)
    readPdoSist(path, data, '')

    