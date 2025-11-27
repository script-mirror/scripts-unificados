import os
import re
import sys
import warnings
import datetime
from datetime import date
import numpy as np
import pandas as pd
import sqlalchemy as db
import logging
from logging.handlers import RotatingFileHandler

# ====================== CONFIGURAÇÃO DO LOGGER ======================
try:
    import colorlog
    handler = colorlog.StreamHandler()
    handler.setFormatter(colorlog.ColoredFormatter(
        "%(log_color)s%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%H:%M:%S",
        log_colors={
            'DEBUG':    'cyan',
            'INFO':     'green',
            'WARNING':  'yellow',
            'ERROR':    'red',
            'CRITICAL': 'red,bg_white',
        }))
except ImportError:
    # fallback sem cores
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%H:%M:%S"
    ))

# Logger principal
logger = logging.getLogger("PDO_SIST")
logger.setLevel(logging.DEBUG)        # Muda para INFO se quiser menos detalhe
if not logger.handlers:
    logger.addHandler(handler)

# Opcional: salvar também em arquivo (máximo 5MB, 5 backups)
file_handler = RotatingFileHandler("pdo_sist_debug.log", maxBytes=5_000_000, backupCount=5)
file_handler.setFormatter(logging.Formatter(
    "%(asctime)s | %(levelname)-8s | %(funcName)s | %(lineno)d | %(message)s"
))
logger.addHandler(file_handler)
# =====================================================================

warnings.filterwarnings("ignore")

sys.path.insert(1, "/WX2TB/Documentos/fontes/PMO/scripts_unificados/")
from bibliotecas import wx_dbClass


def getFromFile(path):
    logger.info(f"getFromFile chamado com path: {path}")
    dir_file = os.path.dirname(path)
    fullname_file = os.path.basename(path)
    name_file, extension_file = os.path.splitext(fullname_file)
    extension_file = extension_file.lstrip('.')

    possible_paths = [
        os.path.join(dir_file, f'{name_file.lower()}.{extension_file}'),
        os.path.join(dir_file, f'{name_file.upper()}.{extension_file}'),
        os.path.join(dir_file, f'{name_file.lower()}.{extension_file.upper()}'),
        os.path.join(dir_file, f'{name_file.upper()}.{extension_file.upper()}'),
    ]

    for p in possible_paths:
        if os.path.exists(p):
            logger.debug(f"Arquivo encontrado: {p}")
            with open(p, 'r', encoding="latin-1") as f:
                lines = f.readlines()
            logger.info(f"Arquivo lido com sucesso – {len(lines)} linhas")
            return lines

    logger.error(f"Arquivo não encontrado em nenhum dos caminhos tentados: {possible_paths}")
    raise FileNotFoundError(f"Não foi possível localizar o arquivo: {path}")


def leituraArquivo(filePath):
    logger.info(f"leituraArquivo iniciado para: {filePath}")
    arquivo = getFromFile(filePath)

    dados = {'SIST': []}
    iLine = 0
    total_linhas = len(arquivo)

    while iLine < total_linhas:
        line = arquivo[iLine].rstrip("\n")
        logger.debug(f"Linha {iLine+1}/{total_linhas}: {line[:100]}{'...' if len(line)>100 else ''}")

        if '------;--------;------;------------;' in line:
            logger.info("Cabeçalho do bloco SIST encontrado")
            iLine += 4
            if iLine >= total_linhas:
                logger.warning("Fim do arquivo alcançado após cabeçalho")
                break
            line = arquivo[iLine]
            bloco = []
            while iLine < total_linhas:
                bloco.append(arquivo[iLine])
                iLine += 1
                if iLine < total_linhas:
                    line = arquivo[iLine]
                else:
                    break
            dados['SIST'] = bloco
            logger.info(f"Bloco SIST extraído com {len(bloco)} linhas")
            break
        else:
            iLine += 1

    logger.info("leituraArquivo finalizado")
    return dados


def getInfoBlocos():
    logger.debug("getInfoBlocos chamado")
    blocos = {
        'SIST': {
            'campos': [
                'iper', 'pat', 'sist', 'cmo', 'demanda', 'perdas', 'gpqusi', 'gfixbar',
                'grenova', 'somatgh', 'somatgt', 'conseleva', 'import.', 'export.',
                'cortcarg.', 'saldo', 'recebimento', 'somagtmin', 'somatgtmax', 'earm'
            ],
            'regex': r'(.{6});(.{8});(.{6});(.{12});(.{12});(.{12});(.{12});(.{12});(.{12});(.{12});(.{12});(.{12});(.{12});(.{12});(.{12});(.{12});(.{12});(.{12});(.{12});(.{20});(.*)',
            'formatacao': '{:>6};{:>8};{:>6};{:>12};{:>12};{:>12};{:>12};{:>12};{:>12};{:>12};{:>12};{:>12};{:>12};{:>12};{:>12};{:>12};{:>12};{:>12};{:>12};{:>20};'
        }
    }
    return blocos


def extrairInfoBloco(listaLinhas, mnemonico, regex):
    logger.debug(f"extrairInfoBloco chamado para mnemonico={mnemonico}")
    blocos = []
    if mnemonico in listaLinhas:
        logger.info(f"Bloco {mnemonico} encontrado com {len(listaLinhas[mnemonico])} linhas")
        for i, linha in enumerate(listaLinhas[mnemonico]):
            infosLinha = re.split(regex, linha)
            if len(infosLinha) < 3:
                logger.warning(f"Linha {i+1} do bloco {mnemonico} não casou com regex: {linha.strip()}")
                continue
            blocos.append(infosLinha[1:-2])  # remove vazio inicial e resto final
    else:
        logger.warning(f"Bloco {mnemonico} não encontrado na estrutura")
    return blocos


def read_pdo_sist(path: str) -> pd.DataFrame:        
    
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
            data.append(dict(zip(columns, [valor.strip() for valor in parts])))
        last_line = line
    df = pd.DataFrame(data)
    print(df)
    
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


def leituraSist(pdoSistPath):
    logger.info(f"leituraSist iniciado – caminho: {pdoSistPath}")
    infoBlocos = getInfoBlocos()
    pdoSist = leituraArquivo(pdoSistPath)
    precos = extrairInfoBloco(pdoSist, 'SIST', infoBlocos['SIST']['regex'])
    df_sist = pd.DataFrame(precos, columns=infoBlocos['SIST']['campos'])
    logger.info(f"DataFrame SIST criado: {df_sist.shape[0]} linhas × {df_sist.shape[1]} colunas")
    return df_sist


def calculoIntercambio(df_sist):
    logger.info("calculoIntercambio iniciado")
    df = df_sist.copy()
    for i in range(1, 49):
        df_aux = df[df['iper'] == i].apply(pd.to_numeric, errors='coerce').sum()
        ande = df_aux['demanda'] - df_aux['grenova'] - df_aux['somatgh'] - df_aux['somatgt'] + df_aux['conseleva']
        idx = df[(df['sist'] == 'SE') & (df['iper'] == i)].index
        if not idx.empty:
            df.loc[idx, 'somatgh'] = df.loc[idx, 'somatgh'].astype(float) + ande

    df['intercambio'] = df.apply(
        lambda x: x['demanda'] - x['grenova'] - x['somatgh'] - x['somatgt'] + x['conseleva'], axis=1
    )
    logger.info("calculoIntercambio concluído")
    return df


def insertData(df_sist, dataDeck):
    logger.info("insertData iniciado")
    df_out = pd.DataFrame()
    subsistemas = df_sist[df_sist['iper'] == 1]['sist'].unique()
    logger.debug(f"Subsistemas encontrados: {list(subsistemas)}")

    for subm in subsistemas:
        iper = 1
        while iper < 48:
            try:
                cmo1 = float(df_sist[(df_sist['sist'] == subm) & (df_sist['iper'] == iper)]['cmo'].iloc[0])
                cmo2 = float(df_sist[(df_sist['sist'] == subm) & (df_sist['iper'] == iper + 1)]['cmo'].iloc[0])
                cmo_media = int((cmo1 + cmo2) / 2)

                # mesma lógica para as outras colunas...
                demanda1 = int(df_sist[(df_sist['sist'] == subm) & (df_sist['iper'] == iper)]['demanda'].iloc[0])
                demanda2 = int(df_sist[(df_sist['sist'] == subm) & (df_sist['iper'] == iper + 1)]['demanda'].iloc[0])
                conseleva1 = int(df_sist[(df_sist['sist'] == subm) & (df_sist['iper'] == iper)]['conseleva'].iloc[0])
                conseleva2 = int(df_sist[(df_sist['sist'] == subm) & (df_sist['iper'] == iper + 1)]['conseleva'].iloc[0])
                demanda_media = int((demanda1 + demanda2 + conseleva1 + conseleva2) / 2)

                # ... (demais campos com mesmo padrão)

                dict_aux = {
                    'dataHora': f"{dataDeck.day}/{dataDeck.month}/{dataDeck.year} {int((iper-1)*30//60):02d}:00",
                    'sist': subm,
                    'cmo': str(cmo_media),
                    'demanda': str(demanda_media),
                    # completar os demais campos se precisar...
                }
                df_out = pd.concat([df_out, pd.DataFrame([dict_aux])], ignore_index=True)
            except Exception as e:
                logger.error(f"Erro ao processar iper={iper}, sist={subm}: {e}")
            iper += 2
    logger.info(f"insertData concluído – {len(df_out)} registros gerados")
    return df_out


def calculo_pld(lista_input, PLD_min, PLDmax_h, PLDmax_estr):

    for i in range(len(lista_input)):
        val = float(lista_input[i])
        if val > PLDmax_h:
            lista_input[i] = PLDmax_h
        if val < PLD_min:
            lista_input[i] = PLD_min

    PLD_md = sum(lista_input) / len(lista_input)
    cont = 0
    while abs(PLD_md - PLDmax_estr) > 0.01 and cont < 30:
        f_est = PLDmax_estr / PLD_md
        lista_input = [round(x * f_est, 2) for x in lista_input]
        PLD_md = sum(lista_input) / len(lista_input)
        cont += 1
    return lista_input


def calculaPLD(df_sist, data):
    logger.info("calculaPLD iniciado")
    db_ons = wx_dbClass.db_mysql_master('db_ons')
    db_ons.connect()
    tb_pld = db_ons.getSchema('tb_pld')
    query = db.select(tb_pld).where(tb_pld.c.str_ano == data.year)
    result = db_ons.conn.execute(query).fetchall()

    if not result:
        logger.error(f"Não encontrado limite de PLD para o ano {data.year}")
        return

    PLDmax_hora = result[0].vl_PLDmax_hora
    PLDmax_estr = result[0].vl_PLDmax_estr
    PLDmin = result[0].vl_PLDmin
    logger.info(f"Limites PLD {data.year}: min={PLDmin}, max_hora={PLDmax_hora}, max_estr={PLDmax_estr}")

    regioes = {'SE': 'SE', 'S': 'S', 'NE': 'NE', 'N': 'N'}
    listas_pld = {}
    for sigla, nome in regioes.items():
        lista_cmo = df_sist[df_sist['sist'] == nome]['cmo'].astype(float).tolist()
        listas_pld[sigla] = calculo_pld(lista_cmo, PLDmin, PLDmax_hora, PLDmax_estr)

    # Monta lista final intercalada
    listPld = []
    for i in range(len(listas_pld['SE'])):
        for reg in ['SE', 'S', 'NE', 'N']:
            listPld.append(listas_pld[reg][i])

    df_sist = df_sist.copy()
    df_sist['pld'] = listPld
    logger.info("PLDs calculados e inseridos no DataFrame")
    return df_sist


def readPdoSist(path, data, pathOut):
    data = datetime.datetime.strptime(data, '%Y-%m-%d')
    logger.info(f"=== INÍCIO DO PROCESSAMENTO ===")
    logger.info(f"Data de referência: {data}")
    logger.info(f"Pasta de entrada: {path}")

    # Leitura do arquivo
    
    df_sist = read_pdo_sist(path)
    
    
    """    try:
            df_sist = leituraSist(os.path.join(path, 'pdo_sist.dat'))
        except Exception as e:
            logger.warning(f"pdo_sist.dat não encontrado, tentando PDO_SIST.DAT")
            df_sist = leituraSist(os.path.join(path, 'PDO_SIST.DAT'))
    """
    logger.info(f"DataFrame bruto carregado: {df_sist.shape}")

    # Limpeza e tipagem
    colunas_remover = ['perdas', 'gpqusi', 'gfixbar', 'import.', 'export.', 'cortcarg.', 'saldo', 'recebimento']
    df_sist = df_sist.drop(columns=[c for c in colunas_remover if c in df_sist.columns])
    df_sist['iper'] = df_sist['iper'].astype(int)
    df_sist = df_sist[df_sist['iper'] <= 48]

    numeric_cols = ['demanda', 'grenova', 'somatgh', 'conseleva', 'somatgt',
                    'somagtmin', 'somatgtmax', 'earm', 'cmo']
    for col in numeric_cols:
        if col in df_sist.columns:
            df_sist[col] = pd.to_numeric(df_sist[col], errors='coerce').fillna(0).astype(int)

    df_sist['sist'] = df_sist['sist'].str.strip()

    # Cálculos
    df_sist = calculoIntercambio(df_sist)
    df_sist = df_sist[df_sist['sist'] != 'FC']
    df_sist = calculaPLD(df_sist, data)

    # Preparação para insert
    pdoSist_final = insertData(df_sist, data)
    logger.info(f"Total de registros para insert: {len(pdoSist_final)}")

    # Persistência no banco
    db_decks = wx_dbClass.db_mysql_master('db_decks')
    db_decks.connect()
    tb_balanco = db_decks.getSchema('tb_balanco_dessem')

    # Converte string de data para datetime
    registros = []
    for reg in pdoSist_final.itertuples(index=False):
        data_str = reg.dataHora
        dt = datetime.datetime.strptime(data_str, '%d/%m/%Y %H:%M')
        linha = list(reg)
        linha[0] = dt
        registros.append(linha)

    # Delete + Insert
    inicio_dia = datetime.datetime.combine(data.date(), datetime.time.min)
    fim_dia = datetime.datetime.combine(data.date(), datetime.time.max)

    logger.info(f"Apagando registros existentes entre {inicio_dia} e {fim_dia}")
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

    