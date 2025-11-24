import os
import re
import sys
import warnings
import datetime
import numpy as np
import pandas as pd
import sqlalchemy as db
import logging

warnings.filterwarnings("ignore")

# ===================================================================
# LOGGER SIMPLES E LIMPO - SÓ NA TELA (sem arquivo)
# ===================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S',
    handlers=[logging.StreamHandler(sys.stdout)]
)

log = logging.getLogger(__name__)

# ===================================================================
# Seu código com logs claros e úteis (só no terminal)
# ===================================================================

sys.path.insert(1,"/WX2TB/Documentos/fontes/PMO/scripts_unificados/")
from bibliotecas import wx_dbClass


def getFromFile(path):
    dir_file = os.path.dirname(path)
    fullname_file = os.path.basename(path)
    name_file, extension_file = fullname_file.split('.')

    tentativa1 = os.path.join(dir_file, f'{name_file.lower()}.{extension_file}')
    tentativa2 = os.path.join(dir_file, f'{name_file.upper()}.{extension_file}')

    if os.path.exists(tentativa1):
        caminho = tentativa1
    elif os.path.exists(tentativa2):
        caminho = tentativa2
    else:
        log.error(f"Arquivo não encontrado: {path}")
        raise FileNotFoundError(path)

    log.info(f"Lendo: {caminho}")
    with open(caminho, 'r', encoding="latin-1") as f:
        return f.readlines()


def leituraArquivo(filePath):
    log.info(f"Procurando e lendo arquivo: {filePath}")
    arquivo = getFromFile(filePath)
    dados = {'SIST': []}
    iLine = 0

    while iLine < len(arquivo):
        line = arquivo[iLine].strip()

        if '------;--------;------;------------;' in line:
            log.info("Bloco SIST encontrado!")
            iLine += 4
            bloco = []
            while iLine < len(arquivo):
                bloco.append(arquivo[iLine])
                iLine += 1
            dados['SIST'] = bloco
            log.info(f"Bloco SIST extraído → {len(bloco)} linhas")
            break
        iLine += 1

    return dados


def getInfoBlocos():
    return {
        'SIST': {
            'campos': ['iper','pat','sist','cmo','demanda','perdas','gpqusi','gfixbar','grenova','somatgh',
                       'somatgt','conseleva','import.','export.','cortcarg.','saldo','recebimento',
                       'somagtmin','somatgtmax','earm'],
            'regex': r'(.{6});(.{8});(.{6});(.{12});(.{12});(.{12});(.{12});(.{12});(.{12});(.{12});' +
                     r'(.{12});(.{12});(.{12});(.{12});(.{12});(.{12});(.{12});(.{12});(.{12});(.{20});(.*)'
        }
    }


def extrairInfoBloco(listaLinhas, mnemonico, regex):
    blocos = []
    if mnemonico not in listaLinhas or not listaLinhas[mnemonico]:
        log.warning(f"Bloco {mnemonico} não encontrado")
        return blocos

    for linha in listaLinhas[mnemonico]:
        partes = re.split(regex, linha)
        if len(partes) > 3:
            blocos.append(partes[1:-2])
    return blocos


def leituraSist(pdoSistPath):
    log.info(f"Processando PDO_SIST: {pdoSistPath}")
    info = getInfoBlocos()
    dados = leituraArquivo(pdoSistPath)
    linhas = extrairInfoBloco(dados, 'SIST', info['SIST']['regex'])
    df = pd.DataFrame(linhas, columns=info['SIST']['campos'])
    log.info(f"DataFrame criado → {df.shape[0]} linhas, {df.shape[1]} colunas")
    return df


def calculoIntercambio(df_sist):
    log.info("Calculando intercâmbio e ajustando SE/CO...")
    df_sist = df_sist.copy()
    df_sist[['demanda','grenova','somatgh','somatgt','conseleva']] = df_sist[['demanda','grenova','somatgh','somatgt','conseleva']].astype(float)

    for i in range(1, 49):
        filtro = df_sist['iper'].astype(int) == i
        aux = df_sist[filtro].iloc[:, 4:12].astype(float).sum()
        ande = aux['demanda'] - aux['grenova'] - aux['somatgh'] - aux['somatgt'] + aux['conseleva']
        se_idx = df_sist[(df_sist['sist'] == 'SE') & filtro].index
        if not se_idx.empty:
            df_sist.loc[se_idx, 'somatgh'] = float(df_sist.loc[se_idx, 'somatgh']) + ande

    df_sist['intercambio'] = (df_sist['demanda'] - df_sist['grenova'] - df_sist['somatgh'] -
                              df_sist['somatgt'] + df_sist['conseleva'])
    return df_sist


def insertData(df_sist, dataDeck):
    log.info("Convertendo para formato horário (média de 30 em 30 min)")
    df_out = pd.DataFrame()
    subs = df_sist[df_sist['iper'].astype(int) == 1]['sist'].unique()

    for subm in subs:
        for i in range(1, 48, 2):
            l1 = df_sist[(df_sist['sist'] == subm) & (df_sist['iper'].astype(int) == i)]
            l2 = df_sist[(df_sist['sist'] == subm) & (df_sist['iper'].astype(int) == i+1)]
            if l1.empty or l2.empty: continue

            def avg(col): return str(int((float(l1.iloc[0][col]) + float(l2.iloc[0][col])) / 2))

            hora = f"{int((i-1)*0.5):02d}:00"
            dict_aux = {
                'dataHora': f"{dataDeck.day}/{dataDeck.month}/{dataDeck.year} {hora}",
                'sist': subm,
                'cmo': avg('cmo'),
                'demanda': avg('demanda'),
                'grenova': avg('grenova'),
                'somatgh': avg('somatgh'),
                'somatgt': avg('somatgt'),
                'somagtmin': avg('somagtmin'),
                'somagtmax': avg('somatgtmax'),
                'intercambio': avg('intercambio'),
                'pld': '0'
            }
            df_out = pd.concat([df_out, pd.DataFrame([dict_aux])], ignore_index=True)
    return df_out


def calculo_pld(lista_input, PLD_min, PLDmax_h, PLDmax_estr):
    lista = [float(x) for x in lista_input]
    # piso e teto horário
    lista = [max(PLD_min, min(PLDmax_h, x)) for x in lista]
    # ajuste estrutural
    media = sum(lista) / len(lista)
    cont = 0
    while media > PLDmax_estr + 0.01 and cont < 30:
        lista = [round(x * PLDmax_estr / media, 2) for x in lista]
        media = sum(lista) / len(lista)
        cont += 1
    return lista


def calculaPLD(df_sist, data):
    log.info("Buscando limites de PLD no banco...")
    db_ons = wx_dbClass.db_mysql_master('db_ons')
    db_ons.connect()
    tb = db_ons.getSchema('tb_pld')
    result = db_ons.conn.execute(db.select(tb).where(tb.c.str_ano == data.year)).fetchall()
    if not result:
        log.error("Limites de PLD não encontrados!")
        return
    PLDmax_h, PLDmax_estr, PLDmin = result[0].vl_PLDmax_hora, result[0].vl_PLDmax_estr, result[0].vl_PLDmin

    subs = ['SE','S','NE','N']
    plds = []
    for s in subs:
        cmo = df_sist[df_sist['sist'] == s]['cmo'].tolist()
        plds.append(calculo_pld(cmo, PLDmin, PLDmax_h, PLDmax_estr))

    pld_final = [x for sub in zip(*plds) for x in sub]
    df_sist['pld'] = pld_final[:len(df_sist)]
    log.info("PLD calculado e aplicado")


def readPdoSist(path, data, pathOut=None):
    log.info("="*60)
    log.info(f"INICIANDO PROCESSAMENTO PDO_SIST → {data.strftime('%d/%m/%Y')}")
    log.info("="*60)

    # Leitura
    try:
        df_sist = leituraSist(os.path.join(path, 'pdo_sist.dat'))
    except:
        df_sist = leituraSist(os.path.join(path, 'PDO_SIST.DAT'))

    # Limpeza
    cols_drop = ['perdas','gpqusi','gfixbar','import.','export.','cortcarg.','saldo','recebimento']
    df_sist = df_sist.drop(columns=[c for c in cols_drop if c in df_sist.columns], errors='ignore')
    df_sist = df_sist[df_sist['iper'].astype(int) <= 48]
    df_sist['sist'] = df_sist['sist'].str.strip()

    # Conversões
    num_cols = ['demanda','grenova','somatgh','conseleva','somatgt','somagtmin','somatgtmax','earm','cmo']
    for c in num_cols:
        if c in df_sist.columns:
            df_sist[c] = pd.to_numeric(df_sist[c], errors='coerce').fillna(0).astype(int)

    # Processamentos
    df_sist = calculoIntercambio(df_sist)
    df_final = df_sist[df_sist['sist'] != 'FC']
    calculaPLD(df_final, data)
    resultado = insertData(df_final, data)

    # Inserção no banco
    db_decks = wx_dbClass.db_mysql_master('db_decks')
    db_decks.connect()
    tb = db_decks.getSchema('tb_balanco_dessem')

    valores = []
    for _, row in resultado.iterrows():
        dt = datetime.datetime.strptime(row['dataHora'], '%d/%m/%Y %H:%M')
        valores.append([dt, row['sist'], row['cmo'], row['demanda'], row['grenova'],
                        row['somatgh'], row['somatgt'], row['somagtmin'], row['somagtmax'],
                        row['intercambio'], row['pld']])

    # Delete + Insert
    ini = datetime.datetime.combine(data.date(), datetime.time.min)
    fim = datetime.datetime.combine(data.date(), datetime.time.max)
    db_decks.conn.execute(tb.delete().where(tb.c.dt_data_hora.between(ini, fim)))
    db_decks.conn.execute(tb.insert().values(valores).prefix_with('IGNORE'))

    log.info(f"Sucesso! {len(valores)} registros inseridos para {data.date()}")
    log.info("PROCESSAMENTO FINALIZADO\n")


if __name__ == '__main__':
    caminho = r'/home/thiago/workspace/wx/alpha/apps/dessem/arquivos/20210217/entrada/ons_entrada_saida'
    data = datetime.datetime(2021, 2, 17)
    readPdoSist(caminho, data)