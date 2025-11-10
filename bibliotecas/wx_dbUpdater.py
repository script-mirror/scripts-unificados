import re
import os
import sys
import sqlite3
import datetime
import os.path
import shutil
import zipfile
import pandas as pd
from pathlib import Path

sys.path.insert(1,"/WX2TB/Documentos/fontes/PMO/scripts_unificados/")
from bibliotecas import wx_opweek
from bibliotecas.wx_dbLib import *

home_path =  os.path.expanduser("~")
dropbox_middle_path = os.path.join(home_path, 'Dropbox', 'WX - Middle')

path_bibliotecas = os.path.dirname(os.path.abspath(__file__))
dirPrincipal = os.path.dirname(path_bibliotecas)
path_arquivos = os.path.join(dirPrincipal, 'arquivos')
path_apps = os.path.join(dirPrincipal, 'apps')

path_app_decomp = os.path.join(path_apps, 'decomp')
path_app_dessem = os.path.join(path_apps, 'dessem')

path_libs_decomp = os.path.join(path_app_decomp, 'libs')
sys.path.insert(1, path_libs_decomp)


def extractFiles(file):
    zipNameFile = os.path.basename(file)
    path = os.path.dirname(file)
    dstFolder = os.path.join(path, zipNameFile.split('.')[0])

    zipNameFile = os.path.basename(file)
    path = os.path.dirname(file)
    dstFolder = os.path.join(path, zipNameFile.split('.')[0])

    if os.path.exists(dstFolder):
        shutil.rmtree(dstFolder)

    if not os.path.exists(dstFolder):
        os.makedirs(dstFolder)

    with zipfile.ZipFile(file, 'r') as zip_ref:
        zip_ref.extractall(dstFolder)

    return dstFolder




def createTabelasRodadasProspec():

    pathBanco = os.path.join(path_arquivos, 'db_rodadas_prospec.db')

    if not os.path.exists(pathBanco):

        conn = sqlite3.connect(pathBanco)
        cursor = conn.cursor()

        query = """CREATE TABLE TB_CADASTRO_RODADAS_PROSPEC (
                    ID INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                    ID_PROSPEC INTEGER NOT NULL,
                    DT_RODADA TEXT NOT NULL,
                    DT_PRIMEIRA_RV TEXT NOT NULL,
                    TX_MODELO TEXT,
                    TX_COMENTARIO TEXT,
                    TX_FCF TEXT,
                    ID_RODADA_SMAP INTEGER,
                    FL_MATRIX INTEGER);"""

        cursor.execute(query)

        query = """CREATE TABLE TB_RESULTADOS_RODADAS_PROSPEC (
                        ID_CADASTRO INTEGER NOT NULL,
                        DT_REFERENTE TEXT NOT NULL,
                        VL_CMO_MEDIO NUMERIC NOT NULL,
                        VL_EA_INICIAL NUMERIC NOT NULL,
                        VL_ENA NUMERIC NOT NULL,
                        VL_ENA_PERCENT_MENSAL NUMERIC,
                         CD_SUBMERCADO INTEGER NOT NULL);"""
        cursor.execute(query)

        conn.close()
        
        print('Banco criado com sucesso: {}'.format(pathBanco))

def insertEstudoProspec(pathEstudo,data,nomeRodadaOriginal='Original',comentario=None,fcf=None,id_smap=None,flagMatrix=0,fazer_media_mes=0):

    # createTabelasRodadasProspec()

    pathBanco = os.path.join(path_arquivos, 'db_rodadas_prospec.db')
    data = data.date()
    pathCompilado = extractFiles(pathEstudo)

    nomeEstudo = os.path.basename(pathCompilado)
    match = re.match('Estudo_([0-9]{1,})_compilation',nomeEstudo)
    numeroEstudo = match.group(1)

    print('Inserindo estudo numero '+numeroEstudo)
    
    df = pd.read_csv(os.path.join(pathCompilado, 'compila_cmo_medio.csv'), sep=';')
    df['Sensibilidade'] = df['Sensibilidade'].replace({'Original':nomeRodadaOriginal})
    df = df[df['MEN=0-SEM=1'] == 1]
    df['Sensibilidade'] = df['Sensibilidade'] 
    compila_cmo_medio = df

    df = pd.read_csv(os.path.join(pathCompilado, 'compila_ea_inicial.csv'), sep=';')
    df['Sensibilidade'] = df['Sensibilidade'].replace({'Original':nomeRodadaOriginal})
    df = df[df['MEN=0-SEM=1'] == 1]
    df['Sensibilidade'] = df['Sensibilidade']
    compila_eaInicial = df

    df = pd.read_csv(os.path.join(pathCompilado, 'compila_ena.csv'), sep=';')
    df['Sensibilidade'] = df['Sensibilidade'].replace({'Original':nomeRodadaOriginal})
    df = df[df['MEN=0-SEM=1'] == 1]
    df['Sensibilidade'] = df['Sensibilidade']
    compila_ena = df

    df = pd.read_csv(os.path.join(pathCompilado, 'compila_ena_mensal_percentual.csv'), sep=';')
    df['Sensibilidade'] = df['Sensibilidade'].replace({'Original':nomeRodadaOriginal})		
    df = df[df['MEN=0-SEM=1'] == 1]
    df['Sensibilidade'] = df['Sensibilidade']
    compila_enaPercentualMensal = df

    dicionarioDecks = {}
    for dk in compila_cmo_medio['Deck'].unique():
        mes, sem, encadeamento = re.findall('[0-9]+', dk)
        inicioMesEletrico = wx_opweek.getLastSaturday(datetime.datetime.strptime(mes, '%Y%m').date())
        dt = wx_opweek.ElecData(inicioMesEletrico + datetime.timedelta(days=7*(int(sem)+int(encadeamento)-2)))
        dicionarioDecks[dk] = dt.data

    if fazer_media_mes and flagMatrix:
        compila_cmo_medio_media = pd.DataFrame(columns=compila_cmo_medio.columns)
        for sens, grupo in compila_cmo_medio.groupby('Sensibilidade').__iter__():
            media = grupo.iloc[0].copy()
            media.update(grupo[['SUDESTE','SUL','NORDESTE','NORTE']].mean())
            compila_cmo_medio_media = compila_cmo_medio_media.append(media)
        compila_cmo_medio = compila_cmo_medio_media

        compila_eaInicial = compila_eaInicial[compila_eaInicial['Deck'].str[-3:] == '_s1']
        compila_ena = compila_ena[compila_ena['Deck'].str[-3:] == '_s1']
        compila_enaPercentualMensal = compila_enaPercentualMensal[compila_enaPercentualMensal['Deck'].str[-3:] == '_s1']

    elif fazer_media_mes:
        print('Não foi feita a média, essa funcionalidade so foi desenvolvida para matrizes')

    compila_cmo_medio['Deck'] = compila_cmo_medio['Deck'].replace(dicionarioDecks)
    compila_eaInicial['Deck'] = compila_eaInicial['Deck'].replace(dicionarioDecks)
    compila_ena['Deck'] = compila_ena['Deck'].replace(dicionarioDecks)
    compila_enaPercentualMensal['Deck'] = compila_enaPercentualMensal['Deck'].replace(dicionarioDecks)

    submercados = {'SUDESTE':1,'SUL':2,'NORDESTE':3,'NORTE':4}
    cmo = {}
    ear = {}
    ena = {}
    enaPercentMensal = {}
    for sub in submercados:

        cmo[sub] = compila_cmo_medio.pivot(index='Sensibilidade', columns='Deck', values=sub)
        ear[sub] = compila_eaInicial.pivot(index='Sensibilidade', columns='Deck', values=sub)
        ena[sub] = compila_ena.pivot(index='Sensibilidade', columns='Deck', values=sub)
        enaPercentMensal[sub] = compila_enaPercentualMensal.pivot(index='Sensibilidade', columns='Deck', values=sub)
        # Simulacoes nao encadeadas nao vao apresentar todas as colunas com todas as datas
        # portanto serao completadas com null
        enaPercentMensal[sub] = enaPercentMensal[sub].reindex(columns = cmo[sub].columns)
    
    dtRvs = cmo['SUDESTE'].columns

    conn = sqlite3.connect(pathBanco)
    cursor = conn.cursor()

    query = """SELECT ID FROM TB_CADASTRO_RODADAS_PROSPEC WHERE ID_PROSPEC = ? """
    cursor.execute(query,(numeroEstudo,))
    idsDeletar = cursor.fetchall()
    idsDeletar = [i[0] for i in idsDeletar]

    if len(idsDeletar):
        query = """DELETE FROM TB_RESULTADOS_RODADAS_PROSPEC WHERE ID_CADASTRO IN ({})""".format(str(idsDeletar)[1:-1])
        print('Rodada ja cadastrada. Os resultados anteriores serao excluidos')
        cursor.execute(query)
        conn.commit()


    query = """DELETE FROM TB_CADASTRO_RODADAS_PROSPEC WHERE ID_PROSPEC = ?""" 
    cursor.execute(query,(numeroEstudo,))
    conn.commit()

    query = """INSERT INTO TB_CADASTRO_RODADAS_PROSPEC
                (ID_PROSPEC, DT_RODADA, DT_PRIMEIRA_RV, TX_MODELO, TX_COMENTARIO, TX_FCF, ID_RODADA_SMAP, FL_MATRIX)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?);"""

    vals = []
    for index, row in cmo['SUDESTE'].iterrows():
        coment = comentario
        if flagMatrix:
            coment += ', NE:{}, N:{}'.format(round(enaPercentMensal['NORDESTE'].loc[index][0]),round(enaPercentMensal['NORTE'].loc[index][0]))
        vals.append((numeroEstudo,data,min(dtRvs),index,coment,fcf,id_smap, flagMatrix))
    
    cursor.executemany(query,vals)
    conn.commit()

    query = """SELECT TX_MODELO, ID FROM TB_CADASTRO_RODADAS_PROSPEC WHERE ID_PROSPEC = ?"""
    cursor.execute(query,(numeroEstudo,))
    idRodadas = dict(cursor.fetchall())

    query = """INSERT INTO TB_RESULTADOS_RODADAS_PROSPEC
                (ID_CADASTRO, DT_REFERENTE, VL_CMO_MEDIO, VL_EA_INICIAL, VL_ENA, VL_ENA_PERCENT_MENSAL, CD_SUBMERCADO)
                VALUES(?, ?, ?, ?, ?, ?, ?);"""

    vals = []
    for dt in dtRvs:
        dt = wx_opweek.ElecData(dt)

        for sub in submercados:

            val_ear = ear[sub][dt.data]
            val_cmo = cmo[sub][dt.data]
            val_ena = ena[sub][dt.data]
            val_enaPerceMens = enaPercentMensal[sub][dt.data]
            
            for model in val_cmo.keys():
                vals.append((idRodadas[model],dt.data,val_cmo[model],val_ear[model],val_ena[model],val_enaPerceMens[model],submercados[sub]))

    cursor.executemany(query,vals)
    conn.commit()

    conn.close()

    if os.path.exists(pathCompilado):
        shutil.rmtree(pathCompilado)

def removeEstudoProspec(numeroEstudo):

    pathBanco = os.path.join(path_arquivos, 'db_rodadas_prospec.db')
    conn = sqlite3.connect(pathBanco)
    cursor = conn.cursor()

    query = """SELECT ID FROM TB_CADASTRO_RODADAS_PROSPEC WHERE ID_PROSPEC = ? """
    cursor.execute(query,(numeroEstudo,))
    idsDeletar = cursor.fetchall()
    idsDeletar = [i[0] for i in idsDeletar]

    if len(idsDeletar):
        query = """DELETE FROM TB_RESULTADOS_RODADAS_PROSPEC WHERE ID_CADASTRO IN ({})""".format(str(idsDeletar)[1:-1])
        cursor.execute(query)
        conn.commit()

        query = """DELETE FROM TB_CADASTRO_RODADAS_PROSPEC WHERE ID_PROSPEC = ?""" 
        cursor.execute(query,(numeroEstudo,))
        conn.commit()

        print('Estudo {} deletado com sucesso!'.format(numeroEstudo))


def print_helper():

    dtHoje = datetime.datetime.now()
    ultimo_sabado = wx_opweek.getLastSaturday(dtHoje.date())

    path_aux_acomph = os.path.abspath('/WX2TB/Documentos/fontes/PMO/monitora_ONS/plan_acomph_rdh/')
    path_resultados_prospec = os.path.abspath('/WX2TB/Documentos/fontes/PMO/API_Prospec/DownloadResults')

    helper = 'Script utilizado para inserir RDH, ACOMPH, PREVS, SMAP ... no banco de dados.\n'
    helper += 'Exemplos de possiveis entradas:\n'
    helper += 'python wx_dbUpdater.py importar_estudos_prospec {} dt {} nomeOriginal "PCONJUNTO" comentario "Rodada diaria automatica" flagMatriz 0 fazer_media_mensal 0\n'.format(os.path.join(path_resultados_prospec,'Estudo_9284_compilation.zip'), dtHoje.strftime('%d/%m/%Y'))
 
    helper += '\nRemocao de dados do banco\n'
    helper += 'python wx_dbUpdater.py remover_estudos_prospec id 9284\n'

    helper += '\nObs: O parametro "data" se refere a data em que o modelo foi rodada (dd/mm/aaaa) e o parametro "dataInicialPrevs" e o dia em que o mes eletrico se inicia referente (dd/mm/aaaa) referente aquela rodada'
    print(helper)
    
def runWithParams():
    if len(sys.argv) > 1:
        # passar o print do help pra quando nao entrar com nenhum parametro
        if 'help' in sys.argv or '--h' in sys.argv or '-help' in sys.argv:
            print_helper()
            quit()

        executar = {'funcao':'', 'path':'', 'dataInicialPrevs':'', 'dataExec':datetime.datetime.now(), 'nomeoriginal': 'Original'}

        # Verificacao e substituicao dos valores defaults com 
        # os valores passados por parametro 
        for i in range(1, len(sys.argv)):

            # transforma para lowercase o argumento
            argumento = sys.argv[i].lower()

            if argumento == 'importar_estudos_prospec':
                executar['funcao']='insertEstudoProspec'
                executar['path'] = Path(sys.argv[i+1])

            elif argumento == 'remover_estudos_prospec':
                executar['funcao']='removeEstudoProspec'

            elif argumento == 'nomeoriginal':
                executar['nomeoriginal'] = sys.argv[i+1]

            elif argumento == 'comentario':
                executar['comentario'] = sys.argv[i+1]

            elif argumento == 'flagmatriz':
                executar['flagmatriz'] = sys.argv[i+1]

            elif argumento == 'fazer_media_mensal':
                executar['flagmediames'] = int(sys.argv[i+1])
    
            elif argumento == 'dtrodada':
                    executar['dtrodada'] = datetime.datetime.strptime(sys.argv[i+1], '%d/%m/%Y')

            elif argumento == 'dt':
                executar['data'] = datetime.datetime.strptime(sys.argv[i+1], '%d/%m/%Y')
                
            elif argumento == 'id':
                executar['id'] = int(sys.argv[i+1])

    
            elif argumento == 'remover_estudos_prospec':
                executar['funcao']='removeEstudoProspec'

            elif argumento == 'nomeoriginal':
                executar['nomeoriginal'] = sys.argv[i+1]

            elif argumento == 'comentario':
                executar['comentario'] = sys.argv[i+1]

            elif argumento == 'flagmatriz':
                executar['flagmatriz'] = sys.argv[i+1]

            elif argumento == 'fazer_media_mensal':
                executar['flagmediames'] = int(sys.argv[i+1])
    
            elif argumento == 'dtrodada':
                    executar['dtrodada'] = datetime.datetime.strptime(sys.argv[i+1], '%d/%m/%Y')

            elif argumento == 'dt':
                executar['data'] = datetime.datetime.strptime(sys.argv[i+1], '%d/%m/%Y')
                
            elif argumento == 'id':
                executar['id'] = int(sys.argv[i+1])
            elif argumento == 'modelo':
                executar['modelo'] = sys.argv[i+1]
            elif argumento == 'rodada':
                executar['rodada'] = int(sys.argv[i+1])


            elif argumento in ['data']:
                try:
                    data_exec = datetime.datetime.strptime(sys.argv[i+1], '%d/%m/%Y')
                    executar['dataExec'] = data_exec
                    if 'dataInicialPrevs' not in sys.argv:
                        mes_eletrico = wx_opweek.ElecData(data_exec.date())
                        executar['dataInicialPrevs'] = mes_eletrico.primeiroDiaMes
                except Exception as e:
                    print('Erro ao tentar converter a data, entre com a data no seguinte formato: dd/mm/yyyy')
                    quit()

            elif argumento in ['datainicialprevs']:
                executar['dataInicialPrevs'] = datetime.datetime.strptime(sys.argv[i+1], '%d/%m/%Y').date()

    else:
        print_helper()
        exit()


    # verificar se o arquivo existe
    if executar['path'] != '':
        if executar['path'].exists() :
            pass
        else:
            print('Arquivo {0} nao encontrado!'.format(executar['path']))
            quit()

    
    elif executar['funcao'] == 'importFolder':
        cmd = "importFolder(path=executar['path'], dataExec=executar['dataExec'], dataInicialPrevs=executar['dataInicialPrevs'])"

    elif executar['funcao'] == 'insertEstudoProspec':
        insertEstudoProspec(pathEstudo=executar['path'],data=executar['data'],nomeRodadaOriginal=executar['nomeoriginal'],comentario=executar['comentario'],flagMatrix=executar['flagmatriz'],fazer_media_mes=executar['flagmediames'])
        cmd = 'print("Estudo inserido com sucesso!")'

    elif executar['funcao'] == 'removeEstudoProspec':
        removeEstudoProspec(numeroEstudo=executar['id'])
        cmd = 'print("Estudo removido com sucesso!")'

    else:
        print_helper()
        exit()
        
    eval(cmd)
        
if __name__ == '__main__':

    pdocmoPath = '/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/dessem/arquivos/20210210/saida/resultados/PDO_CMOSIST.DAT'
    dtRodada = datetime.datetime(2021,2,10)
    runWithParams()