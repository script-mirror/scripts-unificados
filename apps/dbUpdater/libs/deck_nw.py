

import os 
import re
import pdb
import sys
import locale
import zipfile
import datetime
import pandas as pd
import sqlalchemy as db
from calendar import monthrange

PATH_DIR_LOCAL = os.path.dirname(os.path.abspath(__file__))

locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')

path_fontes = "/WX2TB/Documentos/fontes/"
sys.path.insert(1,path_fontes)
from PMO.scripts_unificados.bibliotecas import wx_dbClass , rz_dir_tools


DIR_TOOLS = rz_dir_tools.DirTools()
FLAGS_REGEX_SISTEMA = re.compile("CUSTO DO DEFICIT|INTERCAMBIO|ENERGIA|GERACAO|")

def update_flag_sistema(linha, flag):

    if FLAGS_REGEX_SISTEMA.search(linha):

        if re.search("CUSTO DO DEFICIT", linha):
            flag = 'custo_intercambio'
        elif re.search("INTERCAMBIO", linha):
            flag = 'intercambio'
        elif re.search("ENERGIA", linha):
            flag = 'energia'
        elif re.search("GERACAO", linha):
            flag = 'geracao'
    
    return flag


def process_lines(flag, lines ,dict_final):

    i_linha=0

    while i_linha < len(lines):

        linha = lines[i_linha]

        flag = update_flag_sistema(linha, flag)

        if flag == 'custo_intercambio':
                
            if re.search(r'\b\d{1}\b', linha):
                newline_values = linha.split()
                dict_final['sistema']['custo_intercambio'] += newline_values,

        elif flag == 'intercambio':

            if re.search(r'\b\d{1}\b', linha):
                count = 0
                newline_values = linha.split()

                #sistemas
                src = newline_values[0]
                dst = newline_values[1]

            elif re.search(r'\b\d{4}\b', linha):
                
                if count == 5:
                    src,dst = dst,src

                newline_values = linha.split()

                qnt_nulls = 13 - len(newline_values) 
                null_list = [None] * qnt_nulls
                newline_values = newline_values[:1] + null_list + newline_values[1:] +[src,dst]
                
                dict_final['sistema']['intercambio'] += newline_values,

                count+= 1
            
        elif flag  == 'energia':

            if re.search(r'\b\d{1}\b', linha):
                newline_values = linha.split()
                submercado = newline_values[0]

            elif re.search(r'^\d{1,7}', linha):
                newline_values = linha.split()
                qnt_nulls = 13 - len(newline_values) 
                null_list = [None] * qnt_nulls
                newline_values = newline_values[:1] + null_list + newline_values[1:] + [submercado]
                dict_final['sistema']['energia'] += newline_values,
                
        elif flag == 'geracao':
            if re.search(r'\b\d{1}\b', linha) and re.search(r"\s+([A-Z]+)\s*$", linha):
                newline_values = linha.split()
                submercado = newline_values[0]

                if "MMGD" in linha: 
                    tipo_geracao = newline_values[-2] + "MMGD"
                else:
                    tipo_geracao = newline_values[-1]

                if not dict_final['sistema']['geracao'].get(tipo_geracao):
                    dict_final['sistema']['geracao'][tipo_geracao] = []

            elif re.search(r'^\d{1,7}', linha):

                newline_values = linha.split()
                qnt_nulls = 13 - len(newline_values) 
                null_list = [None] * qnt_nulls
                newline_values = newline_values[:1] + null_list + newline_values[1:] + [submercado]

                dict_final['sistema']['geracao'][tipo_geracao] += newline_values,
        
        elif flag == 'c_adic':

            if re.search(r'\b\d{1}\b', linha) and re.search(r"([A-Z]+)", linha): 
                newline_values = linha.split()

                if len(newline_values) >3:
                    grupo = f"{newline_values[-2]} {newline_values[-1]}"
                else:
                    grupo = newline_values[-1]

                if not dict_final['c_adic'].get(grupo):
                    dict_final['c_adic'][grupo] = []

            elif re.search(r'^\d{1,7}', linha):
                newline_values = linha.split()
                qnt_nulls = 13 - len(newline_values) 
                null_list = [None] * qnt_nulls
                newline_values = newline_values[:1] + null_list + newline_values[1:]

                dict_final['c_adic'][grupo] += newline_values,
        i_linha += 1
    return dict_final
    
def read_deck_nw_files(path_zip, files = ['SISTEMA.DAT']):

    dt_now = datetime.datetime.now().strftime("%d%m%Y %H%M%S")
    path_dst = os.path.join(PATH_DIR_LOCAL,"tmp",dt_now)
    extracted_file = DIR_TOOLS.extract_specific_files_from_zip(path=path_zip,files_name_template=files ,dst=path_dst, extracted_files = [])
    
    dict_final = {}
    for path_arquivo_base in set(extracted_file):

        if 'SISTEMA.DAT' in path_arquivo_base:
            dict_final['sistema'] = {'custo_intercambio':[],'intercambio':[],'energia':[],'geracao':{}}
            flag = None

        elif 'C_ADIC.DAT' in path_arquivo_base:
            dict_final['c_adic'] = {}
            flag = 'c_adic'

        with open(path_arquivo_base) as arquivo_base:
            lines = arquivo_base.readlines()
            dict_final = process_lines(flag,lines,dict_final)

    DIR_TOOLS.remove_src(path_dst)
    return dict_final

def processar_sist_energia_geracao_NW(dict_final):

        #ENERGIA + GERACAO
        meses = [1,2,3,4,5,6,7,8,9,10,11,12]
        columns = ['Ano'] + meses + ['cd_submercado']

        df_energia = pd.DataFrame(dict_final['sistema']['energia'], columns=columns)
        df_energia_melted = pd.melt(df_energia, id_vars=['Ano', 'cd_submercado'], value_vars=meses,var_name='Mês', value_name='Energia Total')

        df_geracao = pd.DataFrame()
        for geracao in dict_final['sistema']['geracao']:
            
            tt2 = pd.DataFrame(dict_final['sistema']['geracao'][geracao], columns=columns) 
            
            df_geracao_especifica = pd.melt(tt2, id_vars=['Ano', 'cd_submercado'], value_vars=meses,var_name='Mês', value_name=geracao)  
            
            if df_geracao.empty:
                df_geracao = df_geracao_especifica
            else:
                df_geracao = pd.merge(df_geracao,df_geracao_especifica, on=['Ano', "cd_submercado",  "Mês"])
        
        df_energia_geracao = pd.merge(df_geracao,df_energia_melted, on = ['Ano', 'cd_submercado',"Mês"])

        return df_energia_geracao


def processar_sist_limites_intercambio_NW(dict_final):
    
    #INTERCAMBIO
    meses = [1,2,3,4,5,6,7,8,9,10,11,12]
    columns = ['Ano'] + meses + ['sistema_src', 'sistema_dst']
    df_intercambio = pd.DataFrame(dict_final['sistema']['intercambio'],columns=columns)
    df_intercambio_melted = pd.melt(df_intercambio, id_vars=['Ano', 'sistema_src','sistema_dst'], value_vars=meses,var_name='Mês', value_name='intercambio')

    return df_intercambio_melted

def processar_c_adic_NW(dict_final):

    meses = [1,2,3,4,5,6,7,8,9,10,11,12]
    columns = ['Ano'] + meses 

    df_c_adic = pd.DataFrame()
    for geracao in dict_final['c_adic']:
        tt2 = pd.DataFrame(dict_final['c_adic'][geracao], columns=columns) 
        df_grupo_especifica = pd.melt(tt2, id_vars=['Ano'], value_vars=meses,var_name='Mês', value_name=geracao)  
        if df_c_adic.empty:
            df_c_adic = df_grupo_especifica
        else:
            df_c_adic = pd.merge(df_c_adic,df_grupo_especifica, on=['Ano', "Mês"])

    return df_c_adic


def importar_deck_values_nw(path_zip):

    # regex = "NW(\d{6})"
    file_name = os.path.basename(path_zip)
    
    str_date = file_name.replace('.zip', '').replace('NW', '')
    print(str_date)
    # date = datetime.datetime.strptime(str_date, "%Y%m")
    date = datetime.datetime.strptime(str_date, "%Y%m")
    str_date_formated = date.strftime("%Y-%m-%d")

    dict_final = read_deck_nw_files(path_zip, files = ['SISTEMA.DAT','C_ADIC.DAT'])

    df_cadic_values = processar_c_adic_NW(dict_final)
    df_sist_intercambio_values = processar_sist_limites_intercambio_NW(dict_final)
    df_sist_values = processar_sist_energia_geracao_NW(dict_final)
    
    df_cadic_values['versao'] = 'definitivo'
    df_sist_values['versao'] = 'definitivo'
    table_values = [
        ("tb_nw_cadic", df_cadic_values[['Ano','Mês','CONS.ITAIPU','ANDE','MMGD SE','MMGD S','MMGD NE','BOA VISTA','MMGD N', 'versao']].copy()),
        ("tb_nw_sist_intercambio", df_sist_intercambio_values[['Ano','Mês','sistema_src','sistema_dst','intercambio']].copy()),
        ("tb_nw_sist_energia", df_sist_values[['cd_submercado','Ano','Mês','Energia Total','PCH','PCT','EOL','UFV','PCHMMGD','PCTMMGD','EOLMMGD','UFVMMGD', 'versao']].copy())
        ]
    
    db_decks = wx_dbClass.db_mysql_master('db_decks')
    db_decks.connect()

    for table_name, values in table_values:

        table = db_decks.getSchema(table_name)
        values.loc[:,'dt_deck'] = str_date_formated
        sql_delete = table.delete().where(table.c.dt_deck == str_date_formated)
        n_values = db_decks.db_execute(sql_delete).rowcount
        print(f"{n_values} Linhas deletadas na {table_name}!")

        sql_insert = table.insert().values(values.dropna(axis=0).values.tolist())
        n_values = db_decks.db_execute(sql_insert).rowcount
        print(f"{n_values} Linhas inseridas na {table_name}!")
    return str_date
    

def importar_carga_nw(pathZip, dataReferente, str_fonte, tx_comentario=""):


    dicionario_fonte = {'ons': 1, 'ccee': 2}

    # validando o id_fonte
    if str_fonte in dicionario_fonte:
        id_fonte = dicionario_fonte[str_fonte]
    else:
        raise KeyError(f"str_fonte '{str_fonte}' chave invalida. Chaves validas são: {list(dicionario_fonte.keys())}")

    db_decks = wx_dbClass.db_mysql_master("db_decks")
    db_decks.connect()
    tb_cadastro_newave = db_decks.getSchema('tb_cadastro_newave')
    tb_nw_carga = db_decks.getSchema('tb_nw_carga')
    
    # Verifica se o caminho do arquivo que foi enviado existe no diretorio correto.
    isExist = os.path.exists(pathZip)
    if isExist == False:
        print("Nome do arquivo ou caminho inserido está incorreto, digite novamente!")
    # abre o arquivo e faz o unzip
    else:
        archive = zipfile.ZipFile(pathZip, 'r')
        xlfile = archive.open(archive.filelist[0])
        # Faz a checagem se a dataReferente é um sábado, se for o primeiro sábado do mês ele usa a data caso contrario ele pega o ultimo sabado do mes atual.
        if dataReferente.weekday() == 5:
            dataReferente = dataReferente
        elif dataReferente.weekday() == 6:
            dataReferente = dataReferente + datetime.timedelta(days=dataReferente.weekday()-7)
        else:
            dataReferente = dataReferente - datetime.timedelta(days=dataReferente.weekday()+2)
        # Faz a primeira query para buscar o ID e Data referente da tabela cadastro newave.
        querySelect = db.select([tb_cadastro_newave.c.id])\
        .where(db.and_(tb_cadastro_newave.c.dt_inicio_rv == dataReferente, tb_cadastro_newave.c.id_fonte == id_fonte))
        idCadastro = db_decks.conn.execute(querySelect).fetchone()
        # Faz a filtragem do arquivo excel e cria um dataframe com os dados necessários.
        columns_to_skip = ['WEEK', 'GAUGE','Base_CGH','Base_EOL','Base_UFV','Base_UTE','Base_MMGD','LOAD_cMMGD']
        try:
            pmo_mes = pd.read_excel(xlfile, usecols=lambda x: x not in columns_to_skip,sheet_name=1)
        except:
            pmo_mes = pd.read_excel(xlfile, usecols=lambda x: x not in columns_to_skip)
        pmo_mes_filtrado = pmo_mes[(pmo_mes.TYPE == "MEDIUM")]
        pmo_mes_filtrado.reset_index(inplace=True, drop=True)
        pmo_mes_filtrado_rename = pmo_mes_filtrado.reset_index().rename(columns={'index':'id','DATE':'dt_referente',
        'SOURCE':'cd_submercado','LOAD_sMMGD':'vl_carga','REVISION':'dt_inicio_rv'}).drop(['TYPE'],axis=1)
        
        pmo_mes_filtrado_rename['cd_submercado'] = pmo_mes_filtrado_rename['cd_submercado'].apply(lambda x : 1 if x == 'SUDESTE' 
        else 2 if x  == 'SUL' else 3 if x == 'NORDESTE' else 4)
        # Filtro para armazenar a data da revisão que está dentro do arquivo da ONS.
        dt_inicio_rv_atual = pmo_mes_filtrado_rename['dt_inicio_rv'].unique()
        dt_inicio_rv_atual_dateTime = pd.to_datetime(dt_inicio_rv_atual[0])
        datetime_O = datetime.datetime.strftime(dt_inicio_rv_atual_dateTime, "%Y/%m/%d %H:%M:%S")
        data_Filtrada = datetime.datetime.strptime(datetime_O, "%Y/%m/%d %H:%M:%S")
        ultima_data_Filtrada = data_Filtrada.replace(day=1) + datetime.timedelta(monthrange(data_Filtrada.year, data_Filtrada.month)[1] - 1)
        ultima_data_Filtrada += datetime.timedelta(days = 1)
        # Verifica se a revisão após a filtragem é o primeiro sabado do proximo mês ou o ultimo.
        if ultima_data_Filtrada.weekday() == 5:
            ultimoSabadoFiltrado = ultima_data_Filtrada
        elif ultima_data_Filtrada.weekday() == 6:
            ultimoSabadoFiltrado = ultima_data_Filtrada + datetime.timedelta(days=ultima_data_Filtrada.weekday()-7)
        else:
            ultimoSabadoFiltrado = ultima_data_Filtrada - datetime.timedelta(days=ultima_data_Filtrada.weekday()+2)
        # Filtro para verificar se a data referente digitada/recebida pela ONS é a mesma data revisao que está dentro do arquivo da ONS.
        if dataReferente == ultimoSabadoFiltrado:
            # Após a filtrar o id, será verificado se tem o id na tabela e caso não tenha o id, ele adiciona nas duas tabelas e caso tenha o id, ele apenas atualiza a carga da tabela nw carga.
            if idCadastro == None :
                tb_cadastro_newave_list = [idCadastro, dataReferente,id_fonte,tx_comentario]
                db_decks.conn.execute(tb_cadastro_newave.insert().values(tb_cadastro_newave_list))
                print(f'Inserido novo cadastro na tabela Newave!')
                # Faz a query para filtragem do id com o maior valor da tabela cadastro newave, ou seja do ultimo id que foi adicionado na tabela cadastro newave.
                querySelect = db.select([db.func.max(tb_cadastro_newave.c.id)])
                idCadastro = db_decks.conn.execute(querySelect).scalar()
                id = int(idCadastro)
                # Com o id da tabela cadastro newave em maos, alteramos o id do dataframe para poder adicionar as informacoes na tabela carga nw.
                pmo_mes_filtrado_rename['id'] = pmo_mes_filtrado_rename['id'].apply(lambda x : id)
                df_nw_carga = pmo_mes_filtrado_rename.drop(['dt_inicio_rv'], axis=1)
                tb_nw_carga_list = df_nw_carga.values.tolist()
                linhasInseridas = db_decks.conn.execute(tb_nw_carga.insert().values(tb_nw_carga_list)).rowcount
                print(f"Inserido, {linhasInseridas} linhas novas na tabela nw carga.")
                # Caso o id for existente na tabela cadastro newave, vamos apenas fazer a atualizacao da carga na tabela carga nw.
            else:
                # Faz a query para buscar o ID e Data referente da tabela cadastro newave.
                querySelect = db.select([tb_cadastro_newave.c.id])\
                .where(db.and_(tb_cadastro_newave.c.dt_inicio_rv == dataReferente, tb_cadastro_newave.c.id_fonte == id_fonte))
                idCadastro = db_decks.conn.execute(querySelect).scalar()
                id = int(idCadastro)
                # Com o id da tabela cadastro newave em maos, alteramos o id do dataframe para poder adicionar as informacoes na tabela carga nw.
                pmo_mes_filtrado_rename['id'] = pmo_mes_filtrado_rename['id'].apply(lambda x : id)
                df_nw_carga = pmo_mes_filtrado_rename.drop(['dt_inicio_rv'], axis=1)
                tb_nw_carga_list = df_nw_carga.values.tolist()
                # Caso o id for existente na tabela cadastro newave, vamos apenas fazer a atualizacao da carga na tabela carga nw.
                linhasDeletadas = db_decks.conn.execute(tb_nw_carga.delete().where(tb_nw_carga.c.id_deck == id)).rowcount
                print(f"Cargas com data inicio rev: {dataReferente.strftime(format='%d/%m/%Y')} um total de {linhasDeletadas} linhas deletadas, na tabela nw carga!")
                linhasInseridas = db_decks.conn.execute(tb_nw_carga.insert().values(tb_nw_carga_list)).rowcount
                print(f"Cargas atualizadas, {linhasInseridas} linhas inseridas na tabela nw carga.") 
        else:
            print(f'A data correta da revisão é: {ultimoSabadoFiltrado.strftime(format="%d/%m/%Y")}')
            print(f'Os dados não foram atualizado.')


if __name__ == '__main__':



    # pass
    path = r"C:/Users/cs399274/Downloads/NW202404 (1).zip"

    # path  = "/WX2TB/Documentos/fontes/PMO/decks/ccee/nw/NW202404.zip"
    teste = importar_deck_values_nw('/WX2TB/Documentos/fontes/PMO/decks/ccee/nw/NW202506.zip')

