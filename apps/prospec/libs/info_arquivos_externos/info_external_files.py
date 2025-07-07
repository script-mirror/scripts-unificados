import os
import re
import sys
import pdb
import csv
import glob
import datetime
import pdfplumber
import numpy as np
import pandas as pd


sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.bibliotecas import rz_dir_tools,wx_opweek
from PMO.scripts_unificados.apps.prospec.libs.ccee import service as ccee_api
from PMO.scripts_unificados.apps.prospec.libs.decomp.dadger import dadger
from PMO.scripts_unificados.apps.prospec.libs.newave.sistema import sistema
from PMO.scripts_unificados.apps.prospec.libs import utils

from typing import List
import logging
import requests as r
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO,
                    format='%(levelname)s:\t%(asctime)s\t %(name)s.py:%(lineno)d\t %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers=[
                        logging.StreamHandler()
                    ])

logger = logging.getLogger(__name__)

load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")),'.env'))
__HOST_SERVIDOR = os.getenv('HOST_SERVIDOR')
URL_COGNITO = os.getenv('URL_COGNITO')
CONFIG_COGNITO = os.getenv('CONFIG_COGNITO')

PATH_DOWNLOAD_TMP = os.path.join(os.path.dirname(__file__),"tmp")

def get_access_token() -> str:
    response = r.post(
        URL_COGNITO,
        data=CONFIG_COGNITO,
        headers={'Content-Type': 'application/x-www-form-urlencoded'}
    )
    return response.json()['access_token']

def organizar_info_cvu(fontes_to_search:List[str], dt_atualizacao:datetime.date):

    URL_API_RAIZEN = f'http://{__HOST_SERVIDOR}:8000/api/v2'

    df_aux = pd.DataFrame()
    for title_fonte in fontes_to_search:
        response = r.get(
            params = {
                'dt_atualizacao': dt_atualizacao,
                'fonte': title_fonte
            },
            url = f"{URL_API_RAIZEN}/decks/cvu",
            headers={
                'Authorization': f'Bearer {get_access_token()}'
            }
        )
        answer = response.json()
        df_result = pd.DataFrame(answer)

        df_result['dt_atualizacao'] = pd.to_datetime(df_result['dt_atualizacao']).dt.strftime('%Y-%m-%d')
        df_result[df_result['dt_atualizacao'] == pd.to_datetime(dt_atualizacao).strftime('%Y-%m-%d')]

        df_aux = pd.concat([df_aux,df_result]) if not df_aux.empty else df_result
        
    df_aux.dropna(subset=['vl_cvu'], inplace=True)
    df_aux.drop_duplicates(['cd_usina'], keep='last', inplace=True)
    return df_aux



def extract_carga_decomp_ons(path_carga,path_saida=PATH_DOWNLOAD_TMP):
        DIR_TOOLS = rz_dir_tools.DirTools()

        extracted_zip_carga = DIR_TOOLS.extract_specific_files_from_zip(
                path=path_carga,
                files_name_template=["*CargaDecomp*.txt"],
                dst=path_saida,
                extracted_files=[]
                )

        return extracted_zip_carga


def organizar_info_carga(path_carga_zip,path_deck, data_revisao:datetime.date = datetime.date.today()):

    carga_decomp_txt = extract_carga_decomp_ons(
        path_carga_zip
    )

    df_dadger_novo, comentarios = dadger.leituraArquivo(carga_decomp_txt[0])

    bloco_dp:pd.DataFrame = df_dadger_novo['DP']
    bloco_dp['ip'] = bloco_dp['ip'].astype(int)

    nome_arquivo_zip = os.path.basename(path_carga_zip).split('.')[0]
    # inicio_rv_atual = datetime.date(int(df_dadger_novo['DT']['ano'].iloc[0]),int(df_dadger_novo['DT']['mes'].iloc[0]),int(df_dadger_novo['DT']['dia'].iloc[0]))
    
    while data_revisao.weekday() != 6:
        data_revisao = data_revisao + datetime.timedelta(days=1)
    semana_eletrica_atual = wx_opweek.ElecData(data_revisao)
    semana_eletrica = wx_opweek.ElecData(data_revisao)
    semanas_a_remover = 0

    novo_bloco = {}
    while 1:
        nome_dadger = f'dadger.rv{semana_eletrica.atualRevisao}'
        path_dadger = os.path.join(path_deck,f"DC{semana_eletrica.anoReferente}{semana_eletrica.mesRefente:0>2}-sem{ semana_eletrica.atualRevisao + 1}",nome_dadger)
        if not os.path.exists(path_dadger):
            break


        if semana_eletrica.atualRevisao == 0 and semana_eletrica_atual.primeiroDiaMes != semana_eletrica.primeiroDiaMes:
            if semana_eletrica.primeiroDiaMes.day != 1:

                df_bloco_atual, comentarios_bloco_atual = dadger.leituraArquivo(
                    glob.glob(
                        os.path.join(
                            path_deck,f"DC{semana_eletrica.anoReferente}{semana_eletrica.mesRefente:0>2}-sem{semana_eletrica.atualRevisao+1}/dadger*"
                            )
                        )[0]
                    )

                df_bloco_atual = df_bloco_atual['DP']
                df_bloco_atual['ip'] = df_bloco_atual['ip'].astype(int)

                index_ultimo_bloco = int(bloco_dp['ip'].max()) - 1
                ultimo_bloco = bloco_dp[bloco_dp['ip']==index_ultimo_bloco].copy()
                ultimo_bloco['ip'] = 1
                df_bloco_atual[df_bloco_atual['ip']==1] = ultimo_bloco.values

                dt_referente = f"{semana_eletrica.anoReferente}{semana_eletrica.mesRefente:0>2}"
                novo_bloco[dt_referente] = {} if dt_referente not in novo_bloco.keys() else novo_bloco[dt_referente]
                novo_bloco[dt_referente][semana_eletrica.atualRevisao] = utils.trim_df(df_bloco_atual)

        else:
            df_novo_bloco = bloco_dp.copy()
            df_novo_bloco['ip'] = df_novo_bloco['ip'] - semanas_a_remover
            df_novo_bloco = df_novo_bloco.loc[df_novo_bloco['ip']>=1].copy()
            semanas_a_remover = semanas_a_remover + 1

            dt_referente = f"{semana_eletrica.anoReferente}{semana_eletrica.mesRefente:0>2}"
            novo_bloco[dt_referente] = {} if dt_referente not in novo_bloco.keys() else novo_bloco[dt_referente]
            novo_bloco[dt_referente][semana_eletrica.atualRevisao] = utils.trim_df(df_novo_bloco)

        semana_eletrica = wx_opweek.ElecData(semana_eletrica.data + datetime.timedelta(days=7))

        if semana_eletrica.mesRefente != semana_eletrica_atual.mesRefente and semana_eletrica.data.month != semana_eletrica_atual.data.month:
            break

    return novo_bloco

def completar_estagios(df):
    df_completo = pd.DataFrame(columns=df.columns)
    
    for nome in df['nome'].unique():
        df_nome = df[df['nome'] == nome]
        estagios = df_nome['estagio'].tolist()
        estagio_min = min(estagios)
        estagio_max = max(estagios)

        for estagio in range(estagio_min, estagio_max + 1):
            if estagio not in estagios:
                linha_base = df_nome.iloc[0].copy()
                linha_base['estagio'] = estagio
                
                df_completo = pd.concat([df_completo, pd.DataFrame([linha_base])], ignore_index=True)

        df_completo = pd.concat([df_completo, df_nome], ignore_index=True)

    df_completo = df_completo.sort_values(by=['nome', 'estagio']).reset_index(drop=True)
    df_completo = df_completo.drop_duplicates(['nome', 'estagio'])
    return df_completo


def df_pq_to_dadger(df: pd.DataFrame) -> str:
    result = ''.join(
        f"{row['mnemonico']:<4}{row['nome']:<11}{row['sub']:<1}{row['estagio']:>5}{row['gerac_p1']:>8}{row['gerac_p2']:>5}{row['gerac_p3']:>5}\n"
        for _, row in df.iterrows()
    )
    return result.rstrip("&\n").rstrip("\n")


def df_pq_to_dadger_carga(df: pd.DataFrame) -> str:
    result = ''.join(    f"{str(row['mnemonico'])[:4]:<4}{str(row['nome'])[:11]:<11}{str(row['sub'])[:1]:<1}{str(row['estagio'])[:5]:>5}{str(row['gerac_p1'])[:5]:>8}{str(row['gerac_p2'])[:5]:>5}{str(row['gerac_p3'])[:5]:>5}\n"    for _, row in df.iterrows())
    return result.rstrip("&\n").rstrip("\n")

def get_weol(data_produto:datetime.date, data_inicio_semana:datetime.datetime) -> pd.DataFrame:
    weol_decomp = r.get(f"http://{__HOST_SERVIDOR}:8000/api/v2/decks/weol/start-week-date",
                        params={"dataProduto":str(data_produto), "inicioSemana":str(data_inicio_semana)},
                        headers={
                'Authorization': f'Bearer {get_access_token()}'
            })
    weol_decomp.raise_for_status()
    if weol_decomp.json() == []:
        logger.info("Nenhum dado encontrado")
        return pd.DataFrame()

    df = pd.DataFrame(weol_decomp.json())
    df['valor'] = df['valor'].astype(str).str[:5]
    df_estagio = pd.DataFrame({'inicio_semana': df['inicio_semana'].sort_values().unique()}).reset_index().rename(columns={'index': 'estagio'})
    df_estagio['estagio'] += 1
    df = df.merge(df_estagio, on='inicio_semana', how='inner')
    return df


def get_carga_decomp(data_produto:datetime.date) -> pd.DataFrame:
    res = r.get(f"http://{__HOST_SERVIDOR}:8000/api/v2/decks/carga-patamar",
                        params={"dataProduto":str(data_produto)},
                        headers={
                'Authorization': f'Bearer {get_access_token()}'
            })
    res.raise_for_status()
    if res.json() == []:
        logger.info("Nenhum dado encontrado")
        return pd.DataFrame()
        
    df = pd.DataFrame(res.json())
    
    return df



def get_df_dadger(path_dadger:str) -> pd.DataFrame:
    df = utils.trim_df(dadger.leituraArquivo(path_dadger)[0]['PQ'])
    df['estagio'] = df['estagio'].astype(int)
    df['sub'] = df['sub'].astype(int)
    return df


def organizar_info_eolica(paths_dadgers:List[str], data_produto:datetime.date):
    novos_blocos = {}
    for path_dadger in paths_dadgers:
        data = utils.get_date_from_dadger(path_dadger)

        df_weol = get_weol(data_produto, data)

        # Completando estagios apenas para os blocos de EOL
        df_bloco_atual = get_df_dadger(path_dadger)
        df_eol = completar_estagios(df_bloco_atual[df_bloco_atual['nome'].str.endswith('_EOL')])

        # Removendo os blocos de EOL do df original, e adicionando os completos
        df_bloco_atual = df_bloco_atual[~df_bloco_atual['nome'].str.endswith('_EOL')]
        df_bloco_atual = pd.concat([df_bloco_atual, df_eol])

        df_bloco_atual = df_bloco_atual.reset_index()[df_bloco_atual.columns].sort_values(['sub', 'nome', 'estagio'])
        estagio_mensal = max(df_bloco_atual[df_bloco_atual['nome'].str.endswith('_EOL')]['estagio'].tolist())

        for index, row in df_bloco_atual.iterrows():
            submercado = row['nome']
            if submercado == 'SUL_EOL':
                submercado = 'S'
            elif submercado == 'NE_EOL':
                submercado = 'NE'
            elif submercado == 'N_EOL':
                submercado = 'N'
            else:
                continue
            if row['estagio'] == estagio_mensal:
                continue
            try:
                df_bloco_atual.at[index, 'gerac_p1'] = df_weol[(df_weol['submercado'] == submercado) & (df_weol['patamar'] == 'pesado') & (df_weol['estagio'] == row['estagio'])]['valor'].values[0]
                df_bloco_atual.at[index, 'gerac_p2'] = df_weol[(df_weol['submercado'] == submercado) & (df_weol['patamar'] == 'medio') & (df_weol['estagio'] == row['estagio'])]['valor'].values[0]
                df_bloco_atual.at[index, 'gerac_p3'] = df_weol[(df_weol['submercado'] == submercado) & (df_weol['patamar'] == 'leve') & (df_weol['estagio'] == row['estagio'])]['valor'].values[0]
            except:
                continue
        df_bloco_atual = df_bloco_atual.sort_values(['sub', 'nome', 'estagio'])
        novos_blocos[path_dadger] = df_pq_to_dadger(df_bloco_atual)
    return novos_blocos

def organizar_info_dadger_carga_pq(paths_dadgers:List[str], data_produto:datetime.date):
    novos_blocos = {}
    for path_dadger in paths_dadgers:

        df_carga = get_carga_decomp(data_produto)
        fonte_map = {'PCHgd': 'Exp_CGH', 'PCTgd': 'Exp_UTE', 'EOLgd': 'Exp_EOL', 'UFVgd': 'Exp_UFV'}
        colunas_alterar = [x.lower() for x in fonte_map.keys()]
        # Completando estagios apenas para os blocos de EOL
        df_bloco_atual = get_df_dadger(path_dadger)
        
        mask = df_bloco_atual['nome'].apply(lambda x: any(x.endswith(value) for value in fonte_map.keys()))
        df_estagios_preenchidos = completar_estagios(df_bloco_atual[mask])

        # Removendo os blocos de EOL do df original, e adicionando os completos
        df_bloco_atual = df_bloco_atual[~mask]
        df_bloco_atual = pd.concat([df_bloco_atual, df_estagios_preenchidos])

        df_bloco_atual = df_bloco_atual[df_bloco_atual.columns].sort_values(['sub', 'nome', 'estagio']).reset_index(drop=True)
        estagio_mensal = max(df_bloco_atual['estagio'].tolist())
        for index, row in df_bloco_atual.iterrows():
            submercado = row['nome'].split('_')[0]
            coluna_alterar = row['nome'].split('_')[1]
            if coluna_alterar in fonte_map.keys():
                # apenas SUL precisa ser ajustado
                if submercado == 'SUL':
                    submercado = 'S'
                if row['estagio'] == estagio_mensal:
                    # print(f"{row} estagio mensal")
                    continue
                try:
                    # Check for pesada patamar
                    dfcarga_p1 = df_carga[
                        (df_carga['submercado'] == submercado) &
                        (df_carga['patamar'] == 'pesada') &
                        (df_carga['estagio'] == row['estagio'])
                    ][fonte_map[coluna_alterar].lower()].values[0]
                    
                    current_value_p1 = df_bloco_atual.at[index, 'gerac_p1']
                    
                    # Check if one value is contained in the other as strings
                    current_str = str(current_value_p1)
                    dfcarga_str = str(dfcarga_p1)
                    values_match = current_str in dfcarga_str or dfcarga_str in current_str

                    if not values_match:
                        df_bloco_atual.at[index, 'gerac_p1'] = dfcarga_p1
                        print(f"\nUpdated {coluna_alterar} gerac_p1 from {current_value_p1} => {dfcarga_p1}")
                    else:
                        print(f"\nNo change needed for {coluna_alterar} gerac_p1 (value unchanged: {current_value_p1})")
                    
                    # Check for media patamar\\
                    dfcarga_p2 = df_carga[
                        (df_carga['submercado'] == submercado) &
                        (df_carga['patamar'] == 'media') &
                        (df_carga['estagio'] == row['estagio'])
                    ][fonte_map[coluna_alterar].lower()].values[0]
                    
                    current_value_p2 = df_bloco_atual.at[index, 'gerac_p2']
                    
                    # Check if one value is contained in the other as strings
                    current_str = str(current_value_p2)
                    dfcarga_str = str(dfcarga_p2)
                    values_match = current_str in dfcarga_str or dfcarga_str in current_str

                    if not values_match:
                        df_bloco_atual.at[index, 'gerac_p2'] = dfcarga_p2
                        print(f"\nUpdated {coluna_alterar} gerac_p2 from {current_value_p2} to {dfcarga_p2}")
                    else:
                        print(f"\nNo change needed for {coluna_alterar} gerac_p2 (value unchanged: {current_value_p2})")
                    
                    # Check for leve patamar
                    dfcarga_p3 = df_carga[
                        (df_carga['submercado'] == submercado) &
                        (df_carga['patamar'] == 'leve') &
                        (df_carga['estagio'] == row['estagio'])
                    ][fonte_map[coluna_alterar].lower()].values[0]
                    
                    current_value_p3 = df_bloco_atual.at[index, 'gerac_p3']
                    
                    # Check if one value is contained in the other as strings
                    current_str = str(current_value_p3)
                    dfcarga_str = str(dfcarga_p3)
                    values_match = current_str in dfcarga_str or dfcarga_str in current_str

                    if not values_match:
                        df_bloco_atual.at[index, 'gerac_p3'] = dfcarga_p3
                        print(f"\nUpdated {coluna_alterar} gerac_p3 from {current_value_p3} to {dfcarga_p3}")
                    else:
                        print(f"\nNo change needed for {coluna_alterar} gerac_p3 (value unchanged: {current_value_p3})")
                        
                except Exception as e:
                    print(f"Error updating {coluna_alterar} for estagio {row['estagio']}: {str(e)}")
                    continue
            else:
                print(f"coluna nao alterada {coluna_alterar}")
        df_bloco_atual = df_bloco_atual.sort_values(['sub', 'nome', 'estagio'])
        novos_blocos[path_dadger] = df_pq_to_dadger_carga(df_bloco_atual)
    return novos_blocos

def extract_carga_nw(path_carga,path_saida=PATH_DOWNLOAD_TMP):
        DIR_TOOLS = rz_dir_tools.DirTools()

        extracted_zip_carga = DIR_TOOLS.extract_specific_files_from_zip(
                path=path_carga,
                files_name_template=['PLAN*_CargaGlobal*','CargaMensal_PMO-*'],
                dst=path_saida,
                extracted_files=[]
                )  
        
        return extracted_zip_carga


def organizar_info_carga_nw(path_carga_zip):

    '''
    Essa função extrair e calcula para todas as cargas a média ponderada por patamar
    '''

    extracted_zip_carga = extract_carga_nw(path_carga_zip)
    df = pd.read_excel(extracted_zip_carga[0]) 
    df["DATE"] = pd.to_datetime(df["DATE"])

    df = df.rename(columns={"TYPE": "PATAMAR", "GAUGE": "HOURS"})
    pivot_df = df.pivot_table(
        index=["DATE", "SOURCE"],
        columns="PATAMAR",
        values=[col for col in df.columns if col not in ["DATE",'WEEK',"SOURCE","PATAMAR",'REVISION']],
        aggfunc="sum"
    )
    numeric_columns = [col for col in pivot_df.columns.levels[0] if col not in ["HOURS"]]

    for col in numeric_columns:
        pivot_df[(col, "Weighted")] = (
            (pivot_df[(col, "LOW")] * pivot_df[("HOURS", "LOW")]) +
            (pivot_df[(col, "MIDDLE")] * pivot_df[("HOURS", "MIDDLE")]) +
            (pivot_df[(col, "HIGH")] * pivot_df[("HOURS", "HIGH")])
        ) / (
            pivot_df[("HOURS", "LOW")] +
            pivot_df[("HOURS", "MIDDLE")] +
            pivot_df[("HOURS", "HIGH")]
        )
    result_df = pivot_df.reset_index()

    if 'carga_mensal' in os.path.basename(path_carga_zip).lower():
        mes_referencia_inicial = datetime.datetime.strptime(os.path.basename(extracted_zip_carga[0]).lower(),"cargamensal_pmo-%B%Y.xlsx")
        result_df = result_df[(result_df['DATE'] >= mes_referencia_inicial) & (result_df['DATE'] <= (mes_referencia_inicial + pd.DateOffset(months=1) ))]

    result_dict = {}
    result_df["YearMonth"] = result_df["DATE"].dt.strftime("%Y%m")

    for year_month, group in result_df.groupby("YearMonth"):

        weighted_columns = [col for col in result_df.columns if "Weighted" in col]
        relevant_columns = [("SOURCE",'')] + weighted_columns
        
        filtered_df = group[relevant_columns].copy()
        filtered_df.columns = ["SOURCE"] + [col[0] for col in weighted_columns]
        result_dict[year_month] = filtered_df

    return result_dict

def organizar_info_eolica_nw(paths_sistema:List[str], data_produto:datetime.date):
    novos_blocos = {}
    weol_decomp = r.get(f"http://{__HOST_SERVIDOR}:8000/api/v2/decks/weol/weighted-average", params={"dataProduto":str(data_produto)}, 
                        headers={
                'Authorization': f'Bearer {get_access_token()}'
            })
    weol_decomp.raise_for_status()
        
    df_weol = pd.DataFrame(weol_decomp.json())
    df_weol['inicioSemana'] = pd.to_datetime(df_weol['inicioSemana'])
    df_weol['mes'] = df_weol['inicioSemana'].dt.to_period('M')

    df_weol = df_weol.groupby(['mes', 'submercado'])['mediaPonderada'].mean().reset_index()
    for path_to_modify in paths_sistema:
        df_geracao:pd.DataFrame = sistema.leituraArquivo(path_to_modify)['GERACAO DE USINAS NAO SIMULADAS']
        df_geracao[0] = df_geracao[0].str.strip().apply(lambda x: "{:<6}".format(x))
        for submercado in sistema.SUBMERCADOS_MNEMONICO:
            index_sup = int(df_geracao[df_geracao[0] == "{:<6}".format(submercado)][df_geracao[1] == '3'].index[0])
            df_sub = df_geracao[index_sup:index_sup+6]

            df_weol_sub = df_weol[df_weol['submercado']==sistema.SUBMERCADOS_MNEMONICO[submercado]].reset_index().drop(columns='index')
            
            for i, _ in df_weol_sub['mes'].dt.month.reset_index().drop(columns='index').iterrows():
                
                aux_ano = "{:<6}".format(df_weol_sub['mes'].dt.year.reset_index().drop(columns='index').iloc[i]['mes'])
                aux_mes = int(df_weol_sub['mes'].dt.month.reset_index().drop(columns='index').iloc[i]['mes'])

                media_ponderada = f"{float(df_weol_sub.iloc[i]['mediaPonderada']):6.1f}"
                media_ponderada = media_ponderada[:media_ponderada.find('.')+1]
                
                media_ponderada = f"{media_ponderada:>7}"
                df_sub.loc[df_sub[0] == aux_ano, aux_mes] = media_ponderada
            df_geracao[index_sup:index_sup+6] = df_sub

        values_geracao = df_geracao.replace(np.nan, '').to_string(index=False, header=False).split('\n')
        
        for i in range(1, len(values_geracao)-1, 6):
            aux = values_geracao[i]
            parts = ' '.join(aux.split()).split(' ')
            if len(parts) == 4:
                parts[2] = f'{parts[2]} {parts[3]}'
            values_geracao[i] = f"{parts[0]:>4} {parts[1]:>4}  {parts[2]:<8}"
        values_geracao[-1] = f' {values_geracao[-1].strip()} '
        novos_blocos[path_to_modify] = values_geracao
    return novos_blocos

#===================================RESTRICOES ELETRICAS=====================================

# Caminho do arquivo PDF
def organizar_info_restricoes_eletricas_dc(pdf_path, table_name="Tabela 4-1: Resultados dos Limites Elétricos" ):
    dict_num = {'IPU60':462,'IPU50':461, 'Ger. MAD': 401, 'RNE': 403,'FNS':405,'FNESE':409,
                'FNNE':413, 'FNEN':415, 'EXPNE':417,'SE/CO→FIC':419,'EXPN':427, 'FNS+FNESE':429,'FSENE':431,'FSUL':437,
                'RSUL':439, 'RSE':441, '-RSE':443, 'FETXG+FTRXG':445,'FXGET+FXGTR':447 }

    info_restricoes={}

    file_pattern = re.compile(r".*_Limites PMO_(.*)\.pdf$")
    match = file_pattern.match(pdf_path)
    if not match:
        logger.info(f"Nome do arquivo PDF {pdf_path} não corresponde ao padrão esperado.")
        return {}
        
    primeiro_mes = pd.to_datetime(match.group(1).split("_")[0],format="%B-%Y") 
    segundo_mes = primeiro_mes + pd.DateOffset(months=1) 
 
    table_page = find_table_page(pdf_path, table_name)

    if table_page is None:
        logger.info("Tabela não encontrada no PDF.")
        return {}
    else:      
        df = extract_table_from_pdf(pdf_path, table_page)
        df_reformated = reformat_dataframe(df,dict_num).round(1)
        info_restricoes[primeiro_mes.strftime("%Y%m")] = df_reformated.copy()
        info_restricoes[segundo_mes.strftime("%Y%m")] = df_reformated[["Limite","2º Mês Pesada", "2º Mês Média", "2º Mês Leve"]].copy()
        return  info_restricoes

 # Função para processar a tabela do PDF
def extract_table_from_pdf(pdf_path, table_page):

    with pdfplumber.open(pdf_path) as pdf:

        page = pdf.pages[table_page - 1]  
        tables = page.extract_tables()
        
        if tables:
            df = pd.DataFrame(tables[0])
            df.columns = df.iloc[0] 
            df = df[1:] 
            df.reset_index(drop=True, inplace=True)
            return df
        else:
            logger.info("Nenhuma tabela encontrada na página especificada.")
            return None

 # Função para localizar a página pelo nome da tabela
def find_table_page(pdf_path, table_name):
    with pdfplumber.open(pdf_path) as pdf:
        for page_number, page in enumerate(pdf.pages):
            text = page.extract_text()
            if table_name in text:
                return page_number + 1 
    return None

def reformat_dataframe(df, dict_num):

    info_restricoes = {}

    reformatted_df = pd.DataFrame()
    reformatted_df["Item"] = df.iloc[2:, 0].str.strip()  # Coluna 0: Item
    reformatted_df["Limite"] = df.iloc[2:, 1].str.strip()  # Coluna 1: Limite
    reformatted_df["1º Mês Pesada"] = df.iloc[2:, 2].str.strip()  # Coluna 2
    reformatted_df["1º Mês Média"] = df.iloc[2:, 5].str.strip()  # Coluna 5
    reformatted_df["1º Mês Leve"] = df.iloc[2:, 8].str.strip()  # Coluna 8
    reformatted_df["2º Mês Pesada"] = df.iloc[2:, 11].str.strip()  # Coluna 11
    reformatted_df["2º Mês Média"] = df.iloc[2:, 14].str.strip()  # Coluna 14
    reformatted_df["2º Mês Leve"] = df.iloc[2:, 17].str.strip()  # Coluna 17
    reformatted_df.dropna(how="all", inplace=True)
    reformatted_df.fillna(method='ffill', axis=1, inplace=True)
    reformatted_df['RE'] = reformatted_df['Limite'].map(dict_num)
    reformatted_df.dropna(subset=['RE'], inplace=True)
    reformatted_df['RE'] = reformatted_df['RE'].astype(int)
    reformatted_df.drop('Item', axis=1, inplace=True)
    reformatted_df.set_index("RE", inplace=True)
    columns_to_multiply = ["1º Mês Pesada", "1º Mês Média", "1º Mês Leve",  "2º Mês Pesada", "2º Mês Média", "2º Mês Leve" ]
    reformatted_df[columns_to_multiply] = reformatted_df[columns_to_multiply].apply(pd.to_numeric, errors='coerce')
    reformatted_df[columns_to_multiply] *= 1000  # Multiplicar por 1000
    
    return reformatted_df

#============================================================================================



if __name__ == "__main__":
    # organizar_info_eolica_nw([
    #     '/home/arthur-moraes/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/prospec/libs/info_arquivos_externos/tmp/Estudo_22805/NW202503/sistema.dat'
    #     ], datetime.date(2025,1,22))
#     rvs = ['2024-11-30',
#  '2024-12-07',
#  '2024-12-14',
#  '2024-12-21',
#  '2024-12-28',
#  '2025-01-04',
#  '2025-01-11']
#     for rv in rvs:
#         print(wx_opweek.ElecData(datetime.datetime.strptime(rv, "%Y-%m-%d")).atualRevisao)
    # teste = ler_csv_para_dicionario("/home/arthur-moraes/Downloads/Deck_PrevMes_20241128/Arquivos Saida/Previsoes Subsistemas Finais/Total/Prev_20241128_Semanas_20241130_20250103.csv")

    organizar_info_dadger_carga_pq(['/home/arthur-moraes/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/prospec/libs/info_arquivos_externos/tmp/Estudo_25085/DC202505-sem2/dadger.rv1'], datetime.date(2025, 4, 26))