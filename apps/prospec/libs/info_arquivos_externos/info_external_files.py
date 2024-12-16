import os, re, sys, pdb, datetime, glob
import pandas as pd
import csv

sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.bibliotecas import rz_dir_tools,wx_opweek
from PMO.scripts_unificados.apps.verificadores.ccee import rz_download_cvu
from PMO.scripts_unificados.apps.prospec.libs.decomp.dadger import dadger
from PMO.scripts_unificados.apps.prospec.libs import utils
from typing import List
import logging
import requests as r
from pprint import pp
from dotenv import load_dotenv

logging.basicConfig(level=logging.DEBUG,
                    format='%(levelname)s:\t%(asctime)s\t %(name)s.py:%(lineno)d\t %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers=[
                        logging.StreamHandler()
                    ])

logger = logging.getLogger(__name__)

load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")),'.env'))
__HOST_SERVIDOR = os.getenv('HOST_SERVIDOR')

PATH_DOWNLOAD_TMP = os.path.join(os.path.dirname(__file__),"tmp")


def organizar_info_cvu(ano_referencia, mes_referencia,path_saida=PATH_DOWNLOAD_TMP):

    info_cvu={}

    extracted_files = rz_download_cvu.download_cvu_acervo_ccee(
        ano_referencia,
        mes_referencia,
        path_saida
        )

    for file in extracted_files:

        dt_referente = f"{file['anoReferencia']}{file['mesReferencia']}"
        info_cvu[dt_referente] = {} if dt_referente not in info_cvu.keys() else info_cvu[dt_referente]

        df = pd.read_excel(file['extractedPathFile'], sheet_name=0, skiprows=4)

        #busca a coluna conjuntural nos arquivos nao merchant
        try:
            df_conjuntural = df[['CÓDIGO','CVU CONJUNTURAL']].replace('-',None).dropna()
            info_cvu[dt_referente]['conjuntural'] = df_conjuntural
        except:
            pass

        #busca a coluna estrutural nos arquivos nao merchant, pode ser que nao tenha essa coluna
        try:
            df_estrutural = df[['CÓDIGO','CVU ESTRUTURAL']].replace('-',None).dropna()
            info_cvu[dt_referente]['estrutural'] = df_estrutural
        except:
            pass


        #busca a coluna conjuntural e estrutural (as duas usam a mesma coluna), nos arquivos merchant. Aqui ainda vem uma verificação para saber se pega a coluna com custo fixo ou sem custo fixo
        # por enquanto manter a coluna com custo fixo

        try:
            df = pd.read_excel(file['extractedPathFile'], sheet_name=0, skiprows=3)

            df_conjuntural = df[['CVU CF [R$/MWh]','Código']].copy()
            df_conjuntural.columns = ['CVU CONJUNTURAL','CÓDIGO']
            df_conjuntural_completo = pd.concat([info_cvu[dt_referente]['conjuntural'],df_conjuntural],axis=0)
            info_cvu[dt_referente]['conjuntural'] = df_conjuntural_completo


            df_estrutural = df[['CVU CF [R$/MWh]','Código']].copy()
            df_estrutural.columns = ['CVU ESTRUTURAL','CÓDIGO']
            info_cvu[dt_referente]['estrutural'] = df_estrutural

        except:
            pass

        try:
            info_cvu[dt_referente]['conjuntural']['CÓDIGO'] = info_cvu[dt_referente]['conjuntural']['CÓDIGO'].astype(str)
            info_cvu[dt_referente]['estrutural']['CÓDIGO'] = info_cvu[dt_referente]['estrutural']['CÓDIGO'].astype(str)
        except:
            pass

        os.remove(file['pathFile'])
        os.remove(file['extractedPathFile'])

    return info_cvu


def extract_carga_decomp_ons(path_carga,path_saida=PATH_DOWNLOAD_TMP):
        DIR_TOOLS = rz_dir_tools.DirTools()

        extracted_zip_carga = DIR_TOOLS.extract_specific_files_from_zip(
                path=path_carga,
                files_name_template=["*CargaDecomp*.txt"],
                dst=path_saida,
                extracted_files=[]
                )

        return extracted_zip_carga


def organizar_info_carga(path_carga_zip,path_deck):

    carga_decomp_txt = extract_carga_decomp_ons(
        path_carga_zip
    )

    df_dadger_novo, comentarios = dadger.leituraArquivo(carga_decomp_txt[0])

    bloco_dp:pd.DataFrame = df_dadger_novo['DP']
    bloco_dp['ip'] = bloco_dp['ip'].astype(int)

    nome_arquivo_zip = os.path.basename(path_carga_zip).split('.')[0]
    inicio_rv_atual = utils.get_rv_atual_from_zip(nome_arquivo_zip)

    semana_eletrica_atual = wx_opweek.ElecData(inicio_rv_atual.date())
    semana_eletrica = wx_opweek.ElecData(inicio_rv_atual.date())
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
                df_completo = df_completo._append(linha_base, ignore_index=True)

        df_completo = df_completo._append(df_nome, ignore_index=True)

    df_completo = df_completo.sort_values(by=['nome', 'estagio']).reset_index(drop=True)
    return df_completo

def df_pq_to_dadger(df: pd.DataFrame) -> str:
    result = ''.join(
        f"{row['mnemonico']:<4}{row['nome']:<11}{row['sub']:<1}{row['estagio']:>5}{row['gerac_p1']:>8}{row['gerac_p2']:>5}{row['gerac_p3']:>5}\n"
        for _, row in df.iterrows()
    )
    return result.removesuffix("&\n").removesuffix("\n")

def get_weol(data_produto:datetime.date, data_inicio_semana:datetime.datetime) -> pd.DataFrame:
    weol_decomp = r.get(f"http://{__HOST_SERVIDOR}:8000/api/v2/decks/weol/start-week-date", params={"dataProduto":str(data_produto), "inicioSemana":str(data_inicio_semana)})
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



if __name__ == "__main__":
    organizar_info_eolica([
        '/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/prospec/libs/info_arquivos_externos/tmp/Estudo_21904_Entrada/DC202411-sem4/dadger.rv3',
        '/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/prospec/libs/info_arquivos_externos/tmp/Estudo_21904_Entrada/DC202411-sem5/dadger.rv4',
        '/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/prospec/libs/info_arquivos_externos/tmp/Estudo_21904_Entrada/DC202412-sem1/dadger.rv0'
        ], datetime.date(2024,11,16))
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
