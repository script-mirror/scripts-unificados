import os
import re
import sys
import pdb
import glob
import logging
import datetime
import warnings
import pandas as pd
from typing import List

warnings.simplefilter(action='ignore', category=FutureWarning)
sys.path.insert(1,"/WX2TB/Documentos/fontes/")

from PMO.scripts_unificados.apps.prospec.libs import utils
from PMO.scripts_unificados.apps.prospec.libs.decomp.dadger import dadger
from PMO.scripts_unificados.apps.prospec.libs.info_arquivos_externos import info_external_files


logging.basicConfig(level=logging.INFO,
                    format='%(levelname)s:\t%(asctime)s\t %(name)s.py:%(lineno)d\t %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers=[
                        logging.StreamHandler()
                    ])
logger = logging.getLogger(__name__)


def atualizar_cvu_DC(info_cvu,paths_to_modify):

    paths_modified=[]
    for path_dadger in paths_to_modify:

        df_dadger, comentarios = dadger.leituraArquivo(path_dadger)

        #LEITURA DO DECK 
        folder_name = os.path.basename(os.path.dirname(path_dadger))
        padrao_folder = r"DC(\d{4})(\d{2})-sem(\d{1})"

        match = re.match(padrao_folder, folder_name)
        if match:
            ano = match.group(1)
            mes = match.group(2)
            semana = match.group(3)
            dt_referente = f"{ano}{mes}"

        else:
            quit(f"Erro ao tentar extrair ano e mes do nome da pasta {folder_name}")

        if dt_referente not in info_cvu['mes_referencia'].unique():
           dt_referente = max(info_cvu['mes_referencia'])

        logger.info(f"\n\n\nModificando arquivo {path_dadger}")


        cvu_map = info_cvu.set_index(['mes_referencia','tipo_cvu']).loc[(dt_referente,'conjuntural')].set_index('cd_usina')['vl_cvu'].to_dict()

        df_dadger['CT'] = utils.trim_df(df_dadger['CT'])
        df_dadger['CT']['cod'] = df_dadger['CT']['cod'].str.strip().astype(int)

        for col in ["cvu_p1", "cvu_p2", "cvu_pat3"]:
            df_dadger['CT'][col].update(df_dadger['CT']["cod"].map(cvu_map))
        
        dadger.escrever_dadger(df_dadger, comentarios, path_dadger)

        paths_modified.append(path_dadger)

    return paths_modified

def atualizar_carga_DC(info_cargas,paths_to_modify):
    
    paths_modified=[]

    for path_dadger in paths_to_modify:

        #LEITURA DO DECK 
        folder_name = os.path.basename(os.path.dirname(path_dadger))
        padrao_folder = r"DC(\d{4})(\d{2})-sem(\d{1})"

        match = re.match(padrao_folder, folder_name)
        if match:
            ano = match.group(1)
            mes = match.group(2)
            semana = match.group(3)
            dt_referente = f"{ano}{mes}"

        else:
            quit(f"Erro ao tentar extrair ano e mes do nome da pasta {folder_name}")
        
        formatacao = '{:>2}  {:>2}   {:>2}   {:>1}    {:>10}{:>10}{:>10}{:>10}{:>10}{:>10}'
        
        if not info_cargas.get(dt_referente):
            continue
        
        if info_cargas[dt_referente].get(int(semana)- 1,pd.DataFrame()).empty:
            continue

        logger.info(f"\n\n\nModificando arquivo {path_dadger}")

        linhas_formatadas = info_cargas[dt_referente][int(semana) - 1].apply(
            lambda row: formatacao.format(*row), 
            axis=1
            ).tolist()

        linhas_formatadas = utils.adicionar_simbolo(
            linhas_formatadas,
            simbolos_sequenciais=['&'],
            intervalo_linhas=5
        )

        dadger.sobrescreve_bloco(
            path_to_modify=path_dadger,
            mnemonico_bloco='DP',
            values=linhas_formatadas,
            skip_lines=len(linhas_formatadas)
            )

        paths_modified.append(path_dadger)

    return paths_modified
            
def update_eolica_DC(paths_to_modify:List[str], data_produto:datetime.date):
    info_eolica = info_external_files.organizar_info_eolica(
        paths_to_modify,
        data_produto
        )
    for path_dadger in info_eolica:
        bloco = info_eolica[path_dadger].split('\n')
        try:
            dadger.sobrescreve_bloco(
                path_to_modify=path_dadger,
                mnemonico_bloco='PQ',
                values=bloco,
                skip_lines=len(bloco)
            )
            logger.info(f"Bloco {path_dadger} sobrescrito com sucesso")
        except Exception as e:
            logger.error(f"Erro ao tentar sobrescrever bloco {path_dadger}: {str(e)}")
            continue    

def update_restricoes_eletricas_DC(info_restricoes:pd.DataFrame,paths_to_modify:List[str]):

    colunas_primeiro_mes = ['1º Mês Pesada','1º Mês Média','1º Mês Leve']
    colunas_segundo_mes = ['2º Mês Pesada','2º Mês Média','2º Mês Leve']
    primeiro_mes, segundo_mes = sorted(info_restricoes.keys()) 
    codigos_restricoes = info_restricoes[primeiro_mes].index.tolist()

    for i, path_dadger in enumerate(paths_to_modify):

        #LEITURA DO DECK 
        folder_name = os.path.basename(os.path.dirname(path_dadger))
        padrao_folder = r"DC(\d{4})(\d{2})-sem(\d{1})"

        match = re.match(padrao_folder, folder_name)
        if match:
            ano = match.group(1)
            mes = match.group(2)
            dt_referente = f"{ano}{mes}"

        else:
            quit(f"Erro ao tentar extrair ano e mes do nome da pasta {folder_name}")

        df_dadger, comentarios = dadger.leituraArquivo(path_dadger)
        df_restricoes_re = df_dadger['RE'].set_index('id_restricao')
        df_restricoes_re.index = df_restricoes_re.index.str.strip().astype(int)
        df_dadger['LU']['id_restricao'] = df_dadger['LU']['id_restricao'].str.strip().astype(int)

        if info_restricoes.get(dt_referente,pd.DataFrame()).empty:
            continue

        for codigo in codigos_restricoes:
            estagio_final = int(df_restricoes_re.loc[codigo]['estag_final'].strip())
            flag_append_LU = True

            for index,row in df_dadger['LU'][df_dadger['LU']['id_restricao'] == codigo].iterrows(): 

                if primeiro_mes == dt_referente:

                    if int(row['est']) != estagio_final:
                        new_values = info_restricoes[dt_referente].loc[codigo][colunas_primeiro_mes].values.tolist()
                    else:
                        new_values = info_restricoes[dt_referente].loc[codigo][colunas_segundo_mes].values.tolist()
                        flag_append_LU = False
                
                elif segundo_mes==dt_referente:

                    if int(row['est']) != estagio_final:
                        if int(row['est']) == 1: 
                            copy_primeiro_estagio = df_dadger['LU'].loc[index, ['gmax_p1', 'gmax_p2', 'gmax_p3']].values.tolist()

                        new_values = info_restricoes[dt_referente].loc[codigo][colunas_segundo_mes].values.tolist()
                    else:
                        new_values = df_dadger['LU'].loc[index, ['gmax_p1', 'gmax_p2', 'gmax_p3']].values.tolist()
                        flag_append_LU = False

                df_dadger['LU'].loc[index, ['gmax_p1', 'gmax_p2', 'gmax_p3']] = new_values

            #caso a linha do ultimo estagio não esteja escrita no dadger
            if flag_append_LU:

                df_dadger['LU'] = pd.concat([df_dadger['LU'],df_dadger['LU'].loc[[index]]],ignore_index=True)

                if primeiro_mes == dt_referente:
                    new_values = info_restricoes[dt_referente].loc[codigo][colunas_segundo_mes].values.tolist()
                    df_dadger['LU'].loc[df_dadger['LU'].index[-1], ['est','gmax_p1', 'gmax_p2', 'gmax_p3']] = [estagio_final] + new_values
                
                elif segundo_mes==dt_referente:
                    new_values = copy_primeiro_estagio

                df_dadger['LU'].loc[df_dadger['LU'].index[-1], ['est','gmax_p1', 'gmax_p2', 'gmax_p3']] = [estagio_final] + new_values

        dadger.escrever_dadger(df_dadger, comentarios, path_dadger)

if __name__ == "__main__":

    # update_eolica_DC([
    #     '/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/prospec/libs/info_arquivos_externos/tmp/Estudo_21904_Entrada/DC202411-sem4/dadger.rv3',
    #     '/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/prospec/libs/info_arquivos_externos/tmp/Estudo_21904_Entrada/DC202411-sem5/dadger.rv4',
    #     '/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/prospec/libs/info_arquivos_externos/tmp/Estudo_21904_Entrada/DC202412-sem1/dadger.rv0'
    #     ], datetime.date(2025,11,16))
    # path_saida = "/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/prospec/libs/info_arquivos_externos/tmp"
    # #EXTRAINDO ZIP
    # file_estudo = "/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/verificadores/ccee/Estudo_21904_Entrada.zip"
    # extracted_zip_estudo = utils.extract_file_estudo(
    #     file_estudo,
    #     path_saida,
    #     )

    # ORGANIZA INFORMACOES DE CVU
    info_cvu = info_external_files.organizar_info_cvu(
        titles_cvu_ccee=[
            'custo_variavel_unitario_conjuntural_revisado',
            'custo_variavel_unitario_estrutural',
            'custo_variavel_unitario_conjuntural',
            'custo_variavel_unitario_merchant'
            ],
        )

    extracted_zip_estudo="C:/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/prospec/libs/info_arquivos_externos/tmp/Estudo_23188_Entrada"
    # #ALTERAR CVU EM DECKS DC
    paths_to_modify = glob.glob(os.path.join(extracted_zip_estudo,"**",f"*dadger*"),recursive=True)
    atualizar_cvu_DC(
        info_cvu,
        paths_to_modify
        )

    # #ORGANIZA INFORMACOES DE CARGA
    # path_carga_zip= "/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/verificadores/ccee/RV3_PMO_Novembro_2024_carga_semanal.zip"
    # info_cargas = info_external_files.organizar_info_carga(
    #     path_carga_zip,
    #     extracted_zip_estudo,
    #     path_saida
    #     )

    # #ALTERAR CARGA EM DECKS DC
    # paths_to_modify = glob.glob(os.path.join(extracted_zip_estudo,"**",f"*dadger*"),recursive=True)
    # atualizar_carga_DC(
    #     info_cargas,
    #     paths_to_modify
    # )
    
    # path_eolica_zip= "/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/verificadores/ccee/RV3_PMO_Novembro_2024_carga_semanal.zip"
    # paths_dadgers = glob.glob(os.path.join(extracted_zip_estudo,"**",f"*dadger*"),recursive=True)
    # pdb.set_trace()
    # info_eolica = info_external_files.organizar_info_eolica(
    #     paths_dadgers,
    #     path_saida
    #     )
    
    # atualizar_eolica_DC(
    #     info_eolica,
    #     paths_to_modify
    # )

    #RESTRICOES
    # file_path = "/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/webhook/arquivos/tmp/PRELIMINAR - RELATÓRIO MENSAL DE LIMITES DE INTERCÂMBIO/Preliminar - RT-ONS DPL 0037-2025_Limites PMO_Fevereiro-2025.pdf"
    # info_restricoes = info_external_files.read_table(file_path, "Tabela 4-1: Resultados dos Limites Elétricos")
    # update_restricoes_eletricas_DC(
    #     info_restricoes=info_restricoes,
    #     paths_to_modify=["/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/prospec/libs/info_arquivos_externos/tmp/Estudo_22971/DC202502-sem1/dadger.rv0"]
    #     )