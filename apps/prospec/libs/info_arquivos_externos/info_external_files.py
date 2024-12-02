import os
import re
import sys
import pdb
import datetime
import glob
import pandas as pd
import csv

sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.bibliotecas import rz_dir_tools,wx_opweek
from PMO.scripts_unificados.apps.verificadores.ccee import rz_download_cvu
from PMO.scripts_unificados.apps.prospec.libs.decomp.dadger import dadger
from PMO.scripts_unificados.apps.prospec.libs import utils
from typing import List

def organizar_info_cvu(ano_referencia, mes_referencia,path_saida):

    info_cvu={}

    extracted_files = rz_download_cvu.download_cvu_acervo_ccee(ano_referencia, mes_referencia, path_saida)

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

    return info_cvu


def extract_carga_decomp_ons(path_carga,path_saida):
        DIR_TOOLS = rz_dir_tools.DirTools()

    
        extracted_zip_carga = DIR_TOOLS.extract_specific_files_from_zip(
                path=path_carga,
                files_name_template=["*CargaDecomp*.txt"],
                dst=path_saida,
                extracted_files=[]
                )  
        return extracted_zip_carga


def organizar_info_carga(path_carga_zip,path_deck,path_saida):

    carga_decomp_txt = extract_carga_decomp_ons(
        path_carga_zip,
        path_saida
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

def ler_csv_para_dicionario(caminho_csv):
    with open(caminho_csv, mode='r') as file:
        reader = csv.reader(file, delimiter=';')
        headers = next(reader)[1:]
        data_dict = {}
        for row in reader:
            regiao = row[0]
            valores = row[1:]
            for i in range(0, len(headers), 3):
                data_key = f"{headers[i]} {headers[i+2]}"
                if data_key not in data_dict:
                    data_dict[data_key] = {}
                data_dict[data_key][regiao] = {
                    "pesado": valores[i],
                    "medio": valores[i+1],
                    "leve": valores[i+2]
                }
        return data_dict

def organizar_info_eolica(paths_dadgers:List[str], path_saida:str):
    
    # carga_decomp_txt = extract_carga_decomp_ons(
    #     path_carga_zip,
    #     path_saida
    # )
    
    # df_dadger_novo, comentarios = dadger.leituraArquivo(carga_decomp_txt[0])
    
    inicio_rv_atual = utils.get_rv_atual_from_dadger(paths_dadgers[0])
    df_dadger_novo, comentarios = dadger.leituraArquivo(paths_dadgers[0])
    data = datetime.date(int(df_dadger_novo['DT']['ano'][0]), int(df_dadger_novo['DT']['mes'][0]), int(df_dadger_novo['DT']['dia'][0]))

    semana_eletrica_atual = wx_opweek.ElecData(inicio_rv_atual.date())
    semana_eletrica = wx_opweek.ElecData(inicio_rv_atual.date())
        
    bloco_dp:pd.DataFrame = df_dadger_novo['DP']
    bloco_dp['ip'] = bloco_dp['ip'].astype(int)
    
    nome_arquivo_zip = os.path.basename(path_carga_zip).split('.')[0]
    inicio_rv_atual = utils.get_rv_atual_from_zip(nome_arquivo_zip)
    
    semana_eletrica_atual = wx_opweek.ElecData(inicio_rv_atual.date())
    semana_eletrica = wx_opweek.ElecData(inicio_rv_atual.date())
    semanas_a_remover = 0
    
    novo_bloco = {}
    for path_dadger in paths_dadgers:

        if semana_eletrica.atualRevisao == 0 and semana_eletrica_atual.primeiroDiaMes != semana_eletrica.primeiroDiaMes:
            if semana_eletrica.primeiroDiaMes.day != 1:

                df_bloco_atual, comentarios_bloco_atual = dadger.leituraArquivo(
                    glob.glob(
                        path_dadger
                    )
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


if __name__ == "__main__":
    ler_csv_para_dicionario("tmp/Prev_20241128_Semanas_20241130_20250103.csv")
