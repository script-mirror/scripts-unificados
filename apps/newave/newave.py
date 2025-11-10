import os
import pdb
import sys
import shutil
import datetime
from dateutil.relativedelta import relativedelta
import numpy as np
import pandas as pd
from dateutil.relativedelta import *
import matplotlib.pyplot as plt

path_home =  os.path.expanduser("~")
path_app = os.path.dirname(os.path.realpath(__file__))
sys.path.insert("/WX2TB/Documentos/fontes/PMO/scripts_unificados/")
from bibliotecas import wx_opweek
from apps.newave.libs import wx_dger,wx_sistema,wx_expt,wx_manutt,wx_term,wx_c_adic,wx_confhd,wx_curva,wx_agrint,wx_desvagua,wx_ghmin,wx_modif,wx_patamar,wx_penalid

from apps.newave.libs import wx_restricao_eletrica
from apps.newave.libs import wx_volume_referencia

def montar_decks_base(data_inicio_deck, path_arquivo_base, caminho_saida, IGP_DI):
    
    if os.path.exists(caminho_saida):
        shutil.rmtree(caminho_saida)
    os.makedirs(caminho_saida)

    anoFinalBase = data_inicio_deck.year + 3
    anoInicioBase = data_inicio_deck.year - 1
    anoFinalNovoDeck = data_inicio_deck.year + 4

    path_dger_base = os.path.join(path_arquivo_base, 'DGER.DAT')
    wx_dger.gerar_dger(path_dger_base, caminho_saida, data_inicio_deck)

    path_sistema_base = os.path.join(path_arquivo_base, 'SISTEMA.DAT')
    wx_sistema.gerar_sistema(path_sistema_base, caminho_saida, data_inicio_deck, IGP_DI)

    gerar_arquivos_termica(path_arquivo_base, data_inicio_deck, caminho_saida)

    ##### AGRINT
    path_agrint_base = os.path.join(path_arquivo_base, 'AGRINT.DAT')
    path_agrint_saida = os.path.join(caminho_saida, 'AGRINT.DAT')
    comentarios, df_agrint = wx_agrint.leituraArquivo(path_agrint_base)

    df_agrint_novo = pd.DataFrame(columns=df_agrint['limites_agrupamento'].columns)


    for index, df_group in df_agrint['limites_agrupamento'].groupby(['ag']).__iter__():
        df_agrint_novo_ano = pd.DataFrame(columns=df_agrint['limites_agrupamento'].columns)
        for i_limite, limite in df_group.iterrows():

            if limite['ag'].strip() != '14':
                df_agrint_novo = df_agrint_novo.append(limite.copy())

            else:
                limite_copy =  limite.copy()

                anoFinal = (limite['anof']).strip()
                mesFinal = (limite['mf']).strip()
                if anoFinal == '':
                    anoFinal = anoFinalBase
                    mesFinal = 12
                anoFinal = int(anoFinal)
                mesFinal = int(mesFinal)
                anoInicial = int((limite['anoi']).strip())
                mesInicial = int((limite['mi']).strip())

                if anoFinal == anoFinalBase:
                    if anoInicial < anoFinalBase and mesFinal != 12:
                        limite_copy['mi'] = 1
                        limite_copy['anoi'] = anoFinalNovoDeck
                        limite_copy['anof'] = anoFinalNovoDeck
                    elif anoInicial == anoFinalBase:
                        limite_copy['anoi'] = anoFinalNovoDeck
                        if mesFinal != 12:
                            limite_copy['anof'] = anoFinalNovoDeck
                            limite_copy['mf'] = mesFinal
                        else:
                            limite_copy['anof'] = ''
                            limite_copy['mf'] = ''
                        limite['anof'] = anoFinalBase
                        limite['mf'] = mesFinal
                    else:
                        print('condicao else')

                    df_agrint_novo_ano = df_agrint_novo_ano.append(limite_copy)

                df_agrint_novo = df_agrint_novo.append(limite.copy())
    df_agrint['limites_agrupamento'] = df_agrint_novo.append(df_agrint_novo_ano, ignore_index=True)

    wx_agrint.escrever_arquivo(df_agrint, comentarios, path_agrint_saida)
    ################

    ##### C_ADIC
    path_cadic_base = os.path.join(path_arquivo_base, 'C_ADIC.DAT')
    path_cadic_saida = os.path.join(caminho_saida, 'C_ADIC.DAT')
    comentarios, blocos = wx_c_adic.leituraArquivo(path_cadic_base)

    anoInicialBase = data_inicio_deck.year - 1
    anoFinalBase = data_inicio_deck.year + 3
    anoFinalNovoDeck = data_inicio_deck.year + 4

    df_novas_cargas= pd.DataFrame(columns=blocos['carga_adicional_mensal'].columns)

    nome_cols = ['carg_jan', 'carg_fev', 'carg_mar', 'carg_abr', 'carg_mai', 'carg_jun', 
                'carg_jul', 'carg_ago', 'carg_set', 'carg_out', 'carg_nov', 'carg_dez']

    for i_cargaAdd, cargaAdd in blocos['carga_adicional_mensal'].iterrows():

        anoRef = cargaAdd['ano'].strip()
        try:
            anoRef = int(anoRef)
        except:
            pass

        if anoRef == anoInicialBase:
            continue
        elif anoRef == anoFinalBase:
            df_novas_cargas = df_novas_cargas.append(cargaAdd.copy())
            
            carga_penultimo_ano = blocos['carga_adicional_mensal'].iloc[i_cargaAdd-1]
            carga_ultimo_ano = cargaAdd.copy()
            nova_carga = cargaAdd.copy()

            nova_carga[nome_cols] = carga_ultimo_ano[nome_cols].astype(float)*(carga_ultimo_ano[nome_cols].astype(float)/carga_penultimo_ano[nome_cols].astype(float))
            nova_carga[nome_cols] = nova_carga[nome_cols].apply(lambda x: '{:6.0f}.'.format(x))

            nova_carga['ano'] = anoFinalNovoDeck
            df_novas_cargas = df_novas_cargas.append(nova_carga)
        else:
            df_novas_cargas = df_novas_cargas.append(cargaAdd.copy())

    blocos['carga_adicional_mensal'] = df_novas_cargas.reset_index(drop=True)
    wx_c_adic.escrever_arquivo(blocos, comentarios, path_cadic_saida)
    ################
    

    ##### CONFHD
    path_curva_base = os.path.join(path_arquivo_base, 'CURVA.DAT')
    path_curva_saida = os.path.join(caminho_saida, 'CURVA.DAT')
    comentarios, blocos = wx_curva.leituraArquivo(path_curva_base)
    blocos['valor_penalizacao']['penalidade'] = blocos['valor_penalizacao']['penalidade'].astype(float) *(1+IGP_DI/100)
    df_nova_curva_seguranca= pd.DataFrame(columns=blocos['curva_seguranca'].columns)
    
    for i_row, row in blocos['curva_seguranca'].iterrows():
        if int(row['ano']) == anoInicioBase:
            continue
        else:
            df_nova_curva_seguranca = df_nova_curva_seguranca.append(row.copy())
            if int(row['ano']) == anoFinalBase:
                row['ano'] = anoFinalNovoDeck
                df_nova_curva_seguranca = df_nova_curva_seguranca.append(row.copy())
    blocos['curva_seguranca'] = df_nova_curva_seguranca.reset_index(drop=True)
    wx_curva.escrever_arquivo(blocos, comentarios, path_curva_saida)
    ################


    ##### DSVAGUA
    path_dsvagua_base = os.path.join(path_arquivo_base, 'DSVAGUA.DAT')
    path_dsvagua_saida = os.path.join(caminho_saida, 'DSVAGUA.DAT')
    comentarios, blocos = wx_desvagua.leituraArquivo(path_dsvagua_base)
    df_novo_desvio= pd.DataFrame(columns=blocos['devios'].columns)
    
    proximoComentario = None 
    for i_row, row in blocos['devios'].iterrows():
        if int(row['ano']) == anoInicioBase:
            proximoComentario = row['coment']
            continue
        else:
            if proximoComentario:
                row['coment'] = proximoComentario
                proximoComentario = None 
            df_novo_desvio = df_novo_desvio.append(row.copy())
            if int(row['ano']) == anoFinalBase:
                row['ano'] = anoFinalNovoDeck
                df_novo_desvio = df_novo_desvio.append(row.copy())
    blocos['devios'] = df_novo_desvio.reset_index(drop=True)
    wx_desvagua.escrever_arquivo(blocos, comentarios, path_dsvagua_saida)
    ################

    ### GHMIN
    path_ghmin_base = os.path.join(path_arquivo_base, 'GHMIN.DAT')
    path_ghmin_saida = os.path.join(caminho_saida, 'GHMIN.DAT')
    comentarios, blocos = wx_ghmin.leituraArquivo(path_ghmin_base)
    df_novo_ghmin= pd.DataFrame(columns=blocos['geracao_min'].columns)
    
    row_anterior = None

    df_novo_geracao_min = pd.DataFrame(columns=blocos['geracao_min'].columns)
    for index, df_group in blocos['geracao_min'].groupby(['uh']).__iter__():
        df_novo_geracao_min_uh = pd.DataFrame(columns=blocos['geracao_min'].columns)
        flag_inserido = False
        for i_row, row in df_group.iterrows():
            if row['ano'].strip() != 'POS':
                if int(row['ano']) >= anoFinalBase:
                    if int(row['me'])!=1 and df_novo_geracao_min_uh.shape[0] == 0:
                        row_anterior['ano'] = anoFinalNovoDeck
                        row_anterior['me'] = 1
                        df_novo_geracao_min_uh = df_novo_geracao_min_uh.append(row_anterior)
                    new_row = row.copy()
                    new_row['ano'] = anoFinalNovoDeck
                    df_novo_geracao_min_uh = df_novo_geracao_min_uh.append(new_row)
                else:
                    row_anterior = row.copy()
            elif flag_inserido == False:
                df_novo_geracao_min = df_novo_geracao_min.append(df_novo_geracao_min_uh).reset_index(drop=True)
                flag_inserido = True
            
            df_novo_geracao_min = df_novo_geracao_min.append(row.copy())

    blocos['geracao_min'] = df_novo_geracao_min
    wx_ghmin.escrever_arquivo(blocos, comentarios, path_ghmin_saida)
    ################


    ### MODIF
    path_modif_base = os.path.join(path_arquivo_base, 'MODIF.DAT')
    path_modif_saida = os.path.join(caminho_saida, 'MODIF.DAT')
    comentarios, blocos = wx_modif.leituraArquivo(path_modif_base)      
    for usina in blocos:
        for nome_bloco in blocos[usina]:
            if nome_bloco == 'vmint':
                if int(blocos[usina][nome_bloco].iloc[0]['mes']) != 1 and int(blocos[usina][nome_bloco]['ano'].iloc[-1]) == anoInicioBase:
                    blocos[usina][nome_bloco]['mes'] = 1
                    print('[warming] vmint nao inicia no mes 1!')
                blocos[usina][nome_bloco]['ano'] = blocos[usina][nome_bloco]['ano'].astype(int) + 1

            if 'mes' in blocos[usina][nome_bloco].columns:
                novo_bloco = blocos[usina][nome_bloco][blocos[usina][nome_bloco]['ano'].astype(int)==anoFinalBase].copy()
                if novo_bloco.shape[0]>=1:
                    novo_bloco['ano'] = anoFinalNovoDeck
                    if int(novo_bloco.iloc[0]['mes']) != 1:
                        idx = novo_bloco.iloc[0].name
                        inicio_ano_final = blocos[usina][nome_bloco].loc[idx-1].copy()
                        inicio_ano_final['mes'] = 1
                        inicio_ano_final['ano'] = anoFinalNovoDeck
                        novo_bloco = pd.concat([pd.DataFrame([inicio_ano_final]),novo_bloco])
                blocos[usina][nome_bloco] = pd.concat([blocos[usina][nome_bloco], novo_bloco]).reset_index(drop=True)

    wx_modif.escrever_arquivo(blocos, comentarios, path_modif_saida)
    ################



    ### PATAMAR
    path_patamar_base = os.path.join(path_arquivo_base, 'PATAMAR.DAT')
    path_patamar_saida = os.path.join(caminho_saida, 'PATAMAR.DAT')
    comentarios, blocos = wx_patamar.leituraArquivo(path_patamar_base)
    
    for nome_bloco in blocos:
        novo_bloco = pd.DataFrame(columns=blocos[nome_bloco].columns)
        for index, row in blocos[nome_bloco].iterrows():
            if row['ano'].strip() != '':
                ano_ref = int(row['ano'])
                pat = 1
            else:
                pat += 1

            if ano_ref == anoInicioBase:
                continue

            novo_bloco = novo_bloco.append(row)
            if ano_ref == anoFinalBase:
                new_line = row.copy()
                if pat == 1:
                    ano_adicional = pd.DataFrame(columns=blocos[nome_bloco].columns)
                    new_line['ano'] = anoFinalNovoDeck
                ano_adicional = ano_adicional.append(new_line)
                
                if pat == 3:
                    novo_bloco = pd.concat([novo_bloco, ano_adicional]).reset_index(drop=True)
        blocos[nome_bloco] = novo_bloco
    wx_patamar.escrever_arquivo(blocos, comentarios, path_patamar_saida)
    ################

    ### PATAMAR
    path_penalid_base = os.path.join(path_arquivo_base, 'PENALID.DAT')
    path_penalid_saida = os.path.join(caminho_saida, 'PENALID.DAT')
    comentarios, blocos = wx_penalid.leituraArquivo(path_penalid_base)
    blocos['penalidades']['penalid1'] = blocos['penalidades']['penalid1'].astype(float)
    filtro1 = blocos['penalidades']['pchave']  == 'DESVIO'
    blocos['penalidades'].loc[filtro1,'penalid1'] = blocos['penalidades'].loc[filtro1,'penalid1'] * (1+IGP_DI/100)
    wx_penalid.escrever_arquivo(blocos, comentarios, path_penalid_saida)
    ################
    
    
    ### VOLUME REFERENCIA
    path_volumeRef = os.path.join(path_arquivo_base, 'VOLUMES-REFERENCIA.CSV')
    path_volumeRef_saida = os.path.join(caminho_saida, 'VOLUMES-REFERENCIA.CSV')
    wx_volume_referencia.gerarvolumeReferencia(path_volumeRef, data_inicio_deck, path_volumeRef_saida)
    ################

    ### RESTRICAO ELETRICA
    dtFim = data_inicio_deck + relativedelta(months=12)
    path_restricaoEletrica = os.path.join(path_arquivo_base, 'RESTRICAO-ELETRICA.CSV')
    path_restricaoEletrica_saida = os.path.join(caminho_saida, 'RESTRICAO-ELETRICA.CSV')
    wx_restricao_eletrica.gerarProximosMeses(path_restricaoEletrica, data_inicio_deck, dtFim, path_restricaoEletrica_saida)
    ################
    
    decks = [
        {
            "dataInicio": data_inicio_deck,
            "pathDeck": path_arquivo_base,
        },
        {
            "dataInicio": anoInicioBase,
            "pathDeck": caminho_saida,
        }
    ]
    compararExpt(decks)


def get_peso_manutencoes(dt_inicio, duracao):

    peso = {}
    dt_aux = dt_inicio + datetime.timedelta(days = duracao - 1)
    while duracao > 0:
        mes = dt_aux.replace(day=1)
        # num_dias_mes = ((mes + relativedelta(months=+1)) - mes).days
        num_dias_mes = 30
        if duracao >= dt_aux.day:

            peso[mes] = dt_aux.day/num_dias_mes
            duracao = duracao - dt_aux.day
            dt_aux = dt_aux - datetime.timedelta(days = dt_aux.day)
        # elif duracao == dt_aux.day:
        #     dt_aux = dt_aux - datetime.timedelta(days = dt_aux.day)
        else:
            peso[mes] = duracao/num_dias_mes
            dt_aux = dt_aux - datetime.timedelta(days = duracao)
            duracao = duracao - duracao
    
    return peso


def gerar_arquivos_termica(path_deck_base, data_inicio_deck, caminho_saida):
    
    dt_final = data_inicio_deck + relativedelta(months=5*12-1)
    
    filePath_term = os.path.join(path_deck_base, 'TERM.DAT')
    fileSaida_term = os.path.join(caminho_saida, 'term.dat')
    comentarios_term, arq_term = wx_term.leituraArquivo(filePath_term)
    # arq_term = pd.DataFrame(bloco, columns=wx_term.info_bloco['campos'])
    # wx_term.escrever_arquivo(arq_term, comentarios, fileSaida_term)

    filepath_expt = os.path.join(path_deck_base, 'EXPT.DAT')
    filesaida_expt = os.path.join(caminho_saida, 'expt.dat')
    comentarios_expt, arq_expt = wx_expt.leituraArquivo(filepath_expt)
    # arq_expt = pd.DataFrame(bloco, columns=wx_expt.info_bloco['campos'])
    # wx_expt.escrever_arquivo(arq_expt, comentarios, filesaida_expt)
    
    filepath_manutt = os.path.join(path_deck_base, 'MANUTT.DAT')
    filesaida_manutt = os.path.join(caminho_saida, 'manutt.dat')
    comentarios_manutt, arq_manutt = wx_manutt.leituraArquivo(filepath_manutt)
    # arq_manutt = pd.DataFrame(bloco, columns=wx_manutt.info_bloco['campos'])

    df_term_saida = {}
    
    dt = data_inicio_deck
    idx = pd.date_range(start=dt, end=dt_final, freq='MS')
    
    for tipo in ['POTEF','FCMAX','TEIFT','GTMIN','IPTER']:
        df_term_saida[tipo] = pd.DataFrame(index=idx)

    df_manutencao_formatada = pd.DataFrame(index=idx)
    # inicia com os valores do arquivo term.dat
    for index, row in arq_term.iterrows():

        cd_usin = row['cod_usina'].strip()
        df_term_saida['POTEF'][cd_usin] = row['capacidade'].strip()
        df_term_saida['FCMAX'][cd_usin] = row['fator_capacidade_max'].strip()
        df_term_saida['TEIFT'][cd_usin] = row['teif'].strip()
        df_term_saida['IPTER'][cd_usin] = row['ip'].strip()
        df_term_saida['GTMIN'][cd_usin] = row['gmin_horizonte'].strip()
            
        
        if float(row['gmin_horizonte']) != 0:
            df_term_saida['GTMIN'].loc[data_inicio_deck+relativedelta(months=+0), cd_usin] = row['gmin_jan'].strip()
            df_term_saida['GTMIN'].loc[data_inicio_deck+relativedelta(months=+1), cd_usin] = row['gmin_fev'].strip()
            df_term_saida['GTMIN'].loc[data_inicio_deck+relativedelta(months=+2), cd_usin] = row['gmin_mar'].strip()
            df_term_saida['GTMIN'].loc[data_inicio_deck+relativedelta(months=+3), cd_usin] = row['gmin_abr'].strip()
            df_term_saida['GTMIN'].loc[data_inicio_deck+relativedelta(months=+4), cd_usin] = row['gmin_mai'].strip()
            df_term_saida['GTMIN'].loc[data_inicio_deck+relativedelta(months=+5), cd_usin] = row['gmin_jun'].strip()
            df_term_saida['GTMIN'].loc[data_inicio_deck+relativedelta(months=+6), cd_usin] = row['gmin_jul'].strip()
            df_term_saida['GTMIN'].loc[data_inicio_deck+relativedelta(months=+7), cd_usin] = row['gmin_ago'].strip()
            df_term_saida['GTMIN'].loc[data_inicio_deck+relativedelta(months=+8), cd_usin] = row['gmin_set'].strip()
            df_term_saida['GTMIN'].loc[data_inicio_deck+relativedelta(months=+9), cd_usin] = row['gmin_out'].strip()
            df_term_saida['GTMIN'].loc[data_inicio_deck+relativedelta(months=+10), cd_usin] = row['gmin_nov'].strip()
            df_term_saida['GTMIN'].loc[data_inicio_deck+relativedelta(months=+11), cd_usin] = row['gmin_dez'].strip()
            
        df_manutencao_formatada[cd_usin] = 0

    # atualiza com os valores do expt.dat
    for cod_usin, expt_usin in arq_expt.groupby('cod_usina'):
        for index, row in expt_usin.iterrows():
            cd_usim = row['cod_usina'].strip()
            tipo = row['tipo'].strip()
            val = row['modif']
            mi = str(row['mi']).strip()
            anoi = str(row['anoi']).strip()
            mf = str(row['mf']).strip()
            anof = str(row['anof']).strip()
            
            if mf == '' and anof == '':
                mf = dt_final.month
                anof = dt_final.year
            
            dt = datetime.datetime(int(anoi), int(mi), 1)
            dt_f = datetime.datetime(int(anof), int(mf), 1)
            
            df_term_saida[tipo][cd_usim].loc[dt:dt_f] = float(val)
            if tipo == 'POTEF':
                df_term_saida[tipo][cd_usim].loc[dt_f+relativedelta(months=1):] = 0

    # repetindo a sazo para o ultimo ano nos postos que possuem FCMAX maior que 0 para o novo ano inserido
    postos_fcmax_maior_zero_dez = df_term_saida['FCMAX'].columns[df_term_saida['FCMAX'].loc[dt_final] != 0].tolist()
    for tipo in ['POTEF','FCMAX','TEIFT','GTMIN','IPTER']:
        idx_ultimo_ano_deck_base = df_term_saida[tipo].index.year == dt_final.year-1
        idx_ultimo_ano_deck_novo = df_term_saida[tipo].index.year == dt_final.year
        df_term_saida[tipo].loc[idx_ultimo_ano_deck_novo, postos_fcmax_maior_zero_dez] = df_term_saida[tipo].loc[idx_ultimo_ano_deck_base, postos_fcmax_maior_zero_dez].values
    
    # atualiza com o manutt
    for index, row in arq_manutt.iterrows():
        cod_usin = row['cod_usina'].strip()
        potencia_manutencao = float(row['potencia'].strip())
        dt_inicio_manutencao_str = row['dt_inicial'].strip()
        duracao_manutencao = int(row['duracao_dias'].strip())
        
        dt_inicio_manutencao = datetime.datetime.strptime(
            dt_inicio_manutencao_str, '%d%m%Y')
        
        dt_inicio_manutencao = dt_inicio_manutencao + relativedelta(years=+1)
        pesos = get_peso_manutencoes(dt_inicio_manutencao, duracao_manutencao)
        
        for mes in pesos:
            potencia_max_mes = float(df_term_saida['POTEF'].loc[mes,cod_usin])
            df_manutencao_formatada.loc[mes,cod_usin] += potencia_manutencao*pesos[mes]
            
            df_term_saida['TEIFT'].loc[mes,cod_usin] = 0
            df_term_saida['IPTER'].loc[mes,cod_usin] = 0
    
    df_term_saida['GTMIN'] = df_term_saida['GTMIN'].apply(pd.to_numeric)
    df_pdisp = ((100-df_term_saida['TEIFT'].apply(pd.to_numeric)-df_term_saida['IPTER'].apply(pd.to_numeric))/100)*df_term_saida['POTEF'].apply(pd.to_numeric)*df_term_saida['FCMAX'].apply(pd.to_numeric)/100 - df_manutencao_formatada
    diff = df_pdisp - df_term_saida['GTMIN'] < 0    
    # df_pdisp['65'] - df_term_saida['GTMIN']['65'].apply(pd.to_numeric)
    df_term_saida['GTMIN'][diff] = df_pdisp[diff]
    
    nomes_usinas = arq_expt[['cod_usina','usina']].copy()
    nomes_usinas.replace(r'^\s*$', np.nan, regex=True,inplace=True)
    nomes_usinas['cod_usina'] = nomes_usinas['cod_usina'].astype(int)
    nomes_usinas.set_index('cod_usina',inplace=True)
    nomes_usinas = nomes_usinas.dropna().to_dict()
    nomes_usinas = nomes_usinas['usina']
    
    lez = df_term_saida['GTMIN'] < 0
    df_term_saida['GTMIN'][lez] = 0
    
    arq_expt_out = pd.DataFrame(columns=wx_expt.info_bloco['campos'])
    for cod_usin in df_manutencao_formatada.columns:
        for tipo in ['POTEF','FCMAX','TEIFT','GTMIN','IPTER']:
            nomeUsin = ''
            if tipo == 'POTEF':
                nomeUsin = nomes_usinas[int(cod_usin)]
            ultimo_valor = -9999
            index = []
            for idx, val in df_term_saida[tipo][cod_usin].iteritems():
                if val != ultimo_valor:
                    index.append(idx)
                    ultimo_valor = val
            linhas_tipo = df_term_saida[tipo].loc[index,cod_usin]
            num_linhas_usina_tipo = 0
            for dt_ref, val in linhas_tipo.iteritems():
                num_linhas_usina_tipo +=1
                if num_linhas_usina_tipo == linhas_tipo.shape[0]:
                    arq_expt_out = arq_expt_out.append({'cod_usina':cod_usin, 'tipo':tipo, 'modif':f'{float(val):.2f}', 'mi':dt_ref.month, 'anoi':dt_ref.year, 'mf':'', 'anof':'', 'usina':nomeUsin}, ignore_index=True)
                else:
                    dt_final_manut = linhas_tipo.index[num_linhas_usina_tipo] + relativedelta(months=-1)
                    arq_expt_out = arq_expt_out.append({'cod_usina':cod_usin, 'tipo':tipo, 'modif':f'{float(val):.2f}', 'mi':dt_ref.month, 'anoi':dt_ref.year, 'mf':dt_final_manut.month, 'anof':dt_final_manut.year, 'usina':nomeUsin}, ignore_index=True)
    wx_expt.escrever_arquivo(arq_expt_out, comentarios_expt, filesaida_expt)
    
    arq_term['gmin_jan'] = f'{float(val):.2f}'
    arq_term['gmin_fev'] = f'{float(val):.2f}'
    arq_term['gmin_mar'] = f'{float(val):.2f}'
    arq_term['gmin_abr'] = f'{float(val):.2f}'
    arq_term['gmin_mai'] = f'{float(val):.2f}'
    arq_term['gmin_jun'] = f'{float(val):.2f}'
    arq_term['gmin_jul'] = f'{float(val):.2f}'
    arq_term['gmin_ago'] = f'{float(val):.2f}'
    arq_term['gmin_set'] = f'{float(val):.2f}'
    arq_term['gmin_out'] = f'{float(val):.2f}'
    arq_term['gmin_nov'] = f'{float(val):.2f}'
    arq_term['gmin_dez'] = f'{float(val):.2f}'
    arq_term['gmin_horizonte'] = f'{float(val):.2f}'
    wx_term.escrever_arquivo(arq_term, comentarios_term, fileSaida_term)
    

    arq_manutt['dt_inicial'] = arq_manutt['dt_inicial'].str.replace(str(data_inicio_deck.year-1), str(data_inicio_deck.year))
    wx_manutt.escrever_arquivo(arq_manutt, comentarios_manutt, filesaida_manutt)
    
def compararExpt(decks:list, gerarXlsx:bool=False):
    
    if gerarXlsx:
        writer = pd.ExcelWriter('dados.xlsx')
    
    for deck in decks:
        df_term_saida = {}

        pathDeck = deck['pathDeck']
        dtInicial = deck['dataInicio']
        dtFinal = dtInicial + relativedelta(months=5*12-1)
        
        termPath = os.path.join(pathDeck, 'TERM.DAT')
        exptPath = os.path.join(pathDeck, 'EXPT.DAT')
        
        comentarios_term, arq_term = wx_term.leituraArquivo(termPath)
        comentarios_expt, arq_expt = wx_expt.leituraArquivo(exptPath)
        
        idx = pd.date_range(start=dtInicial, end=dtFinal, freq='MS')
        for tipo in ['POTEF','GTMIN']:
            df_term_saida[tipo] = pd.DataFrame(index=idx)
        
        for index, row in arq_term.iterrows():

            cd_usin = row['cod_usina'].strip()
            df_term_saida['POTEF'][cd_usin] = row['capacidade'].strip()
            df_term_saida['GTMIN'][cd_usin] = row['gmin_horizonte'].strip()
            
            if float(row['gmin_horizonte']) != 0:
                df_term_saida['GTMIN'].loc[dtInicial+relativedelta(months=+0), cd_usin] = row['gmin_jan'].strip()
                df_term_saida['GTMIN'].loc[dtInicial+relativedelta(months=+1), cd_usin] = row['gmin_fev'].strip()
                df_term_saida['GTMIN'].loc[dtInicial+relativedelta(months=+2), cd_usin] = row['gmin_mar'].strip()
                df_term_saida['GTMIN'].loc[dtInicial+relativedelta(months=+3), cd_usin] = row['gmin_abr'].strip()
                df_term_saida['GTMIN'].loc[dtInicial+relativedelta(months=+4), cd_usin] = row['gmin_mai'].strip()
                df_term_saida['GTMIN'].loc[dtInicial+relativedelta(months=+5), cd_usin] = row['gmin_jun'].strip()
                df_term_saida['GTMIN'].loc[dtInicial+relativedelta(months=+6), cd_usin] = row['gmin_jul'].strip()
                df_term_saida['GTMIN'].loc[dtInicial+relativedelta(months=+7), cd_usin] = row['gmin_ago'].strip()
                df_term_saida['GTMIN'].loc[dtInicial+relativedelta(months=+8), cd_usin] = row['gmin_set'].strip()
                df_term_saida['GTMIN'].loc[dtInicial+relativedelta(months=+9), cd_usin] = row['gmin_out'].strip()
                df_term_saida['GTMIN'].loc[dtInicial+relativedelta(months=+10), cd_usin] = row['gmin_nov'].strip()
                df_term_saida['GTMIN'].loc[dtInicial+relativedelta(months=+11), cd_usin] = row['gmin_dez'].strip()
        
        # atualiza com os valores do expt.dat
        for cod_usin, expt_usin in arq_expt.groupby('cod_usina'):
            for index, row in expt_usin.iterrows():
                cd_usim = row['cod_usina'].strip()
                tipo = row['tipo'].strip()
                
                if tipo not in ['POTEF','GTMIN']:
                    continue
                
                val = row['modif']
                mi = str(row['mi']).strip()
                anoi = str(row['anoi']).strip()
                mf = str(row['mf']).strip()
                anof = str(row['anof']).strip()
                
                if mf == '' and anof == '':
                    mf = dtFinal.month
                    anof = dtFinal.year
                
                dt = datetime.datetime(int(anoi), int(mi), 1)
                dt_f = datetime.datetime(int(anof), int(mf), 1)
                
                df_term_saida[tipo][cd_usim].loc[dt:dt_f] = float(val)
                if tipo == 'POTEF':
                    df_term_saida[tipo][cd_usim].loc[dt_f+relativedelta(months=1):] = 0

        df_term_saida['POTEF'] = df_term_saida['POTEF'].astype(float)
        df_term_saida['GTMIN'] = df_term_saida['GTMIN'].astype(float)
        
        if gerarXlsx:
            df_term_saida['POTEF'].to_excel(writer, sheet_name=f"POTEF-{dtInicial.year}")
            df_term_saida['GTMIN'].to_excel(writer, sheet_name=f"GTMIN-{dtInicial.year}")
        
        plt.plot(df_term_saida['POTEF'].astype(float).sum(axis=1), label=f"POTEF-{dtInicial.year}")
        plt.plot(df_term_saida['GTMIN'].astype(float).sum(axis=1), label=f"GTMIN-{dtInicial.year}")
    plt.legend()
    plt.show()
    
    if gerarXlsx:
        writer.save()
    
def testUnitarios(path_deck_base):
    
    # TODO
    # path_dger_base = os.path.join(path_deck_base, 'DGER.DAT')
    
    # path_expt_base = os.path.join(path_deck_base, 'EXPT.DAT')
    # comentarios, bloco = wx_expt.leituraArquivo(path_expt_base)    
    # wx_expt.escrever_arquivo(arq_expt, comentarios, path_expt_base.replace('EXPT.DAT','EXPT_.DAT'))
    
    # path_sistema_base = os.path.join(path_deck_base, 'SISTEMA.DAT')
    # comentarios, df_sistema = wx_sistema.leituraArquivo(path_sistema_base)
    # wx_sistema.escrever_arquivo(df_sistema, comentarios, path_sistema_base.replace('SISTEMA.DAT','SISTEMA_.DAT'))
    
    # path_penalid_base = os.path.join(path_deck_base, 'PENALID.DAT')
    # comentarios, df_penalid = wx_penalid.leituraArquivo(path_penalid_base)
    # wx_penalid.escrever_arquivo(df_penalid, comentarios, path_penalid_base.replace('PENALID.DAT','PENALID_.DAT'))
    
    pdb.set_trace()
    
    path_agrint_base = os.path.join(path_deck_base, 'AGRINT.DAT')
    comentarios, df_agrint = wx_agrint.leituraArquivo(path_agrint_base)
    wx_agrint.escrever_arquivo(df_agrint, comentarios, path_agrint_base.replace('AGRINT.DAT','AGRINT_.DAT'))
    
    path_cadic_base = os.path.join(path_deck_base, 'C_ADIC.DAT')
    comentarios, df_cadic = wx_c_adic.leituraArquivo(path_cadic_base)
    wx_c_adic.escrever_arquivo(df_cadic, comentarios, path_cadic_base.replace('C_ADIC.DAT','C_ADIC_.DAT'))
    
    path_confh_base = os.path.join(path_deck_base, 'CONFHD.DAT')
    comentarios, df_confhd = wx_confhd.leituraArquivo(path_confh_base)
    wx_confhd.escrever_arquivo(df_confhd, comentarios, path_confh_base.replace('CONFHD.DAT','CONFHD_.DAT'))
    
    path_curva_base = os.path.join(path_deck_base, 'CURVA.DAT')
    comentarios, df_curva = wx_curva.leituraArquivo(path_curva_base)
    wx_curva.escrever_arquivo(df_curva, comentarios, path_curva_base.replace('CURVA.DAT','CURVA_.DAT'))
    
    path_dsvagua_base = os.path.join(path_deck_base, 'DSVAGUA.DAT')
    comentarios, df_dsvagua = wx_desvagua.leituraArquivo(path_dsvagua_base)
    wx_desvagua.escrever_arquivo(df_dsvagua, comentarios, path_dsvagua_base.replace('DSVAGUA.DAT','DSVAGUA_.DAT'))
    
    path_ghmin_base = os.path.join(path_deck_base, 'GHMIN.DAT')
    comentarios, df_ghmin = wx_ghmin.leituraArquivo(path_ghmin_base)
    wx_ghmin.escrever_arquivo(df_ghmin, comentarios, path_ghmin_base.replace('GHMIN.DAT','GHMIN_.DAT'))
    
    path_modif_base = os.path.join(path_deck_base, 'MODIF.DAT')
    comentarios, df_modif = wx_modif.leituraArquivo(path_modif_base)
    wx_modif.escrever_arquivo(df_modif, comentarios, path_modif_base.replace('MODIF.DAT','MODIF_.DAT'))
    
    path_patamar_base = os.path.join(path_deck_base, 'PATAMAR.DAT')
    comentarios, df_patamar = wx_patamar.leituraArquivo(path_patamar_base)
    wx_patamar.escrever_arquivo(df_patamar, comentarios, path_patamar_base.replace('PATAMAR.DAT','PATAMAR_.DAT'))
    
    path_term_base = os.path.join(path_deck_base, 'TERM.DAT')
    comentarios, df_term = wx_term.leituraArquivo(path_term_base)
    wx_term.escrever_arquivo(df_term, comentarios, path_term_base.replace('TERM.DAT','TERM_.DAT'))
    
    path_manutt_base = os.path.join(path_deck_base, 'MANUTT.DAT')
    comentarios, df_manutt = wx_manutt.leituraArquivo(path_manutt_base)
    wx_manutt.escrever_arquivo(df_manutt, comentarios, path_manutt_base.replace('MANUTT.DAT','MANUTT_.DAT'))


def printHelper():
    dt_hoje = datetime.datetime.now()
    dt_dia_primeiro = dt_hoje.replace(day=1,)
    dtE_proximo_deck = wx_opweek.ElecData(wx_opweek.getLastSaturday(
        dt_dia_primeiro + datetime.timedelta(days=32)))

    path_deck_base = os.path.join(path_home, 'Downloads', 'NW202306_base')
    caminho_saida = os.path.join(path_app, 'arquivos', 'saida')
    
    print(f"python {sys.argv[0]} montar_decks_base dt_inicio_deck {dtE_proximo_deck.data.strftime('%d/%m/%Y')} path_deck_base {path_deck_base} caminho_saida {caminho_saida} IGP_DI -5.5")
    print(f"python {sys.argv[0]} teste path_deck_base {path_deck_base} ")
    
    quit()
    
if __name__ == "__main__":
    
    if len(sys.argv) > 1:
        if 'help' in sys.argv or '--h' in sys.argv or '-help' in sys.argv:
            printHelper()
            
        for i in range(len(sys.argv)):
            argumento = sys.argv[i].lower()
            
            if argumento == 'dt_inicio_deck':
                str_dt_inicio_deck = sys.argv[i+1]
                p_dt_inicio_deck = datetime.datetime.strptime(str_dt_inicio_deck, "%d/%m/%Y")

            elif argumento == 'path_deck_base':
                p_path_deck_base = sys.argv[i+1]

            elif argumento == 'caminho_saida':
                p_path_saida = sys.argv[i+1]

            elif argumento.lower() == 'igp_di':
                p_IGP_DI = float(sys.argv[i+1])


    else:
        printHelper()
    
    if sys.argv[1].lower() == 'teste':
        testUnitarios(p_path_deck_base)
        
    elif sys.argv[1].lower() == 'montar_decks_base':

        montar_decks_base(p_dt_inicio_deck, p_path_deck_base, p_path_saida, p_IGP_DI)
