
import os
import re
import pdb
import sys
import shutil
import datetime

sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.bibliotecas import wx_opweek
from PMO.scripts_unificados.apps.dessem.libs import wx_pdoSist


path_decks = os.path.abspath(f'/WX2TB/Documentos/fontes/PMO/decks')
path_decks_ccee = os.path.join(path_decks, 'ccee', 'ds', )
path_decks_ons = os.path.join(path_decks, 'ons', 'ds', )

def converter_entdados(dt_referente):

    restricoes_comentar = {}
    restricoes_comentar['RE'] = [141,143]
    path_saida = os.path.join(path_decks, 'rz', 'ds', f'{dt_referente.strftime("%y%m%d")}_comentado')
    if os.path.exists(path_saida):
        shutil.rmtree(path_saida)

    os.makedirs(path_saida)

    dt_seguinte = dt_referente + datetime.timedelta(days=1)

    data_inicio_rev_atual = wx_opweek.getLastSaturday(dt_referente)
    semana_eletrica_atual = wx_opweek.ElecData(data_inicio_rev_atual)
    mes_ref_atual = semana_eletrica_atual.mesReferente
    ano_ref_atual = semana_eletrica_atual.anoReferente
    rev_ref_atual = semana_eletrica_atual.atualRevisao

    dt_referente_anterior = dt_referente - datetime.timedelta(days=1)
    data_inicio_rev_dia_anterior = wx_opweek.getLastSaturday(dt_referente_anterior)
    semana_eletrica_dia_anterior = wx_opweek.ElecData(data_inicio_rev_dia_anterior)
    mes_ref_anterior = semana_eletrica_dia_anterior.mesReferente
    ano_ref_anterior = semana_eletrica_dia_anterior.anoReferente
    rev_ref_anterior = semana_eletrica_dia_anterior.atualRevisao

    dt_referente_seguinte = dt_referente + datetime.timedelta(days=1)
    data_inicio_rev_dia_seguinte = wx_opweek.getLastSaturday(dt_referente_seguinte)
    semana_eletrica_dia_seguinte = wx_opweek.ElecData(data_inicio_rev_dia_seguinte)
    rev_ref_seguinte = semana_eletrica_dia_seguinte.atualRevisao

    path_ultimo_deck_ons = os.path.join(path_decks_ons,f'ds_ons_{mes_ref_atual:0>2}{ano_ref_atual}_rv{rev_ref_atual}d{dt_referente.strftime("%d")}')
    path_penultimo_deck_ons = os.path.join(path_decks_ons,f'ds_ons_{mes_ref_anterior:0>2}{ano_ref_anterior}_rv{rev_ref_anterior}d{dt_referente_anterior.strftime("%d")}')
    path_ultimo_deck_ccee = os.path.join(path_decks_ccee,f'DS_CCEE_{mes_ref_anterior:0>2}{ano_ref_anterior}_SEMREDE_RV{rev_ref_anterior}D{dt_referente_anterior.strftime("%d")}')
    ultimo_entdados_ons = open(os.path.join(path_ultimo_deck_ons,'entdados.dat'), 'r', encoding="utf8").readlines()
    penultimo_entdados_ons = open(os.path.join(path_penultimo_deck_ons,'entdados.dat'), 'r', encoding="utf8").readlines()
    print(path_ultimo_deck_ccee)
    try:
        penultimo_entdados_ccee = open(os.path.join(path_ultimo_deck_ccee,'entdados.dat'), 'r', encoding="utf8").readlines()
    except:
        penultimo_entdados_ccee = open(os.path.join(path_ultimo_deck_ccee,'entdados.dat'), 'r', encoding="Windows-1252").readlines()

    mnemonicos_restricoes = ['RE','LU','FI','FH','FE','FT','FC','FR']
    mnemonicos_comentados = ['TM','DP','RD','CI','CE'] + mnemonicos_restricoes
    barra_submercado = {}
    barra_submercado['CE'] = {}
    barra_submercado['CI'] = {}

    di_restricoes = {}
    deck_penultimo_ons = {}
    deck_penultimo_ccee = {}
    for mnemonico in mnemonicos_comentados:
        deck_penultimo_ons[mnemonico] = set()
        deck_penultimo_ccee[mnemonico] = set()
        di_restricoes[mnemonico] = {}

    for linha in penultimo_entdados_ons:
        if linha[0] == '&':
            continue
        info_linha = linha.split() 
        mnemonico = info_linha[0]
        if mnemonico in mnemonicos_comentados:
            id_ = info_linha[1]
            deck_penultimo_ons[mnemonico].add(id_)

            # de-para de barramento e submercado
            if mnemonico in ['CE','CI']:
                barramento = info_linha[3][:-1]
                barra_submercado[mnemonico][id_] = {barramento:None}

            # Inicia com valor nulo
            di_restricoes[mnemonico][id_] = {info_linha[2]: None}

    flag_mudanca_id = False
    ultimo_id_analisado = None
    restricoes_nao_mapeadas_ons = {}
    restricao_tmp = ''
    for linha in penultimo_entdados_ccee:
        if flag_mudanca_id:
            restricao_tmp = ''
            flag_mudanca_id = False

        if linha[0] == '&':
            restricao_tmp += linha
            continue
        info_linha = linha.split() 
        mnemonico = info_linha[0]
        if mnemonico in mnemonicos_comentados:
            id_ = info_linha[1]
            deck_penultimo_ccee[mnemonico].add(id_)

            restricao_tmp += linha
            
            if id_ != ultimo_id_analisado :
                if mnemonico in mnemonicos_restricoes:
                    if ultimo_id_analisado not in di_restricoes[mnemonico]:
                        restricoes_nao_mapeadas_ons[ultimo_id_analisado] = restricao_tmp
                flag_mudanca_id = True
                ultimo_id_analisado = id_

            if mnemonico in ['CE','CI']:
                submercado = info_linha[3][:-1]
                barramento = list(barra_submercado[mnemonico][id_].keys())[0]
                barra_submercado[mnemonico][id_][barramento] = submercado[0]
            elif id_ in di_restricoes[mnemonico]:
                valor_ons = list(di_restricoes[mnemonico][id_].keys())[0]
                di_restricoes[mnemonico][id_][valor_ons] = info_linha[2]

    apenas_ons = {}
    apenas_ccee = {}
    for mnemonico in deck_penultimo_ons:
        apenas_ons[mnemonico] = set(deck_penultimo_ons[mnemonico]) - set(deck_penultimo_ccee[mnemonico])
        apenas_ccee[mnemonico] = set(deck_penultimo_ccee[mnemonico]) - set(deck_penultimo_ons[mnemonico])

    #Atualizar a demanda [DP]
    pmoSist = wx_pdoSist.leituraSist(os.path.join(path_ultimo_deck_ons,'PDO_SIST.dat'))
    formatacao_DP = '{:>2}  {:>2}  {:>2} {:>2} {:>1} {:>2} {:>2} {:>1} {:>10}\n'
    id_submercado = {'SE':1,'S':2,'NE':3,'N':4,}
    novo_bloco_dp = {}
    for index, row in pmoSist.iterrows():
        iper = int(row['iper'].strip())
        subm = row['sist'].strip()
        if iper == 49:
            break
        if subm == 'FC':
            continue
        if subm not in novo_bloco_dp:
            novo_bloco_dp[subm] = []
        dt = dt_referente + datetime.timedelta(minutes=30*(iper-1))
        novo_bloco_dp[subm].append(formatacao_DP.format('DP', id_submercado[subm], dt.day, dt.hour, int(dt.minute/30), 'F', '', '', row['demanda'].strip()))

    novo_bloco_dp = novo_bloco_dp['SE'] + novo_bloco_dp['S'] + novo_bloco_dp['NE'] + novo_bloco_dp['N']

    i_linha_DP = 0

    pattern_RE9XX_comentada = r'^&(RE|LU|FI|FH|FE|FT|FC|FR) +9[0-9]{2} '
    # Cria o novo entdados com o ultimo deck do ONS
    for i_linha, linha in enumerate(ultimo_entdados_ons):
        if linha[0] == '&':
            # Descomentar as restricoes de sexta e colocar I no dia inicial
            if dt_referente.weekday() == 4 and re.match(pattern_RE9XX_comentada, linha):
                # Descomentar
                linha = linha[1:]
                # trocar o valor do dia para I
                # print(linha)
                ultimo_entdados_ons[i_linha] = re.sub(r"(RE|LU|FI|FH|FE|FT|FC|FR)( +)(\d+)( +)(\d+)(.*)", r"\1\2\3\4 I\6", linha)
                # print(linha)
            # Descomentar as restricoes de sexta e colocar I no dia inicial
            # elif dt_referente.weekday() == 5 or re.match(pattern_RE9XX_comentada, linha):
            else:
                continue
        info_linha = linha.split() 
        mnemonico = info_linha[0]

        if mnemonico in mnemonicos_comentados:
            id_ = info_linha[1]

            # Existe apenas no ultimo deck do ons ou está comentado no deck da ccee
            # Ira aparecer comentado
            if id_ in apenas_ons[mnemonico]:
                ultimo_entdados_ons[i_linha] = '&'+linha
                continue

            # Converter barramento para submercado
            if mnemonico in ['CE','CI']:
                barramento = info_linha[3][:-1]
                try:
                    subm = barra_submercado[mnemonico][id_][barramento]
                    ultimo_entdados_ons[i_linha] = '{}{: >5}{}'.format(linha[:18], barra_submercado[mnemonico][id_][barramento],linha[18+5:])
                except:
                    ultimo_entdados_ons[i_linha] = '&'+linha
                    print('Não foi encontrado o de-para do barramento {} para o deck do dia {}'.format(barramento, dt_referente.strftime('%d/%m/%Y')))
                    continue
                if subm == None:
                    pdb.set_trace()
                    continue

            # Retirada de rede no bloco TM
            elif mnemonico in ['TM']:
                ultimo_entdados_ons[i_linha] = '{}{}{}'.format(linha[:29],'0',linha[30:])
            
            elif mnemonico in ['DP']:
                if int(info_linha[2]) == dt_referente.day and int(info_linha[1]) != 11:
                    ultimo_entdados_ons[i_linha] = novo_bloco_dp[i_linha_DP]
                    i_linha_DP += 1
            elif id_ in di_restricoes[mnemonico]:
                
                # val_ons = list(di_restricoes[mnemonico][id_].keys())[0]
                # val_ccee = di_restricoes[mnemonico][id_][val_ons]

                # if val_ccee == 'I' and val_ons != val_ccee:
                if info_linha[2] == dt_seguinte.strftime('%d') and  info_linha[3] == 'F':
                    if mnemonico in ['RE']:
                        ultimo_entdados_ons[i_linha] = '{}{: >2}{}'.format(linha[:9],'I',linha[11:])
                    elif mnemonico in ['FC','FR']:
                        ultimo_entdados_ons[i_linha] = '{}{: >2}{}'.format(linha[:10],'I',linha[12:])
                    else:
                        ultimo_entdados_ons[i_linha] = '{}{: >2}{}'.format(linha[:8],'I',linha[10:])

    path_saida_entdados = os.path.join(path_saida, 'entdados.dat')
    with open(path_saida_entdados, 'w', encoding="utf8") as f:
        for linha in ultimo_entdados_ons:
            f.write(linha)
    print(path_saida_entdados)

    path_ultimo_renovaveis_ons = os.path.join(path_ultimo_deck_ons,'renovaveis.dat')
    ultimo_renovaveis_ons = open(path_ultimo_renovaveis_ons, 'r').readlines()

    for i_linha, linha in enumerate(ultimo_renovaveis_ons):
        if linha[0:8] == 'EOLICA ;' and linha[74] == '1':
            linha = linha[:74] + '0' + linha[75:]

    path_saida_renovaveis = os.path.join(path_saida,'renovaveis.dat')
    with open(path_saida_renovaveis, 'w') as f:
        for linha in ultimo_renovaveis_ons:
            f.write(linha)
    print(path_saida_renovaveis)
    
    
    arquivos_ultimo_deck_ccee = [f'cortdeco.rv{rev_ref_anterior}','dessem.arq',f'mapcut.rv{rev_ref_anterior}','restseg.dat','rstlpp.dat']
    
    # Arquivos copiados do ultimo deck ONS sem nenhuma alteracao
    for f in os.listdir(path_ultimo_deck_ons):
        if f in arquivos_ultimo_deck_ccee + ['entdados.dat', 'renovaveis.dat'] \
        or f.endswith('.afp') or f.startswith('pdo_'):
            continue
        pathSource = os.path.join(path_ultimo_deck_ons, f)
        pathDest = os.path.join(path_saida, f)
        shutil.copyfile(pathSource, pathDest)
        print(pathDest)

    # Arquivos copiados do ultimo deck CCEE sem nenhuma alteracao
    for arq in arquivos_ultimo_deck_ccee:
        # rev_ref_anterior
        pathSrc = os.path.join(path_ultimo_deck_ccee, arq.format(rev_ref_anterior))
        pathDest = os.path.join(path_saida, arq.format(rev_ref_seguinte))
        shutil.copyfile(pathSrc, pathDest)
        print(pathDest)
        
    return path_saida


if '__main__' == __name__:

    dt_inicial = datetime.datetime(2024,2,24)
    dt_final = dt_inicial
    # dt_final = datetime.datetime(2024,3,1)

    dt_referente = dt_inicial
    while dt_referente <= dt_final:
        path_saida = os.path.abspath(r'C:\WX2TB\Documentos\fontes\PMO\scripts_unificados\apps\dessem\arquivos')
        path_saida = os.path.join(path_saida,dt_referente.strftime('%Y%m%d'))
        converter_entdados(dt_referente)
        dt_referente = dt_referente + datetime.timedelta(days=1)