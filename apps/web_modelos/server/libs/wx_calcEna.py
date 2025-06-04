import sys
import datetime
import pandas as pd
import sqlalchemy as db
import pdb

import os
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")), ".env"))
PATH_PROJETO = os.getenv("PATH_PROJETO", "/WX2TB/Documentos/fontes/PMO")
sys.path.insert(1,f"{PATH_PROJETO}/scripts_unificados")

from bibliotecas import wx_dbClass
from apps.web_modelos.server.libs import rz_ena

def propagarPostos(vazao, acomph, postosDesejados):
    tempo_viagem = {18:18, 33:30, 99:46, 241:28, 261:28, 34:5, 243:7, 245:48, 154:32, 246:56}
    acomphCombinadoSmap = vazao.combine_first(acomph)
    vazoesPropagadas = pd.DataFrame(columns=vazao.columns)
    for posto in postosDesejados:
        tv_horas = tempo_viagem[posto]  # tempo de viagem em horas para o posto
        tv_dia = tv_horas//24                   # numero de dias INTEIRO no tempo de viagem
        for dia in vazoesPropagadas.columns:
            try:
                vaz1 = acomphCombinadoSmap.loc[posto, dia-datetime.timedelta(days=tv_dia)]
            # exception no caso do acomph do dia atual nao tiver saido, ele ira repetir o do dia anterior
            except:
                vaz1 = acomphCombinadoSmap.loc[posto, dia_aux-datetime.timedelta(days=1+tv_dia)]

            try:
                vaz2 = acomphCombinadoSmap.loc[posto, dia-datetime.timedelta(days=1+tv_dia)]
            # exception no caso do acomph do dia atual nao tiver saido, ele ira repetir o do dia anterior
            except:
                vaz2 = acomphCombinadoSmap.loc[posto, dia-datetime.timedelta(days=2+tv_dia)]

            vazoesPropagadas.loc[posto, dia] = (((24*(1+tv_dia))-tv_horas)*vaz1 + (tv_horas-24*tv_dia)*vaz2)/24

    return vazoesPropagadas

def propagarCalcularNaturais(data, vazao,df_acomph = None ,dias=7):
    """ Faz a propagacao e o calculo das naturais dos postos de alguns postos artificiais
    :param vazao: [dataframe] Informacoes de vazoes 
    :return vazao: [dataframe] Informacoes de vazoes atualizados
    """
    # delay na data para pegar todos os tempos de viagens
    acomph = rz_ena.getAcomph(data-datetime.timedelta(days=dias)) if not df_acomph else df_acomph


    df_acomph = pd.DataFrame(acomph, columns=['CD_POSTO', 'DT_REFERENTE', 'VL_VAZ_INC_CONSO', 'VL_VAZ_NAT_CONSO', 'DT_ACOMPH', 'ROW_NUMBER'])
    df_acomph = df_acomph.pivot(index='CD_POSTO', columns='DT_REFERENTE', values='VL_VAZ_NAT_CONSO')

    vazoesPropagadas = propagarPostos(vazao, df_acomph, [18,33,99,241,261,154])

    vazNatural = pd.DataFrame(columns=vazao.columns)
    vazNatural.loc[239] = vazao.loc[238] + vazao.loc[239]
    vazNatural.loc[242] = vazao.loc[242] + vazao.loc[240]

    vazNatural.loc[243] = vazNatural.loc[242] + vazao.loc[243]
    vazoesPropagadas = pd.concat([vazoesPropagadas, propagarPostos(vazNatural, df_acomph, [243])])

    vazNatural.loc[34] = vazoesPropagadas.loc[[18,33,99,241,261]].sum() + vazao.loc[34]
    vazoesPropagadas = pd.concat([vazoesPropagadas, propagarPostos(vazNatural, df_acomph, [34])])

    vazNatural.loc[245] = vazoesPropagadas.loc[[34, 243]].sum() + vazao.loc[245]
    vazoesPropagadas = pd.concat([vazoesPropagadas, propagarPostos(vazNatural, df_acomph, [245])])

    vazNatural.loc[246] = vazoesPropagadas.loc[[154, 245]].sum() + vazao.loc[246]
    vazoesPropagadas = pd.concat([vazoesPropagadas, propagarPostos(vazNatural, df_acomph, [246])])

    vazNatural.loc[266] = vazoesPropagadas.loc[246] + vazao.loc[266] + vazao.loc[63]
    vazNatural.loc[244] = vazoesPropagadas.loc[[34, 243]].sum()

    vazNatural.loc[191] = vazao.loc[[191, 270]].sum()
    vazNatural.loc[253] = vazNatural.loc[191] + vazao.loc[253]
    vazNatural.loc[273] = vazao.loc[[273, 257]].sum()
    vazNatural.loc[271] = vazNatural.loc[273] + vazao.loc[271]
    vazNatural.loc[275] = vazNatural.loc[271] + vazao.loc[275]
    

    vazao.loc[244] = vazNatural.loc[244]
    vazao.loc[245] = vazNatural.loc[245]
    vazao.loc[246] = vazNatural.loc[246]
    vazao.loc[266] = vazNatural.loc[266]
    vazao.loc[34] = vazNatural.loc[34]
    # vazao.loc[154] = vazNatural.loc[154]
    vazao.loc[239] = vazNatural.loc[239]
    vazao.loc[242] = vazNatural.loc[242]
    vazao.loc[243] = vazNatural.loc[243]

    vazao.loc[191] = vazNatural.loc[191]
    vazao.loc[253] = vazNatural.loc[253]
    vazao.loc[273] = vazNatural.loc[273]
    vazao.loc[271] = vazNatural.loc[271]
    vazao.loc[275] = vazNatural.loc[275]

    return vazao

def calcPostosArtificiais_df(
    vazao:pd.DataFrame,
    ignorar_erros=False
) -> pd.DataFrame:
    ordemCalculoPostos = [226, 118, 109, 119, 104, 116, 160, 171, 175, 176, 203, 230, 244, 252, 320, 37, 38, 39, 40, 42, 43, 44, 45, 46, 66, 75, 298, 317, 315, 316, 304, 127, 126, 131, 132, 292, 299, 302, 303, 306, 318, 227, 228, 81, 183]

    idxJan = vazao.columns.strftime('%m') == '01'
    idxfev = vazao.columns.strftime('%m') == '02'
    idxMar = vazao.columns.strftime('%m') == '03'
    idxAbr = vazao.columns.strftime('%m') == '04'
    idxMai = vazao.columns.strftime('%m') == '05'
    idxJun = vazao.columns.strftime('%m') == '06'
    idxJul = vazao.columns.strftime('%m') == '07'
    idxAgo = vazao.columns.strftime('%m') == '08'
    idxSet = vazao.columns.strftime('%m') == '09'
    idxOut = vazao.columns.strftime('%m') == '10'
    idxNov = vazao.columns.strftime('%m') == '11'
    idxDez = vazao.columns.strftime('%m') == '12'

    for posto in ordemCalculoPostos:
        if posto not in vazao.index:
            try:
                if posto == 104:
                    vazao.loc[104] = vazao.loc[117] + vazao.loc[118]
                elif posto == 226:
                    vazao.loc[226] = 0
                    
                elif posto == 109:
                    vazao.loc[109] = vazao.loc[118]

                elif posto == 118:
                    vazao.loc[118] = vazao.loc[119] * 0.8103 + 0.185

                elif posto == 116:
                    vazao.loc[116] = vazao.loc[119] - vazao.loc[118]
                    
                elif posto == 318:
                    vazao.loc[318] = vazao.loc[116] + vazao.loc[117] + vazao.loc[118] + 0.1 * (vazao.loc[161] - vazao.loc[117] - vazao.loc[118])

                elif posto == 160:
                    vazao.loc[160] = 10

                elif posto == 171:
                    vazao.loc[171] = 0

                elif posto == 175:
                    vazao.loc[175] = vazao.loc[173]

                elif posto == 176:
                    vazao.loc[176] = vazao.loc[173]

                elif posto == 244:
                    vazao.loc[244] = vazao.loc[34] + vazao.loc[243]

                elif posto == 252:
                    vazao.loc[252] = vazao.loc[259]

                elif posto == 320:
                    vazao.loc[320] = vazao.loc[119]

                elif posto == 37:
                    vazao.loc[37] = vazao.loc[237]-0.1*(vazao.loc[161]-vazao.loc[117]-vazao.loc[118])-vazao.loc[117]-vazao.loc[118]

                elif posto == 38:
                    vazao.loc[38] = vazao.loc[238]-0.1*(vazao.loc[161]-vazao.loc[117]-vazao.loc[118])-vazao.loc[117]-vazao.loc[118]

                elif posto == 39:
                    vazao.loc[39] = vazao.loc[239]-0.1*(vazao.loc[161]-vazao.loc[117]-vazao.loc[118])-vazao.loc[117]-vazao.loc[118]

                elif posto == 40:
                    vazao.loc[40] = vazao.loc[240]-0.1*(vazao.loc[161]-vazao.loc[117]-vazao.loc[118])-vazao.loc[117]-vazao.loc[118]

                elif posto == 42:
                    vazao.loc[42] = vazao.loc[242]-0.1*(vazao.loc[161]-vazao.loc[117]-vazao.loc[118])-vazao.loc[117]-vazao.loc[118]

                elif posto == 43:
                    vazao.loc[43] = vazao.loc[243]-0.1*(vazao.loc[161]-vazao.loc[117]-vazao.loc[118])-vazao.loc[117]-vazao.loc[118]

                elif posto == 44:
                    vazao.loc[44] = vazao.loc[244]-0.1*(vazao.loc[161]-vazao.loc[117]-vazao.loc[118])-vazao.loc[117]-vazao.loc[118]

                elif posto == 45:
                    vazao.loc[45] = vazao.loc[245]-0.1*(vazao.loc[161]-vazao.loc[117]-vazao.loc[118])-vazao.loc[117]-vazao.loc[118]

                elif posto == 46:
                    vazao.loc[46] = vazao.loc[246]-0.1*(vazao.loc[161]-vazao.loc[117]-vazao.loc[118])-vazao.loc[117]-vazao.loc[118]

                elif posto == 66:
                    vazao.loc[66] = vazao.loc[266]-0.1*(vazao.loc[161]-vazao.loc[117]-vazao.loc[118])-vazao.loc[117]-vazao.loc[118]

                elif posto == 75:
                    vazao.loc[75] = vazao.loc[76] + pd.DataFrame({'vaz': vazao.loc[73]-10, 'limiar':173.5}).min(axis=1)

                elif posto == 317:
                    vazao.loc[317] = pd.DataFrame({'vaz': vazao.loc[201]-25, 'limiar':0}).max(axis=1)

                elif posto == 315:
                    vazao.loc[315] = vazao.loc[203] - vazao.loc[201] + vazao.loc[317] + vazao.loc[298]

                elif posto == 316:
                    vazao.loc[316] = pd.DataFrame({'vaz': vazao.loc[315], 'limiar':190}).min(axis=1)

                elif posto == 304:
                    vazao.loc[304] = vazao.loc[315] - vazao.loc[316]

                elif posto == 127:
                    vazao.loc[127] = vazao.loc[129] - vazao.loc[298] - vazao.loc[203] + vazao.loc[304]

                elif posto == 126:
                    vazTemp = pd.DataFrame({'vaz': vazao.loc[127]-90, 'limiar':0}).max(axis=1)
                    vazao.loc[126] = pd.DataFrame({'vaz': vazTemp, 'limiar':340}).min(axis=1)

                elif posto == 131:
                    vazao.loc[131] = pd.DataFrame({'vaz': vazao.loc[315], 'limiar':144}).min(axis=1)

                elif posto == 132:
                    vazao.loc[132] = vazao.loc[202] + pd.DataFrame({'vaz': vazao.loc[201], 'limiar':25}).min(axis=1)

                elif posto == 299:
                    vazao.loc[299] = vazao.loc[130] - vazao.loc[298] - vazao.loc[203] + vazao.loc[304]

                elif posto == 302:
                    vazao.loc[302] = vazao.loc[288] - vazao.loc[292]

                elif posto == 306:
                    vazao.loc[306] = vazao.loc[303] + vazao.loc[131]

                elif posto == 298:
                    idx1 = vazao.loc[125] <= 190
                    vazao.loc[posto, idx1] = vazao.loc[125, idx1]*(119/190)
                    idx2 = vazao.loc[125].between(190, 209, inclusive='right')
                    vazao.loc[posto, idx2] = 119
                    idx3 = vazao.loc[125].between(209, 250, inclusive='right')
                    vazao.loc[posto, idx3] = vazao.loc[125, idx3] - 90
                    idx4 = vazao.loc[125] >= 251
                    vazao.loc[posto, idx4] = 160

                elif posto == 303:
                    idx1 = vazao.loc[132] < 17
                    vazao.loc[posto, idx1] = vazao.loc[132, idx1]
                    vazao.loc[posto, ~idx1] = 17 + pd.DataFrame({'vaz': vazao.loc[316]-vazao.loc[131], 'limiar':34}).min(axis=1)


                elif posto == 119:
                    
                    postoBase = 118
                    vazao.loc[posto, idxJan] = vazao.loc[postoBase, idxJan]*1.217 + 0.608
                    vazao.loc[posto, idxfev] = vazao.loc[postoBase, idxfev]*1.232 + 0.123
                    vazao.loc[posto, idxMar] = vazao.loc[postoBase, idxMar]*1.311 - 2.359
                    vazao.loc[posto, idxAbr] = vazao.loc[postoBase, idxAbr]*1.241 - 0.496
                    vazao.loc[posto, idxMai] = vazao.loc[postoBase, idxMai]*1.167 + 0.467
                    vazao.loc[posto, idxJun] = vazao.loc[postoBase, idxJun]*1.333 - 0.533
                    vazao.loc[posto, idxJul] = vazao.loc[postoBase, idxJul]*1.247 - 0.374
                    vazao.loc[posto, idxAgo] = vazao.loc[postoBase, idxAgo]*1.200 + 0.360
                    vazao.loc[posto, idxSet] = vazao.loc[postoBase, idxSet]*1.292 - 1.292
                    vazao.loc[posto, idxOut] = vazao.loc[postoBase, idxOut]*1.250 - 0.250
                    vazao.loc[posto, idxNov] = vazao.loc[postoBase, idxNov]*1.294 - 1.682
                    vazao.loc[posto, idxDez] = vazao.loc[postoBase, idxDez]*1.215 + 0.729

                elif posto == 203:
                    postoBase = 201
                    vazao.loc[posto, idxJan] = vazao.loc[postoBase, idxJan]*1.476 - 0.4
                    vazao.loc[posto, idxfev] = vazao.loc[postoBase, idxfev]*1.449 + 0.4
                    vazao.loc[posto, idxMar] = vazao.loc[postoBase, idxMar]*1.477 - 0.2
                    vazao.loc[posto, idxAbr] = vazao.loc[postoBase, idxAbr]*1.453 + 0.3
                    vazao.loc[posto, idxMai] = vazao.loc[postoBase, idxMai]*1.320 + 1.7
                    vazao.loc[posto, idxJun] = vazao.loc[postoBase, idxJun]*1.419 + 0.3
                    vazao.loc[posto, idxJul] = vazao.loc[postoBase, idxJul]*1.436 + 0.2
                    vazao.loc[posto, idxAgo] = vazao.loc[postoBase, idxAgo]*1.462 + 0.0
                    vazao.loc[posto, idxSet] = vazao.loc[postoBase, idxSet]*1.477 - 0.1
                    vazao.loc[posto, idxOut] = vazao.loc[postoBase, idxOut]*1.467 - 0.1
                    vazao.loc[posto, idxNov] = vazao.loc[postoBase, idxNov]*1.457 + 0.1
                    vazao.loc[posto, idxDez] = vazao.loc[postoBase, idxDez]*1.457 + 0.1

                elif posto == 230:
                    postoBase = 229
                    vazao.loc[posto, idxJan] = -vazao.loc[postoBase, idxJan]*0.1 + 1.009
                    vazao.loc[posto, idxfev] = vazao.loc[postoBase, idxfev]*0.3 + 1.009
                    vazao.loc[posto, idxMar] = vazao.loc[postoBase, idxMar]*0.4 + 1.009
                    vazao.loc[posto, idxAbr] = vazao.loc[postoBase, idxAbr]*0.6 + 1.009
                    vazao.loc[posto, idxMai] = vazao.loc[postoBase, idxMai]*0.0 + 1.009
                    vazao.loc[posto, idxJun] = -vazao.loc[postoBase, idxJun]*0.2 + 1.009
                    vazao.loc[posto, idxJul] = vazao.loc[postoBase, idxJul]*0.1 + 1.009
                    vazao.loc[posto, idxAgo] = vazao.loc[postoBase, idxAgo]*0 + 1.009
                    vazao.loc[posto, idxSet] = vazao.loc[postoBase, idxSet]*0 + 1.009
                    vazao.loc[posto, idxOut] = vazao.loc[postoBase, idxOut]*0.1 + 1.009
                    vazao.loc[posto, idxNov] = -vazao.loc[postoBase, idxNov]*0.1 + 1.009
                    vazao.loc[posto, idxDez] = -vazao.loc[postoBase, idxDez]*0.1 + 1.009

                elif posto == 292:
                    postoBase = 288
                    vazao.loc[posto, idxJan] = vazao.loc[postoBase, idxJan] - 1100
                    vazao.loc[posto, idxfev] = vazao.loc[postoBase, idxfev] - 1600
                    vazao.loc[posto, idxMar] = vazao.loc[postoBase, idxMar] - 3250
                    vazao.loc[posto, idxAbr] = vazao.loc[postoBase, idxAbr] - 6000
                    vazao.loc[posto, idxMai] = vazao.loc[postoBase, idxMai] - 2900
                    vazao.loc[posto, idxJun] = vazao.loc[postoBase, idxJun] - 1600
                    vazao.loc[posto, idxJul] = vazao.loc[postoBase, idxJul] - 1100
                    vazao.loc[posto, idxAgo] = vazao.loc[postoBase, idxAgo] - 900
                    vazao.loc[posto, idxSet] = vazao.loc[postoBase, idxSet] - 750
                    vazao.loc[posto, idxOut] = vazao.loc[postoBase, idxOut] - 700
                    vazao.loc[posto, idxNov] = vazao.loc[postoBase, idxNov] - 800
                    vazao.loc[posto, idxDez] = vazao.loc[postoBase, idxDez] - 900

                    vazao.loc[posto] = pd.DataFrame({'vaz': vazao.loc[posto], 'limiar':13900}).min(axis=1)
                    vazao.loc[posto] = pd.DataFrame({'vaz': vazao.loc[posto], 'limiar':0}).max(axis=1)

                elif posto == 227:
                    postoBase = 229
                    vazao.loc[posto, idxJan] = vazao.loc[postoBase, idxJan]*0.392 + 0
                    vazao.loc[posto, idxfev] = vazao.loc[postoBase, idxfev]*0.375 + 0
                    vazao.loc[posto, idxMar] = vazao.loc[postoBase, idxMar]*0.362 + 0
                    vazao.loc[posto, idxAbr] = vazao.loc[postoBase, idxAbr]*0.359 + 0
                    vazao.loc[posto, idxMai] = vazao.loc[postoBase, idxMai]*0.385 + 0
                    vazao.loc[posto, idxJun] = vazao.loc[postoBase, idxJun]*0.446 + 0
                    vazao.loc[posto, idxJul] = vazao.loc[postoBase, idxJul]*0.501 + 0
                    vazao.loc[posto, idxAgo] = vazao.loc[postoBase, idxAgo]*0.552 + 0
                    vazao.loc[posto, idxSet] = vazao.loc[postoBase, idxSet]*0.583 + 0
                    vazao.loc[posto, idxOut] = vazao.loc[postoBase, idxOut]*0.540 + 0
                    vazao.loc[posto, idxNov] = vazao.loc[postoBase, idxNov]*0.474 + 0
                    vazao.loc[posto, idxDez] = vazao.loc[postoBase, idxDez]*0.423 + 0

                elif posto == 228:
                    postoBase = 229
                    vazao.loc[posto, idxJan] = vazao.loc[postoBase, idxJan]*0.343 + 314
                    vazao.loc[posto, idxfev] = vazao.loc[postoBase, idxfev]*0.362 + 203.3
                    vazao.loc[posto, idxMar] = vazao.loc[postoBase, idxMar]*0.313 + 366.7
                    vazao.loc[posto, idxAbr] = vazao.loc[postoBase, idxAbr]*0.279 + 405.8
                    vazao.loc[posto, idxMai] = vazao.loc[postoBase, idxMai]*0.283 + 319.4
                    vazao.loc[posto, idxJun] = vazao.loc[postoBase, idxJun]*0.351 + 204
                    vazao.loc[posto, idxJul] = vazao.loc[postoBase, idxJul]*0.372 + 185.8
                    vazao.loc[posto, idxAgo] = vazao.loc[postoBase, idxAgo]*0.376 + 184.6
                    vazao.loc[posto, idxSet] = vazao.loc[postoBase, idxSet]*0.393 + 173.4
                    vazao.loc[posto, idxOut] = vazao.loc[postoBase, idxOut]*0.378 + 190
                    vazao.loc[posto, idxNov] = vazao.loc[postoBase, idxNov]*0.340 + 265
                    vazao.loc[posto, idxDez] = vazao.loc[postoBase, idxDez]*0.346 + 299.3

                elif posto == 81:
                    postoBase = 222
                    vazao.loc[posto, idxJan] = vazao.loc[postoBase, idxJan]*1.081
                    vazao.loc[posto, idxfev] = vazao.loc[postoBase, idxfev]*1.081
                    vazao.loc[posto, idxMar] = vazao.loc[postoBase, idxMar]*1.081
                    vazao.loc[posto, idxAbr] = vazao.loc[postoBase, idxAbr]*1.081
                    vazao.loc[posto, idxMai] = vazao.loc[postoBase, idxMai]*1.081
                    vazao.loc[posto, idxJun] = vazao.loc[postoBase, idxJun]*1.081
                    vazao.loc[posto, idxJul] = vazao.loc[postoBase, idxJul]*1.081
                    vazao.loc[posto, idxAgo] = vazao.loc[postoBase, idxAgo]*1.081
                    vazao.loc[posto, idxSet] = vazao.loc[postoBase, idxSet]*1.081
                    vazao.loc[posto, idxOut] = vazao.loc[postoBase, idxOut]*1.081
                    vazao.loc[posto, idxNov] = vazao.loc[postoBase, idxNov]*1.081
                    vazao.loc[posto, idxDez] = vazao.loc[postoBase, idxDez]*1.081

                elif posto == 183:
                    postoBase = 262
                    vazao.loc[posto, idxJan] = vazao.loc[postoBase, idxJan]*1.518
                    vazao.loc[posto, idxfev] = vazao.loc[postoBase, idxfev]*1.552
                    vazao.loc[posto, idxMar] = vazao.loc[postoBase, idxMar]*1.579
                    vazao.loc[posto, idxAbr] = vazao.loc[postoBase, idxAbr]*1.636
                    vazao.loc[posto, idxMai] = vazao.loc[postoBase, idxMai]*1.571
                    vazao.loc[posto, idxJun] = vazao.loc[postoBase, idxJun]*1.623
                    vazao.loc[posto, idxJul] = vazao.loc[postoBase, idxJul]*1.629
                    vazao.loc[posto, idxAgo] = vazao.loc[postoBase, idxAgo]*1.550
                    vazao.loc[posto, idxSet] = vazao.loc[postoBase, idxSet]*1.553
                    vazao.loc[posto, idxOut] = vazao.loc[postoBase, idxOut]*1.546
                    vazao.loc[posto, idxNov] = vazao.loc[postoBase, idxNov]*1.550
                    vazao.loc[posto, idxDez] = vazao.loc[postoBase, idxDez]*1.517


            except Exception as e:
                print("Erro ao calcular ENA para o posto artificial {0}".format(posto))
                if ignorar_erros:
                    print(f"IGNORANDO ERROS\n{e}")
                    pass
                else:
                    quit()
    return vazao

def getFatorConversaoDb():

    db_ons = wx_dbClass.db_mysql_master("db_ons", connect=True)
    tb_produtibilidade = db_ons.db_schemas['tb_produtibilidade']

    sql_select = db.select(
        tb_produtibilidade.c.cd_posto,
        tb_produtibilidade.c.vl_produtibilidade,
        tb_produtibilidade.c.str_submercado,
        tb_produtibilidade.c.str_bacia,
        tb_produtibilidade.c.str_sigla,
        )
    
    answer = db_ons.db_execute(sql_select).fetchall()
    db_ons.db_dispose()
    return answer

def gera_ena_df(vazao, divisao='submercado'):
    """ Calcula o valor de ena fazendo o agrupamento de acordo com a divisao escolhida 
    :param vazao: Dicionario com as vazoes diaria/semanal de cada posto
    :param divisao: Subdivisao de interesse (posto, bacia, submercado ou ree)
    :return ena: Dicionario com os valores de ena separados pela subdivisao
    """

    divisao = divisao.lower()

    produtibilidade = getFatorConversaoDb()
    df_produtibilidade = pd.DataFrame(produtibilidade, columns=['CD_POSTO', 'VL_PRODUTIBILIDADE', 'STR_SUBMERCADO', 'STR_BACIA', 'STR_SIGLA'])
    df_produtibilidade['STR_SIGLA'] = df_produtibilidade['STR_SIGLA'].str.strip()
    df_produtibilidade = df_produtibilidade.set_index('CD_POSTO')

    postosRelato = df_produtibilidade.index.to_list()
    # postosPrevis = list(vazao.keys())
    ena = pd.DataFrame(columns=vazao.columns)

    for posto in vazao.index:
        if posto in postosRelato:
            ena.loc[posto] = vazao.loc[posto]*df_produtibilidade.loc[posto]['VL_PRODUTIBILIDADE']


    if divisao == 'posto':
        return ena

    elif divisao == 'bacia':

        df_produtibilidade['STR_BACIA_COMPLETA'] = df_produtibilidade['STR_BACIA']

        renomear = df_produtibilidade[(df_produtibilidade['STR_BACIA']=='PARANAPANEMA') & (df_produtibilidade['STR_SIGLA']=='S')].index
        df_produtibilidade.loc[renomear, 'STR_BACIA_COMPLETA'] = df_produtibilidade.loc[renomear]['STR_BACIA'] + ' (' + df_produtibilidade.loc[renomear]['STR_SIGLA'] + ')'

        renomear = df_produtibilidade[df_produtibilidade['STR_BACIA'].isin(['SÃO FRANCISCO', 'TOCANTINS', 'JEQUITINHONHA', 'AMAZONAS'])].index
        df_produtibilidade.loc[renomear, 'STR_BACIA_COMPLETA'] = df_produtibilidade.loc[renomear]['STR_BACIA'] + ' (' + df_produtibilidade.loc[renomear]['STR_SIGLA'] + ')'
        
        enaBacia = pd.DataFrame(columns=ena.columns)
        for bacia in df_produtibilidade['STR_BACIA_COMPLETA'].unique():
            postosPorBacia = df_produtibilidade[df_produtibilidade['STR_BACIA_COMPLETA']==bacia].index
            enaBacia.loc[bacia] = ena[ena.index.isin(postosPorBacia)].sum()
            
        return enaBacia

    elif divisao == 'submercado':
        enaSub = pd.DataFrame(columns=ena.columns)
        for sub in df_produtibilidade['STR_SIGLA'].unique():
            postosPorSubmercado = df_produtibilidade[df_produtibilidade['STR_SIGLA']==sub].index
            enaSub.loc[sub] = ena[ena.index.isin(postosPorSubmercado)].sum()
        return enaSub
