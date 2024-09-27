# -*- coding: utf-8 -*-
import os
import sys
import pymssql

sys.path.insert(1,"/WX2TB/Documentos/fontes")
from PMO.scripts_unificados.apps.web_modelos.server import utils 

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")),'.env'))


__HOST_MSSQL = os.getenv('HOST_MSSQL') 
__PORT_DB_MSSQL = os.getenv('PORT_DB_MSSQL') 


__USER_DB_MSSQL = os.getenv('USER_DB_MSSQL') 
__PASSWORD_DB_MSSQL = os.getenv('PASSWORD_DB_MSSQL')


def printDB(dataB):
    print(dataB)

def connectHost():
    # dbHost = 'localhost'
    # dbUser = 'root'
    # dbPassword = ''
    # dbDatabase = 'climenergy'
    # port = 3306

    dbHost = __HOST_MSSQL
    dbUser = __USER_DB_MSSQL
    dbPassword = __PASSWORD_DB_MSSQL
    dbDatabase = 'climenergy'
    port = __PORT_DB_MSSQL

    conn = pymssql.connect(host=dbHost, user=dbUser, password=dbPassword, database=dbDatabase, port=port)
    # conn = pymysql.connect(host=dbHost, user=dbUser, password=dbPassword, database=dbDatabase, port=port)
    return conn



# Extração dos dados das Revisões
def getRevResultsGraphics(results, roundsInfo):
    subMercados=["'SUDESTE'","'SUL'","'NORDESTE'","'NORTE'"]
    # subMercados=["'SUDESTE'"]

    conn = connectHost()
    cursor = conn.cursor()

    gResults={}
    revisionStr = ''

    for subMercado in subMercados:
        gResults[subMercado[1:-1]]={}


       # Extraction revision data
        cursor.execute('''SELECT DAY(TB_VE.DT_INICIO_SEMANA) AS DIA, MONTH(TB_VE.DT_INICIO_SEMANA) AS MES, YEAR(TB_VE.DT_INICIO_SEMANA) AS ANO, TB_VE.VL_ENA AS ENA, TB_VE.CD_REVISAO AS REV
                    FROM TB_VE INNER JOIN TB_SUBMERCADO ON TB_VE.CD_SUBMERCADO = TB_SUBMERCADO.CD_SUBMERCADO
                    WHERE (TB_SUBMERCADO.STR_SUBMERCADO = '''+subMercado+''') AND
                          (TB_VE.VL_ANO = '''+str(roundsInfo['year'])+''') AND
                          (TB_VE.VL_MES = '''+str(roundsInfo['month'])+''') AND
                          (TB_VE.CD_REVISAO = '''+str(roundsInfo['rev'])+''')
                    ORDER BY TB_VE.DT_INICIO_SEMANA''')

        rows = cursor.fetchall()

        if not rows:
            cursor.execute('''SELECT DAY(TB_VE.DT_INICIO_SEMANA) AS DIA, MONTH(TB_VE.DT_INICIO_SEMANA) AS MES, YEAR(TB_VE.DT_INICIO_SEMANA) AS ANO, TB_VE.VL_ENA AS ENA, TB_VE.CD_REVISAO AS REV
                    FROM TB_VE INNER JOIN TB_SUBMERCADO ON TB_VE.CD_SUBMERCADO = TB_SUBMERCADO.CD_SUBMERCADO
                    WHERE (TB_SUBMERCADO.STR_SUBMERCADO = '''+subMercado+''') AND 
                          (TB_VE.VL_ANO = (SELECT MAX(VL_ANO) FROM TB_VE)) AND 
                          (TB_VE.VL_MES = (SELECT MAX(VL_MES) FROM TB_VE WHERE VL_ANO = (SELECT MAX(VL_ANO) FROM TB_VE))) AND 
                          (TB_VE.CD_REVISAO = (SELECT MAX(CD_REVISAO) FROM TB_VE WHERE VL_ANO = (SELECT MAX(VL_ANO) FROM TB_VE) AND VL_MES = (SELECT MAX(VL_MES) FROM TB_VE WHERE VL_ANO = (SELECT MAX(VL_ANO) FROM TB_VE))))
                    ORDER BY TB_VE.DT_INICIO_SEMANA''')
            rows = cursor.fetchall()


        revisions = {}
        revisions['day']=[]
        revisions['month']=[]
        revisions['year']=[]
        revisions['ena']=[]
        revisions['date']=[]
        revisions['rev']=[]

        for row in rows:
            revisions['day'].append(row[0])
            revisions['month'].append(row[1])
            revisions['year'].append(row[2])
            revisions['ena'].append(row[3])
            revisions['date'].append(utils.timeStampGmt([row[0],row[1],row[2]]))
            revisions['rev'].append(row[4])

        revisionStr = str(revisions['rev'][0])#+' ('+str(revisions['date'][0])+')'



        start_date_graphic = (revisions['date'][0]) - 14*24*60*60

#DATEPART(dw,DT_REFERENTE) AS DIA_SEMANA,
#WEEKDAY(DT_REFERENTE) AS DIA_SEMANA,
        # Extraction RDH data
        aux= "'"+utils.readableDateFromTimeS(start_date_graphic)+"'"
        dbAction = '''SELECT  DAY(DT_REFERENTE) AS DIA, 
                        MONTH(DT_REFERENTE) AS MES,
                        YEAR(DT_REFERENTE) AS ANO,
                        DATEPART(dw,DT_REFERENTE) AS DIA_SEMANA,
                        VL_MEDIA_SEMANA_65 AS ENA_MEDIA,
                        VL_MLT AS MLT,
                        VL_ENA AS ENA
                FROM VW_GRAFICO_RDH
                WHERE DT_REFERENTE >= '''+aux+''' AND STR_SUBMERCADO = '''+subMercado+'''
                ORDER BY DT_REFERENTE'''

        cursor.execute(dbAction)

        rows = cursor.fetchall()

        rdh = {}
        rdh['day']=[]
        rdh['month']=[]
        rdh['year']=[]
        rdh['day_week']=[]
        rdh['ena_media']=[]
        rdh['mlt']=[]
        rdh['ena']=[]
        rdh['date']=[]

        for row in rows:
            rdh['day'].append(row[0])
            rdh['month'].append(row[1])
            rdh['year'].append(row[2])
            rdh['day_week'].append(row[3])
            rdh['ena_media'].append(row[4])
            rdh['mlt'].append(row[5])
            rdh['ena'].append(row[6])
            rdh['date'].append(utils.timeStampGmt([row[0],row[1],row[2]]))

        # ???? // Arrumar o vetor de médias semanais #330
        index = (len(rdh['ena_media'])-2)
        for i in reversed(rdh['day_week'][:-1]):
            if i != 6:
                rdh['ena_media'][index] = rdh['ena_media'][index+1]
            index = index-1


        # ??? Atualiza sozinho todo o mes
        cursor.execute('''SELECT TB_MLT.VL_MES AS MES, TB_MLT.VL_MLT AS MLT
                FROM   TB_MLT INNER JOIN TB_SUBMERCADO ON TB_MLT.CD_SUBMERCADO = TB_SUBMERCADO.CD_SUBMERCADO
                WHERE  TB_SUBMERCADO.STR_SUBMERCADO = '''+subMercado+'''
                ORDER BY TB_MLT.VL_MES''')

        rows = cursor.fetchall()
        
        mlt = {}
        mlt['ena']=[]
        mlt['month']=[]

        for row in rows:
            mlt['month'].append(row[0])
            mlt['ena'].append(row[1])

        all_dates=[]

        for roundD in results:
            all_dates=[]
            gResults[subMercado[1:-1]][roundD]={}

            gResults[subMercado[1:-1]][roundD]['mlt_complete'] = []
            gResults[subMercado[1:-1]][roundD]['ena_complete'] = []
            gResults[subMercado[1:-1]][roundD]['ena_media_complete'] = []
            gResults[subMercado[1:-1]][roundD]['ena_revisions_complete'] = []
            gResults[subMercado[1:-1]][roundD]['ena_round_complete'] = []
            # gResults[subMercado[1:-1]][roundD]['ena_estimations_complete'] = []

            # Extraction round data (PREVIVAZ)
            cursor.execute('''SELECT  DAY(TB_RESULTADO_RODADA_DECOMP.DT_INICIO_SEMANA) AS DIA,
                                MONTH(TB_RESULTADO_RODADA_DECOMP.DT_INICIO_SEMANA) AS MES, 
                                YEAR(TB_RESULTADO_RODADA_DECOMP.DT_INICIO_SEMANA) AS ANO, 
                                TB_RESULTADO_RODADA_DECOMP.VL_ENA AS ENA, 
                                TB_RESULTADO_RODADA_DECOMP.VL_CMO AS CMO, 
                                TB_RESULTADO_RODADA_DECOMP.VL_EAR AS EAR, 
                                TB_SUBMERCADO.STR_SUBMERCADO
                        FROM    TB_CADASTRO_RODADA_DECOMP INNER JOIN
                                TB_RESULTADO_RODADA_DECOMP ON 
                                TB_CADASTRO_RODADA_DECOMP.CD_RODADA = TB_RESULTADO_RODADA_DECOMP.CD_RODADA INNER JOIN
                                TB_SUBMERCADO ON TB_RESULTADO_RODADA_DECOMP.CD_SUBMERCADO = TB_SUBMERCADO.CD_SUBMERCADO
                        WHERE  (TB_SUBMERCADO.STR_SUBMERCADO = '''+subMercado+''') AND 
                               (TB_CADASTRO_RODADA_DECOMP.CD_RODADA = '''+str(roundD)+''') AND
                               (TB_RESULTADO_RODADA_DECOMP.FL_PREVIVAZ = 1)
                        ORDER BY TB_RESULTADO_RODADA_DECOMP.DT_INICIO_SEMANA''')

            rows = cursor.fetchall()

            previsions = {}
            previsions['day']=[]
            previsions['month']=[]
            previsions['year']=[]
            previsions['date']=[]
            previsions['ena']=[]
            previsions['cmo']=[]
            previsions['ear']=[]


            for row in rows:
                previsions['day'].append(row[0])
                previsions['month'].append(row[1])
                previsions['year'].append(row[2])
                previsions['ena'].append(row[3])
                previsions['cmo'].append(row[4])
                if row[5] is not None:
                    previsions['ear'].append(row[5]*100)
                else:
                    previsions['ear'].append(None)
                previsions['date'].append(utils.timeStampGmt([row[0],row[1],row[2]]))

            for i in previsions['cmo']:
                if i is not None:
                    pos=previsions['cmo'].index(i)


            # Extraction round data (ESTIMATIVA)
            # cursor.execute ('''SELECT DAY(TB_RESULTADO_RODADA_DECOMP.DT_INICIO_SEMANA) AS DIA,
            #                 MONTH(TB_RESULTADO_RODADA_DECOMP.DT_INICIO_SEMANA) AS MES, 
            #                 YEAR(TB_RESULTADO_RODADA_DECOMP.DT_INICIO_SEMANA) AS ANO, 
            #                 TB_RESULTADO_RODADA_DECOMP.VL_ENA AS ENA, 
            #                 TB_RESULTADO_RODADA_DECOMP.VL_CMO AS CMO, 
            #                 TB_RESULTADO_RODADA_DECOMP.VL_EAR AS EAR, 
            #                 TB_SUBMERCADO.STR_SUBMERCADO,
            #                 TB_CADASTRO_RODADA_DECOMP.TX_COMENTARIO AS COMENTARIO
            #         FROM    TB_CADASTRO_RODADA_DECOMP INNER JOIN
            #                 TB_RESULTADO_RODADA_DECOMP ON 
            #                 TB_CADASTRO_RODADA_DECOMP.CD_RODADA = TB_RESULTADO_RODADA_DECOMP.CD_RODADA INNER JOIN
            #                 TB_SUBMERCADO ON TB_RESULTADO_RODADA_DECOMP.CD_SUBMERCADO = TB_SUBMERCADO.CD_SUBMERCADO
            #         WHERE  (TB_SUBMERCADO.STR_SUBMERCADO = '''+subMercado+''') AND 
            #                (TB_CADASTRO_RODADA_DECOMP.CD_RODADA = '''+str(roundD)+''') AND
            #                (TB_RESULTADO_RODADA_DECOMP.FL_PREVIVAZ IS NOT NULL)
            #         ORDER BY TB_RESULTADO_RODADA_DECOMP.DT_INICIO_SEMANA''')

            # rows = cursor.fetchall()

            # estimations = {}
            # estimations['day']=[]
            # estimations['month']=[]
            # estimations['year']=[]
            # estimations['date']=[]
            # estimations['ena']=[]
            # estimations['cmo']=[]
            # estimations['ear']=[]

            # for row in rows:
            #     estimations['day'].append(row[0])
            #     estimations['month'].append(row[1])
            #     estimations['year'].append(row[2])
            #     estimations['ena'].append(row[3])
            #     estimations['cmo'].append(row[4])
            #     if row[5] is not None:
            #         estimations['ear'].append(row[5]*100)
            #     else:
            #         estimations['ear'].append(None)
            #     estimations['date'].append(utils.timeStampGmt([row[0],row[1],row[2]]))


            # Array de tempo completo
            num_dates = int((max([previsions['date'][-1],revisions['date'][-1]])-start_date_graphic)/(24*60*60))+7

            for i in range(num_dates):
                aux = start_date_graphic + i*24*60*60
                all_dates.append(aux)


            # revResults{rodada: {coment: , date: , fl_excluida: , 
            # subRegiao: {ena: , cmo:, ear: , mlt_complete:, ena_complete: ena_media_complete,
            # ena_revisions_complete:, ena_round_complete:, ena_estimations_complete: }}

            # // Completar informações para cada instante
            mlt_complete = []
            ena_complete = []
            ena_media_complete = []
            ena_revisions_complete = []
            ena_round_complete = []
            # ena_estimations_complete = []

            final_revision=(all_dates.index(revisions['date'][-1]))+7
            final_round=(all_dates.index(previsions['date'][-1]))+7
            # final_estimation=(all_dates.index(estimations['date'][-1]))+7


            for i in range(num_dates):
                # // Ajeitar informações de Ena da Revisão
                try:
                    key=revisions['date'].index(all_dates[i])
                    ena_revisions_complete.append(revisions['ena'][key])
                except ValueError:
                     ena_revisions_complete.append(None)

                # // Ajeitar informações de Ena da Rodada (PREVIVAZ)
                try:
                    key=previsions['date'].index(all_dates[i])
                    ena_round_complete.append(previsions['ena'][key])
                except ValueError:
                     ena_round_complete.append(None)

                # // Ajeitar informações de Ena da Rodada (ESTIMATIVA)
                # try:
                #     key=estimations['date'].index(all_dates[i])
                #     ena_estimations_complete.append(estimations['ena'][key])
                # except ValueError:
                #      ena_estimations_complete.append(None)            

                # // Ajeitar informações do RDH
                try:
                    key=rdh['date'].index(all_dates[i])
                    ena_complete.append(rdh['ena'][key])
                    ena_media_complete.append(rdh['ena_media'][key])
                except ValueError:
                     ena_complete.append(None)
                     ena_media_complete.append(None) 

                # // Ajeitar informações de MLT
                aux = utils.getMonthFromTimeS(all_dates[i])-1
                mlt_complete.append(mlt['ena'][aux])



            #// Arrumar o indice 0 da Ena da Revisão
            key=all_dates.index(revisions['date'][0])
            ena_revisions_complete[key]=revisions['ena'][0]
            for i in range(final_revision):
                if ena_revisions_complete[i] is None:
                    ena_revisions_complete[i]=ena_revisions_complete[i-1]

            # // Arrumar o indice 0 da Ena da Rodada (PRREVIVAZ)
            key=all_dates.index(previsions['date'][0])
            ena_round_complete[key]=previsions['ena'][0]
            for i in range(final_round):
                if ena_round_complete[i] is None:
                    ena_round_complete[i]=ena_round_complete[i-1]
            
            # // Arrumar o indice 0 da Ena da Rodada (ESTIMATIVA)
            # key=all_dates.index(estimations['date'][0])
            # ena_estimations_complete[key] = estimations['ena'][0]
            # for i in range(final_estimation):
            #     if ena_estimations_complete[i] is None:
            #         ena_estimations_complete[i]=ena_estimations_complete[i-1]            

            # // Arrumar o indice 0 da Ena do RDH
            key=all_dates.index(rdh['date'][0])
            ena_complete[key] = rdh['ena'][0]
            ena_media_complete[key] = rdh['ena_media'][0]

            gResults[subMercado[1:-1]][roundD]['mlt_complete'] = mlt_complete
            gResults[subMercado[1:-1]][roundD]['ena_complete'] = ena_complete
            gResults[subMercado[1:-1]][roundD]['ena_media_complete'] = ena_media_complete
            gResults[subMercado[1:-1]][roundD]['ena_revisions_complete'] = ena_revisions_complete
            gResults[subMercado[1:-1]][roundD]['ena_round_complete'] = ena_round_complete
            # gResults[subMercado[1:-1]][roundD]['ena_estimations_complete'] = ena_estimations_complete


    xLabel = []
    for date in all_dates:
        xLabel.append(utils.convertToDayMonth(date))
    gResults['xLabel'] = xLabel

    conn.close()

    return gResults, revisionStr



# @def - Take results from revisions registered in DB
# def getRevResults(year, month, rev):
def getTableResults(roundInfo):

    year = roundInfo['year']
    month = roundInfo['month']
    rev = roundInfo['rev']
    fcf = "'"+roundInfo['fcf']+"'"
    smap = roundInfo['smap']
    slider = roundInfo['slider']
    rodada = roundInfo['rodada']

    rangeSlider={}
    if slider:
        slider = slider.split(',')
        rangeSlider['SUDESTE']=slider[0][2:].split('_')
        rangeSlider['SUL']=slider[1][2:].split('_')
        rangeSlider['NORDESTE']=slider[2][2:].split('_')
        rangeSlider['NORTE']=slider[3][2:].split('_')


    revResults={}
    conn = connectHost()
    cursor = conn.cursor()
    dbAction = '''SELECT CD_RODADA,TX_COMENTARIO,DT_RODADA,FL_EXCLUIDA,FL_SMAP,FL_MATRIZ
                    FROM TB_CADASTRO_RODADA_DECOMP
                    WHERE   VL_ANO = '''+str(year)+''' AND 
                            VL_MES = '''+str(month)+''' AND 
                            VL_REVISAO = '''+str(rev)
    if rodada != "all":
        dbAction=dbAction+''' AND CD_RODADA = ''' +str(rodada)
    elif fcf != "'all'":
        dbAction=dbAction+''' AND TX_FCF LIKE ''' +str(fcf)
        if smap != "all":
            dbAction=dbAction+''' AND FL_SMAP = ''' +str(smap)
    dbAction=dbAction+ ''' ORDER BY CD_RODADA DESC'''

    cursor.execute(dbAction)
    rows = cursor.fetchall()

    for row in rows:
        tempDict={}
        tempDict['coment']=row[1]
        tempDict['date']=str(row[2])
        if not row[3]:
            tempDict['fl_excluida']=0
        else:
            tempDict['fl_excluida']=row[3]
        if not row[4]:
            tempDict['fl_smap']=0
        else:
            tempDict['fl_smap']=row[4]
        if not row[5]:
            tempDict['fl_matriz']=0
        else:
            tempDict['fl_matriz']=row[5]
        revResults[row[0]]=tempDict

   
    for id_round in revResults:
        cursor.execute('''SELECT  TB_RESUMO_RESULTADO_RODADA_DECOMP.VL_ENA_MES_MLT AS ENA,
                                    TB_RESUMO_RESULTADO_RODADA_DECOMP.VL_CMO_SEMANA_1 AS CMO, 
                                    TB_RESUMO_RESULTADO_RODADA_DECOMP.VL_EAR_INICIAL AS EAR, 
                                    TB_SUBMERCADO.STR_SUBMERCADO AS SUB
                            FROM    (TB_RESUMO_RESULTADO_RODADA_DECOMP INNER JOIN
                                    TB_CADASTRO_RODADA_DECOMP ON 
                                    TB_RESUMO_RESULTADO_RODADA_DECOMP.CD_RODADA = TB_CADASTRO_RODADA_DECOMP.CD_RODADA) INNER JOIN
                                    (TB_AUX_ORDEM_SUBMERCADOS INNER JOIN
                                    TB_SUBMERCADO ON TB_AUX_ORDEM_SUBMERCADOS.STR_SUBMERCADO = TB_SUBMERCADO.STR_SUBMERCADO) ON 
                                    TB_RESUMO_RESULTADO_RODADA_DECOMP.CD_SUBMERCADO = TB_SUBMERCADO.CD_SUBMERCADO
                            WHERE  (TB_CADASTRO_RODADA_DECOMP.CD_RODADA = '''+str(id_round)+''')
                            ORDER BY TB_AUX_ORDEM_SUBMERCADOS.CD_SUBMERCADO''')
        rows = cursor.fetchall()

        for row in rows:
            tempDict={}
            tempDict['ena']=("%.2f" % row[0])
            tempDict['cmo']=("%.2f" % float(row[1]))
            tempDict['ear']=("%.2f" % (row[2]*100))
            revResults[id_round][row[3]]=tempDict

        cursor.execute('''SELECT DAY(TB_RESULTADO_RODADA_DECOMP.DT_INICIO_SEMANA) AS DIA, 
                       MONTH(TB_RESULTADO_RODADA_DECOMP.DT_INICIO_SEMANA) AS MES, 
                       YEAR(TB_RESULTADO_RODADA_DECOMP.DT_INICIO_SEMANA) AS ANO, 
                       TB_RESULTADO_RODADA_DECOMP.VL_ENA AS ENA, 
                       TB_RESULTADO_RODADA_DECOMP.VL_CMO AS CMO, 
                       TB_RESULTADO_RODADA_DECOMP.VL_EAR AS EAR, 
                       TB_SUBMERCADO.STR_SUBMERCADO AS SUB
                FROM   TB_CADASTRO_RODADA_DECOMP INNER JOIN
                       TB_RESULTADO_RODADA_DECOMP ON 
                       TB_CADASTRO_RODADA_DECOMP.CD_RODADA = TB_RESULTADO_RODADA_DECOMP.CD_RODADA INNER JOIN
                       TB_SUBMERCADO ON TB_RESULTADO_RODADA_DECOMP.CD_SUBMERCADO = TB_SUBMERCADO.CD_SUBMERCADO INNER JOIN
                       TB_AUX_ORDEM_SUBMERCADOS ON TB_SUBMERCADO.STR_SUBMERCADO = TB_AUX_ORDEM_SUBMERCADOS.STR_SUBMERCADO
            WHERE     (TB_CADASTRO_RODADA_DECOMP.CD_RODADA = '''+str(id_round)+''') AND 
                      (TB_RESULTADO_RODADA_DECOMP.FL_PREVIVAZ = 1) AND 
                      (TB_RESULTADO_RODADA_DECOMP.VL_CMO IS NOT NULL)
            ORDER BY TB_AUX_ORDEM_SUBMERCADOS.CD_SUBMERCADO, TB_RESULTADO_RODADA_DECOMP.DT_INICIO_SEMANA''')
        rows = cursor.fetchall()

        enaList = []
        cmoList = []
        for row in rows:
            if 'first_week' not in revResults[id_round][row[6]]:
                revResults[id_round][row[6]]['first_week']={}
                revResults[id_round][row[6]]['first_week']['ena']=[]
                revResults[id_round][row[6]]['first_week']['cmo']=[]
                revResults[id_round][row[6]]['first_week']['date']=[]

            revResults[id_round][row[6]]['first_week']['ena'].append(int(row[3]))
            revResults[id_round][row[6]]['first_week']['cmo'].append(("%.2f" % row[4]))
            revResults[id_round][row[6]]['first_week']['date'].append(str(row[0])+'-'+str(row[1])+'-'+str(row[2]))
            # tempDict={}
            # tempDict['day']=row[0]
            # tempDict['month']=row[1]
            # tempDict['year']=row[2]
            # tempDict['ena']=row[3]*100
            # tempDict['cmo']=float(row[4])
            # tempDict['ear']=row[5]
            # tempDict['date']=utils.timeStampGmt([row[0],row[1],row[2]])
            
            # if 'first_week' not in revResults[id_round][row[6]]:
            #     tempDict={}
            #     tempDict['ena']=row[3]
            #     tempDict['cmo']=row[4]
            #     tempDict['date']=str(row[0])+'-'+str(row[1])+'-'+str(row[2])
            #     revResults[id_round][row[6]]['first_week']=tempDict
            #     print(str(id_round)+'['+row[6]+']- ')


    conn.close()
    if slider:
        revResults = filterSliderEna(revResults, rangeSlider)
    return revResults

def filterSliderEna(revResults, rangeSlider):
    delFlag=0
    for id_round in list(revResults):
        for subMercado in revResults[id_round]:
            if subMercado in ['SUDESTE', 'SUL', 'NORTE', 'NORDESTE']:
                if not (int(rangeSlider[subMercado][0]) <= float(revResults[id_round][subMercado]['ena'])  and int(rangeSlider[subMercado][1]) >= float(revResults[id_round][subMercado]['ena'])):
                    delFlag=1
        if delFlag:
            delFlag=0
            del revResults[id_round]
            continue
    return revResults



# @def - Take information about all revisions registered in DB
def getRevInformation(flags):
    conn = connectHost()
    cursor = conn.cursor()
    dbAction ='''SELECT VL_ANO, VL_MES, VL_REVISAO, COUNT(CD_RODADA) AS NUM_RODADAS, CAST(TX_FCF as Varchar(50)) AS FCF FROM TB_CADASTRO_RODADA_DECOMP WHERE''' 
    if 'rodada' in flags:
        dbAction = dbAction + ''' CD_RODADA = '''+ str(flags['rodada'])
    else:
        if flags['matriz']:
            dbAction = dbAction + ''' FL_MATRIZ = 1'''
        else:
            dbAction = dbAction + ''' FL_MATRIZ = 0 or FL_MATRIZ is NULL'''

    dbAction = dbAction+ ''' 
    GROUP BY VL_ANO, VL_MES, VL_REVISAO, CAST(TX_FCF as Varchar(50)) 
    ORDER BY VL_ANO DESC, VL_MES DESC, VL_REVISAO DESC''' 
    # cursor.execute('SELECT VL_ANO, VL_MES, VL_REVISAO, COUNT(CD_RODADA) AS NUM_RODADAS, CAST(TX_FCF as Varchar(25)) AS FCF FROM TB_CADASTRO_RODADA_DECOMP GROUP BY VL_ANO, VL_MES, VL_REVISAO, CAST(TX_FCF as Varchar(25)) ORDER BY VL_ANO DESC, VL_MES DESC, VL_REVISAO DESC')
    cursor.execute(dbAction)
    row = cursor.fetchall()
    infoRevs=[]

    for i in range(len(row)):
        infoRevs.append(row[i])

    # infoRevs[0][0] => ano
    # infoRevs[0][1] => mes
    # infoRevs[0][2] => revisao
    # infoRevs[0][3] => num_rodadas

    conn.close()
    return infoRevs




if __name__ == "__main__":
    results=getTableResults(2019,7,3)
    getRevResultsGraphics(results, subMercados)

