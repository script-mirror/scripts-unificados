import os
import pdb
import sys
import datetime

sys.path.insert(1,"/WX2TB/Documentos/fontes/PMO/scripts_unificados/")
from bibliotecas import wx_opweek,rz_dir_tools
from apps.dbUpdater.libs import rodadas,chuva,carga_ons,temperatura,deck_ds,bbce,vazao


DIR_TOOLS = rz_dir_tools.DirTools()
path_fontes = "/WX2TB/Documentos/fontes"
PATH_WEBHOOK_TMP = os.path.join(path_fontes,"PMO","scripts_unificados","apps","webhook","arquivos","tmp")


def printHelper():
    hoje = datetime.datetime.now()
    dia_anterior = hoje - datetime.timedelta(days=1)
    dia_seginte = hoje + datetime.timedelta(days=1)
    
    inicio_proxima_semana = wx_opweek.getLastSaturday(hoje.date()) + datetime.timedelta(days=7)
    semana_eletrica = wx_opweek.ElecData(inicio_proxima_semana)
    proxRv = wx_opweek.ElecData(wx_opweek.getLastSaturday(hoje)+datetime.timedelta(days=7))
    mesRef = datetime.date(hoje.month,proxRv.mesReferente,1).strftime('%B')
    mesRef = mesRef[0].upper()+mesRef[1:]

    #ONS
    path_rev_consistido = os.path.join(PATH_WEBHOOK_TMP, "Resultados preliminares consistidos (vazões semanais - PMO)","Consistido_%Y%m_[REV].zip")
    path_psath = os.path.join(PATH_WEBHOOK_TMP,"Histórico de Precipitação por Satélite",f"psath_{hoje.strftime('%d%m%Y')}.zip")
    path_arquivos_weol = os.path.join(PATH_WEBHOOK_TMP,"Dados utilizados na previsão de geração eólica", f"Deck_Previsao_{hoje.strftime('%Y%m%d')}.zip")
    path_ipdo = os.path.join(PATH_WEBHOOK_TMP,"IPDO (Informativo Preliminar Diário da Operação)",f"IPDO-{dia_anterior.strftime('%d-%m-%Y')}.zip")
    path_acomph = os.path.join(PATH_WEBHOOK_TMP,"Acomph",f"acomph_{hoje.strftime('%d.%m.%Y')}.xls")
    path_rdh = os.path.join(PATH_WEBHOOK_TMP,"RDH",f"rdh_{dia_anterior.strftime('%d%b%Y')}.xlsx")

    #MODELO PREV CARGA     
    path_deck_ds_entrada = os.path.join(PATH_WEBHOOK_TMP,"Decks de entrada do PrevCargaDESSEM" ,f"Deck_{hoje.strftime('%Y-%m-%d')}.zip")
    path_deck_PrevCargaDesssem = os.path.join(PATH_WEBHOOK_TMP,"Arquivos de Previsão de Carga para o DESSEM - PrevCargaDESSEM",f"PrevCargaDESSEM_{hoje.strftime('%Y-%m-%d')}.zip")
    
    #decks/ arquivos de carga
    path_carga_nw = os.path.join(PATH_WEBHOOK_TMP,"Previsões de carga mensal e por patamar - NEWAVE",f'RV{proxRv.atualRevisao}_PMO_{mesRef}_{proxRv.anoReferente}_carga_mensal.zip')
    path_deck_ds_entrada_saida = os.path.join(PATH_WEBHOOK_TMP,"Decks de entrada e saída - Modelo DESSEM","ds_ons_%m%Y_rv[REV]d%d.zip")
    path_deck_decomp = os.path.join(PATH_WEBHOOK_TMP,"Deck Preliminar DECOMP - Valor Esperado","DEC_ONS_%m%Y_RV[REV]_VE.zip")

    #CCEE
    path_deck_nw = f"/WX2TB/Documentos/fontes/PMO/decks/ccee/nw/NW{hoje.strftime('%Y%m')}.zip"
    
    
    
    print("\n==========RODADAS===============")
    print("python {} importar_chuva data {} modelo 'GEFS' rodada 0 estudo 0".format(
        sys.argv[0], hoje.strftime("%d/%m/%Y")))
    
    print("python {} importar_smap data {} modelo 'GEFS' rodada 0 preliminar 0 pdp 0 psat 0 estudo 0 inicio_previsao {}".format(
              sys.argv[0], 
              hoje.strftime("%d/%m/%Y"),
              dia_seginte.strftime("%d/%m/%Y"),
              ))
    
    print(f"python {sys.argv[0]} importar_prevs data {hoje.strftime('%d/%m/%Y')} modelo 'GEFS' rodada 0 dt_rv {semana_eletrica.data.strftime('%d/%m/%Y')} preliminar 0 pdp 0 psat 0 estudo 0")
    
    print("\n==========REVISAO===============")
    print(f'python {sys.argv[0]} atualizar_patamar_ano_atual path "{path_deck_ds_entrada}" data {hoje.strftime("%d/%m/%Y")}')
    print(f'python {sys.argv[0]} importar_rev_consistido path "{path_rev_consistido}"')
    
    print("\n==========CHUVA===============")
    print(f'python {sys.argv[0]} importar_chuva_observada modelo pobs data {hoje.strftime("%d/%m/%Y")}')
    print(f'python {sys.argv[0]} verificar_chuva_observada data {hoje.strftime("%d/%m/%Y")}')
    print(f'python {sys.argv[0]} importar_chuva_psath path "{path_psath}"')

    print("\n==========GERACAO - EOLICA WEOL===============")
    print(f'python {sys.argv[0]} importar_eolica_files path "{path_arquivos_weol}"')

    print("\n==========TEMPERATURA===============")
    print(f'python {sys.argv[0]} importar_temperatura_prev_dessem path "{path_deck_ds_entrada}"')
    print(f'python {sys.argv[0]} importar_temperatura_obs path "{path_deck_ds_entrada}"')
    
    print("\n==========CARGA ONS===============")
    print(f'python {sys.argv[0]} importar_prev_carga_dessem_saida path {path_deck_PrevCargaDesssem}')
    print(f'python {sys.argv[0]} importar_carga_ipdo path "{path_ipdo}" data {dia_anterior.strftime("%d/%m/%Y")}')
    
    
    print("\n==========VAZAO===============")
    print(f'python {sys.argv[0]} importar_acomph path "{path_acomph}"')
    print(f'python {sys.argv[0]} importar_rdh path "{path_rdh}"')
    
    print("\n==========BBCE===============")
    print(f"python {sys.argv[0]} importar_operacoes_bbce data {hoje.strftime('%d/%m/%Y')}")

    print("\n==========DECK NEWAVE===============")
    print(f"python {sys.argv[0]} importar_deck_values_NW path {path_deck_nw}")
    print(f'python {sys.argv[0]} importar_carga_nw path "{path_carga_nw}" data {"dd/mm/yyyy"} str_fonte {"ons"}')
    
    print("\n=====DECK DESSEM====")
    print(f'python {sys.argv[0]} importar_renovaveis_ds path "{path_deck_ds_entrada_saida}" data {dia_seginte.strftime("%d/%m/%Y")} str_fonte {"ons"}')
    print(f'python {sys.argv[0]} importar_pdo_cmosist_ds path "{path_deck_ds_entrada_saida}" data {dia_seginte.strftime("%d/%m/%Y")} str_fonte {"ons"}')
    print(f'python {sys.argv[0]} importar_ds_bloco_dp path "{path_deck_ds_entrada_saida}" data {dia_seginte.strftime("%d/%m/%Y")} str_fonte {"ons"}')

    print("\n=======DECK DECOMP======")
    print(f'python {sys.argv[0]} importar_dc_bloco_pq path "{path_deck_decomp}" str_fonte {"ons"}')
    print(f'python {sys.argv[0]} importar_dc_bloco_dp path "{path_deck_decomp}" str_fonte {"ons"}')

    quit()

if __name__ == '__main__':
    
    if len(sys.argv) > 1:
        if 'help' in sys.argv or '--h' in sys.argv or '-help' in sys.argv:
            printHelper()
            quit()

        for i in range(len(sys.argv)):
            argumento = sys.argv[i].lower()

            #ARGUMENTOS INICIAIS
            if argumento == 'data':
                p_dataRodada = sys.argv[i+1]
                p_dataRodada = datetime.datetime.strptime(p_dataRodada, "%d/%m/%Y")

            elif argumento == 'rodada':
                p_rodada = int(sys.argv[i+1])
                
            elif argumento == 'modelo':
                p_modelo = sys.argv[i+1]
                
            elif argumento == 'dt_rv':
                dtRv = sys.argv[i+1]
                p_dtRv = datetime.datetime.strptime(dtRv, '%d/%m/%Y' )
            
            elif argumento == 'preliminar':
                p_preliminar = int(sys.argv[i+1])

            elif argumento == 'pdp':
                p_pdp = int(sys.argv[i+1])

            elif argumento == 'psat':
                p_psat = int(sys.argv[i+1])
    
            elif argumento == 'estudo':
                p_estudo = int(sys.argv[i+1])
                
            elif argumento == 'inicio_previsao':
                inicioPrevisao = sys.argv[i+1]
                p_inicioPrevisao = datetime.datetime.strptime(inicioPrevisao, '%d/%m/%Y')
                
            elif argumento == 'path':
                p_path = sys.argv[i+1]
                real_path = DIR_TOOLS.get_name_insentive_name(p_path)
                if not real_path:
                    print(f'Arquivo não existe: {p_path}')
                    quit()
                else:
                    p_path = real_path
                
            elif argumento == 'id_fonte':
                p_idFonte = int(sys.argv[i+1])
            
            elif argumento == 'str_fonte':
                p_strFonte = sys.argv[i+1]

            
        
        #RODADA
        if sys.argv[1].lower() == 'importar_chuva':
            rodadas.importar_chuva(data=p_dataRodada, rodada=p_rodada,
                                modelo=p_modelo, flag_estudo=p_estudo)

        elif sys.argv[1].lower() == 'importar_smap':
            rodadas.importar_smap(data=p_dataRodada, rodada=p_rodada,
                                modelo=p_modelo, flag_preliminar=p_preliminar,
                                flag_pdp=p_pdp, flag_psat=p_psat,
                                flag_estudo=p_estudo, 
                                dt_inicio_previsao_chuva=p_inicioPrevisao)
            
        elif sys.argv[1].lower() == 'importar_prevs':
            rodadas.importar_prevs(data=p_dataRodada, rodada=p_rodada,
                                modelo=p_modelo, dt_rv=p_dtRv,
                                flag_preliminar=p_preliminar,
                                flag_pdp=p_pdp, flag_psat=p_psat, 
                                flag_estudo=p_estudo)
            
        #CARGA ONS
        
        elif sys.argv[1].lower() == 'importar_prev_carga_dessem_saida':
            carga_ons.importar_prev_carga_dessem_saida(path_zip = p_path)

        #CHUVA
        elif sys.argv[1].lower() == 'importar_chuva_observada':
            chuva.importar_chuva_observada(modelo = p_modelo,dt_final_observado=p_dataRodada)

        elif sys.argv[1].lower() == 'verificar_chuva_observada':
            chuva.verificar_chuva_observada(dt_final_observado=p_dataRodada)

        elif sys.argv[1].lower() == 'importar_chuva_psath':
            chuva.importar_chuva_psath(path_psath = p_path)


        #TEMPERATURA
        elif sys.argv[1].lower() == 'importar_temperatura_prev_dessem':
            temperatura.importar_temperatura_prev_dessem(path_zip = p_path)

        elif sys.argv[1].lower() == 'importar_temperatura_obs':
            temperatura.importar_temperatura_obs(path_zip = p_path)


        #VAZAO 
        elif sys.argv[1].lower() == 'importar_acomph':
            vazao.importAcomph(path = p_path)

        elif sys.argv[1].lower() == 'importar_rdh':
            vazao.importRdh(path = p_path)

        
        #BBCE
        elif sys.argv[1].lower() == 'importar_operacoes_bbce':
            bbce.importar_operacoes_bbce(data = p_dataRodada)


        
        #DECK DESSEM

        elif sys.argv[1].lower() == 'importar_renovaveis_ds':
            deck_ds.importar_renovaveis_ds(path_file=p_path,dt_ref=p_dataRodada, str_fonte = p_strFonte)

        elif sys.argv[1].lower() == 'importar_pdo_cmosist_ds':
            deck_ds.importar_pdo_cmosist_ds(path_file=p_path,dt_ref=p_dataRodada, str_fonte = p_strFonte)

        elif sys.argv[1].lower() == 'importar_ds_bloco_dp':
            deck_ds.importar_ds_bloco_dp(path_zip=p_path,dt_ref=p_dataRodada, str_fonte = p_strFonte)




    else:
        printHelper()



