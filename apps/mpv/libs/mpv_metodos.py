import os
import pdb
import sys
import time
import shutil
import datetime

import pandas as pd

path_home =  os.path.expanduser("~")
path_modulo = os.path.abspath(__file__)
path_libs = os.path.dirname(path_modulo)
path_app = os.path.dirname(path_libs)

path_apps = os.path.dirname(path_app)
path_raiz = os.path.dirname(path_apps)
path_libs_universal = os.path.abspath(os.path.join(path_raiz, 'bibliotecas'))

sys.path.insert(1, path_libs_universal)
import wx_manipulaArqCpins

path_saida_mpv = os.path.abspath(os.path.join(path_app, 'arquivos', 'saida'))
path_exe_pv = os.path.abspath(os.path.join(path_app, 'arquivos', 'exe'))

path_dropbox = os.path.join(path_home, 'Dropbox', )
path_dropbox_middle = os.path.join(path_dropbox, 'WX - Middle')

info_bacias = {}
info_bacias['Sao_Francisco'] = {}
info_bacias['Sao_Francisco']['Trechos'] = {}
info_bacias['Sao_Francisco']['Direta'] = {}
# info_bacias['Sao_Francisco']['topologia'] = {}


info_bacias['Sao_Francisco']['Trechos']['UQMD'] = {
    'entrada': {
        'cod_ref': 'UQMD',
        'orig_vaz': 'COO',
        'period': 'DI',
        'tipo_grand': 'DFL',
        'historico': '',
        'previsao': {
            'repetir': 1
        }
    },
    'topologia': {
        'metodo': 1,
        'trecho_origem': 'UQMD',
        'final_propagacao': 'N',
        'requer_arq_entrada': 'S',
        'jusante': 'UTMD',
        'ordem_entrada': '15',
        'quant_dias': ''
    },
    'tempo_viagem': 0
    }

info_bacias['Sao_Francisco']['Trechos']['UTMD'] = {
    'entrada': {
        'cod_ref': 'UTMD',
        'orig_vaz': 'COO',
        'period': 'DI',
        'tipo_grand': 'DFL',
        'historico': '',
        'previsao': {
            'repetir': 1
        }
    },
    'topologia': {
        'metodo': 3,
        'trecho_origem': 'UTMD',
        'final_propagacao': 'S',
        'requer_arq_entrada': 'S',
        'jusante': '',
        'ordem_entrada': '',
        'quant_dias': ''
    },
    'vaz_temp':{
        0.0: 0.0,
        200.0: 48.0,
        400.0: 54.0,
        500.0: 55.0,
        1000.0: 68.0,
        2000.0: 72.0,
        20000.0: 72.0
    },
    'parametros': {
        'numero_fases': 3,
        'max_iteracoes': 100,
        'erro': 0.11,
        'num_dias_passado': 15
    }

}

info_bacias['Sao_Francisco']['Trechos']['SROMAO'] = {
    'entrada': {
        'cod_ref': '43200000',
        'orig_vaz': 'COA',
        'period': 'HO',
        'tipo_grand': 'VMD',
        'historico': '',
        'previsao': {
            'mpv_saida': ['UTMD'],
            'smap_saida': ['SRM2']
        }
    },
    'topologia': {
        'metodo': 3,
        'trecho_origem': '43200000',
        'final_propagacao': 'S',
        'requer_arq_entrada': 'S',
        'jusante': '',
        'ordem_entrada': '',
        'quant_dias': ''
    },
    'vaz_temp':{
        0.0: 0.0,
        500.0: 20.0,
        2000.0: 24.0,
        5000.0: 24.0,
        6000.0: 24.0,
        20000.0: 24.0
    },
    'parametros': {
        'numero_fases': 1,
        'max_iteracoes': 100,
        'erro': 0.11,
        'num_dias_passado': 15
    }
}

info_bacias['Sao_Francisco']['Trechos']['SFRANCISCO'] = {
    'entrada': {
        'cod_ref': '44200000',
        'orig_vaz': 'COA',
        'period': 'HO',
        'tipo_grand': 'VMD',
        'historico': '',
        'previsao': {
            'mpv_saida': ['SROMAO'],
            'smap_saida': ['SFR2']
        }
    },
    'topologia': {
        'metodo': 3,
        'trecho_origem': '44200000',
        'final_propagacao': 'S',
        'requer_arq_entrada': 'S',
        'jusante': '',
        'ordem_entrada': '',
        'quant_dias': ''
    },
    'vaz_temp':{
        0.0: 0.0,
        500.0: 40.0,
        1000.0: 60.0,
        3000.0: 65.0,
        4000.0: 70.0,
        6000.0: 80.0,
        7000.0: 90.0,
        8000.0: 160.0,
        10000.0: 180.0,
        13000.0: 144.0,
        14000.0: 144.0,
        18000.0: 120.0,
        20000.0: 120.0
    },
    'parametros': {
        'numero_fases': 2,
        'max_iteracoes': 100,
        'erro': 0.11,
        'num_dias_passado': 15
    }
}

info_bacias['Sao_Francisco']['Trechos']['CARINHANHA'] = {
    'entrada': {
        'cod_ref': '45298000',
        'orig_vaz': 'COA',
        'period': 'HO',
        'tipo_grand': 'VMD',
        'historico': '',
        'previsao': {
            'mpv_saida': ['SFRANCISCO'],
            'cpins_anterior': ['INCR. SFR/CRH']
        }
    },
    'topologia': {
        'metodo': 3,
        'trecho_origem': '45298000',
        'final_propagacao': 'S',
        'requer_arq_entrada': 'S',
        'jusante': '',
        'ordem_entrada': '',
        'quant_dias': ''
    },
    'vaz_temp':{
        0.0: 0.0,
        500.0: 94.0,
        1000.0: 96.0,
        3000.0: 98.0,
        4000.0: 100.0,
        5000.0: 120.0,
        6000.0: 250.0,
        7000.0: 384.0,
        8000.0: 384.0,
        9000.0: 384.0,
        10000.0: 288.0,
        11000.0: 288.0,
        16000.0: 264.0,
        16000.0: 200.0,
        18000.0: 200.0,
        20000.0: 264.0
    },
    'parametros': {
        'numero_fases': 5,
        'max_iteracoes': 100,
        'erro': 0.11,
        'num_dias_passado': 15
    }
}

info_bacias['Sao_Francisco']['Trechos']['BOQ'] = {
    'entrada': {
        'cod_ref': '46902000',
        'orig_vaz': 'COA',
        'period': 'HO',
        'tipo_grand': 'VMD',
        'historico': '',
        'previsao': {
            'smap_saida': ['BOQ']
        }
    },
    'topologia': {
        'metodo': 1,
        'trecho_origem': '46902000',
        'final_propagacao': 'N',
        'requer_arq_entrada': 'S',
        'jusante': '46360000',
        'ordem_entrada': '15',
        'quant_dias': ''
    },
    'tempo_viagem': 0
}

info_bacias['Sao_Francisco']['Trechos']['MORPARA'] = {
    'entrada': {
        'cod_ref': '46360000',
        'orig_vaz': 'COA',
        'period': 'HO',
        'tipo_grand': 'VMD',
        'historico': '',
        'previsao': {
            'mpv_saida': ['CARINHANHA'],
            'cpins_anterior': ['INCR. CRH/MPR']
        }
    },
    'topologia': {
        'metodo': 3,
        'trecho_origem': '46360000',
        'final_propagacao': 'N',
        'requer_arq_entrada': 'S',
        'jusante': 'SFSOBR',
        'ordem_entrada': '',
        'quant_dias': ''
    },
    'vaz_temp':{
        0.0: 0.0,
        500.0: 48.0,
        1000.0: 55.0,
        3000.0: 100.0,
        4000.0: 180.0,
        5000.0: 200.0,
        6000.0: 200.0,
        8000.0: 200.0,
        20000.0: 200.0
    },
    'parametros': {
        'numero_fases': 8,
        'max_iteracoes': 100,
        'erro': 0.11,
        'num_dias_passado': 15
    }
}

info_bacias['Sao_Francisco']['Trechos']['SOBRADINHO'] = {
    'entrada': {
        'cod_ref': '46360000',
        'orig_vaz': 'COA',
        'period': 'HO',
        'tipo_grand': 'VMD',
        'historico': '',
        'previsao': {
            'mpv_saida': ['MORPARA'],
            'cpins_anterior': ['INCR. SOMA/USBA']
        }
    },
    'topologia': {
        'metodo': 1,
        'trecho_origem': 'SFSOBR',
        'final_propagacao': 'S',
        'requer_arq_entrada': 'S',
        'jusante': '',
        'ordem_entrada': '15',
        'quant_dias': ''
    },
    'tempo_viagem': 0
}

# info_bacias['Sao_Francisco']['Direta'] = {}


def criar_arquivo_bat(path):
    path_bat = os.path.join(path, 'mpv.bat')
    with open(path_bat, 'w') as arquivo_bat:
        arquivo_bat.write('@echo off\n')
        arquivo_bat.write('set caminho='+path_exe_pv+'\n')
        arquivo_bat.write('set pathEntrada='+os.path.join(path, 'Arq_Entrada')+'\n')
        arquivo_bat.write('set pathSaida='+os.path.join(path, 'Arq_Saida')+'\n')
        arquivo_bat.write('echo n | start /b /d "%caminho%" ONS.MPV.ConsoleApplication.exe "%pathEntrada%" "%pathSaida%"\n')
        # arquivo_bat.write('pause')
    print(path_bat)
    return path_bat

def criar_arquivo_parametros(path, data):
    num_dias_historicos = 51
    num_dias_previsao = 24
    path_arquivo_parametros = os.path.join(path, 'PARAMETROS.txt')
    with open(path_arquivo_parametros, 'w') as arq_parametros:
        arq_parametros.write((data-datetime.timedelta(days=num_dias_historicos)).strftime('   %d/%m/%Y')+'\n')
        arq_parametros.write((data+datetime.timedelta(days=num_dias_previsao)).strftime('   %d/%m/%Y')+'\n')
        arq_parametros.write('   S')
    print(path_arquivo_parametros)



def criar_arquivos_entrada(path, bacia, etapa, data):

    path_saida = path.replace('Arq_Entrada', 'Arq_Saida')

    path_dropbox_novoSmap = os.path.join(path_dropbox_middle, 'NovoSMAP')
    path_dropbox_mpv_ontem = os.path.join(path_dropbox_novoSmap, (data-datetime.timedelta(days=1)).strftime('%Y%m%d'), 'MPV', 'SSARR', bacia, etapa)

    path_arquivo_topologia = os.path.join(path, 'TOPOLOGIA_PROPAGACAO.txt')
    with open(path_arquivo_topologia, 'w') as arq_topologia:
            arq_topologia.write('(Nome Trecho        )MIFEJOE\n')

    path_arquivo_tempo_viagem = os.path.join(path, 'TEMPO_VIAGEM.txt')
    with open(path_arquivo_tempo_viagem, 'w') as arq_temp_viag:
            arq_temp_viag.write('(Nome Trecho        )(Tempo)\n')
    
    colunas = ['cod_ref', 'orig_vaz', 'period', 'tipo_grandeza', 'data', 'vazao']
    for trecho in info_bacias[bacia][etapa]:

        path_arq_saida = os.path.join(path, '{}_VAZOES_ENTRADA.txt'.format(trecho))
        # if os.path.exists(path_arq_saida):
        #     continue

        info_trecho = info_bacias[bacia][etapa][trecho]['entrada']

        path_arquivo_parametros = os.path.join(path, '{}_PARAMETROS.txt'.format(trecho))
        if os.path.exists(path_arquivo_parametros):
            os.remove(path_arquivo_parametros) 

        path_arq_entrada = os.path.join(path_dropbox_mpv_ontem, '{}_VAZOES_ENTRADA.txt'.format(trecho))
        vaz_entrada = pd.read_csv(path_arq_entrada, sep = '|', header = None)
        vaz_entrada.columns = colunas

        if trecho == 'SOBRADINHO':
            pdb.set_trace()

        # Sem dados, portante sera repetido o ultimo dia
        if 'repetir' in info_trecho['previsao']:
            for i_rep in range(info_trecho['previsao']['repetir']):
                vaz_entrada.drop(i_rep, inplace = True)
                ultimo_index = vaz_entrada.iloc[-1].name
                ultima_data = datetime.datetime.strptime(vaz_entrada.loc[ultimo_index]['data'], '%Y-%m-%d %H:%M:%S')

                vaz_entrada.loc[ultimo_index+1] = vaz_entrada.loc[ultimo_index].copy()
                vaz_entrada.loc[ultimo_index+1, 'data'] = (ultima_data + datetime.timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
        
        else:
            vaz_entrada['data'] = pd.to_datetime(vaz_entrada['data'], format='%Y-%m-%d %H:%M:%S')
            new_index = vaz_entrada.index[-1] + 1
            vaz_entrada = vaz_entrada.append(pd.DataFrame(index=[new_index], data=vaz_entrada.tail(1).values, columns=vaz_entrada.columns))
            vaz_entrada.loc[new_index, 'data'] = vaz_entrada.loc[new_index, 'data']  + datetime.timedelta(days=1)
            vaz_entrada.set_index('data', inplace=True)

            datas_atualizar = vaz_entrada[vaz_entrada.index >= data].index
            vaz_entrada.loc[datas_atualizar, 'vazao'] = 0

            if 'mpv_saida' in info_trecho['previsao']:
                for trecho_complementar in info_trecho['previsao']['mpv_saida']:
                    path_saida_trecho_complementar = os.path.join(path_saida, 'PROCESSADOS', '{}_VAZOES_SAIDA.txt'.format(trecho_complementar))

                    if not os.path.exists(path_saida_trecho_complementar):
                        return False

                    vazao_trecho_complementar = pd.read_csv(path_saida_trecho_complementar, skiprows=[0], sep = ' ', header = None, decimal=',')
                    vazao_trecho_complementar[0] = pd.to_datetime(vazao_trecho_complementar[0], format='%d/%m/%Y')
                    vazao_trecho_complementar.set_index(0, inplace=True)

                    vaz_entrada.loc[datas_atualizar, 'vazao'] += vazao_trecho_complementar.loc[datas_atualizar,1]
            
            if 'smap_saida' in info_trecho['previsao']:
                # path_saidas_smap = os.path.abspath(r'C:\WX2TB\Documentos\fontes\PMO\scripts_unificados\apps\smap\arquivos\opera-smap\{}\NE\ARQ_SAIDA'.format(data.strftime('%Y%m%d')))
                # Alguns trechos nao sao mapeados no nosso smap, portanto esta sendo utilizado os dados do dia anterior e estamos repetindo o ultimo dia faltante
                for trecho_complementar in info_trecho['previsao']['smap_saida']:
                    path_saida_trecho_complementar = os.path.join(path_dropbox_novoSmap, (data-datetime.timedelta(days=1)).strftime('%Y%m%d'), 'SMAP', 'NE', 'ARQ_SAIDA', '{}_PMEDIA_ORIG_PREVISAO.txt'.format(trecho_complementar))
                    vazao_trecho_complementar = pd.read_csv(path_saida_trecho_complementar,  skiprows=[0], sep = ' ', header = None)
                    vazao_trecho_complementar[0] = pd.to_datetime(vazao_trecho_complementar[0], format='%d/%m/%Y')
                    vazao_trecho_complementar.set_index(0, inplace=True)

                    vazao_trecho_complementar_desejada = pd.DataFrame(index=datas_atualizar, columns=[1])
                    vazao_trecho_complementar_desejada.update(vazao_trecho_complementar)
                    vazao_trecho_complementar_desejada = vazao_trecho_complementar_desejada.fillna(method='ffill')

                    vaz_entrada.loc[datas_atualizar, 'vazao'] += vazao_trecho_complementar_desejada.loc[datas_atualizar,1]

                vaz_entrada = vaz_entrada.reset_index()
                vaz_entrada = vaz_entrada[['cod_ref', 'orig_vaz', 'period', 'tipo_grandeza', 'data', 'vazao']]
                vaz_entrada['data'] = vaz_entrada['data'].dt.strftime('%Y-%m-%d %H:%M:%S')

            if 'cpins_anterior' in info_trecho['previsao']:
                path_planilha_usb = os.path.join(path_dropbox_novoSmap, 'PlanilhaUSB.xls')
                cpins_df = wx_manipulaArqCpins.get_all_cpins(path_planilha_usb)
                for trecho_complementar in info_trecho['previsao']['cpins_anterior']:

                    # if trecho_complementar == 'INCR. SOMA/USBA':
                    #     pdb.set_trace()

                    vazao_trecho_complementar_desejada = pd.DataFrame(index=datas_atualizar, columns=[trecho_complementar])
                    vazao_trecho_complementar_desejada.update(cpins_df.loc[datas_atualizar[:-1], trecho_complementar])
                    vazao_trecho_complementar_desejada = vazao_trecho_complementar_desejada.fillna(method='ffill')
                    
                    vaz_entrada.loc[datas_atualizar, 'vazao'] += vazao_trecho_complementar_desejada.loc[datas_atualizar,trecho_complementar]

                vaz_entrada = vaz_entrada.reset_index()
                vaz_entrada = vaz_entrada[['cod_ref', 'orig_vaz', 'period', 'tipo_grandeza', 'data', 'vazao']]
                vaz_entrada['data'] = vaz_entrada['data'].dt.strftime('%Y-%m-%d %H:%M:%S')

                
                    

        with open(path_arquivo_topologia, 'a+') as arq_topologia:
            linha = '{: <19} '.format(trecho)
            linha += '{}'.format(info_bacias[bacia][etapa][trecho]['topologia']['metodo'])
            linha += '{: <9} '.format(info_bacias[bacia][etapa][trecho]['topologia']['trecho_origem'])
            linha += '{}'.format(info_bacias[bacia][etapa][trecho]['topologia']['final_propagacao'])
            linha += '{}'.format(info_bacias[bacia][etapa][trecho]['topologia']['requer_arq_entrada'])
            linha += '{: <9} '.format(info_bacias[bacia][etapa][trecho]['topologia']['jusante'])
            linha += '{: <2}'.format(info_bacias[bacia][etapa][trecho]['topologia']['ordem_entrada'])
            linha += '{: <2}'.format(info_bacias[bacia][etapa][trecho]['topologia']['quant_dias'])
            arq_topologia.write(linha.strip()+'\n')

        vaz_entrada.to_csv(path_arq_saida, sep = '|', header = False, index=False)
        print(path_arq_saida)

        # 1-Simples Defasagem, 2-Muskingum, 3-SSARR, 4-Todini
        if info_bacias[bacia][etapa][trecho]['topologia']['metodo'] == 1:
            with open(path_arquivo_tempo_viagem, 'a+') as arq_temp_viag:
                arq_temp_viag.write('{: <20}'.format(trecho))
                arq_temp_viag.write('{:0>.1f}\n'.format((info_bacias[bacia][etapa][trecho]['tempo_viagem'])))


        elif info_bacias[bacia][etapa][trecho]['topologia']['metodo'] == 3:

            params = info_bacias[bacia][etapa][trecho]['parametros']
            with open(path_arquivo_parametros, 'a+') as arq_parametros:
                arq_parametros.write('   {:0>2}\n'.format(params['numero_fases']))
                arq_parametros.write('   {:0>3}\n'.format(params['max_iteracoes']))
                arq_parametros.write('   {:0>4.2f}\n'.format(params['erro']))
                arq_parametros.write('   {:0>3}\n'.format(params['num_dias_passado']))

            path_arquivo_vazoes_tempo = os.path.join(path, '{}_VAZOES_TEMPO.txt'.format(trecho))
            with open(path_arquivo_vazoes_tempo, 'w') as arq_vazoes_temp:
                arq_vazoes_temp.write('(-Vazão-) (TEMPO)\n')
                for vaz in info_bacias[bacia][etapa][trecho]['vaz_temp']:
                    temp = info_bacias[bacia][etapa][trecho]['vaz_temp'][vaz]
                    arq_vazoes_temp.write('{:<10.2f}{:.2f}\n'.format(vaz, temp))
            print(path_arquivo_vazoes_tempo)

    return True


def rodar_mpv(path_bat):
    os.system(path_bat)
    

def mpv_saar(data, bacias):

    global path_saida_mpv
    path_saida_mpv = os.path.join(path_saida_mpv, data.strftime('%Y%m%d'))

    if os.path.exists(path_saida_mpv):
        shutil.rmtree(path_saida_mpv)

    os.makedirs(path_saida_mpv)


    for etapa in ['Trechos']:

        for bac in bacias:
            print('\n' + bac)

            path_bac = os.path.join(path_saida_mpv, bac)

            path_bac_entrada = os.path.join(path_bac, etapa, 'Arq_Entrada')
            path_bac_saida = os.path.join(path_bac, etapa, 'Arq_Saida')
            os.makedirs(path_bac_entrada)
            os.makedirs(path_bac_saida)

            path_bat = criar_arquivo_bat(os.path.join(path_bac, etapa))
            criar_arquivo_parametros(path_bac_entrada, data)

            rodou_todos_trechos = False
            while not rodou_todos_trechos:
                rodou_todos_trechos = criar_arquivos_entrada(path_bac_entrada, bac, etapa, data)
                rodar_mpv(path_bat)
                time.sleep(2)
            
            # criar_arquivo_topologia(path_bac_entrada, bac, etapa)
            # criar_arquivos_metodos(path_bac_entrada, bac, etapa)
            # criar_arquivos_entrada(path_bac_entrada, bac, etapa, data)