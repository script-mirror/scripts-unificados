from datetime import datetime
import os
import re
import pdb
import glob
import time
import shutil
import zipfile

def sumario_cmo(path):

    modulo_path = os.path.abspath(__file__)
    libs_path = os.path.dirname(modulo_path)
    app_path = os.path.dirname(libs_path)
    tmp_path = os.path.join(app_path, 'arquivos', 'tmp', 'deck_preliminar')

    if os.path.exists(tmp_path):
        shutil.rmtree(tmp_path)

    os.makedirs(tmp_path)

    with zipfile.ZipFile(path, 'r') as zip_ref:
        time.sleep(1)
        zip_ref.extractall(tmp_path)

    for arquivo in glob.glob(os.path.join(tmp_path, '*.zip')):
        nome_arquivo = os.path.basename(arquivo)
        if 'RESULTADOS' in nome_arquivo:
            # assunto = nome_arquivo.split('.zip')[0]

            match = re.match(r'RESULTADOS_DEC_ONS_([0-9]{2})([0-9]{4})_RV([0-9]{1})_VE.zip', nome_arquivo)
            mes = match.group(1)
            ano = match.group(2)
            rv = match.group(3)

            assunto = 'CMO {}/{} (RV{})'.format(mes, ano, rv)
            path_resultados = os.path.join(tmp_path, 'resultados')

            with zipfile.ZipFile(arquivo, 'r') as zip_ref:
                time.sleep(1)
                zip_ref.extractall(path_resultados)

            for sumario in glob.glob(os.path.join(path_resultados, 'sumario.rv*')):
                f_sumario = open(sumario).readlines()

                i_linha = 0
                for i_linha in range(0, len(f_sumario)):
                    if 'CUSTO MARGINAL DE OPERACAO  ($/MWh)' in f_sumario[i_linha]:
                        i_linha += 2
                        semanas = [''] + f_sumario[i_linha].split()[1:]
                        i_linha += 2
                        cmo = {}
                        for i_sub, sub in enumerate(['SE', 'S', 'NE', 'N']):
                            cmo[sub] = []
                            cmo[sub].append(semanas)
                            cmo[sub].append(['Pat. Pesado'] + f_sumario[i_linha + (i_sub*4)].split()[1:])
                            cmo[sub].append(['Pat. Médio'] + f_sumario[i_linha + (i_sub*4) + 1].split()[1:])
                            cmo[sub].append(['Pat. Leve'] + f_sumario[i_linha + (i_sub*4) + 2].split()[1:])
                            cmo[sub].append(['Media'] + f_sumario[i_linha + (i_sub*4) + 3].split()[1:])
    
    shutil.rmtree(tmp_path)
    return assunto, cmo


            





if __name__ == '__main__':

    path = os.path.abspath(r'C:\Users\cs341052\Downloads\deck_preliminar\PMO_deck_preliminar.zip')
    sumario_cmo(path)
