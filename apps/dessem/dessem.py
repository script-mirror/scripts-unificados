# -*- coding: utf-8 -*-
import os
import sys
import datetime
import subprocess


sys.path.insert(1,"/WX2TB/Documentos/fontes/PMO/scripts_unificados/")
from apps.gerarProdutos import gerarProdutos2 
from apps.dessem.libs import  wx_relatorioIntercambio
from apps.dbUpdater.libs import deck_ds
from apps.dessem.libs.ds_ons_to_ccee import DessemOnsToCcee
GERAR_PRODUTO = gerarProdutos2.GeradorProdutos()
ons_to_ccee = DessemOnsToCcee()

def executar_deck_convertido(data):
    

    ons_to_ccee = DessemOnsToCcee()
    path_deck_convertido = ons_to_ccee.run_process()

    print(f'Iniciando simulação do Dessem para dia: {data.strftime("%d/%m/%Y")}')
    os.chdir(path_deck_convertido)
    resultado = subprocess.run(['sudo','dessem_21'], capture_output=True, text=True)
    print(resultado.stdout)
    
    file = os.path.join(path_deck_convertido, 'PDO_CMOSIST.DAT')
    deck_ds.importar_pdo_cmosist_ds(path_file = file, dt_ref=data, str_fonte='wx')
    GERAR_PRODUTO.enviar({
        "produto":"RESULTADO_DESSEM",
        "data":data,
    })
    

if __name__ == '__main__':

    now = datetime.datetime.now()
    data = datetime.datetime.strptime(now.strftime('%Y%m%d'), '%Y%m%d')


    if len(sys.argv) > 1:
        
        # Verificacao e substituicao dos valores defaults com 
        # os valores passados por parametro 
        for i in range(1, len(sys.argv[1:])):
            argumento = sys.argv[i].lower()

            if argumento in ['data','-data']:
                try:
                    data = datetime.datetime.strptime(sys.argv[i+1], "%d/%m/%Y")
                except:
                    print("Erro ao tentar converter o valor: {} em data!".format(data))
                    print(f"Utilizando a data atual: {data}")


        if 'gerarRelatorioIntercambio' in sys.argv:
            path_deck, path_result= ons_to_ccee.get_latest_deck_ccee(now)
            wx_relatorioIntercambio.analisarIntercambios(path_deck, path_result)

        if 	'executar_deck_convertido' in sys.argv:
            executar_deck_convertido(data)

    else:
        exit()

    
