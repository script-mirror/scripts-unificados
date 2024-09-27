# -*- coding: utf-8 -*-
import os
import sys
import pdb
import json
import datetime
import requests
import lxml.html

sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.bibliotecas import wx_dbClass


def importarCargaGeracao():

    dt_inicial = datetime.datetime.combine(datetime.date.today(), datetime.datetime.min.time())

    response = requests.get('http://www.ons.org.br/paginas/energia-agora/carga-e-geracao')

    tree = lxml.html.fromstring(response.text)

    opts_regioes = tree.cssselect("select[name='regiao'] > option")

    # nomeclatura de cordo com o banco de dados
    submercados = {	'SIN' : 'SIN', 
                    'Norte' : 'NORTE',
                    'Nordeste' : 'NORDESTE',
                    'SudesteECentroOeste' : 'SUDESTE',
                    'Sul' : 'SUL'
                }

    # Codigo utilizado no banco para armazenar
    codigoGeracao = {	
                        'Eolica':1, 
                        'Hidraulica':2,
                        'Nuclear':3,
                        'Solar':4,
                        'Termica':5
                    }

    vals_carga = []
    vals_Geracao = []

    for reg in opts_regioes:

        paramCarga = reg.attrib['value']
        sub = paramCarga.split('_')[-1]

        url = 'http://tr.ons.org.br/Content/Get/{}'.format(paramCarga)
        print(url)
        response = requests.get(url)
        carga = json.loads(response.text)

        for i, minuto in  enumerate(carga['rows']):
            horario = dt_inicial + datetime.timedelta(minutes=i)
            valorCarga = minuto['c'][1]['v']
            vals_carga.append((submercados[sub], horario, valorCarga, horario.date()))

        for ger in codigoGeracao:
            paramGer = f'Geracao_{sub}_{ger}'
            url = 'http://tr.ons.org.br/Content/Get/{}'.format(paramGer)
            print(url)
            response = requests.get(url)
            if response.status_code == 200:
                geracao = json.loads(response.text)

                tipoGeracao = paramGer.split('_')[-1]
                for i, minuto in  enumerate(geracao['rows']):
                    horario = dt_inicial + datetime.timedelta(minutes=i)
                    valorGeracao = minuto['c'][1]['v']
                    vals_Geracao.append((submercados[sub], horario, valorGeracao, codigoGeracao[tipoGeracao], horario.date()))


    db_ons = wx_dbClass.db_mysql_master("db_ons", connect=True)

    tb_carga_horaria = db_ons.db_schemas['tb_carga_horaria']
    tb_geracao_horaria = db_ons.db_schemas['tb_geracao_horaria']

    
    #DELETE
    sql_delete_carga = tb_carga_horaria.delete().where(
        tb_carga_horaria.c.dt_update == dt_inicial.strftime("%Y-%m-%d")
        )
    n_row = db_ons.db_execute(sql_delete_carga).rowcount
    print(f"{n_row} Linhas deletadas da tb_carga_horaria")

    sql_delete_geracao = tb_geracao_horaria.delete().where(
        tb_geracao_horaria.c.dt_update == dt_inicial.strftime("%Y-%m-%d")
        )
    n_row = db_ons.db_execute(sql_delete_geracao).rowcount
    print(f"{n_row} Linhas deletadas da tb_geracao_horaria")


    #INSERT
    sql_insert_carga = tb_carga_horaria.insert().values(vals_carga)
    n_row =db_ons.db_execute(sql_insert_carga).rowcount
    print(f"{n_row} Linhas inseridas da tb_geracao_horaria")

    sql_insert_geracao = tb_geracao_horaria.insert().values(vals_Geracao)
    n_row =db_ons.db_execute(sql_insert_geracao).rowcount
    print(f"{n_row} Linhas inseridas da tb_geracao_horaria")

    db_ons.db_dispose()
    return True


def printHelper():
    print("Formas de executar o script:".format(sys.argv[0]))
    print("\tpython {} importar_carga_geracao".format(sys.argv[0]))

if __name__ == '__main__':

    if len(sys.argv) > 1:
        # passar o print do help pra quando nao entrar com nenhum parametro
        if 'help' in sys.argv or '-h' in sys.argv or '-help' in sys.argv:
            printHelper()
            quit()
        
        # Verificacao e substituicao dos valores defaults com 
        # os valores passados por parametro 
        for i in range(1, len(sys.argv[1:])):
            argumento = sys.argv[i].lower()

            if argumento in ['data','-data']:
                try:
                    data = datetime.datetime.strptime(sys.argv[i+1], "%d/%m/%Y")
                except:
                    print("Erro ao tentar converter {} em data!".format(data))
                    quit()

        if 'importar_carga_geracao' in sys.argv:
            importarCargaGeracao()

    else:
        printHelper()
        exit()
