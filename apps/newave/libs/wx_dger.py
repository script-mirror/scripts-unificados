import os
import re
import sys
import pdb
import codecs
import locale
import datetime

# path_home =  os.path.expanduser("~")
path_modulo = os.path.dirname(os.path.abspath(__file__))
path_app = os.path.dirname(path_modulo)
path_apps = os.path.dirname(path_app)
path_libs = os.path.join(os.path.dirname(path_apps), 'bibliotecas')

locale.setlocale(locale.LC_ALL, 'pt_BR')

sys.path.insert(1, path_libs)
import wx_opweek

def gerar_dger(path_arquivo_base, path_saida, data_inicio_deck):

    arquivo_base = open(path_arquivo_base)
    dger_in = arquivo_base.readlines()
    for i_linha, linha in enumerate(dger_in):
        if re.search("^PLD", linha):
            dger_in[i_linha] = f'PLD {data_inicio_deck.strftime("%B").upper()} - {data_inicio_deck.strftime("%Y")} - Raizen Power\n'
        elif re.search("^MES INICIO DO ESTUDO", linha):
            dger_in[i_linha] = f'MES INICIO DO ESTUDO   {int(data_inicio_deck.strftime("%m")):>2}\n'
        elif re.search("^ANO INICIO DO ESTUDO", linha):
            dger_in[i_linha] = f'ANO INICIO DO ESTUDO {int(data_inicio_deck.strftime("%Y")):>4}\n'

    dger_out = os.path.join(path_saida, os.path.basename(path_arquivo_base))
    arquivo_saida = open(dger_out, 'w')
    arquivo_saida.writelines(dger_in)
    print(dger_out)


if __name__ == '__main__':

    path_arquivo_base = os.path.abspath(r'C:\WX2TB\Documentos\fontes\PMO\scripts_unificados\apps\newave\arquivos\NW202306_base\DGER.DAT')
    path_saida = os.path.abspath(r'C:\WX2TB\Documentos\fontes\PMO\scripts_unificados\apps\newave\arquivos\saida')
    data_inicio_deck = datetime.datetime(2024, 1, 1)

    gerar_dger(path_arquivo_base, path_saida, data_inicio_deck)
    quit()
    
