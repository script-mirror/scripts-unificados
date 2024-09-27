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

def gerar_sistema(path_arquivo_base, path_saida, data_inicio_deck, IGP_DI):

    arquivo_base = open(path_arquivo_base)
    sistema_in = arquivo_base.readlines()

    i_linha = 0
    flags_regex = re.compile("CUSTO DO DEFICIT|INTERCAMBIO|ENERGIA|GERACAO")
    flag = None
    while i_linha < len(sistema_in):
        linha = sistema_in[i_linha]

        if flags_regex.search(linha):

            if re.search("CUSTO DO DEFICIT", linha):
                flag = 'custo_intercambio'
            elif re.search("INTERCAMBIO", linha):
                flag = 'intercambio'
            elif re.search("ENERGIA", linha):
                flag = 'energia'
            elif re.search("GERACAO", linha):
                flag = 'geracao'

        # Atualizacao do valor com o acumulado IGP-DI dos 12 ultimos anos
        if flag == 'custo_intercambio':
            if re.search("^ [1-4] ", linha):
                newline_values = linha.split()
                newline_values[3] = float(newline_values[3])*(1+IGP_DI/100)
                custo_intercambio_regex = ' {:<3} {:<10}  {:>1} {:>5.2f} {:>7} {:>7} {:>7} {:>5} {:>5} {:>5} {:>5}\n'
                sistema_in[i_linha] = custo_intercambio_regex.format(*newline_values)

        # Repete o valor do ultimo ano
        elif flag == 'intercambio':
            if re.search(f"^{data_inicio_deck.year - 1}", linha):
                del sistema_in[i_linha]
                idx_duplicate = i_linha+3
                newline = sistema_in[idx_duplicate]
                newline_values = newline.split()
                newline_values[0] = data_inicio_deck.year+4
                intercambio_regex = "{:<7}{:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7}\n"
                sistema_in.insert(idx_duplicate+1, intercambio_regex.format(*newline_values))

        # Utiliza a mesma taxa de crescimento do ultimo ano para o ano adicionado
        elif flag in ['energia', 'geracao']:
            if re.search(f"^{data_inicio_deck.year - 1}", linha):
                del sistema_in[i_linha]

                idx_penultimo_ano = i_linha+2
                penultimo_ano = sistema_in[idx_penultimo_ano]
                valores_penultimo_ano = penultimo_ano.split()

                idx_ultimo_ano = idx_penultimo_ano+1
                ultimo_ano = sistema_in[idx_ultimo_ano]
                valores_ultimo_ano = ultimo_ano.split()

                try:
                    valores_ano_previsto = [round((float(x) * float(x) / float(y)), 0) for x, y in zip(valores_ultimo_ano, valores_penultimo_ano)]
                except:
                    valores_ano_previsto = [round(float(x)) for x in valores_ultimo_ano]
                bloco_regex = "{:<7.0f}{:>6.0f}. {:>6.0f}. {:>6.0f}. {:>6.0f}. {:>6.0f}. {:>6.0f}. {:>6.0f}. {:>6.0f}. {:>6.0f}. {:>6.0f}. {:>6.0f}. {:>6.0f}.\n"

                sistema_in.insert(idx_ultimo_ano+1, bloco_regex.format(*valores_ano_previsto))

        i_linha += 1

    sistema_out = os.path.join(path_saida, os.path.basename(path_arquivo_base))
    arquivo_saida = open(sistema_out, 'w')
    arquivo_saida.writelines(sistema_in)
    print(sistema_out)


if __name__ == '__main__':

    path_arquivo_base = os.path.abspath(r'C:\WX2TB\Documentos\fontes\PMO\scripts_unificados\apps\newave\arquivos\NW202306_base\SISTEMA.DAT')
    path_saida = os.path.abspath(r'C:\WX2TB\Documentos\fontes\PMO\scripts_unificados\apps\newave\arquivos\saida')
    data_inicio_deck = datetime.datetime(2024, 1, 1)

    gerar_sistema(path_arquivo_base, path_saida, data_inicio_deck)
    quit()
    
