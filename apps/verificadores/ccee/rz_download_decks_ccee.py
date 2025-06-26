import os
import sys
import pdb
import datetime
import requests
import argparse
import pandas as pd
 
sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.bibliotecas import  wx_opweek,wx_download
 
 
def download_decks_acervo_ccee(pathArquivos, nome_deck_pesquisado, dtAtual=None, numDiasHistorico=35):
    """
    Baixa um arquivo do acervo CCEE.
   
    Args:
        pathArquivos (str): Caminho para salvar o arquivo.
        nome_deck_pesquisado (str): Nome do deck, Dessem, Newave, Decomp.
        dtAtual (datetime): Data atual para pesquisa.
        numDiasHistorico (int): Número de dias de histórico para pesquisa.
       
    Returns:
        bool: True se o download for bem-sucedido, False caso contrário.
    """
    print('Baixando arquivo {}'.format(nome_deck_pesquisado))
 
    # Definir valores padrão se não forem fornecidos
    dtAtual = dtAtual or datetime.datetime.now()
 
    dtFinal_str = dtAtual.strftime('%d/%m/%Y')
    dtInicial_str = (dtAtual - datetime.timedelta(days=numDiasHistorico)).strftime('%d/%m/%Y')
 
    ultimoSabado = wx_opweek.getLastSaturday(dtAtual)
    semanaEletrica = wx_opweek.ElecData(ultimoSabado.date())
 
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:94.0) Gecko/20100101 Firefox/94.0',
        'Accept': '*/*',
        'Accept-Language': 'pt-BR,en-US;q=0.7,en;q=0.3',
        'X-Requested-With': 'XMLHttpRequest',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'ADRUM': 'isAjax:true',
        'Origin': 'http://www.ccee.org.br',
        'Connection': 'keep-alive',
        'Referer': 'http://www.ccee.org.br/web/guest/acervo-ccee',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
    }
 
    params = (
        ('p_p_id', 'org_ccee_acervo_portlet_CCEEAcervoPortlet_INSTANCE_tixm'),
        ('p_p_lifecycle', '2'),
        ('p_p_state', 'normal'),
        ('p_p_mode', 'view'),
        ('p_p_cacheability', 'cacheLevelPage'),
        ('_org_ccee_acervo_portlet_CCEEAcervoPortlet_INSTANCE_tixm_param1', 'Value1'),
    )
 
    data = {
      '_org_ccee_acervo_portlet_CCEEAcervoPortlet_INSTANCE_tixm_resultadosPagina': '50',
      '_org_ccee_acervo_portlet_CCEEAcervoPortlet_INSTANCE_tixm_keyword': '{}'.format(nome_deck_pesquisado),
      '_org_ccee_acervo_portlet_CCEEAcervoPortlet_INSTANCE_tixm_numberPage': '0',
      '_org_ccee_acervo_portlet_CCEEAcervoPortlet_INSTANCE_tixm_initialDate': '{}'.format(dtInicial_str),
      '_org_ccee_acervo_portlet_CCEEAcervoPortlet_INSTANCE_tixm_finalDate': '{}'.format(dtFinal_str)
    }
    response = requests.post('https://www.ccee.org.br/web/guest/acervo-ccee', headers=headers, params=params, data=data)
    data = response.json()
 
    dataframe_produtos = pd.DataFrame(data['results'])
    
    padrao_regex = rf'{nome_deck_pesquisado} - \d{{2}}/\d{{4}}'
    if nome_deck_pesquisado == 'Newave':
        url = dataframe_produtos[dataframe_produtos['nomeDocumentoList']=='Deck de Preços - Newave'][['url']].values
    else:
        url = dataframe_produtos[dataframe_produtos['nomeComplementar'].str.contains(padrao_regex, regex=True)][['url']].values
        
    url_string = url[0][0]
    nome_arquivo = url_string.strip().split('/')[-2]

    return wx_download.downloadByRequest(url=url_string, pathSaida=pathArquivos, filename=nome_arquivo)
 
def enviar(parametros):
    """
    Executa o download do arquivo com base nos parâmetros fornecidos.
   
    Args:
        parametros (dict): Dicionário contendo os parâmetros necessários para o download.
    """
   
    # Configurações padrão
    pathArquivos = os.path.abspath('C:/WX2TB/Documentos/fontes/PMO/decks/ccee/')
    nome_arquivo_pesquisa = ''
    dtAtual = datetime.datetime.now()
    numDiasHistorico = 32
 
    print(parametros)
 
    download_decks_acervo_ccee(parametros['pathArquivos'], parametros['nome_deck_pesquisado'], parametros['dtAtual'],
                            parametros['numDiasHistorico'])
 
 
def runWithParams():
    """
    Executa o script com base nos argumentos fornecidos pela linha de comando.
    """
    parser = argparse.ArgumentParser(description="Script para baixar arquivo do acervo CCEE.",
                                     epilog=r"python rz_download_decks_ccee.py --pathArquivos /WX2TB/Documentos/fontes/PMO/decks/ccee/nw --nome_deck_pesquisado Newave --dtAtual dd/mm/yyyy --numDiasHistorico 28")
    parser.add_argument('--pathArquivos', type=str, help="Caminho para salvar o arquivo.")
    parser.add_argument('--nome_deck_pesquisado', type=str, help="Nome do deck, Dessem, Newave, Decomp.")
    parser.add_argument('--dtAtual', type=str, help="Data atual para pesquisa no formato 'dd/mm/yyyy'.")
    parser.add_argument('--numDiasHistorico', type=int, help="Número de dias de histórico para pesquisa.")
 
    # Adicione uma opção especial para exibir a mensagem de ajuda personalizada
    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)
 
    args = parser.parse_args()
 
    # Converter string de data em datetime, se fornecido
    if args.dtAtual:
        try:
            args.dtAtual = datetime.datetime.strptime(args.dtAtual, '%d/%m/%Y')
        except ValueError:
            print("Data no formato errado, utilize o formato 'dd/mm/yyyy'")
            sys.exit(1)
 
    enviar(vars(args))
 
 
if __name__ == '__main__':
    download_decks_acervo_ccee(
            pathArquivos = "/WX2TB/Documentos/fontes/PMO/decks/ccee/nw", 
            nome_deck_pesquisado = "Newave"
        ) 
    # runWithParams()
