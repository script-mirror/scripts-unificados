import os
import re
import pdb
import sys
import datetime
import requests

sys.path.insert(1,"/WX2TB/Documentos/fontes/PMO/scripts_unificados/")
from bibliotecas import wx_opweek,wx_download


def download_arquivo_acervo(pathArquivos, nome_arquivo_pesquisa, regex_nome_arquivo=None ,dtAtual=None, numDiasHistorico=28):

    print('Baixando arquivo {}'.format(nome_arquivo_pesquisa))

    if dtAtual == None:
        dtAtual = datetime.datetime.now()
        
    if regex_nome_arquivo == None:
        regex_nome_arquivo = nome_arquivo_pesquisa

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
        'Origin': 'https://www.ccee.org.br',
        'Connection': 'keep-alive',
        'Referer': 'https://www.ccee.org.br/web/guest/acervo-ccee',
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
      '_org_ccee_acervo_portlet_CCEEAcervoPortlet_INSTANCE_tixm_resultadosPagina': '10',
      '_org_ccee_acervo_portlet_CCEEAcervoPortlet_INSTANCE_tixm_keyword': '{}'.format(nome_arquivo_pesquisa),
      '_org_ccee_acervo_portlet_CCEEAcervoPortlet_INSTANCE_tixm_numberPage': '0',
      '_org_ccee_acervo_portlet_CCEEAcervoPortlet_INSTANCE_tixm_initialDate': '{}'.format(dtInicial_str),
      '_org_ccee_acervo_portlet_CCEEAcervoPortlet_INSTANCE_tixm_finalDate': '{}'.format(dtFinal_str)
    }

    response = requests.post('https://www.ccee.org.br/web/guest/acervo-ccee', headers=headers, params=params, data=data)
    data = response.json()

    for result in data['results']:
        if re.match(regex_nome_arquivo, result['nomeComplementar']):
            url = result['url']
            break

    nome_arquivo = url.strip().split('/')[-2]

    if not os.path.exists(pathArquivos):
        os.makedirs(pathArquivos)

    return wx_download.downloadByRequest(url=url, pathSaida=pathArquivos, filename=nome_arquivo)


if __name__ == '__main__':
    
    pathArquivos = os.path.abspath(r'C:\Users\cs341052\Downloads')
    nome_arquivo_pesquisa = 'Deck decomp'
    regex_nome_arquivo = 'Decomp - _[0-9]{2} - [0-9]{2}/[0-9]{4}'
    dtAtual = datetime.datetime.now()
    numDiasHistorico = 32
    download_arquivo_acervo(pathArquivos, nome_arquivo_pesquisa, regex_nome_arquivo ,dtAtual, numDiasHistorico)