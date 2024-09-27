import os
import pdb
import shutil
import datetime
import requests


def getPrevisaoGeada(dataPrevisao, pathSaida):

    numDiasPrevisto = 4

    arquivosBaixados = []
    print('Baixando previsões de geada')
    for i in range(numDiasPrevisto):
        dtPrevista = dataPrevisao + datetime.timedelta(days=i)
        url = 'http://sisdagro.inmet.gov.br/sisdagro/app/previsao/geada/mapa/{}/RiscoG/fixa/z1'.format(dtPrevista.strftime('%d-%m-%Y'))
        resposta = requests.get(url)
        respostaJson = resposta.json()

        urlMapa = 'http://sisdagro.inmet.gov.br/sisdagro/app/monitoramento/download/img/{}'.format(respostaJson['mapa'])
        print(' > {}'.format(urlMapa))
        response = requests.get(urlMapa, stream=True)
        
        arquivoSaida = os.path.join(pathSaida, 'previsaoGeada{}_{}.png'.format(dataPrevisao.strftime('%Y%m%d'), dtPrevista.strftime('%Y%m%d')))
        print('   \'->{}\n'.format(arquivoSaida))
        with open(arquivoSaida, 'wb') as out_file:
            shutil.copyfileobj(response.raw, out_file)
            arquivosBaixados.append(arquivoSaida)
        
        del response

    return arquivosBaixados

if __name__ == '__main__':

    data = datetime.datetime(2021,7,21)

    pathLocal = os.path.abspath('.')
    pathSaida = os.path.join(os.path.dirname(pathLocal), 'arquivos', 'tmp')
    getPrevisaoGeada(data, pathSaida)