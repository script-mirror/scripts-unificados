import os
import requests
import datetime
import pandas as pd
from bs4 import BeautifulSoup

from .constants import (
    MAPPING,
    URL_API,
    URL_PAGE,
    )

def get_date_atualizacao(title_name):

    for title_name in MAPPING.keys():

        serach_url = f"{URL_PAGE}/{title_name}"

        response = requests.get(serach_url)

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")
            
            section = soup.find("section", class_="additional-info")
            tabela = section.find("tbody")

            if tabela:
                linhas_tabela = tabela.find_all("th")
                
                for i, linha in enumerate(linhas_tabela):
                    if "atualização" in linha.text.lower():
                
                        text_atualizacao = linha.text
                
                        date_atualizacao = datetime.datetime.strptime(
                            tabela.find_all("tr")[i].find("span").text.strip(),
                            "%B %d, %Y, %H:%M (BRT)"
                            )
                
                        print(f"Data de atualização: {date_atualizacao.strftime('%d de %B de %Y')}")
                        return date_atualizacao
            else:
                print("❌ Nenhuma tabela encontrada com a classe 'additional-info'.")
        else:
            print(f"❌ Erro ao acessar a página: {response.status_code}")


def get_data(resource_id, limit=32000, init_offset=0):

    all_records = []
    while True:
        # Montar a URL com offset
        url = f"{URL_API}?resource_id={resource_id}&limit={limit}&offset={init_offset}"
        
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Erro ao buscar dados: {response.status_code}")
            break
        
        records = response.json().get('result', {}).get('records', [])

        if not records:
            break
        
        all_records.extend(records)
        
        init_offset += limit

    return all_records


def search_info(title):

    resource_id = MAPPING.get(title,{}).get('resource')
    if not resource_id:
        print(f"Recurso não encontrado para: {title}")
        return pd.DataFrame()
    
    records = get_data(
        resource_id=resource_id,
    )
    info = pd.DataFrame(records)
    
    return info[MAPPING[title]['columns'].keys()].astype(MAPPING[title]['columns'])
