
import sys
import requests
import argparse
import pandas as pd

sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.bibliotecas import  wx_download,rz_dir_tools

def get_cvu_acervo_ccee():

    url = "https://www.ccee.org.br/web/guest/busca-ccee"

    query_params = {
        "p_p_id": "org_ccee_acervo_portlet_CCEEBuscaPortlet_INSTANCE_fkzo",
        "p_p_lifecycle": 2,
        "p_p_state": "normal",
        "p_p_mode": "view",
        "p_p_cacheability": "cacheLevelPage"
    }

    form_data = {
        "_org_ccee_acervo_portlet_CCEEBuscaPortlet_INSTANCE_fkzo_q": "CVU",
        "_org_ccee_acervo_portlet_CCEEBuscaPortlet_INSTANCE_fkzo_dtIni": "Thu Feb 01 00:00:00 GMT 1990",
        "_org_ccee_acervo_portlet_CCEEBuscaPortlet_INSTANCE_fkzo_dtFim": "Mon Feb 01 00:00:00 GMT 2100",
        "_org_ccee_acervo_portlet_CCEEBuscaPortlet_INSTANCE_fkzo_structure": "",
        "_org_ccee_acervo_portlet_CCEEBuscaPortlet_INSTANCE_fkzo_structureFilter": "",
        "_org_ccee_acervo_portlet_CCEEBuscaPortlet_INSTANCE_fkzo_ordering": "Mais recente",
        "_org_ccee_acervo_portlet_CCEEBuscaPortlet_INSTANCE_fkzo_resultadosPagina": 50,
        "_org_ccee_acervo_portlet_CCEEBuscaPortlet_INSTANCE_fkzo_filtros": "",
        "_org_ccee_acervo_portlet_CCEEBuscaPortlet_INSTANCE_fkzo_numberPage": 0
    }

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
    }

    response = requests.post(url, params=query_params, data=form_data, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print("Erro ao fazer a requisição: ", response.status_code)

def download_cvu_acervo_ccee(ano_referencia, mes_referencia, pathSaida):

    DIR_TOOLS = rz_dir_tools.DirTools()
    
    TITULO_DOCUMENTO='Relatório de Reajuste do CVU'

    CVU_FILES = [
        f"CVU_PMO_{ano_referencia}{str(mes_referencia+1).zfill(2)}.zip",
        f"CVU_PMR_{ano_referencia}{str(mes_referencia).zfill(2)}.zip",
        f"CVU_Merchant_ANEEL_{ano_referencia}{str(mes_referencia).zfill(2)}.zip"
    ]

    json_data = get_cvu_acervo_ccee()

    df_produtos = pd.DataFrame(json_data['results'])
    df_cvu = df_produtos[df_produtos['nomeDocumentoList'].str.contains(TITULO_DOCUMENTO)]

    df_cvu_target = df_cvu[df_cvu['nomeDocumento'].isin(CVU_FILES)]

    if not df_cvu_target.empty:
        info_files = df_cvu_target[['url','nomeDocumento','anoReferencia', 'mesReferencia']].to_dict('records')
        for info in info_files:
            url,filename,ano_referencia,mes_referencia = info.values()
            print("\n\nBaixando Arquivo: ", info['nomeDocumento'])
            file = wx_download.downloadByRequest(url, pathSaida, filename=filename, delay=60*15)
            info['pathFile'] = file

            extracted_file = DIR_TOOLS.extract_specific_files_from_zip(
                path=file,
                files_name_template=["*Custo_Variavel_Unitario*"],
                dst=pathSaida,
                extracted_files=[]
                )    
                
            info['extractedPathFile'] = extracted_file[0]
            print(f"============================================")


        return info_files

    else:
        print("Nenhum arquivo encontrado")
        return None



def runWithParams():

    parser = argparse.ArgumentParser(
        description="Script para baixar arquivo do acervo CCEE.",
        epilog=r"python rz_download_cvu.py --pathSaida /WX2TB/Documentos/fontes/PMO/decks/ccee/nw --anoReferencia 2024 --mesReferencia 11"
        )

    parser.add_argument('--pathSaida', type=str, help="Caminho para salvar o arquivo.")
    parser.add_argument('--anoReferencia', type=int, help="Ano de referencia do arquivo.")
    parser.add_argument('--mesReferencia', type=int, help="Mês de referencia do arquivo.")
    
    # Adicione uma opção especial para exibir a mensagem de ajuda personalizada
    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)
 
    args = parser.parse_args()
    download_cvu_acervo_ccee(args.anoReferencia, args.mesReferencia, args.pathSaida)


if __name__ == '__main__':
    runWithParams()