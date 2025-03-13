import os
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")),'.env'))

__HOST_SERVIDOR = os.getenv('HOST_SERVIDOR')
__USER_EMAIL_CV = os.getenv('USER_EMAIL_CV') 
__PASSWORD_EMAIL_CV = os.getenv('PASSWORD_EMAIL_CV')  

URL_COGNITO = os.getenv('URL_COGNITO')
CONFIG_COGNITO = os.getenv('CONFIG_COGNITO')

WHATSAPP_API = os.getenv('WHATSAPP_API')

NUM_GILSEU = os.getenv('NUM_GILSEU')

PRODUTCS_MAPPING = {

        'SST': "processar_produto_SST",
        'PRECIPTACAO_MODELO': "processar_produto_PRECIPTACAO_MODELO",
        'IPDO': "processar_produto_IPDO",
        'ACOMPH': "processar_produto_ACOMPH",
        'RDH': "processar_produto_RDH",
        'RELATORIO_BBCE': "processar_produto_RELATORIO_BBCE",
        'RESULTADO_DESSEM': "processar_produto_RESULTADO_DESSEM",
        'PREVISAO_ENA_SUBMERCADO': "processar_produto_PREVISAO_ENA_SUBMERCADO",
        'PREVISAO_CARGA_DESSEM': "processar_produto_PREVISAO_CARGA_DESSEM",
        'RESULTADO_CV': "processar_produto_RESULTADO_CV",
        'RELATORIO_CV': "processar_produto_RELATORIO_CV",
        'DIFERENCA_CV': "processar_produto_DIFERENCA_CV",
        'RESULTADO_PREVS': "processar_produto_RESULTADO_PREVS",
        'RESULTADOS_PROSPEC': "processar_produto_RESULTADOS_PROSPEC",
        'PREVISAO_GEADA': "processar_produto_PREVISAO_GEADA",
        'REVISAO_CARGA': "processar_produto_REVISAO_CARGA",
        'CMO_DC_PRELIMINAR': "processar_produto_CMO_DC_PRELIMINAR",
        'GERA_DIFCHUVA': "processar_produto_GERA_DIFCHUVA",
        'REVISAO_CARGA_NW': "processar_produto_REVISAO_CARGA_NW",
        'PSATH_DIFF': "processar_produto_PSATH_DIFF",
        'SITUACAO_RESERVATORIOS': "processar_produto_SITUACAO_RESERVATORIOS",
        'REVISAO_CARGA_NW_PRELIMINAR': "processar_produto_REVISAO_CARGA_NW_preliminar",
        'TABELA_WEOL_MENSAL': "processar_produto_TABELA_WEOL_MENSAL",
        'TABELA_WEOL_SEMANAL': "processar_produto_TABELA_WEOL_SEMANAL",
        'PREV_ENA_CONSISTIDO':"processar_produto_prev_ena_consistido"
        
    } 