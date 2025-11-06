import os
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")),'.env'))

URL_COGNITO = os.getenv('URL_COGNITO')
CONFIG_COGNITO = os.getenv('CONFIG_COGNITO')
WHATSAPP_API = os.getenv('WHATSAPP_API')
NUM_GILSEU = os.getenv('NUM_GILSEU')

PRODUCTS_MAPPING = {

        'ACOMPH': "processar_produto_ACOMPH",
        'ACOMPH_TABELA': "processar_produto_ACOMPH_tabelas_whatsapp",
        'RDH': "processar_produto_RDH",
        'RELATORIO_BBCE': "processar_produto_RELATORIO_BBCE",
        'RESULTADO_DESSEM': "processar_produto_RESULTADO_DESSEM",
        'PREVISAO_CARGA_DESSEM': "processar_produto_PREVISAO_CARGA_DESSEM",
        'SITUACAO_RESERVATORIOS': "processar_produto_SITUACAO_RESERVATORIOS",
        'FSARH':'processar_produto_FSARH',
    } 