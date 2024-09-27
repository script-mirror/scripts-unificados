

# Importar a previsao para o banco de dados
cd /WX2TB/Documentos/fontes/bibliotecas
python wx_dbUpdater.py importar_previsao_vazao_dessem "${1}"

# Gerar o email com os valores de vazao previsata pelo DESSEM
cd /WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/gerarProdutos
python gerarProdutos.py produto PREVISAO_VAZAO_DESSEM data ${2}


