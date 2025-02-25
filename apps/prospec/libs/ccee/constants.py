URL_API = 'https://dadosabertos.ccee.org.br/api/3/action/datastore_search'
URL_PAGE = "https://dadosabertos.ccee.org.br/dataset"

MAPPING = {

    "custo_variavel_unitario_conjuntural": {
        "resource":'9d321849-e9ee-453d-a20c-ba3a419c55de',
        "columns": {'CODIGO_MODELO_PREÇO':int,'MES_REFERENCIA':str,'CVU_CONJUNTURAL':float},
        },

    "custo_variavel_unitario_estrutural": {
        "resource":'8c388d72-f53f-4361-a216-ab342a2ab496',
        "columns": {'CODIGO_MODELO_PRECO':int,'MES_REFERENCIA':str,'CVU_ESTRUTURAL':float,'ANO_HORIZONTE':int},
        },

    "custo_variavel_unitario_conjuntural_revisado": {
        "resource":'909b5f98-78f5-49db-8716-8f340503383a',
        "columns": {'CODIGO_MODELO_PREÇO':int,'MES_REFERENCIA':str,'CVU_CONJUNTURAL':float},
        },

    "custo_variavel_unitario_merchant": {
        "resource":'74e19119-ccc4-45c5-8e07-d34d62572b03',
        "columns": {'CODIGO_MODELO_PRECO':int,'MES_REFERENCIA':str,'CVU_CF':float},
        },

    }
