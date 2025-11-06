import os
import sys
import pdb
import datetime

sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.bibliotecas import wx_emailSender
from PMO.scripts_unificados.apps.verificadores.ons import verificador_reservatorio
from PMO.scripts_unificados.apps.web_modelos.server.libs import rz_chuva

def produto_psath_diff(path_arq,limiar=1):
    limiar = 1
    template = rz_chuva.get_psath_diff(path_arq,limiar)
    return template

def produto_situacao_reservatorios():

    path_libs = os.path.dirname(os.path.abspath(__file__))
    path_gerar_produtos =  os.path.dirname(path_libs)
    path_arquivos_tmp = os.path.join(path_gerar_produtos,'arquivos','tmp')
        
    df_posto, df_submercado = verificador_reservatorio.rotina_situacao_reservatorios()
    values_granularidade = [df_posto, df_submercado]
    
    #gera html estilizado
    template = wx_emailSender.apply_html_table_style(values_granularidade,columns_to_apply='Difereça %',format = "{:.1f}%")

    # #salva html em imagem
    path_fig = wx_emailSender.api_html_to_image(template,path_save=os.path.join(path_arquivos_tmp,'situacaoReservatorios.png'))
    
    print(path_fig)

    return template,path_fig
