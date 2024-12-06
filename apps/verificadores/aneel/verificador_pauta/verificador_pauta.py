import os
import sys
import pdb
import time
import datetime

sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.bibliotecas import  wx_dbClass,wx_emailSender
from PMO.scripts_unificados.apps.verificadores.aneel.verificador_pauta import rz_emailPautas
from PMO.scripts_unificados.apps.verificadores import rz_selenium


path_local = os.path.dirname(os.path.abspath(__file__))
PATH_TMP_DOWNLOAD = os.path.join(path_local,'tmp',str(time.time()))

if __name__ == '__main__':

    dt = datetime.datetime.now()
    database = wx_dbClass.db_mysql_master('db_config')
    database.connect()
    palavras_aneel = database.getSchema('tb_palavras_pauta_aneel')

    driver = rz_selenium.abrir_undetected_chrome(path_tmp_download=PATH_TMP_DOWNLOAD)
    data ,href,pauta, flag_diff = rz_emailPautas.get_ultima_pauta(path_local,driver)
    driver.quit()

    if flag_diff:
        texto_final,flag_envia_email = rz_emailPautas.pinta_pautas_aneel(path_local,data,href,database,palavras_aneel)
        serv_email = wx_emailSender.WxEmail()

        #Configuraçoes do gmail 
        if flag_envia_email:
            assuntoEmail = pauta
            corpoEmail = texto_final

            serv_email.username = 'pauta_aneel@climenergy.com'
            serv_email.send_to = ['joao.filho4@raizen.com']
            serv_email.password = "Clime2@sam"
            serv_email.sendEmail(texto=corpoEmail, assunto=assuntoEmail, anexos=[])
    else:
        print('Não há mudanças, email não será enviado')
        quit()
