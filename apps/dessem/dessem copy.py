# -*- coding: utf-8 -*-
import os
import re
import sys
import pdb
import glob
import time
import shutil
import zipfile
import datetime
import subprocess

diretorioApp = os.path.dirname(os.path.abspath(__file__))
pathArquivos = os.path.join(diretorioApp,'arquivos')

path_decks = os.path.abspath("/WX2TB/Documentos/fontes/PMO/decks")


sys.path.insert(1,"/WX2TB/Documentos/fontes/PMO/scripts_unificados/")
from apps.gerarProdutos import gerarProdutos2 
from bibliotecas import wx_dbUpdater
from apps.dessem.libs import  wx_relatorioIntercambio
from apps.dbUpdater.libs import deck_ds
from apps.dessem.libs.ds_ons_to_ccee import DessemOnsToCcee



"""def get_name_insentive_name(dir_path):
# Defina o caminho do diretório
    nome_arquivo_procurado = os.path.basename(dir_path)

    files = glob.glob(os.path.dirname(dir_path) + '/**/*', recursive=True)

    for f in files:
        if nome_arquivo_procurado.lower() in f.lower():
            return f
        """
"""def extractFiles(arquivoZip, destino=''):

    if destino == '':
        zipNameFile = os.path.basename(arquivoZip)
        path = os.path.dirname(arquivoZip)
        destino = os.path.join(path, zipNameFile.split('.')[0])

    if os.path.exists(destino):
        shutil.rmtree(destino)

    if not os.path.exists(destino):
        os.makedirs(destino)

    with zipfile.ZipFile(arquivoZip, 'r') as zip_ref:
        zip_ref.extractall(destino)

    return destino

def converter_deck_ons_para_ccee(data):
    
    path_deck_convertido = wx_converteons_ccee.converter_entdados(data)
    path_deck_zip = f'{path_deck_convertido}'
    shutil.make_archive(path_deck_zip, 'zip', path_deck_convertido)
    
    return path_deck_zip"""

def importar_arquivos_dessem(lista_arquivos, dtRodada, dtReferente,
                             fonte='wx', comentario=''):
    
    for arquivo in lista_arquivos:
        
        if os.path.basename(arquivo).lower() == 'pdo_cmosist.dat':
            deck_ds.importar_pdo_cmosist_ds(path_file = arquivo, dt_ref=data, str_fonte=fonte)

        elif os.path.basename(arquivo).lower() == 'renovaveis.dat':
            deck_ds.importar_renovaveis_ds(path_file = arquivo, dt_ref=data, str_fonte=fonte)
            
        else:
            print(f'Não encontrei nenhum tratamento para o arquivo:\n{arquivo}')

def executar_deck_convertido(data, executar_prospec=False):
    
    #if data.weekday() == 5:
        #return False
    ons_to_ccee = DessemOnsToCcee()
    path_deck_convertido = ons_to_ccee.run_process()
    #path_deck_convertido = wx_converteons_ccee.converter_entdados(data)
    print(f'Iniciando simulação do desse para dia {data.strftime("%d/%m/%Y")}')
    os.chdir(path_deck_convertido)
    resultado = subprocess.run(['sudo','dessem_21'], capture_output=True, text=True)
    print(resultado.stdout)
    
    pdocmoPath = os.path.join(path_deck_convertido, 'PDO_CMOSIST.DAT')

    importar_arquivos_dessem(lista_arquivos=[pdocmoPath], 
                             dtRodada=data-datetime.timedelta(days=1),
                             dtReferente=data,
                             fonte='ons_convertido')
    
    enviarResultadosEmail(data)

"""def limpezaArquivosAuxiliares(data):
    pathArqEntrada = os.path.join(pathArquivos, data.strftime('%Y%m%d'), 'entrada')
    diretorios = os.listdir(pathArqEntrada)
    for diretorio in diretorios:
        try:
            shutil.rmtree(os.path.join(pathArqEntrada, diretorio))
        except:
            os.remove(os.path.join(pathArqEntrada, diretorio))

"""

def importarResultados(pdocmoPath, dtRodada, dtReferente, fonte='wx', comentario='', enviarEmail=False):
    
    wx_dbUpdater.insertRodadaDessem(pdocmoPath, dtRodada, dtReferente, fonte, comentatio='')

    if enviarEmail == True:
        enviarResultadosEmail(dtReferente)

    data_referente = dtRodada + datetime.timedelta(days=1)
    path_renovaveis = os.path.join(os.path.dirname(pdocmoPath.replace('ccee_saida','ccee_entrada')),'renovaveis.dat')
    wx_dbUpdater.importar_renovaveis_dessem(data_deck=data_referente, path_arquivo=path_renovaveis, fonte=fonte)

def enviarResultadosEmail(dtReferente):
    GERAR_PRODUTO = gerarProdutos2.GeradorProdutos()

    GERAR_PRODUTO.enviar({
        "produto":"RESULTADO_DESSEM",
        "data":dtReferente,
    })
"""
def baixarArquivosCcee(data, pathArquivos, importarDb=False, enviarEmail=False):
    for tentativa in range(40):
        print('Tentativa {} ({})'.format(tentativa+1, datetime.datetime.now().strftime('%H:%M')))
        pathArquivoDessem = wx_dessemBase.downloadArquivoBase(pathArquivos, dtAtual=data)
        if wx_dessemBase.organizarArquivoBase(data, pathArquivoDessem):
            break
        print('Arquivo ainda nao saiu, nova tentativa em 5 minutos!')
        time.sleep(60*10)

    if importarDb == True:
        dtRodada = data-datetime.timedelta(days=1)
        dtReferente = data
        pathEntrada = os.path.join(pathArquivos, dtReferente.strftime('%Y%m%d'), 'entrada')
        pdocmoPath = os.path.join(pathEntrada, 'ccee_saida', 'PDO_CMOSIST.DAT')
        importarResultados(pdocmoPath, dtRodada, dtReferente, fonte='ccee', comentario='')

    if enviarEmail == True:
        enviarResultadosEmail(dtReferente)"""

"""def atualizarHistoricoFaltanteCcee(pathZipFile):

    if not os.path.exists(pathZipFile):
        raise Exception('Arquivo %s nao encontrado' % pathZipFile)

    nomeArquivoZipCompleto = os.path.basename(pathZipFile)
    match = re.match(r'DES_([0-9]{4})([0-9]{2}).zip', nomeArquivoZipCompleto)
    anoRef = match.group(1)
    mesRef = match.group(2)

    pathDessem = os.path.join(pathArquivos, 'temporarios', 'DES_{}{}'.format(anoRef,mesRef))
    if not os.path.exists(pathDessem):
        os.makedirs(pathDessem)
    else:
        shutil.rmtree(pathDessem)
        time.sleep(1)

    with zipfile.ZipFile(pathZipFile, 'r') as zip_ref:
        zip_ref.extractall(pathDessem)

    diaPrimeiro = datetime.datetime(int(anoRef),int(mesRef),1)
    dtInicial = wx_opweek.getLastSaturday(diaPrimeiro)

    primeiroDiaProxRv = wx_opweek.getLastSaturday(diaPrimeiro + datetime.timedelta(days=31))
    ftFinal = primeiroDiaProxRv - datetime.timedelta(days=1)

    dtasAtualizar = verificarDatasFaltantes(dtInicial,ftFinal)

    pdb.set_trace()

    for dt in dtasAtualizar:
        if dt in [datetime.date(2022,1,2),datetime.date(2022,1,8)]:
            continue
        dtElec = wx_opweek.ElecData(wx_opweek.getLastSaturday(dt))
        pathResultDiario = os.path.join(pathDessem, 'Resultado_DS_CCEE_{}{}_SEMREDE_RV{}D{:0>2}'.format(dtElec.mesReferente,dtElec.anoReferente,dtElec.atualRevisao,dt.day))
        if os.path.exists(pathResultDiario):
            shutil.rmtree(pathResultDiario)
            time.sleep(1)

        with zipfile.ZipFile(pathResultDiario+'.zip', 'r') as zip_ref:
            zip_ref.extractall(pathResultDiario)

        pdocmoPath = os.path.join(pathResultDiario,'PDO_CMOSIST.DAT')
        importarResultados(pdocmoPath, dt-datetime.timedelta(days=1), dt, fonte='ccee', comentario='')

    shutil.rmtree(pathDessem)

"""
"""
def verificarDatasFaltantes(dtInicial,ftFinal):

	wxdb = wx_dbLib.WxDataB()
	dtFormat = wxdb.getDateFormat()
	
	if ftFinal > datetime.datetime.now():
		ftFinal = datetime.datetime.combine(datetime.date.today(), datetime.datetime.min.time())

	sql = '''
			SELECT
				DT_REFERENTE
			from
				TB_CADASTRO_DESSEM
			WHERE
				DT_REFERENTE >= \'{}\'
				and DT_REFERENTE <= \'{}\'
				AND TX_FONTE = 'ccee'
			GROUP BY
				DT_REFERENTE'''.format(dtInicial.strftime(dtFormat), ftFinal.strftime(dtFormat))
	
	answer = wxdb.requestServer(sql)
	diasInseridos = [r[0] for r in answer]

	diasFaltantes = []
	dt = dtInicial.date()
	while dt <= ftFinal.date():
		if dt not in diasInseridos:
			diasFaltantes.append(dt)
		dt += datetime.timedelta(days=1)

	return diasFaltantes
"""


"""def organizarArquivosOns(data, pathArquivos, importarDb=False, enviarEmail=False):

    pathArqEntrada = os.path.join(pathArquivos, data.strftime('%Y%m%d'), 'entrada')
    if not os.path.exists(pathArqEntrada):
        os.makedirs(pathArqEntrada)

    ultimoSabado = wx_opweek.getLastSaturday(data)
    dataEletrica =  wx_opweek.ElecData(ultimoSabado)

    fonteArquivos = os.path.abspath('/WX2TB/Documentos/fontes/PMO/arquivos/')
    fonteDiario = os.path.join(fonteArquivos, 'diario')
    pathArqDia = os.path.join(fonteDiario, '{}'.format(data.strftime('%Y%m%d')), 'dessem')

    fileName = 'ds_ons_{:0>2}{}_rv{}d{}.zip'.format(dataEletrica.mesReferente, dataEletrica.anoReferente, dataEletrica.atualRevisao, data.strftime('%d'))
    folderName, _zipextension = fileName.split('.')
    zips_disponiveis = glob.glob(os.path.join(pathArqDia, "*.zip"))
    deckDessemOnsPath = os.path.join(pathArqDia, fileName)
    for _zip in zips_disponiveis:
        if _zip.lower() in deckDessemOnsPath.lower():
            old_path = os.path.join(pathArqDia, _zip)
            new_path = deckDessemOnsPath
            os.rename(old_path, new_path)
            break

    pathDeckExtract = os.path.join(pathArqEntrada, 'ons_entrada_saida')
    pathDeckExtract2 = os.path.join(path_decks, 'ons', 'ds', folderName)
    for pathDst in [pathDeckExtract, pathDeckExtract2]:

        if os.path.exists(pathDst):
            shutil.rmtree(pathDst) 
        with zipfile.ZipFile(deckDessemOnsPath, 'r') as zip_ref:
            zip_ref.extractall(pathDst)
        print(pathDst)

    src = os.path.join(pathDeckExtract, fileName.split('.')[0])
    # as vezes os arquivos podem vir dentro de outra pasta
    if os.path.exists(src):
        listaArqDeck = os.listdir(src)
        for arq in listaArqDeck:
            shutil.move(os.path.join(src, arq), pathDeckExtract)
        time.sleep(1)
        os.rmdir(src)

    dtRodada = data - datetime.timedelta(days=1)
    dtReferente = data
    pathEntrada = os.path.join(pathArquivos, dtReferente.strftime('%Y%m%d'), 'entrada')
    pdocmoPath = os.path.join(pathEntrada, 'ons_entrada_saida', 'pdo_cmosist.dat')
    pdocmoPath = get_name_insentive_name(pdocmoPath)

    if importarDb == True:
        importarResultados(pdocmoPath, dtRodada, dtReferente, fonte='ons', comentario='',enviarEmail=enviarEmail)
"""

if __name__ == '__main__':

    now = datetime.datetime.now()
    data = datetime.datetime.strptime(now.strftime('%Y%m%d'), '%Y%m%d')

    importarDb = False
    enviarEmail = False
    if len(sys.argv) > 1:
        
        # Verificacao e substituicao dos valores defaults com 
        # os valores passados por parametro 
        for i in range(1, len(sys.argv[1:])):
            argumento = sys.argv[i].lower()

            if argumento in ['data','-data']:
                try:
                    data = datetime.datetime.strptime(sys.argv[i+1], "%d/%m/%Y")
                except:
                    print("Erro ao tentar converter {} em data!".format(data))
                    quit()

            elif argumento in ['preliminar']:
                if sys.argv[i+1].lower() == 'true':
                    preliminar = True
                elif sys.argv[i+1].lower() == 'false':
                    preliminar = False

            elif argumento in ['importardb']:
                if sys.argv[i+1].lower() == 'true':
                    importarDb = True
                elif sys.argv[i+1].lower() == 'false':
                    importarDb = False
                else:
                    print('Erro ao tentar converter {} para True ou False'.format(sys.argv[i+1]))

            elif argumento in ['enviaremail']:
                if sys.argv[i+1].lower() == 'true':
                    enviarEmail = TrueS
                elif sys.argv[i+1].lower() == 'false':
                    enviarEmail = False
                else:
                    print('Erro ao tentar converter {} para True ou False'.format(sys.argv[i+1]))

            elif argumento in ['arquivo']:
                arquivo = sys.argv[i+1]


        #if 'organizarArquivosOns' in sys.argv:
            #organizarArquivosOns(data, pathArquivos, importarDb, enviarEmail)

        if 'gerarRelatorioIntercambio' in sys.argv:
            pathArquivosEntradaDessem = os.path.join(pathArquivos, '{}'.format(data.strftime('%Y%m%d')), 'entrada', 'ccee_entrada')
            pathArquivosSaidaDessem = os.path.join(pathArquivos, '{}'.format(data.strftime('%Y%m%d')), 'entrada', 'ccee_saida')
            wx_relatorioIntercambio.analisarIntercambios(pathArquivosEntradaDessem, pathArquivosSaidaDessem)

        #if 	'atualizarHistoricoCcee' in sys.argv:
            #atualizarHistoricoFaltanteCcee(arquivo)

        if 	'executar_deck_convertido' in sys.argv:
            executar_deck_convertido(data)

        # limpezaArquivosAuxiliares(data)

    else:
        exit()

    
