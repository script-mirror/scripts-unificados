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


sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.apps.gerarProdutos import gerarProdutos2 
from PMO.scripts_unificados.bibliotecas import wx_opweek,wx_ssh,wx_dbUpdater
from PMO.scripts_unificados.apps.dessem.libs import wx_operuh, wx_entdados, wx_deflant, wx_dadvaz, wx_operut, wx_respot, wx_dessemBase, wx_renovaveis, wx_cotasr11, wx_configs, wx_relatorioIntercambio, ferramentaAuxiliar, wx_converteons_ccee
from PMO.scripts_unificados.apps.dbUpdater.libs import deck_ds


configs = wx_configs.getConfiguration()

def get_name_insentive_name(dir_path):
# Defina o caminho do diretório
    nome_arquivo_procurado = os.path.basename(dir_path)

    files = glob.glob(os.path.dirname(dir_path) + '/**/*', recursive=True)

    for f in files:
        if nome_arquivo_procurado.lower() in f.lower():
            return f
        
def extractFiles(arquivoZip, destino=''):

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
    
    return path_deck_zip

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
    
    if data.weekday() == 5:
        return False
    
    path_deck_convertido = wx_converteons_ccee.converter_entdados(data)

    if executar_prospec:
        shutil.make_archive(path_deck_convertido, 'zip', path_deck_convertido)
        # TODO subir e colocar o deck executar no PROSPEC
        path_compilado_prospec_zip = 'PATH PARA O COMPILADO DO PROSPEC'
        path_compilado_prospec = extractFiles(path_compilado_prospec_zip)
        path_saida_dessem_zip = os.path.join(path_compilado_prospec,
                                f'DS{data.strftime("%Y%m%d")}.zip')
        path_saida_dessem = extractFiles(path_saida_dessem_zip)
        
    else:
        print(f'Iniciando simulação do desse para dia {data.strftime("%d/%m/%Y")}')
        os.chdir(path_deck_convertido)
        resultado = subprocess.run(['sudo','dessem_20.0.11'], capture_output=True, text=True)
        print(resultado.stdout)
        path_saida_dessem = path_deck_convertido
    
    pdocmoPath = os.path.join(path_saida_dessem, 'PDO_CMOSIST.DAT')

    importar_arquivos_dessem(lista_arquivos=[pdocmoPath], 
                             dtRodada=data-datetime.timedelta(days=1),
                             dtReferente=data,
                             fonte='ons_convertido')
    
    enviarResultadosEmail(data)


def prepararArquivosAuxiliares(data, preliminar=True):

    print('\n## Preparando os arquivos de entrada para o DESSEM ##')
    global pathArquivos

    pathArqEntrada = os.path.join(pathArquivos, data.strftime('%Y%m%d'), 'entrada')
    if not os.path.exists(pathArqEntrada):
        os.makedirs(pathArqEntrada)

    if preliminar:
        pathArqSaida = os.path.join(pathArquivos, data.strftime('%Y%m%d'), 'saida')
    else:
        pathArqSaida = os.path.join(pathArquivos, data.strftime('%Y%m%d'), 'definitiva')

    if not os.path.exists(pathArqSaida):
        os.makedirs(pathArqSaida)

    pathTemp = os.path.join(pathArquivos, 'temporarios')
    if not os.path.exists(pathTemp):
        os.makedirs(pathTemp)

    ultimoSabado = wx_opweek.getLastSaturday(data)
    dataEletrica =  wx_opweek.ElecData(ultimoSabado)

    fonteArquivos = os.path.abspath('/WX2TB/Documentos/fontes/PMO/arquivos/')
    fonteDiario = os.path.join(fonteArquivos, 'diario')

    pathArqDia = os.path.join(fonteDiario, '{}'.format(data.strftime('%Y%m%d')), 'dessem')

    # fileName = 'Blocos_{}.zip'.format((data+datetime.timedelta(days=1)).strftime('%Y-%m-%d'))
    # pathBlocosZip = os.path.join(pathArqDia, fileName)
    # pathExtractBlocos = os.path.join(pathArqEntrada, 'blocos')
    # with zipfile.ZipFile(pathBlocosZip, 'r') as zip_ref:
    # 	zip_ref.extractall(pathExtractBlocos)

    if preliminar: 
        fileName = 'Deck_Previsao_{}.zip'.format(data.strftime('%Y%m%d'))
        pathPrevEolicasZip = os.path.join(pathArqDia, fileName)
        pathPrevEolicas = os.path.join(pathArqEntrada, 'eolicas')
        if os.path.exists(pathPrevEolicas):
            shutil.rmtree(pathPrevEolicas)
        with zipfile.ZipFile(pathPrevEolicasZip, 'r') as zip_ref:
            zip_ref.extractall(pathPrevEolicas)
        # Mover todos os arquivos zip para a pasta eolicas
        src = os.path.join(pathPrevEolicas, fileName.split('.')[0])
        listaArqEolicas = os.listdir(src)
        for arq in listaArqEolicas:
            shutil.move(os.path.join(src, arq), pathPrevEolicas)
        time.sleep(1)
        os.rmdir(src)


    filesCcee = ['respot.dat', 'ils_tri.dat', 'cotasr11.dat', 'rstlpp.dat', 'dadvaz.dat', 'hidr.dat', 'curvtviag.dat', 'infofcf.dat', 
            'areacont.dat', 'termdat.dat', 'rampas.dat', 'restseg.dat', 'ptoper.dat', 'mlt.dat', 'cortdeco.rv{}'.format(dataEletrica.atualRevisao), 'respotele.dat',
            'mapcut.rv{}'.format(dataEletrica.atualRevisao), 'dessem.arq']

    pathEntradaCCEE = os.path.join(pathArqEntrada, 'ccee_entrada')

    for file in filesCcee:
        src = os.path.join(pathEntradaCCEE ,file)
        dst = os.path.join(pathArqSaida ,file)
        shutil.copy(src, dst)

    if not preliminar:
        filesOns = ['areacont.dat', 'cotasr11.dat', 'curvtviag.dat', 'dadvaz.dat', 'deflant.dat', 'hidr.dat', 'ils_tri.dat', 'mlt.dat', 'rampas.dat', 'respot.dat', 'respotele.dat',
        'termdat.dat']
        pathArqEntradaOns = os.path.join(pathArquivos, (data+datetime.timedelta(days=1)).strftime('%Y%m%d'), 'entrada', 'ons_entrada_saida')

        for file in filesOns:
            src = os.path.join(pathArqEntradaOns ,file)
            dst = os.path.join(pathArqSaida ,file)
            shutil.copy(src, dst)

def limpezaArquivosAuxiliares(data):
    pathArqEntrada = os.path.join(pathArquivos, data.strftime('%Y%m%d'), 'entrada')
    diretorios = os.listdir(pathArqEntrada)
    for diretorio in diretorios:
        try:
            shutil.rmtree(os.path.join(pathArqEntrada, diretorio))
        except:
            os.remove(os.path.join(pathArqEntrada, diretorio))

def gerarArquivosEntrada(data):
    global pathArquivos

    prepararArquivosAuxiliares(data, preliminar=True)
    wx_entdados.gerarEntdados(data, pathArquivos)
    wx_operuh.gerarOperuh(data, pathArquivos)
    wx_operut.gerarOperut(data, pathArquivos)
    wx_deflant.gerarDeflant(data, pathArquivos)
    wx_renovaveis.gerarRenovaveis(data, pathArquivos)
    wx_dadvaz.gerarDadvaz(data, pathArquivos)
    wx_respot.gerarRespot(data, pathArquivos)
    wx_cotasr11.gerarCotasr11(data, pathArquivos)

def gerarArquivosDefinitivos(data):
    global pathArquivos

    prepararArquivosAuxiliares(data, preliminar=False)
    wx_entdados.gerarEntdadosDef(data, pathArquivos)
    wx_operuh.gerarOperuhDef(data, pathArquivos)
    wx_operut.gerarOperutDef(data, pathArquivos)
    # wx_deflant.gerarDeflantDef(data, pathArquivos)
    wx_renovaveis.gerarRenovaveisDef(data, pathArquivos)
    # wx_dadvaz.gerarDadvazDef(data, pathArquivos)
    # wx_respot.gerarRespotDef(data, pathArquivos)
    # wx_cotasr11.gerarCotasr11Def(data, pathArquivos)

def rodarDS(data, importarDb=False, enviarEmail=False, preliminar=True):
    
    global pathArquivos

    pathArqEntrada = os.path.join(pathArquivos, data.strftime('%Y%m%d'), 'entrada')

    if preliminar:
        pathArqSaida = os.path.join(pathArquivos, data.strftime('%Y%m%d'), 'saida')
        fonte = 'wx_p'
    else:
        pathArqSaida = os.path.join(pathArquivos, data.strftime('%Y%m%d'), 'definitiva')
        fonte = 'wx_d'

    pathResultados = os.path.join(pathArqSaida, 'resultados')
    if not os.path.exists(pathResultados):
        os.makedirs(pathResultados)

    src = os.path.join(pathArqSaida, '*')

    cmd = 'cd {}; '.format(pathArqSaida)
    cmd = 'dessem; '

    answer = os.popen(cmd).read()

    if 'FIM DO PROCESSAMENTO' in answer.split('\n')[-2]:
        pdocmo = os.path.join(dst, 'PDO_CMOSIST.DAT')
        print('Executado com sucesso, \'PDO_CMOSIST.DAT\' copiados para a pasta:\n{}'.format(pathResultados))

        dtRodada = data
        dtReferente = data + datetime.timedelta(days=1)
        pdocmoPath = os.path.join(pathResultados, 'PDO_CMOSIST.DAT')

        if importarDb == True:
            print('\n## Importando para o banco os resultados ##')
            importarResultados(pdocmoPath, dtRodada, dtReferente, fonte=fonte, comentario='')

        if enviarEmail == True:
            print('\n## Enviando por email os resultados ##')
            enviarResultadosEmail(dtReferente)
        return True

    else:
        relato = os.path.join(dst, 'DES_LOG_RELATO.DAT')
        cmd = 'scp -r {} -P {} {}@{}:{} {}'.format(keyConfig, port, user, host, relato, pathResultados)
        os.system(cmd)
        print('Erro ao executar o dessem, \'DES_LOG_RELATO.DAT\' copiado para a pasta:\n{}'.format(pathResultados))
        return False


def rodarNW(data, importarDb=False, enviarEmail=False, preliminar=True):

    global pathArquivos

    print('\n## Enviando os arquivos para o servidor NW ##')
    pathArqEntrada = os.path.join(pathArquivos, data.strftime('%Y%m%d'), 'entrada')

    if preliminar:
        dst = os.path.join(configs['paths']['dessemNW'], data.strftime('%Y%m%d'), 'preliminar')
        pathArqSaida = os.path.join(pathArquivos, data.strftime('%Y%m%d'), 'saida')
        fonte = 'wx_p'
    else:
        dst = os.path.join(configs['paths']['dessemNW'], data.strftime('%Y%m%d'), 'definitiva')
        pathArqSaida = os.path.join(pathArquivos, data.strftime('%Y%m%d'), 'definitiva')
        fonte = 'wx_d'

    pathResultados = os.path.join(pathArqSaida, 'resultados')
    if not os.path.exists(pathResultados):
        os.makedirs(pathResultados)

    src = os.path.join(pathArqSaida, '*')

    ssh = wx_ssh.SSH()

    port = configs['ssh']['portaNW']
    user = configs['ssh']['user']
    host = configs['ssh']['host']


    keyConfig = ''
    if configs['ssh']['pathKey'] != "":
        keyConfig = '-i {}'.format(configs['ssh']['pathKey'])
        ssh.key_filename = configs['ssh']['pathKey']
        ssh.reconnect()

    # Criacao da pasta na NW
    answer = ssh.exec_cmd('mkdir -p \'{}\''.format(dst))

    # Envia os arquivos de entrada a NW
    cmd = 'scp -r {} -P {} {} {}@{}:{}'.format(keyConfig, port, src, user, host, dst)
    os.system(cmd)

    print('\n## Execussao do modelo ##')
    cmd = 'ssh {} -p {}  {}@{} \'cd {}; dessem_19.0.14.1.3\''.format(keyConfig, port, user, host, dst)
    answer = os.popen(cmd).read()

    if 'FIM DO PROCESSAMENTO' in answer.split('\n')[-2]:
        pdocmo = os.path.join(dst, 'PDO_CMOSIST.DAT')
        cmd = 'scp -r {} -P {} {}@{}:{} {}'.format(keyConfig, port, user, host, pdocmo, pathResultados)
        print('Executado com sucesso, \'PDO_CMOSIST.DAT\' copiados para a pasta:\n{}'.format(pathResultados))
        os.system(cmd)

        dtRodada = data
        dtReferente = data + datetime.timedelta(days=1)
        pdocmoPath = os.path.join(pathResultados, 'PDO_CMOSIST.DAT')

        if importarDb == True:
            print('\n## Importando para o banco os resultados ##')
            importarResultados(pdocmoPath, dtRodada, dtReferente, fonte=fonte, comentario='')

        if enviarEmail == True:
            print('\n## Enviando por email os resultados ##')
            enviarResultadosEmail(dtReferente)
        return True

    else:
        relato = os.path.join(dst, 'DES_LOG_RELATO.DAT')
        cmd = 'scp -r {} -P {} {}@{}:{} {}'.format(keyConfig, port, user, host, relato, pathResultados)
        os.system(cmd)
        print('Erro ao executar o dessem, \'DES_LOG_RELATO.DAT\' copiado para a pasta:\n{}'.format(pathResultados))
        return False


def importarResultados(pdocmoPath, dtRodada, dtReferente, fonte='wx', comentario='', enviarEmail=False):
    
    wx_dbUpdater.insertRodadaDessem(pdocmoPath, dtRodada, dtReferente, fonte, comentatio='')

    if enviarEmail == True:
        enviarResultadosEmail(dtReferente)

    data_referente = dtRodada + datetime.timedelta(days=1)
    path_renovaveis = os.path.join(os.path.dirname(pdocmoPath.replace('ccee_saida','ccee_entrada')),'renovaveis.dat')
    wx_dbUpdater.importar_renovaveis_dessem(data_deck=data_referente, path_arquivo=path_renovaveis, fonte=fonte)

    # if enviarEmail == True:
        #envia figura de previsao de carga e carga liquida no whats
        # enviarPrevCargaWhats(dtReferente)

def enviarResultadosEmail(dtReferente):
    GERAR_PRODUTO = gerarProdutos2.GerardorProdutos()

    GERAR_PRODUTO.enviar({
        "produto":"RESULTADO_DESSEM",
        "data":dtReferente,
    })
    # cmd = 'cd /WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/gerarProdutos;'
    # cmd += 'python gerarProdutos.py produto RESULTADO_DESSEM data {};'.format(dtReferente.strftime('%d/%m/%Y'))
    # os.system(cmd)

# def enviarPrevCargaWhats(dtReferente):
#     cmd = 'cd /WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/gerarProdutos;'
#     cmd += 'python gerarProdutos.py produto PREVISAO_CARGA_DESSEM data {};'.format(dtReferente.strftime('%d/%m/%Y'))
#     os.system(cmd)

def gerarArqRodarNW(data, importarDb=False, enviarEmail=False):
    gerarArquivosEntrada(data)
    rodarNW(data, importarDb=importarDb, enviarEmail=enviarEmail, preliminar=True)

def gerarRodarDefinitiva(data, importarDb=False, enviarEmail=False):
    gerarArquivosDefinitivos(data)
    rodarNW(data, importarDb=importarDb, enviarEmail=enviarEmail, preliminar=False)

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
        enviarResultadosEmail(dtReferente)

def atualizarHistoricoFaltanteCcee(pathZipFile):

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

    dtasAtualizar = ferramentaAuxiliar.verificarDatasFaltantes(dtInicial,ftFinal)

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




def organizarArquivosOns(data, pathArquivos, importarDb=False, enviarEmail=False):

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
    deckDessemOnsPath = os.path.join(pathArqDia, fileName)

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

    

def gerarRelatorioIntercambio(data):
    # pass
    # arquivosSaidaCcee = os.path.abspath(r'C:\Users\thiag\Documents\git\wx_alpha_\apps\dessem\arquivos\20210614\entrada\ccee_saida')
    # arquivosEntradaCcee = os.path.abspath(r'C:\Users\thiag\Documents\git\wx_alpha_\apps\dessem\arquivos\20210614\entrada\ccee_entrada')
    
    arquivosSaidaCcee = os.path.join(pathArquivos, data.strftime('%Y%m%d'), 'entrada', 'ccee_saida')
    arquivosEntradaCcee = os.path.join(pathArquivos, data.strftime('%Y%m%d'), 'entrada', 'ccee_entrada')

    analisarRestricoes(arquivosSaidaCcee, arquivosEntradaCcee)

def printHelper():
    """ Imprime na tela o helper da funcao
    :param None: 
    :return None: 
    """
    dt_hoje = datetime.datetime.now()
    dt_amanha = dt_hoje + datetime.timedelta(days=1)
    ultimoSabado = dt_hoje
    while ultimoSabado.weekday() != 5:
        ultimoSabado -= datetime.timedelta(days=1)
    ultimoSabado = wx_opweek.ElecData(ultimoSabado.date())

    print()
    print("## Executar DS ##")
    print("python {} gerarExecutarPreliminar data {} importarDb true enviarEmail true".format(sys.argv[0], dt_hoje.strftime('%d/%m/%Y')))
    print("python {} gerarExecutarDefinitiva data {} importarDb true enviarEmail true".format(sys.argv[0], dt_hoje.strftime('%d/%m/%Y')))
    print("python {} executarRodada data {}  importarDb true enviarEmail true preliminar false".format(sys.argv[0], dt_hoje.strftime('%d/%m/%Y')))
    print('python {} executar_deck_convertido data {}'.format(sys.argv[0], dt_amanha.strftime('%d/%m/%Y')))
    print("\n## Gerar arquivos de entrada ##")
    print("python {} gerarTodosArquivos data {}".format(sys.argv[0], dt_hoje.strftime('%d/%m/%Y')))
    print("python {} gerarArquivoEntDados data {}".format(sys.argv[0], dt_hoje.strftime('%d/%m/%Y')))
    print("python {} gerarArquivoOperuh data {}".format(sys.argv[0], dt_hoje.strftime('%d/%m/%Y')))
    print("python {} gerarArquivoOperut data {}".format(sys.argv[0], dt_hoje.strftime('%d/%m/%Y')))
    print("python {} gerarArquivoDeflant data {}".format(sys.argv[0], dt_hoje.strftime('%d/%m/%Y')))
    print("python {} gerarArquivoRenovaveis data {}".format(sys.argv[0], dt_hoje.strftime('%d/%m/%Y')))
    print("python {} gerarArquivoDadvaz data {}".format(sys.argv[0], dt_hoje.strftime('%d/%m/%Y')))
    print("python {} gerarArquivoRespot data {}".format(sys.argv[0], dt_hoje.strftime('%d/%m/%Y')))
    print("python {} gerarArquivoCotR11 data {}".format(sys.argv[0], dt_hoje.strftime('%d/%m/%Y')))
    print("\n## Organizar arquivos recebidos ##")
    print("python {} baixarArquivosCCEE data {} importarDb true enviarEmail true".format(sys.argv[0], dt_hoje.strftime('%d/%m/%Y')))
    print("python {} organizarArquivosOns data {} importarDb true enviarEmail true".format(sys.argv[0], dt_amanha.strftime('%d/%m/%Y')))
    print('python {} atualizarHistoricoCcee arquivo {}'.format(sys.argv[0], os.path.join(pathArquivos,'temporarios','DES_{}{:0>2}.zip'.format(ultimoSabado.anoReferente,ultimoSabado.mesReferente))))
    print("\n## Tratar e gerar relatórios ##")
    print("python {} gerarRelatorioIntercambio data {}".format(sys.argv[0], dt_hoje.strftime('%d/%m/%Y')))
    print()
    print("Obs: A data referente sera para o dia seguinte da data passada por parametro")


if __name__ == '__main__':

    now = datetime.datetime.now()
    data = datetime.datetime.strptime(now.strftime('%Y%m%d'), '%Y%m%d')

    importarDb = False
    enviarEmail = False
    if len(sys.argv) > 1:
        # passar o print do help pra quando nao entrar com nenhum parametro
        if 'help' in sys.argv or '-h' in sys.argv or '-help' in sys.argv:
            printHelper()
            quit()
        
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
                    enviarEmail = True
                elif sys.argv[i+1].lower() == 'false':
                    enviarEmail = False
                else:
                    print('Erro ao tentar converter {} para True ou False'.format(sys.argv[i+1]))

            elif argumento in ['arquivo']:
                arquivo = sys.argv[i+1]


        if 'gerarExecutarPreliminar' in sys.argv:
            if data.weekday() == 4:
                print('Ainda nao e possivel gerar arquivos de entrada do DS para o inicio da semana eletrica')
                quit()

            gerarArqRodarNW(data, importarDb=importarDb, enviarEmail=enviarEmail)
            quit()


        if 'gerarExecutarDefinitiva' in sys.argv:
            if data.weekday() == 4:
                print('Ainda nao e possivel gerar arquivos de entrada do DS para o inicio da semana eletrica')
                quit()

            gerarRodarDefinitiva(data, importarDb=importarDb, enviarEmail=enviarEmail)
            quit()

        if 'executarRodada' in sys.argv:
            pdocmoPath = rodarDS(data, importarDb=importarDb, enviarEmail=enviarEmail, preliminar=preliminar)
            quit()

        if 'gerarTodosArquivos' in sys.argv:
            gerarArquivosEntrada(data)
            quit()

        if 'gerarArquivoEntDados' in sys.argv:
            prepararArquivosAuxiliares(data)
            wx_entdados.gerarEntdados(data, pathArquivos)

        if 'gerarArquivoOperuh' in sys.argv:
            prepararArquivosAuxiliares(data)
            wx_operuh.gerarOperuh(data, pathArquivos)

        if 'gerarArquivoOperut' in sys.argv:
            prepararArquivosAuxiliares(data)
            wx_operut.gerarOperut(data, pathArquivos)

        if 'gerarArquivoDeflant' in sys.argv:
            prepararArquivosAuxiliares(data)
            wx_deflant.gerarDeflant(data, pathArquivos)

        if 'gerarArquivoRenovaveis' in sys.argv:
            prepararArquivosAuxiliares(data)
            wx_renovaveis.gerarRenovaveis(data, pathArquivos)
            
        if 'gerarArquivoDadvaz' in sys.argv:
            prepararArquivosAuxiliares(data)
            wx_dadvaz.gerarDadvaz(data, pathArquivos)

        if 'gerarArquivoRespot' in sys.argv:
            prepararArquivosAuxiliares(data)
            wx_respot.gerarRespot(data, pathArquivos)

        if 'gerarArquivoCotR11' in sys.argv:
            prepararArquivosAuxiliares(data)
            wx_cotasr11.gerarCotasr11(data, pathArquivos)
            
        if 'baixarArquivosCCEE' in sys.argv:
            baixarArquivosCcee(data, pathArquivos, importarDb, enviarEmail)

        if 'organizarArquivosOns' in sys.argv:
            organizarArquivosOns(data, pathArquivos, importarDb, enviarEmail)

        if 'gerarRelatorioIntercambio' in sys.argv:
            pathArquivosEntradaDessem = os.path.join(pathArquivos, '{}'.format(data.strftime('%Y%m%d')), 'entrada', 'ccee_entrada')
            pathArquivosSaidaDessem = os.path.join(pathArquivos, '{}'.format(data.strftime('%Y%m%d')), 'entrada', 'ccee_saida')
            wx_relatorioIntercambio.analisarIntercambios(pathArquivosEntradaDessem, pathArquivosSaidaDessem)

        if 	'atualizarHistoricoCcee' in sys.argv:
            atualizarHistoricoFaltanteCcee(arquivo)

        if 	'executar_deck_convertido' in sys.argv:
            executar_deck_convertido(data)

        # limpezaArquivosAuxiliares(data)

    else:
        printHelper()
        exit()

    
