import os
import re
import sys
import pdb
import glob
import json
import shutil
import zipfile
import difflib
import datetime

from tabulate import tabulate
from dotenv import load_dotenv

home_path =  os.path.expanduser("~")
path_downloads = os.path.join(home_path,"Downloads")

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")),'.env'))

sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.apps.prospec.libs.requestsProspecAPI import getToken
from PMO.scripts_unificados.apps.prospec.libs.requestsProspecAPI import getInfoFromAPI
from PMO.scripts_unificados.apps.prospec.libs.requestsProspecAPI import postInAPI
from PMO.scripts_unificados.apps.prospec.libs.requestsProspecAPI import getFileFromAPI
from PMO.scripts_unificados.apps.prospec.libs.requestsProspecAPI import sendFileToAPI
from PMO.scripts_unificados.apps.prospec.libs.requestsProspecAPI import patchInAPI
from PMO.scripts_unificados.apps.prospec.libs.requestsProspecAPI import getFileFromS3viaAPIV2
from PMO.scripts_unificados.apps.prospec.libs.requestsProspecAPI import getCompilationFromAPI



def recursiveDiff(dir1, dir2):
    different_files = []
    missing_files = []
    
    files1 = glob.glob(os.path.join(dir1, '**'), recursive=True)
    files1_ = set([f.replace(files1[0], "") for f in files1[1:]])
    
    files2 = glob.glob(os.path.join(dir2, '**'), recursive=True)
    files2_ = set([f.replace(files2[0], "") for f in files2[1:]])
    
    arquivos_novos = files2_ - files1_
    arquivos_faltantes = files1_ - files2_
    
    if (arquivos_novos):
        print("\nArquivos novos:")
        for f in list(arquivos_novos):
            print(f"\t+ {os.path.join(files1[0],f)}")
        
    if (arquivos_faltantes):
        print("\nArquivos faltantes:")
        for f in list(arquivos_faltantes):
            print(f"\t- {f}")
            files1_ -= arquivos_faltantes
    
    arquivos_diferentes = set({})
    print("\nDiferenças:")
    for f in files1_:
        file_path1 = os.path.join(files1[0],f)
        file_path2 = os.path.join(files2[0],f)
        if os.path.isfile(file_path1) and os.path.isfile(file_path2):
            with open(file_path1, 'r', encoding="ISO-8859-1") as f1, open(file_path2, 'r', encoding="ISO-8859-1") as f2:
                diff = list(difflib.unified_diff(f1.readlines(), f2.readlines(), fromfile=file_path1, tofile=file_path2))
                if diff:
                    arquivos_diferentes.add(f)
                    print(f"\tDif - {file_path1}")
    print("\n")
    
    return arquivos_novos, arquivos_faltantes, arquivos_diferentes
    
    
class RzProspec():
    def __init__(self):
        self.username = os.getenv("API_PROSPEC_USERNAME")
        self.password = os.getenv("API_PROSPEC_PASSWORD")
        self.token = None

    def _getToken(self):
        if self.username and self.password:
            self.token = getToken(self.username, self.password)
        else:
            raise Exception(f'Verificar o {os.path.join("home_path",".env")}\nAPI_PROSPEC_USERNAME = ""\nAPI_PROSPEC_PASSWORD = ""')

    def getInfo(self, endpoint):
        if not self.token:
            self._getToken()

        return getInfoFromAPI(self.token, endpoint)
    
    def getFile(self, endpoint:str, fileNameDownload:str, pathToDownload:str):
        if not self.token:
            self._getToken()
            
        pathToDownload = os.path.join(pathToDownload,'')
        if not os.path.exists(pathToDownload):
            os.makedirs(pathToDownload)
            
        getFileFromAPI(self.token, endpoint, fileNameDownload, pathToDownload)
        
        return os.path.join(pathToDownload,fileNameDownload)

    def getFilesS3(self, endpoint:str, listaArquivos:list, fileName:str, pathToDownload:str):
        if not self.token:
            self._getToken()
        getFileFromS3viaAPIV2(self.token, endpoint, listaArquivos, fileName, pathToDownload)
        
    def getCompilation(self, endpoint:str, fileName:str, pathToDownload:str):
        if not self.token:
            self._getToken()
            
        getCompilationFromAPI(self.token, endpoint,fileName, pathToDownload)
    
    def sendFile(self, endpoint:str, fullPathFile:str):
    
        if not self.token:
            self._getToken()
            
        return sendFileToAPI(self.token, endpoint, fullPathFile, os.path.basename(fullPathFile))
    
    def post(self, endpoint:str, parameter:str, data:dict):
        
        if not self.token:
            self._getToken()

        return postInAPI(self.token, endpoint, parameter, data)
    
    def modificarDeck(self, endpoint:str, parameter, data):
        
        if not self.token:
            self._getToken()
            
        return patchInAPI(self.token, endpoint, parameter, data)

    def getNumeroRequisicoes(self):
        return self.getInfo('/api/Account/Requests')
    
    def getInfoRodadasPorData(self,dtRef:datetime.datetime):

        pag = 1
        header = ['Id', 'CreationDate', 'Title', 'Status']
        tabela = []

        dtInicial = dtRef.replace(hour=0, minute=0, second=0)
        dtFinal = dtInicial + datetime.timedelta(days=1)
        
        verificar_proxima_pagina = True
        while verificar_proxima_pagina:
            info_rodadas = self.getInfo(f'/api/v2/prospectiveStudies?page={pag}&pageSize=20')

            for info in info_rodadas['ProspectiveStudies']:
                dtCreation = datetime.datetime.strptime(info['CreationDate'][:16], '%Y-%m-%dT%H:%M') 
                if dtCreation >= dtInicial and dtCreation <= dtFinal:
                    info['CreationDate'] = dtCreation.strftime('%d/%m/%Y %H:%M')
                    linha = []
                    for key in header:
                        linha.append(info[key])
                    tabela.append(linha)
                elif dtCreation < dtInicial:
                    verificar_proxima_pagina = False
            pag += 1
            
        print(tabulate(tabela, headers=header))
        
    def getInfoRodadaPorId(self,idEstudo:int):
        
        prospecStudy = self.getInfo(f'/api/prospectiveStudies/{idEstudo}')
        
        return prospecStudy
        
    def downloadEstudoPorId(self,idEstudo:int,downloadsFolder=True):

        fileNameDownload = f'Estudo_{idEstudo}.zip'

        if downloadsFolder:
            pathToDownload = path_downloads
        else:
            pathToDownload = os.path.abspath('Download Decks')
            
        fullPath = self.getFile(f'/api/prospectiveStudies/{idEstudo}/DeckDownload', fileNameDownload, pathToDownload)

        print(fullPath)
        return fullPath
    
    def duplicarDeck(self, idEstudo:int, titulo:str='', descricao:str='', 
                     tags:list=[], vazoesDat:int=0, vazoesRvx:int=0,
                     prevsCondition:int=0):
        
        # VazoesDatCondition(integer)
        # 0 - padrão, não faz nenhuma mudança no decks.
        # 1 - exclui todos os vazoes.dat de todos os decks DECOMP do estudo.
        # 2 - exclui os vazoes.dat do segundo deck DECOMP em diante.

        # VazoesRvxCondition(integer)
        # 0 - Modo padrão
        # 1 - exclui todos os vazoes.rvX de todos os decks DECOMP do estudo.
        # 2 - exclui os vazoes.rvX do segundo deck DECOMP em diante.

        # PrevsCondition(integer)
        # 0 - Modo padrão
        # 1 - exclui todos os prevs, inclusive sensibilidade, de todos os decks DECOMP do estudo.
        # 2 - exclui somente os prevs sensibilidade do primeiro deck DECOMP e dos demais decks exclui todos os prevs, inclusive sensibilidade

        listaTags = []

        for tag in tags:
            tagsConfiguration = {}
            tagsConfiguration['Text'] = tag
            listaTags.append(tagsConfiguration)

        parameter = ''
        
        data = {
            "Title": titulo,
            "Description": descricao,
            "Tags": listaTags,
            "VazoesDatCondition": vazoesDat,
            "VazoesRvxCondition": vazoesRvx,
            "PrevsCondition": prevsCondition
        }
        
        endpoint = f'/api/prospectiveStudies/{idEstudo}/Duplicate'
        
        prospecStudyId = self.post(endpoint, parameter, data)
        
        return prospecStudyId
    
        
    
    def downloadCompilacao(self, idsEstudo:list, esperarFim:bool):
        
        for _id in idsEstudo:
            endpoint = f'/api/prospectiveStudies/{_id}//CompilationDownload?compilacaoCompleta={esperarFim}'
            pathToDownload = os.path.join(path_downloads,"")
            fileName = f'compilation_{_id}.zip'
            self.getCompilation(endpoint, fileName, pathToDownload)
    

    def atualizar_estudo(self,diretorioEstudo:str):

        match = re.match(r'.+Estudo_(\d+)', diretorioEstudo)
        idEstudo = match.group(1)
        
        pathDeckRemoto = self.downloadEstudoPorId(idEstudo,downloadsFolder=False)
        pathFileUnzip = pathDeckRemoto.replace(".zip","")
        if os.path.exists(pathFileUnzip):
            shutil.rmtree(pathFileUnzip)

        with zipfile.ZipFile(pathDeckRemoto, 'r') as zip_ref:
            zip_ref.extractall(pathFileUnzip)
        
        arquivos_novos, arquivos_faltantes, arquivos_diferentes = recursiveDiff(pathFileUnzip, diretorioEstudo)
        
        while 1:
            escolha = input("Deseja subir todos os itens modificado/novos? [1=sim, 0=não]: ")
            if escolha.strip() in ["1", "0"]:
                escolha = int(escolha.strip())
                break
        
        if escolha:
            estudoProspec = self.getInfoRodadaPorId(idEstudo)
            listaDeDecks = estudoProspec['Decks']
            for arq in arquivos_diferentes:
                fullPathFile = os.path.join(diretorioEstudo,arq)
                nome_deck = os.path.dirname(arq).upper()
                for deck in listaDeDecks:
                    if nome_deck == deck['FileName'].upper().replace(".ZIP",""):
                        idDeck = deck["Id"]
                        endpoint = f'/api/prospectiveStudies/{idEstudo}/UploadFiles?deckId={idDeck}'
                        prospecStudy = self.sendFile(endpoint, fullPathFile)
                        if 'filesUploaded' in prospecStudy:
                            print(f'{prospecStudy["filesUploaded"][0]} - OK')
                            break

    def update_tags(self, idEstudo:int, tags:list, textColor:str, backgroundColor:str):
        tags_delete = self.getInfoRodadaPorId(idEstudo)['Tags']
        tags_prefix = [x['Text'].split(' ')[0] for x in tags]
        
        for tag in tags_delete:
            if tag.split(' ')[0] not in tags_prefix:
                tags_delete.remove(tag)
            
        parameter = ''
        url = f'/api/prospectiveStudies/{idEstudo}/'
        self.modificarDeck(url+"RemoveTags",
                parameter, tags_delete)
        
        tags_add = []
        for tag in tags:
            tagsConfiguration = {}
            tagsConfiguration['Text'] = tag
            tagsConfiguration['TextColor'] = textColor
            tagsConfiguration['BackgroundColor'] = backgroundColor
            tags_add.append(tagsConfiguration)
        
        self.modificarDeck(url+"AddTags",
                parameter, tags_add)

    def modificar_tags(self,idEstudo:int,tags:list,tipoModificacao:str):
        
        listaTags = []

        for tag in tags:
            tagsConfiguration = {}
            tagsConfiguration['Text'] = tag
            listaTags.append(tagsConfiguration)

        parameter = ''
        
        data = listaTags

        url = f'/api/prospectiveStudies/{idEstudo}/'
        if tipoModificacao == 'add':
            url = url+ 'AddTags'
        elif tipoModificacao == 'remove':
            url = url+ 'RemoveTags'

        self.modificarDeck(url,
                parameter, data)
        
    def getIdsNewave(self):
        return self.getInfo('/api/CepelModels/Newaves')
    def getIdsDecomp(self):
        return self.getInfo('/api/CepelModels/Decomps')
    def getIdsGevazp(self):
        return self.getInfo('/api/CepelModels/Gevazps')
    def getIdsDessem(self):
        return self.getInfo('/api/CepelModels/Dessems')
            
    def criarEstudo(self, titulo, descricao, pathDecks, tags=[], idDecomp=None, idNewave=None, idGevazp=None, idDessem=0):
        
        if idDecomp == None:
            ids = self.getIdsDecomp()
            idDecomp = list(filter(lambda infoVersao: infoVersao['Default'] == True, ids))[0]['Id']
        if idNewave == None:
            ids = self.getIdsNewave()
            idNewave = list(filter(lambda infoVersao: infoVersao['Default'] == True, ids))[0]['Id']
        if idGevazp == None:
            ids = self.getIdsGevazp()
            idGevazp = list(filter(lambda infoVersao: infoVersao['Default'] == True, ids))[0]['Id']
        
        parameter = ''
                    
        data = {
            "Title": titulo,
            "Description": descricao,
            "DecompVersionId": int(idDecomp),
            "NewaveVersionId": int(idNewave),
            "GevazpVersionId": int(idGevazp),
            "DessemVersionId": int(idDessem)
        }

        idEstudo = self.post('/api/prospectiveStudies', parameter, data)
        
        endpoint = f'/api/prospectiveStudies/{idEstudo}/UploadFiles'
        arquivo_enviado = self.sendFile(endpoint, pathDecks)
        
        listaTags = []

        for tag in tags:
            tagsConfiguration = {}
            tagsConfiguration['Text'] = tag
            listaTags.append(tagsConfiguration)
        
        data = {
            "Tags": listaTags,
            "FileName": os.path.basename(pathDecks)
        }
    
        answer = self.post(f'/api/prospectiveStudies/{idEstudo}/Complete', parameter, data)
            
        return idEstudo
        
    def iniciarSimulacao(self,idEstudo):
        pdb.set_trace()

def loop():
    api_prospec = RzProspec()

    while True:
        print()
        print("1 - Conferir número de requisições")
        print("2 - Rodadas de uma data")
        print("3 - Info rodada por ID")
        print("4 - Download estudo por ID")
        print("5 - Atualizar estudo")
        print("6 - Add tags")
        print("7 - Remove tags")
        print("8 - Duplicar estudo")
        print("9 - Download compilado")
        # print("10 - Executar estudo")
        print("11 - Criar estudo")
        print("12 - IDs das versoes disponíveis")
        
        # enviar arquivos
        # executar
        opt = input("Escolha uma opção: ")
        opt = int(opt)
        
        if opt == 1:
            print(api_prospec.getNumeroRequisicoes())
            
        elif opt == 2:
            dtRef = input("Escolha uma data: ").strip()
            if dtRef != '':
                dtRef = datetime.datetime.strptime(dtRef, "%d/%m/%Y")
            else:
                dtRef = datetime.datetime.now()
            api_prospec.getInfoRodadasPorData(dtRef)
            
        elif opt == 3:
            idEstudo = input('id: ').strip()
            info = api_prospec.getInfoRodadaPorId(idEstudo)
            print(json.dumps(info, indent = 2))
            
        elif opt == 4:
            idEstudo = input('id: ').strip()
            api_prospec.downloadEstudoPorId(idEstudo)
            
        elif opt == 5:
            path = input("Entre com o caminho: ")
            api_prospec.atualizar_estudo(path)
            
        elif opt in (6,7):
            idEstudo = input('id: ').strip()
            tags = input("Tags separadas por virgula: ")
            # tags = [t.strip() for t in tags.split(',')]
            tags = tags.split(',')
            
            if opt == 6:
                tipoModificacao = 'add'
            elif opt == 7:
                tipoModificacao = 'remove'
                
            api_prospec.modificar_tags(idEstudo=idEstudo,tags=tags,tipoModificacao=tipoModificacao)
        
        elif opt == 8:
            idEstudo = input('id do estudo a ser duplicado: ').strip()
            titulo = input("Título: ")
            descricao = "" #input("Descrição: ")
            tags = input("Tags separadas por virgula: ")
            tags = tags.split(',')
            vazoesDat = int(input("Deletar todos os vazoes.dat[1=sim, 0=não]: "))
            vazoesRvx = int(input("Deletar todos os vazoes.rvx[1=sim, 0=não]: "))
            prevsCondition = int(input("Deletar todos os prevs[1=sim, 0=não]: "))
            
            api_prospec.duplicarDeck(
                idEstudo, titulo, descricao, tags, 
                vazoesDat, vazoesRvx,prevsCondition
            )
        
        elif opt == 9:
            idsEstudo = input("IDs separadas por virgula: ")
            idsEstudo = [i.strip() for i in idsEstudo.split(',')]
            api_prospec.downloadCompilacao(idsEstudo, False)
        
        elif opt == 10:
            idEstudo = input('id: ').strip()
            api_prospec.iniciarSimulacao(idsEstudo)
            
        elif opt == 11:
            titulo = input("Título: ")
            descricao = input("Descrição: ")
            tags = input("Tags separadas por virgula: ")
                        
            tags = tags.split(',')
            api_prospec.criarEstudo(titulo,descricao,pathDecks,tags)
            
        elif opt == 12:
            modelo = int(input("modelo [0=nw, 1=dc, 2=ds, 3=gvazp]: "))
            
            if modelo == 0:
                informacaos = api_prospec.getIdsNewave()
                modelo = 'NEWAVE'
            elif modelo == 1:
                informacaos = api_prospec.getIdsDecomp()
                modelo = 'DECOMP'
            elif modelo == 2:
                informacaos = api_prospec.getIdsDessem()
                modelo = 'DESSEM'
            elif modelo == 3:
                informacaos = api_prospec.getIdsGevazp()
                modelo = 'GEVAZP'
                
            pdb.set_trace()
            for info in informacaos:
                if info['Default']:
                    print(f"{info['Model']} v{info['Version']} : {info['Id']}")
                else:
                    print(f"{info['Model']} v{info['Version']}(oficial) : {info['Id']}")

            
if __name__ == '__main__':
    loop()