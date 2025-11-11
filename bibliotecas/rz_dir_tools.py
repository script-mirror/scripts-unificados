import os
import re
import glob
import shutil
import zipfile
import fnmatch
import datetime
import requests
from email.header import decode_header, make_header
import zipfile

class DirTools():

    def get_name_insentive_name(self,dir_path):
    # Defina o caminho do diretório
        nome_arquivo_procurado = os.path.basename(dir_path)

        files = glob.glob(os.path.dirname(dir_path) + '/**/*', recursive=True)

        for f in files:
            if nome_arquivo_procurado.lower() in f.lower():
                return f
        
        return None   
    
    def copy_src(self, src,dst,replace_text=[], upper=False):

        print("Procurando por: ", src)
        search_result = glob.glob(src, recursive=True)
        if not search_result: search_result = glob.glob(pathname= os.path.join(os.path.dirname(src),os.path.basename(src).lower()), recursive=True)
        os.makedirs(dst, exist_ok=True)

        dst_out = dst
        path_file = []
        for src_path in search_result:

            name = os.path.basename(src_path)
            
            if replace_text:
                name = name.replace(replace_text[0],replace_text[1])

            if upper:
                file_name, file_extension = os.path.splitext(name)
                name = file_name.upper() + file_extension.lower()
            
            dst_out = os.path.join(dst,name)

            path_file += shutil.copy(src_path, dst_out),

        return path_file
    
    def remove_src(self, src):
        if os.path.isdir(src):
            print(f"{src} será deletado junto com suas subpastas!")
            shutil.rmtree(src)
        else:
            print(f"{src} será deletado!")
            os.remove(src)
    
    def downloadFile(self,url, path=''):

        url_http = re.sub(r'https://', 'http://', url)
        r = requests.get(url_http)
        contentDisposition = r.headers['content-disposition']
        nomeArquivo = re.findall("filename=(.+)", contentDisposition)[0]

        # Alguns nomes dos produtos precisam ser decodificados e outros nao
        try:
                nomeArquivo = nomeArquivo.decode("utf-8")
        except:
                pass

        if '=?utf-8' in nomeArquivo:
                nomeArquivo = str(make_header(decode_header(nomeArquivo))).replace('"','').strip()
        if path != '':
            if not os.path.exists(path):
                    os.makedirs(path)

        
        caminhoArquivo = os.path.join(path, nomeArquivo.replace('"','').strip())
        if os.path.exists(caminhoArquivo):
                os.remove(caminhoArquivo)

        with open(caminhoArquivo, 'wb') as f:
                f.write(r.content)

        return caminhoArquivo
    
    def extract(self,zipFile, path, deleteAfterExtract=False):

        if not os.path.exists(path):
            os.makedirs(path)

        with zipfile.ZipFile(zipFile, 'r') as zip_ref:
            zip_ref.extractall(path)

        if deleteAfterExtract:
            os.remove(zipFile)

        return path

    def extrair_zip_mantendo_nome_diretorio(self, zipFile,path_out=None, deleteAfterExtract=False):
        try:
            # Verifica se o caminho fornecido é um arquivo zip
            if not zipfile.is_zipfile(zipFile):
                print(f"O arquivo '{zipFile}' não é um arquivo zip.Não precisou descompactar!!")
                return None

            # Obtendo o nome do diretório onde o arquivo zip está localizado
            zip_directory = os.path.dirname(zipFile) if not path_out else path_out
            # Obtendo o nome do arquivo zip
            zip_name = os.path.basename(zipFile)
            # Removendo a extensão .zip do nome do arquivo
            folder_name = os.path.splitext(zip_name)[0]
            # Caminho para o diretório onde o conteúdo será extraído
            extract_path = os.path.join(zip_directory, folder_name)

            # Cria o diretório de extração se não existir
            if not os.path.exists(extract_path):
                os.makedirs(extract_path)
            # Extrai o conteúdo do arquivo zip para o diretório de extração
            with zipfile.ZipFile(zipFile, 'r') as zip_ref:
                zip_ref.extractall(extract_path)
            # Remove o arquivo zip após a extração, se especificado
            if deleteAfterExtract:
                os.remove(zipFile)
            return extract_path
        except FileNotFoundError:
            print(f"O arquivo '{zipFile}' não foi encontrado.")
            return None

    def get_date_patterns(self,date_format:str):

        '''
            Parameters
            ----------
                date_format: Formato da data presente no arquivo
            
            Returns
            -------
                date_pattern: Regex para a data especificada
            
        '''

        date_pattern = date_format\
            .replace("%d",r"(\d{2})")\
            .replace("%m",r"(\d{2})")\
            .replace("%Y",r"(\d{4})")\
            .replace("%y",r"(\d{2})")

        if date_pattern != date_format:
            return date_pattern
        else:
            raise Exception(
                "Formato não esta no padrao de data %d, %m, %y ou %Y!",
                )

    def extrair_data(self,nome_arquivo:str,file_template:str,date_format:str):

        '''
            Parameters
            ----------
                nome_arquivo: Nome real do arquivo no caminho
                file_template: Formato do nome do arquivo colocando o {} no lugar da data
                date_format: Formato da data presente no arquivo
            
            Returns
            -------
                None, se não dar match
                ou data com formato datetime presente no arquivo encontrado
            
        '''

        date_pattern = self.get_date_patterns(date_format)
        match = re.search(date_pattern, nome_arquivo)
        if match:
            if (
                nome_arquivo.replace(match.group(0),date_format).lower() 
                == 
                file_template.format(date_format).lower()
                ) or (
                
                fnmatch.fnmatch(nome_arquivo.lower(),file_template.replace('{}','*').lower())
            ):  
                return datetime.datetime.strptime(nome_arquivo, nome_arquivo.replace(match.group(0),date_format))
            else:
                return None
        else:
            return None
        
    def extract_specific_files_from_zip(self,path:str,files_name_template:list ,dst:str,date_format:str=None, data_ini:datetime = None,extracted_files:list=[]):
        '''
            Parameters
            ----------
                path:   
                    caminho + nome do arquivo zip

                files_name_template:    
                    Lista de formatos de nomes de arquivo colocando o {} no lugar da data, a serem extraidos
                
                date_format:    
                    Formato da data presente no arquivo
                
                dst:    
                    Diretorio de destino dos arquivos extraidos
                
                data_ini:   
                    Data inicial, utilizada para pegar arquivos com a data  
                    >= a essa data_ini, caso não seja especificada, não sera 
                    executada essa condiçao

            Returns
            -------
                extracted_files: Lista com os arquivos extraidos
            
        '''
        with zipfile.ZipFile(path, 'r') as zip:
            all_files = zip.infolist()
            for arquivo in all_files:
                if not arquivo.is_dir() and not "zip" in arquivo.filename:
                    for file_template in files_name_template:
                        if date_format:
                            if self.extrair_data(arquivo.filename, file_template, date_format) and (
                                not data_ini 
                                or self.extrair_data(arquivo.filename, file_template, date_format) >= data_ini
                            ):
                                extracted_files +=  zip.extract(arquivo.filename,dst),
                        
                        elif fnmatch.fnmatch(arquivo.filename.lower(),file_template.replace('{}','*').lower()):
                            extracted_files +=  zip.extract(arquivo.filename,dst),
                            
                elif "zip" in arquivo.filename:
                    with zip.open(arquivo) as dir_inside_zip:
                        self.extract_specific_files_from_zip(
                            path = dir_inside_zip,
                            files_name_template = files_name_template,
                            date_format=date_format,
                            dst=dst,
                            data_ini = data_ini,
                            extracted_files = extracted_files)

        return extracted_files

if __name__ == '__main__':
    pass

    #exemplo 1
    # path = r"C:\Users\cs399274\Downloads"
    # date_format = "%Y-%m-%d"
    # files_name_template= ["N_{}_CARGAHIST.csv","S_{}_CARGAHIST.csv"]
    # file_to_search = "Deck_2024-02-22.zip"
    # data_ini = None

    #exemplo 2
    #path = r"C:\Users\cs399274\Downloads"
    # data_ini = datetime.datetime.strptime("31122023", "%d%m%Y")
    # date_format = "%d%m%Y"
    # files_name_template = ["psat_{}.txt"]
    # file_to_search = "psath_15022024.zip"

    # dst = r"C:\WX2TB\Documentos\fontes\testes\teste_extract"

    # dirTools = DirTools()
    # real_path = dirTools.match_insensitive_filename(path,file_to_search)
    # dirTools.extract_specific_files_from_zip(real_path,files_name_template,date_format,dst, data_ini)
