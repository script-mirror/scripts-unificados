import os
import glob
import datetime
import shutil

diretorio_base = '/tmp/'

lista_dir_limpeza_diaria = ['tmp*', 
                            'rust*', 
                            'Temp*']

dirs_limpeza_diaria = [item for diretorios in lista_dir_limpeza_diaria for item in glob.glob(os.path.join(diretorio_base, diretorios))]

def limpeza_diretorios():
    data_atual = datetime.datetime.now()
    
    # toda terça-feira vai apagar esses diretórios
    # if data_atual.weekday() == 1:
    for arquivo in dirs_limpeza_diaria:
        try:
            if os.path.exists(arquivo):
                if os.path.isdir(arquivo):
                    shutil.rmtree(arquivo)
                    print(f"Diretório {arquivo} removido com sucesso.")
                elif os.path.isfile(arquivo):
                    os.remove(arquivo)
                    print(f"Arquivo {arquivo} removido com sucesso.")
        except Exception as e:
            print(f"Erro ao remover {arquivo}: {e}")

if __name__ == '__main__':
    limpeza_diretorios()