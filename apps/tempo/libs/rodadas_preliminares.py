import os
import pdb
import sys
import glob
import shutil
import pathlib
import datetime

path = os.path.abspath(sys.path[0])
home = str(pathlib.Path.home())

path_middle = os.path.join(home, "Dropbox", "WX - Middle")
path_documentos = os.path.abspath('/WX2TB/Documentos')
path_observado_diario = os.path.join(path_documentos, 'chuva-vazao', 'gpm_para_eta-gefs', 'gpm')

def gerarPreliminar(modelo, data, rodada):
    padraoNomeArquivos = os.path.join(path_middle, "NovoSMAP", data.strftime('%Y%m%d'), f"rodada{rodada:0>2}z", f"{modelo}_p{data.strftime('%d%m%y')}a*.dat")
    pastaDestPreliminar = os.path.join(path_middle, "NovoSMAP", (data-datetime.timedelta(days=1)).strftime('%Y%m%d'), f"rodada{rodada:0>2}z")
    arquivosPrevisao = glob.glob(padraoNomeArquivos)
    
    if not os.path.exists(pastaDestPreliminar):
        os.makedirs(pastaDestPreliminar)
    
    print(f'Criando os arquivos preliminares do modelo {modelo}')
    for src in arquivosPrevisao:
        nomeArquivo = os.path.basename(src).replace(modelo, '{}.PRELIMINAR'.format(modelo))
        print(f'{nomeArquivo} -> ',end='')
        nomeArquivo = nomeArquivo.replace(data.strftime('%d%m%y'),(data-datetime.timedelta(days=1)).strftime('%d%m%y'))
        print(f'{nomeArquivo}')
        dst = os.path.join(pastaDestPreliminar,nomeArquivo)
        shutil.copyfile(src, dst)

    # primeiro dia faltando para chuva do preliminar
    # utilizado chuva observada do dia anterior
    data_ontem = data - datetime.timedelta(days=1)
    src = os.path.join(path_observado_diario, f"POBS_p{data_ontem.strftime('%d%m%y')}a{data.strftime('%d%m%y')}.dat")
    print(f'{os.path.basename(src)} -> ',end='')
    nomeArquivo = os.path.basename(src).replace('POBS', '{}.PRELIMINAR'.format(modelo))
    dst = os.path.join(pastaDestPreliminar,nomeArquivo)
    print(f'{nomeArquivo}')
    shutil.copyfile(src, dst)

if __name__ == '__main__':
    modelo = 'PCONJUNTO'
    data = datetime.datetime(2023,2,6)
    rodada = 0
    gerarPreliminar(modelo, data, rodada)
