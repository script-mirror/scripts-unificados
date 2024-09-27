from os import path
import sys
import pdb
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta


sys.path.insert(1,"/WX2TB/Documentos/fontes")

from PMO.scripts_unificados.apps.dadosAbertosOns.libs import coff, geracao_usina, carga

tipos_coff = ['eolica', 'solar']


def printHelper():
  hoje = datetime.now()
  formatado:str = f'''
tipos coff: {str(tipos_coff)[1:-1]}

python {sys.argv[0]} inserir_coff_mes_atual tipo eolica
python {sys.argv[0]} inserir_coff_por_mes tipo eolica data {hoje.strftime("%Y-%m")}
python {sys.argv[0]} inserir_coff_range tipo eolica inicio {hoje.strftime("%Y-%m")} fim {hoje.strftime("%Y-%m")}

python {sys.argv[0]} inserir_geracao_mes_atual 
python {sys.argv[0]} inserir_geracao_por_mes data {hoje.strftime("%Y-%m")}
python {sys.argv[0]} inserir_geracao_range inicio {hoje.strftime("%Y-%m")} fim {hoje.strftime("%Y-%m")}

python {sys.argv[0]} inserir_carga_mes_atual
python {sys.argv[0]} inserir_carga_por_mes data {hoje.strftime("%Y-%m")}
python {sys.argv[0]} inserir_carga_range inicio {hoje.strftime("%Y-%m")} fim {hoje.strftime("%Y-%m")}

  '''
  print(formatado)

  quit()


if __name__ == '__main__':
  if len(sys.argv) > 1:
    
    if 'help' in sys.argv or '--h' in sys.argv or '-help' in sys.argv:
      printHelper()
    for i in range(len(sys.argv)):
      argumento = sys.argv[i].lower()
      if argumento == 'data' or argumento == 'inicio':
        data = datetime.strptime(sys.argv[i + 1]+'-01', '%Y-%m-%d')
      
      elif argumento == 'tipo':
        tb_coff:str = f'tb_restricoes_coff_{sys.argv[i + 1]}'
        if tb_coff.replace('tb_restricoes_coff_', '') not in tipos_coff:
          raise ValueError(f'coff {tb_coff.replace("tb_restricoes_coff_", "")} nao existe/nao esta mapeada')
        
      elif argumento == 'fim':
        fim = datetime.strptime(sys.argv[i + 1]+'-01', '%Y-%m-%d')
        
        
    if sys.argv[1].lower() == 'inserir_coff_mes_atual':
        coff.inserir(tb_coff)
        print('Dados de {} inseridos'.format(datetime.now().strftime('%Y-%m')))

    elif sys.argv[1].lower() == 'inserir_coff_por_mes':
        coff.inserir(tb_coff, data)
        print('Dados de {} inseridos'.format(data.strftime('%Y-%m')))
        
    elif sys.argv[1].lower() == 'inserir_coff_range':
      while data <= fim:
        coff.inserir(tb_coff, data)
        print('Dados de {} inseridos'.format(data.strftime('%Y-%m')))
        data = data + relativedelta(months=1)
        
    elif sys.argv[1].lower() == 'inserir_geracao_mes_atual':
        geracao_usina.inserir()
        print('Dados de {} inseridos'.format(datetime.now().strftime('%Y-%m')))

    elif sys.argv[1].lower() == 'inserir_geracao_por_mes':
        geracao_usina.inserir(data)
        print('Dados de {} inseridos'.format(data.strftime('%Y-%m')))
        
    elif sys.argv[1].lower() == 'inserir_geracao_range':
      while data <= fim:
        geracao_usina.inserir(data)
        print('Dados de {} inseridos'.format(data.strftime('%Y-%m')))
        data = data + relativedelta(months=1)
        
    elif sys.argv[1].lower() == 'inserir_carga_mes_atual':
        carga.inserir()
        print('Dados de {} inseridos'.format(datetime.now().strftime('%Y-%m')))

    elif sys.argv[1].lower() == 'inserir_carga_por_mes':
        carga.inserir(data)
        print('Dados de {} inseridos'.format(data.strftime('%Y-%m')))
        
    elif sys.argv[1].lower() == 'inserir_carga_range':
      while data <= fim:
        carga.inserir(data)
        print('Dados de {} inseridos'.format(data.strftime('%Y-%m')))
        data = data + relativedelta(months=1)
    
  else:
    printHelper()
