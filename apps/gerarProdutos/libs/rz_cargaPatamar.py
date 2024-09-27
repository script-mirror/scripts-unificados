
import os
import re
import pdb
import sys
import glob
import shutil
import locale
import zipfile
import datetime
import pandas as pd
from dateutil.relativedelta import relativedelta

path_fontes = "/WX2TB/Documentos/fontes/"
sys.path.insert(1,path_fontes)
from PMO.scripts_unificados.bibliotecas import wx_emailSender,wx_opweek,rz_dir_tools

DIR_TOOLS = rz_dir_tools.DirTools()


def extractFiles(compiladoZipado):

  zipNameFile = os.path.basename(compiladoZipado)
  path = os.path.dirname(compiladoZipado)
  dstFolder = os.path.join(path, zipNameFile.split('.')[0])

  if os.path.exists(dstFolder):
    shutil.rmtree(dstFolder)

  if not os.path.exists(dstFolder):
    os.makedirs(dstFolder)
  with zipfile.ZipFile(compiladoZipado, 'r') as zip_ref:
    zip_ref.extractall(dstFolder)

  return dstFolder




def gefInfoPeloNome(nomeArquivoZip):

  try:
    match = re.match(r'RV([0-9]{1})_PMO_(.*)([0-9]{4})_carga_semanal.zip', nomeArquivoZip)
    rv = int(match.group(1))
    mes = match.group(2)
    ano = int(match.group(3))
  except:
    match = re.match(r'PMO (.*)([0-9]{4})\(Revisão ([0-9]{1})\).zip', nomeArquivoZip)
    mes = match.group(1)
    ano = int(match.group(2))
    rv = int(match.group(3))
  mes = mes.replace('_','')
  mesRef = datetime.datetime.strptime('1 {} {}'.format(mes.lower(),ano),'%d %B %Y')
  inicioMesElet = wx_opweek.getLastSaturday(mesRef)
  return wx_opweek.ElecData(inicioMesElet + datetime.timedelta(days=7*rv))


def getNomeArquivoCarga_txt(caminhoArquivos):
  arquivos = glob.glob(os.path.join(caminhoArquivos,'*'))
  arquivoCarga = [f for f in arquivos if 'CargaDecomp_PMO' in f]
  return arquivoCarga[0]

# def formatarNomeArquivoCarga(dataRv):
# 	mes = dataRv.data.strftime('%B')
# 	mes = mes[0].upper() + mes[1:]

# 	nomeArquivo = 'CargaDecomp_PMO_{}{}'.format(mes,dataRv.data.strftime('%y'))
# 	if dataRv.atualRevisao:
# 		nomeArquivo += '(Rev {})'.format(dataRv.atualRevisao)
# 	nomeArquivo += '.txt'
# 	return nomeArquivo

def leituraArquivoCarga_txt(pathFile, dataRv):
  CargaDescomentada = []
  carga = open(pathFile,'r')
  for l in carga:
    if l[0]=='&':
      continue
    else:
      CargaDescomentada.append(l.split())

  dfCarga = pd.DataFrame(CargaDescomentada)
  dfCarga.columns = ['mnemonico','IP','S','PAT','CargaPesada','Pat_1(h)','CargaMedia','Pat_2(h)','CargaLeve','Pat_3(h)']
  dfCarga.drop(['mnemonico','PAT'],axis=1,inplace=True)
  dfCarga.drop(dfCarga[dfCarga['S']=='11'].index,inplace=True)
  colunasNumericas = ['CargaPesada','Pat_1(h)','CargaMedia','Pat_2(h)','CargaLeve','Pat_3(h)']
  dfCarga[colunasNumericas] = dfCarga[colunasNumericas].apply(pd.to_numeric, errors='coerce')
  dfCarga['S'] = dfCarga['S'].replace({'1':'SE','2':'S','3':'NE','4':'N'})
  dicData = {}
  for sem in dfCarga['IP'].unique():
    dicData[sem] = dataRv + datetime.timedelta(days=7*(int(sem)-1))
  dfCarga['IP'] = dfCarga['IP'].replace(dicData)

  dfCarga = dfCarga.set_index(['IP','S'])
  return dfCarga

def getNomeArquivoCarga_xlsx(caminhoArquivos):
  arquivos = glob.glob(os.path.join(caminhoArquivos,'*'))
  arquivoCarga = [f for f in arquivos if 'Carga_PMO_' in f and '.xlsx' in f]
  return arquivoCarga[0]

def leituraArquivoCarga_xlsx(pathFile, dataRv):
 
  submercados = {}
  submercados['Subsistema Nordeste'] = 'NE'
  submercados['Subsistema Norte'] = 'N'
  submercados['Subsistema Sudeste/C.Oeste'] = 'SE'
  submercados['Subsistema Sul'] = 'S'
 
  dfCarga = pd.read_excel(pathFile, skiprows=10, usecols=[2, 6, 7, 8, 9, 10, 11,12])
  dfCarga = dfCarga.iloc[2:7]
  dfCarga['Unnamed: 2'] = dfCarga['Unnamed: 2'].replace(submercados)
  dfCarga.set_index('Unnamed: 2', inplace=True)
 
  nome_colunas = []
  for i_col, col_name in enumerate(dfCarga.columns):
    if i_col == 0:
      nome_colunas.append('Mensal')
    elif i_col == len(dfCarga.columns)-1:
      if str(dfCarga.loc['SE', col_name]) == 'nan':
        dfCarga.drop(col_name,axis=1, inplace=True)
      else:
        nome_colunas.append(col_name)
    else:
      # nome_colunas.append(f'{col_name}\nsem {i_col}')
      nome_colunas.append(col_name)
  dfCarga.columns = nome_colunas

  return dfCarga


def cargaPatamar(pathZip):
  locale.setlocale(locale.LC_TIME, 'pt_BR.utf8')
  pathRvAtual = extractFiles(pathZip)
  nomeArquivoZip = os.path.basename(pathZip)
  dataRvAtual = gefInfoPeloNome(nomeArquivoZip)
  dataRvAnterior = wx_opweek.ElecData(dataRvAtual.data - datetime.timedelta(days=7))
  diaPrimeiroMesReferente = datetime.datetime(dataRvAnterior.anoReferente,dataRvAnterior.mesReferente,1)
  mesRevAnterior = diaPrimeiroMesReferente.strftime('%B')
  try:
    nomeArquivoAnt = f'PMO {mesRevAnterior.capitalize()}{dataRvAnterior.anoReferente}(Revisão {dataRvAnterior.atualRevisao}).zip'
    arquivoRvAnt = os.path.join(os.path.dirname(pathZip),nomeArquivoAnt)
    pathRvAnterior = extractFiles(arquivoRvAnt)
  except:
    try:
      nomeArquivoAnt = 'RV{}_PMO_{}{}_carga_semanal.zip'.format(dataRvAnterior.atualRevisao,mesRevAnterior.capitalize(),dataRvAnterior.anoReferente)
      arquivoRvAnt = os.path.join(os.path.dirname(pathZip),nomeArquivoAnt)
      pathRvAnterior = extractFiles(arquivoRvAnt)
    except Exception as e:
      nomeArquivoAnt = 'RV{}_PMO_{}_{}_carga_semanal.zip'.format(dataRvAnterior.atualRevisao,mesRevAnterior.capitalize(),dataRvAnterior.anoReferente)
      arquivoRvAnt = os.path.join(os.path.dirname(pathZip),nomeArquivoAnt)
      pathRvAnterior = extractFiles(arquivoRvAnt)
  
  nomeArquivoAtual_xlsx = getNomeArquivoCarga_xlsx(pathRvAtual)
  nomeArquivoAtual_txt = getNomeArquivoCarga_txt(pathRvAtual)
  pathCargaDecompAtual_xlsx = os.path.join(pathRvAtual,nomeArquivoAtual_xlsx )
  pathCargaDecompAtual_txt = os.path.join(pathRvAtual,nomeArquivoAtual_txt)
 
  nomeArquivoAnterior_xlsx  = getNomeArquivoCarga_xlsx(pathRvAnterior)
  nomeArquivoAnterior_txt = getNomeArquivoCarga_txt(pathRvAnterior)
  pathCargaDecompAnt_xlsx = os.path.join(pathRvAnterior,nomeArquivoAnterior_xlsx)
  pathCargaDecompAnt_txt = os.path.join(pathRvAnterior,nomeArquivoAnterior_txt)

  dfCargaAtual_xlsx = leituraArquivoCarga_xlsx(pathCargaDecompAtual_xlsx, dataRvAtual.data)
  dfCargaAnt_xlsx = leituraArquivoCarga_xlsx(pathCargaDecompAnt_xlsx, dataRvAnterior.data)
 
  dfCargaAtual_txt = leituraArquivoCarga_txt(pathCargaDecompAtual_txt, dataRvAtual.data)
  dfCargaAnt_txt = leituraArquivoCarga_txt(pathCargaDecompAnt_txt, dataRvAnterior.data)
  difCarga = dfCargaAtual_txt - dfCargaAnt_txt
  difCarga.dropna(inplace=True)
 
  difCarga_xlsx = dfCargaAtual_xlsx - dfCargaAnt_xlsx

  difCarga_xlsx = difCarga_xlsx[list(difCarga_xlsx.columns[1:]) + ['Mensal']]
  difCarga_xlsx.rename(columns={'Mensal':'Mensal [M1]'}, inplace=True)
 
  submercados = ['SE','S','NE','N']
  dt_mes_2 = difCarga.index.get_level_values(0).max()
  difCarga_xlsx['Mensal [M2]'] = 0
  for sub in submercados:
    difCargaPesada = difCarga.loc[dt_mes_2,sub]['CargaPesada']
    difCargaMedia = difCarga.loc[dt_mes_2,sub]['CargaMedia']
    difCargaLeve = difCarga.loc[dt_mes_2,sub]['CargaLeve']
  
    horasPatamarPesado = dfCargaAtual_txt.loc[dt_mes_2,sub]['Pat_1(h)']
    horasPatamarMedio = dfCargaAtual_txt.loc[dt_mes_2,sub]['Pat_2(h)']
    horasPatamarLeve = dfCargaAtual_txt.loc[dt_mes_2,sub]['Pat_3(h)']

    media = (difCargaPesada*horasPatamarPesado + difCargaMedia*horasPatamarMedio + difCargaLeve*horasPatamarLeve)/(horasPatamarPesado+horasPatamarMedio+horasPatamarLeve)
    difCarga_xlsx.loc[sub, 'Mensal [M2]'] = media
    difCarga_xlsx.loc['Sistema Interligado Nacional', 'Mensal [M2]'] += media

  if dataRvAtual.atualRevisao == 0:
    difCarga_xlsx = difCarga_xlsx.dropna(axis=1)
    difCarga_xlsx = difCarga_xlsx.drop(['Mensal [M1]','Mensal [M2]'], axis=1)
  
  shutil.rmtree(pathRvAtual)
  shutil.rmtree(pathRvAnterior)
  return difCarga_xlsx, dataRvAtual,dfCargaAtual_xlsx,dfCargaAtual_txt,difCarga


def cargaPatamarPMO(pathZip, difCarga_xlsx,dfCargaAtual_xlsx,dfCargaAtual_txt,difCarga):

  locale.setlocale(locale.LC_TIME, 'pt_BR.utf8')
  nomeArquivoZip = os.path.basename(pathZip)
  dataRvAtual = gefInfoPeloNome(nomeArquivoZip)
  
  if difCarga_xlsx.empty:
    pass
  else:
    try:
      nomeArquivoZipPMO = nomeArquivoZip.replace(nomeArquivoZip[0:3],'RV0')
      arquivoPMO = os.path.join(os.path.dirname(pathZip),nomeArquivoZipPMO)
      pathRvPMO = extractFiles(arquivoPMO)
    except:
      nomeArquivoZipPMO = nomeArquivoZip.replace(nomeArquivoZip[0:3],'RV0')
      nomeArquivoZipPMO = re.sub(r"(Janeiro|Fevereiro|Março|Abril|Maio|Junho|Julho|Agosto|Setembro|Outubro|Novembro|Dezembro)", r"\1_", nomeArquivoZipPMO)
      arquivoPMO = os.path.join(os.path.dirname(pathZip),nomeArquivoZipPMO)
      pathRvPMO = extractFiles(arquivoPMO)
  
    dataRvPMO = wx_opweek.ElecData(dataRvAtual.primeiroDiaMes)
    nomeArquivoPMO_xlsx = getNomeArquivoCarga_xlsx(pathRvPMO)
    nomeArquivoPMO_txt = getNomeArquivoCarga_txt(pathRvPMO)
    pathCargaDecompPMO_xlsx = os.path.join(pathRvPMO,nomeArquivoPMO_xlsx)
    pathCargaDecompPMO_txt = os.path.join(pathRvPMO,nomeArquivoPMO_txt)
    dfCargaPMO_xlsx = leituraArquivoCarga_xlsx(pathCargaDecompPMO_xlsx, dataRvPMO.data)
    dfCargaPMO_txt = leituraArquivoCarga_txt(pathCargaDecompPMO_txt, dataRvPMO.data)
    difCargaPMO = dfCargaAtual_txt - dfCargaPMO_txt
    difCargaPMO.dropna(inplace=True)
    difCargaPMO_xlsx = dfCargaAtual_xlsx - dfCargaPMO_xlsx
  
    difCargaPMO_xlsx = difCargaPMO_xlsx[list(difCargaPMO_xlsx.columns[1:]) + ['Mensal']]
    difCargaPMO_xlsx.rename(columns={'Mensal':'Mensal [M1]'}, inplace=True)
  
    submercados = ['SE','S','NE','N']
    dt_mes_2 = difCargaPMO.index.get_level_values(0).max()
    difCargaPMO_xlsx['Mensal [M2]'] = 0
    for sub in submercados:
      difCargaPesada = difCargaPMO.loc[dt_mes_2,sub]['CargaPesada']
      difCargaMedia = difCargaPMO.loc[dt_mes_2,sub]['CargaMedia']
      difCargaLeve = difCargaPMO.loc[dt_mes_2,sub]['CargaLeve']
    
      horasPatamarPesado = dfCargaPMO_txt.loc[dt_mes_2,sub]['Pat_1(h)']
      horasPatamarMedio = dfCargaPMO_txt.loc[dt_mes_2,sub]['Pat_2(h)']
      horasPatamarLeve = dfCargaPMO_txt.loc[dt_mes_2,sub]['Pat_3(h)']

      media = (difCargaPesada*horasPatamarPesado + difCargaMedia*horasPatamarMedio + difCargaLeve*horasPatamarLeve)/(horasPatamarPesado+horasPatamarMedio+horasPatamarLeve)
      difCargaPMO_xlsx.loc[sub, 'Mensal [M2]'] = media
      difCargaPMO_xlsx.loc['Sistema Interligado Nacional', 'Mensal [M2]'] += media
  return difCargaPMO_xlsx

def get_df_carga_nw(path_toZip,dataMesAtual):
  
  dataMesAtual = pd.to_datetime(dataMesAtual)
  archive = zipfile.ZipFile(path_toZip, 'r')
  xlfile = archive.open(archive.filelist[0])
  columns_to_skip = ['WEEK', 'GAUGE', 'REVISION', 'Base_CGH', 'Base_EOL', 'Base_UFV', 'Base_UTE', 'Base_MMGD','Exp_CGH','Exp_EOL','Exp_UFV','Exp_UTE','Exp_MMGD']

  try:
      pmo_mes = pd.read_excel(xlfile, usecols=lambda x: x not in columns_to_skip,sheet_name=1)
  except:
      pmo_mes = pd.read_excel(xlfile, usecols=lambda x: x not in columns_to_skip)
  pmo_filtrado_mes = pmo_mes[(pmo_mes.TYPE == "MEDIUM")&(pmo_mes.DATE==dataMesAtual)]

  pmo_df_final = pmo_filtrado_mes.drop(columns='TYPE').groupby(['SOURCE']).sum().transpose()
  pmo_df_final= pmo_df_final[['SUDESTE', 'SUL','NORDESTE', 'NORTE']]
  pmo_df_final['SIN'] = pmo_df_final[['SUDESTE', 'SUL','NORDESTE', 'NORTE']].sum(axis=1)

  pmo_df_final_sMMGD = pmo_df_final.loc['LOAD_sMMGD'].to_frame().T
  pmo_df_final_cMMGD = pmo_df_final.loc['LOAD_cMMGD'].to_frame().T

  pmo_df_final_sMMGD.index = ['PMO']
  pmo_df_final_cMMGD.index = ['PMO']
  
  pmo_df_final_sMMGD = pmo_df_final_sMMGD.transpose()
  pmo_df_final_cMMGD = pmo_df_final_cMMGD.transpose()

  pmo_df_final_sMMGD=pmo_df_final_sMMGD.rename_axis(index=(None)).astype(int)
  pmo_df_final_cMMGD=pmo_df_final_cMMGD.rename_axis(index=(None)).astype(int)
  
  
  return pmo_df_final_sMMGD,pmo_df_final_cMMGD
  
def gera_tb_pmo_nw(path, data):

  zip_name = os.path.basename(path)
  dir_nomeMesAtual = datetime.datetime.strptime(zip_name,"RV0_PMO_%B_%Y_carga_mensal.zip").strftime('%B_%Y').capitalize()

  dataMesAtual = pd.to_datetime(dir_nomeMesAtual,format="%B_%Y")
  nomeMesAtual_tb = dataMesAtual.strftime("%b%Y")
  #PATH MES ANTERIOR
  ##achando mes anterior
  mesAnterior = dataMesAtual-datetime.timedelta(days = 1)
  mesAnterior = pd.to_datetime(mesAnterior)

  #nome da data para por na tabela
  nomeMesAnterior_tb = mesAnterior.strftime("%b%Y").capitalize()

  #nomes para substituir no caminho do path
  dir_name_anterior = mesAnterior.strftime("%B_%Y").capitalize()
  path_anterior = path.replace(dir_nomeMesAtual,dir_name_anterior)


  html_sMMGD =""
  html_cMMGD =""
  nome_caption = pd.to_datetime(data).strftime("%b%Y").capitalize()

  pmo_mes_Atual_sMMGD,pmo_mes_Atual_cMMGD = get_df_carga_nw(path,data)

  pmo_mes_Anterior_sMMGD,pmo_mes_Anterior_cMMGD = get_df_carga_nw(path_anterior,data)

  pmo_diff_tb_sMMGD = (pmo_mes_Atual_sMMGD - pmo_mes_Anterior_sMMGD)
  pmo_diff_tb_cMMGD = (pmo_mes_Atual_cMMGD - pmo_mes_Anterior_cMMGD)


  pmo_diff_tb_sMMGD['PORCENTAGEM'] = (((pmo_mes_Atual_sMMGD / pmo_mes_Anterior_sMMGD) - 1 ) * 100).round(1).astype(str)+" %"
  dtFinalDiferenca_sMMGD = pmo_diff_tb_sMMGD.rename(columns={'PMO': 'DIFF'})

  pmo_diff_tb_cMMGD['PORCENTAGEM'] = (((pmo_mes_Atual_cMMGD / pmo_mes_Anterior_cMMGD) - 1 ) * 100).round(1).astype(str)+" %"
  dtFinalDiferenca_cMMGD = pmo_diff_tb_cMMGD.rename(columns={'PMO': 'DIFF'})


  pmo_Anterior_resultado_sMMGD = pmo_mes_Anterior_sMMGD.rename(columns={'PMO': nomeMesAnterior_tb}).astype(int)
  pmo_Anterior_resultado_cMMGD = pmo_mes_Anterior_cMMGD.rename(columns={'PMO': nomeMesAnterior_tb}).astype(int)
  
  
  pmo_Atual_resultado_sMMGD = pmo_mes_Atual_sMMGD.rename(columns={'PMO': nomeMesAtual_tb}).astype(int)
  pmo_Atual_resultado_cMMGD = pmo_mes_Atual_cMMGD.rename(columns={'PMO': nomeMesAtual_tb}).astype(int)

  
  pmo_tb_completo_sMMGD = pd.concat([pmo_Anterior_resultado_sMMGD, dtFinalDiferenca_sMMGD,pmo_Atual_resultado_sMMGD],axis=1)
  pmo_tb_completo_cMMGD = pd.concat([pmo_Anterior_resultado_cMMGD, dtFinalDiferenca_cMMGD,pmo_Atual_resultado_cMMGD],axis=1)
  
  diff_styled_sMMGD = pmo_tb_completo_sMMGD.style.set_caption(f"{nome_caption} - Carga sem MMGD").set_precision(2)
  diff_styled_cMMGD = pmo_tb_completo_cMMGD.style.set_caption(f"{nome_caption} - Carga com MMGD").set_precision(2)
  
  
  html_sMMGD = diff_styled_sMMGD.to_html()
  html_cMMGD = diff_styled_cMMGD.to_html()

  template_sMMGD = wx_emailSender.get_css_df_style(html_sMMGD).replace('\n','')

  template_cMMGD = wx_emailSender.get_css_df_style(html_cMMGD).replace('\n','')
  
  return template_sMMGD,pmo_tb_completo_sMMGD,template_cMMGD,pmo_tb_completo_cMMGD


def gerar_c_adic_dat(path_arquivo_base, path_saida, path_arquivo_extracaoDados,dataAnoMesAtual):
    arquivo_base = open(path_arquivo_base)  
    arquivo_extracaoDados = open(path_arquivo_extracaoDados)
    c_adic__arquivo_extracaoDados = arquivo_extracaoDados.readlines()
    c_adic_in = arquivo_base.readlines()

    contadorMMGD = 0
    i_linha_in = 0
    after_MMGD = False
    regex_stop = r'^\s*\d+\s+\w+\s+MMGD\s+\w+\s*$'
    
    # esse numero 3 é onde o indice no arquivo que eu crio começa a valer para o bloco c_adic.
    indiceInicialArqExtracao = 3
    quantidadeAnosBlocoMMGDcomTitulo = 6

    # pegar o ano atual para poder trocar apenas o valor da linha que tem esse ano.
    anoAtual = dataAnoMesAtual.year
    primeiroMesTrocarValor = dataAnoMesAtual.month
    proximoMes = 2
    segundoMesRemoverTrocarValor = primeiroMesTrocarValor + proximoMes
    
    while i_linha_in < len(c_adic_in):
        linha_in = c_adic_in[i_linha_in]
                        
        if re.search(regex_stop, linha_in):
            after_MMGD = True
            contadorMMGD += 1

        elif after_MMGD and linha_in.startswith(str(anoAtual)):
            # apos a primeira parada com base no REGEX começo a altera o indice no arquivo que criei para poder parear com o arquivo original.
            indice_extracao = (contadorMMGD * quantidadeAnosBlocoMMGDcomTitulo) - indiceInicialArqExtracao
            # Acesse a linha correspondente no arquivo de extração de dados
            linha_extracao = c_adic__arquivo_extracaoDados[indice_extracao] 
            valoresExtracao = linha_extracao.split()
            valoresNovos = linha_in.split()
            espacosNull = 13 - len(valoresNovos)
            espacosNullExtracao = 13 - len(valoresExtracao)
            listaNull = [' '] * espacosNull
            listaNullExtracao = [' '] * espacosNullExtracao
            valoresNovos = valoresNovos[:1] + listaNull + valoresNovos[1:]
            valoresNovosExtracao = valoresExtracao[:1] + listaNullExtracao + valoresExtracao[1:]
            # estou trocando sempre o valor na linha do ano atual e dos dois meses apos o mes que foi rodado o script. Ex: rodei em Abril, troco o valor de Maio e Junho.
            valoresNovos[primeiroMesTrocarValor:segundoMesRemoverTrocarValor] = valoresNovosExtracao[primeiroMesTrocarValor:segundoMesRemoverTrocarValor]
            
            bloco_regex = "{:<7}{:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7}\n"
            c_adic_in[i_linha_in] = bloco_regex.format(*valoresNovos)
            after_MMGD = False  
        i_linha_in += 1
    
    arquivo_base.close()
    arquivo_extracaoDados.close()

    c_adic_out = os.path.join(path_saida, os.path.basename(path_arquivo_base))
    with open(c_adic_out, 'w') as arquivo_saida:
        arquivo_saida.writelines(c_adic_in)
    print(c_adic_out)

    return c_adic_out


def gerar_sistema_dat(path_arquivo_base, path_saida, path_arquivo_extracaoDados,dataAnoMesAtual):

    arquivo_base = open(path_arquivo_base)
    arquivo_extracaoDados = open(path_arquivo_extracaoDados)
    sistema_in = arquivo_base.readlines()
    sistema_arquivo_extracaoDados = arquivo_extracaoDados.readlines()

    i_linha = 0
    linhaExtracao = 0
    contadorMMGD = 0
    
    flags_regex = re.compile("ENERGIA|GERACAO")
    flag = None
    after_MMGD = False
    regex_stop = r'^\s*\d+\s+\d+\s+(?:PCH|PCT|EOL|UFV)\s+MMGD\s*$'

    # esse numero 36 é onde o indice no arquivo que eu crio começa a valer para o bloco geração.
    indiceInicialArqExtracao = 36
    indiceTituloBlocoMMGD = 1
    quantidadeAnosBlocoMMGDcomTitulo = 6
    
    # pegar o ano atual para poder trocar apenas o valor da linha que tem esse ano.
    anoAtual = dataAnoMesAtual.year
    primeiroMesTrocarValor = dataAnoMesAtual.month
    proximoMes = 2
    segundoMesRemoverTrocarValor = primeiroMesTrocarValor + proximoMes
    
    while i_linha < len(sistema_in):
        linha = sistema_in[i_linha]
        
        if flags_regex.search(linha):

            if re.search("ENERGIA", linha):
                linhaExtracao = 1			
                flag = 'energia'
            elif re.search("GERACAO", linha):
                flag = 'geracao'
                after_MMGD = False  # Definindo a variável antes do loop

        # Utiliza a mesma taxa de crescimento do ultimo ano para o ano adicionado
        elif flag in ['energia', 'geracao']:
            if flag == 'energia':
                if re.search(r'^\d{1,7}', linha) and linha.startswith(str(anoAtual)):                       
                    linha_extracao = sistema_arquivo_extracaoDados[linhaExtracao]
                    valoresExtracao = linha_extracao.split()
                    valoresNovos = linha.split()
                    espacosNull = 13 - len(valoresNovos)
                    espacosNullExtracao = 13 - len(valoresExtracao)
                    listaNull = [' '] * espacosNull
                    listaNullExtracao = [' '] * espacosNullExtracao
                    valoresNovos = valoresNovos[:1] + listaNull + valoresNovos[1:]
                    valoresNovosExtracao = valoresExtracao[:1] + listaNullExtracao + valoresExtracao[1:]
                    # estou trocando sempre o valor na linha do ano atual e dos dois meses apos o mes que foi rodado o script. Ex: rodei em Abril, troco o valor de Maio e Junho.
                    valoresNovos[primeiroMesTrocarValor:segundoMesRemoverTrocarValor] = valoresNovosExtracao[primeiroMesTrocarValor:segundoMesRemoverTrocarValor]
                    
                    bloco_regex = "{:<7}{:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7}\n"
                    sistema_in[i_linha] = bloco_regex.format(*valoresNovos)
                    
            elif flag == 'geracao':
                
                if re.search(regex_stop, linha):
                  after_MMGD = True
                  contadorMMGD += 1
                  
                elif after_MMGD and re.search(r'^\d{1,7}', linha) and linha.startswith(str(anoAtual)):


                  indice_extracao = indiceInicialArqExtracao  + (contadorMMGD - indiceTituloBlocoMMGD ) * quantidadeAnosBlocoMMGDcomTitulo 
                  # Acesse a linha correspondente no arquivo de extração de dados
                  linha_extracao = sistema_arquivo_extracaoDados[indice_extracao]
                  valoresExtracao = linha_extracao.split()
                  valoresNovos = linha.split()
                  espacosNull = 13 - len(valoresNovos)
                  espacosNullExtracao = 13 - len(valoresExtracao)
                  listaNull = [' '] * espacosNull
                  listaNullExtracao = [' '] * espacosNullExtracao
                  valoresNovos = valoresNovos[:1] + listaNull + valoresNovos[1:]
                  valoresNovosExtracao = valoresExtracao[:1] + listaNullExtracao + valoresExtracao[1:]
                  # estou trocando sempre o valor na linha do ano atual e dos dois meses apos o mes que foi rodado o script. Ex: rodei em Abril, troco o valor de Maio e Junho.
                  valoresNovos[primeiroMesTrocarValor:segundoMesRemoverTrocarValor] = valoresNovosExtracao[primeiroMesTrocarValor:segundoMesRemoverTrocarValor]
                  
                  bloco_regex = "{:<7}{:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7}\n"
                  sistema_in[i_linha] = bloco_regex.format(*valoresNovos)  
                  after_MMGD = False 
            linhaExtracao += 1
        i_linha += 1
    
    sistema_out = os.path.join(path_saida, os.path.basename(path_arquivo_base))
    arquivo_saida = open(sistema_out, 'w')
    arquivo_saida.writelines(sistema_in)
    print(sistema_out)

    return sistema_out


def criar_arquivo_extracao_sistema_dat(df_agrupado_energiaTotal, df_agrupado_usinasNaoSimuladas,mapeamento_codigo_fonte,dataAnoMesAtual):

    # Inicializando a variável de saída MERCADO DE ENERGIA TOTAL
    output = ""
    # Escrevendo o cabeçalho
    output += " MERCADO DE ENERGIA TOTAL\n"
    output += " XXX\n"
    output += "       XXXJAN. XXXFEV. XXXMAR. XXXABR. XXXMAI. XXXJUN. XXXJUL. XXXAGO. XXXSET. XXXOUT. XXXNOV. XXXDEZ.\n"

    # Iterando sobre as fontes de energia
    for regioes in range(1, 5):
      # Escrevendo o código da fonte
      output += f"   {regioes}\n"

      # Inicialize a variável que rastreará o ano atual
      ano_atual = None

      # Iterando sobre os anos
      for ano in df_agrupado_energiaTotal['DATE'].str.split('-').str[0].unique():
        # Escrevendo o ano
        output += f"{ano}    "

        # Filtrando os dados para o ano e fonte específicos
        dados = df_agrupado_energiaTotal[(df_agrupado_energiaTotal['DATE'].str.startswith(ano)) & (df_agrupado_energiaTotal['SOURCE'] == regioes)]

        # Verificando se há dados para este ano e fonte
        if len(dados) > 0:
          qtd_espacos_primeiro_valor = 4
          qtd_espacos_demais_valores = 2

          # Escrevendo os valores de cada mês para a fonte atual
          for month in range(1, 13):
            month_data = dados[dados['DATE'].str.endswith(f'{str(month).zfill(2)}')]
            if len(month_data) > 0:
              valores = str(month_data['LOAD_sMMGD'].values[0]).split('.')[0]  # Obtendo apenas a parte antes do ponto
              # Se o ano mudou, escreva o primeiro valor com espaço de 4 e os subsequentes com espaço de 2
              if ano_atual is None or ano_atual != ano:
                ano_atual = ano
                output += f"{valores:>{qtd_espacos_primeiro_valor}}."
              else:
                output += f"  {valores:>{qtd_espacos_demais_valores}}."
            else:
              output += " " * (qtd_espacos_primeiro_valor + 1)  # Espaços vazios para valores ausentes
        else:
          output += " " * 10 * 12  # Espaços vazios para valores ausentes
        output += "\n"
        
      # Adicionando a linha "POS" com os valores do último ano de cada seção
      output += "POS   "  # Espaços para alinhar com as células

      ultimo_ano = df_agrupado_energiaTotal['DATE'].str.split('-').str[0].max()  # Obtém o último ano presente nos dados
      dados_ultimo_ano = df_agrupado_energiaTotal[(df_agrupado_energiaTotal['DATE'].str.startswith(ultimo_ano)) & (df_agrupado_energiaTotal['SOURCE'] == regioes)]

      for month in range(1, 13):
        month_data = dados_ultimo_ano[dados_ultimo_ano['DATE'].str.endswith(f'{str(month).zfill(2)}')]
        if len(month_data) > 0:
          valores = str(month_data['LOAD_sMMGD'].values[0]).split('.')[0]  # Obtendo apenas a parte antes do ponto
        # Se o ano mudou, escreva o primeiro valor com espaço de 4 e os subsequentes com espaço de 2
        if ano_atual is None or ano_atual != ano:
          ano_atual = ano
          output += f"{valores:>{qtd_espacos_primeiro_valor}}."
        else:
          output += f"  {valores:>{qtd_espacos_demais_valores}}."    
      output += "\n"

    output += " 999\n"

    mes_atual = dataAnoMesAtual.month
    ano_Atual = dataAnoMesAtual.year

    output_ = f"SISTEMA_DAT{ano_Atual}{mes_atual}.DAT"
    
    # Caminho de saída do arquivo
    caminho_saida = f"/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/webhook/arquivos/tmp/Deck NEWAVE Preliminar/{output_}"

    with open(caminho_saida, "w") as file:
      # Aplicando a substituição com regex
      texto_atualizado = re.sub(r'\b(\d{1,4})\.', lambda x: ' ' * (5 - len(x.group(1))) + x.group(0), output)
      file.write(texto_atualizado)

    # Inicializando a variável de saída GERACAO DE USINAS NAO SIMULADAS
    output = ""
    # Escrevendo o cabeçalho
    output += " GERACAO DE USINAS NAO SIMULADAS\n"
    output += " XXX  XBL  XXXXXXXXXXXXXXXXXXXX  XTE\n"
    output += "       XXXJAN. XXXFEV. XXXMAR. XXXABR. XXXMAI. XXXJUN. XXXJUL. XXXAGO. XXXSET. XXXOUT. XXXNOV. XXXDEZ.\n"

    for regiao in range(1, 5):
        for fonte in ['PCH MMGD', 'PCT MMGD', 'EOL MMGD', 'UFV MMGD']:
            # cabecalho para adicionar codigo regiao, codigo fonte e fonte
            output += f"   {regiao}    {mapeamento_codigo_fonte[fonte]}  {fonte}\n"
            ano_atual = None
            # Iterando sobre os anos
            for ano in df_agrupado_usinasNaoSimuladas['DATE'].str.split('-').str[0].unique():
                output += ano
                
                # Filtrando os dados para o ano e fonte específicos
                dados = df_agrupado_usinasNaoSimuladas[(df_agrupado_usinasNaoSimuladas['DATE'].str.startswith(ano)) & (df_agrupado_usinasNaoSimuladas['SOURCE'] == regiao)]
                
                # Verificando se há dados para este ano e fonte
                if len(dados) > 0:
                    qtd_espacos_primeiro_valor = 1
                    qtd_espacos_demais_valores = 4
                    
                    # Escrevendo os valores de cada mês para a fonte atual
                    for mes in range(1, 13):
                        mes_str = f"{str(mes).zfill(2)}"
                        mes_data = dados[dados['DATE'].str.endswith(mes_str)]
                        
                        if len(mes_data) > 0:
                            valor = mes_data[fonte].values[0]
                            valor_arredondado = round(valor, 2)
                            valores_MMGD = "{:.2f}".format(valor_arredondado)
                                                    
                            if ano_atual is None or ano_atual != ano:
                                ano_atual = ano
                                output += f"   {valores_MMGD:>{qtd_espacos_primeiro_valor}}"
                            else:
                                output += f" {valores_MMGD:>{qtd_espacos_demais_valores}}"
                        else:
                            output += " " * 7  # Espaços para valores ausentes
                else:
                    output += " " * 10 * 12  # Espaços para valores ausentes							
                output += "\n"
    output += " 999\n"

    with open(caminho_saida, "r") as file:
        conteudo_atual = file.read()

    conteudo_atual += output
        
    with open(caminho_saida, "w") as file:
        # Aplicando a substituição com regex
        texto_atualizado = re.sub(r'\b(\d{1,4})\.', lambda x: ' ' * (4 - len(x.group(1))) + x.group(0), conteudo_atual)
        file.write(texto_atualizado)
    
    output_ = f"SISTEMA_DAT{ano_Atual}{mes_atual}.DAT"
    
    path_arquivo_base = os.path.abspath(f"/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/webhook/arquivos/tmp/Deck NEWAVE Preliminar/deck_newave_{ano_Atual}_{mes_atual:02d}_Preliminar/SISTEMA.DAT")
    
    path_saida = "/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/webhook/arquivos/tmp/Deck NEWAVE Preliminar"
    # Caminho de saída do arquivo
    path_arquivo_extracaoDados = f"/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/webhook/arquivos/tmp/Deck NEWAVE Preliminar/{output_}"

    arquivo_sistema_Final = gerar_sistema_dat(path_arquivo_base, path_saida, path_arquivo_extracaoDados,dataAnoMesAtual)
    
    return arquivo_sistema_Final


def criar_arquivo_extracao_c_adic_dat(df_agrupado_MMGD,mapeamento_regiao_titulo,dataAnoMesAtual):

    # Inicializando a variável de saída
    output = ""
    # Escrevendo o cabeçalho
    output += " XXX\n"
    output += "       XXXJAN. XXXFEV. XXXMAR. XXXABR. XXXMAI. XXXJUN. XXXJUL. XXXAGO. XXXSET. XXXOUT. XXXNOV. XXXDEZ.\n"

    for regiao in mapeamento_regiao_titulo.values():
      output += f"   {regiao}\n"
      ano_atual = None
      # Iterando sobre os anos
      for ano in df_agrupado_MMGD['DATE'].str.split('-').str[0].unique():
        output += ano
        
        # Filtrando os dados para o ano e fonte específicos
        dados = df_agrupado_MMGD[(df_agrupado_MMGD['DATE'].str.startswith(ano)) & (df_agrupado_MMGD['SOURCE'] == regiao)]
                    
        if len(dados) > 0:
          qtd_espacos_primeiro_valor = 3
          qtd_espacos_demais_valores = 2

          # Escrevendo os valores de cada mês para a fonte atual
          for month in range(1, 13):
            month_data = dados[dados['DATE'].str.endswith(f'{str(month).zfill(2)}')]
            if len(month_data) > 0:
              valores = str(month_data['Base_MMGD'].values[0]).split('.')[0]  # Obtendo apenas a parte antes do ponto
              # Se o ano mudou, escreva o primeiro valor com espaço de 4 e os subsequentes com espaço de 2
              if ano_atual is None or ano_atual != ano:
                  ano_atual = ano
                  output += f"   {valores:>{qtd_espacos_primeiro_valor}}."
              else:
                  output += f" {valores:>{qtd_espacos_demais_valores}}."
            else:
                output += " " * (qtd_espacos_primeiro_valor + 1)  # Espaços vazios para valores ausentes
        else:
            output += " " * 10 * 12  # Espaços vazios para valores ausentes
        output += "\n"                                                                                                                                      
    output += " 999\n"

    mes_atual = dataAnoMesAtual.month
    ano_Atual = dataAnoMesAtual.year

    output_C_ADIC = F"C_ADIC_{ano_Atual}{mes_atual}.DAT"
    # Caminho de saída do arquivo
    caminho_saida = f"/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/webhook/arquivos/tmp/Deck NEWAVE Preliminar/{output_C_ADIC}"

    with open(caminho_saida, "w") as file:
        # Aplicando a substituição com regex
        texto_atualizado = re.sub(r'\b(\d{1,4})\.', lambda x: ' ' * (6 - len(x.group(1))) + x.group(0), output)
        file.write(texto_atualizado)

    
    # ajustar o caminho para dados dinamicos
    path_arquivo_base = os.path.abspath(f"/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/webhook/arquivos/tmp/Deck NEWAVE Preliminar/deck_newave_{ano_Atual}_{mes_atual:02d}_Preliminar/C_ADIC.DAT")
    
    path_saida = "/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/webhook/arquivos/tmp/Deck NEWAVE Preliminar"
    
    path_arquivo_extracaoDados = f"/WX2TB/Documentos/fontes/PMO/scripts_unificados/apps/webhook/arquivos/tmp/Deck NEWAVE Preliminar/{output_C_ADIC}"
    arquivo_cAdic_Final = gerar_c_adic_dat(path_arquivo_base, path_saida, path_arquivo_extracaoDados,dataAnoMesAtual)
    
    return arquivo_cAdic_Final

def gerar_dados_sistema_dat_NW(path, data):

    dataAnoMesAtual = pd.to_datetime(data, format="%B_%Y")
    archive = zipfile.ZipFile(path, 'r')
    xlfile = archive.open(archive.filelist[0])
    columns_to_skip = ['WEEK', 'GAUGE', 'REVISION', 'LOAD_cMMGD']

    try:
        pmo_mes = pd.read_excel(xlfile, usecols=lambda x: x not in columns_to_skip, sheet_name=1)
    except:
        pmo_mes = pd.read_excel(xlfile, usecols=lambda x: x not in columns_to_skip)

    pmo_filtrado_mes = pmo_mes[(pmo_mes.TYPE == "MEDIUM")]

    df_PMO_energiaTotal = pd.DataFrame(pmo_filtrado_mes)
    df_PMO_usinasNaoSimuladas_resultado = pd.DataFrame(pmo_filtrado_mes)

    # removendo colunas 
    colunas_remover_energiaTotal = ['TYPE', 'Base_CGH', 'Base_EOL', 'Base_UFV', 'Base_UTE', 'Exp_CGH', 'Exp_EOL', 'Exp_UFV', 'Exp_UTE', 'Base_MMGD', 'Exp_MMGD']
    colunas_remover_usinasNaoSimuladas = ['TYPE', 'Base_CGH', 'Base_EOL', 'Base_UFV', 'Base_UTE', 'Base_MMGD', 'Exp_MMGD','LOAD_sMMGD']

    df_PMO_energiaTotal_resultado = df_PMO_energiaTotal.drop(columns=colunas_remover_energiaTotal)
    df_PMO_usinasNaoSimuladas_resultado = df_PMO_usinasNaoSimuladas_resultado.drop(columns=colunas_remover_usinasNaoSimuladas)

    # Definindo o mapeamento de região
    mapeamento_regiao = {'SUDESTE': 1, 'SUL': 2, 'NORDESTE': 3, 'NORTE': 4}
    mapeamento_geracao = {'Exp_CGH':'PCH MMGD','Exp_EOL':'EOL MMGD','Exp_UFV':'UFV MMGD','Exp_UTE':'PCT MMGD'}
    mapeamento_codigo_fonte = {'PCH MMGD':5,'EOL MMGD':7,'UFV MMGD':8,'PCT MMGD':6}

    # Mapeando as regiões de acordo com o region_mapping
    df_PMO_energiaTotal_resultado['SOURCE'] = df_PMO_energiaTotal_resultado['SOURCE'].map(mapeamento_regiao)
    df_PMO_usinasNaoSimuladas_resultado['SOURCE'] = df_PMO_usinasNaoSimuladas_resultado['SOURCE'].map(mapeamento_regiao)
    df_PMO_usinasNaoSimuladas_resultado.rename(columns=mapeamento_geracao, inplace=True)

    # Agrupando por mês e fonte
    df_agrupado_energiaTotal = df_PMO_energiaTotal_resultado.groupby([df_PMO_energiaTotal_resultado['DATE'].dt.strftime('%Y-%m'), 'SOURCE']).mean().reset_index()
    df_agrupado_usinasNaoSimuladas = df_PMO_usinasNaoSimuladas_resultado.groupby([df_PMO_usinasNaoSimuladas_resultado['DATE'].dt.strftime('%Y-%m'), 'SOURCE']).mean().reset_index()

    arquivo_saida = criar_arquivo_extracao_sistema_dat(df_agrupado_energiaTotal,df_agrupado_usinasNaoSimuladas,mapeamento_codigo_fonte,dataAnoMesAtual)

    return arquivo_saida

def gerar_dados_cAdic_NW(path, data):

    dataAnoMesAtual = pd.to_datetime(data, format="%B_%Y")

    archive = zipfile.ZipFile(path, 'r')
    xlfile = archive.open(archive.filelist[0])
    columns_to_skip = ['WEEK', 'GAUGE', 'REVISION', 'LOAD_cMMGD']

    try:
        pmo_mes = pd.read_excel(xlfile, usecols=lambda x: x not in columns_to_skip, sheet_name=1)
    except:
        pmo_mes = pd.read_excel(xlfile, usecols=lambda x: x not in columns_to_skip)

        pmo_filtrado_mes = pmo_mes[(pmo_mes.TYPE == "MEDIUM")]

        df_PMO_MMGD_resultado = pd.DataFrame(pmo_filtrado_mes)

        # removendo colunas 
        colunas_remover_MMGD = ['TYPE', 'Base_CGH', 'Base_EOL', 'Base_UFV', 'Base_UTE', 'Exp_MMGD','LOAD_sMMGD','Exp_CGH','Exp_EOL','Exp_UFV','Exp_UTE']

        df_PMO_MMGD_resultado = df_PMO_MMGD_resultado.drop(columns=colunas_remover_MMGD)

        # Definindo o mapeamento de região
        mapeamento_regiao_titulo = {'SUDESTE':'1''  ''SUDESTE''            ''MMGD SE', 'SUL':'2''      ''SUL''             ''MMGD S', 'NORDESTE':'3'' ''NORDESTE''            ''MMGD NE', 'NORTE':'4''    ''NORTE''             ''MMGD N'}

        # Mapeando as regiões de acordo com o region_mapping
        df_PMO_MMGD_resultado['SOURCE'] = df_PMO_MMGD_resultado['SOURCE'].map(mapeamento_regiao_titulo)

        # Agrupando por mês e fonte e calculando a média
        df_agrupado_MMGD = df_PMO_MMGD_resultado.groupby([df_PMO_MMGD_resultado['DATE'].dt.strftime('%Y-%m'), 'SOURCE']).mean().reset_index()

        arquivo_saida = criar_arquivo_extracao_c_adic_dat(df_agrupado_MMGD,mapeamento_regiao_titulo,dataAnoMesAtual)

        return arquivo_saida

def apagar_arquivos_C_Adic_Sistema_DAT_obsoletos(caminho):

    # Padrões de nomes de arquivos a serem removidos
    padroes = ["SISTEMA_DAT*", "C_ADIC_*"]
    
    # Contador para o número de arquivos removidos
    num_arquivos_removidos = 0
    # Loop pelos padrões de nomes de arquivos
    for padrao in padroes:
        # Lista de arquivos correspondentes ao padrão
        arquivos_correspondentes = glob.glob(os.path.join(caminho, padrao))
        
        # Loop pelos arquivos correspondentes
        for arquivo in arquivos_correspondentes:
            # Verifica se é um arquivo e remove
            if os.path.isfile(arquivo):
                os.remove(arquivo)
                print(f"Arquivo {arquivo} removido com sucesso!")
                num_arquivos_removidos += 1
                
    return num_arquivos_removidos


def gerador_arquivos_cAdic_Sistema_DAT(path,dataAnoMesAtual):
    resposta_cAdic_NW = None
    resposta_Sistema_NW = None
    arquivo_deckUtilizado = ""
    
    mes_atual_numero = dataAnoMesAtual.month
    mes_Atual_string = dataAnoMesAtual.strftime('%B').capitalize()
    ano_Atual = dataAnoMesAtual.year
    
    path_ = os.path.join(path,"Previsões de carga mensal e por patamar - NEWAVE",f"RV0_PMO_{mes_Atual_string}_{ano_Atual}_carga_mensal.zip")
    path_saida = os.path.join(path,"Deck NEWAVE Preliminar")

    try:
        path_arquivo_base = os.path.join(path,"Deck NEWAVE Preliminar",f"deck_newave_{ano_Atual}_{mes_atual_numero:02d}_Preliminar.zip")
        
        DIR_TOOLS.extrair_zip_mantendo_nome_diretorio(path_arquivo_base, deleteAfterExtract=False)
        resposta_cAdic_NW = gerar_dados_cAdic_NW(path_, dataAnoMesAtual)
        resposta_Sistema_NW = gerar_dados_sistema_dat_NW(path_, dataAnoMesAtual)

        arquivo_deckUtilizado = f"Foi utilizado o arquivo deck newave preliminar atual {mes_atual_numero}/{ano_Atual}.<br>"
        
        apagar_arquivos_C_Adic_Sistema_DAT_obsoletos(path_saida)
    except Exception as e:
        print(f"Erro ao manipular arquivos do NEWAVE (atual): {e}")
        try:
            # Ajusta o mês para o mês anterior
            data_AnoMesAnterior = dataAnoMesAtual - relativedelta(months=1)
            mes_anterior_numero = data_AnoMesAnterior.month 
            ano_anterior = data_AnoMesAnterior.year
            path_arquivo_base =  os.path.join(path,"Deck NEWAVE Preliminar",f"deck_newave_{ano_anterior}_{mes_anterior_numero:02d}_Preliminar.zip")
            
            DIR_TOOLS.extrair_zip_mantendo_nome_diretorio(path_arquivo_base, deleteAfterExtract=False)
            
            data_mesAnterior = dataAnoMesAtual - relativedelta(months=1)
            resposta_cAdic_NW = gerar_dados_cAdic_NW(path, data_mesAnterior)
            resposta_Sistema_NW = gerar_dados_sistema_dat_NW(path, data_mesAnterior)
            
            arquivo_deckUtilizado = f"Foi utilizado o deck do mês passado {mes_anterior_numero}/{ano_anterior} para gerar os arquivos de C_ADIC.DAT e SISTEMA.DAT.<br> Arquivo newave preliminar ainda não foi disponibilizado.<br>"
            apagar_arquivos_C_Adic_Sistema_DAT_obsoletos(path_saida)
        except Exception as e:
            print(f"Erro ao manipular arquivos do NEWAVE (anterior): {e}")
    
    return resposta_cAdic_NW, resposta_Sistema_NW, arquivo_deckUtilizado


if __name__ == '__main__':

  pathLibs = os.path.abspath(sys.path[0])
  pathApp = os.path.dirname(pathLibs)
  pathApps = os.path.dirname(pathApp)
  pathRaiz = os.path.dirname(pathApps)
  pathBibliotecas = os.path.join(pathRaiz,'bibliotecas')

  sys.path.insert(1, pathBibliotecas)
  
  import wx_opweek

  pathZip = os.path.abspath(r'C:\WX2TB\Documentos\fontes\PMO\monitora_ONS\plan_acomph_rdh\RV3_PMO_Janeiro2022_carga_semanal.zip')
  cargaPatamar(pathZip)

  
  
  
