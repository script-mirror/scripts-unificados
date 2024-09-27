
import os
import re
import sys
import pdb
import glob
import shutil
import datetime
import zipfile


path_fontes = "/WX2TB/Documentos/fontes/"
sys.path.insert(1,path_fontes)
from PMO.scripts_unificados.bibliotecas import wx_opweek,wx_dbUpdater
from PMO.scripts_unificados.apps.decomp.libs import wx_dadger,wx_dadgnl

path_home =  os.path.expanduser("~")
path_app = os.path.dirname(os.path.realpath(__file__))

p_arquivos = os.path.join(os.path.dirname(__file__), 'arquivos')


def baixar_deck_dc_ccee(path_download='', ):
    from PMO.scripts_unificados.bibliotecas import wx_ccee
    
    pathArquivos = os.path.abspath(r'C:\Users\cs341052\Downloads')
    nome_arquivo_pesquisa = 'Deck decomp'
    regex_nome_arquivo = 'Decomp - _[0-9]{2} - [0-9]{2}/[0-9]{4}'
    dtAtual = datetime.datetime.now()
    numDiasHistorico = 32
    wx_ccee.download_arquivo_acervo(pathArquivos, nome_arquivo_pesquisa, regex_nome_arquivo ,dtAtual, numDiasHistorico)

def baixar_deck_nw_ccee(path_download='', ):
    from PMO.scripts_unificados.bibliotecas import wx_ccee
    global p_arquivos
    pathArquivos = os.path.join(p_arquivos, 'entrada')
    
    # modificar o nome e o regex do arquivo
    nome_arquivo_pesquisa = 'Deck decomp'
    regex_nome_arquivo = 'Decomp - _[0-9]{2} - [0-9]{2}/[0-9]{4}'
    dtAtual = datetime.datetime.now()
    numDiasHistorico = 32
    wx_ccee.download_arquivo_acervo(pathArquivos, nome_arquivo_pesquisa, regex_nome_arquivo ,dtAtual, numDiasHistorico)

def montar_decks_base(data_inicio_rv, path_arquivos_saida=None):
    
    dt_revisao = wx_opweek.ElecData(data_inicio_rv)
    p_arquivos = os.path.join(os.path.dirname(__file__), 'arquivos')
    path_arquivos_entrada = os.path.join(p_arquivos, 'entrada')
    
    if path_arquivos_saida == None:
        path_arquivos_saida = os.path.join(p_arquivos, 'saida')
    
    path_arquivos_saida = os.path.join(p_arquivos, 'saida', 'deck_{}{:0>2}_REV{}'.format(dt_revisao.anoReferente, dt_revisao.mesReferente, dt_revisao.atualRevisao))
    if os.path.exists(path_arquivos_saida):
        shutil.rmtree(path_arquivos_saida)
    
    os.makedirs(path_arquivos_saida)
    
    rv = 'REV{}'.format(dt_revisao.atualRevisao)
    if rv == 'REV0':
        rv = 'PMO'

    path_decks = os.path.join(path_home,'Desktop','deck')
    
    path_gevazp_zip = os.path.join(path_decks, 'gevazp', 'Gevazp_{}{:0>2}_{}.zip'.format(dt_revisao.anoReferente, dt_revisao.mesReferente, rv))
    path_deck_nw = os.path.join(path_decks, "newave", "NW{}{:0>2}.zip".format(dt_revisao.anoReferente, dt_revisao.mesReferente))

    path_deck_dc_ccee = os.path.join(path_decks, "decomp", "ccee", f"DC{dt_revisao.anoReferente}{dt_revisao.mesReferente:0>2}.zip")
    
    ano_rv = dt_revisao.anoReferente
    mes_rv = f'{dt_revisao.mesReferente:0>2}'
    num_rv = dt_revisao.atualRevisao

    # Se o deck da ccee ja tiver saido, ele e utilizado
    if os.path.exists(path_deck_dc_ccee):

        print('Utilizando o deck da CCEE')
        path_deck_ccee = (path_deck_dc_ccee).replace('.zip', '')
        
        if os.path.exists(path_deck_ccee):
            shutil.rmtree(path_deck_ccee)
        
        with zipfile.ZipFile(path_deck_dc_ccee, 'r') as zip_ref:
            zip_ref.extractall(path_deck_ccee)

        path_deck_ccee_rv_atual_zip = os.path.join(path_deck_ccee, f'DC{ano_rv}{mes_rv}-sem{num_rv+1}.zip')
        path_deck_ccee_rv_atual = path_deck_ccee_rv_atual_zip.replace('.zip', '')
        with zipfile.ZipFile(path_deck_ccee_rv_atual_zip, 'r') as zip_ref:
            zip_ref.extractall(path_deck_ccee_rv_atual)
        
        path_deck_dc = path_deck_ccee_rv_atual
            
    else:
        print(f'Deck do decomp da CCEE {path_deck_dc_ccee} não encontrado!')
        print('Utilizando o deck da ONS (preliminar)')        
        path_deck_dc = os.path.join(path_decks, "decomp", "ons", f'DEC_ONS_{mes_rv}{ano_rv}_RV{num_rv}_VE')

    if not os.path.exists(path_deck_dc):
            linkDownload_deck = 'https://sintegre.ons.org.br/sites/9/52/Paginas/servicos/historico-de-produtos.aspx?produto=Deck%20Preliminar%20DECOMP%20-%20Valor%20Esperado'
            linkDownload_deck += '\nou\nhttps://sintegre.ons.org.br/sites/9/52/Produtos/296'
            raise Exception('Deck preliminar nao encontrado:\n%s\n%s' %(path_deck_dc, linkDownload_deck))
    
    if not os.path.exists(path_gevazp_zip):
        linkDownload_gevazp = 'https://sintegre.ons.org.br/sites/9/13/79/Produtos/Forms/AllItems.aspx?RootFolder=%2fsites%2f9%2f13%2f79%2fProdutos%2f237&FolderCTID=0x012000D29A893282068C45A9594DC5F39A639B#InplviewHash55c57170-723a-497f-9d27-422657ca0334=FolderCTID%3D0x012000D29A893282068C45A9594DC5F39A639B-SortField%3DModified-SortDir%3DDesc-RootFolder%3D%252Fsites%252F9%252F13%252F79%252FProdutos%252F237'
        raise Exception('Nenhum deck gevazp nao encontrado:\n%s\n\n%s' %(path_gevazp_zip, linkDownload_gevazp))
    
    if not os.path.exists(path_deck_nw):
        linkDownload_deck_nw = 'https://www.ccee.org.br/en/acervo-ccee'
        raise Exception('Nenhum deck NW nao encontrado:\n%s\n\n%s' %(path_deck_nw, linkDownload_deck_nw))
    
    # https://www.ccee.org.br/acervo-ccee?especie=44884&periodo=365

    path_gevazp = (path_gevazp_zip).replace('.zip', '')
    
    if os.path.exists(path_gevazp):
        shutil.rmtree(path_gevazp)
    
    with zipfile.ZipFile(path_gevazp_zip, 'r') as zip_ref:
        zip_ref.extractall(path_gevazp)
    
    arquivos_copiar_gevasp = ['MODIF.DAT', 'POSTOS.DAT', 'REGRAS.DAT', 'VAZOES.DAT']
    for file in arquivos_copiar_gevasp:
        pathSrc = os.path.join(path_gevazp, 'Gevazp', file)
        pathDst = os.path.join(path_arquivos_saida, file.lower())
        shutil.copyfile(pathSrc, pathDst)
        print(pathDst)
        
    shutil.rmtree(path_gevazp)

    ajustar_deck(path_deck_dc, dt_revisao, mudar_gap=True, path_saida=path_arquivos_saida)
    
    arquivos_deck_ons_ve = glob.glob(os.path.join(path_deck_dc, '*'))
    regex_arquivos_ignorar = r"dadger\.rv[0-9]|dadgnl\.rv[0-9]"
    for file in arquivos_deck_ons_ve:
        nome_arquivo_saida = os.path.basename(file).lower()
        if not re.match(regex_arquivos_ignorar, nome_arquivo_saida):
            pathDst = os.path.join(path_arquivos_saida, nome_arquivo_saida)
            shutil.copyfile(file, pathDst)
            print(pathDst)
    
    print('\nFaltando os seguintes passos a serem feitos manualmente:')
    print('  # {}'.format(os.path.join(path_arquivos_saida, 'dadger.rv'+str(dt_revisao.atualRevisao))))
    print('\t [HQ] - Verificar casos de "Flexibilizada para convergencia"')
    print('  # {}'.format(os.path.join(path_arquivos_saida, 'dadgnl.rv'+str(dt_revisao.atualRevisao))))
    print('\t [GS] - Atualizar o numero de semanas')
    print('\t [GL] - Inserir os valores de geracoes para o ultimo estagio para cada uma das termicas (fonte: Pegar o dagnl da ultima rodada do pluvia no prospec)')
    
def preparar_decks_gerados_pelo_prospec(p_path):

    path_saida = p_path.replace('.zip', '')
    
    if os.path.exists(path_saida):
        shutil.rmtree(path_saida)
    
    with zipfile.ZipFile(p_path, 'r') as zip_ref:
        zip_ref.extractall(path_saida)
    
    decks = glob.glob(os.path.join(path_saida,'*'))
    decks_dict = {}
    for dec in decks:
        nome_deck = os.path.basename(dec)
        
        match = re.match('DC([0-9]{4})([0-9]{2})-sem([0-9]{1})', nome_deck)
        if match:
            ano = match[1]
            mes = match[2]
            rv = int(match[3]) - 1
            key = int(f'{ano}{mes}{rv}')
            decks_dict[key] = nome_deck
            

            path_src = os.path.join(p_arquivos, f'prevs.rv{rv}')
            path_dst = os.path.join(dec, f'prevs.rv{rv}')
            shutil.copyfile(path_src, path_dst)
            
            path_src = os.path.join(p_arquivos, 'volume_uhe.csv')
            path_dst = os.path.join(dec, 'volume_uhe.csv')
            shutil.copyfile(path_src, path_dst)
            
    ordem_decks = list(decks_dict.keys())
    ordem_decks.sort()
    
    deck_src = os.path.join(path_saida, decks_dict[ordem_decks[0]])
    rv_src = int(deck_src[-1]) - 1
    deck_dst = os.path.join(path_saida, decks_dict[ordem_decks[1]])
    rv_dst = int(deck_dst[-1]) - 1
    
    path_src = os.path.join(deck_src, f'dadgnl.rv{rv_src}')
    path_dst = os.path.join(deck_dst, f'dadgnl.rv{rv_dst}')
    shutil.copyfile(path_src, path_dst)
    
    path_src = os.path.join(deck_src, 'vazoes.dat')
    path_dst = os.path.join(deck_dst, 'vazoes.dat')
    shutil.copyfile(path_src, path_dst)
    
    os.remove(os.path.join(deck_dst, 'volume_uhe.csv'))
    
    
            
def importar_deck_entrada(p_path, p_deleteApos):
    
    path_arquivos_tmp = os.path.join(path_app, 'arquivos', 'tmp')
    
    path_arquivo = p_path
    nome_arquivo = os.path.basename(path_arquivo)
    
    dstFolder = os .path.join(path_arquivos_tmp, nome_arquivo.replace('.zip', ''))

    if os.path.exists(dstFolder):
        shutil.rmtree(dstFolder)
        
    os.makedirs(dstFolder)

    with zipfile.ZipFile(path_arquivo, 'r') as zip_ref:
        zip_ref.extractall(dstFolder)
        
    deck_entrada_zip = glob.glob(os.path.join(dstFolder, 'DEC_ONS_*'))[0]
    nome_pasta_dec = os.path.basename(deck_entrada_zip).replace('.zip', '')
    
    match = re.match(r'DEC_ONS_([0-9]{2})([0-9]{4})_RV([0-9]{1})_VE', nome_pasta_dec)
    mes = int(match.group(1))
    ano = int(match.group(2))
    rv = int(match.group(3))
    
    data_inicio_mes = wx_opweek.getLastSaturday(datetime.date(ano, mes, 1))
    data_inicio_rv = data_inicio_mes + datetime.timedelta(days=7*rv)
    
    path_deck_entrada = os.path.join(dstFolder, nome_pasta_dec)
    with zipfile.ZipFile(deck_entrada_zip, 'r') as zip_ref:
        zip_ref.extractall(path_deck_entrada)
    
    path_dadger = os.path.join(path_deck_entrada, 'dadger.rv'+str(rv))
    wx_dbUpdater.importar_dc_bloco_pq(path=path_dadger, dtRef=data_inicio_rv, fonte='ons')
    wx_dbUpdater.importar_dc_bloco_dp(path=path_dadger, dtRef=data_inicio_rv, fonte='ons')
    
    shutil.rmtree(path_deck_entrada)
    
    if p_deleteApos:
        os.remove(path_arquivo)

def ajustar_deck(path_deck, dt_revisao, mudar_gap=False, path_saida=None):
    
    ano_ref = dt_revisao.anoReferente
    mes_ref = f'{dt_revisao.mesReferente:0>2}'
    rv_ref = dt_revisao.atualRevisao
   
    if path_saida == None:
        path_saida = os.path.join(path_deck, 'deck_comentado')
        os.makedirs(path_saida)
    
    # DADGER
    path_dadger = os.path.join(path_deck, 'dadger.rv{}'.format(rv_ref))
    path_novo_dadger = os.path.join(path_saida, 'dadger.rv{}'.format(rv_ref))
    df_dadger, comentarios = wx_dadger.leituraArquivo(path_dadger)
    df_dadger, comentarios = wx_dadger.comentar_dadger_ons_ccee(df_dadger, comentarios)
    df_dadger, comentarios = wx_dadger.add_protecao_gap_negativo(df_dadger, comentarios)
    
    # Mudanca na tolerancia para convergência
    if mudar_gap:
        df_dadger['GP'].loc[0,'valor'] = 0.005
    
    wx_dadger.escrever_dadger(df_dadger, comentarios, path_novo_dadger)
    
    # DADGNL
    path_gadgnl = os.path.join(path_deck, 'dadgnl.rv{}'.format(rv_ref))
    path_novo_dadgnl = os.path.join(path_saida, 'dadgnl.rv{}'.format(rv_ref))
    df_dadgnl, comentarios = wx_dadgnl.leituraArquivo(path_gadgnl)       
    df_dadgnl, comentarios = wx_dadgnl.comentar_dadgnl_ons_ccee(df_dadgnl, comentarios)
    
    
    # Atualizacoes dos CVUs
    df_dadgnl['TG']['ip'] = df_dadgnl['TG']['ip'].astype('int')
    for cod_usina in df_dadgnl['TG']['cod'].unique():
        rows_cvu_usinas = df_dadgnl['TG']['cod'] == cod_usina
        
        # Caso exista mais de um estagio
        if df_dadgnl['TG'].loc[rows_cvu_usinas].shape[0] > 1:
            # caso tenha alguma alteracao no estagio 2, o estagio 1 deve ser dropado
            if df_dadgnl['TG'].loc[rows_cvu_usinas, 'ip'].iloc[1] == 2:
                idx_todos_estagios = df_dadgnl['TG'].loc[rows_cvu_usinas].index
                df_dadgnl['TG'].loc[idx_todos_estagios, 'ip'] = df_dadgnl['TG'].loc[idx_todos_estagios, 'ip'] - 1
                
                idx_estag_1 = df_dadgnl['TG'].loc[rows_cvu_usinas].iloc[0].name
                df_dadgnl['TG'] = df_dadgnl['TG'].drop(idx_estag_1)
                
                if idx_estag_1 in comentarios['TG']:
                    if idx_estag_1 + 1 in comentarios['TG']:
                        comentarios['TG'][idx_estag_1 + 1] += comentarios['TG'].pop(idx_estag_1)
                    else:
                        comentarios['TG'][idx_estag_1 + 1] = comentarios['TG'].pop(idx_estag_1)
                
            # subtrai 1 estagio para os estagios diferentes de 1
            else:
                idx_estagios_3_adiante = df_dadgnl['TG'].loc[rows_cvu_usinas].index[1:]
                df_dadgnl['TG'].loc[idx_estagios_3_adiante, 'ip'] = df_dadgnl['TG'].loc[idx_estagios_3_adiante, 'ip'] - 1
            
    
    # Atualizações de geracoes comandadas
    idx_sem1 = df_dadgnl['GL'].loc[df_dadgnl['GL']['sem'].astype('int') == 1].index
    df_dadgnl['GL'] = df_dadgnl['GL'].drop(idx_sem1)
    df_dadgnl['GL']['sem'] = df_dadgnl['GL']['sem'].astype(int) - 1
    
    for idx in idx_sem1:
        if idx + 1 in comentarios['GL']:
            comentarios['GL'][idx + 1] += comentarios['GL'].pop(idx)
        else:
            comentarios['GL'][idx + 1] = comentarios['GL'].pop(idx)
    
    wx_dadgnl.escrever_dadgnl(df_dadgnl, comentarios, path_novo_dadgnl)
    
    return True



def converter_deck_ons_ccee(path_arquivo_ons, mudar_gap=False, novo_deck=None):
    
    path_diretorio = os.path.dirname(path_arquivo_ons)
    nome_arquivo_zip = os.path.basename(path_arquivo_ons).split('.')[0]
    
    path_deck_resultados = os.path.join(path_diretorio, nome_arquivo_zip)
    
    if os.path.exists(path_deck_resultados):
        shutil.rmtree(path_deck_resultados)
        
    with zipfile.ZipFile(path_arquivo_ons, 'r') as zip_ref:
        zip_ref.extractall(path_deck_resultados)
        
    arquivos = glob.glob(os.path.join(path_deck_resultados, 'DEC_ONS_*.zip'))
    path_deck_zip = arquivos[0]
    nome_deck_zip = os.path.basename(path_deck_zip)
    
    infos = re.match('DEC_ONS_([0-9]{2})([0-9]{4})_RV([0-9]{1})_VE.zip', nome_deck_zip)
    mes_ref = infos.group(1)
    ano_ref = infos.group(2)
    rv_ref = infos.group(3)
    
    path_diretorio_deck = os.path.dirname(path_deck_zip)
    nome_deck = nome_deck_zip.split('.')[0]
    
    path_deck = os.path.join(path_diretorio_deck, nome_deck)
    with zipfile.ZipFile(path_deck_zip, 'r') as zip_ref:
        zip_ref.extractall(path_deck)
    
    if novo_deck == None:
        novo_deck = os.path.join(path_deck, 'deck_comentado')
        os.makedirs(novo_deck)
    
    # DADGER
    path_dadger = os.path.join(path_deck, 'dadger.rv{}'.format(rv_ref))
    path_novo_dadger = os.path.join(novo_deck, 'dadger.rv{}'.format(rv_ref))
    df_dadger, comentarios = wx_dadger.leituraArquivo(path_dadger)
    df_dadger, comentarios = wx_dadger.comentar_dadger_ons_ccee(df_dadger, comentarios)
    
    # Mudanca na tolerancia para convergência
    if mudar_gap:
        df_dadger['GP'].loc[0,'valor'] = 0.005
    
    wx_dadger.escrever_dadger(df_dadger, comentarios, path_novo_dadger)
    
    # DADGNL
    path_gadgnl = os.path.join(path_deck, 'dadgnl.rv{}'.format(rv_ref))
    path_novo_dadgnl = os.path.join(novo_deck, 'dadgnl.rv{}'.format(rv_ref))
    df_dadgnl, comentarios = wx_dadgnl.leituraArquivo(path_gadgnl)       
    df_dadgnl, comentarios = wx_dadgnl.comentar_dadgnl_ons_ccee(df_dadgnl, comentarios)
    
    
    # Atualizacoes dos CVUs
    df_dadgnl['TG']['ip'] = df_dadgnl['TG']['ip'].astype('int')
    for cod_usina in df_dadgnl['TG']['cod'].unique():
        rows_cvu_usinas = df_dadgnl['TG']['cod'] == cod_usina
        
        # Caso exista mais de um estagio
        if df_dadgnl['TG'].loc[rows_cvu_usinas].shape[0] > 1:
            # caso tenha alguma alteracao no estagio 2, o estagio 1 deve ser dropado
            if df_dadgnl['TG'].loc[rows_cvu_usinas, 'ip'].iloc[1] == 2:
                idx_todos_estagios = df_dadgnl['TG'].loc[rows_cvu_usinas].index
                df_dadgnl['TG'].loc[idx_todos_estagios, 'ip'] = df_dadgnl['TG'].loc[idx_todos_estagios, 'ip'] - 1
                
                idx_estag_1 = df_dadgnl['TG'].loc[rows_cvu_usinas].iloc[0].name
                df_dadgnl['TG'] = df_dadgnl['TG'].drop(idx_estag_1)
                
                if idx_estag_1 in comentarios['TG']:
                    if idx_estag_1 + 1 in comentarios['TG']:
                        comentarios['TG'][idx_estag_1 + 1] += comentarios['TG'].pop(idx_estag_1)
                    else:
                        comentarios['TG'][idx_estag_1 + 1] = comentarios['TG'].pop(idx_estag_1)
                
            # subtrai 1 estagio para os estagios diferentes de 1
            else:
                idx_estagios_3_adiante = df_dadgnl['TG'].loc[rows_cvu_usinas].index[1:]
                df_dadgnl['TG'].loc[idx_estagios_3_adiante, 'ip'] = df_dadgnl['TG'].loc[idx_estagios_3_adiante, 'ip'] - 1
            
    
    # Atualizações de geracoes comandadas
    idx_sem1 = df_dadgnl['GL'].loc[df_dadgnl['GL']['sem'].astype('int') == 1].index
    df_dadgnl['GL'] = df_dadgnl['GL'].drop(idx_sem1)
    df_dadgnl['GL']['sem'] = df_dadgnl['GL']['sem'].astype(int) - 1
    
    for idx in idx_sem1:
        if idx + 1 in comentarios['GL']:
            comentarios['GL'][idx + 1] += comentarios['GL'].pop(idx)
        else:
            comentarios['GL'][idx + 1] = comentarios['GL'].pop(idx)
    
    wx_dadgnl.escrever_dadgnl(df_dadgnl, comentarios, path_novo_dadgnl)
    
    return path_deck_resultados

def alterar_bloco_dager(path_dadger, novoBloco):
    path_dadger = os.path.abspath(path_dadger)
    file = open(path_dadger, 'r', encoding='latin-1')
    
    arquivo = file.readlines()
    file.close()
    
    bloco_alteracao = None
    for linha in novoBloco:
        match = re.match(r'& {1,} BLOCO ([0-9]{1}).*', linha)
        if match:
            bloco_alteracao = int(match.group(1))

    inicio_bloco = 0
    fim_bloco = 0
    for i_linha, linha in enumerate(arquivo):
        
        if re.match(r'^&-{1,}-$', linha):
            if i_linha - inicio_bloco > 4:
                if bloco_alteracao == numero_bloco:
                    fim_bloco = i_linha
                    break
                inicio_bloco = i_linha
        
        elif re.match(r'& {1,} BLOCO ([0-9]{1}).*', linha):
            match = re.match(r'& {1,} BLOCO ([0-9]{1}).*', linha)
            numero_bloco = int(match.group(1))

    novoBloco_com_quebralinha = [f'{linha}\n' for linha in novoBloco]
    novo_arquivo = arquivo[:inicio_bloco] + novoBloco_com_quebralinha + arquivo[fim_bloco:]
    with open(path_dadger, 'w') as f:
            f.writelines(novo_arquivo)
    print(path_dadger)

def atualizacao_carga(path_carga_zip,path_deck):
    import locale
    locale.setlocale(locale.LC_TIME, 'pt_BR.utf8')

    path_diretorio = os.path.dirname(path_carga_zip)
    nome_arquivo_zip = os.path.basename(path_carga_zip).split('.')[0]

    match = re.match(r'RV([0-9]{1})_PMO_([A-z]+)([0-9]{4})_carga_semanal', nome_arquivo_zip)
    rv = int(match.group(1))
    mes = match.group(2)
    ano = match.group(3)

    try:
        mes_ref = datetime.datetime.strptime(mes+ano, '%B%Y')
    except:
        mes_ref = datetime.datetime.strptime(mes+ano, '%B_%Y')
        
    inicio_mes_eletrico = mes_ref
    while inicio_mes_eletrico.weekday() != 5:
        inicio_mes_eletrico = inicio_mes_eletrico - datetime.timedelta(days=1)
        
    inicio_rv_atual = inicio_mes_eletrico + datetime.timedelta(days=7*rv)
    
    path_unzip_folder = os.path.join(path_diretorio, nome_arquivo_zip)
    
    if os.path.exists(path_unzip_folder):
        shutil.rmtree(path_unzip_folder)
        
    with zipfile.ZipFile(path_carga_zip, 'r') as zip_ref:
        zip_ref.extractall(path_unzip_folder)

    carga_decomp_txt = glob.glob(os.path.join(path_unzip_folder,'CargaDecomp_PMO*.txt'))[0]
    
    df_dadger, comentarios = wx_dadger.leituraArquivo(carga_decomp_txt)
    
    bloco_dp = df_dadger['DP']
    bloco_dp['ip'] = bloco_dp['ip'].astype(int)
    
    comentarios_dp = comentarios['DP']
    
    semana_eletrica_atual = wx_opweek.ElecData(inicio_rv_atual.date())
    semana_eletrica = wx_opweek.ElecData(inicio_rv_atual.date())
    
    info_blocos = wx_dadger.info_blocos
    while 1:
        nome_dadger = f'dadger.rv{semana_eletrica.atualRevisao}'
        novoBloco = []
        
        for idx, row in bloco_dp.iterrows():
            if idx in comentarios_dp:
                for coment in comentarios_dp[idx]:
                    novoBloco.append(coment.strip())
            novoBloco.append('{}'.format(info_blocos['DP']['formatacao'].format(*row.values).strip()))
            
        path_dadger = os.path.join(path_deck,f"DC{semana_eletrica.anoReferente}{semana_eletrica.mesRefente:0>2}-sem{semana_eletrica.atualRevisao+1}",nome_dadger)
        if not os.path.exists(path_dadger):
            break
        alterar_bloco_dager(path_dadger, novoBloco)
        
        semana_eletrica = wx_opweek.ElecData(semana_eletrica.data + datetime.timedelta(days=7))
        if semana_eletrica.mesRefente != semana_eletrica_atual.mesRefente and semana_eletrica.data.month != semana_eletrica_atual.data.month:
            break
        
        bloco_dp = bloco_dp.loc[bloco_dp['ip']!=1].copy()
        comentarios_dp[bloco_dp.iloc[0].name] = comentarios_dp[0]
        bloco_dp['ip'] = bloco_dp['ip'] - 1

def gerar_rvs_base_zip(p_path, dt_primeira_rv):
    num_rvs_zip_files = [1,2,3,4,5,6,7,8]
    
    primeira_rv_op = wx_opweek.ElecData(dt_primeira_rv)

    for num in num_rvs_zip_files:
        nome_deck_saida = 'DC_{}{:0>2}_rv{}_{}rv'.format(primeira_rv_op.anoReferente,primeira_rv_op.mesReferente,primeira_rv_op.atualRevisao,num)
        decks_zip = []
        fullpath_zip_saida = os.path.join(p_path,f'{nome_deck_saida}.zip')
        print(fullpath_zip_saida)
        with zipfile.ZipFile(fullpath_zip_saida, 'w') as zipObj:
            for i_sem in range(num):
                dt_op_week = wx_opweek.ElecData(dt_primeira_rv + datetime.timedelta(days=7*i_sem))
                deck_DC = 'DC{}{:0>2}-sem{}'.format(dt_op_week.anoReferente,dt_op_week.mesReferente,dt_op_week.atualRevisao+1)
                deck_NW = 'NW{}{:0>2}'.format(dt_op_week.anoReferente,dt_op_week.mesReferente)
                fullpath_deck_DC = os.path.join(p_path,deck_DC)
                fullpath_deck_NW = os.path.join(p_path,deck_NW)
                decks_zip.append(fullpath_deck_DC)
                for folderName, subfolders, filenames in os.walk(fullpath_deck_DC):
                    for filename in filenames:
                        filePath = os.path.join(os.path.basename(folderName), filename)
                        zipObj.write(os.path.join(folderName, filename), filePath)
                    
                if fullpath_deck_NW not in decks_zip:
                    decks_zip.append(fullpath_deck_NW)
                    for folderName, subfolders, filenames in os.walk(fullpath_deck_NW):
                        for filename in filenames:
                            filePath = os.path.join(os.path.basename(folderName), filename)
                            zipObj.write(os.path.join(folderName, filename), filePath)
                    
                    print('\t'+deck_NW)
                print('\t\t'+deck_DC)
        print()
    
def leitura_blocos(p_bloco, p_id, decks_historico=8):
    
    ultimo_sabado = wx_opweek.getLastSaturday(datetime.datetime.now().date())
    proximo_sabado = ultimo_sabado + datetime.timedelta(days=7*1)
    # proxima_rv_elec = wx_opweek.ElecData(proximo_sabado)
    
    path_decks_ons = os.path.join(path_home, 'Desktop','deck','decomp','ons')
    
    info_blocos = wx_dadger.info_blocos
    
    restricoes = {}
    restricoes['RE'] = ['LU', 'FU', 'FT', 'FI']
    restricoes['HQ'] = ['LQ', 'CQ']
    restricoes['HV'] = ['LV', 'CV']
    restricoes['HE'] = ['CM']

    for i_semana in range(1,decks_historico):
        dt_elec = wx_opweek.ElecData(proximo_sabado - datetime.timedelta(days=7*i_semana))
        
        rv_ref = dt_elec.atualRevisao
        mes_ref = dt_elec.mesReferente
        ano_ref = dt_elec.anoReferente
        
        print('############################################################')
        deck = os.path.join(path_decks_ons, 'DEC_ONS_{:0>2}{}_RV{}_VE'.format(mes_ref,ano_ref,rv_ref))
        path_dadger = os.path.join(deck, 'dadger.rv{}'.format(rv_ref))
        print(path_dadger)
        df_dadger, comentarios = wx_dadger.leituraArquivo(path_dadger)
        
        if p_bloco in restricoes:
            
            row = df_dadger[p_bloco].loc[df_dadger[p_bloco]['id_restricao'].astype(int) == int(p_id)].iloc[0]               
            print('{}'.format(info_blocos[p_bloco]['formatacao'].format(*row.values).strip()))
            
            for mnemon_rest in restricoes[p_bloco]:
                
                restricoes_mnemon = df_dadger[mnemon_rest].loc[df_dadger[mnemon_rest]['id_restricao'].astype('int') == int(p_id)]
                
                for index, row in restricoes_mnemon.iterrows():
                    if index in comentarios[mnemon_rest]:
                        for coment in comentarios[mnemon_rest][index]:
                            print(coment.strip())
                    print('{}'.format(info_blocos[mnemon_rest]['formatacao'].format(*row.values).strip()))
                    
        else:
            nomes_colunas = {}
            nomes_colunas['CT'] = 'cod'
            
            bloco = df_dadger[p_bloco].loc[df_dadger[p_bloco][nomes_colunas[p_bloco]].astype('int') == int(p_id)]
            
            for index, row in bloco.iterrows():
                print('{}'.format(info_blocos[p_bloco]['formatacao'].format(*row.values).strip()))
            
            pass
        print('############################################################\n')
    
def printHelper():
    import locale
    locale.setlocale(locale.LC_TIME, 'pt_BR.utf8')

    hoje = datetime.datetime.now()
    ultimo_sabado =  wx_opweek.getLastSaturday(hoje) 
    proximo_sabado =  ultimo_sabado + datetime.timedelta(days=7)
    dois_sabados_a_frente =  ultimo_sabado + datetime.timedelta(days=7*2)
    
    semana_eletrica = wx_opweek.ElecData(proximo_sabado)
    path_decks = os.path.abspath('/WX2TB/Documentos/fontes/PMO/arquivos/decks')
    
    prox_rv = semana_eletrica.atualRevisao
    ano_prox_rv = semana_eletrica.anoReferente
    mes_prox_rv_str = (semana_eletrica.primeiroDiaMes + datetime.timedelta(days=7)).strftime('%B')
    mes_prox_rv_str = mes_prox_rv_str[0].upper() + mes_prox_rv_str[1:].lower()
    
    path_estudo = os.path.join(path_home, "Downloads", "Estudo_18734")
    path_arquivos_saida = os.path.join(path_app,'arquivos','saida')
    
    print("python {} montar_decks_base data {}".format(sys.argv[0], proximo_sabado.strftime("%d/%m/%Y")))
    print("python {} preparar_decks_gerados_pelo_prospec path \"{}\"".format(sys.argv[0], os.path.join(path_arquivos_saida,'Estudo_12920_Entrada.zip')))
    print("python {} gerar_rvs_base_zip dt_primeira_rv {} path \"{}\"".format(sys.argv[0], dois_sabados_a_frente.strftime("%d/%m/%Y"), os.path.join(path_arquivos_saida,'deck_202209_REV2','Estudo_11972_Entrada')))
    print("python {} importar_deck_entrada deck_zip \"{}\" delete_apos true".format(sys.argv[0], os.path.join(path_decks, 'PMO_deck_preliminar.zip')))
    print("python {} leitura_blocos bloco RE id 16".format(sys.argv[0], os.path.join(path_decks, 'PMO_deck_preliminar.zip')))
    print("python {} atualizacao_carga nova_carga_zip \"{}\" deck_path \"{}\"".format(sys.argv[0], os.path.join(path_decks, f'RV{prox_rv}_PMO_{mes_prox_rv_str}{ano_prox_rv}_carga_semanal.zip'),path_estudo))
    
    quit()

if __name__ == "__main__":

    if len(sys.argv) > 1:
        if 'help' in sys.argv or '--h' in sys.argv or '-help' in sys.argv:
            printHelper()

        p_dataReferente = datetime.datetime.now().date()
        p_arquivos = os.path.join(os.path.dirname(__file__), 'arquivos')
        p_arquivos_entrada = os.path.join(p_arquivos, 'entrada')
        p_arquivos_saida = os.path.join(p_arquivos, 'saida')
        
        for i in range(len(sys.argv)):
            argumento = sys.argv[i].lower()

            if argumento in ['data', 'dt_primeira_rv']:
                p_dataReferente = sys.argv[i+1]
                p_dataReferente = datetime.datetime.strptime(p_dataReferente, "%d/%m/%Y")

            if argumento == 'path_zip':
                p_path = sys.argv[i+1]
                
            if argumento == 'path':
                p_path = sys.argv[i+1]
                
            if argumento == 'deck_path':
                p_deckPath = sys.argv[i+1]

            if argumento == 'nova_carga_zip':
                p_novaCargaZip = sys.argv[i+1]

            if argumento == 'path':
                p_path = sys.argv[i+1]
                
            if argumento == 'deck_zip':
                p_path_deck = sys.argv[i+1]
                
            if argumento == 'delete_apos':
                str_deleteApos = sys.argv[i+1]
                p_deleteApos = eval(str_deleteApos[0].upper() + str_deleteApos[1:].lower())

            if argumento == 'bloco':
                p_bloco = sys.argv[i+1]
                
            if argumento == 'id':
                p_id = sys.argv[i+1]
    else:
        printHelper()
    
    if sys.argv[1].lower() == 'montar_decks_base':
        montar_decks_base(p_dataReferente, p_arquivos_saida)
        
    if sys.argv[1].lower() == 'preparar_decks_gerados_pelo_prospec':
        preparar_decks_gerados_pelo_prospec(p_path)
        
    if sys.argv[1].lower() == 'gerar_rvs_base_zip':
        gerar_rvs_base_zip(p_path, p_dataReferente)
        
    if sys.argv[1].lower() == 'importar_deck_entrada':
        importar_deck_entrada(p_path_deck, p_deleteApos)
        
    if sys.argv[1].lower() == 'leitura_blocos':
        leitura_blocos(p_bloco, p_id)
        
    if sys.argv[1].lower() == 'atualizacao_carga':
        atualizacao_carga(p_novaCargaZip,p_deckPath)
        
    # path_deck_zippado = os.path.abspath(r"C:\Users\cs341052\Downloads\PMO_deck_preliminar.zip")
    # # converter_deck_ons_ccee(path_deck_zippado)
    
    # data_inicio_rv = datetime.datetime(2022,7,16)
    # path_saida = r'C:\Users\cs341052\Downloads\decks_base'
    # montar_decks_base(data_inicio_rv, path_saida)
     
