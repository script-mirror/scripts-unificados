import os
import re
import pdb
import sys
import glob
import datetime
import pandas as pd
from sqlalchemy import select,func

path_fontes = "/WX2TB/Documentos/fontes/"
sys.path.insert(1,path_fontes)
from PMO.scripts_unificados.bibliotecas import wx_dbClass , wx_opweek

DROPBOX_MIDDLE_PATH = os.path.join(os.path.expanduser("~"), 'Dropbox', 'WX - Middle')
PATH_CACHE = os.path.join(path_fontes,"PMO","scripts_unificados","apps","web_modelos","server","caches")

def importar_chuva(data, rodada, modelo, flag_estudo):
    
    if flag_estudo:
        prefixo_estudo = '.estudo'
    else:
        prefixo_estudo = ''
        

    path_previsoes = os.path.join(DROPBOX_MIDDLE_PATH, 'NovoSMAP', data.strftime('%Y%m%d'), 'rodada{:0>2}z'.format(rodada))
    padrao_nome = os.path.join(path_previsoes, '{}{}_p{}a*.dat').format(modelo, prefixo_estudo, data.strftime('%d%m%y'))
    arquivos_previsao = glob.glob(padrao_nome)
    if len(arquivos_previsao) == 0:
        raise Exception('Nenhum arquivo encontrado com o padrao de nome:\n%s' %(padrao_nome))

    database = wx_dbClass.db_mysql_master('db_rodadas')
    database.connect()
    
    tb_cadastro_rodadas = database.getSchema('tb_cadastro_rodadas')
    tb_chuva = database.getSchema('tb_chuva')
    tb_subbacia = database.getSchema('tb_subbacia')
    
    selec_query = select([tb_cadastro_rodadas.columns.id,tb_cadastro_rodadas.c.id_chuva]).where(tb_cadastro_rodadas.c.dt_rodada==data.strftime('%Y-%m-%d'), tb_cadastro_rodadas.c.hr_rodada==rodada, tb_cadastro_rodadas.c.str_modelo==modelo)
    info_rodada = database.conn.execute(selec_query)
    
    if info_rodada.rowcount == 0:
        ins_query = tb_cadastro_rodadas.insert().values(str_modelo=modelo, dt_rodada=data, hr_rodada=rodada)
        database.conn.execute(ins_query)
        info_rodada = database.conn.execute(selec_query)

    [id_rodada, id_chuva] = info_rodada.fetchone()
    
    if id_chuva == None:
        
        sel_query = select(func.max(tb_cadastro_rodadas.columns.id_chuva))
        [ultimo_id_chuva] = database.conn.execute(sel_query).fetchone()
        if ultimo_id_chuva == None:
            id_chuva = 1
        else:
            id_chuva = ultimo_id_chuva + 1
            
        upd_query = tb_cadastro_rodadas.update().where(tb_cadastro_rodadas.c.id==id_rodada).values({'id_chuva':id_chuva})
        database.conn.execute(upd_query)
            
    else:
        del_query = tb_chuva.delete().where(tb_chuva.c.id==id_chuva)
        database.conn.execute(del_query)
        
    selec_query = select([tb_subbacia.columns.cd_subbacia,tb_subbacia.c.vl_lat,tb_subbacia.c.vl_lon])
    dados_subbacias = database.conn.execute(selec_query).fetchall()
    df_dados_subbacias = pd.DataFrame(dados_subbacias)
    df_dados_subbacias.columns = ['cd_bacia', 'lat', 'lon']

    vals = []
    for arq_previsao in arquivos_previsao:
        print(arq_previsao)
        nome_arquivo = os.path.basename(arq_previsao)
        match = re.match(r'(.{0,})_p([0-9]{6})a([0-9]{6}).dat', nome_arquivo)
        dt_prevista = datetime.datetime.strptime(match.group(3), '%d%m%y')
        with open(arq_previsao, 'r') as prev:
            for i, linha in enumerate(prev.readlines()):
                lon, lat, precipt = linha.strip().split() 
                subbacia_info = df_dados_subbacias[(df_dados_subbacias['lat'] == float(lat)) & (df_dados_subbacias['lon'] == float(lon))]
                if subbacia_info.shape[0] == 1:
                    vals.append({'id':id_chuva, 'cd_subbacia':subbacia_info['cd_bacia'].item(), 'dt_prevista':dt_prevista, 'vl_chuva':precipt})
                
    database.conn.execute(tb_chuva.insert(), vals)
    print("Arquivos importados com sucesso!")
    
    rodada_format = str(rodada).zfill(2)
    dataFormat = data.strftime("%Y-%m-%d")
    p_modelo_teste = modelo.replace('.PRELIMINAR', '').lower()
    
    cmd = f"cd {PATH_CACHE};"
    rodada_formatada = str(rodada).zfill(2)
    dataFormatada = data.strftime("%Y-%m-%d")
    p_modelo = modelo.replace('.PRELIMINAR', '').lower()
    cmd += f"python rz_cache.py get_resultado_chuva {dataFormatada} {p_modelo} {rodada_formatada} atualizar"
    os.system(cmd)	
    
def get_info_rodadas(dt_rodada, rodada, modelo, flag_preliminar, flag_pdp, flag_psat, flag_estudo, dt_revisao=None):
    
    database = wx_dbClass.db_mysql_master('db_rodadas')
    database.connect()
    
    tb_cadastro_rodadas = database.getSchema('tb_cadastro_rodadas')
    
    colunas_interesse = []
    colunas_interesse.append('id')
    colunas_interesse.append('id_chuva')
    colunas_interesse.append('id_smap')
    colunas_interesse.append('id_previvaz')
    colunas_interesse.append('fl_preliminar')
    colunas_interesse.append('fl_pdp')
    colunas_interesse.append('fl_psat')
    colunas_interesse.append('fl_estudo')
    colunas_interesse.append('dt_revisao')

    col_select = []
    for col in colunas_interesse:
        col_select.append(tb_cadastro_rodadas.c[col])
            
    selec_query = select(col_select).where(
                             tb_cadastro_rodadas.c.dt_rodada==dt_rodada,
                             tb_cadastro_rodadas.c.hr_rodada==rodada,
                             tb_cadastro_rodadas.c.str_modelo==modelo
                            )
    info_rodadas = database.conn.execute(selec_query)
    
    rodada_desejada = {'id':None, 'id_chuva':None, 'id_smap':None, 'id_previvaz':None}
    for rodada in info_rodadas:
        rodada_dict = dict(rodada)

        # O id da chuva sera sempre igual para todas as rodadas 
        # com o mesmo STR_MODELO, DT_RODADA e HR_RODADA
        rodada_desejada['id_chuva'] = rodada_dict['id_chuva']
        
        if rodada_dict['fl_preliminar'] == flag_preliminar \
                    and rodada_dict['fl_pdp'] == flag_pdp \
                    and rodada_dict['fl_psat'] == flag_psat \
                    and rodada_dict['fl_estudo'] == flag_estudo:
            
            rodada_desejada['id_smap'] = rodada_dict['id_smap']
            
            if dt_revisao == None or rodada_dict['dt_revisao'] == None or rodada_dict['dt_revisao'] == dt_revisao: 
                    rodada_desejada = rodada_dict
                    break
            
        elif rodada_dict['fl_preliminar'] == None \
                    and rodada_dict['fl_pdp'] == None \
                    and rodada_dict['fl_psat'] == None \
                    and rodada_dict['dt_revisao'] == None \
                    and rodada_dict['fl_estudo'] == None:
            rodada_desejada = rodada_dict
            break
    
    database.conn.close()
    return rodada_desejada
    

def importar_smap(data, rodada, modelo, flag_preliminar, flag_pdp, flag_psat, flag_estudo, dt_inicio_previsao_chuva):

    if flag_preliminar:
        pasta_saida = 'preliminar'
        prefixo_preliminar = '.PRELIMINAR'
    else:
        pasta_saida = 'teste'
        prefixo_preliminar = ''
    
    if flag_pdp:
        prefixo_pdp = '.PDP'
    else:
        prefixo_pdp = ''
        
    if flag_estudo:
        prefixo_estudo = '.estudo'
    else:
        prefixo_estudo = ''
        
    path_saida_smap = os.path.join(DROPBOX_MIDDLE_PATH, 'Chuva-vazão', 
                                   data.strftime('%Y%m%d'), pasta_saida, 
                                   'smap_nat_{}{}{}{}_r{:0>2}z.txt'.format(
                                       modelo, prefixo_preliminar, 
                                       prefixo_estudo, prefixo_pdp, rodada))

    if not os.path.exists(path_saida_smap):
        raise Exception('Nenhum arquivo encontrado com o padrao de nome:\n%s' %(path_saida_smap))

    print(path_saida_smap)
    
    database = wx_dbClass.db_mysql_master('db_rodadas')
    database.connect()
    
    tb_cadastro_rodadas = database.getSchema('tb_cadastro_rodadas')
    tb_smap = database.getSchema('tb_smap')
        
    info_rodada = get_info_rodadas(data, rodada, modelo, flag_preliminar, 
                                   flag_pdp, flag_psat, flag_estudo)
    
    if info_rodada['id'] == None:
        ins_query = tb_cadastro_rodadas.insert().values(
                                        str_modelo=modelo,
                                        dt_rodada=data,
                                        hr_rodada=rodada,
                                        id_chuva=info_rodada['id_chuva'],
                                        fl_preliminar=flag_preliminar,
                                        fl_pdp=flag_pdp,
                                        fl_psat=flag_psat,
                                        fl_estudo=flag_estudo)
        
        database.conn.execute(ins_query)
        info_rodada = get_info_rodadas(data, rodada, modelo, flag_preliminar, 
                                       flag_pdp, flag_psat, flag_estudo)

    if info_rodada['id_smap'] == None:
        
        sel_query = select(func.max(tb_cadastro_rodadas.columns.id_smap))
        [ultimo_id_smap] = database.conn.execute(sel_query).fetchone()
        
        if ultimo_id_smap == None:
            info_rodada['id_smap'] = 1
        else:
            info_rodada['id_smap'] = ultimo_id_smap + 1
            
        upd_query = tb_cadastro_rodadas.update().where(
            tb_cadastro_rodadas.c.id==info_rodada['id']
            ).values({
                'id_smap': info_rodada['id_smap'],
                'fl_preliminar': flag_preliminar,
                'fl_pdp': flag_pdp,
                'fl_psat': flag_psat,
                'fl_estudo': flag_estudo,
            })
        database.conn.execute(upd_query)
            
    else:
        del_query = tb_smap.delete().where(
            tb_smap.c.id==info_rodada['id_smap']
            )
        database.conn.execute(del_query)

       
    vals = []
    dt_inicial = dt_inicio_previsao_chuva
    
    saida_smap = pd.read_csv(path_saida_smap, sep=' ', skipinitialspace=True,
                             header=None, index_col=0)
    
    num_dias_previsao = saida_smap.shape[1]
    dias_previstos = [dt_inicial + datetime.timedelta(days=x) 
                      for x in range(num_dias_previsao)]
    
    saida_smap.columns = dias_previstos
    
    for num_posto, previsao_vazao in saida_smap.iterrows():
        for dt_prevista in dias_previstos:
            vals.append({
                'id':info_rodada['id_smap'],
                'cd_posto':num_posto, 
                'dt_prevista':dt_prevista,
                'vl_vazao':previsao_vazao[dt_prevista]
                })
    
    database.conn.execute(tb_smap.insert(), vals)
    print("Arquivo importado com sucesso!")
    
    
def importar_prevs(data, rodada, modelo, dt_rv, flag_preliminar, flag_pdp, flag_psat, flag_estudo):

    dt_rv = wx_opweek.ElecData(dt_rv) 
    
    if flag_preliminar:
        pasta_saida = 'preliminar'
        prefixo_preliminar = '.PRELIMINAR'
    else:
        pasta_saida = 'teste'
        prefixo_preliminar = ''
    
    if flag_pdp:
        prefixo_pdp = '.PDP'
    else:
        prefixo_pdp = ''
        
    if flag_estudo:
        prefixo_estudo = '-{:2>0}{:2>0}.estudo'.format(dt_rv.anoReferente, dt_rv.mesReferente)
    else:
        prefixo_estudo = ''
        
        
    path_arquivo_prevs = os.path.join(DROPBOX_MIDDLE_PATH, 'Chuva-vazão', 
                                      data.strftime('%Y%m%d'), pasta_saida, 
                                      'PREVS-{}{}{}{}_r{:0>2}z.RV{}'.format(
                                          modelo, prefixo_preliminar,
                                          prefixo_estudo, prefixo_pdp, 
                                          rodada, dt_rv.atualRevisao)
                                      )

    if not os.path.exists(path_arquivo_prevs):
        raise Exception('Nenhum arquivo encontrado com o padrao de nome:\n%s' %(path_arquivo_prevs))

    print(path_arquivo_prevs)

    database = wx_dbClass.db_mysql_master('db_rodadas')
    database.connect()
    
    tb_cadastro_rodadas = database.getSchema('tb_cadastro_rodadas')
    tb_prevs = database.getSchema('tb_prevs')
    
    info_rodada = get_info_rodadas(data, rodada, modelo, flag_preliminar, 
                                   flag_pdp, flag_psat, flag_estudo, dt_rv.data)
    
    if info_rodada['id'] == None:
        ins_query = tb_cadastro_rodadas.insert().values(
                                        str_modelo=modelo,
                                        dt_rodada=data,
                                        hr_rodada=rodada,
                                        dt_revisao = dt_rv.data,
                                        id_chuva=info_rodada['id_chuva'],
                                        id_smap=info_rodada['id_smap'],
                                        fl_preliminar=flag_preliminar,
                                        fl_pdp=flag_pdp,
                                        fl_psat=flag_psat,
                                        fl_estudo=flag_estudo)
        
        database.conn.execute(ins_query)
        info_rodada = get_info_rodadas(data, rodada, modelo, flag_preliminar, 
                                       flag_pdp, flag_psat, flag_estudo, dt_rv.data)

    if info_rodada['id_previvaz'] == None:
        
        sel_query = select(func.max(tb_cadastro_rodadas.columns.id_previvaz))
        [ultimo_id_previvaz] = database.conn.execute(sel_query).fetchone()
        
        if ultimo_id_previvaz == None:
            info_rodada['id_previvaz'] = 1
        else:
            info_rodada['id_previvaz'] = ultimo_id_previvaz + 1
            
        upd_query = tb_cadastro_rodadas.update().where(
            tb_cadastro_rodadas.c.id==info_rodada['id']
            ).values({
                'id_previvaz': info_rodada['id_previvaz'],
                'fl_preliminar': flag_preliminar,
                'fl_pdp': flag_pdp,
                'fl_psat': flag_psat,
                'fl_estudo': flag_estudo,
                'dt_revisao': dt_rv.data,
                })
        database.conn.execute(upd_query)
            
    else:
        del_query = tb_prevs.delete().where(
            tb_prevs.c.id==info_rodada['id_previvaz']
            )
        database.conn.execute(del_query)
        
    vals = []
    
    prevs = pd.read_csv(path_arquivo_prevs, sep=' ', skipinitialspace=True,
                        header=None)
    
    prevs = prevs.drop(0, axis=1).set_index(1)
    
    num_semanas = prevs.shape[1]
    
    semanas = [dt_rv.primeiroDiaMes + datetime.timedelta(days=sem*7) 
               for sem in range(num_semanas)]
    
    prevs.columns = semanas
    
    for num_posto, previsao_vazao in prevs.iterrows():
        for dt_inicio_semana in semanas:
            vals.append({
                'id':info_rodada['id_previvaz'], 
                'cd_posto':num_posto, 
                'dt_prevista':dt_inicio_semana, 
                'vl_vazao':previsao_vazao[dt_inicio_semana]
                })
    
    database.conn.execute(tb_prevs.insert(), vals)
    print("Arquivo importado com sucesso!")
    
    
if __name__ == '__main__':
    data = datetime.datetime(2022,11,16)
    rodada = 0
    modelo = 'PCONJUNTO'
    rv = 3
    flag_preliminar = False
    flag_pdp = False
    flag_psat = True
    flag_estudo = False
    dt_inicio_mes_eletrico = datetime.datetime(2022,10,29)
    dt_inicio_previsao_chuva = datetime.datetime(2022,11,17)
    importar_chuva(data, rodada, modelo)
    importar_smap(data, rodada, modelo, flag_preliminar, flag_pdp, flag_psat, flag_estudo, dt_inicio_previsao_chuva)
    importar_prevs(data, rodada, modelo, rv, flag_preliminar, flag_pdp, dt_inicio_mes_eletrico)
    