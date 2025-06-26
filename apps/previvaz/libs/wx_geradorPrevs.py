import os
import sys
import shutil
import datetime
import pandas as pd
import sqlalchemy as db
import pdb
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")), ".env"))
PATH_PROJETO = os.getenv("PATH_PROJETO", "/WX2TB/Documentos/fontes/PMO")
sys.path.insert(1,f"{PATH_PROJETO}/scripts_unificados")

path_libs = os.path.dirname(os.path.abspath(__file__))   # ou os.path.realpath(__file__)
path_app = os.path.dirname(path_libs)


from bibliotecas import wx_dbLib,wx_opweek, wx_dbClass
from apps.previvaz.libs import wx_formatacao_prevs



def getVazoesDat():
    vazoes_file = os.path.join(path_app, 'arquivos', 'vazoes.txt')
    return pd.read_csv(vazoes_file, sep=' ', skipinitialspace=True, names=['posto','ano',1,2,3,4,5,6,7,8,9,10,11,12])

def getAcomph(data_inicial, data_final=None):

    db_ons = wx_dbClass.db_mysql_master('db_ons',connect=True)
    tb_acomph = db_ons.getSchema('acomph_consolidado')

    cte = (
    db.select(
        tb_acomph.c.cd_posto,
        tb_acomph.c.dt_referente,
        tb_acomph.c.vl_vaz_inc_conso,
        tb_acomph.c.vl_vaz_nat_conso,
        tb_acomph.c.dt_acomph,
        db.func.row_number().over(
            partition_by=[tb_acomph.c.cd_posto, tb_acomph.c.dt_referente],
            order_by=db.desc(tb_acomph.c.dt_acomph)
        ).label('row_num')
    )
    .where(tb_acomph.c.dt_referente.between(data_inicial, data_final) if data_final != None else tb_acomph.c.dt_referente >= data_inicial)
    .cte('cte_groups')
    )
    query = (
        db.select(cte)
        .where(cte.c.row_num == 1)
        .order_by(cte.c.cd_posto, cte.c.dt_referente)
    )

    answer = db_ons.db_execute(query).fetchall() 
    db_ons.db_dispose()
    return answer

def gerarador_prevs(ano_desejado, ano_referencia):
    global path_libs
    ultimo_mes = 12
    if datetime.date(ano_referencia, 12, 1) > datetime.date.today():
        ultimo_mes = datetime.date.today().month - 1
    primeiro_mes_prevs = datetime.datetime(ano_desejado, 1, 1,)
    ultimo_mes_prevs = datetime.datetime(ano_desejado, ultimo_mes, 1,)

    dt_inicio_prevs = wx_opweek.getLastSaturday(primeiro_mes_prevs)
    dt_fim_prevs = wx_opweek.getLastSaturday(ultimo_mes_prevs) + datetime.timedelta(days=7*6-1)

    datas = pd.date_range(start=dt_inicio_prevs, end=dt_fim_prevs)
    df = pd.DataFrame(index=datas)

    # de_para = pd.Series(dict(zip(datas, datas.month.tolist())))
    de_para = pd.Series(dict(zip(datas, datas.strftime('%Y%m').tolist())))

    path_saida = os.path.join(path_libs, 'saida')

    df_vazoes = getVazoesDat()
    df_vazoes = df_vazoes.set_index(['posto','ano'])

    for posto in wx_formatacao_prevs.ordemPostos:

        df[posto] = de_para
        if posto in [166, 171]:
            df[posto][:] = 1
            df[posto] = df[posto].astype(int)
            continue

        vazoes_ano_menos_um = df_vazoes.loc[posto,ano_referencia-1]
        vazoes_ano_menos_um.index = [f'{ano_desejado-1}{x:0>2}' for x in range(1,13)]
        dicionario_vazoes = vazoes_ano_menos_um.to_dict()

        vazoes_ano_ref = df_vazoes.loc[posto,ano_referencia]
        vazoes_ano_ref.index = [f'{ano_desejado}{x:0>2}' for x in range(1,13)]
        dicionario_vazoes.update(vazoes_ano_ref.to_dict())
        if ultimo_mes == 12:
            vazoes_ano_mais_um = df_vazoes.loc[posto,ano_referencia+1]
            vazoes_ano_mais_um.index = [f'{ano_desejado+1}{x:0>2}' for x in range(1,13)]
            dicionario_vazoes.update(vazoes_ano_mais_um.to_dict())

        df[posto] = df[posto].replace(dicionario_vazoes)

    df_acomph = pd.DataFrame()
    if ano_referencia >= 2014:
        diff_inicio = ano_desejado - dt_inicio_prevs.year
        diff_final = dt_fim_prevs.year - ano_desejado

        dt_inicio_acomph = dt_inicio_prevs.replace(year=ano_referencia-diff_inicio)
        dt_fim_acomph = dt_fim_prevs.replace(year=ano_referencia+diff_final)

        answer = getAcomph(dt_inicio_acomph, dt_fim_acomph)
        
        df_answer = pd.DataFrame(answer, columns=['CD_POSTO','DT_REFERENTE','VL_VAZ_INC_CONSO','VL_VAZ_NAT_CONSO','DT_ACOMPH','ROW_NUMBER'])
        df_acomph = df_answer.pivot_table(columns='CD_POSTO',index='DT_REFERENTE',values='VL_VAZ_NAT_CONSO')

        dicionario_anos = {ano_referencia-1: ano_desejado-1, ano_referencia: ano_desejado, ano_referencia+1: ano_desejado+1}
        # df_acomph.index = pd.to_datetime({'year': df_acomph.index.year.map(dicionario_anos), 'month': df_acomph.index.month, 'day':df_acomph.index.day})
        df_acomph.index = df_acomph.index.to_pydatetime()
    # Atualizando as vazoes com os valores do acomph, caso exista no banco
    if df_acomph.shape[0] > 0:
        df.update(df_acomph)

    # Tirando as vazoes com valores igual a 0
    df = df.replace({0:1})
    df = df.resample('7D').mean()
    df = df.T

    dt_ref = dt_inicio_prevs

    data_aux = dt_fim_prevs - datetime.timedelta(days=7)
    while dt_ref < data_aux:
        if wx_opweek.ElecData(dt_ref.date()).anoReferente != ano_desejado:
            dt_ref +=  datetime.timedelta(days=7)
            continue

        dt_ref_date = dt_ref.date()
        dt_ref_elec = wx_opweek.ElecData(dt_ref_date)
        
        if dt_ref_elec.atualRevisao != 0 :
            
            path_saida_rv = os.path.join(path_saida, '{}{:0>2}'.format(dt_ref_elec.anoReferente, dt_ref_elec.mesReferente))
            rv_anterior = dt_ref_elec.atualRevisao - 1
            nome_arquivo_rv_anterior = 'prevs.rv{}'.format(rv_anterior)
            
            nome_arquivo = 'prevs.rv{}'.format(dt_ref_elec.atualRevisao)
            path_prevs_saida = os.path.join(path_saida_rv, nome_arquivo)
            path_prevs_saida_anterior = os.path.join(path_saida_rv, nome_arquivo_rv_anterior)
            
            shutil.copyfile(path_prevs_saida_anterior, path_prevs_saida)
        
        else:
            nome_arquivo = 'prevs.rv{}'.format(dt_ref_elec.atualRevisao)
            
            path_saida_rv = os.path.join(path_saida, '{}{:0>2}'.format(dt_ref_elec.anoReferente, dt_ref_elec.mesReferente))
            if not os.path.exists(path_saida_rv):
                os.makedirs(path_saida_rv)

            semanas = []
            for num_semanas in range(6):
                semanas.append(dt_ref+datetime.timedelta(days=7*num_semanas))
            # try:
            path_prevs_saida = os.path.join(path_saida_rv, nome_arquivo)    
            wx_formatacao_prevs.write_prevs(path_prevs_saida, df[semanas])
            # except:
                # pdb.set_trace()
        print(path_prevs_saida)

        dt_ref +=  datetime.timedelta(days=7)
    
    return path_saida

if __name__ == '__main__':
    
    # Mexa apenas no ano e no mes, deixe o dia com o dia 1
    ano_desejado = 2025
    anos_referencia = [a for a in range(2024,2025)]
    #anos_referencia = [2019]

    pastas_compress = []
    for ano in anos_referencia:

        path_saida = gerarador_prevs(ano_desejado, ano)

        pasta_dst_raiz = os.path.join(os.path.dirname(path_saida), f'cenarios_para_{ano_desejado}')
        if not os.path.exists(pasta_dst_raiz):
            os.makedirs(pasta_dst_raiz)

        pasta_dst = os.path.join(pasta_dst_raiz, f'{ano}')
        if os.path.exists(pasta_dst):
            shutil.rmtree(pasta_dst)

        os.rename(path_saida, pasta_dst)

    shutil.make_archive(pasta_dst_raiz, 'zip', pasta_dst_raiz)
    shutil.rmtree(pasta_dst_raiz)
    print(pasta_dst_raiz+'.zip')