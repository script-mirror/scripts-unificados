import sys
import pdb
import sqlalchemy as db
import pandas as pd
import locale 

try:
  locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
except:
  locale.setlocale(locale.LC_ALL, '')


sys.path.insert("/WX2TB/Documentos/fontes/PMO/scripts_unificados/")
from bibliotecas.wx_dbClass import db_mysql_master



class tb_produtos_bbce:
  @staticmethod
  def get_produtos_interesse():
    __DB_CONFIG = db_mysql_master('db_config')
    produtos_bbce = __DB_CONFIG.getSchema('tb_produtos_bbce')
    query = db.select(
        produtos_bbce.c['str_produto']
    )
    result = __DB_CONFIG.db_execute(query).all()
    df = pd.DataFrame(result, columns=['palavra'])
    return df.to_dict('records')


if __name__ == '__main__':
    print(tb_produtos_bbce.get_produtos_interesse())
    pass    