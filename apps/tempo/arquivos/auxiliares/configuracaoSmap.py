import sys
import pandas as pd
import sqlalchemy as db

sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.bibliotecas import wx_dbClass

def get_smap_plot_structure():

  db_rodadas = wx_dbClass.db_mysql_master('db_rodadas')
  db_rodadas.connect()
  tb_subbacia = db_rodadas.getSchema('tb_subbacia')

  select_values = db.select(
    tb_subbacia.c.txt_nome_subbacia,
    tb_subbacia.c.txt_bacia,
    tb_subbacia.c.txt_nome_smap,
    tb_subbacia.c.vl_lat,
    tb_subbacia.c.vl_lon,
    tb_subbacia.c.txt_pasta_contorno,
    )

  answer_subbacia = db_rodadas.conn.execute(select_values).fetchall()
  df_subbacia = pd.DataFrame(answer_subbacia, columns =  ['subbacia','bacia','nomeSmap','vl_lat','vl_lon','pastaContorno'])

  subbacias_smap = df_subbacia.set_index('subbacia').to_dict('index')

  for chave, valor in subbacias_smap.items():
      coordenadas = {'lat': valor.pop('vl_lat'), 'lon': valor.pop('vl_lon')}
      valor['coordenadas'] = coordenadas

  return subbacias_smap
