import os
import pdb
import sqlalchemy as sa

home_path =  os.path.expanduser("~")

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")),'.env'))
        

class DbMiddle:
    
    def __init__(self, dbase, connect=False):
        __HOST_MYSQL = os.getenv('HOST_MYSQL')
        __PORT_DB_MYSQL = os.getenv('PORT_DB_MYSQL')

        __USER_DB_MYSQL = os.getenv('USER_DB_MYSQL')
        __PASSWORD_DB_MYSQL = os.getenv('PASSWORD_DB_MYSQL')

        self.meta = sa.MetaData()
        self.user = __USER_DB_MYSQL
        self.password = __PASSWORD_DB_MYSQL
        self.host = __HOST_MYSQL
        self.port = __PORT_DB_MYSQL
        self.dbase = dbase
        self.engine= sa.create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbase}") if connect else None


    def connect(self):
        self.engine = sa.create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbase}") if not self.engine else self.engine
        
        self.conn = self.engine.connect()

    def db_execute(self,query):

        self.connect()
        result = self.conn.execute(query)
        
        self.conn.close()
        self.conn = None
        return result 
    
    def db_dispose(self):
        self.engine.dispose()
        self.engine = None

    def create_table(self, table_name):
        self.get_schema(table_name)
        self.meta.create_all(self.engine)
        
    def get_schema(self, table_name:str) -> sa.Table:
        
        if table_name.lower() == 'tb_subbacia':
            table_schema = sa.Table('tb_subbacia', self.meta,
                sa.Column('cd_subbacia', sa.Integer),
                sa.Column('txt_nome_subbacia', sa.String(100)),
                sa.Column('txt_submercado', sa.String(20)),
                sa.Column('txt_bacia', sa.String(100)),
                sa.Column('vl_lat', sa.Float),
                sa.Column('vl_lon', sa.Float),
                sa.Column('txt_nome_smap', sa.String(150)),
                sa.Column('txt_pasta_contorno', sa.String(150)),
                sa.Column('cd_bacia_mlt', sa.Integer)
            )
        elif table_name.lower() == 'tb_cadastro_rodadas':
            table_schema = sa.Table('tb_cadastro_rodadas', self.meta,
                sa.Column('id', sa.INTEGER, primary_key=True),
                sa.Column('id_chuva', sa.INTEGER),
                sa.Column('id_smap', sa.INTEGER),
                sa.Column('id_previvaz', sa.INTEGER),
                sa.Column('id_prospec', sa.INTEGER),
                sa.Column('dt_rodada', sa.Date, index=True),
                sa.Column('hr_rodada', sa.SmallInteger),
                sa.Column('str_modelo', sa.String(250)),
                sa.Column('fl_preliminar', sa.Boolean),
                sa.Column('fl_pdp', sa.Boolean),
                sa.Column('fl_psat', sa.Boolean),
                sa.Column('fl_estudo', sa.Boolean),
                sa.Column('dt_revisao', sa.Date),
            )
        elif table_name.lower() == 'tb_bacias':
            
            table_schema = sa.Table('tb_bacias', self.meta,
                sa.Column('id_bacia', sa.INTEGER, primary_key=True),
                # sa.Column('id_submercado', sa.INTEGER),
                sa.Column('str_bacia', sa.VARCHAR(100)),
            )
            
        elif table_name.lower() == 'tb_submercado':
            table_schema = sa.Table('tb_submercado', self.meta,
                sa.Column('cd_submercado', sa.Integer, primary_key=True),
                sa.Column('str_submercado', sa.VARCHAR(100)),
                sa.Column('str_sigla', sa.VARCHAR(100))
            )
            
        elif table_name.lower() == 'tb_chuva':
            table_schema = sa.Table('tb_chuva', self.meta,
                sa.Column('id', sa.Integer),
                sa.Column('cd_subbacia', sa.Integer),
                sa.Column('dt_prevista', sa.Date),
                sa.Column('vl_chuva', sa.Float),
            )
            
        return table_schema