# -*- coding: utf-8 -*-
import os
import sys


from flask import Flask, Blueprint
from flask_bootstrap import Bootstrap
from flask_caching import Cache

from flask_sqlalchemy import SQLAlchemy

app = Flask ( __name__ )
app.config.from_pyfile('config.py')

Bootstrap(app)

db_login = SQLAlchemy(app)


PATH_SERVER = os.path.dirname(os.path.abspath(__file__))
PATH_CACHE = os.path.join(PATH_SERVER,"caches")

cache = Cache(app, config={

  'CACHE_TYPE': 'FileSystemCache',
  'CACHE_DIR': os.path.join(PATH_CACHE,'tmp')

  # 'CACHE_TYPE': 'RedisCache',
  # 'CACHE_REDIS_HOST':'35.173.154.94',
  # 'CACHE_REDIS_PORT':6379,
  # 'CACHE_DEFAULT_TIMEOUT': 60*60*24*7,
  # 'CACHE_REDIS_PASSWORD': "rzRedisServer"
}) 

 
cache.init_app(app)

bp = Blueprint('middle_app', __name__,
                        template_folder='templates')

import os
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")), ".env"))
PATH_PROJETO = os.getenv("PATH_PROJETO", "/WX2TB/Documentos/fontes/PMO")
sys.path.insert(1,f"{PATH_PROJETO}/scripts_unificados")

from apps.web_modelos.server.views import *


if __name__ == '__main__':
  
  app.register_blueprint(bp, url_prefix='/middle')

  if 'localhost' in sys.argv:
    app.run( port=5000, host='192.168.1.21')

  elif 'home' in sys.argv:
    app.run(port=5000, host='127.0.0.1')
  else:
    app.run(debug=True, port=8090, host='172.31.13.224')
    # app.run(debug=True, port=5000, host='172.31.13.224')


