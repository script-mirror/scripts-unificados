# -*- coding: utf-8 -*-
import os
import sys


from flask import Flask, Blueprint
from flask_bootstrap import Bootstrap
from flask_caching import Cache

from flask_sqlalchemy import SQLAlchemy

import logging
from logging.handlers import RotatingFileHandler

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler = RotatingFileHandler('web_modelos.log')
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)


app = Flask ( __name__ )
app.config.from_pyfile('config.py')

app.logger.addHandler(handler)

@app.before_request
def log_request():
  app.logger.info(f'{{"url":"{request.url}, "metodo":"{request.method}", "ip":"{request.remote_addr}"}}')

Bootstrap(app)

db = SQLAlchemy(app)


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

sys.path.insert(1,"/WX2TB/Documentos/fontes/")
from PMO.scripts_unificados.apps.web_modelos.server.views import *


if __name__ == '__main__':
  
  app.register_blueprint(bp, url_prefix='/middle')

  if 'localhost' in sys.argv:
    app.run( port=5000, host='192.168.1.21')

  elif 'home' in sys.argv:
    app.run(port=5000, host='127.0.0.1')
  else:
    app.run(debug=True, port=8090, host='172.31.13.224')
    # app.run(debug=True, port=5000, host='172.31.13.224')


