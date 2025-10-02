# -*- coding: utf-8 -*-
import os
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")),'.env'))

DEBUG = True

SECRET_KEY = os.getenv('FLASK_SECRET_KEY')
SESSION_COOKIE_NAME = 'session_webmodelos'

# MySQL Database Configuration (same as wx_dbClass)
HOST_MYSQL = os.getenv('HOST_MYSQL')
PORT_DB_MYSQL = os.getenv('PORT_DB_MYSQL')
USER_DB_MYSQL = os.getenv('USER_DB_MYSQL')
PASSWORD_DB_MYSQL = os.getenv('PASSWORD_DB_MYSQL')

# Database URI for webmodelos database
SQLALCHEMY_DATABASE_URI = f"mysql+pymysql://{USER_DB_MYSQL}:{PASSWORD_DB_MYSQL}@{HOST_MYSQL}:{PORT_DB_MYSQL}/webmodelos"

SQLALCHEMY_TRACK_MODIFICATIONS = False