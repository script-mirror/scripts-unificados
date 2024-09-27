# -*- coding: utf-8 -*-
import os
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.abspath(os.path.expanduser("~")),'.env'))

DEBUG =True

SECRET_KEY= os.getenv('FLASK_SECRET_KEY')

basedir = os.path.abspath(os.path.dirname(__file__))
SQLALCHEMY_DATABASE_URI = 'sqlite:///' + os.path.join(basedir, 'wx_users.db')

SQLALCHEMY_TRACK_MODIFICATIONS = False