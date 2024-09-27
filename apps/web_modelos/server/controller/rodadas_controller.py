# -*- coding: utf-8 -*-
import sys
import pdb

from flask import request
import datetime
from flask_login import login_required

sys.path.insert(1,"/WX2TB/Documentos/fontes")
from PMO.scripts_unificados.apps.web_modelos.server.server import bp
from PMO.scripts_unificados.apps.web_modelos.server import utils 
from PMO.scripts_unificados.apps.web_modelos.server.libs import db_ons
from PMO.scripts_unificados.apps.web_modelos.server.caches import rz_cache

