# -*- coding: utf-8 -*-
import sys
from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, BooleanField
from wtforms.validators import InputRequired, Email, Length
from flask_login import LoginManager, UserMixin

sys.path.insert(1,"/WX2TB/Documentos/fontes/PMO/scripts_unificados/")
from apps.web_modelos.server.server import app, db_login

loginManager = LoginManager()
loginManager.init_app(app)
loginManager.login_view = 'middle_app.login'

class User(UserMixin, db_login.Model):
	id = db_login.Column(db_login.Integer, primary_key=True)
	username = db_login.Column(db_login.String(15), unique=True)
	email = db_login.Column(db_login.String(50), unique=True)
	password = db_login.Column(db_login.String(80))

@loginManager.user_loader
def load_user(user_id):
	return User.query.get(int(user_id))

class LoginForm(FlaskForm):
	username = StringField('Usuario', validators=[InputRequired(), Length(min=4, max=15)])
	password = PasswordField('Senha', validators=[InputRequired(), Length(min=8, max=80)])
	remember = BooleanField('Lembrar-me')

class RegistrationForm(FlaskForm):
	email = StringField('Email', validators=[InputRequired(), Email(message='Email inválido'), Length(max=50)])
	username = StringField('Usuario', validators=[InputRequired(), Length(min=4, max=15)])
	password = PasswordField('Senha', validators=[InputRequired(), Length(min=8, max=80)])

