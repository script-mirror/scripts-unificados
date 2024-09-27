# -*- coding: utf-8 -*-
import sys
from flask_wtf import FlaskForm
from wtforms import StringField, PasswordField, BooleanField
from wtforms.validators import InputRequired, Email, Length
from flask_login import LoginManager, UserMixin

sys.path.insert(1,"/WX2TB/Documentos/fontes")
from PMO.scripts_unificados.apps.web_modelos.server.server import app, db

loginManager = LoginManager()
loginManager.init_app(app)
loginManager.login_view = 'middle_app.login'

class User(UserMixin, db.Model):
	id = db.Column(db.Integer, primary_key=True)
	username = db.Column(db.String(15), unique=True)
	email = db.Column(db.String(50), unique=True)
	password = db.Column(db.String(80))

@loginManager.user_loader
def load_user(user_id):
	return User.query.get(int(user_id))

class LoginForm(FlaskForm):
	username = StringField('Username', validators=[InputRequired(), Length(min=4, max=15)])
	password = PasswordField('Password', validators=[InputRequired(), Length(min=8, max=80)])
	remember = BooleanField('Remember me')

class RegistrationForm(FlaskForm):
	email = StringField('Email', validators=[InputRequired(), Email(message='Invalid email'), Length(max=50)])
	username = StringField('Username', validators=[InputRequired(), Length(min=4, max=15)])
	password = PasswordField('Password', validators=[InputRequired(), Length(min=8, max=80)])

