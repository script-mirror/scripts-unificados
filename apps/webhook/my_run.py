import os
import pdb
import sys
import json
import requests

from datetime import timedelta
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_bcrypt import Bcrypt
from flask_jwt_extended import JWTManager, create_access_token, jwt_required, get_jwt_identity

sys.path.insert(1,"/WX2TB/Documentos/fontes")
from PMO.scripts_unificados.apps.airflow import airflow_tools


diretorioApp = os.getcwd()


from loguru import logger
logger.remove()  # Remover o handler padrão
log_dir = "logs"  # Diretório para os arquivos de log
os.makedirs(log_dir, exist_ok=True)

# Configurar o formato do log
log_format = "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>"

logger.add(
    os.path.join(log_dir, "{time:YYYY-MM-DD}.log"),
    format=log_format,
    rotation="00:00",  # Rotacionar o arquivo de log à meia-noite
    retention="7 days",  # Manter os arquivos dos últimos 7 dias
    compression="zip"  # Comprimir os arquivos de log antigos
)

pathArquivos = os.path.join(diretorioApp, "arquivos")
pathArquivosTmp = os.path.join(pathArquivos, "tmp")
if not os.path.exists(pathArquivosTmp):
    os.makedirs(pathArquivosTmp)

app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///users.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['JWT_SECRET_KEY'] = 'sua_chave_secreta'  # Troque por uma chave secreta forte

db = SQLAlchemy(app)
bcrypt = Bcrypt(app)
jwt = JWTManager(app)

# Modelo de usuário
class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    password = db.Column(db.String(128), nullable=False)

# Mapeamento entre nomes de produtos e funções correspondentes
PRODUCT_MAPPING = {
    "Precipitação por Satélite - PMO": {"funcao": "psat_file"},
    "Modelo GEFS": {"funcao": "modelo_gefs"},
    "Resultados preliminares não consistidos  (vazões semanais - PMO)": {"funcao": "resultados_nao_consistidos_semanal"},
    "Relatório dos resultados finais consistidos da previsão diária (PDP)": {"funcao": "relatorio_resutados_finais_consistidos"},
    "Níveis de Partida para o DESSEM": {"funcao": "niveis_partida_dessem"},
    "DADVAZ – Arquivo de Previsão de Vazões Diárias (PDP)": {"funcao": "dadvaz_vaz_prev"},
    "Deck e Resultados DECOMP - Valor Esperado": {"funcao": "deck_resultados_decomp"},
    "Resultados finais consistidos (vazões diárias - PDP)": {"funcao": "resultados_finais_consistidos"},
    "Resultados preliminares consistidos (vazões semanais - PMO)": {"funcao": "resultados_preliminares_consistidos"},
    "Arquivos dos modelos de previsão de vazões semanais - PMO": {"funcao": "entrada_saida_previvaz"},
    "Arquivos dos modelos de previsão de vazões diárias - PDP": {"funcao": "arquivos_modelo_pdp"},
    "Acomph": {"funcao": "arquivo_acomph"},
    "RDH": {"funcao": "arquivo_rdh"},
    "Histórico de Precipitação por Satélite - PMO": {"funcao": "historico_preciptacao"},
    "Modelo ETA": {"funcao": "modelo_eta"},
    "Deck Preliminar DECOMP - Valor Esperado": {"funcao": "deck_preliminar_decomp"},
    "Decks de entrada e saída - Modelo DESSEM": {"funcao": "deck_entrada_saida_dessem"},
    "Arquivos de Previsão de Carga para o DESSEM": {"funcao": "previsao_carga_dessem"},
    "Decks de entrada do PrevCargaDESSEM": {"funcao": "prevCarga_dessem"},
    "Previsões de carga mensal e por patamar - NEWAVE": {"funcao": "carga_patamar_nw"},
    "IPDO (Informativo Preliminar Diário da Operação)": {"funcao": "carga_IPDO"},
    "Modelo ECMWF": {"funcao": "modelo_ECMWF"},
    "Dados utilizados na previsão de geração eólica": {"funcao": "dados_geracaoEolica"},
    "Arquivos de Previsão de Carga para o DESSEM - PrevCargaDESSEM": {"funcao": "prevCarga_dessem_saida"},
    "DECKS DA PREVISÃO DE GERAÇÃO EÓLICA SEMANAL WEOL-SM": {"funcao": "deck_prev_eolica_semanal_weol"},
    "Preliminar - Relatório Mensal de Limites de Intercâmbio": {"funcao": "relatorio_limites_intercambio"},
    "Relatório Mensal de Limites de Intercâmbio para o Modelo DECOMP": {"funcao": "relatorio_limites_intercambio"},
    "Notas Técnicas - Medio Prazo": {"funcao": "notas_tecnicas_medio_prazo"},
}




# Rota para registro de usuário
@app.route('/webhook/register', methods=['POST'])
def register():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')

    if User.query.filter_by(username=username).first():
        return jsonify({"msg": "Usuário já existe."}), 400

    hashed_password = bcrypt.generate_password_hash(password).decode('utf-8')
    new_user = User(username=username, password=hashed_password)
    db.session.add(new_user)
    db.session.commit()

    return jsonify({"msg": "Usuário registrado com sucesso."}), 201

# Rota para login
@app.route('/webhook/login', methods=['POST'])
def login():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')

    user = User.query.filter_by(username=username).first()
    if user and bcrypt.check_password_hash(user.password, password):
        access_token = create_access_token(identity={'username': user.username}, expires_delta=timedelta(minutes=15))
        return jsonify(access_token=access_token), 200

    return jsonify({"msg": "Credenciais inválidas."}), 401

# Rota protegida
@app.route('/webhook/protected', methods=['GET'])
@jwt_required()
def protected():
    current_user = get_jwt_identity()
    return jsonify(logged_in_as=current_user), 200

# Cria o banco e a tabela caso nao exista
with app.app_context():
    if not os.path.exists('users.db'):
        db.create_all()

@app.route("/webhook", methods=["GET", "POST", "OPTIONS"])
def webhook():

    logger.info(f'ONS: {request.data}')
    if request.data != "" and request.data != b"":
        try:
            data = eval(request.data)
        except Exception as e:
            print(request.data)
            print(e)
            return jsonify(success=True)

        data['enviar'] = True
        produto = data["nome"]
        product_function = PRODUCT_MAPPING.get(produto, None)

        # Verifica se o nome do produto está no mapeamento e inicia a DAG correspondente se estiver
        if produto in PRODUCT_MAPPING:
            
            dag_id = 'WEBHOOK'
            function_name = product_function["funcao"] if product_function else None
            json_produtos = {"product_details": data, "function_name": function_name}

            response = airflow_tools.trigger_airflow_dag(dag_id=dag_id, json_produtos=json_produtos)

            if response.status_code == 200:
                print(f"DAG {dag_id} iniciada com sucesso para o produto: {produto}")
                print(json_produtos)
            else:
                print(f"Erro ao iniciar a DAG {dag_id} para o produto: {(produto)} - {response.text}")

        else:
            print(f"Produto: '{produto}' recebido, mas não foi encontrado nenhum tratamento para o mesmo!")

    return jsonify(success=True)



def correct_encoding_in_dict(d):
    for k, v in d.items():
        if isinstance(v, str):
            d[k] = fix_double_encoding(v)
        elif isinstance(v, dict):
            correct_encoding_in_dict(v)
        elif isinstance(v, list):
            for item in v:
                if isinstance(item, dict):
                    correct_encoding_in_dict(item)

def fix_double_encoding(text):
    try:
        return text.encode('latin1').decode('utf-8')
    except UnicodeDecodeError:
        return text

@app.route("/webhook/glorian", methods=["GET", "POST"])
@jwt_required()
def webhookGlorian():
    
    logger.info(f'Glorian: {request.data}')
    dados = request.data
    
    url = 'http://ec2-54-89-214-53.compute-1.amazonaws.com:5001/static/glorian/webhook/notification'

    try:
        json_data = json.loads(dados.decode('utf-8'))
        correct_encoding_in_dict(json_data)
    
        new_url = "http://172.31.55.249:5001/glorian/webhook/notification"
        headers = {"Content-Type": "application/json"} 
        response = requests.post(new_url, json=json_data, headers=headers)

        if response.status_code == 200:
            print("Dados enviados com sucesso!")
            logger.info("Dados enviados com sucesso!")
        else:
            print(f"Falha ao enviar dados. Status code: {response.status_code}")
            logger.info(f"Falha ao enviar dados. Status code: {response.status_code}")
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
        logger.info(f"Ocorreu um erro: {e}")
    
    return jsonify({'message': 'Success'}), 200


if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == "home":
            app.run(debug=True,port=5000, host='0.0.0.0')
    else:
        app.run(debug=True, port=5001, host="172.31.13.224", threaded=False)
        # debug = True: se run.py ja estiver rodando e for modificado, significa que nao precisa roda-lo novamente. Processo atualiza automatico

