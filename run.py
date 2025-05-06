import os

from flask import Flask
from flask_cors import CORS
from extensions import db

from models import ViagensPorLinha

app = Flask(__name__)

app.config["SQLALCHEMY_DATABASE_URI"] = (
    "postgresql://{}:{}@{}:5432/{}".format(
        os.getenv("DB_USERNAME"),
        os.getenv("DB_PASSWORD"),
        os.getenv("DB_HOST"),
        os.getenv("DB_SCHEMA"),
    )
)

app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db.init_app(app)
CORS(app, resources={r"/*": {"origins": "*"}})


@app.route("/")
def hello():
    return "Hello, World!"


@app.route("/viagens-por-linha")
def viagens_por_linha():
    viagens = ViagensPorLinha.query.all()
    return [
        {"linha": viagem.linha, "quantidade_viagens": viagem.quantidade_viagens}
        for viagem in viagens
    ]


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=80)
