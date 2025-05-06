from extensions import db


class ViagensPorLinha(db.Model):
    __tablename__ = "viagens_por_linha"
    id = db.Column(db.Integer, primary_key=True)
    linha = db.Column(db.String())
    quantidade_viagens = db.Column(db.BigInteger())
