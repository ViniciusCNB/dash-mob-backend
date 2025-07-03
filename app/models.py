from sqlalchemy import Table, Column, Integer, MetaData, TEXT, NUMERIC

# Container para as definições das tabelas
metadata = MetaData()

dim_linha = Table(
    "dim_linha",
    metadata,
    Column("id_linha", Integer, primary_key=True),
    Column("cod_linha", TEXT, nullable=False, unique=True),
    Column("nome_linha", TEXT),
    Column("origem", TEXT),
    Column("extensao_km", NUMERIC(10, 2)),
)
