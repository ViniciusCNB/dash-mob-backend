from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import date


def get_todas_as_concessionarias(db: Session):
    """ Busca todas as concessionárias para popular filtros. """
    query = text("""
        SELECT id_concessionaria, codigo_concessionaria, nome_concessionaria
        FROM dim_concessionaria
        WHERE codigo_concessionaria != 0
        ORDER BY nome_concessionaria;
    """)
    return db.execute(query).all()


def get_ranking_concessionarias(db: Session, data_inicio: date, data_fim: date):
    """ Retorna os dados comparativos entre todas as concessionárias. """
    query = text("""
        WITH
        metricas_agregadas AS (
            SELECT
                id_concessionaria,
                SUM(total_ocorrencias) as total_ocorrencias,
                SUM(total_passageiros) as total_passageiros,
                SUM(total_viagens) as total_viagens
            FROM agg_metricas_concessionarias_diarias
            WHERE data BETWEEN :data_inicio AND :data_fim
            GROUP BY id_concessionaria
        ),
        contagem_linhas AS (
            SELECT
                agg.id_concessionaria,
                COUNT(DISTINCT agg.id_linha) as total_linhas
            FROM agg_metricas_linhas_diarias agg
            WHERE agg.data BETWEEN :data_inicio AND :data_fim
            GROUP BY agg.id_concessionaria
        )
        SELECT
            dc.id_concessionaria,
            dc.codigo_concessionaria,
            dc.nome_concessionaria,
            COALESCE(cl.total_linhas, 0) as total_linhas,
            COALESCE(ma.total_ocorrencias, 0) as total_ocorrencias,
            COALESCE(ma.total_passageiros, 0) as total_passageiros,
            COALESCE((ma.total_ocorrencias * 10000.0) / NULLIF(ma.total_viagens, 0), 0) as taxa_ocorrencias_por_10k_viagens
        FROM dim_concessionaria dc
        LEFT JOIN metricas_agregadas ma ON dc.id_concessionaria = ma.id_concessionaria
        LEFT JOIN contagem_linhas cl ON dc.id_concessionaria = cl.id_concessionaria
        WHERE dc.codigo_concessionaria != 0;
    """)
    return db.execute(query, {"data_inicio": data_inicio, "data_fim": data_fim}).all()


def get_dashboard_concessionaria(db: Session, id_concessionaria_req: int, data_inicio: date, data_fim: date):
    """ Busca todos os dados para o dashboard de uma concessionária específica. """
    query = text("""
    WITH
    metricas_base AS (
        SELECT
            SUM(total_passageiros) as total_passageiros,
            SUM(total_ocorrencias) as total_ocorrencias
        FROM agg_metricas_concessionarias_diarias
        WHERE id_concessionaria = :id_concessionaria AND data BETWEEN :data_inicio AND :data_fim
    ),
    linhas_mais_utilizadas AS (
        SELECT l.id_linha as id, l.cod_linha as codigo, l.nome_linha as nome, SUM(agg.total_passageiros) as valor
        FROM agg_metricas_linhas_diarias agg
        JOIN dim_linha l ON agg.id_linha = l.id_linha
        WHERE agg.id_concessionaria = :id_concessionaria AND agg.data BETWEEN :data_inicio AND :data_fim
        GROUP BY l.id_linha, l.cod_linha, l.nome_linha ORDER BY valor DESC LIMIT 5
    ),
    pass_dia_semana AS (
        SELECT d.dia_da_semana as category, SUM(f.passageiros) / COUNT(DISTINCT d.data_completa) as value
        FROM fact_viagens f
        JOIN dim_data d ON f.id_data = d.id_data
        WHERE f.id_concessionaria = :id_concessionaria AND d.data_completa BETWEEN :data_inicio AND :data_fim
        GROUP BY d.dia_da_semana, EXTRACT(ISODOW FROM d.data_completa)
        ORDER BY EXTRACT(ISODOW FROM d.data_completa)
    )
    SELECT
        (SELECT nome_concessionaria FROM dim_concessionaria WHERE id_concessionaria = :id_concessionaria) as nome_concessionaria,
        (SELECT codigo_concessionaria FROM dim_concessionaria WHERE id_concessionaria = :id_concessionaria) as codigo_concessionaria,
        (SELECT total_passageiros FROM metricas_base) as total_passageiros,
        (SELECT total_ocorrencias FROM metricas_base) as total_ocorrencias,
        (SELECT json_agg(lmu) FROM linhas_mais_utilizadas lmu) as grafico_linhas_mais_utilizadas,
        (SELECT json_agg(pds) FROM pass_dia_semana pds) as grafico_media_passageiros_dia_semana;
    """)
    return db.execute(query, {"id_concessionaria": id_concessionaria_req, "data_inicio": data_inicio, "data_fim": data_fim}).fetchone()
