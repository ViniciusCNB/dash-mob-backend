from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import date


def get_todas_as_empresas(db: Session):
    """ Busca todas as empresas para popular filtros. """
    query = text("""
        SELECT id_empresa, nome_empresa
        FROM dim_empresa
        WHERE codigo_empresa != 0
        ORDER BY nome_empresa;
    """)
    return db.execute(query).all()


def get_ranking_empresas(db: Session, data_inicio: date, data_fim: date):
    """ Retorna os dados comparativos entre todas as empresas. """
    query = text("""
        WITH
        metricas_agregadas AS (
            SELECT
                id_empresa,
                SUM(total_ocorrencias) as total_ocorrencias,
                SUM(total_passageiros) as total_passageiros,
                SUM(total_viagens) as total_viagens
            FROM agg_metricas_empresas_diarias
            WHERE data BETWEEN :data_inicio AND :data_fim
            GROUP BY id_empresa
        ),
        contagem_linhas AS (
            SELECT
                agg.id_empresa,
                COUNT(DISTINCT agg.id_linha) as total_linhas
            FROM agg_metricas_linhas_diarias agg
            WHERE agg.data BETWEEN :data_inicio AND :data_fim
            GROUP BY agg.id_empresa
        )
        SELECT
            de.id_empresa,
            de.nome_empresa,
            COALESCE(cl.total_linhas, 0) as total_linhas,
            COALESCE(ma.total_ocorrencias, 0) as total_ocorrencias,
            COALESCE(ma.total_passageiros, 0) as total_passageiros,
            COALESCE((ma.total_ocorrencias * 10000.0) / NULLIF(ma.total_viagens, 0), 0) as taxa_ocorrencias_por_10k_viagens
        FROM dim_empresa de
        LEFT JOIN metricas_agregadas ma ON de.id_empresa = ma.id_empresa
        LEFT JOIN contagem_linhas cl ON de.id_empresa = cl.id_empresa
        WHERE de.codigo_empresa != 0;
    """)
    return db.execute(query, {"data_inicio": data_inicio, "data_fim": data_fim}).all()


def get_dashboard_empresa(db: Session, id_empresa_req: int, data_inicio: date, data_fim: date):
    """ Busca todos os dados para o dashboard de uma empresa específica. """
    query = text("""
    WITH
    metricas_base AS (
        SELECT
            SUM(total_passageiros) as total_passageiros,
            SUM(total_ocorrencias) as total_ocorrencias,
            SUM(total_viagens) as total_viagens
        FROM agg_metricas_empresas_diarias
        WHERE id_empresa = :id_empresa AND data BETWEEN :data_inicio AND :data_fim
    ),
    justificativas AS (
        SELECT j.nome_justificativa as category, COUNT(*) as value
        FROM fact_viagens f
        JOIN dim_data d ON f.id_data = d.id_data
        JOIN dim_justificativa j ON f.id_justificativa = j.id_justificativa
        WHERE f.id_empresa = :id_empresa AND d.data_completa BETWEEN :data_inicio AND :data_fim
        GROUP BY j.nome_justificativa ORDER BY value DESC
    ),
    linhas_mais_utilizadas AS (
        SELECT l.id_linha as id, l.cod_linha as codigo, l.nome_linha as nome, SUM(agg.total_passageiros) as valor
        FROM agg_metricas_linhas_diarias agg
        JOIN dim_linha l ON agg.id_linha = l.id_linha
        WHERE agg.id_empresa = :id_empresa AND agg.data BETWEEN :data_inicio AND :data_fim
        GROUP BY l.id_linha, l.cod_linha, l.nome_linha ORDER BY valor DESC LIMIT 5
    ),
    pass_dia_semana AS (
        SELECT d.dia_da_semana as category, SUM(f.passageiros) / COUNT(DISTINCT d.data_completa) as value
        FROM fact_viagens f
        JOIN dim_data d ON f.id_data = d.id_data
        WHERE f.id_empresa = :id_empresa AND d.data_completa BETWEEN :data_inicio AND :data_fim
        GROUP BY d.dia_da_semana, EXTRACT(ISODOW FROM d.data_completa)
        ORDER BY EXTRACT(ISODOW FROM d.data_completa)
    ),
    evolucao_passageiros AS (
        SELECT date_trunc('year', data)::date as category, SUM(total_passageiros) as value
        FROM agg_metricas_empresas_diarias
        WHERE id_empresa = :id_empresa AND data BETWEEN :data_inicio AND :data_fim
        GROUP BY category ORDER BY category
    )
    SELECT
        (SELECT nome_empresa FROM dim_empresa WHERE id_empresa = :id_empresa) as nome_empresa,
        (SELECT codigo_empresa FROM dim_empresa WHERE id_empresa = :id_empresa) as codigo_empresa,
        (SELECT total_passageiros FROM metricas_base) as total_passageiros,
        (SELECT total_ocorrencias FROM metricas_base) as total_ocorrencias,
        (SELECT total_viagens FROM metricas_base) as total_viagens,
        (SELECT COUNT(DISTINCT id_linha) FROM agg_metricas_linhas_diarias WHERE id_empresa = :id_empresa AND data BETWEEN :data_inicio AND :data_fim) as total_linhas,
        (SELECT total_passageiros FROM metricas_base) / NULLIF((SELECT COUNT(DISTINCT date_trunc('month', data)) FROM agg_metricas_empresas_diarias WHERE id_empresa = :id_empresa AND data BETWEEN :data_inicio AND :data_fim), 0) as media_pass_mes,
        (SELECT total_passageiros FROM metricas_base) / NULLIF((SELECT COUNT(DISTINCT data) FROM agg_metricas_empresas_diarias WHERE id_empresa = :id_empresa AND data BETWEEN :data_inicio AND :data_fim), 0) as media_pass_dia,
        (SELECT total_passageiros FROM metricas_base) / NULLIF((SELECT total_viagens FROM metricas_base), 0) as media_pass_viagem,
        (SELECT json_agg(j) FROM justificativas j) as grafico_justificativas,
        (SELECT json_agg(lmu) FROM linhas_mais_utilizadas lmu) as grafico_linhas_mais_utilizadas,
        (SELECT json_agg(pds) FROM pass_dia_semana pds) as grafico_media_passageiros_dia_semana,
        (SELECT json_agg(ep) FROM evolucao_passageiros ep) as grafico_evolucao_passageiros_ano;
    """)
    return db.execute(query, {"id_empresa": id_empresa_req, "data_inicio": data_inicio, "data_fim": data_fim}).fetchone()
