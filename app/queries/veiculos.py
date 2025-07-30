from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import date


def get_todos_os_veiculos(db: Session):
    """ Busca todos os veículos para popular filtros. """
    query = text("""
        SELECT id_veiculo, identificador_veiculo
        FROM dim_veiculo
        ORDER BY identificador_veiculo;
    """)
    return db.execute(query).all()


def get_ranking_veiculos(db: Session, metrica: str, data_inicio: date, data_fim: date, limit: int):
    """
    Retorna rankings de veículos por passageiros, ocorrências ou km percorrido.
    """
    if metrica not in ['passageiros', 'ocorrencias', 'km_percorrido']:
        raise ValueError("Métrica inválida.")

    coluna_soma = f"total_{metrica}"
    if metrica == 'km_percorrido':
        coluna_soma = 'total_extensao_km'

    query = text(f"""
        SELECT
            dv.id_veiculo,
            dv.identificador_veiculo,
            de.nome_empresa,
            dv.idade_veiculo_anos,
            SUM(agg.{coluna_soma}) as valor
        FROM agg_metricas_veiculos_diarias agg
        JOIN dim_veiculo dv ON agg.id_veiculo = dv.id_veiculo
        LEFT JOIN mv_empresa_principal_veiculo epv ON dv.id_veiculo = epv.id_veiculo
        LEFT JOIN dim_empresa de ON epv.id_empresa = de.id_empresa
        WHERE agg.data BETWEEN :data_inicio AND :data_fim
        GROUP BY dv.id_veiculo, dv.identificador_veiculo, de.nome_empresa, dv.idade_veiculo_anos
        ORDER BY valor DESC
        LIMIT :limit;
    """)
    return db.execute(query, {"data_inicio": data_inicio, "data_fim": data_fim, "limit": limit}).all()


def get_dashboard_veiculo(db: Session, id_veiculo_req: int, data_inicio: date, data_fim: date):
    """ Busca todos os dados para o dashboard de um veículo específico. """
    query = text("""
    WITH
    metricas_base AS (
        SELECT
            SUM(total_passageiros) as total_passageiros,
            SUM(total_ocorrencias) as total_ocorrencias,
            SUM(total_viagens) as total_viagens,
            SUM(total_extensao_km) as total_extensao_km
        FROM agg_metricas_veiculos_diarias
        WHERE id_veiculo = :id_veiculo AND data BETWEEN :data_inicio AND :data_fim
    ),
    justificativas AS (
        SELECT j.nome_justificativa as category, COUNT(*) as value
        FROM fact_viagens f
        JOIN dim_data d ON f.id_data = d.id_data
        JOIN dim_justificativa j ON f.id_justificativa = j.id_justificativa
        WHERE f.id_veiculo = :id_veiculo AND d.data_completa BETWEEN :data_inicio AND :data_fim
        GROUP BY j.nome_justificativa ORDER BY value DESC
    ),
    linhas_atendidas AS (
        SELECT l.id_linha as id, l.cod_linha as codigo, l.nome_linha as nome, COUNT(f.id_fato_viagem) as valor
        FROM fact_viagens f
        JOIN dim_data d ON f.id_data = d.id_data
        JOIN dim_linha l ON f.id_linha = l.id_linha
        WHERE f.id_veiculo = :id_veiculo AND d.data_completa BETWEEN :data_inicio AND :data_fim
        GROUP BY l.id_linha, l.cod_linha, l.nome_linha ORDER BY valor DESC LIMIT 5
    ),
    pass_dia_semana AS (
        SELECT d.dia_da_semana as category, AVG(f.passageiros) as value
        FROM fact_viagens f
        JOIN dim_data d ON f.id_data = d.id_data
        WHERE f.id_veiculo = :id_veiculo AND d.data_completa BETWEEN :data_inicio AND :data_fim
        GROUP BY d.dia_da_semana, EXTRACT(ISODOW FROM d.data_completa)
        ORDER BY EXTRACT(ISODOW FROM d.data_completa)
    )
    SELECT
        dv.identificador_veiculo,
        dv.idade_veiculo_anos,
        dv.meses_adicionais,
        dv.em_operacao,
        (SELECT total_passageiros FROM metricas_base) as total_passageiros,
        (SELECT total_ocorrencias FROM metricas_base) as total_ocorrencias,
        (SELECT total_viagens FROM metricas_base) as total_viagens,
        (SELECT total_extensao_km FROM metricas_base) as total_extensao_km,
        (SELECT total_passageiros FROM metricas_base) / NULLIF((SELECT COUNT(DISTINCT date_trunc('month', data)) FROM agg_metricas_veiculos_diarias WHERE id_veiculo = :id_veiculo AND data BETWEEN :data_inicio AND :data_fim), 0) as media_pass_mes,
        (SELECT total_passageiros FROM metricas_base) / NULLIF((SELECT COUNT(DISTINCT data) FROM agg_metricas_veiculos_diarias WHERE id_veiculo = :id_veiculo AND data BETWEEN :data_inicio AND :data_fim), 0) as media_pass_dia,
        (SELECT total_passageiros FROM metricas_base) / NULLIF((SELECT total_viagens FROM metricas_base), 0) as media_pass_viagem,
        (SELECT json_agg(j) FROM justificativas j) as grafico_justificativas,
        (SELECT json_agg(la) FROM linhas_atendidas la) as grafico_linhas_atendidas,
        (SELECT json_agg(pds) FROM pass_dia_semana pds) as grafico_media_passageiros_dia_semana
    FROM dim_veiculo dv
    WHERE dv.id_veiculo = :id_veiculo;
    """)
    return db.execute(query, {"id_veiculo": id_veiculo_req, "data_inicio": data_inicio, "data_fim": data_fim}).fetchone()
