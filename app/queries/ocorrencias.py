from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import date


def get_ranking_ocorrencias_por_justificativa(db: Session, data_inicio: date, data_fim: date, limit: int):
    """
    Retorna o ranking de ocorrências por justificativa.
    """
    query = text("""
        SELECT
            j.id_justificativa as id,
            j.nome_justificativa as nome,
            COUNT(f.id_fato_viagem) as total_ocorrencias
        FROM fact_viagens f
        JOIN dim_data d ON f.id_data = d.id_data
        JOIN dim_justificativa j ON f.id_justificativa = j.id_justificativa
        WHERE d.data_completa BETWEEN :data_inicio AND :data_fim
        GROUP BY j.id_justificativa, j.nome_justificativa
        ORDER BY total_ocorrencias DESC
        LIMIT :limit;
    """)
    return db.execute(query, {"data_inicio": data_inicio, "data_fim": data_fim, "limit": limit}).all()


def get_ranking_ocorrencias_por_entidade(db: Session, entidade: str, data_inicio: date, data_fim: date, limit: int):
    """
    Função genérica para retornar o ranking de ocorrências por empresa, concessionária ou linha.
    """
    if entidade not in ['empresa', 'concessionaria', 'linha']:
        raise ValueError("Entidade inválida. Use 'empresa', 'concessionaria' ou 'linha'.")

    dim_tabela = f"dim_{entidade}"
    id_coluna = f"id_{entidade}"
    nome_coluna = f"nome_{entidade}"
    if entidade == 'linha':
        nome_coluna = 'cod_linha'  # Usamos o código da linha como nome no ranking

    query = text(f"""
        SELECT
            dim.{id_coluna} as id,
            dim.{nome_coluna} as nome,
            SUM(agg.total_ocorrencias) as total_ocorrencias
        FROM agg_metricas_linhas_diarias agg
        JOIN {dim_tabela} dim ON agg.{id_coluna} = dim.{id_coluna}
        WHERE agg.data BETWEEN :data_inicio AND :data_fim
        GROUP BY dim.{id_coluna}, dim.{nome_coluna}
        ORDER BY total_ocorrencias DESC
        LIMIT :limit;
    """)
    return db.execute(query, {"data_inicio": data_inicio, "data_fim": data_fim, "limit": limit}).all()


def get_tendencia_temporal_ocorrencias(db: Session, data_inicio: date, data_fim: date):
    """
    Retorna a contagem de ocorrências agregada por mês.
    """
    query = text("""
        SELECT
            date_trunc('month', data)::date as periodo,
            SUM(total_ocorrencias) as total_ocorrencias
        FROM agg_metricas_linhas_diarias
        WHERE data BETWEEN :data_inicio AND :data_fim
        GROUP BY periodo
        ORDER BY periodo;
    """)
    return db.execute(query, {"data_inicio": data_inicio, "data_fim": data_fim}).all()


def get_ocorrencias_por_tipo_dia(db: Session, data_inicio: date, data_fim: date):
    """
    Retorna a contagem de ocorrências por tipo de dia (útil, sábado, domingo/feriado).
    """
    query = text("""
        SELECT
            d.tipo_dia,
            SUM(f.flag_possui_ocorrencia) as total_ocorrencias
        FROM fact_viagens f
        JOIN dim_data d ON f.id_data = d.id_data
        WHERE d.data_completa BETWEEN :data_inicio AND :data_fim
        GROUP BY d.tipo_dia
        ORDER BY total_ocorrencias DESC;
    """)
    return db.execute(query, {"data_inicio": data_inicio, "data_fim": data_fim}).all()


def get_dashboard_justificativa(db: Session, id_justificativa_req: int, data_inicio: date, data_fim: date):
    """
    Busca todos os dados agregados para o dashboard de uma justificativa de ocorrência específica.
    """
    query = text("""
    WITH
    -- CTE 1: Filtra as viagens que tiveram a ocorrência específica no período
    viagens_com_ocorrencia AS (
        SELECT f.*, d.dia_da_semana, EXTRACT(ISODOW FROM d.data_completa) as dow
        FROM fact_viagens f
        JOIN dim_data d ON f.id_data = d.id_data
        WHERE f.id_justificativa = :id_justificativa AND d.data_completa BETWEEN :data_inicio AND :data_fim
    ),
    -- CTE 2: Calcula as estatísticas principais
    estatisticas AS (
        SELECT
            COUNT(*) as total_ocorrencias,
            SUM(passageiros) as passageiros_afetados,
            SUM(flag_viagem_nao_realizada) as viagens_nao_realizadas
        FROM viagens_com_ocorrencia
    ),
    -- CTE 3: Ranking das linhas mais afetadas
    linhas_afetadas AS (
        SELECT
            l.id_linha as id,
            l.cod_linha as codigo,
            COUNT(v.id_fato_viagem) as valor
        FROM viagens_com_ocorrencia v
        JOIN dim_linha l ON v.id_linha = l.id_linha
        GROUP BY l.id_linha, l.cod_linha
        ORDER BY valor DESC
        LIMIT 5
    ),
    -- CTE 4: Ranking dos veículos mais afetados
    veiculos_afetados AS (
        SELECT
            v.id_veiculo as id,
            dv.identificador_veiculo::TEXT as codigo,
            COUNT(v.id_fato_viagem) as valor
        FROM viagens_com_ocorrencia v
        JOIN dim_veiculo dv ON v.id_veiculo = dv.id_veiculo
        GROUP BY v.id_veiculo, dv.identificador_veiculo
        ORDER BY valor DESC
        LIMIT 5
    ),
    -- CTE 5: Média de ocorrências por dia da semana
    ocorrencias_dia_semana AS (
        SELECT
            dia_da_semana as category,
            COUNT(*)::FLOAT as value
        FROM viagens_com_ocorrencia
        GROUP BY dia_da_semana, dow
        ORDER BY dow
    )
    -- Query Final: Junta todas as informações
    SELECT
        (SELECT nome_justificativa FROM dim_justificativa WHERE id_justificativa = :id_justificativa) as desc_ocorrencia,
        (SELECT o.nome_ocorrencia FROM dim_ocorrencia o JOIN dim_justificativa j ON o.id_ocorrencia = j.id_ocorrencia WHERE j.id_justificativa = :id_justificativa) as tipo_ocorrencia,
        (SELECT total_ocorrencias FROM estatisticas) as total_ocorrencias,
        (SELECT passageiros_afetados FROM estatisticas) as passageiros_afetados,
        (SELECT viagens_nao_realizadas FROM estatisticas) as viagens_nao_realizadas,
        (SELECT json_agg(la) FROM linhas_afetadas la) as grafico_linhas_afetadas,
        (SELECT json_agg(va) FROM veiculos_afetados va) as grafico_veiculos_afetados,
        (SELECT json_agg(ods) FROM ocorrencias_dia_semana ods) as grafico_media_ocorrencias_dia_semana,
        (SELECT id FROM linhas_afetadas LIMIT 1) as id_linha_mais_afetada;
    """)
    result = db.execute(
        query,
        {
            "id_justificativa": id_justificativa_req,
            "data_inicio": data_inicio,
            "data_fim": data_fim,
        },
    ).fetchone()
    return result
