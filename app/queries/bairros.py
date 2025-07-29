from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import date


def get_todos_os_bairros(db: Session):
    """ Busca todos os bairros para popular filtros. """
    query = text("SELECT id_bairro, nome_bairro FROM dim_bairro ORDER BY nome_bairro;")
    return db.execute(query).all()


def get_ranking_bairros(db: Session, metrica: str, data_inicio: date, data_fim: date, limit: int):
    """ Retorna rankings de bairros por linhas, ocorrências ou pontos. """
    if metrica == 'linhas':
        query = text("""
            SELECT
                b.id_bairro as id,
                b.id_bairro::TEXT as codigo, -- [CORREÇÃO] Adicionada a coluna 'codigo'
                b.nome_bairro as nome,
                COUNT(blb.id_linha) as valor
            FROM bridge_linha_bairro blb
            JOIN dim_bairro b ON blb.id_bairro = b.id_bairro
            GROUP BY b.id_bairro, b.nome_bairro ORDER BY valor DESC LIMIT :limit;
        """)
        return db.execute(query, {"limit": limit}).all()
    elif metrica == 'ocorrencias':
        query = text("""
            SELECT
                b.id_bairro as id,
                b.id_bairro::TEXT as codigo, -- [CORREÇÃO] Adicionada a coluna 'codigo'
                b.nome_bairro as nome,
                SUM(agg.total_ocorrencias) as valor
            FROM agg_metricas_bairros_diarias agg
            JOIN dim_bairro b ON agg.id_bairro = b.id_bairro
            WHERE agg.data BETWEEN :data_inicio AND :data_fim
            GROUP BY b.id_bairro, b.nome_bairro ORDER BY valor DESC LIMIT :limit;
        """)
        return db.execute(query, {"data_inicio": data_inicio, "data_fim": data_fim, "limit": limit}).all()
    elif metrica == 'pontos':
        query = text("""
            SELECT
                b.id_bairro as id,
                b.id_bairro::TEXT as codigo, -- [CORREÇÃO] Adicionada a coluna 'codigo'
                b.nome_bairro as nome,
                COUNT(bpb.identificador_ponto_onibus) as valor
            FROM bridge_ponto_bairro bpb
            JOIN dim_bairro b ON bpb.id_bairro = b.id_bairro
            GROUP BY b.id_bairro, b.nome_bairro ORDER BY valor DESC LIMIT :limit;
        """)
        return db.execute(query, {"limit": limit}).all()
    else:
        raise ValueError("Métrica de ranking de bairro inválida.")


def get_dashboard_bairro(db: Session, id_bairro_req: int, data_inicio: date, data_fim: date):
    """
    Busca todos os dados para o dashboard de um bairro específico, incluindo
    as geometrias para o mapa.
    """
    query = text("""
    WITH
    -- CTEs 1 a 4 (as mesmas de antes)
    metricas_bairro AS (
        SELECT SUM(total_passageiros) as total_passageiros
        FROM agg_metricas_bairros_diarias
        WHERE id_bairro = :id_bairro AND data BETWEEN :data_inicio AND :data_fim
    ),
    contagens_estaticas AS (
        SELECT
            (SELECT COUNT(*) FROM bridge_linha_bairro WHERE id_bairro = :id_bairro) as qtd_linhas,
            (SELECT COUNT(*) FROM bridge_ponto_bairro WHERE id_bairro = :id_bairro) as qtd_pontos,
            (SELECT COUNT(DISTINCT agg.id_empresa) FROM agg_metricas_linhas_diarias agg JOIN bridge_linha_bairro blb ON agg.id_linha = blb.id_linha WHERE blb.id_bairro = :id_bairro AND agg.data BETWEEN :data_inicio AND :data_fim) as qtd_empresas,
            (SELECT COUNT(DISTINCT agg.id_concessionaria) FROM agg_metricas_linhas_diarias agg JOIN bridge_linha_bairro blb ON agg.id_linha = blb.id_linha WHERE blb.id_bairro = :id_bairro AND agg.data BETWEEN :data_inicio AND :data_fim) as qtd_concessionarias
    ),
    linhas_mais_utilizadas AS (
        SELECT l.id_linha as id, l.cod_linha as codigo, l.nome_linha as nome, SUM(agg.total_passageiros) as valor
        FROM agg_metricas_linhas_diarias agg JOIN dim_linha l ON agg.id_linha = l.id_linha
        WHERE agg.id_linha IN (SELECT id_linha FROM bridge_linha_bairro WHERE id_bairro = :id_bairro)
          AND agg.data BETWEEN :data_inicio AND :data_fim
        GROUP BY l.id_linha, l.cod_linha, l.nome_linha ORDER BY valor DESC LIMIT 5
    ),
    pass_dia_semana AS (
        SELECT d.dia_da_semana as category, SUM(f.passageiros) / COUNT(DISTINCT d.data_completa) as value
        FROM fact_viagens f JOIN dim_data d ON f.id_data = d.id_data
        WHERE f.id_linha IN (SELECT id_linha FROM bridge_linha_bairro WHERE id_bairro = :id_bairro)
          AND d.data_completa BETWEEN :data_inicio AND :data_fim
        GROUP BY d.dia_da_semana, EXTRACT(ISODOW FROM d.data_completa)
        ORDER BY EXTRACT(ISODOW FROM d.data_completa)
    ),
    -- [NOVA CTE] CTE 5: Coleta a geometria do polígono do bairro
    geometria_bairro AS (
        SELECT
            json_build_object(
                'type', 'Feature',
                'geometry', ST_AsGeoJSON(geom)::json,
                'properties', json_build_object('nome_bairro', nome_bairro)
            ) as feature
        FROM dim_bairro
        WHERE id_bairro = :id_bairro
    ),
    -- [NOVA CTE] CTE 6: Coleta as geometrias dos pontos de ônibus DENTRO do bairro
    geometrias_pontos AS (
        SELECT
            json_build_object(
                'type', 'Feature',
                'geometry', ST_AsGeoJSON(p.geom)::json,
                'properties', json_build_object('identificador_ponto', p.identificador_ponto_onibus)
            ) as feature
        FROM staging_pontos_onibus_bh p
        WHERE p.identificador_ponto_onibus IN (
            SELECT identificador_ponto_onibus FROM bridge_ponto_bairro WHERE id_bairro = :id_bairro
        )
        AND p.ano_referencia = (SELECT MAX(ano_referencia) FROM staging_pontos_onibus_bh)
        AND p.mes_referencia = (SELECT MAX(mes_referencia) FROM staging_pontos_onibus_bh WHERE ano_referencia = (SELECT MAX(ano_referencia) FROM staging_pontos_onibus_bh))
        AND p.geom IS NOT NULL
    )
    -- Query Final
    SELECT
        b.nome_bairro, b.populacao, b.domicilios, b.area_km, b.densidade_demografica,
        cs.qtd_linhas, cs.qtd_pontos, cs.qtd_empresas, cs.qtd_concessionarias,
        (SELECT json_agg(lmu) FROM linhas_mais_utilizadas lmu) as grafico_linhas_mais_utilizadas,
        (SELECT json_agg(pds) FROM pass_dia_semana pds) as grafico_media_passageiros_dia_semana,
        -- [NOVA COLUNA] Agrega a feature do bairro em uma FeatureCollection
        (SELECT json_build_object('type', 'FeatureCollection', 'features', json_agg(gb.feature)) FROM geometria_bairro gb) as mapa_geometria_bairro,
        -- [NOVA COLUNA] Agrega todas as features dos pontos em uma FeatureCollection
        (SELECT json_build_object('type', 'FeatureCollection', 'features', COALESCE(json_agg(gp.feature), '[]'::json)) FROM geometrias_pontos gp) as mapa_pontos_bairro
    FROM dim_bairro b, contagens_estaticas cs
    WHERE b.id_bairro = :id_bairro;
    """)

    return db.execute(query, {"id_bairro": id_bairro_req, "data_inicio": data_inicio, "data_fim": data_fim}).fetchone()
