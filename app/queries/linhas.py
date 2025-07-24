from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import date


def get_todas_as_linhas(db: Session):
    """
    Busca todas as linhas da tabela de dimensão para popular filtros.
    """
    query = text("""
        SELECT id_linha, cod_linha, nome_linha
        FROM dim_linha
        ORDER BY cod_linha;
    """)
    return db.execute(query).all()


def get_ranking_linhas(
    db: Session, metrica: str, data_inicio: date, data_fim: date, limit: int
):
    """
    Busca o ranking de linhas por uma métrica específica (passageiros, viagens, ocorrências).
    A consulta é feita na tabela agregada para alta performance.
    """
    if metrica not in ["passageiros", "viagens", "ocorrencias"]:
        raise ValueError(
            "Métrica inválida. Use 'passageiros', 'viagens' ou 'ocorrencias'."
        )

    coluna_soma = f"total_{metrica}"

    query = text(
        f"""
        SELECT
            l.id_linha AS id,
            l.cod_linha AS codigo,
            l.nome_linha AS nome,
            SUM(agg.{coluna_soma}) AS valor
        FROM
            agg_metricas_linhas_diarias agg
        JOIN
            dim_linha l ON agg.id_linha = l.id_linha
        WHERE
            agg.data BETWEEN :data_inicio AND :data_fim
        GROUP BY
            l.id_linha, l.cod_linha, l.nome_linha
        ORDER BY
            valor DESC
        LIMIT :limit;
    """
    )
    result = db.execute(
        query, {"data_inicio": data_inicio, "data_fim": data_fim, "limit": limit}
    ).all()
    return result


def get_contagem_linhas_por_concessionaria(db: Session):
    """
    Conta o número de linhas distintas operadas por cada concessionária.
    """
    query = text(
        """
        SELECT
            c.id_concessionaria AS id,
            c.nome_concessionaria AS nome,
            COUNT(DISTINCT f.id_linha) AS quantidade_linhas
        FROM
            fact_viagens f
        JOIN
            dim_concessionaria c ON f.id_concessionaria = c.id_concessionaria
        WHERE f.id_linha IS NOT NULL AND f.id_concessionaria IS NOT NULL
        GROUP BY
            c.id_concessionaria, c.nome_concessionaria
        ORDER BY
            quantidade_linhas DESC;
    """
    )
    return db.execute(query).all()


def get_contagem_linhas_por_empresa(db: Session):
    """
    Conta o número de linhas distintas operadas por cada empresa.
    """
    query = text(
        """
        SELECT
            e.id_empresa AS id,
            e.nome_empresa AS nome,
            COUNT(DISTINCT f.id_linha) AS quantidade_linhas
        FROM
            fact_viagens f
        JOIN
            dim_empresa e ON f.id_empresa = e.id_empresa
        WHERE f.id_linha IS NOT NULL AND f.id_empresa IS NOT NULL
        GROUP BY
            e.id_empresa, e.nome_empresa
        ORDER BY
            quantidade_linhas DESC;
    """
    )
    return db.execute(query).all()


def get_contagem_pontos_por_linha(db: Session, limit: int):
    """
    Conta o número de pontos de parada distintos para cada linha,
    considerando apenas o mês de referência mais recente.
    """
    query = text(
        """
        WITH ultimo_periodo AS (
            SELECT
                MAX(ano_referencia) AS ano,
                MAX(mes_referencia) AS mes
            FROM
                staging_pontos_onibus_bh
            WHERE
                ano_referencia = (SELECT MAX(ano_referencia) FROM staging_pontos_onibus_bh)
        )
        SELECT
            l.id_linha AS id,
            l.cod_linha AS codigo,
            l.nome_linha AS nome,
            COUNT(DISTINCT p.identificador_ponto_onibus) AS valor
        FROM
            staging_pontos_onibus_bh p
        JOIN
            dim_linha l ON p.cod_linha = l.cod_linha
        JOIN
            ultimo_periodo up ON p.ano_referencia = up.ano AND p.mes_referencia = up.mes
        GROUP BY
            l.id_linha, l.cod_linha, l.nome_linha
        ORDER BY
            valor DESC
        LIMIT :limit;
    """
    )
    return db.execute(query, {"limit": limit}).all()


def get_contagem_linhas_por_bairro(db: Session, limit: int):
    """
    Conta o número de linhas que passam em cada bairro.
    """
    query = text(
        """
        SELECT
            b.id_bairro AS id,
            b.nome_bairro AS nome,
            COUNT(bl.id_linha) AS valor
        FROM
            bridge_linha_bairro bl
        JOIN
            dim_bairro b ON bl.id_bairro = b.id_bairro
        GROUP BY
            b.id_bairro, b.nome_bairro
        ORDER BY
            valor DESC
        LIMIT :limit;
    """
    )
    return db.execute(query, {"limit": limit}).all()


def get_geometria_linha(db: Session, cod_linha: str):
    """
    Busca as coordenadas geográficas dos pontos de uma linha em ordem,
    considerando apenas o mês de referência mais recente.
    A ordenação é feita pelo ID sequencial da tabela de origem,
    que é a melhor aproximação disponível para a sequência da rota.
    """
    query = text(
        """
        WITH ultimo_periodo AS (
            SELECT
                MAX(ano_referencia) AS ano,
                MAX(mes_referencia) AS mes
            FROM
                staging_pontos_onibus_bh
            WHERE
                ano_referencia = (SELECT MAX(ano_referencia) FROM staging_pontos_onibus_bh)
        )
        SELECT
            ST_X(p.geom) AS longitude,
            ST_Y(p.geom) AS latitude
        FROM
            staging_pontos_onibus_bh p
        JOIN
            ultimo_periodo up ON p.ano_referencia = up.ano AND p.mes_referencia = up.mes
        WHERE
            p.cod_linha = :cod_linha
        ORDER BY
            p.id_ponto_onibus_linha;
    """
    )
    result = db.execute(query, {"cod_linha": cod_linha}).all()
    return [[row.longitude, row.latitude] for row in result]


def get_pontos_geometria_linha(db: Session, cod_linha: str):
    """
    Busca as coordenadas e os identificadores dos pontos de uma linha,
    considerando apenas o mês de referência mais recente.
    """
    query = text("""
        WITH ultimo_periodo AS (
            SELECT
                MAX(ano_referencia) AS ano,
                MAX(mes_referencia) AS mes
            FROM
                staging_pontos_onibus_bh
            WHERE
                ano_referencia = (SELECT MAX(ano_referencia) FROM staging_pontos_onibus_bh)
        )
        SELECT DISTINCT -- Usamos DISTINCT para pegar cada ponto físico apenas uma vez
            p.identificador_ponto_onibus,
            ST_X(p.geom) AS longitude,
            ST_Y(p.geom) AS latitude
        FROM
            staging_pontos_onibus_bh p
        JOIN
            ultimo_periodo up ON p.ano_referencia = up.ano AND p.mes_referencia = up.mes
        WHERE
            p.cod_linha = :cod_linha AND p.geom IS NOT NULL;
    """)
    return db.execute(query, {"cod_linha": cod_linha}).all()


def get_dashboard_linha(db: Session, id_linha_req: int, data_inicio: date, data_fim: date):
    """
    Busca todos os dados agregados, incluindo geometrias de pontos e bairros,
    para o dashboard de uma linha específica.
    [VERSÃO FINAL COM MAPA COMPLETO]
    """
    query = text("""
    WITH
    -- CTEs 1 a 8 (as mesmas que já tínhamos)
    entidade_principal AS (
        SELECT f.id_empresa, f.id_concessionaria FROM fact_viagens f JOIN dim_data d ON f.id_data = d.id_data
        WHERE f.id_linha = :id_linha AND d.data_completa BETWEEN :data_inicio AND :data_fim
        GROUP BY f.id_empresa, f.id_concessionaria ORDER BY COUNT(*) DESC LIMIT 1
    ),
    metricas_base AS (
        SELECT SUM(total_viagens) AS total_viagens, SUM(total_passageiros) AS total_passageiros
        FROM agg_metricas_linhas_diarias WHERE id_linha = :id_linha AND data BETWEEN :data_inicio AND :data_fim
    ),
    metricas_ocorrencias AS (
        SELECT SUM(f.flag_viagem_nao_realizada) as viagens_nao_realizadas,
               SUM(f.flag_viagem_interrompida) as viagens_interrompidas,
               SUM(CASE WHEN f.passageiros = 0 THEN 1 ELSE 0 END) as viagens_zero_passageiros
        FROM fact_viagens f JOIN dim_data d ON f.id_data = d.id_data
        WHERE f.id_linha = :id_linha AND d.data_completa BETWEEN :data_inicio AND :data_fim
    ),
    metricas_temporais AS (
        SELECT (SELECT total_passageiros FROM metricas_base) / NULLIF(COUNT(DISTINCT date_trunc('month', data)), 0) AS media_pass_mes,
               (SELECT total_passageiros FROM metricas_base) / NULLIF(COUNT(DISTINCT data), 0) AS media_pass_dia,
               (SELECT total_passageiros FROM metricas_base) / NULLIF((SELECT total_viagens FROM metricas_base), 0) AS media_pass_viagem
        FROM agg_metricas_linhas_diarias WHERE id_linha = :id_linha AND data BETWEEN :data_inicio AND :data_fim
    ),
    contagem_bairros AS (
        SELECT COUNT(*) as qtd FROM bridge_linha_bairro WHERE id_linha = :id_linha
    ),
    contagem_pontos AS (
        SELECT COUNT(DISTINCT identificador_ponto_onibus) as qtd FROM staging_pontos_onibus_bh
        WHERE cod_linha = (SELECT cod_linha FROM dim_linha WHERE id_linha = :id_linha)
          AND ano_referencia = (SELECT MAX(ano_referencia) FROM staging_pontos_onibus_bh)
          AND mes_referencia = (SELECT MAX(mes_referencia) FROM staging_pontos_onibus_bh WHERE ano_referencia = (SELECT MAX(ano_referencia) FROM staging_pontos_onibus_bh))
    ),
    justificativas AS (
        SELECT j.nome_justificativa as category, COUNT(*) as value FROM fact_viagens f
        JOIN dim_data d ON f.id_data = d.id_data JOIN dim_justificativa j ON f.id_justificativa = j.id_justificativa
        WHERE f.id_linha = :id_linha AND d.data_completa BETWEEN :data_inicio AND :data_fim
        GROUP BY j.nome_justificativa
    ),
    pass_dia_semana AS (
        SELECT d.dia_da_semana as category, AVG(f.passageiros) as value FROM fact_viagens f
        JOIN dim_data d ON f.id_data = d.id_data WHERE f.id_linha = :id_linha AND d.data_completa BETWEEN :data_inicio AND :data_fim
        GROUP BY d.dia_da_semana, EXTRACT(ISODOW FROM d.data_completa) ORDER BY EXTRACT(ISODOW FROM d.data_completa)
    ),
    geometrias_pontos AS (
        SELECT json_build_object('type', 'Feature', 'geometry', ST_AsGeoJSON(p.geom)::json, 'properties', json_build_object('identificador_ponto', p.identificador_ponto_onibus)) as feature
        FROM (SELECT DISTINCT ON (identificador_ponto_onibus) identificador_ponto_onibus, geom FROM staging_pontos_onibus_bh
              WHERE cod_linha = (SELECT cod_linha FROM dim_linha WHERE id_linha = :id_linha)
                AND ano_referencia = (SELECT MAX(ano_referencia) FROM staging_pontos_onibus_bh)
                AND mes_referencia = (SELECT MAX(mes_referencia) FROM staging_pontos_onibus_bh WHERE ano_referencia = (SELECT MAX(ano_referencia) FROM staging_pontos_onibus_bh))
        ) p WHERE p.geom IS NOT NULL
    ),
    -- [NOVA CTE] CTE 10: Coleta as geometrias dos BAIRROS por onde a linha passa
    geometrias_bairros AS (
        SELECT
            json_build_object(
                'type', 'Feature',
                'geometry', ST_AsGeoJSON(b.geom)::json,
                'properties', json_build_object('nome_bairro', b.nome_bairro)
            ) as feature
        FROM
            bridge_linha_bairro blb
        JOIN
            dim_bairro b ON blb.id_bairro = b.id_bairro
        WHERE
            blb.id_linha = :id_linha
    )
    -- Query Final: Junta todas as informações calculadas
    SELECT
      (SELECT e.nome_empresa FROM dim_empresa e JOIN entidade_principal ep ON e.id_empresa = ep.id_empresa) as empresa,
      (SELECT c.nome_concessionaria FROM dim_concessionaria c JOIN entidade_principal ep ON c.id_concessionaria = ep.id_concessionaria) as concessionaria,
      (SELECT total_viagens FROM metricas_base) as viagens_realizadas,
      (SELECT viagens_nao_realizadas FROM metricas_ocorrencias) as viagens_nao_realizadas,
      (SELECT viagens_interrompidas FROM metricas_ocorrencias) as viagens_interrompidas,
      (SELECT viagens_zero_passageiros FROM metricas_ocorrencias) as viagens_zero_passageiros,
      (SELECT extensao_km FROM dim_linha WHERE id_linha = :id_linha) as extensao_linha,
      (SELECT qtd FROM contagem_bairros) as bairros_percorridos,
      (SELECT qtd FROM contagem_pontos) as pontos_onibus,
      (SELECT media_pass_mes FROM metricas_temporais) as media_pass_mes,
      (SELECT media_pass_dia FROM metricas_temporais) as media_pass_dia,
      (SELECT media_pass_viagem FROM metricas_temporais) as media_pass_viagem,
      (SELECT json_agg(j) FROM justificativas j) as grafico_justificativas,
      (SELECT json_agg(pds) FROM pass_dia_semana pds) as grafico_media_passageiros_dia_semana,
      (SELECT json_build_object('type', 'FeatureCollection', 'features', COALESCE(json_agg(gp.feature), '[]'::json)) FROM geometrias_pontos gp) as mapa_pontos,
      -- [NOVA COLUNA] Agrega todas as features dos bairros em uma única FeatureCollection GeoJSON
      (SELECT json_build_object('type', 'FeatureCollection', 'features', COALESCE(json_agg(gb.feature), '[]'::json)) FROM geometrias_bairros gb) as mapa_bairros;
    """)
    result = db.execute(query, {"id_linha": id_linha_req, "data_inicio": data_inicio, "data_fim": data_fim}).fetchone()
    return result
