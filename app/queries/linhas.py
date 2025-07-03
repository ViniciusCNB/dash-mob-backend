from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import date


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
