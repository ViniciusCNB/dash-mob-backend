from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import date


def get_analise_eficiencia_linhas(db: Session, data_inicio: date, data_fim: date):
    """
    Calcula as métricas de eficiência (passageiros/km e passageiros/minuto)
    para todas as linhas em um determinado período, usando a tabela agregada.
    [ESTA QUERY ESTÁ CORRETA - NENHUMA MUDANÇA NECESSÁRIA]
    """
    query = text("""
        SELECT
            l.id_linha,
            l.cod_linha,
            l.nome_linha,
            COALESCE(SUM(agg.total_passageiros), 0) AS total_passageiros,
            COALESCE(SUM(agg.total_passageiros) / NULLIF(SUM(agg.total_extensao_km), 0), 0) AS passageiros_por_km,
            COALESCE(SUM(agg.total_passageiros) / NULLIF(SUM(agg.total_duracao_minutos), 0), 0) AS passageiros_por_minuto
        FROM
            agg_metricas_linhas_diarias agg
        JOIN
            dim_linha l ON agg.id_linha = l.id_linha
        WHERE
            agg.data BETWEEN :data_inicio AND :data_fim
            AND agg.total_extensao_km > 0
            AND agg.total_duracao_minutos > 0
        GROUP BY
            l.id_linha, l.cod_linha, l.nome_linha;
    """)
    return db.execute(query, {"data_inicio": data_inicio, "data_fim": data_fim}).all()


def get_taxa_falhas_por_empresa(db: Session, data_inicio: date, data_fim: date):
    """
    Calcula a taxa de falhas mecânicas por 10.000 viagens para cada empresa.
    [CORREÇÃO] Adicionada condição para excluir a empresa "Não Informado" do ranking.
    """
    query = text("""
        WITH falhas AS (
            SELECT id_empresa, COUNT(*) as total_falhas
            FROM agg_falhas_mecanicas_diarias
            WHERE data BETWEEN :data_inicio AND :data_fim
            GROUP BY id_empresa
        ),
        viagens AS (
            SELECT id_empresa, SUM(total_viagens) as total_viagens
            FROM agg_metricas_linhas_diarias
            WHERE data BETWEEN :data_inicio AND :data_fim
            GROUP BY id_empresa
        )
        SELECT
            e.id_empresa,
            e.nome_empresa,
            (COALESCE(f.total_falhas, 0) * 10000.0) / NULLIF(v.total_viagens, 0) AS taxa_falhas_por_10k_viagens
        FROM dim_empresa e
        LEFT JOIN falhas f ON e.id_empresa = f.id_empresa
        LEFT JOIN viagens v ON e.id_empresa = v.id_empresa
        WHERE
            -- [MUDANÇA] Exclui o registro "Não Informado" do resultado final.
            e.codigo_empresa != 0
            AND (v.total_viagens > 0 OR f.total_falhas > 0)
        ORDER BY taxa_falhas_por_10k_viagens DESC;
    """)
    return db.execute(query, {"data_inicio": data_inicio, "data_fim": data_fim}).all()


def get_ranking_justificativas_falhas(db: Session, data_inicio: date, data_fim: date):
    """
    Retorna as justificativas mais comuns para falhas mecânicas.
    [ESTA QUERY ESTÁ CORRETA - NENHUMA MUDANÇA NECESSÁRIA]
    """
    query = text("""
        SELECT
            j.nome_justificativa,
            SUM(agg.total_falhas) AS total_falhas
        FROM agg_falhas_mecanicas_diarias agg
        JOIN dim_justificativa j ON agg.id_justificativa = j.id_justificativa
        WHERE agg.data BETWEEN :data_inicio AND :data_fim
        GROUP BY j.nome_justificativa
        ORDER BY total_falhas DESC;
    """)
    return db.execute(query, {"data_inicio": data_inicio, "data_fim": data_fim}).all()


def get_correlacao_idade_falhas(db: Session, data_inicio: date, data_fim: date):
    """
    Retorna dados para a análise de correlação entre idade do veículo e número de falhas.
    [OTIMIZAÇÃO FINAL] A query agora usa uma VIEW MATERIALIZADA para obter a empresa do veículo.
    """
    query = text("""
        WITH
        -- A CTE de falhas continua a mesma, pois é rápida
        falhas_por_veiculo AS (
            SELECT
                id_veiculo,
                SUM(total_falhas) AS total_falhas
            FROM agg_falhas_mecanicas_diarias
            WHERE data BETWEEN :data_inicio AND :data_fim
            GROUP BY id_veiculo
        )
        -- Query Final: Junta as informações, substituindo a CTE lenta pela VIEW MATERIALIZADA
        SELECT
            v.id_veiculo,
            v.idade_veiculo_anos,
            e.nome_empresa,
            COALESCE(fpv.total_falhas, 0) AS total_falhas
        FROM
            dim_veiculo v
        -- [MUDANÇA] Em vez de uma CTE complexa, usamos a view materializada que é super rápida.
        LEFT JOIN
            mv_empresa_principal_veiculo epv ON v.id_veiculo = epv.id_veiculo
        -- Junta com a dim_empresa para obter o nome
        LEFT JOIN
            dim_empresa e ON epv.id_empresa = e.id_empresa
        -- Junta com as falhas calculadas
        LEFT JOIN
            falhas_por_veiculo fpv ON v.id_veiculo = fpv.id_veiculo
        WHERE
            -- Opcional: exclui veículos cuja empresa não foi identificada
            e.codigo_empresa != 0
        ORDER BY
            total_falhas DESC;
    """)
    return db.execute(query, {"data_inicio": data_inicio, "data_fim": data_fim}).all()


def get_ranking_linhas_por_falhas(db: Session, data_inicio: date, data_fim: date):
    """
    Retorna o ranking de linhas com o maior número de falhas mecânicas.
    [ESTA QUERY ESTÁ CORRETA - NENHUMA MUDANÇA NECESSÁRIA]
    """
    query = text("""
        SELECT
            l.id_linha,
            l.cod_linha,
            l.nome_linha,
            SUM(agg.total_falhas) AS total_falhas
        FROM agg_falhas_mecanicas_diarias agg
        JOIN dim_linha l ON agg.id_linha = l.id_linha
        WHERE agg.data BETWEEN :data_inicio AND :data_fim
        GROUP BY l.id_linha, l.cod_linha, l.nome_linha
        ORDER BY total_falhas DESC;
    """)
    return db.execute(query, {"data_inicio": data_inicio, "data_fim": data_fim}).all()
