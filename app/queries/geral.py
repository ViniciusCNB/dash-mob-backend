from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import date


def get_kpis_gerais(db: Session, data_inicio: date, data_fim: date):
    query = text(
        """
        SELECT
            COALESCE(SUM(f.passageiros), 0) AS total_passageiros,
            COALESCE(COUNT(f.id_fato_viagem), 0) AS total_viagens,
            COALESCE(SUM(f.flag_possui_ocorrencia), 0) AS total_ocorrencias,
            COALESCE(SUM(f.passageiros) / NULLIF(SUM(f.extensao_realizada_km), 0), 0) AS eficiencia_passageiro_km
        FROM fact_viagens f
        JOIN dim_data d ON f.id_data = d.id_data
        WHERE d.data_completa BETWEEN :data_inicio AND :data_fim;
    """
    )
    result = db.execute(
        query, {"data_inicio": data_inicio, "data_fim": data_fim}
    ).fetchone()
    return result
