from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from datetime import date
from typing import List
from enum import Enum

from app import schemas
from app.queries import ocorrencias as queries_ocorrencias
from app.database import get_db


router = APIRouter(
    prefix="/api/v1/ocorrencias",
    tags=["Ocorrências"]
)


class EntidadeRanking(str, Enum):
    empresa = "empresa"
    concessionaria = "concessionaria"
    linha = "linha"


@router.get("/ranking-por-justificativa", response_model=List[schemas.RankingOcorrenciasItem])
def read_ranking_ocorrencias_justificativa(
    data_inicio: date,
    data_fim: date,
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db)
):
    """
    Retorna o ranking de ocorrências agrupadas por justificativa.
    """
    return queries_ocorrencias.get_ranking_ocorrencias_por_justificativa(db, data_inicio, data_fim, limit)


@router.get("/ranking-por-entidade/{entidade}", response_model=List[schemas.RankingOcorrenciasItem])
def read_ranking_ocorrencias_entidade(
    entidade: EntidadeRanking,
    data_inicio: date,
    data_fim: date,
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db)
):
    """
    Retorna o ranking de ocorrências por entidade (empresa, concessionaria ou linha).
    """
    return queries_ocorrencias.get_ranking_ocorrencias_por_entidade(db, entidade.value, data_inicio, data_fim, limit)


@router.get("/tendencia-temporal", response_model=List[schemas.TendenciaTemporalItem])
def read_tendencia_temporal(
    data_inicio: date,
    data_fim: date,
    db: Session = Depends(get_db)
):
    """
    Retorna a série temporal de ocorrências, agregada por mês.
    """
    return queries_ocorrencias.get_tendencia_temporal_ocorrencias(db, data_inicio, data_fim)


@router.get("/por-tipo-dia", response_model=List[schemas.OcorrenciasPorTipoDiaItem])
def read_ocorrencias_por_tipo_dia(
    data_inicio: date,
    data_fim: date,
    db: Session = Depends(get_db)
):
    """
    Retorna a contagem de ocorrências para cada tipo de dia.
    """
    return queries_ocorrencias.get_ocorrencias_por_tipo_dia(db, data_inicio, data_fim)


@router.get("/{id_justificativa}/dashboard", response_model=schemas.JustificativaDashboardResponse)
def read_dashboard_de_justificativa(
    id_justificativa: int,
    data_inicio: date,
    data_fim: date,
    db: Session = Depends(get_db)
):
    """
    Retorna um objeto completo com todas as estatísticas e dados de gráficos
    para a página de análise de uma justificativa de ocorrência individual.
    """
    dados = queries_ocorrencias.get_dashboard_justificativa(db, id_justificativa, data_inicio, data_fim)

    if not dados or not dados.total_ocorrencias:
        raise HTTPException(status_code=404, detail="Nenhuma ocorrência encontrada para esta justificativa no período especificado.")

    # Formata a saída no schema esperado
    response = {
        "estatisticas_detalhadas": [
            {"label": "Descrição da Ocorrência", "value": dados.desc_ocorrencia},
            {"label": "Tipo da Ocorrência", "value": dados.tipo_ocorrencia},
            {"label": "Total de Ocorrências", "value": int(dados.total_ocorrencias or 0)},
            {"label": "Passageiros Afetados", "value": int(dados.passageiros_afetados or 0)},
            {"label": "Viagens Afetadas", "value": int(dados.total_ocorrencias or 0)}, # Total de ocorrências = Viagens Afetadas
        ],
        "grafico_linhas_afetadas": dados.grafico_linhas_afetadas or [],
        "grafico_veiculos_afetados": dados.grafico_veiculos_afetados or [],
        "grafico_media_ocorrencias_dia_semana": dados.grafico_media_ocorrencias_dia_semana or [],
        "id_linha_mais_afetada": dados.id_linha_mais_afetada
    }
    return response
