from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from datetime import date
from typing import List

from app import schemas
from app.queries import concessionarias as queries_concessionarias
from app.database import get_db

router = APIRouter(
    prefix="/api/v1/concessionarias",
    tags=["Concessionárias"]
)


@router.get("/", response_model=List[schemas.ConcessionariaParaFiltro])
def read_concessionarias_para_filtro(db: Session = Depends(get_db)):
    """ Retorna uma lista de todas as concessionárias para filtros. """
    return queries_concessionarias.get_todas_as_concessionarias(db)


@router.get("/ranking-comparativo", response_model=List[schemas.RankingConcessionariaItem])
def read_ranking_de_concessionarias(
    data_inicio: date,
    data_fim: date,
    db: Session = Depends(get_db)
):
    """ Retorna um ranking comparativo de todas as concessionárias. """
    return queries_concessionarias.get_ranking_concessionarias(db, data_inicio, data_fim)


@router.get("/{id_concessionaria}/dashboard", response_model=schemas.ConcessionariaDashboardResponse)
def read_dashboard_de_concessionaria(
    id_concessionaria: int,
    data_inicio: date,
    data_fim: date,
    db: Session = Depends(get_db)
):
    """ Retorna todos os dados para o dashboard de uma concessionária individual. """
    dados = queries_concessionarias.get_dashboard_concessionaria(db, id_concessionaria, data_inicio, data_fim)
    if not dados:
        raise HTTPException(status_code=404, detail="Concessionária não encontrada ou sem dados no período.")

    response = {
        "estatisticas_detalhadas": [
            {"label": "Nome da Concessionária", "value": dados.nome_concessionaria},
            {"label": "Código", "value": dados.codigo_concessionaria},
            {"label": "Total de Passageiros", "value": int(dados.total_passageiros or 0)},
            {"label": "Total de Ocorrências", "value": int(dados.total_ocorrencias or 0)},
        ],
        "grafico_linhas_mais_utilizadas": dados.grafico_linhas_mais_utilizadas or [],
        "grafico_media_passageiros_dia_semana": dados.grafico_media_passageiros_dia_semana or []
    }
    return response
