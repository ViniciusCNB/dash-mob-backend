from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from datetime import date
from typing import List

from app import schemas
from app.queries.estudos import (
    get_analise_eficiencia_linhas,
    get_taxa_falhas_por_empresa,
    get_ranking_justificativas_falhas,
    get_correlacao_idade_falhas,
    get_ranking_linhas_por_falhas,
)
from app.database import get_db


router = APIRouter(prefix="/api/v1/estudos", tags=["Estudos de Caso"])


@router.get("/analise-eficiencia", response_model=List[schemas.EficienciaLinha])
def read_analise_de_eficiencia(
    data_inicio: date,
    data_fim: date,
    db: Session = Depends(get_db)
):
    """
    Retorna os dados de eficiência (passageiros por km e por minuto) para
    todas as linhas no período especificado, para ser usado em um gráfico de quadrantes.
    """
    return get_analise_eficiencia_linhas(db, data_inicio, data_fim)


@router.get("/falhas-mecanicas/taxa-por-empresa", response_model=List[schemas.TaxaFalhasEmpresa])
def read_taxa_falhas_por_empresa(data_inicio: date, data_fim: date, db: Session = Depends(get_db)):
    """
    Retorna a taxa de falhas mecânicas por 10.000 viagens para cada empresa.
    """
    return get_taxa_falhas_por_empresa(db, data_inicio, data_fim)


@router.get("/falhas-mecanicas/ranking-justificativas", response_model=List[schemas.FalhaPorJustificativa])
def read_ranking_justificativas_falhas(data_inicio: date, data_fim: date, db: Session = Depends(get_db)):
    """
    Retorna as justificativas mais comuns para falhas mecânicas.
    """
    return get_ranking_justificativas_falhas(db, data_inicio, data_fim)


@router.get("/falhas-mecanicas/correlacao-idade-veiculo", response_model=List[schemas.CorrelacaoIdadeFalha])
def read_correlacao_idade_falhas(data_inicio: date, data_fim: date, db: Session = Depends(get_db)):
    """
    Retorna dados para a análise de correlação entre idade do veículo e número de falhas.
    """
    return get_correlacao_idade_falhas(db, data_inicio, data_fim)


@router.get("/falhas-mecanicas/ranking-linhas", response_model=List[schemas.RankingLinhasFalhas])
def read_ranking_linhas_por_falhas(data_inicio: date, data_fim: date, db: Session = Depends(get_db)):
    """
    Retorna o ranking de linhas com o maior número de falhas mecânicas.
    """
    return get_ranking_linhas_por_falhas(db, data_inicio, data_fim)
