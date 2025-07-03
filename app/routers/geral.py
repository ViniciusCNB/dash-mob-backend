from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from datetime import date

from app import schemas
from app.database import get_db
from app.queries.geral import get_kpis_gerais

router = APIRouter(prefix="/api/v1/geral", tags=["Visão Geral"])


@router.get("/kpis", response_model=schemas.KpiGeral)
def read_kpis_gerais(data_inicio: date, data_fim: date, db: Session = Depends(get_db)):
    """
    Retorna os Indicadores-Chave de Desempenho (KPIs) para um determinado período.
    """
    kpis = get_kpis_gerais(db=db, data_inicio=data_inicio, data_fim=data_fim)
    return kpis
