from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from datetime import date
from typing import List
from enum import Enum

from app import schemas
from app.queries import bairros as queries_bairros
from app.database import get_db

router = APIRouter(
    prefix="/api/v1/bairros",
    tags=["Bairros"]
)


class MetricaRankingBairro(str, Enum):
    linhas = "linhas"
    ocorrencias = "ocorrencias"
    pontos = "pontos"


@router.get("/", response_model=List[schemas.BairroParaFiltro])
def read_bairros_para_filtro(db: Session = Depends(get_db)):
    """ Retorna uma lista de todos os bairros para filtros. """
    return queries_bairros.get_todos_os_bairros(db)


@router.get("/ranking/{metrica}", response_model=List[schemas.RankingItem])
def read_ranking_de_bairros(
    metrica: MetricaRankingBairro,
    data_inicio: date,
    data_fim: date,
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db)
):
    """ Retorna um ranking de bairros por uma métrica específica. """
    return queries_bairros.get_ranking_bairros(db, metrica.value, data_inicio, data_fim, limit)


@router.get("/{id_bairro}/dashboard", response_model=schemas.BairroDashboardResponse)
def read_dashboard_de_bairro(
    id_bairro: int,
    data_inicio: date,
    data_fim: date,
    db: Session = Depends(get_db)
):
    """ Retorna todos os dados para o dashboard de um bairro individual, incluindo o mapa. """
    dados = queries_bairros.get_dashboard_bairro(db, id_bairro, data_inicio, data_fim)
    if not dados:
        raise HTTPException(status_code=404, detail="Bairro não encontrado ou sem dados no período.")

    response = {
        "estatisticas_detalhadas": [
            {"label": "Nome do Bairro", "value": dados.nome_bairro},
            {"label": "População (2022)", "value": int(dados.populacao or 0)},
            {"label": "Domicílios (2022)", "value": int(dados.domicilios or 0)},
            {"label": "Área", "value": f"{dados.area_km or 0:.2f} km²"},
            {"label": "Densidade Demográfica", "value": f"{dados.densidade_demografica or 0:.2f} hab/km²"},
            {"label": "Linhas que Atendem", "value": int(dados.qtd_linhas or 0)},
            {"label": "Pontos de Ônibus", "value": int(dados.qtd_pontos or 0)},
            {"label": "Empresas Atuantes", "value": int(dados.qtd_empresas or 0)},
            {"label": "Concessionárias Atuantes", "value": int(dados.qtd_concessionarias or 0)},
        ],
        "grafico_linhas_mais_utilizadas": dados.grafico_linhas_mais_utilizadas or [],
        "grafico_media_passageiros_dia_semana": dados.grafico_media_passageiros_dia_semana or [],
        "mapa_geometria_bairro": dados.mapa_geometria_bairro or {"type": "FeatureCollection", "features": []},
        "mapa_pontos_bairro": dados.mapa_pontos_bairro or {"type": "FeatureCollection", "features": []}
    }
    return response
