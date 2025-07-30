from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from datetime import date
from typing import List

from app import schemas
from app.queries import empresas as queries_empresas
from app.database import get_db

router = APIRouter(
    prefix="/api/v1/empresas",
    tags=["Empresas"]
)


@router.get("/", response_model=List[schemas.EmpresaParaFiltro])
def read_empresas_para_filtro(db: Session = Depends(get_db)):
    """ Retorna uma lista de todas as empresas para filtros. """
    return queries_empresas.get_todas_as_empresas(db)


@router.get("/ranking-comparativo", response_model=List[schemas.RankingEmpresaItem])
def read_ranking_de_empresas(
    data_inicio: date,
    data_fim: date,
    db: Session = Depends(get_db)
):
    """ Retorna um ranking comparativo de todas as empresas. """
    return queries_empresas.get_ranking_empresas(db, data_inicio, data_fim)


@router.get("/{id_empresa}/dashboard", response_model=schemas.EmpresaDashboardResponse)
def read_dashboard_de_empresa(
    id_empresa: int,
    data_inicio: date,
    data_fim: date,
    db: Session = Depends(get_db)
):
    """ Retorna todos os dados para o dashboard de uma empresa individual. """
    dados = queries_empresas.get_dashboard_empresa(db, id_empresa, data_inicio, data_fim)
    if not dados:
        raise HTTPException(status_code=404, detail="Empresa não encontrada ou sem dados no período.")

    response = {
        "estatisticas_detalhadas": [
            {"label": "Nome da Empresa", "value": dados.nome_empresa},
            {"label": "Código", "value": dados.codigo_empresa},
            {"label": "Total de Passageiros", "value": int(dados.total_passageiros or 0)},
            {"label": "Total de Viagens", "value": int(dados.total_viagens or 0)},
            {"label": "Total de Ocorrências", "value": int(dados.total_ocorrencias or 0)},
            {"label": "Linhas Atendidas", "value": int(dados.total_linhas or 0)},
            {"label": "Média de Passag. por Mês", "value": int(dados.media_pass_mes or 0)},
            {"label": "Média de Passag. por Dia", "value": int(dados.media_pass_dia or 0)},
            {"label": "Média de Passag. por Viagem", "value": int(dados.media_pass_viagem or 0)},
        ],
        "grafico_justificativas": dados.grafico_justificativas or [],
        "grafico_linhas_mais_utilizadas": dados.grafico_linhas_mais_utilizadas or [],
        "grafico_media_passageiros_dia_semana": dados.grafico_media_passageiros_dia_semana or [],
        "grafico_evolucao_passageiros_ano": dados.grafico_evolucao_passageiros_ano or [],
    }
    return response
