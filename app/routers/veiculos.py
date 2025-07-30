from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from datetime import date
from typing import List
from enum import Enum

from app import schemas
from app.queries import veiculos as queries_veiculos
from app.database import get_db

router = APIRouter(
    prefix="/api/v1/veiculos",
    tags=["Veículos"]
)


class MetricaRankingVeiculo(str, Enum):
    passageiros = "passageiros"
    ocorrencias = "ocorrencias"
    km_percorrido = "km_percorrido"


@router.get("/", response_model=List[schemas.VeiculoParaFiltro])
def read_veiculos_para_filtro(db: Session = Depends(get_db)):
    """ Retorna uma lista de todos os veículos para filtros. """
    return queries_veiculos.get_todos_os_veiculos(db)


@router.get("/ranking/{metrica}", response_model=List[schemas.RankingVeiculoItem])
def read_ranking_de_veiculos(
    metrica: MetricaRankingVeiculo,
    data_inicio: date,
    data_fim: date,
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db)
):
    """ Retorna um ranking de veículos por uma métrica específica. """
    return queries_veiculos.get_ranking_veiculos(db, metrica.value, data_inicio, data_fim, limit)


@router.get("/{id_veiculo}/dashboard", response_model=schemas.VeiculoDashboardResponse)
def read_dashboard_de_veiculo(
    id_veiculo: int,
    data_inicio: date,
    data_fim: date,
    db: Session = Depends(get_db)
):
    """ Retorna todos os dados para o dashboard de um veículo individual. """
    dados = queries_veiculos.get_dashboard_veiculo(db, id_veiculo, data_inicio, data_fim)
    if not dados:
        raise HTTPException(status_code=404, detail="Veículo não encontrado ou sem dados no período.")

    idade_str = f"{dados.idade_veiculo_anos or 0} anos e {dados.meses_adicionais or 0} meses"

    response = {
        "estatisticas_detalhadas": [
            {"label": "Identificação do Veículo", "value": dados.identificador_veiculo},
            {"label": "Idade do Veículo", "value": idade_str},
            {"label": "Em Operação", "value": "Sim" if dados.em_operacao else "Não"},
            {"label": "Total de Passageiros", "value": int(dados.total_passageiros or 0)},
            {"label": "Total de Viagens", "value": int(dados.total_viagens or 0)},
            {"label": "Total de Ocorrências", "value": int(dados.total_ocorrencias or 0)},
            {"label": "Extensão Total Percorrida", "value": f"{dados.total_extensao_km or 0:.2f} km"},
            {"label": "Média de Passag. por Mês", "value": int(dados.media_pass_mes or 0)},
            {"label": "Média de Passag. por Dia", "value": int(dados.media_pass_dia or 0)},
            {"label": "Média de Passag. por Viagem", "value": int(dados.media_pass_viagem or 0)},
        ],
        "grafico_justificativas": dados.grafico_justificativas or [],
        "grafico_linhas_atendidas": dados.grafico_linhas_atendidas or [],
        "grafico_media_passageiros_dia_semana": dados.grafico_media_passageiros_dia_semana or []
    }
    return response
