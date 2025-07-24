from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from datetime import date
from typing import List
from enum import Enum

from app import schemas
from app.database import get_db
from app.queries.linhas import (
    get_ranking_linhas,
    get_contagem_linhas_por_concessionaria,
    get_contagem_linhas_por_empresa,
    get_contagem_pontos_por_linha,
    get_contagem_linhas_por_bairro,
    get_pontos_geometria_linha,
    get_todas_as_linhas,
    get_dashboard_linha,
)

router = APIRouter(prefix="/api/v1/linhas", tags=["Linhas e Pontos"])


class MetricaRanking(str, Enum):
    passageiros = "passageiros"
    viagens = "viagens"
    ocorrencias = "ocorrencias"


@router.get("/", response_model=List[schemas.LinhaParaFiltro])
def read_todas_as_linhas_para_filtro(db: Session = Depends(get_db)):
    """
    Retorna uma lista de todas as linhas de ônibus disponíveis para
    serem usadas em filtros de dropdown.
    """
    linhas = get_todas_as_linhas(db)
    return linhas


@router.get("/ranking/{metrica}", response_model=schemas.RankingResponse)
def read_ranking_de_linhas(
    metrica: MetricaRanking,
    data_inicio: date,
    data_fim: date,
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db),
):
    """
    Retorna um ranking das linhas por uma métrica específica (passageiros, viagens ou ocorrências)
    para um determinado período.
    """
    try:
        ranking_data = get_ranking_linhas(
            db, metrica.value, data_inicio, data_fim, limit
        )
        return {"metrica": metrica.value, "ranking": ranking_data}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get(
    "/contagem-por-concessionaria", response_model=List[schemas.ContagemPorEntidadeItem]
)
def read_contagem_linhas_concessionaria(db: Session = Depends(get_db)):
    """
    Retorna a quantidade de linhas distintas por concessionária.
    """
    return get_contagem_linhas_por_concessionaria(db)


@router.get(
    "/contagem-por-empresa", response_model=List[schemas.ContagemPorEntidadeItem]
)
def read_contagem_linhas_empresa(db: Session = Depends(get_db)):
    """
    Retorna a quantidade de linhas distintas por empresa operadora.
    """
    return get_contagem_linhas_por_empresa(db)


@router.get("/contagem-pontos", response_model=schemas.RankingResponse)
def read_contagem_pontos_por_linha(
    limit: int = Query(10, ge=1, le=50), db: Session = Depends(get_db)
):
    """
    Retorna a quantidade de pontos de parada distintos por linha.
    """
    ranking_data = get_contagem_pontos_por_linha(db, limit)
    return {"metrica": "pontos_por_linha", "ranking": ranking_data}


@router.get("/contagem-por-bairro", response_model=schemas.RankingResponse)
def read_contagem_linhas_por_bairro(
    limit: int = Query(10, ge=1, le=50), db: Session = Depends(get_db)
):
    """
    Retorna a quantidade de linhas que atendem cada bairro.
    """
    ranking_data = get_contagem_linhas_por_bairro(db, limit)
    # Reutilizando o schema RankingItem, onde 'codigo' e 'nome' se referem ao bairro
    # e 'valor' é a contagem de linhas.
    # A resposta é formatada para ser consistente
    bairro_ranking = [
        {"id": item.id, "codigo": str(item.id), "nome": item.nome, "valor": item.valor}
        for item in ranking_data
    ]
    return {"metrica": "linhas_por_bairro", "ranking": bairro_ranking}


@router.get(
    "/{cod_linha}/pontos/geolocalizacao",
    response_model=schemas.GeoJSONFeatureCollection,
)
def read_geolocalizacao_dos_pontos_da_linha(
    cod_linha: str, db: Session = Depends(get_db)
):
    """
    Retorna uma coleção de Features GeoJSON, onde cada feature é um ponto de parada
    de uma linha específica.
    """
    # Busca a lista de pontos (identificador, longitude, latitude) para a linha
    pontos = get_pontos_geometria_linha(db, cod_linha)

    if not pontos:
        raise HTTPException(
            status_code=404, detail="Nenhum ponto de ônibus encontrado para esta linha."
        )

    # Constrói a lista de "features", uma para cada ponto de ônibus
    features = []
    for ponto in pontos:
        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [ponto.longitude, ponto.latitude],
            },
            "properties": {"identificador_ponto": ponto.identificador_ponto_onibus},
        }
        features.append(feature)

    # Monta o objeto final GeoJSON FeatureCollection
    feature_collection = {"type": "FeatureCollection", "features": features}

    return feature_collection


@router.get("/{id_linha}/dashboard", response_model=schemas.LinhaDashboardResponse)
def read_dashboard_de_linha(
    id_linha: int,
    data_inicio: date,
    data_fim: date,
    db: Session = Depends(get_db)
):
    """
    Retorna um objeto completo com todas as estatísticas e dados de gráficos
    para a página de análise de uma linha individual.
    """
    dados_dashboard = get_dashboard_linha(db, id_linha, data_inicio, data_fim)

    if not dados_dashboard:
        raise HTTPException(status_code=404, detail="Dados não encontrados para a linha no período especificado.")

    # Formata a saída no schema esperado
    response = {
        "estatisticas_detalhadas": [
            {"label": "Empresa", "value": dados_dashboard.empresa},
            {"label": "Concessionária", "value": dados_dashboard.concessionaria},
            {"label": "Viagens Realizadas", "value": int(dados_dashboard.viagens_realizadas or 0)},
            {"label": "Viagens Não Realizadas", "value": int(dados_dashboard.viagens_nao_realizadas or 0)},
            {"label": "Viagens Interrompidas", "value": int(dados_dashboard.viagens_interrompidas or 0)},
            {"label": "Viagens com Zero Passageiros", "value": int(dados_dashboard.viagens_zero_passageiros or 0)},
            {"label": "Extensão", "value": f"{dados_dashboard.extensao_linha or 0:.2f} km"},
            {"label": "Bairros Percorridos", "value": int(dados_dashboard.bairros_percorridos or 0)},
            {"label": "Pontos de Ônibus", "value": int(dados_dashboard.pontos_onibus or 0)},
            {"label": "Média de Passag. por Mês", "value": int(dados_dashboard.media_pass_mes or 0)},
            {"label": "Média de Passag. por Dia", "value": int(dados_dashboard.media_pass_dia or 0)},
            {"label": "Média de Passag. por Viagem", "value": int(dados_dashboard.media_pass_viagem or 0)},
        ],
        "grafico_justificativas": dados_dashboard.grafico_justificativas or [],
        "grafico_media_passageiros_dia_semana": dados_dashboard.grafico_media_passageiros_dia_semana or [],
        "mapa_pontos": dados_dashboard.mapa_pontos or {"type": "FeatureCollection", "features": []},
        "mapa_bairros": dados_dashboard.mapa_bairros or {"type": "FeatureCollection", "features": []}
    }
    return response
