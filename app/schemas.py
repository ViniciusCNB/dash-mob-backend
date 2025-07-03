from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any


# Exemplo de schema para os KPIs da Visão Geral
class KpiGeral(BaseModel):
    total_passageiros: int
    total_viagens: int
    total_ocorrencias: int
    eficiencia_passageiro_km: float


# Schema para um item do ranking (usado para passageiros, viagens, etc.)
class RankingItem(BaseModel):
    id: int
    codigo: str
    nome: Optional[str] = None
    valor: int


# Schema para a resposta dos rankings (lista de itens)
class RankingResponse(BaseModel):
    metrica: str
    ranking: List[RankingItem]


# Schema para contagem por entidade (usado para concessionária e empresa)
class ContagemPorEntidadeItem(BaseModel):
    id: int
    nome: str
    quantidade_linhas: int


# Schema para uma Feature GeoJSON, agora aceitando qualquer tipo de geometria
class GeoJSONFeature(BaseModel):
    type: str = "Feature"
    geometry: Dict[str, Any]  # Permite Point, LineString, etc.
    properties: Dict[str, Any] = Field(default_factory=dict)


# Schema para uma coleção de Features (o que vamos retornar)
class GeoJSONFeatureCollection(BaseModel):
    type: str = "FeatureCollection"
    features: List[GeoJSONFeature]
