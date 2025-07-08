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


class EficienciaLinha(BaseModel):
    id_linha: int
    cod_linha: str
    nome_linha: Optional[str] = None
    passageiros_por_km: float
    passageiros_por_minuto: float
    total_passageiros: int


class TaxaFalhasEmpresa(BaseModel):
    id_empresa: int
    nome_empresa: str
    taxa_falhas_por_10k_viagens: float


class FalhaPorJustificativa(BaseModel):
    nome_justificativa: str
    total_falhas: int


class CorrelacaoIdadeFalha(BaseModel):
    id_veiculo: int
    idade_veiculo_anos: int
    total_falhas: int
    nome_empresa: str


class RankingLinhasFalhas(BaseModel):
    id_linha: int
    cod_linha: str
    nome_linha: Optional[str] = None
    total_falhas: int


class LinhaParaFiltro(BaseModel):
    id_linha: int
    cod_linha: str
    nome_linha: Optional[str] = None

    class Config:
        from_attributes = True
