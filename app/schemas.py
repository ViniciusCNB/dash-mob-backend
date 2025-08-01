from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any, Union
from datetime import date


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


class StatItem(BaseModel):
    label: str
    value: Union[int, float, str]


class ChartDataItem(BaseModel):
    category: str
    value: float


class LinhaDashboardResponse(BaseModel):
    estatisticas_detalhadas: List[StatItem]
    grafico_justificativas: List[ChartDataItem]
    grafico_media_passageiros_dia_semana: List[ChartDataItem]
    mapa_pontos: GeoJSONFeatureCollection
    mapa_bairros: GeoJSONFeatureCollection


# Schema genérico para rankings (reutilizável)
class RankingOcorrenciasItem(BaseModel):
    id: int
    nome: str
    total_ocorrencias: int


# Schema para a análise de tendência temporal
class TendenciaTemporalItem(BaseModel):
    periodo: date  # Usaremos o primeiro dia do mês como referência
    total_ocorrencias: int


# Schema para a análise de ocorrências por tipo de dia
class OcorrenciasPorTipoDiaItem(BaseModel):
    tipo_dia: str
    total_ocorrencias: int


# Schema para um item de ranking (pode ser usado para linhas ou veículos)
class RankingOcorrenciaDetalheItem(BaseModel):
    id: int
    codigo: str  # Para cod_linha ou identificador_veiculo
    valor: int  # Total de ocorrências


# O modelo de resposta completo para a página de detalhes da ocorrência
class JustificativaDashboardResponse(BaseModel):
    estatisticas_detalhadas: List[StatItem]
    grafico_linhas_afetadas: List[RankingOcorrenciaDetalheItem]
    grafico_veiculos_afetados: List[RankingOcorrenciaDetalheItem]
    grafico_media_ocorrencias_dia_semana: List[ChartDataItem]
    id_linha_mais_afetada: Optional[int] = None  # Para o front-end buscar o mapa


# Schema para um bairro na lista de filtros
class BairroParaFiltro(BaseModel):
    id_bairro: int
    nome_bairro: str


# Schema para a resposta do dashboard de um bairro individual, agora com dados do mapa
class BairroDashboardResponse(BaseModel):
    estatisticas_detalhadas: List[StatItem]
    grafico_linhas_mais_utilizadas: List[RankingItem]
    grafico_media_passageiros_dia_semana: List[ChartDataItem]
    mapa_geometria_bairro: GeoJSONFeatureCollection
    mapa_pontos_bairro: GeoJSONFeatureCollection


# Schema para uma concessionária na lista de filtros
class ConcessionariaParaFiltro(BaseModel):
    id_concessionaria: int
    codigo_concessionaria: int
    nome_concessionaria: str


# Schema para o ranking comparativo de concessionárias
class RankingConcessionariaItem(BaseModel):
    id_concessionaria: int
    codigo_concessionaria: int
    nome_concessionaria: str
    total_linhas: int
    total_ocorrencias: int
    total_passageiros: int
    taxa_ocorrencias_por_10k_viagens: float


# Schema para a resposta do dashboard de uma concessionária individual
class ConcessionariaDashboardResponse(BaseModel):
    estatisticas_detalhadas: List[StatItem]
    grafico_linhas_mais_utilizadas: List[RankingItem]
    grafico_media_passageiros_dia_semana: List[ChartDataItem]


# Schema para um veículo na lista de filtros
class VeiculoParaFiltro(BaseModel):
    id_veiculo: int
    identificador_veiculo: int


# Schema para o ranking comparativo de veículos
class RankingVeiculoItem(BaseModel):
    id_veiculo: int
    identificador_veiculo: int
    nome_empresa: Optional[str] = None
    idade_veiculo_anos: Optional[int] = None
    valor: Union[int, float]


# Schema para a resposta do dashboard de um veículo individual
class VeiculoDashboardResponse(BaseModel):
    estatisticas_detalhadas: List[StatItem]
    grafico_justificativas: List[ChartDataItem]
    grafico_linhas_atendidas: List[RankingItem]
    grafico_media_passageiros_dia_semana: List[ChartDataItem]


# Schema para uma empresa na lista de filtros
class EmpresaParaFiltro(BaseModel):
    id_empresa: int
    nome_empresa: str


# Schema para o ranking comparativo de empresas
class RankingEmpresaItem(BaseModel):
    id_empresa: int
    nome_empresa: str
    total_linhas: int
    total_ocorrencias: int
    total_passageiros: int
    taxa_ocorrencias_por_10k_viagens: float


# Schema para a resposta do dashboard de uma empresa individual
class EmpresaDashboardResponse(BaseModel):
    estatisticas_detalhadas: List[StatItem]
    grafico_justificativas: List[ChartDataItem]
    grafico_linhas_mais_utilizadas: List[RankingItem]
    grafico_media_passageiros_dia_semana: List[ChartDataItem]
    grafico_evolucao_passageiros_ano: List[ChartDataItem]
