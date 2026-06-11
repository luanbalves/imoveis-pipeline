"""
Schemas Pydantic definem a estrutura das respostas da API.

valida automaticamente os tipos e serializa para JSON.
"""

from pydantic import BaseModel
from typing import Optional
from datetime import date, datetime


class IndicadorResumo(BaseModel):
    serie_nome:       str
    serie_codigo:     str
    categoria:        str
    unidade:          str
    ultima_data:      Optional[date]
    ultimo_valor:     Optional[float]
    acumulado_ano:    Optional[float]
    tendencia_3m:     Optional[str]
    interpretacao_mercado: Optional[str]


class IndicadorPonto(BaseModel):
    data_ref:          Optional[date]
    ano:               Optional[int]
    mes:               Optional[int]
    valor:             Optional[float]
    acumulado_ano:     Optional[float]
    variacao_pct:      Optional[float]
    variacao_absoluta: Optional[float]
    tendencia_3m:      Optional[str]


class MunicipioRanking(BaseModel):
    codigo_ibge:       str
    nome:              str
    uf:                str
    regiao:            str
    populacao:         Optional[float]
    porte_municipio:   Optional[str]
    perfil_mercado:    Optional[str]
    score_atratividade: Optional[float]
    ranking_geral:     Optional[int]
    ranking_na_regiao: Optional[int]


class AnomaliaItem(BaseModel):
    serie_nome:          str
    data_ref:            Optional[date]
    valor:               Optional[float]
    variacao_pct:        Optional[float]
    anomalia_score:      Optional[float]
    anomalia_score_norm: Optional[float]
    severidade:          Optional[str]


class AnomaliaResumo(BaseModel):
    serie_nome:    str
    total:         int
    anomalias:     int
    pct_anomalias: float


class SaudeResponse(BaseModel):
    status:      str
    versao:      str
    timestamp:   datetime
    db_conectado: bool