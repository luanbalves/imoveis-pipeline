"""
Routers organiza os endpoints por domínio.
"""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from datetime import datetime
from .database import get_db
from .schemas import (
    IndicadorResumo, IndicadorPonto, MunicipioRanking,
    AnomaliaItem, AnomaliaResumo, SaudeResponse
)

# ── Saúde ──────────────────────────────────────────────────────────────────────
router_saude = APIRouter(tags=["saúde"])

@router_saude.get("/saude", response_model=SaudeResponse)
def verificar_saude():
    """Verifica se a API e o banco estão operacionais."""
    db_ok = False
    try:
        with get_db() as conn:
            conn.execute("SELECT 1").fetchone()
            db_ok = True
    except Exception:
        pass

    return SaudeResponse(
        status="ok" if db_ok else "off",
        versao="1.0.0",
        timestamp=datetime.now(),
        db_conectado=db_ok,
    )


# ── Indicadores ────────────────────────────────────────────────────────────────
router_indicadores = APIRouter(prefix="/indicadores", tags=["indicadores"])

@router_indicadores.get("", response_model=list[IndicadorResumo])
def listar_indicadores():
    """Lista todos os indicadores econômicos com seu último valor."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT
                serie_nome, serie_codigo, categoria, unidade,
                ultima_data, ultimo_valor, acumulado_ano,
                tendencia_3m, interpretacao_mercado
            FROM gold_panorama_economico
            ORDER BY categoria, serie_nome
        """).fetchall()

        colunas = [
            "serie_nome", "serie_codigo", "categoria", "unidade",
            "ultima_data", "ultimo_valor", "acumulado_ano",
            "tendencia_3m", "interpretacao_mercado"
        ]
        return [dict(zip(colunas, row)) for row in rows]


@router_indicadores.get("/{serie_nome}", response_model=list[IndicadorPonto])
def serie_historica(
    serie_nome: str,
    ano_inicio: Optional[int] = Query(None, description="Ano inicial do filtro"),
    ano_fim:    Optional[int] = Query(None, description="Ano final do filtro"),
):
    """
    Retorna a série histórica completa de um indicador.
    
    Exemplos de serie_nome: IPCA, SELIC, IGP-M, INPC
    """
    with get_db() as conn:
        # Verifica se a série existe
        existe = conn.execute("""
            SELECT COUNT(*) FROM silver_indicadores_economicos
            WHERE UPPER(serie_nome) = UPPER(?)
        """, [serie_nome]).fetchone()[0]

        if not existe:
            raise HTTPException(
                status_code=404,
                detail=f"Série '{serie_nome}' não encontrada. "
                       f"Use GET /indicadores para ver as disponíveis."
            )

        query = """
            SELECT
                data_ref, ano, mes, valor, acumulado_ano,
                variacao_pct, variacao_absoluta, tendencia_3m
            FROM silver_indicadores_economicos
            WHERE UPPER(serie_nome) = UPPER(?)
        """
        params = [serie_nome]

        if ano_inicio:
            query += " AND ano >= ?"
            params.append(ano_inicio)
        if ano_fim:
            query += " AND ano <= ?"
            params.append(ano_fim)

        query += " ORDER BY data_ref"

        rows = conn.execute(query, params).fetchall()
        colunas = [
            "data_ref", "ano", "mes", "valor", "acumulado_ano",
            "variacao_pct", "variacao_absoluta", "tendencia_3m"
        ]
        return [dict(zip(colunas, row)) for row in rows]


@router_indicadores.get("/{serie_nome}/ultimo", response_model=IndicadorPonto)
def ultimo_valor(serie_nome: str):
    """Retorna o último valor disponível de um indicador."""
    with get_db() as conn:
        row = conn.execute("""
            SELECT
                data_ref, ano, mes, valor, acumulado_ano,
                variacao_pct, variacao_absoluta, tendencia_3m
            FROM silver_indicadores_economicos
            WHERE UPPER(serie_nome) = UPPER(?)
            ORDER BY data_ref DESC
            LIMIT 1
        """, [serie_nome]).fetchone()

        if not row:
            raise HTTPException(status_code=404, detail=f"Série '{serie_nome}' não encontrada")

        colunas = [
            "data_ref", "ano", "mes", "valor", "acumulado_ano",
            "variacao_pct", "variacao_absoluta", "tendencia_3m"
        ]
        return dict(zip(colunas, row))


# ── Panorama ───────────────────────────────────────────────────────────────────
router_panorama = APIRouter(prefix="/panorama", tags=["panorama"])

@router_panorama.get("", response_model=list[IndicadorResumo])
def panorama_mercado():
    """
    Visão do ambiente econômico para mercado imobiliário.
    Inclui interpretação automática de cada indicador.
    """
    with get_db() as conn:
        rows = conn.execute("""
            SELECT
                serie_nome, serie_codigo, categoria, unidade,
                ultima_data, ultimo_valor, acumulado_ano,
                tendencia_3m, interpretacao_mercado
            FROM gold_panorama_economico
            ORDER BY categoria, serie_nome
        """).fetchall()

        colunas = [
            "serie_nome", "serie_codigo", "categoria", "unidade",
            "ultima_data", "ultimo_valor", "acumulado_ano",
            "tendencia_3m", "interpretacao_mercado"
        ]
        return [dict(zip(colunas, row)) for row in rows]


# ── Municípios ─────────────────────────────────────────────────────────────────
router_municipios = APIRouter(prefix="/municipios", tags=["municípios"])

@router_municipios.get("", response_model=list[MunicipioRanking])
def ranking_municipios(
    regiao:  Optional[str] = Query(None, description="Filtra por região (ex: Sudeste)"),
    uf:      Optional[str] = Query(None, description="Filtra por UF (ex: SP)"),
    limite:  int           = Query(20, le=100, description="Máximo de resultados"),
):
    """Ranking de municípios por atratividade para investimento imobiliário."""
    with get_db() as conn:
        query = """
            SELECT
                codigo_ibge, nome, uf, regiao, populacao,
                porte_municipio, perfil_mercado,
                score_atratividade, ranking_geral, ranking_na_regiao
            FROM gold_ranking_municipios
            WHERE 1=1
        """
        params = []

        if regiao:
            query += " AND UPPER(regiao) = UPPER(?)"
            params.append(regiao)
        if uf:
            query += " AND UPPER(uf) = UPPER(?)"
            params.append(uf)

        query += " ORDER BY ranking_geral LIMIT ?"
        params.append(limite)

        rows = conn.execute(query, params).fetchall()
        colunas = [
            "codigo_ibge", "nome", "uf", "regiao", "populacao",
            "porte_municipio", "perfil_mercado",
            "score_atratividade", "ranking_geral", "ranking_na_regiao"
        ]
        return [dict(zip(colunas, row)) for row in rows]


# ── Anomalias ──────────────────────────────────────────────────────────────────
router_anomalias = APIRouter(prefix="/anomalias", tags=["anomalias"])

@router_anomalias.get("", response_model=list[AnomaliaItem])
def listar_anomalias(
    serie:      Optional[str] = Query(None, description="Filtra por série"),
    severidade: Optional[str] = Query(None, description="alta, média ou baixa"),
    limite:     int           = Query(50, le=200),
):
    """Lista anomalias detectadas pelo modelo de ML."""
    with get_db() as conn:
        # Verifica se a tabela existe
        existe = conn.execute("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_name = 'ml_anomalias'
        """).fetchone()[0]

        if not existe:
            raise HTTPException(
                status_code=503,
                detail="Modelo ML ainda não executado. "
                       "Rode ml/anomaly_detector.py primeiro."
            )

        query = """
            SELECT
                serie_nome, data_ref, valor, variacao_pct,
                anomalia_score, anomalia_score_norm, severidade
            FROM ml_anomalias
            WHERE anomalia = true
        """
        params = []

        if serie:
            query += " AND UPPER(serie_nome) = UPPER(?)"
            params.append(serie)
        if severidade:
            query += " AND severidade = ?"
            params.append(severidade)

        query += " ORDER BY anomalia_score ASC LIMIT ?"
        params.append(limite)

        rows = conn.execute(query, params).fetchall()
        colunas = [
            "serie_nome", "data_ref", "valor", "variacao_pct",
            "anomalia_score", "anomalia_score_norm", "severidade"
        ]
        return [dict(zip(colunas, row)) for row in rows]


@router_anomalias.get("/resumo", response_model=list[AnomaliaResumo])
def resumo_anomalias():
    """Resumo agregado de anomalias por série."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT
                serie_nome,
                COUNT(*)                            AS total,
                SUM(anomalia::INT)                  AS anomalias,
                ROUND(
                    SUM(anomalia::INT) * 100.0 / COUNT(*), 2
                )                                   AS pct_anomalias
            FROM ml_anomalias
            GROUP BY serie_nome
            ORDER BY anomalias DESC
        """).fetchall()

        colunas = ["serie_nome", "total", "anomalias", "pct_anomalias"]
        return [dict(zip(colunas, row)) for row in rows]