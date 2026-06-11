"""
Pipeline de Inteligência Imobiliária — API REST

Expõe os dados processados pelo pipeline para consumo externo.
Documentação automática disponível em /docs (Swagger UI).
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .routers import (
    router_saude,
    router_indicadores,
    router_panorama,
    router_municipios,
    router_anomalias,
)

app = FastAPI(
    title="Pipeline de Inteligência Imobiliária",
    description="""
API REST que expõe dados processados pelo pipeline de inteligência
de mercado imobiliário brasileiro.

## Fontes de dados
- **BCB**: séries econômicas (IPCA, SELIC, IGP-M, crédito imobiliário)
- **IBGE**: dados municipais (população, região, porte)

## Capacidades
- Séries históricas desde 2015
- Detecção automática de anomalias (Isolation Forest)
- Ranking de municípios por atratividade imobiliária
- Interpretação automática do ambiente econômico
    """,
    version="1.0.0",
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# Registra os routers
app.include_router(router_saude)
app.include_router(router_indicadores)
app.include_router(router_panorama)
app.include_router(router_municipios)
app.include_router(router_anomalias)


@app.get("/", include_in_schema=False)
def root():
    return {
        "api": "Pipeline de Inteligência Imobiliária",
        "versao": "1.0.0",
        "docs": "/docs",
        "saude": "/saude",
    }