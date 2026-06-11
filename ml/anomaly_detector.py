"""
Detecção de Anomalias — Isolation Forest

Detecta comportamentos atípicos nas séries econômicas do BCB.
Serve para identificar:
- Variações bruscas de IPCA, SELIC, IGP-M
- Pontos fora do padrão histórico
- Possíveis erros de coleta ou eventos extraordinários

O modelo é treinado por série (uma instância por indicador),
permitindo que cada série tenha seu próprio threshold de normalidade.
"""

import duckdb
import pandas as pd
import numpy as np
import joblib
import json
import os
from pathlib import Path
from datetime import datetime
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

DB_PATH = str(Path(__file__).parent.parent / "data" / "imoveis_pipeline.duckdb")
MODELS_DIR  = Path(__file__).parent / "models"
OUTPUT_DIR  = Path(__file__).parent / "output"

MODELS_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)


def carregar_dados(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Carrega os indicadores econômicos da camada Silver.
    Só usa séries com histórico suficiente para treinar (mínimo 24 meses).
    """
    return conn.execute("""
        SELECT
            serie_nome,
            serie_codigo,
            categoria,
            data_ref,
            ano,
            mes,
            valor,
            acumulado_ano,
            variacao_pct,
            variacao_absoluta,
            tendencia_3m
        FROM silver_indicadores_economicos
        WHERE valor IS NOT NULL
          AND variacao_pct IS NOT NULL
        ORDER BY serie_nome, data_ref
    """).df()


def preparar_features(df_serie: pd.DataFrame) -> np.ndarray:
    """
    Prepara as features para o modelo.
    
    Usamos valor, variação e acumulado, capturando tanto o
    nível absoluto quanto o comportamento relativo da série.
    """
    features = df_serie[[
        "valor",
        "variacao_pct",
        "variacao_absoluta",
        "acumulado_ano",
    ]].fillna(0).values

    # StandardScaler normaliza as features para média 0 e desvio 1
    # Importante porque valor e variacao_pct têm escalas muito diferentes
    scaler = StandardScaler()
    return scaler.fit_transform(features), scaler


def treinar_modelo(features: np.ndarray, serie_nome: str) -> IsolationForest:
    """
    Treina o Isolation Forest para uma série específica.
    
    contamination=0.05 significa que esperamos ~5% fixo de anomalias (para todos).
    eventos extremos são raros mas existem (crises, choques, mudanças de política monetária).
    """
    modelo = IsolationForest(
        n_estimators=100,      # número de árvores mais estável
        contamination=0.05,    # proporção esperada de anomalias
        random_state=42,       # reprodutibilidade
        n_jobs=-1,             # usa todos os cores disponíveis
    )
    modelo.fit(features)

    # Salva o modelo treinado em disco
    caminho = MODELS_DIR / f"isolation_forest_{serie_nome.lower()}.pkl"
    joblib.dump(modelo, caminho)
    print(f"  💾 Modelo salvo: {caminho.name}")

    return modelo


def detectar_anomalias(
    df_serie: pd.DataFrame,
    modelo: IsolationForest,
    features: np.ndarray,
    serie_nome: str
) -> pd.DataFrame:
    """
    Aplica o modelo e retorna os registros com scores de anomalia.
    
    O Isolation Forest retorna:
    - predict(): 1 = normal, -1 = anomalia
    - score_samples(): score de anomalia (quanto mais negativo = mais anômalo)
    """
    predicoes    = modelo.predict(features)
    scores       = modelo.score_samples(features)

    resultado = df_serie.copy()
    resultado["anomalia"]       = predicoes == -1
    resultado["anomalia_score"] = np.round(scores, 4)

    # Normaliza o score para 0-100 (mais fácil de interpretar)
    score_min = scores.min()
    score_max = scores.max()
    resultado["anomalia_score_norm"] = np.round(
        (scores - score_min) / (score_max - score_min) * 100, 1
    )

    # Classifica a severidade
    resultado["severidade"] = pd.cut(
        resultado["anomalia_score_norm"],
        bins=[0, 20, 40, 100],
        labels=["alta", "média", "baixa"],
        include_lowest=True
    ).astype(str)

    # Só severidade alta/média para quem for anomalia de verdade
    resultado.loc[~resultado["anomalia"], "severidade"] = "normal"

    anomalias = resultado[resultado["anomalia"]].copy()
    print(f"  🔍 {serie_nome}: {len(anomalias)} anomalias detectadas "
          f"de {len(df_serie)} registros "
          f"({len(anomalias)/len(df_serie)*100:.1f}%)")

    return resultado


def salvar_resultados(
    conn: duckdb.DuckDBPyConnection,
    df_resultados: pd.DataFrame
) -> None:
    """
    Persiste os resultados no DuckDB como tabela analítica.
    Essa tabela será consumida pela API.
    """
    conn.execute("DROP TABLE IF EXISTS ml_anomalias")
    conn.execute("""
        CREATE TABLE ml_anomalias AS
        SELECT
            serie_nome,
            serie_codigo,
            categoria,
            data_ref,
            ano,
            mes,
            valor,
            variacao_pct,
            acumulado_ano,
            anomalia,
            anomalia_score,
            anomalia_score_norm,
            severidade,
            CURRENT_TIMESTAMP AS detectado_em
        FROM df_resultados
    """)

    total     = len(df_resultados)
    anomalias = df_resultados["anomalia"].sum()
    print(f"\n✅ Resultados salvos: {total} registros, {anomalias} anomalias")


def gerar_relatorio(df_resultados: pd.DataFrame) -> dict:
    """Gera um resumo JSON dos resultados para auditoria."""
    anomalias = df_resultados[df_resultados["anomalia"]]

    relatorio = {
        "gerado_em":       datetime.now().isoformat(),
        "total_registros": len(df_resultados),
        "total_anomalias": int(anomalias["anomalia"].sum()),
        "pct_anomalias":   round(len(anomalias) / len(df_resultados) * 100, 2),
        "por_serie": {}
    }

    for serie in df_resultados["serie_nome"].unique():
        df_s   = df_resultados[df_resultados["serie_nome"] == serie]
        anom_s = df_s[df_s["anomalia"]]

        relatorio["por_serie"][serie] = {
            "total":     len(df_s),
            "anomalias": int(len(anom_s)),
            "pct":       round(len(anom_s) / len(df_s) * 100, 2),
            "top_anomalias": anom_s.nsmallest(3, "anomalia_score")[[
                "data_ref", "valor", "variacao_pct", "severidade"
            ]].to_dict("records")
        }

    caminho = OUTPUT_DIR / "relatorio_anomalias.json"
    with open(caminho, "w", encoding="utf-8") as f:
        json.dump(relatorio, f, ensure_ascii=False, indent=2, default=str)

    print(f"📄 Relatório salvo: {caminho}")
    return relatorio


def executar() -> None:
    """Pipeline completo de detecção de anomalias."""
    print("\n" + "="*60)
    print("DETECÇÃO DE ANOMALIAS — Isolation Forest")
    print("="*60)

    conn = duckdb.connect(DB_PATH)
    df   = carregar_dados(conn)

    series_disponiveis = df["serie_nome"].unique()
    print(f"\n📊 Séries disponíveis: {list(series_disponiveis)}")

    todos_resultados = []

    for serie_nome in series_disponiveis:
        print(f"\n🔄 Processando: {serie_nome}")
        df_serie = df[df["serie_nome"] == serie_nome].copy()

        # Precisa de pelo menos 24 pontos para treinar com qualidade
        if len(df_serie) < 24:
            print(f"  ⚠️  Poucos dados ({len(df_serie)} pontos) — pulando")
            continue

        features, scaler = preparar_features(df_serie)
        modelo            = treinar_modelo(features, serie_nome)
        df_resultado      = detectar_anomalias(df_serie, modelo, features, serie_nome)
        todos_resultados.append(df_resultado)

    if not todos_resultados:
        print("\n⚠️  Nenhuma série com dados suficientes para treinar")
        return

    df_final = pd.concat(todos_resultados, ignore_index=True)
    salvar_resultados(conn, df_final)
    relatorio = gerar_relatorio(df_final)

    print(f"\n{'='*60}")
    print(f"RESUMO FINAL")
    print(f"{'='*60}")
    print(f"Total de registros analisados: {relatorio['total_registros']}")
    print(f"Anomalias detectadas:          {relatorio['total_anomalias']}")
    print(f"Percentual:                    {relatorio['pct_anomalias']}%")
    print(f"\nTop anomalias por série:")

    for serie, dados in relatorio["por_serie"].items():
        if dados["anomalias"] > 0:
            print(f"\n  {serie}: {dados['anomalias']} anomalia(s)")
            for a in dados["top_anomalias"]:
                print(f"    → {a['data_ref']} | valor: {a['valor']} "
                      f"| variação: {a['variacao_pct']}% | {a['severidade']}")

    conn.close()


if __name__ == "__main__":
    executar()