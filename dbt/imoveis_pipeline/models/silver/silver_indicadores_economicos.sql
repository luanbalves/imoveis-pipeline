/*
  SILVER — Indicadores econômicos consolidados
  
  Transforma as séries brutas em uma tabela analítica com:
  - Variação mensal calculada (lag)
  - Acumulado no ano (YTD)
  - Classificação de tendência (alta/baixa/estável)
  - Dados de inflação relevantes para mercado imobiliário
*/

WITH base AS (
    SELECT * FROM {{ ref('bronze_bcb_series') }}
    WHERE data_ref IS NOT NULL
),

com_variacao AS (
    SELECT
        *,
        -- Variação em relação ao mês anterior da mesma série
        LAG(valor) OVER (
            PARTITION BY serie_nome
            ORDER BY data_ref
        ) AS valor_mes_anterior,

        -- Acumulado no ano até o mês atual
        SUM(valor) OVER (
            PARTITION BY serie_nome, ano
            ORDER BY data_ref
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS acumulado_ano

    FROM base
),

com_tendencia AS (
    SELECT
        *,
        ROUND(valor - valor_mes_anterior, 4)    AS variacao_absoluta,
        ROUND(
            CASE
                WHEN valor_mes_anterior IS NOT NULL AND valor_mes_anterior != 0
                THEN (valor - valor_mes_anterior) / ABS(valor_mes_anterior) * 100
                ELSE NULL
            END
        , 4)                                     AS variacao_pct,

        -- Tendência baseada nos últimos 3 meses
        CASE
            WHEN valor > LAG(valor, 1) OVER (PARTITION BY serie_nome ORDER BY data_ref)
             AND valor > LAG(valor, 2) OVER (PARTITION BY serie_nome ORDER BY data_ref)
            THEN 'alta'
            WHEN valor < LAG(valor, 1) OVER (PARTITION BY serie_nome ORDER BY data_ref)
             AND valor < LAG(valor, 2) OVER (PARTITION BY serie_nome ORDER BY data_ref)
            THEN 'queda'
            ELSE 'estável'
        END                                      AS tendencia_3m,

        -- Flag: inflação acima da meta (4.5% para IPCA)
        CASE
            WHEN serie_nome = 'IPCA' AND acumulado_ano > 4.5
            THEN true ELSE false
        END                                      AS acima_meta_inflacao

    FROM com_variacao
)

SELECT
    serie_codigo,
    serie_nome,
    unidade,
    categoria,
    data_ref,
    ano,
    mes,
    ROUND(valor, 4)             AS valor,
    ROUND(acumulado_ano, 4)     AS acumulado_ano,
    variacao_absoluta,
    variacao_pct,
    tendencia_3m,
    acima_meta_inflacao,
    CURRENT_TIMESTAMP           AS processado_em
FROM com_tendencia