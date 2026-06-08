/*
  GOLD — Panorama econômico para mercado imobiliário
  
  Visão consolidada dos principais indicadores por período,
  pronta para consumo em dashboards e pela API que usaremos.
  
  Responde: "Como está o ambiente econômico para compra/venda
  de imóveis neste momento?"
*/

WITH indicadores AS (
    SELECT * FROM {{ ref('silver_indicadores_economicos') }}
),

-- Último valor disponível de cada indicador
ultimo_valor AS (
    SELECT DISTINCT ON (serie_nome)
        serie_nome,
        serie_codigo,
        categoria,
        unidade,
        data_ref         AS ultima_data,
        valor            AS ultimo_valor,
        acumulado_ano,
        tendencia_3m,
        variacao_pct     AS variacao_ultimo_mes
    FROM indicadores
    ORDER BY serie_nome, data_ref DESC
),

-- Resumo anual de cada indicador
resumo_anual AS (
    SELECT
        serie_nome,
        ano,
        ROUND(AVG(valor), 4)    AS media_anual,
        ROUND(MAX(valor), 4)    AS maxima_anual,
        ROUND(MIN(valor), 4)    AS minima_anual,
        COUNT(*)                AS meses_com_dados
    FROM indicadores
    GROUP BY serie_nome, ano
)

SELECT
    u.serie_nome,
    u.serie_codigo,
    u.categoria,
    u.unidade,
    u.ultima_data,
    u.ultimo_valor,
    u.acumulado_ano,
    u.tendencia_3m,
    u.variacao_ultimo_mes,

    -- Dados do ano corrente
    r.media_anual,
    r.maxima_anual,
    r.minima_anual,
    r.meses_com_dados,

    -- Contexto para mercado imobiliário
    CASE
        WHEN u.serie_nome = 'SELIC' AND u.ultimo_valor > 10
        THEN 'juros_altos_financiamento_caro'
        WHEN u.serie_nome = 'SELIC' AND u.ultimo_valor <= 10
        THEN 'juros_moderados_financiamento_acessivel'
        WHEN u.serie_nome = 'IPCA'  AND u.acumulado_ano > 4.5
        THEN 'inflacao_pressionada_reajuste_aluguel_alto'
        WHEN u.serie_nome = 'IGP-M' AND u.ultimo_valor > 0.5
        THEN 'igpm_elevado_impacto_contratos_aluguel'
        ELSE 'cenario_neutro'
    END                         AS interpretacao_mercado,

    CURRENT_TIMESTAMP           AS atualizado_em

FROM ultimo_valor u
LEFT JOIN resumo_anual r
    ON u.serie_nome = r.serie_nome
    AND r.ano = YEAR(CURRENT_DATE)