/*
  SILVER — Municípios enriquecidos
  
  Consolida dados geográficos e demográficos do IBGE
  numa visão única por município, eliminando duplicatas
  entre os dois tipos de registro (ibge e ibge_populacao).
*/

WITH geografico AS (
    SELECT
        codigo_ibge,
        nome,
        uf,
        regiao,
        microrregiao,
        mesorregiao,
        processado_em
    FROM {{ ref('bronze_ibge_municipios') }}
    WHERE fonte = 'ibge'
      AND uf IS NOT NULL
),

demografico AS (
    SELECT
        codigo_ibge,
        populacao,
        porte_municipio
    FROM {{ ref('bronze_ibge_municipios') }}
    WHERE fonte = 'ibge_populacao'
      AND populacao IS NOT NULL
),

consolidado AS (
    SELECT
        g.codigo_ibge,
        g.nome,
        g.uf,
        g.regiao,
        g.microrregiao,
        g.mesorregiao,
        d.populacao,
        d.porte_municipio,

        -- Score de relevância para mercado imobiliário
        -- Capitais e metrópoles têm mais liquidez e dados
        CASE
            WHEN d.populacao > 1000000 THEN 5
            WHEN d.populacao > 500000  THEN 4
            WHEN d.populacao > 200000  THEN 3
            WHEN d.populacao > 100000  THEN 2
            ELSE 1
        END                         AS score_relevancia,

        -- Região para análise macro
        CASE g.regiao
            WHEN 'Sudeste' THEN 'mercado_maduro'
            WHEN 'Sul'     THEN 'mercado_maduro'
            WHEN 'Centro-Oeste' THEN 'mercado_expansao'
            ELSE 'mercado_emergente'
        END                         AS perfil_mercado,

        CURRENT_TIMESTAMP           AS processado_em

    FROM geografico g
    LEFT JOIN demografico d ON g.codigo_ibge = d.codigo_ibge
)

SELECT * FROM consolidado