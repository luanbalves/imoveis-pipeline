/*
  GOLD — Ranking de municípios por atratividade imobiliária
  
  Combina dados demográficos do IBGE com contexto de mercado
  para fazer um ranking de cidades por potencial imobiliário.
  
  Responde: "Quais cidades têm maior potencial para
  investimento imobiliário?"
*/

WITH municipios AS (
    SELECT * FROM {{ ref('silver_municipios_enriquecidos') }}
    WHERE populacao IS NOT NULL
),

-- Normaliza o score de relevância em percentil
com_percentil AS (
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY populacao)     AS percentil_populacao,
        PERCENT_RANK() OVER (ORDER BY score_relevancia) AS percentil_score
    FROM municipios
)

SELECT
    codigo_ibge,
    nome,
    uf,
    regiao,
    microrregiao,
    populacao,
    porte_municipio,
    perfil_mercado,
    score_relevancia,

    ROUND(percentil_populacao * 100, 1)     AS percentil_populacao,

    -- Score final composto
    ROUND(
        (percentil_populacao * 0.6 + percentil_score * 0.4) * 100
    , 1)                                    AS score_atratividade,

    -- Ranking geral
    ROW_NUMBER() OVER (
        ORDER BY (percentil_populacao * 0.6 + percentil_score * 0.4) DESC
    )                                       AS ranking_geral,

    -- Ranking dentro da região
    ROW_NUMBER() OVER (
        PARTITION BY regiao
        ORDER BY populacao DESC
    )                                       AS ranking_na_regiao,

    CURRENT_TIMESTAMP                       AS atualizado_em

FROM com_percentil
ORDER BY score_atratividade DESC