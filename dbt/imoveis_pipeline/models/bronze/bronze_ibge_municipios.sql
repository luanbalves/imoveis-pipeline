/*
  BRONZE — Dados municipais do IBGE
  Combina os dois tipos de registro:
  - ibge: dados geográficos (UF, região, microrregião)
  - ibge_populacao: dados demográficos (população)
*/

SELECT
    fonte,
    codigo_ibge,
    nome,
    uf,
    regiao,
    microrregiao,
    mesorregiao,
    populacao,
    porte_municipio,
    processado_em,
    kafka_timestamp
FROM read_parquet(
    's3://imoveis-processed/ibge_municipios/**/*.parquet',
    hive_partitioning = true,
    union_by_name = true
)
WHERE codigo_ibge IS NOT NULL