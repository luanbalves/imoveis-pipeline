/*
  BRONZE — Séries temporais do Banco Central
  Lê diretamente os arquivos Parquet do Delta Lake no MinIO.
  
  O DuckDB com extensão httpfs consegue ler arquivos S3/MinIO
  como se fossem tabelas locais, sem copiar dados.
*/

SELECT
    fonte,
    serie_codigo,
    serie_nome,
    unidade,
    data_ref,
    valor,
    categoria,
    ano,
    mes,
    processado_em,
    kafka_timestamp,
    partition   AS kafka_partition,
    "offset"      AS kafka_offset
FROM read_parquet(
    's3://imoveis-processed/bcb_series/**/*.parquet',
    hive_partitioning = true,
    union_by_name = true
)
WHERE valor IS NOT NULL