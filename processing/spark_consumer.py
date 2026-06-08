"""
Spark Structured Streaming Consumer

Lê mensagens dos tópicos Kafka e processa em micro-batches,
salvando os dados transformados no MinIO em formato Delta Lake.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType
)
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(__file__).parent.parent / ".env")

# ── Configurações ──────────────────────────────────────────────────────────────

KAFKA_SERVERS  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_KEY      = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "minioadmin")

# Onde salvar os dados processados no MinIO
OUTPUT_BASE    = "s3a://imoveis-processed"

# Checkpoint — onde o Spark guarda o progresso do streaming
# Permite retomar de onde parou em caso de falha
CHECKPOINT_BASE = "/tmp/spark-checkpoints"


def criar_spark_session() -> SparkSession:
    """
    Cria SparkSession com suporte a:
    - Kafka (para leitura do stream)
    - Delta Lake (para escrita ACID)
    - MinIO/S3 (para storage)
    """
    return (
        SparkSession.builder
        .appName("imoveis-pipeline-streaming")
        .master("local[2]")   # local com 2 threads, em producao trocaria.
        .config(
            "spark.jars.packages",
            ",".join([
                # Conector Kafka → Spark
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
                # Delta Lake
                "io.delta:delta-spark_2.12:3.2.0",
                # Cliente S3 para MinIO
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.262",
            ])
        )
        # Delta Lake como formato padrão
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Configuração do MinIO (compatível com S3)
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Menos logs
        .config("spark.ui.showConsoleProgress", "false")
        # Ignora restrições do Java 18+ para o Hadoop UserGroupInformation
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
        .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow")
        .getOrCreate()
    )


# ── Schemas ────────────────────────────────────────────────────────────────────

# Schema das séries temporais do BCB
SCHEMA_BCB = StructType([
    StructField("fonte",        StringType(), True),
    StructField("serie_codigo", StringType(), True),
    StructField("serie_nome",   StringType(), True),
    StructField("unidade",      StringType(), True),
    StructField("data_ref",     StringType(), True),
    StructField("valor",        DoubleType(), True),
    StructField("coletado_em",  StringType(), True),
    StructField("data_coleta",  StringType(), True),
])

# Schema dos municípios IBGE
SCHEMA_IBGE = StructType([
    StructField("fonte",        StringType(), True),
    StructField("codigo_ibge",  StringType(), True),
    StructField("nome",         StringType(), True),
    StructField("uf",           StringType(), True),
    StructField("regiao",       StringType(), True),
    StructField("microrregiao", StringType(), True),
    StructField("mesorregiao",  StringType(), True),
    StructField("populacao",    DoubleType(), True),
    StructField("coletado_em",  StringType(), True),
    StructField("data_coleta",  StringType(), True),
])


# ── Transformações ─────────────────────────────────────────────────────────────

def transformar_bcb(df):
    """
    Transforma os dados brutos do BCB:
    - Converte data_ref de string para date
    - Calcula variação percentual em relação ao período anterior
    - Filtra registros com valor nulo
    - Adiciona coluna de ano e mês para particionamento
    """
    return (
        df
        .filter(F.col("valor").isNotNull())
        .withColumn(
            "data_ref",
            F.to_date(F.col("data_ref"), "dd/MM/yyyy")
        )
        .withColumn("ano",  F.year("data_ref"))
        .withColumn("mes",  F.month("data_ref"))
        .withColumn("processado_em", F.current_timestamp())
        # Classifica a série por categoria
        .withColumn(
            "categoria",
            F.when(F.col("serie_nome").isin("IPCA", "IGP-M", "INPC"), "inflacao")
             .when(F.col("serie_nome") == "SELIC", "juros")
             .otherwise("credito_imobiliario")
        )
    )


def transformar_ibge(df):
    """
    Transforma os dados do IBGE:
    - Normaliza nomes de UF
    - Filtra registros sem código IBGE
    - Adiciona timestamp de processamento
    """
    return (
        df
        .filter(F.col("codigo_ibge").isNotNull())
        .withColumn("processado_em", F.current_timestamp())
        .withColumn(
            "porte_municipio",
            F.when(F.col("populacao") > 1_000_000, "metrópole")
             .when(F.col("populacao") > 500_000,   "grande")
             .when(F.col("populacao") > 100_000,   "médio")
             .otherwise("pequeno")
        )
    )


# ── Jobs de Streaming ──────────────────────────────────────────────────────────

def iniciar_stream_bcb(spark: SparkSession):
    """
    Lê o tópico Kafka de séries BCB e salva em Delta Lake.
    
    Funciona assim:
    1. Define a fonte (Kafka)
    2. Define as transformações
    3. Define o destino (Delta Lake no MinIO)
    4. Inicia e aguarda — processa micro-batches continuamente
    """

    # Cria bucket de destino no MinIO se não existir
    import boto3
    s3 = boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_KEY,
        aws_secret_access_key=MINIO_SECRET,
    )
    for bucket in ["imoveis-processed"]:
        try:
            s3.create_bucket(Bucket=bucket)
            print(f"✅ Bucket '{bucket}' criado")
        except Exception:
            pass  # já existe

    print("🚀 Iniciando stream BCB...")

    # Lê do Kafka
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", "imoveis.bcb.series")
        # "earliest" = processa todas as mensagens desde o início
        # Em produção usaria "latest" para só processar novas
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # O value do Kafka chega como bytes, precisa deserializar
    # from_json converte a string JSON para colunas estruturadas
    parsed = (
        raw_stream
        .select(
            F.from_json(
                F.col("value").cast("string"),
                SCHEMA_BCB
            ).alias("data"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("partition"),
            F.col("offset"),
        )
        .select("data.*", "kafka_timestamp", "partition", "offset")
    )

    # Aplica transformações
    transformado = transformar_bcb(parsed)

    # Escreve em Delta Lake no MinIO, particionado por série e ano
    query = (
        transformado.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/bcb")
        .partitionBy("serie_nome", "ano")
        .start(f"{OUTPUT_BASE}/bcb_series")
    )

    return query


def iniciar_stream_ibge(spark: SparkSession):
    """Lê o tópico Kafka de municípios IBGE e salva em Delta Lake."""

    print("🚀 Iniciando stream IBGE...")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", "imoveis.ibge.municipios")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw_stream
        .select(
            F.from_json(
                F.col("value").cast("string"),
                SCHEMA_IBGE
            ).alias("data"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .select("data.*", "kafka_timestamp")
    )

    transformado = transformar_ibge(parsed)

    query = (
        transformado.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/ibge")
        .partitionBy("uf")
        .start(f"{OUTPUT_BASE}/ibge_municipios")
    )

    return query


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    spark = criar_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("\n" + "="*60)
    print("IMOVEIS PIPELINE — Spark Structured Streaming")
    print("="*60)

    # Inicia os dois streams em paralelo
    query_bcb  = iniciar_stream_bcb(spark)
    query_ibge = iniciar_stream_ibge(spark)

    print("\n✅ Streams iniciados. Aguardando mensagens do Kafka...")
    print("   BCB  → imoveis.bcb.series")
    print("   IBGE → imoveis.ibge.municipios")
    print("\n   Ctrl+C para encerrar\n")

    # Aguarda ambos os streams — o Spark fica em loop processando
    # micro-batches até ser interrompido
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n⏹  Encerrando streams...")
        query_bcb.stop()
        query_ibge.stop()
        spark.stop()
        print("✅ Encerrado com sucesso")