"""
Producer Kafka — lê arquivos JSONL do MinIO e publica no Kafka.

Fluxo:
  MinIO (arquivos JSONL) → Producer → Kafka Topics

Cada linha do JSONL vira uma mensagem independente no Kafka. 
Isso permite que o consumer (Spark) processe cada registro
individualmente, em paralelo.
"""

import json
import boto3
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError
from io import BytesIO
import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(__file__).parent.parent / ".env")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Mapeamento: prefixo da partição MinIO → tópico Kafka
TOPICO_POR_FONTE = {
    "bcb":            "imoveis.bcb.series",
    "ibge":           "imoveis.ibge.municipios",
    "ibge_populacao": "imoveis.ibge.municipios",
}


def criar_producer() -> KafkaProducer:
    """Cria e retorna um KafkaProducer configurado."""
    return KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        # Serializa o valor como JSON em bytes
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        # Chave também em bytes
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        # Garante que a mensagem foi recebida por todas as réplicas ( maior segurança)
        acks="all",
        # 3 Retries em caso de falha
        retries=3,
        # Agrupa mensagens pequenas para melhor throughput (economiza rede)
        batch_size=16384,
        linger_ms=10,
    )


def criar_cliente_minio():
    """Cria e retorna um cliente S3 apontando para o MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=f"http://{os.getenv('MINIO_ENDPOINT', 'localhost:9000')}",
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
    )


def extrair_fonte_do_caminho(chave: str) -> str | None:
    """
    Extrai o nome da fonte do caminho da partição.
    Ex: 'fonte=bcb/ano=2026/...' → 'bcb'
    """
    for parte in chave.split("/"):
        if parte.startswith("fonte="):
            return parte.split("=")[1]
    return None


def publicar_arquivo(s3, producer: KafkaProducer, bucket: str, chave: str) -> int:
    """
    Lê um arquivo JSONL do MinIO e publica cada linha como mensagem no Kafka.
    Retorna o número de mensagens publicadas.
    """
    fonte = extrair_fonte_do_caminho(chave)
    topico = TOPICO_POR_FONTE.get(fonte)

    if not topico:
        logger.warning(f"Fonte desconhecida para {chave} — pulando...")
        return 0

    # Baixa o arquivo do MinIO
    resposta = s3.get_object(Bucket=bucket, Key=chave)
    conteudo = resposta["Body"].read().decode("utf-8")

    count = 0
    for linha in conteudo.strip().split("\n"):
        if not linha:
            continue
        try:
            mensagem = json.loads(linha)

            # A chave da mensagem Kafka determina em qual partição vai
            # Cidade ou codigo como chave para garantir que
            # dados do mesmo lugar vão para a mesma partição
            # Ex resultado: chave_msg = "1111111", Brasília -> Partição 3
            chave_msg = (
                mensagem.get("cidade_id")
                or mensagem.get("codigo_ibge")
                or mensagem.get("serie_codigo")
                or "default"
            )

            producer.send(
                topic=topico,
                key=chave_msg,
                value=mensagem,
            )
            count += 1

        except json.JSONDecodeError as e:
            logger.error(f"JSON inválido na linha: {e}")

    return count


def executar(bucket: str = "imoveis-raw") -> None:
    """Pipeline completo: lê todos os arquivos do MinIO e publica no Kafka."""
    s3       = criar_cliente_minio()
    producer = criar_producer()

    logger.info("🚀 Iniciando producer Kafka...")
    logger.info(f"   Bucket: {bucket}")

    # Lista todos os objetos no bucket
    paginator = s3.get_paginator("list_objects_v2")
    pages     = paginator.paginate(Bucket=bucket)

    total_arquivos  = 0
    total_mensagens = 0

    for page in pages:
        for obj in page.get("Contents", []):
            chave = obj["Key"]
            if not chave.endswith(".jsonl"):
                continue

            logger.info(f"📄 Processando: {chave}")
            qtd = publicar_arquivo(s3, producer, bucket, chave)
            total_mensagens += qtd
            total_arquivos  += 1
            logger.info(f"   → {qtd} mensagens publicadas")

    # Aguarda todas as mensagens serem confirmadas pelo broker
    producer.flush() # Força envio do que estava em buffer
    producer.close()

    logger.info(f"\n✅ Concluído!")
    logger.info(f"   Arquivos processados: {total_arquivos}")
    logger.info(f"   Mensagens publicadas: {total_mensagens}")


if __name__ == "__main__":
    executar()