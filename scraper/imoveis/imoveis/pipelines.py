# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
"""
Pipeline de saída: salva os itens coletados no MinIO (S3 local)
como arquivos JSON particionados por data e cidade.

Particionamento: imoveis-raw/ano=2026/mes=05/dia=27/cidade=sao-paulo/
Isso é uma boa prática de engenharia para facilitar leitura eficiente pelo Spark.
"""

"""
Pipeline de saída: salva itens no MinIO particionados por fonte e data.
"""

import json
import boto3
from io import BytesIO
from datetime import datetime
from collections import defaultdict
from itemadapter import ItemAdapter
import os
from dotenv import load_dotenv

load_dotenv()


class MinIOPipeline:

    def open_spider(self, spider):
        self.s3 = boto3.client(
            "s3",
            endpoint_url=f"http://{os.getenv('MINIO_ENDPOINT', 'localhost:9000')}",
            aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        )
        self.bucket = os.getenv("MINIO_BUCKET", "imoveis-raw")
        self.buffer = defaultdict(list)
        self.total  = 0
        spider.logger.info(f"MinIOPipeline: conectado em {os.getenv('MINIO_ENDPOINT')}")

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        # Determina a fonte para organizar a partição
        fonte = adapter.get("fonte", "anuncios")
        cidade = adapter.get("cidade_id") or adapter.get("cidade", "desconhecido")

        particao = (
            f"fonte={fonte}"
            f"/ano={datetime.now().year}"
            f"/mes={datetime.now().strftime('%m')}"
            f"/dia={datetime.now().strftime('%d')}"
            f"/cidade={cidade}"
        )

        self.buffer[particao].append(dict(adapter))
        self.total += 1
        return item

    def close_spider(self, spider):
        spider.logger.info(f"Salvando {self.total} itens no MinIO...")

        for particao, itens in self.buffer.items():
            conteudo  = "\n".join(json.dumps(i, ensure_ascii=False) for i in itens)
            dados     = BytesIO(conteudo.encode("utf-8"))
            timestamp = datetime.now().strftime("%H%M%S")
            chave     = f"{particao}/dados_{timestamp}.jsonl"

            self.s3.put_object(
                Bucket=self.bucket,
                Key=chave,
                Body=dados,
                ContentType="application/json",
            )
            spider.logger.info(f"  ✅ {len(itens)} itens → s3://{self.bucket}/{chave}")

        spider.logger.info(f"Total salvo: {self.total} itens")