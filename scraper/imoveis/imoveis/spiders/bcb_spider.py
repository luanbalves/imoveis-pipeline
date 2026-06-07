"""
Spider para coleta de séries temporais do Banco Central do Brasil.
API pública, sem autenticação necessária.

Séries coletadas:
- 433:  IPCA (inflação oficial)
- 189:  IGP-M (índice de reajuste de aluguéis)
- 432:  INPC
- 11:   Taxa SELIC (juros básicos — afeta financiamento imobiliário)
- 7478: Taxa média de financiamento imobiliário

Documentação: https://www.bcb.gov.br/estatisticas/sgspub
"""

import scrapy
from datetime import datetime
from ..items import SerieTemporalItem


class BcbSpider(scrapy.Spider):
    name = "bcb_series"

    custom_settings = {
        "DOWNLOAD_DELAY": 1,
        "AUTOTHROTTLE_START_DELAY": 1,
    }

    # Séries relevantes para mercado imobiliário
    SERIES = [
        {"codigo": 433,  "nome": "IPCA",                        "unidade": "% a.m."},
        {"codigo": 189,  "nome": "IGP-M",                       "unidade": "% a.m."},
        {"codigo": 432,  "nome": "INPC",                        "unidade": "% a.m."},
        {"codigo": 11,   "nome": "SELIC",                       "unidade": "% a.a."},
        {"codigo": 7478, "nome": "Taxa_Financiamento_Imob",     "unidade": "% a.m."},
        {"codigo": 4390, "nome": "Credito_Imobiliario_Total",   "unidade": "R$ milhoes"},
    ]

    DATA_INICIO = "01/01/2015"
    DATA_FIM    = datetime.now().strftime("%d/%m/%Y")

    def start_requests(self):
        for serie in self.SERIES:
            url = (
                f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{serie['codigo']}"
                f"/dados?formato=json"
                f"&dataInicial={self.DATA_INICIO}"
                f"&dataFinal={self.DATA_FIM}"
            )
            yield scrapy.Request(
                url=url,
                callback=self.parse_serie,
                meta={"serie": serie},
                headers={"Accept": "application/json"},
            )

    def parse_serie(self, response):
        serie = response.meta["serie"]

        try:
            dados = response.json()
        except Exception as e:
            self.logger.error(f"Erro ao parsear {serie['nome']}: {e}")
            return

        self.logger.info(f"  Serie {serie['nome']}: {len(dados)} pontos coletados")

        for ponto in dados:
            item = SerieTemporalItem()
            item["fonte"]       = "bcb"
            item["serie_codigo"]= str(serie["codigo"])
            item["serie_nome"]  = serie["nome"]
            item["unidade"]     = serie["unidade"]
            item["data_ref"]    = ponto.get("data")
            item["valor"]       = self._to_float(ponto.get("valor"))
            item["coletado_em"] = datetime.now().isoformat()
            item["data_coleta"] = datetime.now().strftime("%Y-%m-%d")
            yield item

    @staticmethod
    def _to_float(valor):
        try:
            return float(str(valor).replace(",", "."))
        except (ValueError, TypeError):
            return None