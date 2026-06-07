"""
Spider para coleta de dados municipais do IBGE via API oficial.
Coleta população, área, PIB per capita e IDH dos principais municípios.

API IBGE: https://servicodados.ibge.gov.br/api/docs
"""

import scrapy
from datetime import datetime
from ..items import MunicipioItem


class IbgeSpider(scrapy.Spider):
    name = "ibge_municipios"
    custom_settings = {
        "DOWNLOAD_DELAY": 0.5,
    }

    # Capitais e grandes cidades — códigos IBGE
    MUNICIPIOS = {
        "3550308": "São Paulo/SP",
        "3304557": "Rio de Janeiro/RJ",
        "5300108": "Brasília/DF",
        "3106200": "Belo Horizonte/MG",
        "4106902": "Curitiba/PR",
        "4314902": "Porto Alegre/RS",
        "2304400": "Fortaleza/CE",
        "2927408": "Salvador/BA",
        "2611606": "Recife/PE",
        "1302603": "Manaus/AM",
        "5208707": "Goiânia/GO",
        "4205407": "Florianópolis/SC",
        "3205309": "Vitória/ES",
        "3509502": "Campinas/SP",
        "3518800": "Guarulhos/SP",
        "3543402": "Ribeirão Preto/SP",
        "3548708": "Santo André/SP",
        "3529401": "Osasco/SP",
    }

    def start_requests(self):
        # Busca dados básicos de todos os municípios de uma vez
        codigos = "|".join(self.MUNICIPIOS.keys())
        url = (
            f"https://servicodados.ibge.gov.br/api/v1/localidades/municipios/"
            f"{codigos}"
        )
        yield scrapy.Request(
            url=url,
            callback=self.parse_municipios,
            headers={"Accept": "application/json"},
        )

    def parse_municipios(self, response):
        municipios = response.json()
        self.logger.info(f"Municípios retornados: {len(municipios)}")

        for m in municipios:
            codigo = str(m["id"])
            item = MunicipioItem()
            item["fonte"]          = "ibge"
            item["codigo_ibge"]    = codigo
            item["nome"]           = m["nome"]
            item["uf"]             = m["microrregiao"]["mesorregiao"]["UF"]["sigla"]
            item["regiao"]         = m["microrregiao"]["mesorregiao"]["UF"]["regiao"]["nome"]
            item["microrregiao"]   = m["microrregiao"]["nome"]
            item["mesorregiao"]    = m["microrregiao"]["mesorregiao"]["nome"]
            item["coletado_em"]    = datetime.now().isoformat()
            item["data_coleta"]    = datetime.now().strftime("%Y-%m-%d")
            yield item

            # Busca indicadores adicionais (população, área)
            yield scrapy.Request(
                url=f"https://servicodados.ibge.gov.br/api/v3/agregados/4709/periodos/2022/variaveis/93?localidades=N6[{codigo}]",
                callback=self.parse_populacao,
                meta={"codigo_ibge": codigo, "nome": m["nome"]},
                headers={"Accept": "application/json"},
            )

    def parse_populacao(self, response):
        """Coleta dados de população do Censo 2022."""
        codigo = response.meta["codigo_ibge"]
        nome   = response.meta["nome"]

        try:
            dados = response.json()
            valor = (
                dados[0]["resultados"][0]["series"][0]["serie"].get("2022")
                if dados else None
            )
        except (IndexError, KeyError):
            valor = None

        item = MunicipioItem()
        item["fonte"]       = "ibge_populacao"
        item["codigo_ibge"] = codigo
        item["nome"]        = nome
        item["populacao"]   = int(valor) if valor else None
        item["coletado_em"] = datetime.now().isoformat()
        item["data_coleta"] = datetime.now().strftime("%Y-%m-%d")
        yield item