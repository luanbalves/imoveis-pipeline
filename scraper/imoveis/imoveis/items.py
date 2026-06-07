# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


# TODO: utilizar quando integrar fonte de anúncios individuais
# (API de classificados, dados abertos de prefeitura etc)
# class ImovelItem(scrapy.Item):
#     titulo      = scrapy.Field()
#     url         = scrapy.Field()
#     cidade      = scrapy.Field()
#     estado      = scrapy.Field()
#     bairro      = scrapy.Field()
#     tipo        = scrapy.Field()
#     preco       = scrapy.Field()
#     metragem    = scrapy.Field()
#     quartos     = scrapy.Field()
#     banheiros   = scrapy.Field()
#     vagas       = scrapy.Field()
#     descricao   = scrapy.Field()
#     coletado_em = scrapy.Field()
#     data_coleta = scrapy.Field()


class SerieTemporalItem(scrapy.Item):
    """Séries temporais econômicas (BCB, IPCA, SELIC etc)."""
    fonte        = scrapy.Field()
    serie_codigo = scrapy.Field()
    serie_nome   = scrapy.Field()
    unidade      = scrapy.Field()
    data_ref     = scrapy.Field()
    valor        = scrapy.Field()
    coletado_em  = scrapy.Field()
    data_coleta  = scrapy.Field()


class MunicipioItem(scrapy.Item):
    """Dados municipais do IBGE."""
    fonte        = scrapy.Field()
    codigo_ibge  = scrapy.Field()
    nome         = scrapy.Field()
    uf           = scrapy.Field()
    regiao       = scrapy.Field()
    microrregiao = scrapy.Field()
    mesorregiao  = scrapy.Field()
    populacao    = scrapy.Field()
    coletado_em  = scrapy.Field()
    data_coleta  = scrapy.Field()