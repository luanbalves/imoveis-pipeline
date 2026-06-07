# Scrapy settings for imoveis project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = "imoveis"
SPIDER_MODULES = ["imoveis.spiders"]
NEWSPIDER_MODULE = "imoveis.spiders"

# ── Comportamento ético ────────────────────────────────────────────────
ROBOTSTXT_OBEY     = True       
DOWNLOAD_DELAY     = 2          
RANDOMIZE_DOWNLOAD_DELAY = True 
CONCURRENT_REQUESTS          = 4
CONCURRENT_REQUESTS_PER_DOMAIN = 2

# ── User-Agent rotativo ────────────────────────────────────────────────
DOWNLOADER_MIDDLEWARES = {
    "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": None,
    "scrapy_user_agents.middlewares.RandomUserAgentMiddleware": 400,
}

# ── Pipeline ───────────────────────────────────────────────────────────
ITEM_PIPELINES = {
    "imoveis.pipelines.MinIOPipeline": 300,
}

# ── Performance ────────────────────────────────────────────────────────
AUTOTHROTTLE_ENABLED    = True
AUTOTHROTTLE_START_DELAY = 2
AUTOTHROTTLE_MAX_DELAY  = 10

# ── Logs ───────────────────────────────────────────────────────────────
LOG_LEVEL = "INFO"
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"
