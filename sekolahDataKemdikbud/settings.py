# Scrapy settings for sekolahDataKemdikbud project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = "sekolahDataKemdikbud"


SPIDER_MODULES = ["sekolahDataKemdikbud.spiders"]
NEWSPIDER_MODULE = "sekolahDataKemdikbud.spiders"


# Crawl responsibly by identifying yourself (and your website) on the user-agent
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

import logging
from .log_config import error_logger, links_logger, detail_links_logger

# Pengaturan logging Scrapy
LOG_ENABLED = True
LOG_LEVEL = 'DEBUG'  # Atur level logging sesuai kebutuhan Anda

# !retry scrapy
RETRY_ENABLED = True
RETRY_TIMES = 10  # Jumlah maksimum percobaan
RETRY_HTTP_CODES = [503, 500]  # Kode HTTP yang ingin di-retry
RETRY_BACKOFF_FACTOR = 2


CONCURRENT_REQUESTS = 5

DOWNLOAD_DELAY = 0.5 

AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1
AUTOTHROTTLE_MAX_DELAY = 60
AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
AUTOTHROTTLE_DEBUG = False

DONWLOAD_TIMEOUT = 300

# Set settings whose default value is deprecated to a future-proof value
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"
