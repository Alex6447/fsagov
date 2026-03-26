import json
import os
from pathlib import Path

DEFAULT_BEARER_TOKEN = "eyJhbGciOiJFZERTQSJ9.eyJpc3MiOiJGQVUgTklBIiwic3ViIjoiYW5vbnltb3VzIiwiZXhwIjoxNzc0NDY5MDU5LCJpYXQiOjE3NzQ0NDAyNTl9.6l0MvCc8_8KN2FH0mB8GIpoy0Z16PBBji3CVyz4TlJPEyoVGARLCdBrHOQi-NKKxfir5ygvCu8nQJm70t-wwAg"

CONFIG_FILE = Path("config.json")
if CONFIG_FILE.exists():
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        user_config = json.load(f)
        BEARER_TOKEN = user_config.get("fgis_token", DEFAULT_BEARER_TOKEN)
else:
    BEARER_TOKEN = DEFAULT_BEARER_TOKEN

# Основной API
API_URL = "https://pub.fsa.gov.ru/api/v1/ral/common/showcases/get"

# Мод 1: получение сессии и токена
SESSION_URL = "https://pub.fsa.gov.ru/ral"

# Мод 2: справочник округов / регионов
NSI_URL = "https://pub.fsa.gov.ru/nsi/api/tree/federalDistrictsAndSubjects/get"

# Мод 6: детальная карточка записи (компании)
DETAILS_URL = "https://pub.fsa.gov.ru/api/v1/ral/common/companies/{id}"

TIMEOUT = 30
RETRY_MAX = 3
DELAY_BETWEEN_REQUESTS = 3
DELAY_AFTER_ERROR = 60
PAGES_BEFORE_TOKEN_REFRESH = 5

PAGE_SIZE = 100

# Мод 3+4: лимит страниц перед добавлением фильтров
MAX_PAGES_BEFORE_FILTER = 40

DB_PATH = Path("data/rosreestr.db")

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "Content-Type": "application/json",
    "Origin": "https://pub.fsa.gov.ru",
    "Referer": "https://pub.fsa.gov.ru/ral",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}
