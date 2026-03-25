from pathlib import Path

API_URL = "https://pub.fsa.gov.ru/api/v1/ral/common/showcases/get"
API_METHOD = "POST"

TIMEOUT = 30
RETRY_MAX = 3
DELAY_BETWEEN_REQUESTS = 3
DELAY_AFTER_ERROR = 60
PAGES_BEFORE_TOKEN_REFRESH = 5

PAGE_SIZE = 100

DB_PATH = Path("data/rosreestr.db")

BEARER_TOKEN = "eyJhbGciOiJFZERTQSJ9.eyJpc3MiOiJGQVUgTklBIiwic3ViIjoiYW5vbnltb3VzIiwiZXhwIjoxNzc0MzkwMjY2LCJpYXQiOjE3NzQzNjE0NjZ9.ROC6o-5ExfKCF6Ad1PwsQn64r5Mw9raFVFzyl2FM-TsZo2zYCIVVk-Xn5WWTA0eS9CTPLTp5WLJAsoFuz_3KCQ"

TOKEN_REFRESH_URL = "https://pub.fsa.gov.ru/api/v1/nsi/public/actual"

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
