import time
from loguru import logger
import json
from typing import Optional
import requests

from config import (
    API_URL,
    API_METHOD,
    HEADERS,
    TIMEOUT,
    RETRY_MAX,
    DELAY_BETWEEN_REQUESTS,
    DELAY_AFTER_ERROR,
    PAGES_BEFORE_TOKEN_REFRESH,
    PAGE_SIZE,
    BEARER_TOKEN,
    TOKEN_REFRESH_URL,
)



class RosreestrAPIClient:
    def __init__(self):
        self.url = API_URL
        self.method = API_METHOD
        self.headers = HEADERS.copy()
        if BEARER_TOKEN:
            self.headers["Authorization"] = f"Bearer {BEARER_TOKEN}"
        self.timeout = TIMEOUT
        self.retry_max = RETRY_MAX
        self.delay = DELAY_BETWEEN_REQUESTS
        self.delay_after_error = DELAY_AFTER_ERROR
        self.pages_before_refresh = PAGES_BEFORE_TOKEN_REFRESH
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.total_records = 0
        self.cookies_obtained = False
        self.pages_fetched = 0

    def _wait(self, seconds: float = None):
        time.sleep(seconds or self.delay)

    def _get_cookies(self) -> bool:
        try:
            response = self.session.get(
                "https://pub.fsa.gov.ru/ral",
                timeout=self.timeout,
            )
            if response.status_code == 200:
                self.session.cookies.update(response.cookies)
                self.cookies_obtained = True
                logger.info("Cookies obtained successfully")
                return True
        except Exception as e:
            logger.warning(f"Failed to get cookies: {e}")
        return False

    def _refresh_session(self) -> bool:
        logger.info("Refreshing session...")
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.cookies_obtained = False
        self._get_cookies()
        self.pages_fetched = 0
        self._wait(15)  # Wait 15s after refresh
        return True

    def fetch_page(self, offset: int, limit: int = PAGE_SIZE) -> Optional[dict]:
        if not self.cookies_obtained:
            self._get_cookies()

        payload = {
            "columns": [],
            "sort": ["-id"],
            "limit": limit,
            "offset": offset,
            "sortBy": "id",
            "sortDest": "desc",
            "numberOfAllRecords": False,
            "page": 0,
        }

        for attempt in range(self.retry_max):
            try:
                logger.debug(f"Fetching offset {offset}, attempt {attempt + 1}")

                headers = self.session.headers.copy()
                headers["Cache-Control"] = "no-cache"
                logger.debug(f"{headers=}")
                logger.debug(f"{payload=}")

                response = self.session.request(
                    method=self.method,
                    url=self.url,
                    json=payload,
                    headers=headers,
                    timeout=self.timeout,
                )

                if response.status_code == 200:
                    data = response.json()
                    if not self.cookies_obtained:
                        self.session.cookies.update(response.cookies)
                        self.cookies_obtained = True

                    if "total" in data:
                        self.total_records = data["total"]

                    self.pages_fetched += 1
                    return data

                elif response.status_code == 429:
                    logger.warning(f"Rate limited, waiting {self.delay_after_error}s")
                    self._wait(self.delay_after_error)
                    continue

                elif response.status_code == 400:
                    logger.warning(f"400 Bad Request {response.text}")
                    self._wait(self.delay_after_error)
                    continue  # Don't count this as an attempt

                elif response.status_code == 403:
                    logger.warning(f"403 Forbidden - refreshing session")
                    self._refresh_session()
                    continue

                else:
                    logger.error(f"HTTP {response.status_code}: {response.text[:200]}")

            except requests.exceptions.Timeout:
                logger.warning(f"Timeout on attempt {attempt + 1}")
                self._wait(self.delay_after_error)
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error: {e}")
                self._wait(self.delay_after_error)

            if attempt < self.retry_max - 1:
                wait_time = self.delay * (attempt + 1) * 2
                self._wait(wait_time)

        return None

    def get_total(self) -> int:
        if self.total_records == 0:
            data = self.fetch_page(0, 1)
            if data:
                return data.get("total", 0)
        return self.total_records

    def fetch_by_page(self, offset: int, size: int = PAGE_SIZE) -> Optional[dict]:
        return self.fetch_page(offset, size)
