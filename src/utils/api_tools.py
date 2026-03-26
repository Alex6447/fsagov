import base64
import json
import re
import time
from typing import Optional

import requests
from loguru import logger

from config import (
    API_URL,
    NSI_URL,
    DETAILS_URL,
    SESSION_URL,
    HEADERS,
    TIMEOUT,
    RETRY_MAX,
    DELAY_BETWEEN_REQUESTS,
    DELAY_AFTER_ERROR,
    PAGES_BEFORE_TOKEN_REFRESH,
    PAGE_SIZE,
    BEARER_TOKEN,
)


class TokenExpiredError(RuntimeError):
    """Критическая ошибка: Bearer-токен недействителен и не может быть обновлён."""


class SessionManager:
    """Мод 1: управление жизненным циклом сессии и Bearer-токена."""

    def __init__(self, headers: dict, timeout: int = TIMEOUT):
        self.base_headers = headers.copy()
        self.timeout = timeout
        self._config_token = BEARER_TOKEN
        self._token: Optional[str] = None
        self._token_exp: Optional[float] = None
        self.session = self._new_session()

        if self._config_token:
            self._token = self._config_token
            self._token_exp = self._decode_exp(self._config_token)
            self.session.headers["Authorization"] = f"Bearer {self._config_token}"
            logger.info("Bearer token loaded from config")

    def _new_session(self) -> requests.Session:
        s = requests.Session()
        s.headers.update(self.base_headers)
        if self._config_token:
            s.headers["Authorization"] = f"Bearer {self._config_token}"
        return s

    def _extract_token(self, response: requests.Response) -> Optional[str]:
        # 1. Authorization заголовок ответа
        auth = response.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            return auth[7:]

        # 2. JWT в теле страницы (встроен в HTML/JS)
        match = re.search(
            r'["\']?(eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+)["\']?',
            response.text,
        )
        if match:
            return match.group(1)

        return None

    def _decode_exp(self, token: str) -> Optional[float]:
        try:
            payload_b64 = token.split(".")[1]
            payload_b64 += "=" * (4 - len(payload_b64) % 4)
            payload = json.loads(base64.urlsafe_b64decode(payload_b64))
            return float(payload.get("exp", 0))
        except Exception:
            return None

    def is_valid(self) -> bool:
        if not self._token or not self._token_exp:
            return False
        return time.time() < self._token_exp - 60

    def refresh(self) -> bool:
        logger.info("Refreshing session...")
        self.session = self._new_session()
        try:
            resp = self.session.get(SESSION_URL, timeout=self.timeout)
            if resp.status_code == 200:
                self.session.cookies.update(resp.cookies)
                token = self._extract_token(resp)
                if token:
                    self._token = token
                    self._token_exp = self._decode_exp(token)
                    self.session.headers["Authorization"] = f"Bearer {token}"
                    logger.info("Bearer token obtained from /ral response")
                elif self._config_token:
                    logger.info("Using config Bearer token")
                else:
                    logger.warning(
                        "Bearer token not found in /ral response, session cookies only"
                    )
                time.sleep(2)
                return True
        except Exception as e:
            logger.warning(f"Session refresh failed: {e}")
        return False

    def ensure_valid(self) -> bool:
        if not self.is_valid():
            return self.refresh()
        return True


class FilterBuilder:
    """Мод 3: построитель фильтров для API запросов."""

    def __init__(self):
        self._filters: dict = {}

    def with_region(self, region_ids: list) -> "FilterBuilder":
        self._filters["idAddressSubject"] = region_ids
        return self

    def with_statuses(self, status_ids: list) -> "FilterBuilder":
        self._filters["idStatus"] = status_ids
        return self

    def with_government_company(self, value: bool) -> "FilterBuilder":
        self._filters["isGovernmentCompany"] = [value]
        return self

    def build(self) -> dict:
        return self._filters.copy()


class RosreestrAPIClient:
    """Мод 0+1+2+3+4+6: основной клиент API."""

    def __init__(self):
        self.url = API_URL
        self.timeout = TIMEOUT
        self.retry_max = RETRY_MAX
        self.delay = DELAY_BETWEEN_REQUESTS
        self.delay_after_error = DELAY_AFTER_ERROR
        self.pages_before_refresh = PAGES_BEFORE_TOKEN_REFRESH
        self.page_size = PAGE_SIZE

        self.session_mgr = SessionManager(HEADERS)
        self.session_mgr.refresh()

        self.pages_fetched = 0
        self._total_records: int = 0

    @property
    def session(self) -> requests.Session:
        return self.session_mgr.session

    def _wait(self, seconds: float = None):
        time.sleep(seconds if seconds is not None else self.delay)

    def _handle_403(self):
        logger.warning("403 Forbidden — refreshing session")
        self.session_mgr.refresh()
        self.pages_fetched = 0
        self._wait(15)

    # ── Основная пагинация (Мод 3+4) ──────────────────────────────────────

    def fetch_page(
        self,
        offset: int,
        limit: int = None,
        sort_dir: str = "desc",
        filters: dict = None,
    ) -> Optional[dict]:
        limit = limit or self.page_size

        if self.pages_fetched >= self.pages_before_refresh:
            self.session_mgr.ensure_valid()
            self.pages_fetched = 0

        payload = {
            "columns": [],
            "sort": ["-id"] if sort_dir == "desc" else ["id"],
            "limit": limit,
            "offset": offset,
            "sortBy": "id",
            "sortDest": sort_dir,
            "numberOfAllRecords": False,
            "page": offset,  # offset = page (0, 1, 2...)
        }
        if filters:
            payload.update(filters)

        logger.info(f"REQUEST: {payload}")

        for attempt in range(self.retry_max):
            try:
                response = self.session.request(
                    method="POST",
                    url=self.url,
                    json=payload,
                    timeout=self.timeout,
                )

                if response.status_code == 200:
                    data = response.json()
                    total = data.get("total", 0)
                    items_count = len(data.get("items", []))
                    pages_count = (total + 99) // 100
                    logger.info(
                        f"RESPONSE: total={total}, items={items_count}, pages={pages_count}"
                    )
                    if "total" in data:
                        self._total_records = data["total"]
                    self.pages_fetched += 1
                    self._wait()
                    return data

                elif response.status_code == 429:
                    logger.warning(f"Rate limited, waiting {self.delay_after_error}s")
                    self._wait(self.delay_after_error)

                elif response.status_code == 403:
                    self._handle_403()

                elif response.status_code == 401:
                    error_msg = (
                        response.text[:500] if response.text else "Empty response"
                    )
                    logger.warning(f"HTTP 401 - {error_msg} - refreshing session...")
                    self.session_mgr.refresh()
                    if not self.session_mgr.is_valid():
                        msg = (
                            "Bearer token is expired and could not be refreshed. "
                            "Please update fgis_token in Settings."
                        )
                        logger.error(msg)
                        raise TokenExpiredError(msg)
                    self._wait(self.delay_after_error)

                elif response.status_code == 400:
                    logger.warning(f"400 Bad Request: {response.text[:300]}")
                    self._wait(self.delay_after_error)

                else:
                    error_msg = (
                        response.text[:300] if response.text else "Empty response"
                    )
                    logger.error(f"HTTP {response.status_code}: {error_msg}")

            except requests.exceptions.Timeout:
                logger.warning(f"Timeout on attempt {attempt + 1}")
                self._wait(self.delay_after_error)
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error: {e}")
                self._wait(self.delay_after_error)

            if attempt < self.retry_max - 1:
                self._wait(self.delay * (attempt + 1) * 2)

        return None

    def get_total(self, filters: dict = None):
        if self._total_records == 0 or filters:
            data = self.fetch_page(0, 1, filters=filters)
            if data is None:
                return None  # ошибка запроса — не возвращаем 0
            return data.get("total", 0)
        return self._total_records

    def get_page_count(self, filters: dict = None) -> int:
        total = self.get_total(filters)
        return (total + self.page_size - 1) // self.page_size

    # ── NSI справочники (Мод 2) ────────────────────────────────────────────

    def fetch_federal_districts(self) -> list:
        try:
            resp = self.session.post(
                NSI_URL,
                json={"parentId": None},
                timeout=self.timeout,
            )
            if resp.status_code == 200:
                data = resp.json()
                items = (
                    data
                    if isinstance(data, list)
                    else data.get("items", data.get("data", []))
                )
                logger.info(f"Fetched {len(items)} federal districts")
                return items
        except Exception as e:
            logger.error(f"Failed to fetch federal districts: {e}")
        return []

    def fetch_regions(self, district_id: str) -> list:
        try:
            resp = self.session.post(
                NSI_URL,
                json={"parentId": district_id},
                timeout=self.timeout,
            )
            if resp.status_code == 200:
                data = resp.json()
                items = (
                    data
                    if isinstance(data, list)
                    else data.get("items", data.get("data", []))
                )
                logger.info(f"Fetched {len(items)} regions for {district_id}")
                return items
        except Exception as e:
            logger.error(f"Failed to fetch regions for {district_id}: {e}")
        return []

    # ── Расширенные данные (Мод 6) ─────────────────────────────────────────

    def fetch_details(self, record_id: int) -> Optional[dict]:
        url = DETAILS_URL.format(id=record_id)
        for attempt in range(self.retry_max):
            try:
                resp = self.session.get(url, timeout=self.timeout)
                if resp.status_code == 200:
                    return resp.json()
                elif resp.status_code == 403:
                    self._handle_403()
                elif resp.status_code == 401:
                    logger.warning(
                        f"Details {record_id}: HTTP 401 - refreshing session..."
                    )
                    self.session_mgr.refresh()
                    if not self.session_mgr.is_valid():
                        msg = (
                            "Bearer token is expired and could not be refreshed. "
                            "Please update fgis_token in Settings."
                        )
                        logger.error(msg)
                        raise TokenExpiredError(msg)
                    self._wait(self.delay_after_error)
                elif resp.status_code == 404:
                    return None
                else:
                    logger.warning(f"Details {record_id}: HTTP {resp.status_code}")
            except Exception as e:
                logger.warning(f"Details {record_id} attempt {attempt + 1}: {e}")
            if attempt < self.retry_max - 1:
                self._wait(self.delay * (attempt + 1))
        return None

    def enrich_details(self, raw: dict) -> dict:
        """Нормализует сырой ответ деталей в плоский dict для БД.

        API: /api/v1/ral/common/companies/{id}
        Извлекает:
        - headPersonFIO: surname + name + patronymic
        - contactPhone: contacts[idType=1]
        - contactEmail: contacts[idType=4]
        """
        applicant = raw.get("applicant", {})
        contacts = applicant.get("contacts", [])
        head_person = raw.get("headPerson", {})

        head_fio = ""
        if head_person:
            parts = [
                head_person.get("surname", ""),
                head_person.get("name", ""),
                head_person.get("patronymic", ""),
            ]
            head_fio = " ".join(p for p in parts if p)

        phone = ""
        email = ""
        if contacts:
            for c in contacts:
                if c.get("idType") == 1:
                    phone = c.get("value", "")
                elif c.get("idType") == 4:
                    email = c.get("value", "")

        return {
            "headFullName": head_fio,
            "phones": [phone] if phone else [],
            "emails": [email] if email else [],
        }
