import sqlite3
from loguru import logger
from pathlib import Path
from typing import List, Optional
from datetime import date, datetime

from config import DB_PATH


class Database:
    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn: Optional[sqlite3.Connection] = None

    def connect(self):
        self.conn = sqlite3.connect(self.db_path, timeout=30)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

    def _commit_and_close(self):
        if self.conn:
            self.conn.commit()
            self.close()

    def init_db(self):
        self.connect()
        cursor = self.conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS showcases (
                id INTEGER PRIMARY KEY,
                id_type INTEGER,
                name_type TEXT,
                id_status INTEGER,
                name_status TEXT,
                name_type_activity TEXT,
                ids_type_activity TEXT,
                reg_number TEXT,
                reg_date TEXT,
                full_name TEXT,
                address TEXT,
                federal_district TEXT,
                region TEXT,
                fa_country TEXT,
                fa_name TEXT,
                fa_name_eng TEXT,
                solution_number TEXT,
                unique_register_number TEXT,
                fa_id_status INTEGER,
                has_eng_version INTEGER,
                full_name_eng TEXT,
                short_name_eng TEXT,
                head_full_name_eng TEXT,
                address_eng TEXT,
                applicant_full_name_eng TEXT,
                applicant_inn TEXT,
                applicant_full_name TEXT,
                oa_description TEXT,
                oa_description_eng TEXT,
                combined_sign_id INTEGER,
                okved_nsi_name TEXT,
                is_government_company INTEGER,
                is_foreign_organization INTEGER,
                insert_national_part_name TEXT,
                phones TEXT,
                emails TEXT,
                head_person_fio TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_reg_number ON showcases(reg_number)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_reg_date ON showcases(reg_date)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_id_status ON showcases(id_status)
        """)

        # Мод 2: справочник округов и регионов
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS nsi_districts (
                id TEXT PRIMARY KEY,
                name TEXT,
                total_source INTEGER DEFAULT NULL,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Миграция: добавить поле если таблица уже существует
        try:
            cursor.execute("ALTER TABLE nsi_districts ADD COLUMN total_source INTEGER DEFAULT NULL")
        except Exception:
            pass  # поле уже есть
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS nsi_regions (
                id TEXT PRIMARY KEY,
                name TEXT,
                district_id TEXT,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (district_id) REFERENCES nsi_districts(id)
            )
        """)

        # Мод 7: метрики выполнения
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS run_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_at TEXT DEFAULT CURRENT_TIMESTAMP,
                duration_sec REAL,
                total_fetched INTEGER,
                total_inserted INTEGER,
                total_errors INTEGER,
                filters_used TEXT
            )
        """)

        self._commit_and_close()
        logger.info("Database initialized")

    # ── showcases ──────────────────────────────────────────────────────────

    def insert_batch(self, records: List[dict]) -> int:
        if not records:
            return 0

        self.connect()
        cursor = self.conn.cursor()

        columns = [
            "id",
            "region",
            "id_type",
            "name_type",
            "id_status",
            "name_status",
            "name_type_activity",
            "ids_type_activity",
            "reg_number",
            "reg_date",
            "full_name",
            "address",
            "federal_district",
            "fa_country",
            "fa_name",
            "fa_name_eng",
            "solution_number",
            "unique_register_number",
            "fa_id_status",
            "has_eng_version",
            "full_name_eng",
            "short_name_eng",
            "head_full_name_eng",
            "address_eng",
            "applicant_full_name_eng",
            "applicant_inn",
            "applicant_full_name",
            "oa_description",
            "oa_description_eng",
            "combined_sign_id",
            "okved_nsi_name",
            "is_government_company",
            "is_foreign_organization",
            "insert_national_part_name",
        ]

        placeholders = ", ".join(["?"] * len(columns))
        sql = f"INSERT OR IGNORE INTO showcases ({', '.join(columns)}) VALUES ({placeholders})"

        inserted = 0
        for r in records:
            reg_date = r.get("regDate")
            if isinstance(reg_date, date):
                reg_date = reg_date.isoformat()
            elif reg_date:
                reg_date = str(reg_date)

            def bool_int(val):
                if val is True:
                    return 1
                if val is False:
                    return 0
                return None

            data = (
                r.get("id"),
                r.get("region"),  # название региона
                r.get("idType"),
                r.get("nameType"),
                r.get("idStatus"),
                r.get("nameStatus"),
                r.get("nameTypeActivity"),
                r.get("idsTypeActivity"),
                r.get("regNumber"),
                reg_date,
                r.get("fullName"),
                r.get("address"),
                r.get("federalDistrict"),
                r.get("faCountry"),
                r.get("faName"),
                r.get("faNameEng"),
                r.get("solutionNumber"),
                r.get("uniqueRegisterNumber"),
                r.get("faIdStatus"),
                bool_int(r.get("hasEngVersion")),
                r.get("fullNameEng"),
                r.get("shortNameEng"),
                r.get("headFullNameEng"),
                r.get("addressEng"),
                r.get("applicantFullNameEng"),
                r.get("applicantInn"),
                r.get("applicantFullName"),
                r.get("oaDescription"),
                r.get("oaDescriptionEng"),
                r.get("combinedSignId"),
                r.get("okvedNsiName"),
                bool_int(r.get("isGovernmentCompany")),
                bool_int(r.get("isForeignOrganization")),
                r.get("insertNationalPartName"),
            )

            try:
                cursor.execute(sql, data)
                if cursor.rowcount > 0:
                    inserted += 1
            except sqlite3.IntegrityError:
                pass

        self._commit_and_close()
        logger.info(f"Inserted {inserted} new records")
        return inserted

    def get_count(self) -> int:
        self.connect()
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM showcases")
        result = cursor.fetchone()[0]
        self.close()
        return result

    def get_all_ids(self) -> List[int]:
        self.connect()
        cursor = self.conn.cursor()
        cursor.execute("SELECT id FROM showcases")
        result = [row[0] for row in cursor.fetchall()]
        self.close()
        return result

    def get_all_records(self) -> List[dict]:
        self.connect()
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM showcases")
        result = [dict(row) for row in cursor.fetchall()]
        self.close()
        return result

    # ── showcase_details (Мод 6) ───────────────────────────────────────────

    def upsert_details(self, record_id: int, details: dict):
        phones = details.get("phones", [])
        emails = details.get("emails", [])

        phone = phones[0] if phones else None
        email = emails[0] if emails else None

        self.connect()
        cursor = self.conn.cursor()

        cursor.execute(
            """
            UPDATE showcases SET
                phones = ?,
                emails = ?,
                head_person_fio = ?
            WHERE id = ?
        """,
            (
                phone,
                email,
                details.get("headFullName"),
                record_id,
            ),
        )

        self._commit_and_close()

    def update_region_batch(self, record_ids: List[int], region: str):
        if not record_ids:
            return
        self.connect()
        cursor = self.conn.cursor()
        placeholders = ",".join(["?"] * len(record_ids))
        cursor.execute(
            f"UPDATE showcases SET region = ? WHERE id IN ({placeholders})",
            [region] + record_ids,
        )
        self._commit_and_close()

    def get_ids_without_details(self) -> List[int]:
        self.connect()
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT id FROM showcases
            WHERE phones IS NULL OR phones = '' OR phones = '[]'
        """)
        result = [row[0] for row in cursor.fetchall()]
        self.close()
        return result

    # ── NSI справочники (Мод 2) ────────────────────────────────────────────

    def upsert_districts(self, districts: List[dict]):
        self.connect()
        cursor = self.conn.cursor()
        for d in districts:
            cursor.execute(
                """
                INSERT OR REPLACE INTO nsi_districts (id, name, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
            """,
                (d.get("id") or d.get("dicId"), d.get("name")),
            )
        self._commit_and_close()
        logger.info(f"Saved {len(districts)} districts")

    def upsert_regions(self, regions: List[dict], district_id: str):
        self.connect()
        cursor = self.conn.cursor()
        for r in regions:
            # Используем masterId для API фильтров
            region_id = r.get("masterId") or r.get("FDM-55849") or r.get("id")
            cursor.execute(
                """
                INSERT OR REPLACE INTO nsi_regions (id, name, district_id, master_id, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
                (r.get("id") or r.get("dicId"), r.get("name"), district_id, region_id),
            )
        self._commit_and_close()
        logger.info(f"Saved {len(regions)} regions for district {district_id}")

    def get_districts(self) -> List[dict]:
        self.connect()
        cursor = self.conn.cursor()
        cursor.execute("SELECT id, name, total_source FROM nsi_districts")
        result = [
            {"id": row["id"], "name": row["name"], "total_source": row["total_source"]}
            for row in cursor.fetchall()
        ]
        self.close()
        return result

    def update_district_total(self, district_id: str, total: int):
        self.connect()
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE nsi_districts SET total_source = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (total, district_id),
        )
        self._commit_and_close()
        logger.info(f"District {district_id}: total_source = {total}")

    def get_regions(self, district_id: str) -> List[dict]:
        self.connect()
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT id, name, master_id FROM nsi_regions WHERE district_id = ?",
            (district_id,),
        )
        result = [
            {"id": row["id"], "name": row["name"], "masterId": row["master_id"]}
            for row in cursor.fetchall()
        ]
        self.close()
        return result

    def get_all_regions(self) -> List[dict]:
        self.connect()
        cursor = self.conn.cursor()
        cursor.execute("SELECT id, name, master_id, district_id FROM nsi_regions")
        result = [
            {"id": row["id"], "name": row["name"], "masterId": row["master_id"]}
            for row in cursor.fetchall()
        ]
        self.close()
        return result

    # ── Метрики (Мод 7) ────────────────────────────────────────────────────

    def save_metrics(
        self,
        duration_sec: float,
        total_fetched: int,
        total_inserted: int,
        total_errors: int,
        filters_used: str = "",
    ):
        self.connect()
        cursor = self.conn.cursor()
        cursor.execute(
            """
            INSERT INTO run_metrics
                (duration_sec, total_fetched, total_inserted, total_errors, filters_used)
            VALUES (?, ?, ?, ?, ?)
        """,
            (duration_sec, total_fetched, total_inserted, total_errors, filters_used),
        )
        self._commit_and_close()
