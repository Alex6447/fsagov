import sqlite3
from loguru import logger
from pathlib import Path
from typing import List, Optional
from datetime import date

from config import DB_PATH


class Database:
    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn: Optional[sqlite3.Connection] = None

    def connect(self):
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

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
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_reg_number ON showcases(reg_number)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_reg_date ON showcases(reg_date)
        """)

        self.conn.commit()
        logger.info("Database initialized")

    def insert_batch(self, records: List[dict]):
        if not records:
            return

        self.connect()
        cursor = self.conn.cursor()

        columns = [
            "id",
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

            data = (
                r.get("id"),
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
                1
                if r.get("hasEngVersion")
                else 0
                if r.get("hasEngVersion") is not None
                else None,
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
                1
                if r.get("isGovernmentCompany")
                else 0
                if r.get("isGovernmentCompany") is not None
                else None,
                1
                if r.get("isForeignOrganization")
                else 0
                if r.get("isForeignOrganization") is not None
                else None,
                r.get("insertNationalPartName"),
            )

            try:
                cursor.execute(sql, data)
                if cursor.rowcount > 0:
                    inserted += 1
            except sqlite3.IntegrityError:
                pass

        self.conn.commit()
        logger.info(f"Inserted {inserted} new records")

    def get_count(self) -> int:
        self.connect()
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM showcases")
        return cursor.fetchone()[0]
