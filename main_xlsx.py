import sys
from pathlib import Path
from datetime import datetime

from loguru import logger

from src.utils.db_tools import Database
from src.utils.xlsx_tools import XLSXExporter
from src.utils.log_tools import setup_logging

setup_logging()


def main(district: str = None, region: str = None):
    logger.info(f"Starting XLSX export: district={district}, region={region}")

    db = Database()
    records = db.get_all_records()

    if district and district != "Все":
        records = [r for r in records if r.get("federal_district") == district]
        logger.info(f"Filtered by district '{district}': {len(records)} records")

    if region and region != "Все":
        records = [r for r in records if r.get("region") == region]
        logger.info(f"Filtered by region '{region}': {len(records)} records")

    if not records:
        logger.warning("No records to export")
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"data/export_{timestamp}.xlsx"

    exporter = XLSXExporter(output_path)
    exporter.export(records)

    logger.info(f"Export complete: {output_path}")
    db.close()


if __name__ == "__main__":
    district_arg = sys.argv[1] if len(sys.argv) > 1 else None
    region_arg = sys.argv[2] if len(sys.argv) > 2 else None

    if district_arg == "":
        district_arg = None
    if region_arg == "":
        region_arg = None

    main(district_arg, region_arg)
