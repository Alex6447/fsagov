import time
import sys
from pathlib import Path
from datetime import datetime
import argparse

from loguru import logger

from config import PAGE_SIZE
from src.utils.api_tools import RosreestrAPIClient, FilterBuilder
from src.utils.db_tools import Database
from src.utils.log_tools import setup_logging

# Parse command line args
parser = argparse.ArgumentParser()
parser.add_argument("--token", type=str, default="", help="Override fgis_token")
args, _ = parser.parse_known_args()

if args.token:
    import config

    config.BEARER_TOKEN = args.token

setup_logging()


def fetch_new_only(
    client: RosreestrAPIClient, db: Database, region_id: int, region_name: str
) -> tuple[int, int]:
    """Получить только новые записи для региона.
    Возвращает (fetched, inserted)."""
    fetched = inserted = 0
    existing_ids = set(db.get_all_ids())

    filter_builder = FilterBuilder().with_region([region_id])
    region_filter = filter_builder.build()

    page = 0
    while True:
        data = client.fetch_page(
            page, PAGE_SIZE, sort_dir="desc", filters=region_filter
        )
        if not data:
            break

        items = data.get("items", [])
        if not items:
            break

        if fetched == 0:
            total = data.get("total", 0)
            logger.info(f"Region '{region_name}': total={total}")

        new_items = []
        for item in items:
            item_id = item.get("id")
            if item_id not in existing_ids:
                new_items.append(item)
                existing_ids.add(item_id)
            else:
                logger.info(f"Found existing id={item_id}, stopping for this region")
                break

        if new_items:
            for item in new_items:
                item["region"] = region_name
            n = db.insert_batch(new_items)
            inserted += n
            fetched += len(new_items)

            for item in new_items:
                record_id = item.get("id")
                raw = client.fetch_details(record_id)
                if raw:
                    details = client.enrich_details(raw)
                    db.upsert_details(record_id, details)
                time.sleep(0.5)

        if len(items) < PAGE_SIZE:
            break

        page += 1

    logger.info(f"Region '{region_name}': fetched={fetched}, inserted={inserted}")
    return fetched, inserted


def main():
    logger.info("Starting update: fetching only new records")
    start_time = time.time()
    total_errors = 0

    client = RosreestrAPIClient()
    db = Database()
    db.init_db()

    existing_count = db.get_count()
    logger.info(f"Records in DB before update: {existing_count}")

    districts = db.get_districts()
    if not districts:
        logger.info("No districts in DB, fetching from API")
        districts = client.fetch_federal_districts()
        if districts:
            db.upsert_districts(districts)

    total_fetched = total_inserted = 0

    for district in districts:
        regions = db.get_regions(district["id"])
        if not regions:
            regions = client.fetch_regions(district["id"])
            if regions:
                db.upsert_regions(regions, district["id"])
            time.sleep(1)

        for region in regions:
            region_id = (
                region.get("masterId") or region.get("FDM-55849") or region["id"]
            )
            region_name = region.get("name", region_id)

            try:
                f, i = fetch_new_only(client, db, region_id, region_name)
                total_fetched += f
                total_inserted += i
            except Exception as e:
                logger.error(f"Error fetching region '{region_name}': {e}")
                total_errors += 1

    duration = time.time() - start_time

    final_count = db.get_count()
    new_records = final_count - existing_count

    logger.info(
        f"Update complete: fetched={total_fetched}, inserted={total_inserted}, new_total={final_count}"
    )
    logger.info(f"Duration: {duration:.1f}s")

    db.save_metrics(
        duration_sec=duration,
        total_fetched=total_fetched,
        total_inserted=total_inserted,
        total_errors=total_errors,
    )

    db.close()


if __name__ == "__main__":
    main()
