import sys
import time
from loguru import logger

from src.utils.api_tools import RosreestrAPIClient, FilterBuilder
from src.utils.db_tools import Database
from src.utils.xlsx_tools import XLSXExporter
from src.utils.log_tools import setup_logging
from config import PAGE_SIZE, MAX_PAGES_BEFORE_FILTER


setup_logging()


def _fetch_direction(
    client: RosreestrAPIClient,
    db: Database,
    sort_dir: str,
    existing_ids: set,
    filters: dict,
) -> tuple[int, int]:
    """Выполняет пагинацию в одном направлении сортировки.
    Возвращает (fetched, inserted)."""
    fetched = inserted = page = 0
    while page < 20:  # API ограничивает 20 страниц в одном направлении
        offset = page * PAGE_SIZE
        data = client.fetch_page(offset, PAGE_SIZE, sort_dir=sort_dir, filters=filters)
        if not data:
            logger.warning(f"Failed to fetch offset={offset} sort={sort_dir}, skipping")
            break

        items = data.get("items", [])
        if not items:
            break

        new_items = [r for r in items if r.get("id") not in existing_ids]
        if new_items:
            n = db.insert_batch(new_items)
            inserted += n
            existing_ids.update(r.get("id") for r in new_items)

        fetched += len(items)

        if len(items) < PAGE_SIZE:
            break
        page += 1

    return fetched, inserted


def run_parse(
    client: RosreestrAPIClient,
    db: Database,
    filters: dict = None,
    depth: int = 0,
) -> tuple[int, int]:
    """Рекурсивно парсит с автоматическим сплитом по регионам при > MAX_PAGES_BEFORE_FILTER.
    Возвращает (total_fetched, total_inserted)."""
    total_pages = client.get_page_count(filters)
    label = f"filters={filters}" if filters else "no filters"
    logger.info(f"[depth={depth}] {label}: total_pages={total_pages}")

    # Мод 3+4: слишком много страниц → сплит по регионам
    if total_pages > MAX_PAGES_BEFORE_FILTER and depth == 0:
        logger.info("Too many pages, splitting by federal district/region...")

        districts = db.get_districts()
        if not districts:
            districts = client.fetch_federal_districts()
            if districts:
                db.upsert_districts(districts)

        if not districts:
            logger.warning("No districts available, proceeding without split")
        else:
            total_f = total_i = 0
            for district in districts:
                regions = db.get_regions(district["id"])
                if not regions:
                    regions = client.fetch_regions(district["id"])
                    if regions:
                        db.upsert_regions(regions, district["id"])

                for region in regions:
                    region_filter = FilterBuilder().with_region([region["id"]]).build()
                    f, i = run_parse(client, db, filters=region_filter, depth=depth + 1)
                    total_f += f
                    total_i += i
            return total_f, total_i

    existing_ids = set(db.get_all_ids())
    total_fetched = total_inserted = 0

    # Мод 4: обход лимита — два направления
    directions = ["desc", "asc"] if total_pages > 20 else ["desc"]
    for direction in directions:
        f, i = _fetch_direction(client, db, direction, existing_ids, filters)
        total_fetched += f
        total_inserted += i
        logger.info(f"Direction '{direction}': fetched={f}, inserted={i}")

    return total_fetched, total_inserted


def fetch_extended_data(client: RosreestrAPIClient, db: Database):
    """Мод 6: загружает расширенные данные для записей без деталей."""
    ids = db.get_ids_without_details()
    if not ids:
        logger.info("All records already have details")
        return

    logger.info(f"Fetching details for {len(ids)} records...")
    for i, record_id in enumerate(ids, start=1):
        raw = client.fetch_details(record_id)
        if raw:
            details = client.enrich_details(raw)
            db.upsert_details(record_id, details)
        if i % 100 == 0:
            logger.info(f"Details progress: {i}/{len(ids)}")


def main():
    logger.info("Starting FSA parser")
    start_time = time.time()
    total_errors = 0

    client = RosreestrAPIClient()
    db = Database()
    db.init_db()

    logger.info(f"Records in DB: {db.get_count()}")

    # Основной парсинг
    try:
        total_fetched, total_inserted = run_parse(client, db)
        logger.info(f"Parsing done: fetched={total_fetched}, inserted={total_inserted}")
    except Exception as e:
        logger.error(f"Parsing failed: {e}")
        total_fetched = total_inserted = 0
        total_errors += 1

    # Мод 6: расширенные данные
    try:
        fetch_extended_data(client, db)
    except Exception as e:
        logger.error(f"Extended data fetch failed: {e}")
        total_errors += 1

    # Мод 7: сохранить метрики
    duration = time.time() - start_time
    db.save_metrics(
        duration_sec=duration,
        total_fetched=total_fetched,
        total_inserted=total_inserted,
        total_errors=total_errors,
    )

    # Мод 5: экспорт в XLSX
    logger.info("Exporting to XLSX...")
    records = db.get_all_records()
    exporter = XLSXExporter("data/export.xlsx")
    exporter.export(records)

    logger.info(f"Done. Total records in DB: {db.get_count()}, duration: {duration:.1f}s")
    db.close()


if __name__ == "__main__":
    main()
