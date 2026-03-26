import time
import sys
from typing import Optional
import argparse

from loguru import logger

from config import PAGE_SIZE
from src.utils.api_tools import RosreestrAPIClient, FilterBuilder
from src.utils.db_tools import Database
from src.utils.xlsx_tools import XLSXExporter
from src.utils.log_tools import setup_logging

MAXPAGES = 20  # Лимит страниц на один запрос (API ограничивает 20)

# Parse command line args
parser = argparse.ArgumentParser()
parser.add_argument("--token", type=str, default="", help="Override fgis_token")
args, _ = parser.parse_known_args()

if args.token:
    # Override token in config module
    import config

    config.BEARER_TOKEN = args.token

setup_logging()


def get_page_count(client: RosreestrAPIClient, filters: dict = None) -> int:
    data = client.fetch_page(0, 1, filters=filters)
    if data:
        total = data.get("total", 0)
        return (total + PAGE_SIZE - 1) // PAGE_SIZE
    return 0


def fetch_and_insert(
    client: RosreestrAPIClient,
    db: Database,
    filters: dict,
    existing_ids: set,
    sort_dir: str = "desc",
    max_pages: int = 20,
    region_name: str = None,
) -> tuple[int, int]:
    """Получить данные и вставить в БД.
    Возвращает (fetched, inserted)."""
    fetched = inserted = 0
    total = 0

    for page in range(max_pages):
        data = client.fetch_page(page, PAGE_SIZE, sort_dir=sort_dir, filters=filters)
        if not data:
            logger.warning(f"Failed to fetch page={page} sort={sort_dir}")
            break

        items = data.get("items", [])
        if not items:
            break

        if total == 0:
            total = data.get("total", 0)

        # Добавляем region во ВСЕ записи
        if region_name:
            for item in items:
                item["region"] = region_name
            # Обновляем region для всех записей на странице
            db.update_region_batch([r.get("id") for r in items], region_name)

        new_items = [r for r in items if r.get("id") not in existing_ids]
        if new_items:
            n = db.insert_batch(new_items)
            inserted += n
            existing_ids.update(r.get("id") for r in new_items)

        # Обогащаем ВСЕ записи со страницы (включая старые) - данные могут измениться
        for item in items:
            record_id = item.get("id")
            raw = client.fetch_details(record_id)
            if raw:
                details = client.enrich_details(raw)
                db.upsert_details(record_id, details)
            time.sleep(0.5)  # пауза между запросами обогащения

        fetched += len(items)

        if fetched >= total:
            logger.info(f"  Fetched all {total} records")
            break

    logger.info(f"  {sort_dir}: fetched={fetched}, inserted={inserted}")
    return fetched, inserted


def parse_with_filters(
    client: RosreestrAPIClient,
    db: Database,
    filters: dict,
    existing_ids: set,
    depth: int = 0,
    label: str = "",
    region_name: str = None,
) -> tuple[int, int]:
    """Основная функция парсинга по логике из документа.

    Логика:
    1. Получаем count_pages для текущих фильтров
    2. Если count_pages > MAXPAGES * 2:
       - Добавляем следующий уровень фильтрации (регион -> статус -> гос)
    3. Иначе:
       - Парсим до 20 страниц (desc)
       - Если count_pages > MAXPAGES * 2/2:
          - Инвертируем сортировку (asc) и парсим до пересечения с БД
    """
    count_pages = get_page_count(client, filters)
    current_label = label or "all"
    logger.info(f"=== CHECK: {current_label} -> pages={count_pages} ===")

    total_fetched = total_inserted = 0

    # Уровень 0: фильтр по городу (region) - сначала собираем все регионы
    if depth == 0:
        # Получаем/создаём округа
        districts = db.get_districts()
        if not districts:
            districts = client.fetch_federal_districts()
            if districts:
                db.upsert_districts(districts)

        if not districts:
            logger.warning("No districts available")
            return 0, 0

        # Собираем все регионы для всех округов (один раз)
        for district in districts:
            regions = db.get_regions(district["id"])
            if not regions:
                regions = client.fetch_regions(district["id"])
                if regions:
                    db.upsert_regions(regions, district["id"])
                # Небольшая пауза между запросами округов
                time.sleep(1)

        # Теперь перебираем все регионы
        all_regions = db.get_all_regions() if hasattr(db, "get_all_regions") else []
        if not all_regions:
            for district in districts:
                regions = db.get_regions(district["id"])
                all_regions.extend(regions)

        logger.info(f"Total regions: {len(all_regions)}")

        for region in all_regions:
            region_id = (
                region.get("masterId") or region.get("FDM-55849") or region["id"]
            )
            region_name = region.get("name", region_id)

            region_filter = FilterBuilder().with_region([region_id]).build()
            f, i = parse_with_filters(
                client,
                db,
                region_filter,
                existing_ids,
                depth=1,
                label=f"region={region_name}",
                region_name=region_name,
            )
            total_fetched += f
            total_inserted += i

        return total_fetched, total_inserted

    # Уровень 1: фильтр по статусу (если страниц > MAXPAGES * 2)
    if depth == 1 and count_pages > MAXPAGES * 2:
        logger.info(
            f">>> SPLIT: {count_pages} > {MAXPAGES * 2} -> adding STATUS filter"
        )

        statuses = get_statuses(client)
        for status_id, status_name in statuses:
            status_filter = {**filters}
            status_filter["idStatus"] = [status_id]

            f, i = parse_with_filters(
                client,
                db,
                status_filter,
                existing_ids,
                depth=2,
                label=f"status={status_name}",
                region_name=region_name,
            )
            total_fetched += f
            total_inserted += i

        return total_fetched, total_inserted

    # Уровень 1: фильтр по статусу (если страниц > MAXPAGES * 2)
    if depth == 1 and count_pages > MAXPAGES * 2:
        logger.info(
            f">>> SPLIT: {count_pages} > {MAXPAGES * 2} -> adding STATUS filter"
        )

        statuses = get_statuses(client)
        for status_id, status_name in statuses:
            status_filter = {**filters}
            status_filter["idStatus"] = [status_id]

            f, i = parse_with_filters(
                client,
                db,
                status_filter,
                existing_ids,
                depth=2,
                label=f"status={status_name}",
                region_name=region_name,
            )
            total_fetched += f
            total_inserted += i

        return total_fetched, total_inserted

    # Уровень 2: фильтр по гос-компании (если страниц > MAXPAGES * 2)
    if depth == 2 and count_pages > MAXPAGES * 2:
        logger.info(f">>> SPLIT: {count_pages} > {MAXPAGES * 2} -> adding GOV filter")

        for is_gov in [True, False]:
            gov_filter = {**filters}
            gov_filter["isGovernmentCompany"] = [is_gov]
            gov_label = "gov" if is_gov else "private"

            f, i = parse_with_filters(
                client,
                db,
                gov_filter,
                existing_ids,
                depth=3,
                label=f"{gov_label}",
                region_name=region_name,
            )
            total_fetched += f
            total_inserted += i

        return total_fetched, total_inserted

    # Уровень 2: фильтр по гос-компании (если страниц > MAXPAGES * 2)
    if depth == 2 and count_pages > MAXPAGES * 2:
        logger.info(f">>> SPLIT: {count_pages} > {MAXPAGES * 2} -> adding GOV filter")

        for is_gov in [True, False]:
            gov_filter = {**filters}
            gov_filter["isGovernmentCompany"] = [is_gov]
            gov_label = "gov" if is_gov else "private"

            f, i = parse_with_filters(
                client,
                db,
                gov_filter,
                existing_ids,
                depth=3,
                label=f"{gov_label}",
            )
            total_fetched += f
            total_inserted += i

        return total_fetched, total_inserted

    # Уровень 3 или финальный: парсим данные
    logger.info(f">>> PARSING: {count_pages} pages (max 20 per direction)")

    f, i = fetch_and_insert(
        client,
        db,
        filters,
        existing_ids,
        sort_dir="desc",
        max_pages=20,
        region_name=region_name,
    )
    total_fetched += f
    total_inserted += i

    # Если страниц больше MAXPAGES - парсим в обратную сторону
    if count_pages > MAXPAGES:
        logger.info(f">>> REVERSE: {count_pages} > {MAXPAGES} -> parsing asc direction")
        f, i = fetch_and_insert(
            client,
            db,
            filters,
            existing_ids,
            sort_dir="asc",
            max_pages=20,
            region_name=region_name,
        )
        total_fetched += f
        total_inserted += i

    return total_fetched, total_inserted


def get_statuses(client: RosreestrAPIClient) -> list:
    """Список статусов: 1-Архивный, 6-Действует, 14-Прекращен, 15-Приостановлен, 19-Частично приостановлен"""
    full_status_list = [
        (1, "Архивный"),
        (6, "Действует"),
        (14, "Прекращен"),
        (15, "Приостановлен"),
        (19, "Частично приостановлен"),
    ]
    logger.info(f"Using statuses: {full_status_list}")
    return full_status_list


def main():
    logger.info("Starting FSA parser")
    start_time = time.time()
    total_errors = 0

    client = RosreestrAPIClient()
    db = Database()
    db.init_db()

    logger.info(f"Records in DB: {db.get_count()}")

    existing_ids = set(db.get_all_ids())

    try:
        total_fetched, total_inserted = parse_with_filters(
            client, db, {}, existing_ids, depth=0
        )
        logger.info(f"Parsing done: fetched={total_fetched}, inserted={total_inserted}")
    except Exception as e:
        logger.error(f"Parsing failed: {e}")
        total_fetched = total_inserted = 0
        total_errors += 1

    try:
        fetch_extended_data(client, db)
    except Exception as e:
        logger.error(f"Extended data fetch failed: {e}")
        total_errors += 1

    duration = time.time() - start_time
    db.save_metrics(
        duration_sec=duration,
        total_fetched=total_fetched,
        total_inserted=total_inserted,
        total_errors=total_errors,
    )

    logger.info("Exporting to XLSX...")
    records = db.get_all_records()
    exporter = XLSXExporter("data/export.xlsx")
    exporter.export(records)

    logger.info(
        f"Done. Total records in DB: {db.get_count()}, duration: {duration:.1f}s"
    )
    db.close()


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


if __name__ == "__main__":
    main()
