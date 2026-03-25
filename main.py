from loguru import logger
import sys
import json
from pathlib import Path
from tqdm import tqdm

from parser import RosreestrAPIClient, Database
from config import PAGE_SIZE, DELAY_BETWEEN_REQUESTS, DB_PATH


logger.add('logs\\log.log', level="DEBUG")

STATE_FILE = Path("data/parser_state.json")


def save_state(page: int):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump({"last_page": page}, f)


def load_state() -> int:
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, "r") as f:
                data = json.load(f)
                return data.get("last_page", 0)
        except Exception:
            pass
    return 0


def main():
    logger.info("Starting Росреестр parser")

    client = RosreestrAPIClient()
    db = Database()

    logger.info("Initializing database")
    db.init_db()

    current_count = db.get_count()
    logger.info(f"Current records in DB: {current_count}")

    total = client.get_total()
    if not total:
        logger.error("Failed to get total records count")
        return

    logger.info(f"Total records to fetch: {total}")

    if current_count >= total:
        logger.info("Database is up to date")
        db.close()
        return

    start_page = load_state()
    logger.info(f"Resuming from page: {start_page}")

    remaining = total - current_count
    pages = (remaining + PAGE_SIZE - 1) // PAGE_SIZE
    logger.info(f"Pages to fetch: {pages}")

    with tqdm(total=remaining, desc="Fetching records") as pbar:
        page = start_page
        while True:
            logger.debug(f"{page=}")
            data = client.fetch_by_page(page)
            if not data:
                logger.warning(f"Failed to fetch page {page}, waiting and retrying...")
                import time

                time.sleep(30)
                continue

            items = data.get("items", [])
            if not items:
                break

            db.insert_batch(items)
            pbar.update(len(items))

            logger.info(
                f"Page {page}: fetched {len(items)} records, total: {db.get_count()}"
            )

            save_state(page)

            if len(items) < PAGE_SIZE:
                break

            page += 1

    STATE_FILE.unlink(missing_ok=True)
    final_count = db.get_count()
    logger.info(f"Finished. Total records in DB: {final_count}")
    db.close()


if __name__ == "__main__":
    main()
