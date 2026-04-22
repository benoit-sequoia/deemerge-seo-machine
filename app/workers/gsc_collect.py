from __future__ import annotations

from datetime import date, timedelta

from app.services.gsc_service import GSCService
from app.workers._common import ensure_run_log, finish_run_log


def _daterange(days: int) -> list[str]:
    end = date.today() - timedelta(days=1)
    return [(end - timedelta(days=i)).isoformat() for i in range(days)][::-1]


def run(*, db, settings, logger, limit: int = 10) -> int:
    run_id = ensure_run_log(db, "gsc_collect")
    site_row = db.fetchone("SELECT id FROM sites WHERE site_key='deemerge'")
    if not site_row:
        raise RuntimeError("Site row missing. Run seed_base first.")
    site_id = int(site_row["id"])

    service = GSCService(settings)
    pages = service.query_pages(days=28)
    queries = service.query_queries(days=28)
    dates = _daterange(28)

    # Lightweight fixture style load until live API wiring is added.
    processed = 0
    for row in pages:
        for d in dates:
            db.execute(
                """
                INSERT OR REPLACE INTO gsc_page_daily(site_id, date, page_url, clicks, impressions, ctr, position, data_state)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'final')
                """,
                [site_id, d, row["page_url"], row["clicks"] / 28.0, row["impressions"] / 28.0, row["ctr"], row["position"]],
            )
            processed += 1
    for row in queries:
        for d in dates:
            db.execute(
                """
                INSERT OR REPLACE INTO gsc_query_daily(site_id, date, query, page_url, clicks, impressions, ctr, position, data_state)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'final')
                """,
                [site_id, d, row["query"], row["page_url"], row["clicks"] / 28.0, row["impressions"] / 28.0, row["ctr"], row["position"]],
            )
            processed += 1
    finish_run_log(db, run_id, "success", items_processed=processed)
    logger.info("Stored GSC fixture data rows: %s", processed)
    return 0
