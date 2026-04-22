from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

from app.services.gsc_service import GSCService
from app.workers._common import ensure_run_log, finish_run_log


def run(*, db, settings, logger, limit: int = 10) -> int:
    run_id = ensure_run_log(db, "gsc_inspect_recent")
    service = GSCService(settings)
    cutoff = (datetime.now(UTC) - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
    pages = db.fetchall(
        "SELECT page_url FROM content_pages WHERE last_published_at IS NOT NULL AND last_published_at >= ? ORDER BY last_published_at DESC LIMIT ?",
        [cutoff, limit],
    )
    processed = 0
    site_id = int(db.fetchone("SELECT id FROM sites WHERE site_key='deemerge'")["id"])
    for row in pages:
        data = service.inspect_url(str(row["page_url"]))
        result = data.get("inspectionResult", {}).get("indexStatusResult", {})
        db.execute(
            """
            INSERT INTO gsc_url_inspections(site_id, page_url, inspection_ts, coverage_state, indexing_state, last_crawl_time, canonical_url, raw_json)
            VALUES (?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?)
            """,
            [site_id, row["page_url"], result.get("coverageState"), result.get("indexingState"), result.get("lastCrawlTime"), result.get("googleCanonical"), json.dumps(data)],
        )
        processed += 1
    finish_run_log(db, run_id, "success", items_processed=processed)
    logger.info("Inspected %s recent pages", processed)
    return 0
