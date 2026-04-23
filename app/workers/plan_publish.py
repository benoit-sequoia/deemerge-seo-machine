from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.workers._common import ensure_run_log, finish_run_log


def _next_slot(index: int) -> str:
    base = datetime.now(timezone.utc).replace(microsecond=0, second=0) + timedelta(minutes=5)
    slot = base + timedelta(minutes=index * 5)
    return slot.isoformat().replace('+00:00', 'Z')


def run(*, db, settings, logger, limit: int = 10) -> int:
    run_id = ensure_run_log(db, 'plan_publish')
    rows = db.fetchall(
        """
        SELECT wi.page_type, wi.source_id, wi.slug
        FROM webflow_items wi
        LEFT JOIN publish_plan wp ON wp.source_type = wi.page_type AND wp.source_id = wi.source_id AND wp.status IN ('planned','published')
        WHERE wi.sync_status='synced' AND wi.is_draft=1 AND wp.id IS NULL
        ORDER BY CASE wi.page_type WHEN 'article' THEN 0 ELSE 1 END, wi.id ASC
        LIMIT ?
        """,
        [limit],
    )
    planned = 0
    for idx, row in enumerate(rows):
        db.execute(
            "INSERT INTO publish_plan(source_type, source_id, planned_publish_ts_utc, status) VALUES (?, ?, ?, 'planned')",
            [row['page_type'], row['source_id'], _next_slot(idx)],
        )
        planned += 1
    logger.info('Planned %s publish items', planned)
    finish_run_log(db, run_id, 'success', items_processed=planned)
    return 0
