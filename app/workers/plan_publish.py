from __future__ import annotations

from datetime import UTC, datetime

from app.utils import next_weekday_slots
from app.workers._common import ensure_run_log, finish_run_log


def run(*, db, settings, logger, limit: int = 10) -> int:
    run_id = ensure_run_log(db, "plan_publish")
    existing = db.fetchall("SELECT source_type, source_id FROM publish_plan WHERE status IN ('planned','published')")
    existing_keys = {(str(r['source_type']), int(r['source_id'])) for r in existing}

    candidates = []
    for row in db.fetchall(
        """
        SELECT 'rewrite' AS source_type, pv.id AS source_id, rq.priority, rq.queued_at
        FROM recovery_queue rq
        JOIN page_versions pv ON pv.page_id = rq.page_id
        WHERE rq.status='scheduled'
        ORDER BY rq.priority DESC, rq.queued_at ASC
        LIMIT ?
        """,
        [limit * 2],
    ):
        key = (str(row["source_type"]), int(row["source_id"]))
        if key not in existing_keys:
            candidates.append(key)
    for row in db.fetchall(
        """
        SELECT 'new_article' AS source_type, ad.id AS source_id, agq.priority, agq.queued_at
        FROM article_generation_queue agq
        JOIN article_drafts ad ON ad.queue_id = agq.id
        WHERE agq.status='scheduled'
        ORDER BY agq.priority DESC, agq.queued_at ASC
        LIMIT ?
        """,
        [limit * 2],
    ):
        key = (str(row["source_type"]), int(row["source_id"]))
        if key not in existing_keys:
            candidates.append(key)

    candidates = candidates[:limit]
    slots = next_weekday_slots(len(candidates), start=datetime.now(UTC))
    processed = 0
    for (source_type, source_id), slot in zip(candidates, slots, strict=False):
        db.execute(
            "INSERT INTO publish_plan(source_type, source_id, planned_publish_ts_utc, status) VALUES (?, ?, ?, 'planned')",
            [source_type, source_id, slot.isoformat()],
        )
        processed += 1
    finish_run_log(db, run_id, "success", items_processed=processed)
    logger.info("Planned %s publish slots", processed)
    return 0
