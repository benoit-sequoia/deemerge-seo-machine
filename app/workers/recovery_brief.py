from __future__ import annotations

from app.workers._common import ensure_run_log, finish_run_log


def run(*, db, settings, logger, limit: int = 10) -> int:
    run_id = ensure_run_log(db, "recovery_brief")
    rows = db.fetchall(
        """
        SELECT rq.id AS queue_id, cp.id AS page_id, cp.slug, cp.title_current, rc.top_queries_json
        FROM recovery_queue rq
        JOIN content_pages cp ON cp.id = rq.page_id
        JOIN recovery_candidates rc ON rc.id = rq.candidate_id
        WHERE rq.status='queued'
        ORDER BY rq.priority DESC, rq.id ASC
        LIMIT ?
        """,
        [limit],
    )
    for row in rows:
        db.execute("UPDATE recovery_queue SET status='briefing' WHERE id=?", [row["queue_id"]])
    finish_run_log(db, run_id, "success", items_processed=len(rows))
    logger.info("Prepared %s recovery brief jobs", len(rows))
    return 0
