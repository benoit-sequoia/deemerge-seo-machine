from __future__ import annotations

from app.html_tools import has_forbidden_wrapper
from app.workers._common import ensure_run_log, finish_run_log


def run(*, db, settings, logger, limit: int = 10) -> int:
    run_id = ensure_run_log(db, 'validate_rewrites')
    rows = db.fetchall(
        """
        SELECT rq.id AS queue_id, pv.id AS version_id, pv.title_tag, pv.meta_description, pv.h1, pv.body_html
        FROM recovery_queue rq
        JOIN page_versions pv ON pv.page_id = rq.page_id
        WHERE rq.status='drafted'
        ORDER BY pv.id DESC
        LIMIT ?
        """,
        [limit],
    )
    processed = 0
    for row in rows:
        ok = all(bool(row[key]) for key in ['title_tag', 'meta_description', 'h1', 'body_html']) and not has_forbidden_wrapper(row['body_html'] or '')
        db.execute('UPDATE recovery_queue SET status=? WHERE id=?', ['ready' if ok else 'needs_review', row['queue_id']])
        processed += 1
    finish_run_log(db, run_id, 'success', items_processed=processed)
    logger.info('Validated %s rewrite drafts', processed)
    return 0
