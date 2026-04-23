from __future__ import annotations

from app.html_tools import has_forbidden_wrapper, has_generic_meta, has_h1_tag, has_required_heading, has_unsupported_claim
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
        body = row['body_html'] or ''
        ok = all(bool(row[key]) for key in ['title_tag', 'meta_description', 'h1', 'body_html'])
        ok = ok and not has_forbidden_wrapper(body)
        ok = ok and not has_h1_tag(body)
        ok = ok and not has_generic_meta(row['meta_description'] or '')
        ok = ok and not has_unsupported_claim(body)
        ok = ok and has_required_heading(body, 'How DEEMERGE solves this in practice')
        ok = ok and has_required_heading(body, 'Next step with DEEMERGE')
        db.execute('UPDATE recovery_queue SET status=? WHERE id=?', ['ready' if ok else 'needs_review', row['queue_id']])
        processed += 1
    finish_run_log(db, run_id, 'success', items_processed=processed)
    logger.info('Validated %s rewrite drafts', processed)
    return 0
