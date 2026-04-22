from __future__ import annotations

import json

from app.utils import html_contains_phrase, word_count_from_html
from app.workers._common import ensure_run_log, finish_run_log


def run(*, db, settings, logger, limit: int = 10) -> int:
    run_id = ensure_run_log(db, "validate_rewrites")
    rows = db.fetchall(
        """
        SELECT rq.id AS queue_id, rb.primary_query, pv.id AS version_id, pv.title_tag, pv.h1, pv.intro_html, pv.body_html, pv.notes_json
        FROM recovery_queue rq
        JOIN recovery_briefs rb ON rb.queue_id = rq.id
        JOIN page_versions pv ON pv.page_id = rq.page_id AND pv.id = (SELECT MAX(id) FROM page_versions WHERE page_id = rq.page_id)
        WHERE rq.status='drafted'
        ORDER BY pv.id DESC
        LIMIT ?
        """,
        [limit],
    )
    processed = 0
    for row in rows:
        primary = str(row["primary_query"] or "").strip()
        body_html = row["body_html"] or ""
        score = 0
        if primary and primary.lower() in str(row["title_tag"] or "").lower():
            score += 25
        if primary and primary.lower() in str(row["h1"] or "").lower():
            score += 20
        if primary and html_contains_phrase(str(row["intro_html"] or ""), primary):
            score += 15
        score += 20 if word_count_from_html(body_html) >= 180 else 10
        if "deemerge" in body_html.lower():
            score += 10
        if body_html.count("<a href=") >= 2:
            score += 10
        status = "ready" if score >= 70 else "failed"
        notes = json.loads(row["notes_json"] or "{}")
        notes["validation"] = {"quality_score": score, "status": status}
        db.execute("UPDATE page_versions SET notes_json=? WHERE id=?", [json.dumps(notes), row["version_id"]])
        db.execute("UPDATE recovery_queue SET status=? WHERE id=?", [status, row["queue_id"]])
        processed += 1
    finish_run_log(db, run_id, "success", items_processed=processed)
    logger.info("Validated %s rewrite drafts", processed)
    return 0
