from __future__ import annotations

import json

from app.workers._common import ensure_run_log, finish_run_log


def run(*, db, settings, logger, limit: int = 10) -> int:
    run_id = ensure_run_log(db, "validate_articles")
    rows = db.fetchall(
        """
        SELECT ad.id, ad.queue_id, ad.title_tag, ad.meta_description, ad.h1, ad.body_html, ad.slug
        FROM article_drafts ad
        JOIN article_generation_queue q ON q.id = ad.queue_id
        WHERE q.status='drafted'
        ORDER BY ad.id ASC
        LIMIT ?
        """,
        [limit],
    )
    processed = 0
    for row in rows:
        checks = {
            "has_title": bool(row["title_tag"]),
            "has_meta": bool(row["meta_description"]),
            "has_h1": bool(row["h1"]),
            "has_body": bool(row["body_html"]),
            "has_slug": bool(row["slug"]),
            "mentions_deemerge": "deemerge" in (row["body_html"] or "").lower(),
        }
        quality = sum(1 for value in checks.values() if value) / len(checks) * 100
        db.execute(
            "UPDATE article_drafts SET quality_score=?, validation_json=? WHERE id=?",
            [quality, json.dumps(checks), row["id"]],
        )
        next_status = "ready" if quality >= 80 else "needs_review"
        db.execute("UPDATE article_generation_queue SET status=? WHERE id=?", [next_status, row["queue_id"]])
        processed += 1
    finish_run_log(db, run_id, "success", items_processed=processed)
    logger.info("Validated %s article drafts", processed)
    return 0
