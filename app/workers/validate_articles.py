from __future__ import annotations

import json

from app.utils import html_contains_phrase, word_count_from_html
from app.workers._common import ensure_run_log, finish_run_log


def run(*, db, settings, logger, limit: int = 10) -> int:
    run_id = ensure_run_log(db, "validate_articles")
    rows = db.fetchall(
        """
        SELECT agq.id AS queue_id, ab.primary_keyword, ab.secondary_keywords_json, ad.id AS draft_id,
               ad.title_tag, ad.h1, ad.body_html, ad.meta_description, ad.validation_json
        FROM article_generation_queue agq
        JOIN article_briefs ab ON ab.queue_id = agq.id
        JOIN article_drafts ad ON ad.queue_id = agq.id
        WHERE agq.status='drafted'
        ORDER BY agq.priority DESC, agq.queued_at ASC
        LIMIT ?
        """,
        [limit],
    )
    processed = 0
    for row in rows:
        primary = str(row["primary_keyword"])
        secondaries = json.loads(row["secondary_keywords_json"] or "[]")
        body_html = row["body_html"] or ""
        score = 0
        if primary.lower() in str(row["title_tag"] or "").lower():
            score += 20
        if primary.lower() in str(row["h1"] or "").lower():
            score += 15
        if html_contains_phrase(body_html, primary):
            score += 10
        matched_secondaries = sum(1 for kw in secondaries if html_contains_phrase(body_html, kw))
        score += min(20, matched_secondaries * 5)
        score += 20 if word_count_from_html(body_html) >= 220 else 10
        if len(str(row["meta_description"] or "")) >= 80:
            score += 5
        if "deemerge" in body_html.lower():
            score += 10
        if body_html.count("<h2>") >= 3:
            score += 10
        status = "ready" if score >= 70 else "failed"
        validation = json.loads(row["validation_json"] or "{}")
        validation.update({"quality_score": score, "status": status, "matched_secondaries": matched_secondaries})
        db.execute("UPDATE article_drafts SET quality_score=?, validation_json=? WHERE id=?", [score, json.dumps(validation), row["draft_id"]])
        db.execute("UPDATE article_generation_queue SET status=? WHERE id=?", [status, row["queue_id"]])
        processed += 1
    finish_run_log(db, run_id, "success", items_processed=processed)
    logger.info("Validated %s article drafts", processed)
    return 0
