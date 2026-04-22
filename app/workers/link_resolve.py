from __future__ import annotations

import json

from app.utils import append_related_links
from app.workers._common import ensure_run_log, finish_run_log


def _related_pages(db, cluster_id: int | None, exclude_page_id: int | None = None, limit: int = 4) -> list[tuple[str, str]]:
    if not cluster_id:
        rows = db.fetchall("SELECT id, title_current, page_url FROM content_pages WHERE page_type='blog' ORDER BY COALESCE(last_published_at, '1970-01-01 00:00:00') DESC, id DESC LIMIT ?", [limit])
    else:
        if exclude_page_id:
            rows = db.fetchall(
                "SELECT id, title_current, page_url FROM content_pages WHERE page_type='blog' AND cluster_id=? AND id<>? ORDER BY id DESC LIMIT ?",
                [cluster_id, exclude_page_id, limit],
            )
        else:
            rows = db.fetchall(
                "SELECT id, title_current, page_url FROM content_pages WHERE page_type='blog' AND cluster_id=? ORDER BY id DESC LIMIT ?",
                [cluster_id, limit],
            )
    return [(str(r["title_current"] or r["page_url"]), str(r["page_url"])) for r in rows]


def run(*, db, settings, logger, limit: int = 10) -> int:
    run_id = ensure_run_log(db, "link_resolve")
    processed = 0

    article_rows = db.fetchall(
        """
        SELECT ad.id AS draft_id, agq.id AS queue_id, kc.cluster_id, ad.body_html, ad.validation_json
        FROM article_generation_queue agq
        JOIN keyword_candidates kc ON kc.id = agq.keyword_candidate_id
        JOIN article_drafts ad ON ad.queue_id = agq.id
        WHERE agq.status='ready'
        ORDER BY agq.priority DESC, agq.queued_at ASC
        LIMIT ?
        """,
        [limit],
    )
    for row in article_rows:
        validation = json.loads(row["validation_json"] or "{}")
        if validation.get("links_resolved"):
            continue
        related = _related_pages(db, row["cluster_id"], None, 4)
        body_html = append_related_links(str(row["body_html"]), related, include_demo=True)
        validation["links_resolved"] = True
        validation["internal_links"] = len(related) + 2
        db.execute("UPDATE article_drafts SET body_html=?, validation_json=? WHERE id=?", [body_html, json.dumps(validation), row["draft_id"]])
        processed += 1

    rewrite_rows = db.fetchall(
        """
        SELECT pv.id AS version_id, rq.page_id, cp.cluster_id, pv.body_html, pv.notes_json
        FROM recovery_queue rq
        JOIN content_pages cp ON cp.id = rq.page_id
        JOIN page_versions pv ON pv.page_id = rq.page_id AND pv.id = (SELECT MAX(id) FROM page_versions WHERE page_id = rq.page_id)
        WHERE rq.status='ready'
        ORDER BY pv.id DESC
        LIMIT ?
        """,
        [limit],
    )
    for row in rewrite_rows:
        notes = json.loads(row["notes_json"] or "{}")
        validation = notes.get("validation", {})
        if validation.get("links_resolved"):
            continue
        related = _related_pages(db, row["cluster_id"], row["page_id"], 4)
        body_html = append_related_links(str(row["body_html"]), related, include_demo=True)
        validation["links_resolved"] = True
        validation["internal_links"] = len(related) + 2
        notes["validation"] = validation
        db.execute("UPDATE page_versions SET body_html=?, notes_json=? WHERE id=?", [body_html, json.dumps(notes), row["version_id"]])
        processed += 1

    finish_run_log(db, run_id, "success", items_processed=processed)
    logger.info("Resolved internal links for %s items", processed)
    return 0
