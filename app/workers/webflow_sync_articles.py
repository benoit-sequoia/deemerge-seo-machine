from __future__ import annotations

import hashlib
import json

from app.services.webflow_service import WebflowService
from app.workers._common import ensure_run_log, finish_run_log


def _payload_hash(payload: dict) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def run(*, db, settings, logger, limit: int = 10) -> int:
    run_id = ensure_run_log(db, "webflow_sync_articles")
    service = WebflowService(settings)
    rows = db.fetchall(
        """
        SELECT ad.id AS draft_id, ad.slug, ad.title_tag, ad.h1, ad.meta_description, ad.excerpt, ad.body_html,
               agq.id AS queue_id
        FROM article_generation_queue agq
        JOIN article_drafts ad ON ad.queue_id = agq.id
        LEFT JOIN webflow_items wi ON wi.page_type='new_article' AND wi.source_id=ad.id
        WHERE agq.status='ready' AND wi.id IS NULL
        ORDER BY agq.priority DESC, agq.queued_at ASC
        LIMIT ?
        """,
        [limit],
    )
    processed = 0
    for row in rows:
        field_data = {
            "name": row["h1"],
            "slug": row["slug"],
            "summary": row["excerpt"],
            "post-body": row["body_html"],
            "seo-title": row["title_tag"],
            "seo-description": row["meta_description"],
        }
        remote_item_id = None
        sync_status = "staged"
        if service.enabled:
            try:
                data = service.create_staged_item(field_data, is_draft=True)
                remote_item_id = data.get("id") or (data.get("items") or [{}])[0].get("id")
            except Exception as exc:
                logger.warning("Webflow article sync fallback used for draft %s: %s", row["draft_id"], exc)
                sync_status = "local_only"
        else:
            sync_status = "local_only"
        db.execute(
            """
            INSERT INTO webflow_items(page_type, source_id, collection_id, item_id, slug, is_draft, sync_status, last_sync_at, payload_hash)
            VALUES ('new_article', ?, ?, ?, ?, 1, ?, CURRENT_TIMESTAMP, ?)
            ON CONFLICT(page_type, source_id) DO UPDATE SET
              item_id=excluded.item_id,
              slug=excluded.slug,
              is_draft=excluded.is_draft,
              sync_status=excluded.sync_status,
              last_sync_at=CURRENT_TIMESTAMP,
              payload_hash=excluded.payload_hash,
              updated_at=CURRENT_TIMESTAMP
            """,
            [row["draft_id"], settings.webflow_collection_id or "local", remote_item_id, row["slug"], sync_status, _payload_hash(field_data)],
        )
        db.execute("UPDATE article_generation_queue SET status='scheduled' WHERE id=?", [row["queue_id"]])
        processed += 1
    finish_run_log(db, run_id, "success", items_processed=processed)
    logger.info("Synced %s article drafts", processed)
    return 0
