from __future__ import annotations

import hashlib
import json

from app.services.webflow_service import WebflowService
from app.workers._common import ensure_run_log, finish_run_log


def _payload_hash(payload: dict) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def run(*, db, settings, logger, limit: int = 10) -> int:
    run_id = ensure_run_log(db, "webflow_sync_rewrites")
    service = WebflowService(settings)
    rows = db.fetchall(
        """
        SELECT rq.id AS queue_id, cp.webflow_item_id AS existing_item_id, pv.id AS version_id, cp.slug, pv.title_tag, pv.h1,
               pv.meta_description, pv.body_html
        FROM recovery_queue rq
        JOIN content_pages cp ON cp.id = rq.page_id
        JOIN page_versions pv ON pv.page_id = rq.page_id AND pv.id = (SELECT MAX(id) FROM page_versions WHERE page_id = rq.page_id)
        LEFT JOIN webflow_items wi ON wi.page_type='rewrite' AND wi.source_id=pv.id
        WHERE rq.status='ready' AND wi.id IS NULL
        ORDER BY rq.priority DESC, rq.queued_at ASC
        LIMIT ?
        """,
        [limit],
    )
    processed = 0
    for row in rows:
        field_data = {
            "name": row["h1"],
            "slug": row["slug"],
            "post-body": row["body_html"],
            "seo-title": row["title_tag"],
            "seo-description": row["meta_description"],
        }
        item_id = row["existing_item_id"]
        sync_status = "staged"
        if service.enabled and item_id:
            try:
                service.update_staged_item(item_id, field_data, is_draft=True)
            except Exception as exc:
                logger.warning("Webflow rewrite sync fallback used for version %s: %s", row["version_id"], exc)
                sync_status = "local_only"
        else:
            sync_status = "local_only"
        db.execute(
            """
            INSERT INTO webflow_items(page_type, source_id, collection_id, item_id, slug, is_draft, sync_status, last_sync_at, payload_hash)
            VALUES ('rewrite', ?, ?, ?, ?, 1, ?, CURRENT_TIMESTAMP, ?)
            ON CONFLICT(page_type, source_id) DO UPDATE SET
              item_id=excluded.item_id,
              slug=excluded.slug,
              is_draft=excluded.is_draft,
              sync_status=excluded.sync_status,
              last_sync_at=CURRENT_TIMESTAMP,
              payload_hash=excluded.payload_hash,
              updated_at=CURRENT_TIMESTAMP
            """,
            [row["version_id"], settings.webflow_collection_id or "local", item_id, row["slug"], sync_status, _payload_hash(field_data)],
        )
        db.execute("UPDATE recovery_queue SET status='scheduled' WHERE id=?", [row["queue_id"]])
        processed += 1
    finish_run_log(db, run_id, "success", items_processed=processed)
    logger.info("Synced %s rewrites", processed)
    return 0
