from __future__ import annotations

import json
from datetime import datetime, timezone

from app.services.webflow_service import WebflowService
from app.workers._common import ensure_run_log, finish_run_log


def _field_data_from_version(page, version, field_map: dict[str, str]) -> dict:
    now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return {
        field_map["name"]: version["h1"] or page["title_current"],
        field_map["slug"]: page["slug"],
        field_map["summary"]: version["meta_description"] or version["h1"] or page["title_current"],
        field_map["body"]: version["body_html"] or "",
        field_map["seo_title"]: version["title_tag"] or page["title_current"],
        field_map["seo_description"]: version["meta_description"] or page["title_current"],
        field_map["published_date"]: now_iso,
    }


def run(*, db, settings, logger, limit: int = 10) -> int:
    run_id = ensure_run_log(db, "webflow_sync_rewrites")
    field_map = settings.webflow_field_map()
    service = WebflowService(settings)
    rows = db.fetchall(
        """
        SELECT pv.id AS version_id, cp.id AS page_id, cp.slug, cp.title_current, cp.webflow_item_id, pv.title_tag, pv.meta_description, pv.h1, pv.body_html
        FROM page_versions pv
        JOIN content_pages cp ON cp.id = pv.page_id
        JOIN recovery_queue rq ON rq.page_id = cp.id
        WHERE pv.source_type='rewrite' AND rq.status='ready'
        ORDER BY pv.id DESC
        LIMIT ?
        """,
        [limit],
    )
    processed = 0
    errors = 0
    for row in rows:
        try:
            item_id = row["webflow_item_id"]
            if settings.webflow_token and not item_id:
                existing = service.find_item_by_slug(row["slug"])
                item_id = existing["id"] if existing else None
            if not item_id:
                logger.warning("Skipping rewrite sync for %s because no existing Webflow item was found", row["slug"])
                continue
            field_data = _field_data_from_version(row, row, field_map)
            service.update_item(item_id, field_data, is_draft=True)
            db.execute(
                """
                INSERT INTO webflow_items(page_type, source_id, collection_id, item_id, slug, is_draft, sync_status, payload_hash)
                VALUES ('rewrite', ?, ?, ?, ?, 1, 'synced', ?)
                ON CONFLICT(page_type, source_id) DO UPDATE SET
                  item_id=excluded.item_id,
                  slug=excluded.slug,
                  is_draft=1,
                  sync_status='synced',
                  last_sync_at=CURRENT_TIMESTAMP,
                  payload_hash=excluded.payload_hash,
                  updated_at=CURRENT_TIMESTAMP
                """,
                [row["version_id"], settings.webflow_collection_id or '', item_id, row["slug"], json.dumps(field_data, sort_keys=True)],
            )
            processed += 1
        except Exception as exc:
            errors += 1
            logger.exception("webflow_sync_rewrites failed for page %s: %s", row["page_id"], exc)
    finish_run_log(db, run_id, "success" if errors == 0 else "warning", items_processed=processed, error_count=errors)
    return 0
