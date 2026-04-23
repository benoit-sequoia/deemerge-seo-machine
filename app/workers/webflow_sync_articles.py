from __future__ import annotations

import json
from datetime import datetime, timezone

from app.services.webflow_service import WebflowService
from app.workers._common import ensure_run_log, finish_run_log


def _estimate_read_time_minutes(html: str) -> str:
    words = len([w for w in html.replace('<', ' <').replace('>', '> ').split() if not w.startswith('<')])
    mins = max(4, round(words / 200))
    return f"{mins} - {mins + 2} minutes"


def _field_data_from_draft(draft, field_map: dict[str, str], image_value: dict | None) -> dict:
    now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    field_data = {
        field_map["name"]: draft["h1"],
        field_map["slug"]: draft["slug"],
        field_map["summary"]: draft["excerpt"] or draft["meta_description"] or draft["h1"],
        field_map["body"]: draft["body_html"],
        field_map["seo_title"]: draft["title_tag"],
        field_map["seo_description"]: draft["meta_description"] or draft["excerpt"] or draft["h1"],
        field_map["published_date"]: now_iso,
    }
    if image_value:
        if field_map.get("featured_image"):
            field_data[field_map["featured_image"]] = image_value
        if field_map.get("og_image"):
            field_data[field_map["og_image"]] = image_value
    return field_data


def run(*, db, settings, logger, limit: int = 10) -> int:
    run_id = ensure_run_log(db, "webflow_sync_articles")
    field_map = settings.webflow_field_map()
    service = WebflowService(settings)
    image_value = None
    try:
        image_value = service.find_fallback_image_field_value(field_map.get("og_image", "og-image"), field_map.get("featured_image", "post-image"))
    except Exception as exc:
        logger.warning("Could not resolve fallback Webflow image: %s", exc)
    if not image_value:
        logger.warning("webflow_sync_articles skipped: no fallback image available for required og-image")
        finish_run_log(db, run_id, "skipped", items_processed=0)
        return 0

    site_row = db.fetchone("SELECT id FROM sites WHERE site_key='deemerge'")
    site_id = int(site_row["id"]) if site_row else None
    rows = db.fetchall(
        """
        SELECT ad.id AS article_draft_id, q.id AS queue_id, kc.id AS keyword_candidate_id, kc.primary_keyword, ad.slug, ad.h1, ad.excerpt, ad.body_html, ad.title_tag, ad.meta_description
        FROM article_drafts ad
        JOIN article_generation_queue q ON q.id = ad.queue_id
        JOIN keyword_candidates kc ON kc.id = q.keyword_candidate_id
        LEFT JOIN webflow_items wi ON wi.page_type='article' AND wi.source_id = ad.id
        WHERE wi.id IS NULL AND q.status IN ('ready','drafted','validated','queued','briefing')
        ORDER BY ad.id ASC
        LIMIT ?
        """,
        [limit],
    )
    processed = 0
    errors = 0
    logger.info("webflow_sync_articles candidates: %s", len(rows))
    for row in rows:
        try:
            existing = service.find_item_by_slug(row["slug"])
            if existing:
                item_id = existing["id"]
                field_data = _field_data_from_draft(row, field_map, image_value)
                service.update_item(item_id, field_data, is_draft=True)
            else:
                field_data = _field_data_from_draft(row, field_map, image_value)
                created = service.create_item(field_data, is_draft=True)
                item_id = created.get("id") or created.get("item", {}).get("id")
            db.execute(
                """
                INSERT INTO webflow_items(page_type, source_id, collection_id, item_id, slug, is_draft, sync_status, payload_hash)
                VALUES ('article', ?, ?, ?, ?, 1, 'synced', ?)
                ON CONFLICT(page_type, source_id) DO UPDATE SET
                  item_id=excluded.item_id,
                  slug=excluded.slug,
                  is_draft=1,
                  sync_status='synced',
                  last_sync_at=CURRENT_TIMESTAMP,
                  payload_hash=excluded.payload_hash,
                  updated_at=CURRENT_TIMESTAMP
                """,
                [row["article_draft_id"], settings.webflow_collection_id or '', item_id, row["slug"], json.dumps(field_data, sort_keys=True)],
            )
            if site_id:
                page_url = f"{settings.blog_base_url.rstrip('/')}/{row['slug']}"
                db.execute(
                    """
                    INSERT INTO content_pages(site_id, page_url, slug, title_current, h1_current, page_type, status, webflow_item_id)
                    VALUES (?, ?, ?, ?, ?, 'blog', 'active', ?)
                    ON CONFLICT(page_url) DO UPDATE SET
                      title_current=excluded.title_current,
                      h1_current=excluded.h1_current,
                      webflow_item_id=COALESCE(excluded.webflow_item_id, content_pages.webflow_item_id),
                      updated_at=CURRENT_TIMESTAMP
                    """,
                    [site_id, page_url, row['slug'], row['title_tag'], row['h1'], item_id],
                )
            db.execute("UPDATE article_generation_queue SET status='synced' WHERE id=?", [row['queue_id']])
            processed += 1
        except Exception as exc:
            errors += 1
            logger.exception("webflow_sync_articles failed for article_draft %s: %s", row['article_draft_id'], exc)
    finish_run_log(db, run_id, "success" if errors == 0 else "warning", items_processed=processed, error_count=errors)
    return 0
