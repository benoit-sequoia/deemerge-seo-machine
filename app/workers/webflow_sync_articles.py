from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from app.image_tools import ensure_article_svg, build_article_image_prompt
from app.services.openai_image_service import OpenAIImageService
from app.services.webflow_service import WebflowService
from app.workers._common import ensure_run_log, finish_run_log


def _estimate_read_time_minutes(html: str) -> str:
    words = len([w for w in html.replace('<', ' <').replace('>', '> ').split() if not w.startswith('<')])
    mins = max(4, round(words / 200))
    return f"{mins} - {mins + 2} minutes"


def _ensure_local_article_image(db, draft, settings, logger) -> tuple[str, str]:
    existing = db.fetchone(
        """
        SELECT gi.local_path, gi.alt_text, iq.id AS queue_id
        FROM generated_images gi
        JOIN image_queue iq ON iq.id = gi.queue_id
        WHERE iq.source_type='article' AND iq.source_id=?
        ORDER BY gi.id DESC LIMIT 1
        """,
        [draft['article_draft_id']],
    )
    if existing and existing['local_path']:
        local_path = str(existing['local_path'])
        if not local_path.lower().endswith('.svg') and Path(local_path).exists():
            return local_path, (existing['alt_text'] or draft['title_tag'])
        queue_id = int(existing['queue_id'])
    else:
        iq = db.fetchone("SELECT id FROM image_queue WHERE source_type='article' AND source_id=?", [draft['article_draft_id']])
        if iq:
            queue_id = int(iq['id'])
        else:
            queue_id = db.insert(
                "INSERT INTO image_queue(source_type, source_id, prompt, status) VALUES ('article', ?, ?, 'queued')",
                [draft['article_draft_id'], f"Editorial DEEMERGE blog image for {draft['title_tag']}"]
            )

    alt_text = draft['title_tag']
    outdir = Path('/data/generated_images')
    outdir.mkdir(parents=True, exist_ok=True)
    png_path = outdir / f"{draft['slug']}.png"

    try:
        openai_image = OpenAIImageService(settings)
        if openai_image.available():
            prompt = build_article_image_prompt(draft['title_tag'], draft['slug'], draft.get('excerpt'))
            local_path = openai_image.generate_image_file(prompt=prompt, output_path=str(png_path))
        else:
            logger.warning('OPENAI_API_KEY missing, using placeholder image for %s', draft['slug'])
            local_path, alt_text = ensure_article_svg(draft['title_tag'], draft['slug'])
    except Exception as exc:
        logger.warning('Real image generation failed for %s, falling back to placeholder: %s', draft['slug'], exc)
        local_path, alt_text = ensure_article_svg(draft['title_tag'], draft['slug'])

    db.execute(
        """
        INSERT INTO generated_images(queue_id, local_path, alt_text, status)
        VALUES (?, ?, ?, 'generated')
        ON CONFLICT(queue_id) DO UPDATE SET
          local_path=excluded.local_path,
          alt_text=excluded.alt_text,
          status='generated'
        """,
        [queue_id, local_path, alt_text],
    )
    db.execute("UPDATE image_queue SET status='generated', updated_at=CURRENT_TIMESTAMP WHERE id=?", [queue_id])
    return local_path, alt_text


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
    read_time_key = field_map.get("read_time", "post-read-time-3")
    if read_time_key:
        field_data[read_time_key] = _estimate_read_time_minutes(draft["body_html"])
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

    site_row = db.fetchone("SELECT id FROM sites WHERE site_key='deemerge'")
    site_id = int(site_row["id"]) if site_row else None
    rows = db.fetchall(
        """
        SELECT ad.id AS article_draft_id, q.id AS queue_id, kc.id AS keyword_candidate_id, kc.primary_keyword, ad.slug, ad.h1, ad.excerpt, ad.body_html, ad.title_tag, ad.meta_description, wi.id AS webflow_row_id, wi.item_id AS existing_item_id, wi.sync_status
        FROM article_drafts ad
        JOIN article_generation_queue q ON q.id = ad.queue_id
        JOIN keyword_candidates kc ON kc.id = q.keyword_candidate_id
        LEFT JOIN webflow_items wi ON wi.page_type='article' AND wi.source_id = ad.id
        LEFT JOIN image_queue iq ON iq.source_type='article' AND iq.source_id = ad.id
        LEFT JOIN generated_images gi ON gi.queue_id = iq.id
        WHERE q.status IN ('ready','drafted','validated','queued','briefing','synced')
          AND (wi.id IS NULL OR wi.sync_status='needs_image_resync' OR gi.id IS NULL OR gi.local_path LIKE '%.svg')
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
            local_path, alt_text = _ensure_local_article_image(db, row, settings, logger)
            image_value = service.upload_asset_file(local_path, alt=alt_text)
            field_data = _field_data_from_draft(row, field_map, image_value)
            if row['existing_item_id']:
                item_id = row['existing_item_id']
                service.update_item(item_id, field_data, is_draft=True)
            else:
                existing = service.find_item_by_slug(row["slug"])
                if existing:
                    item_id = existing["id"]
                    service.update_item(item_id, field_data, is_draft=True)
                else:
                    created = service.create_item(field_data, is_draft=True)
                    item_id = created.get("id") or created.get("item", {}).get("id")
            db.execute(
                """
                INSERT INTO webflow_items(page_type, source_id, collection_id, item_id, slug, is_draft, sync_status, payload_hash)
                VALUES ('article', ?, ?, ?, ?, 1, 'synced_with_image', ?)
                ON CONFLICT(page_type, source_id) DO UPDATE SET
                  item_id=excluded.item_id,
                  slug=excluded.slug,
                  is_draft=1,
                  sync_status='synced_with_image',
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
            logger.info("webflow_sync_articles synced draft %s -> %s", row['slug'], item_id)
        except Exception as exc:
            errors += 1
            logger.exception("webflow_sync_articles failed for article_draft %s: %s", row['article_draft_id'], exc)
    finish_run_log(db, run_id, "success" if errors == 0 else "warning", items_processed=processed, error_count=errors)
    return 0
