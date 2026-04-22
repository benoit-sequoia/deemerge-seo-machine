from __future__ import annotations

from datetime import UTC, datetime

from app.services.slack_service import SlackService
from app.services.webflow_service import WebflowService
from app.workers._common import ensure_run_log, finish_run_log


def _is_due(value: str) -> bool:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00")) if value else None
    return bool(dt and dt <= datetime.now(UTC))


def run(*, db, settings, logger, limit: int = 10) -> int:
    run_id = ensure_run_log(db, "publish_due")
    webflow = WebflowService(settings)
    slack = SlackService(settings)
    rows = db.fetchall(
        "SELECT id, source_type, source_id, planned_publish_ts_utc FROM publish_plan WHERE status='planned' ORDER BY planned_publish_ts_utc ASC LIMIT ?",
        [limit],
    )
    processed = 0
    for row in rows:
        if not _is_due(str(row["planned_publish_ts_utc"])) and settings.app_env == "production":
            continue
        if row["source_type"] == "new_article":
            draft = db.fetchone("SELECT id, slug, title_tag, h1 FROM article_drafts WHERE id=?", [row["source_id"]])
            wf = db.fetchone("SELECT item_id FROM webflow_items WHERE page_type='new_article' AND source_id=?", [row["source_id"]])
            if webflow.enabled and wf and wf["item_id"]:
                try:
                    webflow.publish_items([wf["item_id"]])
                except Exception as exc:
                    logger.warning("Webflow publish fallback for article %s: %s", draft["id"], exc)
            page_url = f"{settings.blog_base_url.rstrip('/')}/{draft['slug']}"
            site_id = int(db.fetchone("SELECT id FROM sites WHERE site_key='deemerge'")["id"])
            cluster_row = db.fetchone("SELECT kc.cluster_id FROM article_drafts ad JOIN article_generation_queue agq ON agq.id=ad.queue_id JOIN keyword_candidates kc ON kc.id=agq.keyword_candidate_id WHERE ad.id=?", [row['source_id']])
            db.execute(
                """
                INSERT INTO content_pages(site_id, page_url, slug, title_current, h1_current, cluster_id, page_type, status, last_published_at)
                VALUES (?, ?, ?, ?, ?, ?, 'blog', 'active', CURRENT_TIMESTAMP)
                ON CONFLICT(page_url) DO UPDATE SET
                  title_current=excluded.title_current,
                  h1_current=excluded.h1_current,
                  cluster_id=COALESCE(excluded.cluster_id, content_pages.cluster_id),
                  last_published_at=CURRENT_TIMESTAMP,
                  updated_at=CURRENT_TIMESTAMP
                """,
                [site_id, page_url, draft["slug"], draft["title_tag"], draft["h1"], cluster_row['cluster_id'] if cluster_row else None],
            )
            db.execute("UPDATE article_generation_queue SET status='published', completed_at=CURRENT_TIMESTAMP WHERE id=(SELECT queue_id FROM article_drafts WHERE id=?)", [row["source_id"]])
            slack.send_message(f"Published new DEEMERGE article: {draft['h1']}\n{page_url}")
        else:
            version = db.fetchone(
                "SELECT pv.id, pv.title_tag, pv.h1, pv.webflow_item_id, cp.id AS page_id, cp.page_url FROM page_versions pv JOIN content_pages cp ON cp.id=pv.page_id WHERE pv.id=?",
                [row["source_id"]],
            )
            wf = db.fetchone("SELECT item_id FROM webflow_items WHERE page_type='rewrite' AND source_id=?", [row["source_id"]])
            if webflow.enabled and wf and wf["item_id"]:
                try:
                    webflow.publish_items([wf["item_id"]])
                except Exception as exc:
                    logger.warning("Webflow publish fallback for rewrite %s: %s", version["id"], exc)
            db.execute("UPDATE content_pages SET title_current=?, h1_current=?, last_published_at=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP WHERE id=?", [version["title_tag"], version["h1"], version["page_id"]])
            db.execute("UPDATE page_versions SET published_at=CURRENT_TIMESTAMP WHERE id=?", [row["source_id"]])
            db.execute("UPDATE recovery_queue SET status='published', completed_at=CURRENT_TIMESTAMP WHERE page_id=?", [version["page_id"]])
            slack.send_message(f"Published DEEMERGE rewrite: {version['h1']}\n{version['page_url']}")
        db.execute("UPDATE publish_plan SET status='published', actual_publish_ts_utc=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP WHERE id=?", [row["id"]])
        processed += 1
    finish_run_log(db, run_id, "success", items_processed=processed)
    logger.info("Published %s due items", processed)
    return 0
