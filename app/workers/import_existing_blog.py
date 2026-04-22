from __future__ import annotations

from app.services.webflow_service import WebflowService
from app.workers._common import ensure_run_log, finish_run_log


def infer_cluster_from_slug(slug: str) -> str | None:
    value = slug.lower()
    if any(x in value for x in ["shared-inbox", "team-inbox", "mailbox", "unified-inbox"]):
        return "shared_inbox"
    if any(x in value for x in ["gmail", "slack", "outlook"]):
        return "gmail_slack_coordination"
    if any(x in value for x in ["triage", "follow-up", "email-management", "notification", "context-switching", "time-management"]):
        return "email_triage"
    if any(x in value for x in ["alternative", "alternatives", "vs-"]):
        return "alternatives"
    return None


def run(*, db, settings, logger, limit: int = 100) -> int:
    run_id = ensure_run_log(db, "import_existing_blog")
    site_row = db.fetchone("SELECT id FROM sites WHERE site_key='deemerge'")
    if not site_row:
        raise RuntimeError("Site row missing. Run seed_base first.")
    site_id = int(site_row["id"])

    items = []
    service = WebflowService(settings)
    if service.enabled:
        try:
            items = service.list_all_items(limit=min(limit, 100))
        except Exception as exc:
            logger.warning("Webflow import failed, using fallback fixtures: %s", exc)

    if not items:
        items = [
            {"id": "fixture_1", "fieldData": {"slug": "best-unified-inbox-apps-in-2025", "name": "The 7 Best Unified Inbox Apps in 2025"}},
            {"id": "fixture_2", "fieldData": {"slug": "spike-email-alternatives", "name": "Spike Email Alternatives"}},
            {"id": "fixture_3", "fieldData": {"slug": "integrate-gmail-and-slack", "name": "Integrate Gmail and Slack"}},
            {"id": "fixture_4", "fieldData": {"slug": "best-shared-inbox-solution-for-collaboration", "name": "Best Shared Inbox Solution for Collaboration"}},
        ]

    processed = 0
    base_blog = settings.blog_base_url.rstrip("/")
    for item in items[:limit]:
        field_data = item.get("fieldData", {})
        slug = field_data.get("slug")
        title = field_data.get("name")
        if not slug:
            continue
        cluster_key = infer_cluster_from_slug(slug)
        cluster_id = None
        if cluster_key:
            cluster = db.fetchone("SELECT id FROM clusters WHERE cluster_key=?", [cluster_key])
            cluster_id = int(cluster["id"]) if cluster else None
        page_url = f"{base_blog}/{slug}"
        db.execute(
            """
            INSERT INTO content_pages(site_id, page_url, slug, title_current, h1_current, cluster_id, page_type, status, webflow_item_id)
            VALUES (?, ?, ?, ?, ?, ?, 'blog', 'active', ?)
            ON CONFLICT(page_url) DO UPDATE SET
              title_current=excluded.title_current,
              h1_current=excluded.h1_current,
              cluster_id=COALESCE(excluded.cluster_id, content_pages.cluster_id),
              webflow_item_id=COALESCE(excluded.webflow_item_id, content_pages.webflow_item_id),
              updated_at=CURRENT_TIMESTAMP
            """,
            [site_id, page_url, slug, title, title, cluster_id, item.get("id")],
        )
        processed += 1

    finish_run_log(db, run_id, "success", items_processed=processed)
    logger.info("Imported %s existing blog pages", processed)
    return 0
