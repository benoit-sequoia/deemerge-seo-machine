from __future__ import annotations

import os

from app.workers._common import ensure_run_log, finish_run_log


def run(*, db, settings, logger, limit: int = 10) -> int:
    run_id = ensure_run_log(db, 'publish_due')
    service = None
    from app.services.webflow_service import WebflowService
    service = WebflowService(settings)

    target_slug = os.environ.get('PUBLISH_SLUG')
    target_page_type = os.environ.get('PUBLISH_PAGE_TYPE')
    processed = 0
    errors = 0

    if target_slug:
        sql = """
        SELECT id, page_type, source_id, item_id, slug
        FROM webflow_items
        WHERE sync_status IN ('synced','synced_with_image') AND slug=?
        """
        params = [target_slug]
        if target_page_type:
            sql += " AND page_type=?"
            params.append(target_page_type)
        row = db.fetchone(sql, params)
        if not row:
            logger.warning('publish_due: no synced webflow item found for slug=%s page_type=%s', target_slug, target_page_type)
            finish_run_log(db, run_id, 'warning', items_processed=0, error_count=0)
            return 0
        try:
            service.publish_items([row['item_id']])
            db.execute(
                "UPDATE webflow_items SET is_draft=0, last_published=CURRENT_TIMESTAMP, last_sync_at=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                [row['id']],
            )
            db.execute(
                "INSERT INTO publish_plan(source_type, source_id, planned_publish_ts_utc, actual_publish_ts_utc, status) VALUES (?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'published')",
                [row['page_type'], row['source_id']],
            )
            logger.info('Published selected Webflow item %s (%s, %s)', row['item_id'], row['page_type'], row['slug'])
            processed = 1
            finish_run_log(db, run_id, 'success', items_processed=processed, error_count=0)
            return 0
        except Exception as exc:
            errors = 1
            logger.exception('publish_due failed for selected item %s: %s', row['item_id'], exc)
            finish_run_log(db, run_id, 'warning', items_processed=0, error_count=errors)
            return 1

    rows = db.fetchall(
        """
        SELECT wp.id AS plan_id, wi.id AS webflow_item_row_id, wi.item_id, wi.page_type, wi.source_id, wi.slug
        FROM publish_plan wp
        JOIN webflow_items wi ON wi.page_type = wp.source_type AND wi.source_id = wp.source_id
        WHERE wp.status='planned' AND wp.planned_publish_ts_utc <= CURRENT_TIMESTAMP AND wi.sync_status IN ('synced','synced_with_image')
        ORDER BY wp.planned_publish_ts_utc ASC
        LIMIT ?
        """,
        [limit],
    )
    if not rows:
        logger.info('publish_due: no due items')
        finish_run_log(db, run_id, 'success', items_processed=0, error_count=0)
        return 0

    item_ids = [row['item_id'] for row in rows if row['item_id']]
    if not item_ids:
        logger.warning('publish_due: due rows exist but no Webflow item_ids found')
        finish_run_log(db, run_id, 'warning', items_processed=0, error_count=0)
        return 0

    try:
        service.publish_items(item_ids)
        for row in rows:
            db.execute(
                "UPDATE webflow_items SET is_draft=0, last_published=CURRENT_TIMESTAMP, last_sync_at=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                [row['webflow_item_row_id']],
            )
            db.execute(
                "UPDATE publish_plan SET status='published', actual_publish_ts_utc=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                [row['plan_id']],
            )
        processed = len(rows)
        logger.info('Published %s due items', processed)
    except Exception as exc:
        errors = len(rows)
        logger.exception('publish_due batch failed: %s', exc)

    finish_run_log(db, run_id, 'success' if errors == 0 else 'warning', items_processed=processed, error_count=errors)
    return 0 if errors == 0 else 1
