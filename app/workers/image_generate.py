from __future__ import annotations

from app.image_tools import ensure_article_svg
from app.workers._common import ensure_run_log, finish_run_log


def run(*, db, settings, logger, limit: int = 10) -> int:
    run_id = ensure_run_log(db, 'image_generate')
    rows = db.fetchall(
        """
        SELECT ad.id AS article_draft_id, ad.slug, ad.title_tag
        FROM article_drafts ad
        JOIN article_generation_queue q ON q.id = ad.queue_id
        LEFT JOIN image_queue iq ON iq.source_type='article' AND iq.source_id = ad.id
        LEFT JOIN generated_images gi ON gi.queue_id = iq.id
        WHERE gi.id IS NULL AND q.status IN ('ready','drafted','validated','synced','queued','briefing')
        ORDER BY ad.id ASC
        LIMIT ?
        """,
        [limit],
    )
    processed = 0
    for row in rows:
        iq = db.fetchone("SELECT id FROM image_queue WHERE source_type='article' AND source_id=?", [row['article_draft_id']])
        if iq:
            queue_id = int(iq['id'])
        else:
            queue_id = db.insert(
                "INSERT INTO image_queue(source_type, source_id, prompt, status) VALUES ('article', ?, ?, 'queued')",
                [row['article_draft_id'], f"Editorial DEEMERGE blog image for {row['title_tag']}"]
            )
        local_path, alt_text = ensure_article_svg(row['title_tag'], row['slug'])
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
        processed += 1
    logger.info('Generated %s article image files', processed)
    finish_run_log(db, run_id, 'success', items_processed=processed)
    return 0
