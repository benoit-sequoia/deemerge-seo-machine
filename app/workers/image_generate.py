from __future__ import annotations

from pathlib import Path

from app.image_tools import ensure_article_svg, build_article_image_prompt
from app.services.openai_image_service import OpenAIImageService
from app.workers._common import ensure_run_log, finish_run_log


def _needs_regeneration(local_path: str | None) -> bool:
    if not local_path:
        return True
    lp = str(local_path).lower()
    return lp.endswith('.svg') or not Path(local_path).exists()


def run(*, db, settings, logger, limit: int = 10) -> int:
    run_id = ensure_run_log(db, 'image_generate')
    rows = db.fetchall(
        """
        SELECT ad.id AS article_draft_id, ad.slug, ad.title_tag, ad.excerpt, gi.local_path, iq.id as queue_id
        FROM article_drafts ad
        JOIN article_generation_queue q ON q.id = ad.queue_id
        LEFT JOIN image_queue iq ON iq.source_type='article' AND iq.source_id = ad.id
        LEFT JOIN generated_images gi ON gi.queue_id = iq.id
        WHERE q.status IN ('ready','drafted','validated','synced','queued','briefing')
        ORDER BY ad.id ASC
        LIMIT ?
        """,
        [limit],
    )
    processed = 0
    openai_image = OpenAIImageService(settings)
    for row in rows:
        queue_id = int(row['queue_id']) if row.get('queue_id') else None
        if not queue_id:
            queue_id = db.insert(
                "INSERT INTO image_queue(source_type, source_id, prompt, status) VALUES ('article', ?, ?, 'queued')",
                [row['article_draft_id'], f"Editorial DEEMERGE blog image for {row['title_tag']}"]
            )

        local_path = row.get('local_path')
        if not _needs_regeneration(local_path):
            continue

        outdir = Path('/data/generated_images')
        outdir.mkdir(parents=True, exist_ok=True)
        png_path = outdir / f"{row['slug']}.png"
        alt_text = row['title_tag']

        try:
            if openai_image.available():
                prompt = build_article_image_prompt(row['title_tag'], row['slug'], row.get('excerpt'))
                local_path = openai_image.generate_image_file(prompt=prompt, output_path=str(png_path))
            else:
                local_path, alt_text = ensure_article_svg(row['title_tag'], row['slug'])
                logger.warning('OPENAI_API_KEY missing, generated placeholder image for %s', row['slug'])
        except Exception as exc:
            logger.warning('OpenAI image generation failed for %s, using placeholder: %s', row['slug'], exc)
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
        db.execute("UPDATE webflow_items SET sync_status='needs_image_resync', updated_at=CURRENT_TIMESTAMP WHERE page_type='article' AND source_id=?", [row['article_draft_id']])
        processed += 1

    logger.info('Generated %s article image files', processed)
    finish_run_log(db, run_id, 'success', items_processed=processed)
    return 0
