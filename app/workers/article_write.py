from __future__ import annotations

import json
import re

from app.services.anthropic_service import AnthropicService
from app.workers._common import ensure_run_log, finish_run_log


def _slugify(text: str) -> str:
    text = re.sub(r"[^a-zA-Z0-9\s-]", "", text).strip().lower()
    text = re.sub(r"[\s_-]+", "-", text)
    return text[:120].strip("-")


def _fallback_html(primary: str, secondaries: list[str]) -> tuple[str, str, str, str]:
    h1 = f"Best {primary.title()} Solutions for Teams in 2026"
    title_tag = h1
    meta = f"Compare {primary} options, understand the main workflow problems, and see how DEEMERGE helps teams move faster with less context switching."
    excerpt = f"Understand {primary}, the common workflow problems around it, and where DEEMERGE fits."
    body = f"""
<h2>What {primary} means for teams</h2>
<p>{primary.title()} is usually not just a tool question. It is a workflow question about visibility, accountability, and execution across multiple conversations.</p>
<h2>Main problems teams face</h2>
<p>Teams usually lose time because information is split between email, chat, and follow ups. That is where delays, duplicate work, and missed replies happen.</p>
<h2>How DEEMERGE solves this in practice</h2>
<p>DEEMERGE helps by pulling scattered conversations into structured topics, showing what matters, and reducing the time needed to understand context and act.</p>
<h2>Related terms</h2>
<p>{', '.join(secondaries[:8])}</p>
""".strip()
    return h1, title_tag, meta, excerpt, body


def run(*, db, settings, logger, limit: int = 10) -> int:
    run_id = ensure_run_log(db, "article_write")
    rows = db.fetchall(
        """
        SELECT ab.queue_id, ab.primary_keyword, ab.secondary_keywords_json, ab.article_angle, ab.title_options_json
        FROM article_briefs ab
        JOIN article_generation_queue q ON q.id = ab.queue_id
        LEFT JOIN article_drafts ad ON ad.queue_id = ab.queue_id
        WHERE ad.id IS NULL AND q.status IN ('queued','briefing')
        ORDER BY q.priority DESC, q.id ASC
        LIMIT ?
        """,
        [limit],
    )
    processed = 0
    anthropic = AnthropicService(settings) if settings.anthropic_api_key else None
    for row in rows:
        secondary = json.loads(row["secondary_keywords_json"] or "[]")
        if anthropic:
            # Keep prompt simple for first runnable version.
            prompt = (
                f"Write a concise SEO blog article in HTML. Primary keyword: {row['primary_keyword']}. "
                f"Secondary keywords: {', '.join(secondary[:10])}. "
                f"Angle: {row['article_angle']}. Include a section called 'How DEEMERGE solves this in practice'."
            )
            try:
                body = anthropic.generate(prompt, fast=True, max_tokens=1800)
                h1 = json.loads(row["title_options_json"] or "[]")[0]
                title_tag = h1
                meta = f"Learn about {row['primary_keyword']} and how DEEMERGE fits this workflow."
                excerpt = f"Guide to {row['primary_keyword']} for teams."
            except Exception as exc:
                logger.warning("Anthropic generation failed, using fallback for %s: %s", row["primary_keyword"], exc)
                h1, title_tag, meta, excerpt, body = _fallback_html(row["primary_keyword"], secondary)
        else:
            h1, title_tag, meta, excerpt, body = _fallback_html(row["primary_keyword"], secondary)
        slug = _slugify(row["primary_keyword"])
        db.execute(
            """
            INSERT INTO article_drafts(queue_id, title_tag, meta_description, slug, h1, excerpt, body_html, faq_json, schema_json, image_prompt, quality_score, validation_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, '[]', '{}', ?, 0, '{}')
            ON CONFLICT(queue_id) DO UPDATE SET
              title_tag=excluded.title_tag,
              meta_description=excluded.meta_description,
              slug=excluded.slug,
              h1=excluded.h1,
              excerpt=excluded.excerpt,
              body_html=excluded.body_html,
              image_prompt=excluded.image_prompt,
              created_at=CURRENT_TIMESTAMP
            """,
            [row["queue_id"], title_tag, meta, slug, h1, excerpt, body, f"Editorial image for {row['primary_keyword']}"]
        )
        db.execute("UPDATE article_generation_queue SET status='drafted' WHERE id=?", [row["queue_id"]])
        processed += 1
    finish_run_log(db, run_id, "success", items_processed=processed)
    logger.info("Created %s article drafts", processed)
    return 0
