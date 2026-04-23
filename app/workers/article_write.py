from __future__ import annotations

import json
import re

from app.html_tools import ensure_section, sanitize_article_fragment
from app.services.anthropic_service import AnthropicService
from app.workers._common import ensure_run_log, finish_run_log


def _slugify(text: str) -> str:
    text = re.sub(r"[^a-zA-Z0-9\s-]", "", text).strip().lower()
    text = re.sub(r"[\s_-]+", "-", text)
    return text[:120].strip("-")


def _article_meta(primary: str) -> str:
    return (
        f"Compare {primary} options, common team workflow problems, and how DEEMERGE helps reduce missed replies and context switching."
    )[:155]


def _article_excerpt(primary: str) -> str:
    return f"What {primary} means for teams, where basic workflows break, and how DEEMERGE fits."


def _fallback_html(primary: str, secondaries: list[str]) -> tuple[str, str, str, str]:
    human = primary.title()
    h1 = f"{human}: Best Solutions for Teams in 2026"
    title_tag = h1
    meta = _article_meta(primary)
    excerpt = _article_excerpt(primary)
    body = f"""
<p>{human} is usually not just a tool question. It is a workflow question about visibility, accountability, and execution across multiple conversations.</p>
<h2>Where teams struggle</h2>
<p>Teams lose time when email, chat, and follow ups are split across tools. That creates delays, duplicate work, and missed replies.</p>
<h2>What to look for</h2>
<p>The best option is the one that makes ownership clear, keeps context visible, and helps teams act faster.</p>
<h2>How DEEMERGE solves this in practice</h2>
<p>DEEMERGE pulls scattered communication into structured topics so teams can understand context, see what matters, and move work forward faster.</p>
<h2>Next step with DEEMERGE</h2>
<p>See pricing or book a demo if you want a system that helps your team reduce missed replies and context switching.</p>
""".strip()
    return h1, title_tag, meta, excerpt, body


def run(*, db, settings, logger, limit: int = 10) -> int:
    run_id = ensure_run_log(db, 'article_write')
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
        secondary = json.loads(row['secondary_keywords_json'] or '[]')
        title_options = json.loads(row['title_options_json'] or '[]')
        primary = row['primary_keyword']
        title_tag = title_options[0] if title_options else f"{primary.title()}: Best Solutions for Teams in 2026"
        h1 = title_tag
        meta = _article_meta(primary)
        excerpt = _article_excerpt(primary)
        if anthropic:
            prompt = (
                'Write only a clean HTML fragment for a B2B SaaS blog article body. '
                'No markdown fences. No <html>, <head>, <body>, <style>, <script>, CSS, or <h1>. '
                'Use only <p>, <h2>, <h3>, <ul>, <ol>, <li>, <strong>, <em>, <blockquote>, and <a>. '
                'Do not invent studies, percentages, averages, benchmarks, or research claims. '
                'Do not write generic filler or motivational fluff. '
                f"Primary keyword: {primary}. "
                f"Secondary keywords: {', '.join(secondary[:10])}. "
                f"Angle: {row['article_angle']}. "
                "Include one section with the exact heading 'How DEEMERGE solves this in practice'. "
                "Include one final section with the exact heading 'Next step with DEEMERGE'. "
                "The article should be sharp, practical, and directly aligned to the keyword intent."
            )
            try:
                raw_body = anthropic.generate(prompt, fast=True, max_tokens=1800)
                body = sanitize_article_fragment(raw_body)
                body = ensure_section(body, 'How DEEMERGE solves this in practice', '<h2>How DEEMERGE solves this in practice</h2><p>DEEMERGE helps teams by pulling scattered conversations into structured topics so people can understand context faster and act without chasing updates across tools.</p>')
                body = ensure_section(body, 'Next step with DEEMERGE', '<h2>Next step with DEEMERGE</h2><p>Book a demo or view pricing if you want a workflow that reduces missed replies, context switching, and unclear ownership across email and chat.</p>')
                if not body:
                    raise RuntimeError('Empty body after HTML cleanup')
            except Exception as exc:
                logger.warning('Anthropic generation failed, using fallback for %s: %s', primary, exc)
                h1, title_tag, meta, excerpt, body = _fallback_html(primary, secondary)
        else:
            h1, title_tag, meta, excerpt, body = _fallback_html(primary, secondary)
        slug = _slugify(primary)
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
            [row['queue_id'], title_tag, meta, slug, h1, excerpt, body, f"Editorial image for {primary}"]
        )
        db.execute("UPDATE article_generation_queue SET status='drafted' WHERE id=?", [row['queue_id']])
        processed += 1
    finish_run_log(db, run_id, 'success', items_processed=processed)
    logger.info('Created %s article drafts', processed)
    return 0
