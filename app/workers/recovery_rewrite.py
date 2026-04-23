from __future__ import annotations

import json
import re

from app.html_tools import ensure_section, sanitize_article_fragment
from app.services.anthropic_service import AnthropicService
from app.workers._common import ensure_run_log, finish_run_log


def _slug_phrase(slug: str) -> str:
    return re.sub(r'-+', ' ', slug or '').strip()


def _rewrite_meta(title: str, slug: str) -> str:
    phrase = _slug_phrase(slug)
    return (
        f"Understand {phrase}, the workflow problems behind it, and how DEEMERGE helps teams reduce missed replies and context switching."
    )[:155]


def _fallback(page_title: str, slug: str) -> tuple[str, str, str, str]:
    title = page_title or slug.replace('-', ' ').title()
    meta = _rewrite_meta(title, slug)
    h1 = title
    body = f"""
<p>Teams usually lose time because the work is spread across multiple conversations, inboxes, and follow ups.</p>
<h2>Where the workflow breaks down</h2>
<p>When ownership is unclear and context is fragmented, teams miss replies, duplicate work, and slow down execution.</p>
<h2>What to look for instead</h2>
<p>The best setup is the one that keeps context visible, reduces duplicate effort, and makes ownership clear.</p>
<h2>How DEEMERGE solves this in practice</h2>
<p>DEEMERGE groups scattered communication into structured topics so teams can understand context quickly and move work forward with less friction.</p>
<h2>Next step with DEEMERGE</h2>
<p>If your team is losing time across email and chat, book a demo or review pricing to see how DEEMERGE can help.</p>
""".strip()
    return title, meta, h1, body


def run(*, db, settings, logger, limit: int = 10) -> int:
    run_id = ensure_run_log(db, 'recovery_rewrite')
    anthropic = AnthropicService(settings) if settings.anthropic_api_key else None
    rows = db.fetchall(
        """
        SELECT rq.id AS queue_id, cp.id AS page_id, cp.slug, cp.title_current, rc.top_queries_json
        FROM recovery_queue rq
        JOIN content_pages cp ON cp.id = rq.page_id
        JOIN recovery_candidates rc ON rc.id = rq.candidate_id
        WHERE rq.status IN ('queued','briefing')
        ORDER BY rq.priority DESC, rq.id ASC
        LIMIT ?
        """,
        [limit],
    )
    processed = 0
    for row in rows:
        title = row['title_current'] or row['slug'].replace('-', ' ').title()
        meta = _rewrite_meta(title, row['slug'])
        h1 = title
        if anthropic:
            prompt = (
                'Rewrite the article body as a clean HTML fragment only. '
                'No markdown fences. No <html>, <head>, <body>, <style>, <script>, CSS, or <h1>. '
                'Use only semantic content markup. '
                'Do not invent studies, percentages, averages, benchmarks, or research claims. '
                'Do not sound generic, fluffy, or motivational. '
                f"Article title: {title}. "
                f"Slug context: {row['slug']}. "
                "Keep the search intent tighter and make the article more practical. "
                "Include one section with the exact heading 'How DEEMERGE solves this in practice'. "
                "Include one final section with the exact heading 'Next step with DEEMERGE'."
            )
            try:
                raw_body = anthropic.generate(prompt, fast=True, max_tokens=1400)
                body = sanitize_article_fragment(raw_body)
                body = ensure_section(body, 'How DEEMERGE solves this in practice', '<h2>How DEEMERGE solves this in practice</h2><p>DEEMERGE helps by turning scattered communication into structured topics so teams can understand context faster and act without chasing updates across tools.</p>')
                body = ensure_section(body, 'Next step with DEEMERGE', '<h2>Next step with DEEMERGE</h2><p>Book a demo or view pricing if you want a workflow that reduces missed replies, context switching, and unclear ownership.</p>')
                if not body:
                    raise RuntimeError('Empty body after HTML cleanup')
            except Exception as exc:
                logger.warning('Anthropic rewrite failed for %s: %s', row['slug'], exc)
                title, meta, h1, body = _fallback(row['title_current'], row['slug'])
        else:
            title, meta, h1, body = _fallback(row['title_current'], row['slug'])
        current = db.fetchone('SELECT COALESCE(MAX(version_no),0) AS max_version FROM page_versions WHERE page_id=?', [row['page_id']])
        next_version = int(current['max_version']) + 1
        db.execute(
            """
            INSERT INTO page_versions(page_id, version_no, source_type, title_tag, meta_description, h1, body_html, notes_json)
            VALUES (?, ?, 'rewrite', ?, ?, ?, ?, ?)
            """,
            [row['page_id'], next_version, title, meta, h1, body, json.dumps({'queue_id': row['queue_id']})],
        )
        db.execute("UPDATE recovery_queue SET status='drafted' WHERE id=?", [row['queue_id']])
        processed += 1
    finish_run_log(db, run_id, 'success', items_processed=processed)
    logger.info('Created %s rewrite drafts', processed)
    return 0
