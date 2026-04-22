from __future__ import annotations

import json

from app.services.anthropic_service import AnthropicService
from app.workers._common import ensure_run_log, finish_run_log


def _fallback(page_title: str, slug: str) -> tuple[str, str, str, str]:
    title = page_title or slug.replace('-', ' ').title()
    meta = f"Updated guide for {title} with clearer intent match and a practical DEEMERGE section."
    h1 = title
    body = f"""
<h2>Why this workflow breaks down</h2>
<p>Teams often lose time because the work is spread across multiple conversations and follow ups.</p>
<h2>What to look for</h2>
<p>The best setup is the one that gives visibility, reduces duplicate work, and makes ownership clear.</p>
<h2>How DEEMERGE solves this in practice</h2>
<p>DEEMERGE helps by grouping scattered conversations into structured topics so teams can understand context and act faster.</p>
""".strip()
    return title, meta, h1, body


def run(*, db, settings, logger, limit: int = 10) -> int:
    run_id = ensure_run_log(db, "recovery_rewrite")
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
        if anthropic:
            prompt = f"Rewrite the article '{row['title_current']}' for better SEO intent match and clearer conversion to DEEMERGE. Output HTML only."
            try:
                body = anthropic.generate(prompt, fast=True, max_tokens=1400)
                title = row["title_current"]
                meta = f"Updated guide to {row['title_current']} with a clearer DEEMERGE angle."
                h1 = row["title_current"]
            except Exception as exc:
                logger.warning("Anthropic rewrite failed for %s: %s", row["slug"], exc)
                title, meta, h1, body = _fallback(row["title_current"], row["slug"])
        else:
            title, meta, h1, body = _fallback(row["title_current"], row["slug"])
        current = db.fetchone("SELECT COALESCE(MAX(version_no),0) AS max_version FROM page_versions WHERE page_id=?", [row["page_id"]])
        next_version = int(current["max_version"]) + 1
        db.execute(
            """
            INSERT INTO page_versions(page_id, version_no, source_type, title_tag, meta_description, h1, body_html, notes_json)
            VALUES (?, ?, 'rewrite', ?, ?, ?, ?, ?)
            """,
            [row["page_id"], next_version, title, meta, h1, body, json.dumps({"queue_id": row["queue_id"]})],
        )
        db.execute("UPDATE recovery_queue SET status='drafted' WHERE id=?", [row["queue_id"]])
        processed += 1
    finish_run_log(db, run_id, "success", items_processed=processed)
    logger.info("Created %s rewrite drafts", processed)
    return 0
