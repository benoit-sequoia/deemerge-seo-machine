from __future__ import annotations

import json

from app.services.anthropic_service import AnthropicService
from app.utils import append_related_links, extract_json_object, load_prompt, render_prompt, word_count_from_html
from app.workers._common import ensure_run_log, finish_run_log


PROMPT_NAME = "rewrite_article.txt"


def _fallback_version(title: str, primary_query: str, page_url: str) -> dict:
    clean_title = primary_query.title() if primary_query else title
    body = (
        f"<p>{clean_title} matters because teams lose time when email and chat context is scattered.</p>"
        f"<h2>What the query is really asking</h2><p>People searching for {primary_query} usually want a practical way to reduce confusion, improve ownership, and stop missed follow ups.</p>"
        f"<h2>A better way to work</h2><p>DEEMERGE helps teams pull Slack and email into one place, group related threads, surface what matters, and move faster without losing context.</p>"
    )
    return {
        "title_tag": clean_title,
        "meta_description": f"Learn {primary_query} best practices and see how DEEMERGE helps teams act faster with less context switching.",
        "h1": clean_title,
        "intro_html": f"<p>{clean_title} is easier when your team can see all related context in one place.</p>",
        "body_html": body,
        "cta_variant": "standard",
    }


def run(*, db, settings, logger, limit: int = 10) -> int:
    run_id = ensure_run_log(db, "recovery_rewrite")
    anthropic = AnthropicService(settings)
    template = load_prompt(PROMPT_NAME)
    rows = db.fetchall(
        """
        SELECT rq.id AS queue_id, rq.page_id, cp.page_url, cp.slug, cp.title_current, cp.h1_current,
               rb.primary_query, rb.secondary_queries_json, rb.suggested_title, rb.suggested_h1, rb.rewrite_focus_json, rb.brief_text
        FROM recovery_queue rq
        JOIN content_pages cp ON cp.id = rq.page_id
        JOIN recovery_briefs rb ON rb.queue_id = rq.id
        WHERE rq.status='briefed'
        ORDER BY rq.priority DESC, rq.queued_at ASC
        LIMIT ?
        """,
        [limit],
    )
    processed = 0
    for row in rows:
        version = _fallback_version(str(row["title_current"] or row["h1_current"] or row["slug"]), str(row["primary_query"] or row["slug"]), str(row["page_url"]))
        if anthropic.enabled:
            prompt = render_prompt(
                template,
                {
                    "page_title": row["title_current"] or row["h1_current"] or row["slug"],
                    "page_url": row["page_url"],
                    "primary_query": row["primary_query"],
                    "secondary_queries": json.loads(row["secondary_queries_json"] or "[]"),
                    "rewrite_focus": json.loads(row["rewrite_focus_json"] or "[]"),
                    "brief_text": row["brief_text"],
                },
            )
            try:
                generated = anthropic.generate(prompt, max_tokens=2200)
                parsed = extract_json_object(generated)
                if parsed:
                    version.update({
                        "title_tag": parsed.get("title_tag") or version["title_tag"],
                        "meta_description": parsed.get("meta_description") or version["meta_description"],
                        "h1": parsed.get("h1") or version["h1"],
                        "intro_html": parsed.get("intro_html") or version["intro_html"],
                        "body_html": parsed.get("body_html") or version["body_html"],
                        "cta_variant": parsed.get("cta_variant") or version["cta_variant"],
                    })
            except Exception as exc:
                logger.warning("Anthropic recovery rewrite fallback used for queue %s: %s", row["queue_id"], exc)
        version_no = db.fetchone("SELECT COALESCE(MAX(version_no),0)+1 AS next_no FROM page_versions WHERE page_id=?", [row["page_id"]])["next_no"]
        body_html = append_related_links(version["body_html"], [], include_demo=True)
        notes = {"word_count": word_count_from_html(body_html), "secondary_queries": json.loads(row["secondary_queries_json"] or "[]")}
        db.execute(
            """
            INSERT INTO page_versions(page_id, version_no, source_type, title_tag, meta_description, h1, intro_html, body_html, headings_json, internal_links_json, cta_variant, notes_json)
            VALUES (?, ?, 'rewrite', ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                row["page_id"],
                version_no,
                version["title_tag"],
                version["meta_description"],
                version["h1"],
                version["intro_html"],
                body_html,
                json.dumps([version["h1"], "What the query is really asking", "A better way to work"]),
                json.dumps([]),
                version["cta_variant"],
                json.dumps(notes),
            ],
        )
        db.execute("UPDATE recovery_queue SET status='drafted', attempt_no=attempt_no+1 WHERE id=?", [row["queue_id"]])
        processed += 1
    finish_run_log(db, run_id, "success", items_processed=processed)
    logger.info("Created %s recovery rewrite drafts", processed)
    return 0
