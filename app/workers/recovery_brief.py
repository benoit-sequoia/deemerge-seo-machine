from __future__ import annotations

import json

from app.services.anthropic_service import AnthropicService
from app.utils import extract_json_object, load_prompt, render_prompt
from app.workers._common import ensure_run_log, finish_run_log


PROMPT_NAME = "rewrite_brief.txt"


def _fallback_brief(page_title: str, top_queries: list[dict]) -> dict:
    primary = top_queries[0]["query"] if top_queries else page_title
    secondaries = [q["query"] for q in top_queries[1:6]]
    return {
        "primary_query": primary,
        "secondary_queries": secondaries,
        "suggested_title": primary.title() if primary else page_title,
        "suggested_h1": primary.title() if primary else page_title,
        "rewrite_focus": [
            "tighten title to match search intent",
            "answer the query faster in the intro",
            "add internal links to related DEEMERGE pages",
        ],
        "brief_text": f"Rewrite this page to better match the main query '{primary}' and improve CTR.",
    }


def run(*, db, settings, logger, limit: int = 10) -> int:
    run_id = ensure_run_log(db, "recovery_brief")
    anthropic = AnthropicService(settings)
    queued = db.fetchall(
        """
        SELECT rq.id AS queue_id, rq.page_id, cp.title_current, cp.h1_current, cp.page_url, cp.slug,
               rc.top_queries_json, rc.opportunity_score
        FROM recovery_queue rq
        JOIN content_pages cp ON cp.id = rq.page_id
        JOIN recovery_candidates rc ON rc.id = rq.candidate_id
        LEFT JOIN recovery_briefs rb ON rb.queue_id = rq.id
        WHERE rq.status='queued' AND rb.id IS NULL
        ORDER BY rq.priority DESC, rq.queued_at ASC
        LIMIT ?
        """,
        [limit],
    )
    processed = 0
    template = load_prompt(PROMPT_NAME)
    for row in queued:
        top_queries = json.loads(row["top_queries_json"] or "[]")
        brief_data = _fallback_brief(str(row["title_current"] or row["h1_current"] or row["slug"]), top_queries)
        if anthropic.enabled:
            prompt = render_prompt(
                template,
                {
                    "page_title": row["title_current"] or row["h1_current"] or row["slug"],
                    "page_url": row["page_url"],
                    "top_queries": top_queries,
                    "opportunity_score": row["opportunity_score"],
                },
            )
            try:
                generated = anthropic.generate(prompt, fast=True, max_tokens=1200)
                parsed = extract_json_object(generated)
                if parsed:
                    brief_data = {
                        "primary_query": parsed.get("primary_query") or brief_data["primary_query"],
                        "secondary_queries": parsed.get("secondary_queries") or brief_data["secondary_queries"],
                        "suggested_title": parsed.get("suggested_title") or brief_data["suggested_title"],
                        "suggested_h1": parsed.get("suggested_h1") or brief_data["suggested_h1"],
                        "rewrite_focus": parsed.get("rewrite_focus") or brief_data["rewrite_focus"],
                        "brief_text": parsed.get("brief_text") or brief_data["brief_text"],
                    }
            except Exception as exc:
                logger.warning("Anthropic recovery brief fallback used for queue %s: %s", row["queue_id"], exc)
        db.execute(
            """
            INSERT INTO recovery_briefs(queue_id, page_id, primary_query, secondary_queries_json, suggested_title, suggested_h1, rewrite_focus_json, brief_text)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                row["queue_id"],
                row["page_id"],
                brief_data["primary_query"],
                json.dumps(brief_data["secondary_queries"]),
                brief_data["suggested_title"],
                brief_data["suggested_h1"],
                json.dumps(brief_data["rewrite_focus"]),
                brief_data["brief_text"],
            ],
        )
        db.execute("UPDATE recovery_queue SET status='briefed' WHERE id=?", [row["queue_id"]])
        processed += 1
    finish_run_log(db, run_id, "success", items_processed=processed)
    logger.info("Created %s recovery briefs", processed)
    return 0
