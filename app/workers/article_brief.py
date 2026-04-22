from __future__ import annotations

import json

from app.services.anthropic_service import AnthropicService
from app.utils import extract_json_object, load_prompt, render_prompt
from app.workers._common import ensure_run_log, finish_run_log


PROMPT_NAME = "article_brief.txt"


def _fallback_brief(keyword: str, secondary: list[str], cluster_name: str) -> dict:
    title = keyword.title()
    return {
        "search_intent": "commercial" if "alternative" in keyword or "shared inbox" in keyword else "informational",
        "article_angle": f"Explain {keyword} clearly, show what buyers should look for, and connect it to DEEMERGE.",
        "title_options": [title, f"Best {title} Options in 2026"],
        "outline": [
            f"What {keyword} means",
            f"Common problems with {keyword}",
            f"What to look for in {keyword}",
            "How DEEMERGE fits",
        ],
        "internal_links": [],
        "cta_angle": f"Position DEEMERGE as a practical way to solve {keyword} related workflow issues.",
        "brief_text": f"Write a cluster supporting article for the {cluster_name} cluster around {keyword}.",
    }


def run(*, db, settings, logger, limit: int = 10) -> int:
    run_id = ensure_run_log(db, "article_brief")
    anthropic = AnthropicService(settings)
    template = load_prompt(PROMPT_NAME)
    rows = db.fetchall(
        """
        SELECT agq.id AS queue_id, kc.cluster_id, kc.primary_keyword, kc.secondary_keywords_json, c.cluster_name
        FROM article_generation_queue agq
        JOIN keyword_candidates kc ON kc.id = agq.keyword_candidate_id
        JOIN clusters c ON c.id = kc.cluster_id
        LEFT JOIN article_briefs ab ON ab.queue_id = agq.id
        WHERE agq.status='queued' AND ab.id IS NULL
        ORDER BY agq.priority DESC, agq.queued_at ASC
        LIMIT ?
        """,
        [limit],
    )
    processed = 0
    for row in rows:
        secondaries = json.loads(row["secondary_keywords_json"] or "[]")
        brief = _fallback_brief(str(row["primary_keyword"]), secondaries, str(row["cluster_name"]))
        if anthropic.enabled:
            prompt = render_prompt(
                template,
                {
                    "primary_keyword": row["primary_keyword"],
                    "secondary_keywords": secondaries,
                    "cluster_name": row["cluster_name"],
                },
            )
            try:
                parsed = extract_json_object(anthropic.generate(prompt, fast=True, max_tokens=1500))
                if parsed:
                    brief.update(
                        {
                            "search_intent": parsed.get("search_intent") or brief["search_intent"],
                            "article_angle": parsed.get("article_angle") or brief["article_angle"],
                            "title_options": parsed.get("title_options") or brief["title_options"],
                            "outline": parsed.get("outline") or brief["outline"],
                            "internal_links": parsed.get("internal_links") or brief["internal_links"],
                            "cta_angle": parsed.get("cta_angle") or brief["cta_angle"],
                            "brief_text": parsed.get("brief_text") or brief["brief_text"],
                        }
                    )
            except Exception as exc:
                logger.warning("Anthropic article brief fallback used for queue %s: %s", row["queue_id"], exc)
        db.execute(
            """
            INSERT INTO article_briefs(queue_id, cluster_id, primary_keyword, secondary_keywords_json, search_intent, article_angle, title_options_json, outline_json, internal_links_json, cta_angle, brief_text)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                row["queue_id"],
                row["cluster_id"],
                row["primary_keyword"],
                json.dumps(secondaries),
                brief["search_intent"],
                brief["article_angle"],
                json.dumps(brief["title_options"]),
                json.dumps(brief["outline"]),
                json.dumps(brief["internal_links"]),
                brief["cta_angle"],
                brief["brief_text"],
            ],
        )
        db.execute("UPDATE article_generation_queue SET status='briefed' WHERE id=?", [row["queue_id"]])
        processed += 1
    finish_run_log(db, run_id, "success", items_processed=processed)
    logger.info("Created %s article briefs", processed)
    return 0
