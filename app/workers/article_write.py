from __future__ import annotations

import json

from app.services.anthropic_service import AnthropicService
from app.utils import extract_json_object, load_prompt, render_prompt, slugify
from app.workers._common import ensure_run_log, finish_run_log


PROMPT_NAME = "article_write.txt"


def _fallback_article(keyword: str, secondaries: list[str], title_options: list[str]) -> dict:
    title = title_options[0] if title_options else keyword.title()
    body = (
        f"<p>{keyword.title()} becomes harder as teams grow and information spreads across inboxes and chat.</p>"
        f"<h2>What teams struggle with</h2><p>Teams often miss context, duplicate replies, and lose accountability when communication is scattered.</p>"
        f"<h2>What to look for</h2><p>Look for clear ownership, visibility, and better coordination across email and chat.</p>"
        f"<h2>How DEEMERGE helps</h2><p>DEEMERGE helps teams group related communication, summarize context, and act faster on what matters.</p>"
    )
    return {
        "title_tag": title,
        "meta_description": f"Learn about {keyword} and see how DEEMERGE helps teams reduce confusion and move faster.",
        "slug": slugify(keyword),
        "h1": title,
        "excerpt": f"A practical guide to {keyword} for teams.",
        "body_html": body,
        "faq": [
            {"q": f"What is {keyword}?", "a": f"It refers to workflows and tools around {keyword}."},
            {"q": "How does DEEMERGE fit?", "a": "DEEMERGE helps teams centralize context and act faster."},
        ],
        "schema": {"type": "BlogPosting", "keywords": [keyword, *secondaries[:5]]},
        "image_prompt": f"Editorial SaaS illustration about {keyword}, inboxes, collaboration, modern software UI", 
    }


def run(*, db, settings, logger, limit: int = 10) -> int:
    run_id = ensure_run_log(db, "article_write")
    anthropic = AnthropicService(settings)
    template = load_prompt(PROMPT_NAME)
    rows = db.fetchall(
        """
        SELECT agq.id AS queue_id, ab.primary_keyword, ab.secondary_keywords_json, ab.title_options_json, ab.outline_json, ab.cta_angle, ab.brief_text
        FROM article_generation_queue agq
        JOIN article_briefs ab ON ab.queue_id = agq.id
        LEFT JOIN article_drafts ad ON ad.queue_id = agq.id
        WHERE agq.status='briefed' AND ad.id IS NULL
        ORDER BY agq.priority DESC, agq.queued_at ASC
        LIMIT ?
        """,
        [limit],
    )
    processed = 0
    for row in rows:
        secondaries = json.loads(row["secondary_keywords_json"] or "[]")
        title_options = json.loads(row["title_options_json"] or "[]")
        draft = _fallback_article(str(row["primary_keyword"]), secondaries, title_options)
        if anthropic.enabled:
            prompt = render_prompt(
                template,
                {
                    "primary_keyword": row["primary_keyword"],
                    "secondary_keywords": secondaries,
                    "title_options": title_options,
                    "outline": json.loads(row["outline_json"] or "[]"),
                    "cta_angle": row["cta_angle"],
                    "brief_text": row["brief_text"],
                },
            )
            try:
                parsed = extract_json_object(anthropic.generate(prompt, max_tokens=3000))
                if parsed:
                    draft.update({
                        "title_tag": parsed.get("title_tag") or draft["title_tag"],
                        "meta_description": parsed.get("meta_description") or draft["meta_description"],
                        "slug": slugify(parsed.get("slug") or draft["slug"]),
                        "h1": parsed.get("h1") or draft["h1"],
                        "excerpt": parsed.get("excerpt") or draft["excerpt"],
                        "body_html": parsed.get("body_html") or draft["body_html"],
                        "faq": parsed.get("faq") or draft["faq"],
                        "schema": parsed.get("schema") or draft["schema"],
                        "image_prompt": parsed.get("image_prompt") or draft["image_prompt"],
                    })
            except Exception as exc:
                logger.warning("Anthropic article write fallback used for queue %s: %s", row["queue_id"], exc)
        db.execute(
            """
            INSERT INTO article_drafts(queue_id, title_tag, meta_description, slug, h1, excerpt, body_html, faq_json, schema_json, image_prompt, quality_score, validation_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
            """,
            [
                row["queue_id"],
                draft["title_tag"],
                draft["meta_description"],
                draft["slug"],
                draft["h1"],
                draft["excerpt"],
                draft["body_html"],
                json.dumps(draft["faq"]),
                json.dumps(draft["schema"]),
                draft["image_prompt"],
                json.dumps({"secondary_keywords": secondaries}),
            ],
        )
        db.execute("UPDATE article_generation_queue SET status='drafted', attempt_no=attempt_no+1 WHERE id=?", [row["queue_id"]])
        processed += 1
    finish_run_log(db, run_id, "success", items_processed=processed)
    logger.info("Created %s article drafts", processed)
    return 0
