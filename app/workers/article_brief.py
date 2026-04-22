from __future__ import annotations

import json

from app.workers._common import ensure_run_log, finish_run_log


def run(*, db, settings, logger, limit: int = 10) -> int:
    run_id = ensure_run_log(db, "article_brief")
    rows = db.fetchall(
        """
        SELECT q.id AS queue_id, kc.cluster_id, kc.primary_keyword, kc.secondary_keywords_json, kc.intent_type
        FROM article_generation_queue q
        JOIN keyword_candidates kc ON kc.id = q.keyword_candidate_id
        LEFT JOIN article_briefs ab ON ab.queue_id = q.id
        WHERE ab.id IS NULL AND q.status='queued'
        ORDER BY q.priority DESC, q.id ASC
        LIMIT ?
        """,
        [limit],
    )
    processed = 0
    for row in rows:
        secondary = json.loads(row["secondary_keywords_json"] or "[]")
        intent = row["intent_type"]
        angle = f"Explain {row['primary_keyword']} clearly, then show how DEEMERGE fits for teams that manage work across email and chat."
        title_options = [
            f"Best {row['primary_keyword'].title()} Solutions for Teams in 2026",
            f"{row['primary_keyword'].title()}: What It Is, How It Works, and the Best Options",
            f"How to Improve {row['primary_keyword'].title()} for Fast Moving Teams",
        ]
        outline = [
            "What the keyword means and why teams search it",
            "Main problems and pain points",
            "How teams handle it today",
            "How DEEMERGE solves this in practice",
            "Best practices and final recommendations",
        ]
        db.execute(
            """
            INSERT INTO article_briefs(queue_id, cluster_id, primary_keyword, secondary_keywords_json, search_intent, article_angle, title_options_json, outline_json, internal_links_json, cta_angle, brief_text)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, '[]', ?, ?)
            """,
            [
                row["queue_id"],
                row["cluster_id"],
                row["primary_keyword"],
                json.dumps(secondary),
                intent,
                angle,
                json.dumps(title_options),
                json.dumps(outline),
                "Book a DEEMERGE demo or view pricing",
                angle,
            ],
        )
        db.execute("UPDATE article_generation_queue SET status='briefing' WHERE id=?", [row["queue_id"]])
        processed += 1
    finish_run_log(db, run_id, "success", items_processed=processed)
    logger.info("Created %s article briefs", processed)
    return 0
