from __future__ import annotations

import json

from app.workers._common import ensure_run_log, finish_run_log


def _title_options(primary: str) -> list[str]:
    keyword = primary.strip()
    lower = keyword.lower()
    human = keyword.title()
    if 'alternative' in lower:
        return [
            f"{human}: Better Options for Teams in 2026",
            f"Best {human} for Teams That Need Better Workflow Visibility",
            f"{human}: What to Choose When Teams Outgrow Basic Tools",
        ]
    if 'shared inbox' in lower or 'shared mailbox' in lower:
        return [
            f"{human}: Best Options for Teams in 2026",
            f"{human}: How Teams Reduce Missed Replies and Duplicate Work",
            f"{human}: What Growing Teams Should Use Instead of a Basic Shared Mailbox",
        ]
    return [
        f"{human}: Best Solutions for Teams in 2026",
        f"{human}: How Teams Solve the Workflow Problem Behind the Keyword",
        f"{human}: What It Is, Where Teams Struggle, and What to Use",
    ]


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
        secondary = json.loads(row["secondary_keywords_json"] or '[]')
        intent = row["intent_type"]
        angle = (
            f"Target the keyword '{row['primary_keyword']}' clearly and directly. "
            "Explain the real workflow problem behind the query, avoid fluff, avoid unsupported stats, "
            "and show how DEEMERGE helps teams reduce missed replies, context switching, and unclear ownership across email and chat."
        )
        title_options = _title_options(row['primary_keyword'])
        outline = [
            "Direct introduction to the keyword and why teams search it",
            "What breaks in the current workflow",
            "Where basic tools fall short",
            "How DEEMERGE solves this in practice",
            "Next step with DEEMERGE",
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
