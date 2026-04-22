from __future__ import annotations

from app.services.email_service import EmailService
from app.workers._common import ensure_run_log, finish_run_log


def run(*, db, settings, logger, limit: int = 10) -> int:
    run_id = ensure_run_log(db, "send_weekly_summary")
    email = EmailService(settings)
    new_articles = db.fetchone("SELECT COUNT(*) AS c FROM article_generation_queue WHERE status='published'")["c"]
    rewrites = db.fetchone("SELECT COUNT(*) AS c FROM recovery_queue WHERE status='published'")["c"]
    queued_articles = db.fetchone("SELECT COUNT(*) AS c FROM article_generation_queue WHERE status IN ('queued','briefed','drafted','ready','scheduled')")['c']
    queued_rewrites = db.fetchone("SELECT COUNT(*) AS c FROM recovery_queue WHERE status IN ('queued','briefed','drafted','ready','scheduled')")['c']
    body = (
        f"DEEMERGE SEO weekly summary\n\n"
        f"Published new articles: {new_articles}\n"
        f"Published rewrites: {rewrites}\n"
        f"Articles still in queue: {queued_articles}\n"
        f"Rewrites still in queue: {queued_rewrites}\n"
    )
    if email.enabled:
        try:
            email.send_message("DEEMERGE SEO weekly summary", body)
        except Exception as exc:
            logger.warning("Weekly summary email failed: %s", exc)
    else:
        logger.info(body)
    finish_run_log(db, run_id, "success", items_processed=1)
    return 0
