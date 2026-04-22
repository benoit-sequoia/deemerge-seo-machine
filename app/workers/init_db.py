from __future__ import annotations

from app.workers._common import ensure_run_log, finish_run_log, load_sql


def run(*, db, settings, logger, limit: int = 10) -> int:
    run_id = ensure_run_log(db, "init_db")
    sql = load_sql("migrations/001_init.sql", "migrations/002_indexes.sql")
    db.executescript(sql)
    finish_run_log(db, run_id, "success")
    logger.info("Database initialized at %s", settings.sqlite_path)
    return 0
