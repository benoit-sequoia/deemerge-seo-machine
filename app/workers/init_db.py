from __future__ import annotations

from pathlib import Path

from app.workers._common import ensure_run_log, finish_run_log


def run(*, db, settings, logger, limit: int = 10) -> int:
    run_id = ensure_run_log(db, "init_db")
    migrations_dir = Path(__file__).resolve().parents[2] / "migrations"
    sql_parts = []
    for path in sorted(migrations_dir.glob("*.sql")):
        sql_parts.append(path.read_text(encoding="utf-8"))
    db.executescript("\n\n".join(sql_parts))
    finish_run_log(db, run_id, "success")
    logger.info("Database initialized at %s", settings.sqlite_path)
    return 0
