from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from app.db import Database


def load_sql(*paths: str) -> str:
    root = Path(__file__).resolve().parents[2]
    sql = []
    for path in paths:
        sql.append((root / path).read_text(encoding="utf-8"))
    return "\n\n".join(sql)


def ensure_run_log(db: Database, worker_name: str) -> int:
    try:
        return db.insert("INSERT INTO runs_log(worker_name) VALUES (?)", [worker_name])
    except Exception:
        return 0


def finish_run_log(db: Database, run_id: int, status: str, items_processed: int = 0, error_count: int = 0, cost_json: Any | None = None) -> None:
    if not run_id:
        return
    db.execute(
        "UPDATE runs_log SET finished_at=CURRENT_TIMESTAMP, status=?, items_processed=?, error_count=?, cost_json=? WHERE id=?",
        [status, items_processed, error_count, json.dumps(cost_json) if cost_json is not None else None, run_id],
    )
