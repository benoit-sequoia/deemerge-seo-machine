from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.db import Database


def load_sql(*paths: str) -> str:
    root = Path(__file__).resolve().parents[2]
    sql = []
    for path in paths:
        sql.append((root / path).read_text())
    return "\n\n".join(sql)


def logs_dir() -> Path:
    path = Path("/logs")
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_json_log(filename: str, data: Any) -> Path:
    path = logs_dir() / filename
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False))
    return path


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def ensure_run_log(db: Database, worker_name: str) -> int:
    try:
        db.execute("INSERT INTO runs_log(worker_name) VALUES (?)", [worker_name])
        row = db.fetchone("SELECT last_insert_rowid() AS id")
        return int(row["id"])
    except Exception:
        return 0


def finish_run_log(db: Database, run_id: int, status: str, items_processed: int = 0, error_count: int = 0, cost_json: Any | None = None) -> None:
    if not run_id:
        return
    db.execute(
        "UPDATE runs_log SET finished_at=CURRENT_TIMESTAMP, status=?, items_processed=?, error_count=?, cost_json=? WHERE id=?",
        [status, items_processed, error_count, json.dumps(cost_json) if cost_json is not None else None, run_id],
    )
