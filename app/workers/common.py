from __future__ import annotations
from pathlib import Path
from ..helpers import utcnow_iso, write_json


def start_run(conn, worker_name: str) -> int:
    cur = conn.execute("INSERT INTO runs_log(worker_name, started_at, status) VALUES (?, ?, ?)", (worker_name, utcnow_iso(), "running"))
    conn.commit()
    return int(cur.lastrowid)


def finish_run(conn, run_id: int, items_processed: int = 0, error_count: int = 0, status: str = "ok", cost_json: str | None = None):
    conn.execute("UPDATE runs_log SET finished_at=?, items_processed=?, error_count=?, status=?, cost_json=? WHERE id=?", (utcnow_iso(), items_processed, error_count, status, cost_json, run_id))
    conn.commit()


def dump_log(name: str, data):
    write_json(Path("/logs") / name, data)
