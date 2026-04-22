from __future__ import annotations
import sqlite3
from pathlib import Path
from typing import Iterable, Sequence
from .config import load_settings


def connect() -> sqlite3.Connection:
    settings = load_settings()
    Path(settings.sqlite_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(settings.sqlite_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn


def apply_migrations(conn: sqlite3.Connection, migrations_dir: str | Path) -> None:
    for path in sorted(Path(migrations_dir).glob("*.sql")):
        conn.executescript(path.read_text(encoding="utf-8"))
    conn.commit()


def query_all(conn: sqlite3.Connection, sql: str, params: Sequence | None = None):
    return conn.execute(sql, params or []).fetchall()


def query_one(conn: sqlite3.Connection, sql: str, params: Sequence | None = None):
    return conn.execute(sql, params or []).fetchone()
