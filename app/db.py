from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, Iterable

from app.config import Settings


class Database:
    def __init__(self, settings: Settings):
        self.settings = settings
        db_path = Path(settings.sqlite_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path

    @contextmanager
    def connect(self) -> Generator[sqlite3.Connection, None, None]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("PRAGMA busy_timeout = 5000;")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def executescript(self, sql: str) -> None:
        with self.connect() as conn:
            conn.executescript(sql)

    def execute(self, sql: str, params: Iterable | None = None) -> None:
        with self.connect() as conn:
            conn.execute(sql, params or [])

    def fetchall(self, sql: str, params: Iterable | None = None) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return list(conn.execute(sql, params or []))

    def fetchone(self, sql: str, params: Iterable | None = None):
        with self.connect() as conn:
            return conn.execute(sql, params or []).fetchone()

    def insert(self, sql: str, params: Iterable | None = None) -> int:
        with self.connect() as conn:
            cur = conn.execute(sql, params or [])
            return int(cur.lastrowid)
