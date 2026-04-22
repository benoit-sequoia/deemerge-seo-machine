from __future__ import annotations
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def utcnow_iso() -> str:
    return utcnow().replace(microsecond=0).isoformat()


def ensure_dir(path: str | Path) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def write_json(path: str | Path, data: Any) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def slugify(text: str) -> str:
    value = re.sub(r"[^a-zA-Z0-9\s-]", "", text.lower())
    value = re.sub(r"[-\s]+", "-", value).strip("-")
    return value[:96] or "untitled"


def clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def normalize(value: float, max_value: float) -> float:
    if max_value <= 0:
        return 0.0
    return clamp((value / max_value) * 100.0)


def score_position(position: float) -> float:
    if position <= 0:
        return 0.0
    if 8 <= position <= 40:
        return 100.0 - abs(24 - position) * 3
    if position < 8:
        return 60.0 - abs(8 - position) * 5
    return clamp(50.0 - (position - 40) * 1.2)


def expected_ctr_for_position(position: float) -> float:
    if position <= 1: return 0.28
    if position <= 2: return 0.15
    if position <= 3: return 0.11
    if position <= 5: return 0.07
    if position <= 10: return 0.035
    if position <= 20: return 0.015
    if position <= 40: return 0.007
    return 0.003


def next_weekday_slots(start: datetime, count: int, hours: Iterable[int] = (7, 13)) -> list[datetime]:
    current = start.astimezone(timezone.utc).replace(minute=15, second=0, microsecond=0)
    out: list[datetime] = []
    while len(out) < count:
        for hour in hours:
            candidate = current.replace(hour=hour, minute=15, second=0, microsecond=0)
            if candidate > start and candidate.weekday() < 5 and candidate not in out:
                out.append(candidate)
                if len(out) >= count:
                    break
        current = (current + timedelta(days=1)).replace(hour=min(hours), minute=15, second=0, microsecond=0)
        while current.weekday() >= 5:
            current = (current + timedelta(days=1)).replace(hour=min(hours), minute=15, second=0, microsecond=0)
    return sorted(out)[:count]
