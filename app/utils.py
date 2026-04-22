from __future__ import annotations

import json
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from string import Template
from typing import Any


def slugify(value: str) -> str:
    value = value.lower().strip()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    value = re.sub(r"-{2,}", "-", value).strip("-")
    return value[:96] or "article"


def load_prompt(name: str) -> str:
    root = Path(__file__).resolve().parents[1]
    return (root / "prompts" / name).read_text(encoding="utf-8")


def render_prompt(template_text: str, context: dict[str, Any]) -> str:
    safe = {k: json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else str(v) for k, v in context.items()}
    return Template(template_text).safe_substitute(safe)


def extract_json_object(text: str) -> dict[str, Any]:
    text = text.strip()
    if not text:
        return {}
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if not match:
        return {}
    try:
        return json.loads(match.group(0))
    except json.JSONDecodeError:
        return {}


def word_count_from_html(html: str) -> int:
    text = re.sub(r"<[^>]+>", " ", html or "")
    return len(re.findall(r"\b\w+\b", text))


def html_contains_phrase(html: str, phrase: str) -> bool:
    text = re.sub(r"<[^>]+>", " ", html or "").lower()
    return phrase.lower() in text


def append_related_links(body_html: str, related: list[tuple[str, str]], include_demo: bool = True) -> str:
    parts = [body_html.strip()]
    if related:
        items = "".join([f'<li><a href="{url}">{anchor}</a></li>' for anchor, url in related])
        parts.append(f"<h2>Related reading</h2><ul>{items}</ul>")
    if include_demo:
        parts.append(
            '<h2>How DEEMERGE fits</h2>'
            '<p>DEEMERGE helps teams pull scattered email and chat communication into one place, group related threads, surface what matters, and act faster without losing context.</p>'
            '<p><a href="https://www.deemerge.ai/pricing">See pricing</a> or <a href="https://www.deemerge.ai/demo">book a demo</a>.</p>'
        )
    return "\n".join([p for p in parts if p])


def next_weekday_slots(total: int, start: datetime | None = None) -> list[datetime]:
    start = start or datetime.now(UTC)
    cursor = start.replace(second=0, microsecond=0)
    slots: list[datetime] = []
    morning_hour = 7
    afternoon_hour = 13
    while len(slots) < total:
        if cursor.weekday() < 5:
            morning = cursor.replace(hour=morning_hour, minute=15)
            afternoon = cursor.replace(hour=afternoon_hour, minute=15)
            for slot in (morning, afternoon):
                if slot > start:
                    slots.append(slot)
                    if len(slots) >= total:
                        break
        cursor = (cursor + timedelta(days=1)).replace(hour=0, minute=0)
    return slots


def iso_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()
