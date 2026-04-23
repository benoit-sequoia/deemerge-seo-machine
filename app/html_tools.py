from __future__ import annotations

import re

FORBIDDEN_TAG_PATTERNS = [
    r'<html[\s>]',
    r'<head[\s>]',
    r'<body[\s>]',
    r'<style[\s>]',
    r'<!DOCTYPE',
]


def strip_code_fences(text: str) -> str:
    text = re.sub(r'^```(?:html)?\s*', '', text.strip(), flags=re.IGNORECASE)
    text = re.sub(r'\s*```$', '', text, flags=re.IGNORECASE)
    return text.strip()


def html_fragment_only(text: str) -> str:
    text = strip_code_fences(text)
    body_match = re.search(r'<body[^>]*>(.*?)</body>', text, flags=re.IGNORECASE | re.DOTALL)
    if body_match:
        text = body_match.group(1)
    text = re.sub(r'<!DOCTYPE[^>]*>', '', text, flags=re.IGNORECASE)
    text = re.sub(r'<head[^>]*>.*?</head>', '', text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r'</?(html|body)[^>]*>', '', text, flags=re.IGNORECASE)
    return text.strip()


def has_forbidden_wrapper(text: str) -> bool:
    value = text or ''
    if '```' in value:
        return True
    return any(re.search(pattern, value, flags=re.IGNORECASE) for pattern in FORBIDDEN_TAG_PATTERNS)
