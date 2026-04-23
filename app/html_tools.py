from __future__ import annotations

import re

FORBIDDEN_TAG_PATTERNS = [
    r'<html[\s>]',
    r'<head[\s>]',
    r'<body[\s>]',
    r'<style[\s>]',
    r'<!DOCTYPE',
]
UNSUPPORTED_CLAIM_PATTERNS = [
    r'\bresearch shows\b',
    r'\bstudies show\b',
    r'\baccording to (?:industry )?research\b',
    r'\baccording to studies\b',
    r'\bon average\b',
    r'\baverage of \d',
    r'\bup to \d+%\b',
    r'\b\d+% of\b',
    r'\bprofessionals lose an average\b',
    r'\bteams using .* waste an average\b',
]
GENERIC_META_PATTERNS = [
    r'^updated guide to ',
    r'^learn about ',
    r'^guide to ',
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


def remove_h1_tags(text: str) -> str:
    return re.sub(r'<h1[^>]*>.*?</h1>', '', text or '', flags=re.IGNORECASE | re.DOTALL).strip()


def strip_unsupported_claim_blocks(text: str) -> str:
    value = text or ''
    for pattern in UNSUPPORTED_CLAIM_PATTERNS:
        value = re.sub(rf'<p[^>]*>.*?(?:{pattern}).*?</p>', '', value, flags=re.IGNORECASE | re.DOTALL)
        value = re.sub(rf'<li[^>]*>.*?(?:{pattern}).*?</li>', '', value, flags=re.IGNORECASE | re.DOTALL)
    value = re.sub(r'\n{3,}', '\n\n', value)
    return value.strip()


def sanitize_article_fragment(text: str) -> str:
    value = html_fragment_only(text)
    value = remove_h1_tags(value)
    value = strip_unsupported_claim_blocks(value)
    return value.strip()


def has_forbidden_wrapper(text: str) -> bool:
    value = text or ''
    if '```' in value:
        return True
    return any(re.search(pattern, value, flags=re.IGNORECASE) for pattern in FORBIDDEN_TAG_PATTERNS)


def has_h1_tag(text: str) -> bool:
    return bool(re.search(r'<h1[\s>]', text or '', flags=re.IGNORECASE))


def has_unsupported_claim(text: str) -> bool:
    value = re.sub(r'<[^>]+>', ' ', text or '')
    return any(re.search(pattern, value, flags=re.IGNORECASE) for pattern in UNSUPPORTED_CLAIM_PATTERNS)


def has_generic_meta(meta: str) -> bool:
    value = (meta or '').strip().lower()
    return any(re.search(pattern, value, flags=re.IGNORECASE) for pattern in GENERIC_META_PATTERNS)


def has_required_heading(text: str, heading: str) -> bool:
    pattern = rf'<h[23][^>]*>\s*{re.escape(heading)}\s*</h[23]>'
    return bool(re.search(pattern, text or '', flags=re.IGNORECASE))


def ensure_section(text: str, heading: str, html_block: str) -> str:
    value = (text or '').strip()
    if has_required_heading(value, heading):
        return value
    if value:
        value += '\n\n'
    value += html_block.strip()
    return value
