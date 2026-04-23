from __future__ import annotations

from pathlib import Path
from xml.sax.saxutils import escape


def _wrap_lines(text: str, width: int = 22) -> list[str]:
    words = text.split()
    lines: list[str] = []
    current: list[str] = []
    current_len = 0
    for word in words:
        add = len(word) + (1 if current else 0)
        if current and current_len + add > width:
            lines.append(" ".join(current))
            current = [word]
            current_len = len(word)
        else:
            current.append(word)
            current_len += add
    if current:
        lines.append(" ".join(current))
    return lines[:6]


def build_article_svg(title: str, slug: str) -> str:
    lines = _wrap_lines(title, width=24)
    if not lines:
        lines = [slug.replace('-', ' ').title()]
    title_svg = []
    y = 265
    for line in lines:
        title_svg.append(
            f'<text x="120" y="{y}" font-family="Arial, Helvetica, sans-serif" font-size="56" font-weight="700" fill="#0f172a">{escape(line)}</text>'
        )
        y += 78
    title_block = "\n  ".join(title_svg)
    slug_text = escape(slug.replace('-', ' '))
    return f'''<svg xmlns="http://www.w3.org/2000/svg" width="1600" height="900" viewBox="0 0 1600 900" role="img" aria-label="{escape(title)}">
  <defs>
    <linearGradient id="bg" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0%" stop-color="#eff6ff" />
      <stop offset="100%" stop-color="#dbeafe" />
    </linearGradient>
  </defs>
  <rect width="1600" height="900" fill="url(#bg)" />
  <rect x="72" y="72" width="1456" height="756" rx="32" fill="#ffffff" stroke="#cbd5e1" stroke-width="4" />
  <text x="120" y="150" font-family="Arial, Helvetica, sans-serif" font-size="34" font-weight="700" fill="#2563eb">DEEMERGE BLOG</text>
  <text x="120" y="200" font-family="Arial, Helvetica, sans-serif" font-size="24" font-weight="600" fill="#475569">AI assistant for work across email and chat</text>
  {title_block}
  <rect x="120" y="708" width="460" height="64" rx="12" fill="#0f172a" />
  <text x="146" y="750" font-family="Arial, Helvetica, sans-serif" font-size="28" font-weight="700" fill="#ffffff">deemerge.ai/blog/{slug_text}</text>
  <circle cx="1390" cy="162" r="62" fill="#2563eb" opacity="0.18" />
  <circle cx="1310" cy="730" r="92" fill="#38bdf8" opacity="0.16" />
  <circle cx="1470" cy="792" r="42" fill="#0ea5e9" opacity="0.18" />
</svg>
'''


def ensure_article_svg(title: str, slug: str, output_dir: str | Path = '/data/generated_images') -> tuple[str, str]:
    outdir = Path(output_dir)
    outdir.mkdir(parents=True, exist_ok=True)
    path = outdir / f'{slug}.svg'
    svg = build_article_svg(title=title, slug=slug)
    path.write_text(svg, encoding='utf-8')
    return str(path), title
