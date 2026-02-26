"""Watermark detection and removal utilities.

Four-layer strategy:
  1. Line-level removal   — exact markers + regex patterns
  2. HTML table removal    — social media stats tables (粉丝 AND 转评赞)
  3. Inline word substitution — embedded fragments inside real content lines
  4. Repeated HTML comment removal — comments appearing 3+ times
"""

from __future__ import annotations

import re
from collections import Counter

# ---------------------------------------------------------------------------
# Layer 1 — line-level markers (substring match → drop entire line)
# ---------------------------------------------------------------------------

WATERMARK_MARKERS = (
    "macroamy",
    "nacroany",
    "mroamy",
    "macrcy",
    "roamy",
    "付费",
    "扫一扫",
    "坦途宏观",
    "查看微博主页",
    "微信收藏",
    "GMF Research（坦途宏观）",
)

# Regex patterns that match an entire line (anchored)
WATERMARK_LINE_PATTERNS = [
    re.compile(r"^专业的宏(?:观.*)?$"),
    re.compile(r"^<!--\s*\*{0,2}联系我们\*{0,2}\s*-->$"),
    re.compile(r"^<!--.*?@Degg.*?-->$"),
    re.compile(r"^<!--\s*微博\s*-->$"),
]

# ---------------------------------------------------------------------------
# Layer 2 — HTML table removal (social media stats tables)
# ---------------------------------------------------------------------------

_TABLE_RE = re.compile(r"<table[\s>].*?</table>", re.DOTALL)


def _strip_social_media_tables(text: str) -> str:
    """Remove <table>…</table> blocks that contain both 粉丝 AND 转评赞."""

    def _is_social_table(match: re.Match[str]) -> bool:
        fragment = match.group(0)
        return "粉丝" in fragment and "转评赞" in fragment

    return _TABLE_RE.sub(lambda m: "" if _is_social_table(m) else m.group(0), text)


# ---------------------------------------------------------------------------
# Layer 3 — inline word substitution (strip fragments, keep the line)
# ---------------------------------------------------------------------------

WATERMARK_INLINE_SUBS = [
    (re.compile(r"macroamy整理"), ""),
    (re.compile(r"nacroany整理"), ""),
    (re.compile(r"roamy整理"), ""),
]


# ---------------------------------------------------------------------------
# Layer 4 — repeated HTML comment removal (3+ occurrences = watermark)
# ---------------------------------------------------------------------------

def _strip_repeated_html_comments(text: str) -> str:
    """Remove HTML comments that appear 3+ times (repeated watermarks).

    Single-occurrence or rare comments are preserved as they may be meaningful.
    """
    comments = re.findall(r"<!--.*?-->", text, re.DOTALL)
    if not comments:
        return text

    counts = Counter(comments)
    for comment, count in counts.items():
        if count >= 3:
            pattern = re.compile(
                r"\n*" + re.escape(comment) + r"\n*",
                re.DOTALL,
            )
            text = pattern.sub("\n", text)

    return text.strip("\n") + "\n" if text.strip() else text


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def strip_watermarks(markdown: str) -> str:
    """Remove watermark noise from *markdown* using all four layers."""
    # Layer 1 — inline substitution (before line removal so partial
    #   matches like "私营部roamy整理" → "私营部" aren't dropped entirely)
    text = markdown
    for pattern, repl in WATERMARK_INLINE_SUBS:
        text = pattern.sub(repl, text)

    # Layer 2 — line-level removal
    #   Also strip empty HTML comments and symbol-garbage lines
    text = re.sub(r"<!--\s*-->", "", text)
    lines = text.splitlines()
    cleaned: list[str] = []
    for ln in lines:
        if any(m in ln for m in WATERMARK_MARKERS):
            continue
        if any(p.match(ln.strip()) for p in WATERMARK_LINE_PATTERNS):
            continue
        if "()■()" in ln:
            continue
        cleaned.append(ln)
    text = "\n".join(cleaned)

    # Layer 3 — HTML table removal
    text = _strip_social_media_tables(text)

    # Layer 4 — repeated HTML comment removal (3+ occurrences = watermark)
    text = _strip_repeated_html_comments(text)

    return text


# Backward-compatible alias
strip_watermark_lines = strip_watermarks
