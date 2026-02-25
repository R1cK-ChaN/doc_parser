"""Watermark detection and removal utilities."""

from __future__ import annotations

WATERMARK_MARKERS = ("macroamy",)


def strip_watermark_lines(markdown: str) -> str:
    """Remove lines that contain known watermark markers."""
    lines = markdown.splitlines()
    cleaned = [ln for ln in lines if not any(m in ln for m in WATERMARK_MARKERS)]
    return "\n".join(cleaned)
