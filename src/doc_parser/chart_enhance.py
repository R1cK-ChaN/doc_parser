"""Chart enhancement: replace hallucinated table HTML with VLM summaries."""

from __future__ import annotations

import base64
import logging
import re
from pathlib import Path
from typing import Any

import httpx
import pymupdf

from doc_parser.config import Settings
from doc_parser.watermark import strip_watermarks

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# System prompt for chart summarization
# ---------------------------------------------------------------------------

_CHART_SYSTEM_PROMPT = """\
You are a concise chart/graph analyst. Describe the chart in the image: \
what type it is (bar, line, pie, etc.), what the axes represent, key data \
points, and the main takeaway. Be brief (2-4 sentences). Do not fabricate \
specific numbers unless they are clearly visible in the chart.\
"""


# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------


def _gather_page_text(detail: list[dict[str, Any]], page_id: int) -> str:
    """Collect text from non-image detail elements on the same page.

    Returns concatenated text truncated to ~1000 characters.
    """
    parts: list[str] = []
    for el in detail:
        el_page = el.get("page_id") or el.get("page_number", 0)
        if el_page != page_id:
            continue
        if el.get("type") in ("image",):
            continue
        text = el.get("text", "").strip()
        if text:
            parts.append(text)
    combined = "\n".join(parts)
    if len(combined) > 1000:
        combined = combined[:1000]
    return combined


def extract_chart_image(
    pdf_path: str | Path,
    page_index: int,
    position: list | dict,
    *,
    textin_page_size: tuple[float, float] | None = None,
    scale: float = 2.0,
) -> bytes:
    """Crop a chart region from a PDF page and return PNG bytes.

    Args:
        pdf_path: Path to the PDF file.
        page_index: 0-based page index.
        position: Bounding box from TextIn detail elements.
            Flat list [x0,y0, x1,y1, x2,y2, x3,y3] or dict with quad/points/x,y keys.
        textin_page_size: (width, height) from TextIn pages JSON, used to scale
            coordinates to PyMuPDF space. None = no scaling.
        scale: Render scale factor (2.0 = 144 DPI for a 72-DPI page).

    Returns:
        PNG image bytes of the cropped chart region.
    """
    doc = pymupdf.open(str(pdf_path))
    try:
        page = doc[page_index]

        # Build the clip rectangle from position data
        clip = _position_to_rect(position, page)

        # Scale from TextIn coordinate space to PyMuPDF space if needed
        if textin_page_size:
            sx = page.rect.width / textin_page_size[0]
            sy = page.rect.height / textin_page_size[1]
            clip = pymupdf.Rect(
                clip.x0 * sx, clip.y0 * sy,
                clip.x1 * sx, clip.y1 * sy,
            )

        # Render the clipped region
        mat = pymupdf.Matrix(scale, scale)
        pix = page.get_pixmap(matrix=mat, clip=clip)
        return pix.tobytes("png")
    finally:
        doc.close()


def _position_to_rect(
    position: list | dict,
    page: pymupdf.Page,
) -> pymupdf.Rect:
    """Convert a TextIn position to a PyMuPDF Rect.

    TextIn detail elements use either:
    - Flat list: [x0, y0, x1, y1, x2, y2, x3, y3] (4 quad points)
    - Dict with "quad", "points", or "x"/"y"/"width"/"height" keys
    """
    # Flat list of 8 numbers: [x0,y0, x1,y1, x2,y2, x3,y3]
    if isinstance(position, list):
        if len(position) == 8:
            xs = [position[i] for i in (0, 2, 4, 6)]
            ys = [position[i] for i in (1, 3, 5, 7)]
            return pymupdf.Rect(min(xs), min(ys), max(xs), max(ys))
        elif len(position) == 4:
            # [x0, y0, x1, y1]
            return pymupdf.Rect(position[0], position[1], position[2], position[3])
        else:
            logger.warning("Unexpected position list length %d, using full page", len(position))
            return page.rect

    # Dict formats below
    # Quad-point format: [[x0,y0], [x1,y1], [x2,y2], [x3,y3]]
    if "quad" in position:
        pts = position["quad"]
        xs = [p[0] for p in pts]
        ys = [p[1] for p in pts]
        return pymupdf.Rect(min(xs), min(ys), max(xs), max(ys))

    # Array of corner points
    if "points" in position:
        pts = position["points"]
        xs = [p[0] for p in pts]
        ys = [p[1] for p in pts]
        return pymupdf.Rect(min(xs), min(ys), max(xs), max(ys))

    # Simple rect format
    if "x" in position and "y" in position:
        x = position["x"]
        y = position["y"]
        w = position.get("width", 0)
        h = position.get("height", 0)
        return pymupdf.Rect(x, y, x + w, y + h)

    # Fallback: full page
    logger.warning("Unrecognized position format, using full page: %s", position)
    return page.rect


async def summarize_chart(
    image_bytes: bytes,
    settings: Settings,
    page_text: str = "",
) -> str:
    """Send a chart image to a VLM and return a text summary.

    Uses the OpenAI-compatible vision API format via the same
    llm_base_url and llm_api_key as the LLM extraction provider.
    """
    b64 = base64.b64encode(image_bytes).decode()

    user_content: list[dict[str, Any]] = [
        {
            "type": "image_url",
            "image_url": {"url": f"data:image/png;base64,{b64}"},
        },
    ]
    if page_text:
        user_content.append({
            "type": "text",
            "text": f"Surrounding text from the same page:\n{page_text}",
        })

    payload = {
        "model": settings.vlm_model,
        "messages": [
            {"role": "system", "content": _CHART_SYSTEM_PROMPT},
            {
                "role": "user",
                "content": user_content,
            },
        ],
        "max_tokens": settings.vlm_max_tokens,
        "temperature": 0.0,
    }

    url = f"{settings.llm_base_url.rstrip('/')}/chat/completions"

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(120.0, connect=30.0),
        headers={
            "Authorization": f"Bearer {settings.llm_api_key}",
            "Content-Type": "application/json",
        },
    ) as client:
        resp = await client.post(url, json=payload)
        resp.raise_for_status()
        body = resp.json()

    return body["choices"][0]["message"]["content"].strip()


def replace_chart_table(
    markdown: str,
    hallucinated_html: str,
    summary: str,
) -> str:
    """Replace a hallucinated HTML table in the markdown with a VLM summary.

    Args:
        markdown: The full markdown string.
        hallucinated_html: The <table>...</table> HTML to replace.
        summary: The VLM-generated chart description.

    Returns:
        Updated markdown with the table replaced by the summary.
    """
    replacement = f"[Chart Summary] {summary}"
    return markdown.replace(hallucinated_html, replacement, 1)


async def enhance_charts(
    pdf_path: str | Path,
    markdown: str,
    detail: list[dict[str, Any]],
    settings: Settings,
    pages: list[dict[str, Any]] | None = None,
) -> tuple[str, int]:
    """Orchestrate chart enhancement: find charts, crop, summarize, replace.

    Args:
        pdf_path: Path to the source PDF.
        markdown: The original markdown from TextIn.
        detail: The detail elements list from the parse result.
        settings: Application settings (must have vlm_model set).
        pages: TextIn pages list (with width/height per page) for coordinate scaling.

    Returns:
        Tuple of (enhanced_markdown, chart_count).
    """
    # Find chart elements
    chart_elements = [
        el for el in detail
        if el.get("type") == "image" and el.get("sub_type") == "chart"
    ]

    if not chart_elements:
        return markdown, 0

    # Build page size lookup: page_id (1-based) -> (width, height)
    page_sizes: dict[int, tuple[float, float]] = {}
    if pages:
        for p in pages:
            pid = p.get("page_id", 0)
            if pid and "width" in p and "height" in p:
                page_sizes[pid] = (p["width"], p["height"])

    enhanced = markdown
    chart_count = 0

    for el in chart_elements:
        text = el.get("text", "")
        position = el.get("position")
        # TextIn uses page_id (1-based); fall back to page_number
        page_id = el.get("page_id") or el.get("page_number", 1)

        if not text or position is None:
            logger.warning("Chart element missing text or position, skipping")
            continue

        # page_id is 1-based in TextIn, PyMuPDF uses 0-based
        page_index = page_id - 1
        textin_page_size = page_sizes.get(page_id)

        try:
            image_bytes = extract_chart_image(
                pdf_path, page_index, position,
                textin_page_size=textin_page_size,
            )
            page_text = _gather_page_text(detail, page_id)
            summary = await summarize_chart(image_bytes, settings, page_text=page_text)
            enhanced = replace_chart_table(enhanced, text, summary)
            chart_count += 1
            logger.info(
                "Enhanced chart on page %d: %s",
                page_id,
                summary[:80] + "..." if len(summary) > 80 else summary,
            )
        except Exception as exc:
            logger.warning(
                "Failed to enhance chart on page %d: %s",
                page_id,
                exc,
            )

    enhanced = strip_watermarks(enhanced)
    return enhanced, chart_count
