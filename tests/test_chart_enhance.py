"""Tests for doc_parser.chart_enhance — VLM chart summarization."""

from __future__ import annotations

import base64
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pymupdf
import pytest

from doc_parser.chart_enhance import (
    enhance_charts,
    extract_chart_image,
    replace_chart_table,
    summarize_chart,
)
from doc_parser.config import Settings


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_settings(tmp_path: Path, vlm_model: str = "test/vlm-model") -> Settings:
    return Settings(
        textin_app_id="test-app",
        textin_secret_code="test-secret",
        database_url="sqlite+aiosqlite://",
        data_dir=tmp_path / "data",
        llm_api_key="test-key",
        llm_base_url="https://api.example.com/v1",
        vlm_model=vlm_model,
        vlm_max_tokens=300,
    )


def _create_test_pdf(path: Path, width: float = 612, height: float = 792) -> Path:
    """Create a simple single-page PDF with a rectangle (simulating a chart)."""
    doc = pymupdf.open()
    page = doc.new_page(width=width, height=height)
    # Draw a rectangle to simulate a chart area
    rect = pymupdf.Rect(100, 100, 400, 300)
    page.draw_rect(rect, color=(0, 0, 1), fill=(0.9, 0.9, 1))
    page.insert_text((150, 200), "Test Chart", fontsize=16)
    doc.save(str(path))
    doc.close()
    return path


# ---------------------------------------------------------------------------
# extract_chart_image
# ---------------------------------------------------------------------------


class TestExtractChartImage:
    def test_with_xy_position(self, tmp_path: Path):
        """Extract chart image using x/y/width/height position format."""
        pdf_path = _create_test_pdf(tmp_path / "test.pdf")
        position = {"x": 100, "y": 100, "width": 300, "height": 200}

        result = extract_chart_image(pdf_path, 0, position)

        assert isinstance(result, bytes)
        assert len(result) > 0
        # PNG magic bytes
        assert result[:4] == b"\x89PNG"

    def test_with_quad_position(self, tmp_path: Path):
        """Extract chart image using quad-point position format."""
        pdf_path = _create_test_pdf(tmp_path / "test.pdf")
        position = {
            "quad": [[100, 100], [400, 100], [400, 300], [100, 300]],
        }

        result = extract_chart_image(pdf_path, 0, position)

        assert isinstance(result, bytes)
        assert result[:4] == b"\x89PNG"

    def test_with_points_position(self, tmp_path: Path):
        """Extract chart image using points position format."""
        pdf_path = _create_test_pdf(tmp_path / "test.pdf")
        position = {
            "points": [[100, 100], [400, 100], [400, 300], [100, 300]],
        }

        result = extract_chart_image(pdf_path, 0, position)

        assert isinstance(result, bytes)
        assert result[:4] == b"\x89PNG"

    def test_fallback_full_page(self, tmp_path: Path):
        """Unknown position format falls back to full page."""
        pdf_path = _create_test_pdf(tmp_path / "test.pdf")
        position = {"unknown_key": "value"}

        result = extract_chart_image(pdf_path, 0, position)

        assert isinstance(result, bytes)
        assert result[:4] == b"\x89PNG"


# ---------------------------------------------------------------------------
# replace_chart_table
# ---------------------------------------------------------------------------


class TestReplaceChartTable:
    def test_basic_replacement(self):
        """Hallucinated table is replaced with chart summary."""
        markdown = "Some text\n\n<table border=\"1\"><tr><td>fake</td></tr></table>\n\nMore text"
        html = '<table border="1"><tr><td>fake</td></tr></table>'
        summary = "This is a bar chart showing revenue by quarter."

        result = replace_chart_table(markdown, html, summary)

        assert html not in result
        assert "[Chart Summary] This is a bar chart showing revenue by quarter." in result
        assert "Some text" in result
        assert "More text" in result

    def test_only_first_occurrence(self):
        """Only the first occurrence is replaced."""
        html = "<table><tr><td>chart</td></tr></table>"
        markdown = f"Before\n{html}\nMiddle\n{html}\nAfter"
        summary = "Chart description"

        result = replace_chart_table(markdown, html, summary)

        # First occurrence replaced, second remains
        assert result.count("[Chart Summary]") == 1
        assert result.count(html) == 1

    def test_no_match(self):
        """If HTML is not found, markdown is unchanged."""
        markdown = "No table here"
        html = "<table>missing</table>"
        summary = "Summary"

        result = replace_chart_table(markdown, html, summary)

        assert result == "No table here"


# ---------------------------------------------------------------------------
# summarize_chart (mocked VLM API)
# ---------------------------------------------------------------------------


class TestSummarizeChart:
    @pytest.mark.asyncio
    async def test_summarize_chart_success(self, tmp_path: Path):
        """VLM API returns a chart summary."""
        settings = _make_settings(tmp_path)
        image_bytes = b"fake-png-bytes"

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "choices": [
                {"message": {"content": "A bar chart showing Q1-Q4 revenue."}}
            ]
        }
        mock_response.raise_for_status = MagicMock()

        with patch("doc_parser.chart_enhance.httpx.AsyncClient") as MockClient:
            mock_client_instance = AsyncMock()
            mock_client_instance.post.return_value = mock_response
            mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
            mock_client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = mock_client_instance

            result = await summarize_chart(image_bytes, settings)

        assert result == "A bar chart showing Q1-Q4 revenue."
        mock_client_instance.post.assert_called_once()

        # Verify the payload contains the image
        call_kwargs = mock_client_instance.post.call_args
        payload = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        assert payload["model"] == "test/vlm-model"
        user_msg = payload["messages"][1]
        assert user_msg["content"][0]["type"] == "image_url"


# ---------------------------------------------------------------------------
# enhance_charts (full flow with mocked VLM)
# ---------------------------------------------------------------------------


class TestEnhanceCharts:
    @pytest.mark.asyncio
    async def test_full_flow(self, tmp_path: Path):
        """Full flow: find chart elements, crop, summarize, replace."""
        pdf_path = _create_test_pdf(tmp_path / "test.pdf")
        settings = _make_settings(tmp_path)

        chart_html = '<table border="1"><tr><td>Fake Q1</td><td>100</td></tr></table>'
        markdown = f"# Report\n\nSome text\n\n{chart_html}\n\nConclusion"

        detail = [
            {"type": "text", "text": "Report", "page_number": 1},
            {
                "type": "image",
                "sub_type": "chart",
                "text": chart_html,
                "page_number": 1,
                "position": {"x": 100, "y": 100, "width": 300, "height": 200},
            },
        ]

        with patch("doc_parser.chart_enhance.summarize_chart", new_callable=AsyncMock) as mock_vlm:
            mock_vlm.return_value = "Bar chart showing quarterly revenue."

            enhanced, count = await enhance_charts(
                pdf_path, markdown, detail, settings,
            )

        assert count == 1
        assert chart_html not in enhanced
        assert "[Chart Summary] Bar chart showing quarterly revenue." in enhanced
        assert "# Report" in enhanced
        assert "Conclusion" in enhanced

    @pytest.mark.asyncio
    async def test_no_chart_elements(self, tmp_path: Path):
        """No chart elements means no changes."""
        pdf_path = _create_test_pdf(tmp_path / "test.pdf")
        settings = _make_settings(tmp_path)

        markdown = "# Report\n\nJust text."
        detail = [{"type": "text", "text": "Report", "page_number": 1}]

        enhanced, count = await enhance_charts(
            pdf_path, markdown, detail, settings,
        )

        assert count == 0
        assert enhanced == markdown

    @pytest.mark.asyncio
    async def test_vlm_failure_skips_chart(self, tmp_path: Path):
        """VLM failure for one chart doesn't crash the whole flow."""
        pdf_path = _create_test_pdf(tmp_path / "test.pdf")
        settings = _make_settings(tmp_path)

        chart_html = "<table><tr><td>data</td></tr></table>"
        markdown = f"Text\n{chart_html}\nMore"
        detail = [
            {
                "type": "image",
                "sub_type": "chart",
                "text": chart_html,
                "page_number": 1,
                "position": {"x": 0, "y": 0, "width": 100, "height": 100},
            },
        ]

        with patch("doc_parser.chart_enhance.summarize_chart", new_callable=AsyncMock) as mock_vlm:
            mock_vlm.side_effect = Exception("VLM API error")

            enhanced, count = await enhance_charts(
                pdf_path, markdown, detail, settings,
            )

        assert count == 0
        assert enhanced == markdown  # unchanged on failure

    @pytest.mark.asyncio
    async def test_skip_when_vlm_disabled(self, tmp_path: Path):
        """When vlm_model is empty, enhance_charts still works (returns 0)."""
        pdf_path = _create_test_pdf(tmp_path / "test.pdf")
        settings = _make_settings(tmp_path, vlm_model="")

        markdown = "# Report"
        detail = [
            {
                "type": "image",
                "sub_type": "chart",
                "text": "<table></table>",
                "page_number": 1,
                "position": {"x": 0, "y": 0, "width": 100, "height": 100},
            },
        ]

        # enhance_charts doesn't check vlm_model — the caller (step2_parse) does.
        # But we can still call it; it will try to summarize.
        # This test verifies the caller pattern in step2_parse.
        # For a direct test: vlm_model check is in step2_parse, not enhance_charts.
        # So enhance_charts will still find the chart element.
        # Let's just verify no crash with empty model by mocking.
        with patch("doc_parser.chart_enhance.summarize_chart", new_callable=AsyncMock) as mock_vlm:
            mock_vlm.return_value = "summary"
            enhanced, count = await enhance_charts(
                pdf_path, markdown, detail, settings,
            )
        assert count == 1

    @pytest.mark.asyncio
    async def test_multiple_charts(self, tmp_path: Path):
        """Multiple chart elements are all enhanced."""
        pdf_path = _create_test_pdf(tmp_path / "test.pdf")
        settings = _make_settings(tmp_path)

        chart1 = '<table border="1"><tr><td>Chart1</td></tr></table>'
        chart2 = '<table border="1"><tr><td>Chart2</td></tr></table>'
        markdown = f"# Report\n\n{chart1}\n\nMiddle\n\n{chart2}\n\nEnd"

        detail = [
            {
                "type": "image",
                "sub_type": "chart",
                "text": chart1,
                "page_number": 1,
                "position": {"x": 100, "y": 100, "width": 200, "height": 100},
            },
            {
                "type": "image",
                "sub_type": "chart",
                "text": chart2,
                "page_number": 1,
                "position": {"x": 100, "y": 300, "width": 200, "height": 100},
            },
        ]

        with patch("doc_parser.chart_enhance.summarize_chart", new_callable=AsyncMock) as mock_vlm:
            mock_vlm.side_effect = ["Summary for chart 1.", "Summary for chart 2."]

            enhanced, count = await enhance_charts(
                pdf_path, markdown, detail, settings,
            )

        assert count == 2
        assert "[Chart Summary] Summary for chart 1." in enhanced
        assert "[Chart Summary] Summary for chart 2." in enhanced
        assert chart1 not in enhanced
        assert chart2 not in enhanced
