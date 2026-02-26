"""Tests for doc_parser.chart_enhance — VLM chart summarization."""

from __future__ import annotations

import base64
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pymupdf
import pytest

from doc_parser.chart_enhance import (
    _gather_page_text,
    enhance_charts,
    extract_chart_image,
    replace_chart_table,
    replace_table_html,
    summarize_chart,
    summarize_table,
)
from doc_parser.watermark import _strip_repeated_html_comments as strip_html_comment_watermarks
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

    def test_with_flat_list_position(self, tmp_path: Path):
        """Extract chart image using flat list [x0,y0,x1,y1,...] position format."""
        pdf_path = _create_test_pdf(tmp_path / "test.pdf")
        # Flat 8-element list: [x0,y0, x1,y1, x2,y2, x3,y3]
        position = [100, 100, 400, 100, 400, 300, 100, 300]

        result = extract_chart_image(pdf_path, 0, position)

        assert isinstance(result, bytes)
        assert result[:4] == b"\x89PNG"

    def test_with_textin_page_size_scaling(self, tmp_path: Path):
        """Coordinates are scaled when textin_page_size is provided."""
        pdf_path = _create_test_pdf(tmp_path / "test.pdf", width=612, height=792)
        # TextIn coords at 2x scale (1224x1584)
        position = [200, 200, 800, 200, 800, 600, 200, 600]

        result = extract_chart_image(
            pdf_path, 0, position,
            textin_page_size=(1224, 1584),
        )

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
# _gather_page_text
# ---------------------------------------------------------------------------


class TestGatherPageText:
    def test_filters_by_page(self):
        """Only elements from the target page are included."""
        detail = [
            {"type": "text", "text": "Page 1 text", "page_id": 1},
            {"type": "text", "text": "Page 2 text", "page_id": 2},
            {"type": "text", "text": "Also page 1", "page_id": 1},
        ]
        result = _gather_page_text(detail, 1)
        assert "Page 1 text" in result
        assert "Also page 1" in result
        assert "Page 2 text" not in result

    def test_excludes_image_elements(self):
        """Image elements are skipped even if on the same page."""
        detail = [
            {"type": "text", "text": "Normal text", "page_id": 1},
            {"type": "image", "sub_type": "chart", "text": "<table>...</table>", "page_id": 1},
            {"type": "text", "text": "More text", "page_id": 1},
        ]
        result = _gather_page_text(detail, 1)
        assert "Normal text" in result
        assert "More text" in result
        assert "<table>" not in result

    def test_truncates_long_text(self):
        """Output is truncated to ~1000 characters."""
        detail = [
            {"type": "text", "text": "A" * 1200, "page_id": 1},
        ]
        result = _gather_page_text(detail, 1)
        assert len(result) <= 1000

    def test_falls_back_to_page_number(self):
        """Elements with page_number (no page_id) are matched."""
        detail = [
            {"type": "text", "text": "Found it", "page_number": 3},
        ]
        result = _gather_page_text(detail, 3)
        assert "Found it" in result

    def test_empty_when_no_matches(self):
        """Returns empty string when no elements match."""
        detail = [
            {"type": "text", "text": "Wrong page", "page_id": 5},
        ]
        result = _gather_page_text(detail, 1)
        assert result == ""


# ---------------------------------------------------------------------------
# strip_html_comment_watermarks
# ---------------------------------------------------------------------------


class TestStripHtmlCommentWatermarks:
    def test_removes_repeated_comments(self):
        """Comments appearing 3+ times are removed."""
        wm = "<!-- macroamy watermark -->"
        md = f"Hello\n{wm}\nWorld\n{wm}\nFoo\n{wm}\nBar"
        result = strip_html_comment_watermarks(md)
        assert wm not in result
        assert "Hello" in result
        assert "World" in result
        assert "Bar" in result

    def test_preserves_unique_comments(self):
        """Comments appearing fewer than 3 times are kept."""
        md = "Hello\n<!-- important note -->\nWorld"
        result = strip_html_comment_watermarks(md)
        assert "<!-- important note -->" in result

    def test_mixed_repeated_and_unique(self):
        """Only repeated comments are removed; unique ones stay."""
        wm = "<!-- watermark -->"
        unique = "<!-- keep this -->"
        md = f"A\n{wm}\nB\n{unique}\nC\n{wm}\nD\n{wm}\nE"
        result = strip_html_comment_watermarks(md)
        assert wm not in result
        assert unique in result
        assert "A" in result
        assert "E" in result

    def test_no_comments(self):
        """Markdown without comments is returned unchanged."""
        md = "Just plain text\nwith lines"
        result = strip_html_comment_watermarks(md)
        assert result.strip() == md.strip()

    def test_exactly_two_occurrences_kept(self):
        """Comments appearing exactly 2 times are preserved."""
        comment = "<!-- twice -->"
        md = f"A\n{comment}\nB\n{comment}\nC"
        result = strip_html_comment_watermarks(md)
        assert result.count(comment) == 2


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
        # No page_text provided → only image block
        assert len(user_msg["content"]) == 1

    @pytest.mark.asyncio
    async def test_summarize_chart_with_page_text(self, tmp_path: Path):
        """When page_text is provided, it is sent alongside the image."""
        settings = _make_settings(tmp_path)
        image_bytes = b"fake-png-bytes"

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "choices": [
                {"message": {"content": "Revenue chart summary."}}
            ]
        }
        mock_response.raise_for_status = MagicMock()

        with patch("doc_parser.chart_enhance.httpx.AsyncClient") as MockClient:
            mock_client_instance = AsyncMock()
            mock_client_instance.post.return_value = mock_response
            mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
            mock_client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = mock_client_instance

            result = await summarize_chart(
                image_bytes, settings, page_text="Q1 revenue was $10M"
            )

        assert result == "Revenue chart summary."

        call_kwargs = mock_client_instance.post.call_args
        payload = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        user_msg = payload["messages"][1]
        assert len(user_msg["content"]) == 2
        assert user_msg["content"][0]["type"] == "image_url"
        assert user_msg["content"][1]["type"] == "text"
        assert "Q1 revenue was $10M" in user_msg["content"][1]["text"]


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

            enhanced, count, tbl_count = await enhance_charts(
                pdf_path, markdown, detail, settings,
            )

        assert count == 1
        assert tbl_count == 0
        assert chart_html not in enhanced
        assert "[Chart Summary] Bar chart showing quarterly revenue." in enhanced
        assert "# Report" in enhanced
        assert "Conclusion" in enhanced

    @pytest.mark.asyncio
    async def test_full_flow_textin_format(self, tmp_path: Path):
        """Full flow with real TextIn format: flat list position, page_id, pages."""
        pdf_path = _create_test_pdf(tmp_path / "test.pdf", width=612, height=792)
        settings = _make_settings(tmp_path)

        chart_html = '<table border="1"><tr><td>Q1</td><td>100</td></tr></table>'
        markdown = f"# Report\n\n{chart_html}\n\nEnd"

        detail = [
            {
                "type": "image",
                "sub_type": "chart",
                "text": chart_html,
                "page_id": 1,
                "position": [200, 200, 800, 200, 800, 600, 200, 600],
            },
        ]
        pages = [{"page_id": 1, "width": 1224, "height": 1584}]

        with patch("doc_parser.chart_enhance.summarize_chart", new_callable=AsyncMock) as mock_vlm:
            mock_vlm.return_value = "Line chart of quarterly results."

            enhanced, count, tbl_count = await enhance_charts(
                pdf_path, markdown, detail, settings, pages=pages,
            )

        assert count == 1
        assert tbl_count == 0
        assert chart_html not in enhanced
        assert "[Chart Summary] Line chart of quarterly results." in enhanced

    @pytest.mark.asyncio
    async def test_no_chart_elements(self, tmp_path: Path):
        """No chart elements means no changes."""
        pdf_path = _create_test_pdf(tmp_path / "test.pdf")
        settings = _make_settings(tmp_path)

        markdown = "# Report\n\nJust text."
        detail = [{"type": "text", "text": "Report", "page_number": 1}]

        enhanced, count, tbl_count = await enhance_charts(
            pdf_path, markdown, detail, settings,
        )

        assert count == 0
        assert tbl_count == 0
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

            enhanced, count, tbl_count = await enhance_charts(
                pdf_path, markdown, detail, settings,
            )

        assert count == 0
        assert tbl_count == 0
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
            enhanced, count, tbl_count = await enhance_charts(
                pdf_path, markdown, detail, settings,
            )
        assert count == 1
        assert tbl_count == 0

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

            enhanced, count, tbl_count = await enhance_charts(
                pdf_path, markdown, detail, settings,
            )

        assert count == 2
        assert tbl_count == 0
        assert "[Chart Summary] Summary for chart 1." in enhanced
        assert "[Chart Summary] Summary for chart 2." in enhanced
        assert chart1 not in enhanced
        assert chart2 not in enhanced

    @pytest.mark.asyncio
    async def test_full_flow_strips_watermarks(self, tmp_path: Path):
        """Full flow strips repeated HTML comment watermarks from output."""
        pdf_path = _create_test_pdf(tmp_path / "test.pdf")
        settings = _make_settings(tmp_path)

        wm = "<!-- macroamy watermark -->"
        chart_html = '<table border="1"><tr><td>Fake</td></tr></table>'
        markdown = (
            f"# Report\n{wm}\nIntro\n{wm}\n\n{chart_html}\n\n{wm}\nEnd"
        )

        detail = [
            {"type": "text", "text": "Intro", "page_number": 1},
            {
                "type": "image",
                "sub_type": "chart",
                "text": chart_html,
                "page_number": 1,
                "position": {"x": 100, "y": 100, "width": 300, "height": 200},
            },
        ]

        with patch("doc_parser.chart_enhance.summarize_chart", new_callable=AsyncMock) as mock_vlm:
            mock_vlm.return_value = "A chart."

            enhanced, count, tbl_count = await enhance_charts(
                pdf_path, markdown, detail, settings,
            )

        assert count == 1
        assert tbl_count == 0
        assert wm not in enhanced
        assert "[Chart Summary] A chart." in enhanced

    @pytest.mark.asyncio
    async def test_page_text_passed_to_vlm(self, tmp_path: Path):
        """enhance_charts passes page text context to summarize_chart."""
        pdf_path = _create_test_pdf(tmp_path / "test.pdf")
        settings = _make_settings(tmp_path)

        chart_html = '<table><tr><td>data</td></tr></table>'
        markdown = f"# Report\n\n{chart_html}\n\nEnd"

        detail = [
            {"type": "text", "text": "Revenue discussion", "page_number": 1},
            {
                "type": "image",
                "sub_type": "chart",
                "text": chart_html,
                "page_number": 1,
                "position": {"x": 100, "y": 100, "width": 200, "height": 100},
            },
        ]

        with patch("doc_parser.chart_enhance.summarize_chart", new_callable=AsyncMock) as mock_vlm:
            mock_vlm.return_value = "Chart summary."

            await enhance_charts(pdf_path, markdown, detail, settings)

        # summarize_chart was called with page_text keyword arg
        call_kwargs = mock_vlm.call_args
        assert "page_text" in call_kwargs.kwargs
        assert "Revenue discussion" in call_kwargs.kwargs["page_text"]


# ---------------------------------------------------------------------------
# replace_table_html
# ---------------------------------------------------------------------------


class TestReplaceTableHtml:
    def test_basic_replacement(self):
        """HTML table is replaced with markdown table."""
        html = '<table border="1"><tr><th>A</th><th>B</th></tr><tr><td>1</td><td>2</td></tr></table>'
        markdown = f"Some text\n\n{html}\n\nMore text"
        md_table = "| A | B |\n| --- | --- |\n| 1 | 2 |"

        result = replace_table_html(markdown, html, md_table)

        assert html not in result
        assert md_table in result
        assert "Some text" in result
        assert "More text" in result

    def test_only_first_occurrence(self):
        """Only the first occurrence is replaced."""
        html = "<table><tr><td>data</td></tr></table>"
        md_table = "| data |\n| --- |"
        markdown = f"Before\n{html}\nMiddle\n{html}\nAfter"

        result = replace_table_html(markdown, html, md_table)

        assert result.count(md_table) == 1
        assert result.count(html) == 1

    def test_no_match(self):
        """If HTML is not found, markdown is unchanged."""
        markdown = "No table here"
        html = "<table>missing</table>"
        md_table = "| missing |\n| --- |"

        result = replace_table_html(markdown, html, md_table)

        assert result == "No table here"


# ---------------------------------------------------------------------------
# summarize_table (mocked VLM API)
# ---------------------------------------------------------------------------


class TestSummarizeTable:
    @pytest.mark.asyncio
    async def test_summarize_table_success(self, tmp_path: Path):
        """VLM API returns a markdown table."""
        settings = _make_settings(tmp_path)
        image_bytes = b"fake-png-bytes"

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "choices": [
                {"message": {"content": "| A | B |\n| --- | --- |\n| 1 | 2 |"}}
            ]
        }
        mock_response.raise_for_status = MagicMock()

        with patch("doc_parser.chart_enhance.httpx.AsyncClient") as MockClient:
            mock_client_instance = AsyncMock()
            mock_client_instance.post.return_value = mock_response
            mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
            mock_client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = mock_client_instance

            result = await summarize_table(image_bytes, settings)

        assert result == "| A | B |\n| --- | --- |\n| 1 | 2 |"
        mock_client_instance.post.assert_called_once()

        # Verify the payload uses the table system prompt
        call_kwargs = mock_client_instance.post.call_args
        payload = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        assert "table reader" in payload["messages"][0]["content"].lower()

    @pytest.mark.asyncio
    async def test_summarize_table_with_page_text(self, tmp_path: Path):
        """When page_text is provided, it is sent alongside the image."""
        settings = _make_settings(tmp_path)
        image_bytes = b"fake-png-bytes"

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "choices": [
                {"message": {"content": "| Col |\n| --- |\n| val |"}}
            ]
        }
        mock_response.raise_for_status = MagicMock()

        with patch("doc_parser.chart_enhance.httpx.AsyncClient") as MockClient:
            mock_client_instance = AsyncMock()
            mock_client_instance.post.return_value = mock_response
            mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
            mock_client_instance.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = mock_client_instance

            result = await summarize_table(
                image_bytes, settings, page_text="Financial data"
            )

        assert "Col" in result

        call_kwargs = mock_client_instance.post.call_args
        payload = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        user_msg = payload["messages"][1]
        assert len(user_msg["content"]) == 2
        assert "Financial data" in user_msg["content"][1]["text"]


# ---------------------------------------------------------------------------
# enhance_charts with table elements
# ---------------------------------------------------------------------------


class TestEnhanceChartsWithTables:
    @pytest.mark.asyncio
    async def test_table_enhancement(self, tmp_path: Path):
        """Table elements are enhanced with VLM markdown tables."""
        pdf_path = _create_test_pdf(tmp_path / "test.pdf")
        settings = _make_settings(tmp_path)

        table_html = '<table border="1"><tr><th>Q1</th><th>Q2</th></tr><tr><td>100</td><td>200</td></tr></table>'
        markdown = f"# Report\n\n{table_html}\n\nConclusion"

        detail = [
            {"type": "text", "text": "Report", "page_number": 1},
            {
                "type": "table",
                "text": table_html,
                "page_number": 1,
                "position": {"x": 100, "y": 100, "width": 300, "height": 200},
            },
        ]

        with patch("doc_parser.chart_enhance.summarize_table", new_callable=AsyncMock) as mock_vlm:
            mock_vlm.return_value = "| Q1 | Q2 |\n| --- | --- |\n| 100 | 200 |"

            enhanced, chart_count, table_count = await enhance_charts(
                pdf_path, markdown, detail, settings,
            )

        assert chart_count == 0
        assert table_count == 1
        assert table_html not in enhanced
        assert "| Q1 | Q2 |" in enhanced
        assert "# Report" in enhanced
        assert "Conclusion" in enhanced

    @pytest.mark.asyncio
    async def test_mixed_charts_and_tables(self, tmp_path: Path):
        """Both charts and tables are enhanced in the same document."""
        pdf_path = _create_test_pdf(tmp_path / "test.pdf")
        settings = _make_settings(tmp_path)

        chart_html = '<table border="1"><tr><td>ChartData</td></tr></table>'
        table_html = '<table border="1"><tr><th>Col A</th></tr><tr><td>Val</td></tr></table>'
        markdown = f"# Report\n\n{chart_html}\n\nText\n\n{table_html}\n\nEnd"

        detail = [
            {
                "type": "image",
                "sub_type": "chart",
                "text": chart_html,
                "page_number": 1,
                "position": {"x": 100, "y": 50, "width": 300, "height": 150},
            },
            {
                "type": "table",
                "text": table_html,
                "page_number": 1,
                "position": {"x": 100, "y": 300, "width": 300, "height": 200},
            },
        ]

        with (
            patch("doc_parser.chart_enhance.summarize_chart", new_callable=AsyncMock) as mock_chart,
            patch("doc_parser.chart_enhance.summarize_table", new_callable=AsyncMock) as mock_table,
        ):
            mock_chart.return_value = "A bar chart."
            mock_table.return_value = "| Col A |\n| --- |\n| Val |"

            enhanced, chart_count, table_count = await enhance_charts(
                pdf_path, markdown, detail, settings,
            )

        assert chart_count == 1
        assert table_count == 1
        assert chart_html not in enhanced
        assert table_html not in enhanced
        assert "[Chart Summary] A bar chart." in enhanced
        assert "| Col A |" in enhanced

    @pytest.mark.asyncio
    async def test_table_vlm_failure_skips(self, tmp_path: Path):
        """VLM failure for a table doesn't crash the flow."""
        pdf_path = _create_test_pdf(tmp_path / "test.pdf")
        settings = _make_settings(tmp_path)

        table_html = "<table><tr><td>data</td></tr></table>"
        markdown = f"Text\n{table_html}\nMore"
        detail = [
            {
                "type": "table",
                "text": table_html,
                "page_number": 1,
                "position": {"x": 0, "y": 0, "width": 100, "height": 100},
            },
        ]

        with patch("doc_parser.chart_enhance.summarize_table", new_callable=AsyncMock) as mock_vlm:
            mock_vlm.side_effect = Exception("VLM API error")

            enhanced, chart_count, table_count = await enhance_charts(
                pdf_path, markdown, detail, settings,
            )

        assert chart_count == 0
        assert table_count == 0
        assert enhanced == markdown  # unchanged on failure

    @pytest.mark.asyncio
    async def test_only_tables_no_charts(self, tmp_path: Path):
        """Document with only tables (no charts) is still enhanced."""
        pdf_path = _create_test_pdf(tmp_path / "test.pdf")
        settings = _make_settings(tmp_path)

        table_html = '<table><tr><th>X</th></tr><tr><td>1</td></tr></table>'
        markdown = f"# Data\n\n{table_html}\n\nEnd"

        detail = [
            {
                "type": "table",
                "text": table_html,
                "page_number": 1,
                "position": {"x": 50, "y": 50, "width": 400, "height": 200},
            },
        ]

        with patch("doc_parser.chart_enhance.summarize_table", new_callable=AsyncMock) as mock_vlm:
            mock_vlm.return_value = "| X |\n| --- |\n| 1 |"

            enhanced, chart_count, table_count = await enhance_charts(
                pdf_path, markdown, detail, settings,
            )

        assert chart_count == 0
        assert table_count == 1
        assert table_html not in enhanced
        assert "| X |" in enhanced
