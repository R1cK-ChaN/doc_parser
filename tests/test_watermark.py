"""Tests for doc_parser.watermark — watermark stripping utilities."""

from __future__ import annotations

from doc_parser.watermark import strip_watermark_lines


class TestStripWatermarkLines:
    def test_removes_macroamy_lines(self):
        md = "# Title\n## macroamy一手整理，付费加v入群\nReal content\n专业的宏观和行业汇总内容 微信macroamy整理"
        result = strip_watermark_lines(md)
        assert "macroamy" not in result
        assert "# Title" in result
        assert "Real content" in result

    def test_removes_html_comment_watermark(self):
        md = "Line 1\n<!-- macroamy一手整理，付费加v入群 -->\nLine 2"
        result = strip_watermark_lines(md)
        assert "macroamy" not in result
        assert "Line 1" in result
        assert "Line 2" in result

    def test_preserves_clean_markdown(self):
        md = "# Report\n\nSome analysis\n\nConclusion"
        result = strip_watermark_lines(md)
        assert result == md
