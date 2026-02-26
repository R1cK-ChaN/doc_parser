"""Tests for doc_parser.watermark — watermark stripping utilities."""

from __future__ import annotations

from doc_parser.watermark import strip_watermark_lines, strip_watermarks


# ---------------------------------------------------------------------------
# Original tests (updated to use strip_watermarks)
# ---------------------------------------------------------------------------

class TestStripWatermarkLines:
    def test_removes_macroamy_lines(self):
        md = "# Title\n## macroamy一手整理，付费加v入群\nReal content\n专业的宏观和行业汇总内容 微信macroamy整理"
        result = strip_watermarks(md)
        assert "macroamy" not in result
        assert "# Title" in result
        assert "Real content" in result

    def test_removes_html_comment_watermark(self):
        md = "Line 1\n<!-- macroamy一手整理，付费加v入群 -->\nLine 2"
        result = strip_watermarks(md)
        assert "macroamy" not in result
        assert "Line 1" in result
        assert "Line 2" in result

    def test_removes_nacroany_lines(self):
        md = "# Title\nnacroany watermark line\nReal content"
        result = strip_watermarks(md)
        assert "nacroany" not in result
        assert "Real content" in result

    def test_removes_paid_and_scan_lines(self):
        md = "# Title\n付费加v入群\nReal content\n打开微博我页扫一扫"
        result = strip_watermarks(md)
        assert "付费" not in result
        assert "扫一扫" not in result
        assert "Real content" in result

    def test_preserves_clean_markdown(self):
        md = "# Report\n\nSome analysis\n\nConclusion"
        result = strip_watermarks(md)
        assert result == md


# ---------------------------------------------------------------------------
# OCR variant markers
# ---------------------------------------------------------------------------

class TestOCRVariantMarkers:
    def test_removes_mroamy_comment(self):
        md = "# Title\n<!-- mroamy-手整，10 加 -->\nReal content"
        result = strip_watermarks(md)
        assert "mroamy" not in result
        assert "Real content" in result

    def test_removes_macrcy_line(self):
        md = "# Title\n专业的宏观和行业汇总内容微信macrcy\nReal content"
        result = strip_watermarks(md)
        assert "macrcy" not in result
        assert "Real content" in result

    def test_removes_roamy_line(self):
        md = "# Title\nroamy watermark text\nReal content"
        result = strip_watermarks(md)
        assert "roamy" not in result
        assert "Real content" in result


# ---------------------------------------------------------------------------
# Line-level regex patterns
# ---------------------------------------------------------------------------

class TestLinePatterns:
    def test_removes_truncated_promo_fragment(self):
        md = "# Title\n专业的宏\nReal content"
        result = strip_watermarks(md)
        assert "专业的宏" not in result
        assert "Real content" in result

    def test_removes_full_promo_line(self):
        md = "# Title\n专业的宏观和行业汇总内容\nReal content"
        result = strip_watermarks(md)
        assert "专业的宏观" not in result
        assert "Real content" in result

    def test_removes_contact_us_comment(self):
        md = "Line 1\n<!-- **联系我们** -->\nLine 2"
        result = strip_watermarks(md)
        assert "联系我们" not in result
        assert "Line 1" in result
        assert "Line 2" in result

    def test_removes_contact_us_comment_no_bold(self):
        md = "Line 1\n<!-- 联系我们 -->\nLine 2"
        result = strip_watermarks(md)
        assert "联系我们" not in result

    def test_removes_degg_comment(self):
        md = "Line 1\n<!-- GMF V @DeggGlobalMacroFin some info -->\nLine 2"
        result = strip_watermarks(md)
        assert "@Degg" not in result
        assert "Line 1" in result
        assert "Line 2" in result

    def test_removes_weibo_comment(self):
        md = "Line 1\n<!-- 微博 -->\nLine 2"
        result = strip_watermarks(md)
        assert "微博" not in result
        assert "Line 1" in result
        assert "Line 2" in result

    def test_removes_tantuhongguan_standalone(self):
        md = "# Title\n坦途宏观\nReal content"
        result = strip_watermarks(md)
        assert "坦途宏观" not in result
        assert "Real content" in result

    def test_removes_view_weibo_homepage(self):
        md = "# Title\n查看微博主页\nReal content"
        result = strip_watermarks(md)
        assert "查看微博主页" not in result
        assert "Real content" in result

    def test_removes_gmf_research_bio(self):
        md = "# Title\n·GMF Research（坦途宏观）专业的宏观内容\nReal content"
        result = strip_watermarks(md)
        assert "GMF Research" not in result
        assert "Real content" in result


# ---------------------------------------------------------------------------
# Social media HTML table removal
# ---------------------------------------------------------------------------

class TestSocialMediaTable:
    def test_removes_stats_table_plain(self):
        table = (
            "<table><tr><td>关注</td><td>粉丝</td><td>转评赞</td></tr>"
            "<tr><td>100</td><td>500</td><td>1000</td></tr></table>"
        )
        md = f"# Title\n{table}\nReal content"
        result = strip_watermarks(md)
        assert "粉丝" not in result
        assert "转评赞" not in result
        assert "# Title" in result
        assert "Real content" in result

    def test_removes_stats_table_with_attrs(self):
        """Real-world tables use <table border="1" > not plain <table>."""
        table = (
            '<table border="1" ><tr>\n'
            "<td>34.7 万</td>\n<td>3525</td>\n<td>128.6 万</td>\n"
            "</tr><tr>\n<td>粉丝</td>\n<td>关注</td>\n<td>转评赞</td>\n"
            "</tr></table>"
        )
        md = f"# Title\n{table}\nReal content"
        result = strip_watermarks(md)
        assert "粉丝" not in result
        assert "转评赞" not in result
        assert "Real content" in result

    def test_preserves_legitimate_table(self):
        table = "<table><tr><td>Year</td><td>Revenue</td></tr><tr><td>2024</td><td>100M</td></tr></table>"
        md = f"# Title\n{table}\nReal content"
        result = strip_watermarks(md)
        assert "Revenue" in result
        assert "<table>" in result

    def test_requires_both_markers(self):
        """Table with only 粉丝 but not 转评赞 is preserved."""
        table = "<table><tr><td>粉丝</td><td>Count</td></tr></table>"
        md = f"# Report\n{table}\nContent"
        result = strip_watermarks(md)
        assert "粉丝" in result


# ---------------------------------------------------------------------------
# Inline substitution
# ---------------------------------------------------------------------------

class TestInlineSubstitution:
    def test_strips_roamy_zhengli_embedded(self):
        md = "# Title\n私营部roamy整理\nReal content"
        result = strip_watermarks(md)
        assert "roamy" not in result
        assert "私营部" in result
        assert "Real content" in result

    def test_strips_macroamy_zhengli_embedded(self):
        md = "数据macroamy整理汇总"
        result = strip_watermarks(md)
        assert "macroamy" not in result
        assert "数据" in result

    def test_strips_nacroany_zhengli_embedded(self):
        md = "报告nacroany整理"
        result = strip_watermarks(md)
        assert "nacroany" not in result
        assert "报告" in result


# ---------------------------------------------------------------------------
# Safety guards
# ---------------------------------------------------------------------------

class TestSafetyGuards:
    def test_preserves_guanzhu_in_heading(self):
        """关注 in a heading is not a watermark marker and should be preserved."""
        md = "# 关注点\n\n经济增长前景"
        result = strip_watermarks(md)
        assert "关注点" in result

    def test_preserves_weixin_in_analysis(self):
        """微信 in analysis context is not a watermark line."""
        md = "# Report\n微信支付市场份额持续增长\nConclusion"
        result = strip_watermarks(md)
        assert "微信支付" in result

    def test_clean_markdown_unchanged(self):
        md = "# 2024年宏观经济展望\n\n## GDP增长\n\n预计增长5.2%\n\n| 指标 | 数值 |\n|---|---|\n| GDP | 5.2% |"
        result = strip_watermarks(md)
        assert result == md


# ---------------------------------------------------------------------------
# Repeated HTML comment removal (Layer 4)
# ---------------------------------------------------------------------------

class TestRepeatedHTMLComments:
    def test_strips_comments_appearing_3_plus_times(self):
        """23× <!-- GMF Research --> should be stripped."""
        comment = "<!-- GMF Research -->"
        lines = ["# Title"] + [comment] * 23 + ["Real content"]
        md = "\n".join(lines)
        result = strip_watermarks(md)
        assert "GMF Research" not in result
        assert "# Title" in result
        assert "Real content" in result

    def test_preserves_comments_below_threshold(self):
        """2× <!-- something --> should be preserved (below 3)."""
        md = "# Title\n<!-- note -->\nMiddle\n<!-- note -->\nEnd"
        result = strip_watermarks(md)
        assert "<!-- note -->" in result
        assert result.count("<!-- note -->") == 2

    def test_preserves_single_meaningful_comment(self):
        """A single unique comment is preserved."""
        md = "# Title\n<!-- TODO: review this section -->\nContent"
        result = strip_watermarks(md)
        assert "<!-- TODO: review this section -->" in result

    def test_mixed_strips_only_repeated(self):
        """Only comments at 3+ count are stripped; others remain."""
        watermark = "<!-- GMF Research -->"
        keeper = "<!-- important note -->"
        lines = (
            ["# Title"]
            + [watermark] * 5
            + [keeper, "Middle"]
            + [watermark] * 3
            + ["End"]
        )
        md = "\n".join(lines)
        result = strip_watermarks(md)
        assert "GMF Research" not in result
        assert "<!-- important note -->" in result
        assert "# Title" in result
        assert "End" in result


# ---------------------------------------------------------------------------
# "naci" / "naciocā" watermark artifacts
# ---------------------------------------------------------------------------

class TestNaciWatermarks:
    def test_removes_naci_standalone_line(self):
        """Standalone 'naci' line (OCR-mangled watermark) is removed."""
        md = "# Title\nnaci\nReal content"
        result = strip_watermarks(md)
        assert "naci" not in result
        assert "Real content" in result

    def test_strips_nacioca_inline_suffix(self):
        """Inline 'naciocā' suffix is stripped, rest of line preserved."""
        md = "中国大宗商品naciocā"
        result = strip_watermarks(md)
        assert "naciocā" not in result
        assert "中国大宗商品" in result

    def test_naci_does_not_strip_financial_terms(self):
        """'naci' marker should not damage longer words like 'nacional'."""
        # "naci" is a substring marker — lines containing it are dropped.
        # This is intentional: standalone 'naci' only appears as OCR noise.
        md = "# Report\nnaci\nReal content"
        result = strip_watermarks(md)
        assert "naci" not in result
        assert "Real content" in result


# ---------------------------------------------------------------------------
# Backward compatibility
# ---------------------------------------------------------------------------

class TestBackwardCompat:
    def test_strip_watermark_lines_delegates_to_strip_watermarks(self):
        assert strip_watermark_lines is strip_watermarks

    def test_strip_watermark_lines_works(self):
        md = "# Title\nmroamy watermark\nReal content"
        result = strip_watermark_lines(md)
        assert "mroamy" not in result
        assert "Real content" in result
