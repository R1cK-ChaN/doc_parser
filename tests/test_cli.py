"""Tests for doc_parser.cli — Click CLI commands."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from click.testing import CliRunner

from doc_parser.cli import _human_size, cli


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


# ---------------------------------------------------------------------------
# _human_size
# ---------------------------------------------------------------------------

def test_human_size_bytes():
    assert _human_size(500) == "500.0 B"


def test_human_size_kb():
    assert _human_size(2048) == "2.0 KB"


def test_human_size_mb():
    assert _human_size(5 * 1024 * 1024) == "5.0 MB"


def test_human_size_zero():
    assert _human_size(0) == "0.0 B"


# ---------------------------------------------------------------------------
# --help
# ---------------------------------------------------------------------------

def test_help_output(runner: CliRunner):
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "doc-parser" in result.output


def test_help_shows_commands(runner: CliRunner):
    """Help output includes expected commands."""
    result = runner.invoke(cli, ["--help"])
    assert "parse-local" in result.output
    assert "re-extract" in result.output
    assert "status" in result.output


def test_help_no_removed_commands(runner: CliRunner):
    """Help output does not include removed commands."""
    result = runner.invoke(cli, ["--help"])
    assert "parse-file" not in result.output
    assert "parse-folder" not in result.output
    assert "list-files" not in result.output
    assert "init-db" not in result.output
    assert "run-all" not in result.output
    assert "extract-folder" not in result.output
    assert "enhance-charts" not in result.output


# ---------------------------------------------------------------------------
# parse-local
# ---------------------------------------------------------------------------

def test_parse_local_success(runner: CliRunner, tmp_path):
    """parse-local prints success when a sha is returned."""
    pdf = tmp_path / "test.pdf"
    pdf.write_bytes(b"%PDF test")

    sha = "a" * 64

    with (
        patch("doc_parser.cli.get_settings") as mock_gs,
        patch("doc_parser.pipeline.process_local", new_callable=AsyncMock, return_value=sha),
    ):
        mock_settings = MagicMock()
        mock_gs.return_value = mock_settings

        result = runner.invoke(cli, ["parse-local", str(pdf)])
        assert result.exit_code == 0
        assert "Done" in result.output


def test_parse_local_skipped(runner: CliRunner, tmp_path):
    """parse-local prints skipped message when None is returned."""
    pdf = tmp_path / "test.pdf"
    pdf.write_bytes(b"%PDF test")

    with (
        patch("doc_parser.cli.get_settings") as mock_gs,
        patch("doc_parser.pipeline.process_local", new_callable=AsyncMock, return_value=None),
    ):
        mock_settings = MagicMock()
        mock_gs.return_value = mock_settings

        result = runner.invoke(cli, ["parse-local", str(pdf)])
        assert result.exit_code == 0
        assert "Skipped" in result.output


# ---------------------------------------------------------------------------
# re-extract
# ---------------------------------------------------------------------------

def test_re_extract_success(runner: CliRunner, tmp_path):
    """re-extract prints success with extracted fields."""
    with (
        patch("doc_parser.cli.get_settings") as mock_gs,
        patch("doc_parser.storage.resolve_sha_prefix", return_value="a" * 64),
        patch("doc_parser.pipeline.re_extract", new_callable=AsyncMock, return_value={
            "title": "New Title", "broker": "New Broker",
        }),
    ):
        mock_settings = MagicMock()
        mock_gs.return_value = mock_settings

        result = runner.invoke(cli, ["re-extract", "aaaa"])
        assert result.exit_code == 0
        assert "Re-extracted" in result.output


def test_re_extract_bad_prefix(runner: CliRunner):
    """re-extract prints error for unresolvable prefix."""
    with (
        patch("doc_parser.cli.get_settings") as mock_gs,
        patch("doc_parser.storage.resolve_sha_prefix", side_effect=ValueError("No results found for prefix 'zzz'")),
    ):
        mock_settings = MagicMock()
        mock_gs.return_value = mock_settings

        result = runner.invoke(cli, ["re-extract", "zzz"])
        assert result.exit_code == 1
        assert "No results found" in result.output


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------

def test_status_empty(runner: CliRunner):
    """status shows no results message when directory is empty."""
    with (
        patch("doc_parser.cli.get_settings") as mock_gs,
        patch("doc_parser.storage.list_results", return_value=[]),
    ):
        mock_settings = MagicMock()
        mock_gs.return_value = mock_settings

        result = runner.invoke(cli, ["status"])
        assert result.exit_code == 0
        assert "No results" in result.output


def test_status_with_results(runner: CliRunner):
    """status shows counts when results exist."""
    results = [
        {"sha256": "a" * 64, "source": "local", "broker": "GS"},
        {"sha256": "b" * 64, "source": "drive", "broker": "GS"},
        {"sha256": "c" * 64, "source": "local", "broker": "MS"},
    ]

    with (
        patch("doc_parser.cli.get_settings") as mock_gs,
        patch("doc_parser.storage.list_results", return_value=results),
    ):
        mock_settings = MagicMock()
        mock_gs.return_value = mock_settings

        result = runner.invoke(cli, ["status"])
        assert result.exit_code == 0
        assert "Results: 3" in result.output
