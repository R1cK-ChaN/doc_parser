"""Tests for doc_parser.cli — Click CLI commands."""

from __future__ import annotations

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


def test_help_shows_new_commands(runner: CliRunner):
    """Help output includes step commands."""
    result = runner.invoke(cli, ["--help"])
    assert "extract" in result.output
    assert "run-all" in result.output


# ---------------------------------------------------------------------------
# init-db
# ---------------------------------------------------------------------------

def test_init_db_success(runner: CliRunner):
    """init-db prints success when alembic succeeds."""
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout="", stderr="")
        result = runner.invoke(cli, ["init-db"])
        assert result.exit_code == 0
        assert "successfully" in result.output


def test_init_db_failure(runner: CliRunner):
    """init-db prints error and exits 1 when alembic fails."""
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="migration error")
        result = runner.invoke(cli, ["init-db"])
        assert result.exit_code == 1
        assert "failed" in result.output.lower() or "error" in result.output.lower()


# ---------------------------------------------------------------------------
# parse-local
# ---------------------------------------------------------------------------

def test_parse_local_success(runner: CliRunner, tmp_path):
    """parse-local prints success when a parse ID is returned."""
    pdf = tmp_path / "test.pdf"
    pdf.write_bytes(b"%PDF test")

    with (
        patch("doc_parser.cli._init_db_engine") as mock_init,
        patch("doc_parser.pipeline.process_local_file", new_callable=AsyncMock, return_value=42),
    ):
        mock_settings = MagicMock()
        mock_init.return_value = mock_settings

        result = runner.invoke(cli, ["parse-local", str(pdf)])
        assert result.exit_code == 0
        assert "42" in result.output


def test_parse_local_skipped(runner: CliRunner, tmp_path):
    """parse-local prints skipped message when None is returned."""
    pdf = tmp_path / "test.pdf"
    pdf.write_bytes(b"%PDF test")

    with (
        patch("doc_parser.cli._init_db_engine") as mock_init,
        patch("doc_parser.pipeline.process_local_file", new_callable=AsyncMock, return_value=None),
    ):
        mock_settings = MagicMock()
        mock_init.return_value = mock_settings

        result = runner.invoke(cli, ["parse-local", str(pdf)])
        assert result.exit_code == 0
        assert "skipped" in result.output.lower() or "already parsed" in result.output.lower()


# ---------------------------------------------------------------------------
# parse-file
# ---------------------------------------------------------------------------

def test_parse_file_success(runner: CliRunner):
    """parse-file prints success with the doc_parse ID."""
    with (
        patch("doc_parser.cli._init_db_engine") as mock_init,
        patch("doc_parser.pipeline.process_drive_file", new_callable=AsyncMock, return_value=99),
    ):
        mock_settings = MagicMock()
        mock_init.return_value = mock_settings

        result = runner.invoke(cli, ["parse-file", "drive-file-id"])
        assert result.exit_code == 0
        assert "99" in result.output


# ---------------------------------------------------------------------------
# parse-folder
# ---------------------------------------------------------------------------

def test_parse_folder_success(runner: CliRunner):
    """parse-folder prints parsed/skipped counts."""
    with (
        patch("doc_parser.cli._init_db_engine") as mock_init,
        patch("doc_parser.pipeline.process_folder", new_callable=AsyncMock, return_value=[1, 2, None]),
    ):
        mock_settings = MagicMock()
        mock_init.return_value = mock_settings

        result = runner.invoke(cli, ["parse-folder", "folder-abc"])
        assert result.exit_code == 0
        assert "Parsed: 2" in result.output
        assert "Skipped: 1" in result.output


# ---------------------------------------------------------------------------
# extract
# ---------------------------------------------------------------------------

def test_extract_success(runner: CliRunner):
    """extract prints success with the doc_extraction ID."""
    with (
        patch("doc_parser.cli._init_db_engine") as mock_init,
        patch("doc_parser.steps.step3_extract.run_extraction", new_callable=AsyncMock, return_value=10),
        patch("doc_parser.steps.run_extraction", new_callable=AsyncMock, return_value=10),
    ):
        mock_settings = MagicMock()
        mock_init.return_value = mock_settings

        result = runner.invoke(cli, ["extract", "1"])
        assert result.exit_code == 0
        assert "10" in result.output


def test_extract_with_provider_flag(runner: CliRunner):
    """extract --provider llm sets extraction_provider on settings."""
    with (
        patch("doc_parser.cli._init_db_engine") as mock_init,
        patch("doc_parser.steps.step3_extract.run_extraction", new_callable=AsyncMock, return_value=11),
        patch("doc_parser.steps.run_extraction", new_callable=AsyncMock, return_value=11),
    ):
        mock_settings = MagicMock()
        mock_init.return_value = mock_settings

        result = runner.invoke(cli, ["extract", "1", "--provider", "llm"])
        assert result.exit_code == 0
        assert "11" in result.output
        mock_settings.__setattr__("extraction_provider", "llm")


# ---------------------------------------------------------------------------
# run-all
# ---------------------------------------------------------------------------

def test_run_all_success(runner: CliRunner):
    """run-all shows pipeline results."""
    with (
        patch("doc_parser.cli._init_db_engine") as mock_init,
        patch(
            "doc_parser.pipeline.run_all_steps",
            new_callable=AsyncMock,
            return_value={"parse_id": 2, "extraction_id": 3},
        ),
    ):
        mock_settings = MagicMock()
        mock_init.return_value = mock_settings

        result = runner.invoke(cli, ["run-all", "1"])
        assert result.exit_code == 0
        assert "Pipeline Results" in result.output


# ---------------------------------------------------------------------------
# parse (new step-based)
# ---------------------------------------------------------------------------

def test_parse_step_success(runner: CliRunner):
    """parse (step 2) prints success with the doc_parse ID."""
    with (
        patch("doc_parser.cli._init_db_engine") as mock_init,
        patch("doc_parser.steps.step2_parse.run_parse", new_callable=AsyncMock, return_value=7),
        patch("doc_parser.steps.run_parse", new_callable=AsyncMock, return_value=7),
    ):
        mock_settings = MagicMock()
        mock_init.return_value = mock_settings

        result = runner.invoke(cli, ["parse", "1"])
        assert result.exit_code == 0
        assert "7" in result.output
