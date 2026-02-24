"""Tests for doc_parser.config — Settings construction and computed properties."""

from __future__ import annotations

from pathlib import Path

from doc_parser.config import Settings, get_settings


def test_constructor_with_overrides(tmp_path: Path):
    """Settings can be constructed with keyword overrides."""
    s = Settings(
        textin_app_id="app1",
        textin_secret_code="sec1",
        data_dir=tmp_path / "mydata",
    )
    assert s.textin_app_id == "app1"
    assert s.textin_secret_code == "sec1"
    assert s.data_dir == tmp_path / "mydata"


def test_parsed_path_computed(tmp_path: Path):
    """parsed_path is data_dir / 'parsed'."""
    s = Settings(
        textin_app_id="a",
        textin_secret_code="s",
        data_dir=tmp_path / "data",
    )
    assert s.parsed_path == tmp_path / "data" / "parsed"


def test_watermark_path_computed(tmp_path: Path):
    """watermark_path is data_dir / 'watermark'."""
    s = Settings(
        textin_app_id="a",
        textin_secret_code="s",
        data_dir=tmp_path / "data",
    )
    assert s.watermark_path == tmp_path / "data" / "watermark"


def test_extraction_path_computed(tmp_path: Path):
    """extraction_path is data_dir / 'extraction'."""
    s = Settings(
        textin_app_id="a",
        textin_secret_code="s",
        data_dir=tmp_path / "data",
    )
    assert s.extraction_path == tmp_path / "data" / "extraction"


def test_ensure_dirs_creates_directory(tmp_path: Path):
    """ensure_dirs() creates all data directories."""
    s = Settings(
        textin_app_id="a",
        textin_secret_code="s",
        data_dir=tmp_path / "data",
    )
    assert not s.parsed_path.exists()
    assert not s.watermark_path.exists()
    assert not s.extraction_path.exists()
    s.ensure_dirs()
    assert s.parsed_path.is_dir()
    assert s.watermark_path.is_dir()
    assert s.extraction_path.is_dir()


def test_ensure_dirs_idempotent(tmp_path: Path):
    """Calling ensure_dirs() twice does not raise."""
    s = Settings(
        textin_app_id="a",
        textin_secret_code="s",
        data_dir=tmp_path / "data",
    )
    s.ensure_dirs()
    s.ensure_dirs()  # no error
    assert s.parsed_path.is_dir()


def test_default_values():
    """Default values are applied when not overridden."""
    s = Settings(textin_app_id="a", textin_secret_code="s")
    assert s.database_url == "postgresql+psycopg://user:password@localhost:5432/doc_parser"
    assert s.textin_parse_mode == "auto"
    assert s.textin_max_concurrent == 3
    assert s.google_service_account is False


def test_get_settings_factory(tmp_path: Path):
    """get_settings() returns a Settings instance with overrides."""
    s = get_settings(
        textin_app_id="x",
        textin_secret_code="y",
        data_dir=tmp_path,
    )
    assert isinstance(s, Settings)
    assert s.textin_app_id == "x"
