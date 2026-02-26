"""Tests for doc_parser.storage — single-file JSON result storage."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from doc_parser.storage import (
    has_result,
    list_results,
    load_result,
    resolve_sha_prefix,
    result_path,
    save_result,
)


SHA = "a" * 64
SHA2 = "b" * 64


def _make_result(sha: str = SHA, **overrides) -> dict:
    base = {
        "sha256": sha,
        "file_name": "report.pdf",
        "source": "local",
        "title": "Test Report",
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# result_path
# ---------------------------------------------------------------------------

def test_result_path(tmp_path: Path):
    """result_path returns <base>/<sha[:4]>/<sha>.json."""
    p = result_path(tmp_path, SHA)
    assert p == tmp_path / SHA[:4] / f"{SHA}.json"


# ---------------------------------------------------------------------------
# save_result / load_result round-trip
# ---------------------------------------------------------------------------

def test_save_and_load_round_trip(tmp_path: Path):
    """save_result writes JSON; load_result reads it back identically."""
    r = _make_result()
    path = save_result(tmp_path, r)
    assert path.exists()

    loaded = load_result(tmp_path, SHA)
    assert loaded == r


def test_save_creates_directory(tmp_path: Path):
    """save_result creates the parent directory."""
    r = _make_result()
    path = save_result(tmp_path, r)
    assert path.parent.is_dir()


def test_save_overwrites(tmp_path: Path):
    """Calling save_result twice overwrites without error."""
    r1 = _make_result(title="v1")
    save_result(tmp_path, r1)
    r2 = _make_result(title="v2")
    save_result(tmp_path, r2)
    loaded = load_result(tmp_path, SHA)
    assert loaded["title"] == "v2"


# ---------------------------------------------------------------------------
# load_result
# ---------------------------------------------------------------------------

def test_load_nonexistent(tmp_path: Path):
    """load_result returns None for a missing file."""
    assert load_result(tmp_path, SHA) is None


# ---------------------------------------------------------------------------
# has_result
# ---------------------------------------------------------------------------

def test_has_result_true(tmp_path: Path):
    """has_result returns True when the file exists."""
    save_result(tmp_path, _make_result())
    assert has_result(tmp_path, SHA) is True


def test_has_result_false(tmp_path: Path):
    """has_result returns False when the file doesn't exist."""
    assert has_result(tmp_path, SHA) is False


# ---------------------------------------------------------------------------
# list_results
# ---------------------------------------------------------------------------

def test_list_results_empty(tmp_path: Path):
    """list_results returns [] for an empty directory."""
    assert list_results(tmp_path) == []


def test_list_results_nonexistent(tmp_path: Path):
    """list_results returns [] for a nonexistent directory."""
    assert list_results(tmp_path / "nonexistent") == []


def test_list_results_multiple(tmp_path: Path):
    """list_results returns all saved results."""
    save_result(tmp_path, _make_result(SHA))
    save_result(tmp_path, _make_result(SHA2, file_name="other.pdf"))
    results = list_results(tmp_path)
    assert len(results) == 2
    shas = {r["sha256"] for r in results}
    assert shas == {SHA, SHA2}


# ---------------------------------------------------------------------------
# resolve_sha_prefix
# ---------------------------------------------------------------------------

def test_resolve_sha_prefix_full(tmp_path: Path):
    """Full SHA resolves to itself."""
    save_result(tmp_path, _make_result())
    assert resolve_sha_prefix(tmp_path, SHA) == SHA


def test_resolve_sha_prefix_short(tmp_path: Path):
    """Short prefix resolves correctly."""
    save_result(tmp_path, _make_result())
    assert resolve_sha_prefix(tmp_path, SHA[:8]) == SHA


def test_resolve_sha_prefix_very_short(tmp_path: Path):
    """Prefix < 4 chars resolves if unambiguous."""
    save_result(tmp_path, _make_result())
    assert resolve_sha_prefix(tmp_path, SHA[:2]) == SHA


def test_resolve_sha_prefix_not_found(tmp_path: Path):
    """Missing prefix raises ValueError."""
    save_result(tmp_path, _make_result())
    with pytest.raises(ValueError, match="No results found"):
        resolve_sha_prefix(tmp_path, "zzz")


def test_resolve_sha_prefix_ambiguous(tmp_path: Path):
    """Ambiguous prefix raises ValueError."""
    # Both SHA and SHA2 start with different prefixes, so create two with same prefix
    sha_a = "abcd" + "0" * 60
    sha_b = "abcd" + "1" * 60
    save_result(tmp_path, _make_result(sha_a))
    save_result(tmp_path, _make_result(sha_b, file_name="other.pdf"))
    with pytest.raises(ValueError, match="Ambiguous"):
        resolve_sha_prefix(tmp_path, "abcd")


def test_resolve_sha_prefix_empty_dir(tmp_path: Path):
    """Empty directory raises ValueError."""
    with pytest.raises(ValueError, match="No results found"):
        resolve_sha_prefix(tmp_path, "abc")
