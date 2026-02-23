"""Tests for doc_parser.db — engine and session management."""

from __future__ import annotations

from pathlib import Path

import pytest

import doc_parser.db as db_module
from doc_parser.config import Settings


# ---------------------------------------------------------------------------
# Autouse fixture: reset module-level globals before each test
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _reset_db_globals():
    """Reset the module-level _engine and _session_factory before each test."""
    original_engine = db_module._engine
    original_factory = db_module._session_factory
    db_module._engine = None
    db_module._session_factory = None
    yield
    db_module._engine = original_engine
    db_module._session_factory = original_factory


def _make_settings(tmp_path: Path) -> Settings:
    # Use a file-based SQLite so that pool_size/max_overflow (QueuePool) work.
    db_path = tmp_path / "test.db"
    return Settings(
        textin_app_id="a",
        textin_secret_code="s",
        database_url=f"sqlite+aiosqlite:///{db_path}",
    )


# ---------------------------------------------------------------------------
# init_engine
# ---------------------------------------------------------------------------

def test_init_engine_creates_engine_and_factory(tmp_path: Path):
    """init_engine sets up _engine and _session_factory."""
    db_module.init_engine(_make_settings(tmp_path))
    assert db_module._engine is not None
    assert db_module._session_factory is not None


# ---------------------------------------------------------------------------
# get_engine / get_session_factory before init
# ---------------------------------------------------------------------------

def test_get_engine_before_init_raises():
    """get_engine() raises RuntimeError before init_engine()."""
    with pytest.raises(RuntimeError, match="not initialized"):
        db_module.get_engine()


def test_get_session_factory_before_init_raises():
    """get_session_factory() raises RuntimeError before init_engine()."""
    with pytest.raises(RuntimeError, match="not initialized"):
        db_module.get_session_factory()


# ---------------------------------------------------------------------------
# get_engine / get_session_factory after init
# ---------------------------------------------------------------------------

def test_get_engine_after_init(tmp_path: Path):
    """get_engine() returns the engine after init_engine()."""
    db_module.init_engine(_make_settings(tmp_path))
    engine = db_module.get_engine()
    assert engine is db_module._engine


def test_get_session_factory_after_init(tmp_path: Path):
    """get_session_factory() returns the factory after init_engine()."""
    db_module.init_engine(_make_settings(tmp_path))
    factory = db_module.get_session_factory()
    assert factory is db_module._session_factory


# ---------------------------------------------------------------------------
# get_session context manager
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_get_session_yields_async_session(tmp_path: Path):
    """get_session() yields an AsyncSession."""
    from sqlalchemy.ext.asyncio import AsyncSession

    db_module.init_engine(_make_settings(tmp_path))
    async with db_module.get_session() as session:
        assert isinstance(session, AsyncSession)
