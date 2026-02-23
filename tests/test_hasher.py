"""Tests for doc_parser.hasher — SHA-256 file hashing."""

from __future__ import annotations

import hashlib
from pathlib import Path

from doc_parser.hasher import sha256_file


def test_known_content(tmp_path: Path):
    """Known content produces the correct SHA-256 digest."""
    f = tmp_path / "hello.txt"
    f.write_text("hello world")
    expected = hashlib.sha256(b"hello world").hexdigest()
    assert sha256_file(f) == expected


def test_empty_file(tmp_path: Path):
    """Empty file produces the SHA-256 of empty bytes."""
    f = tmp_path / "empty"
    f.write_bytes(b"")
    expected = hashlib.sha256(b"").hexdigest()
    assert sha256_file(f) == expected


def test_large_file_multichunk(tmp_path: Path):
    """File larger than CHUNK_SIZE (8192) is hashed correctly."""
    data = b"A" * 50_000
    f = tmp_path / "large.bin"
    f.write_bytes(data)
    expected = hashlib.sha256(data).hexdigest()
    assert sha256_file(f) == expected


def test_binary_content(tmp_path: Path):
    """Binary (non-text) content is hashed correctly."""
    data = bytes(range(256)) * 10
    f = tmp_path / "binary.bin"
    f.write_bytes(data)
    expected = hashlib.sha256(data).hexdigest()
    assert sha256_file(f) == expected


def test_return_type_is_hex_string(tmp_path: Path):
    """Return value is a 64-character lowercase hex string."""
    f = tmp_path / "check.txt"
    f.write_text("test")
    result = sha256_file(f)
    assert isinstance(result, str)
    assert len(result) == 64
    assert all(c in "0123456789abcdef" for c in result)
