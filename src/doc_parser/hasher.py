"""SHA-256 file hashing for content dedup."""

from __future__ import annotations

import hashlib
from pathlib import Path

CHUNK_SIZE = 8192


def sha256_file(path: Path) -> str:
    """Compute the SHA-256 hex digest of a file, reading in chunks."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(CHUNK_SIZE):
            h.update(chunk)
    return h.hexdigest()
