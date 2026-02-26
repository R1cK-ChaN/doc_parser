"""Single-file JSON result storage."""

from __future__ import annotations

import json
from pathlib import Path


def result_path(extraction_path: Path, sha: str) -> Path:
    """Return the path for a result JSON: <extraction_path>/<sha[:4]>/<sha>.json."""
    return extraction_path / sha[:4] / f"{sha}.json"


def save_result(extraction_path: Path, result: dict) -> Path:
    """Write a result dict as JSON and return the path."""
    sha = result["sha256"]
    path = result_path(extraction_path, sha)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def load_result(extraction_path: Path, sha: str) -> dict | None:
    """Read a result JSON, or return None if it doesn't exist."""
    path = result_path(extraction_path, sha)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def has_result(extraction_path: Path, sha: str) -> bool:
    """Check whether a result JSON exists."""
    return result_path(extraction_path, sha).exists()


def list_results(extraction_path: Path) -> list[dict]:
    """Scan all result JSONs and return them as dicts."""
    results: list[dict] = []
    if not extraction_path.exists():
        return results
    for json_file in sorted(extraction_path.glob("*/*.json")):
        try:
            data = json.loads(json_file.read_text(encoding="utf-8"))
            results.append(data)
        except (json.JSONDecodeError, OSError):
            continue
    return results


def resolve_sha_prefix(extraction_path: Path, prefix: str) -> str:
    """Resolve a short SHA prefix to a full SHA (like git short refs).

    Raises ValueError if zero or multiple matches.
    """
    if not extraction_path.exists():
        raise ValueError(f"No results found for prefix '{prefix}'")

    matches: list[str] = []
    # The bucket directory is sha[:4], so if prefix is >= 4 chars we only check one bucket
    bucket = prefix[:4]
    bucket_dir = extraction_path / bucket
    if bucket_dir.is_dir():
        for json_file in bucket_dir.glob("*.json"):
            sha = json_file.stem
            if sha.startswith(prefix):
                matches.append(sha)
    else:
        # prefix < 4 chars: scan all bucket dirs that start with prefix
        for d in sorted(extraction_path.iterdir()):
            if d.is_dir() and d.name.startswith(prefix):
                for json_file in d.glob("*.json"):
                    sha = json_file.stem
                    if sha.startswith(prefix):
                        matches.append(sha)

    if len(matches) == 0:
        raise ValueError(f"No results found for prefix '{prefix}'")
    if len(matches) > 1:
        raise ValueError(f"Ambiguous prefix '{prefix}' matches {len(matches)} results")
    return matches[0]
