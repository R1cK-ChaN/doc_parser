# doc-parser

Finance report parsing pipeline: Google Drive → TextIn xParse → PostgreSQL.

Downloads PDF and image files from Google Drive (or the local filesystem), sends
them to the [TextIn xParse](https://www.textin.com/) API for OCR and structural
extraction, and persists the results — markdown, element-level detail JSON, page
metadata, and optional Excel tables — in PostgreSQL with content-addressed local
file storage.

## Architecture

```
                        ┌────────────────────┐
                        │   Google Drive API  │
                        └────────┬───────────┘
                                 │ list / download
                                 ▼
┌──────────┐  local    ┌─────────────────┐  HTTP POST   ┌───────────────┐
│  Local   │ ────────► │    pipeline     │ ───────────► │  TextIn xParse│
│  files   │           │  (orchestrator) │ ◄─────────── │  API          │
└──────────┘           └───────┬─────────┘  ParseResult └───────────────┘
                               │
               ┌───────────────┼───────────────┐
               ▼               ▼               ▼
        ┌────────────┐  ┌────────────┐  ┌────────────┐
        │ PostgreSQL │  │   Local    │  │  SHA-256   │
        │  (3 tables)│  │  storage   │  │   dedup    │
        └────────────┘  └────────────┘  └────────────┘
```

### Processing flow

1. **Resolve source** — accept a Google Drive file/folder ID or a local path.
   Drive files are downloaded to a temporary file that is cleaned up after
   parsing.
2. **Hash** — compute the SHA-256 digest for content-based deduplication.
3. **Upsert `doc_file`** — create or update the file record in PostgreSQL.
4. **Dedup check** — skip files that already have a `completed` parse unless
   `--reparse` is passed.
5. **Create `doc_parse`** — record the parse attempt with status `running` and
   the exact TextIn parameters used.
6. **Call TextIn** — POST the file bytes to the xParse sync endpoint.  Retries
   up to 3 times with exponential back-off on 5xx / connection errors.
7. **Store outputs** — write `output.md`, `detail.json`, `pages.json`, and
   optionally `tables.xlsx` under content-addressed paths.
8. **Update `doc_parse`** — set status to `completed` (or `failed`), store
   duration, paths, and page counts.
9. **Extract `doc_element` rows** — one row per structural element (text block,
   table, image, heading, etc.) with bounding-box position and optional
   table-cell JSON.

### Module map

```
src/doc_parser/
├── config.py          Settings via pydantic-settings (.env + overrides)
├── db.py              Async SQLAlchemy engine, session factory, get_session()
├── models.py          ORM: DocFile, DocParse, DocElement
├── hasher.py          SHA-256 file hashing (chunked, streaming)
├── textin_client.py   TextIn xParse HTTP client (httpx + tenacity retry)
├── google_drive.py    Google Drive v3: list, download, metadata
├── storage.py         Content-addressed filesystem writes
├── pipeline.py        Orchestration: download → hash → parse → store
├── cli.py             Click CLI (init-db, parse-folder, parse-file, parse-local, list-files, status)
├── __main__.py        python -m doc_parser entry point
└── __init__.py
```

### Data model

```
doc_file  1 ──── * doc_parse  1 ──── * doc_element
```

| Table | Purpose | Key columns |
|-------|---------|-------------|
| `doc_file` | One row per unique source file | `file_id` (unique), `sha256`, `source`, `file_name`, `mime_type` |
| `doc_parse` | One row per TextIn parse invocation | `status`, `parse_mode`, `parse_config` (JSONB), storage paths, `duration_ms` |
| `doc_element` | One row per structural element | `element_type`, `text`, `position` (JSONB), `table_cells` (JSONB), `page_number` |

All foreign keys cascade on delete.  `doc_parse.parse_config` stores the exact
query parameters sent to TextIn for full reproducibility.

### Local storage layout

```
data/parsed/
└── <sha256[:4]>/
    └── <sha256>/
        └── <parse_id>/
            ├── output.md
            ├── detail.json
            ├── pages.json
            └── tables.xlsx   (optional)
```

The first four characters of the SHA-256 are used as a fan-out prefix to avoid
directories with too many entries.

## Dependencies

### Runtime

| Package | Version | Role |
|---------|---------|------|
| [click](https://click.palletsprojects.com/) | >= 8.1 | CLI framework |
| [httpx](https://www.python-httpx.org/) | >= 0.27 | Async HTTP client for TextIn API |
| [SQLAlchemy](https://www.sqlalchemy.org/) | >= 2.0 (with `asyncio` extra) | Async ORM and query builder |
| [psycopg](https://www.psycopg.org/) | >= 3.1 (binary) | PostgreSQL async driver |
| [Alembic](https://alembic.sqlalchemy.org/) | >= 1.13 | Database schema migrations |
| [pydantic-settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/) | >= 2.0 | Configuration from `.env` / environment variables |
| [google-api-python-client](https://github.com/googleapis/google-api-python-client) | >= 2.100 | Google Drive API v3 |
| [google-auth](https://google-auth.readthedocs.io/) | >= 2.23 | Google OAuth2 / service account credentials |
| [google-auth-oauthlib](https://google-auth-oauthlib.readthedocs.io/) | >= 1.1 | OAuth2 interactive flow |
| [python-dotenv](https://github.com/theskumar/python-dotenv) | >= 1.0 | `.env` file loading |
| [rich](https://rich.readthedocs.io/) | >= 13.0 | Terminal tables and colored output |
| [tenacity](https://tenacity.readthedocs.io/) | >= 8.2 | Retry with exponential back-off |

### Test-only

| Package | Version | Role |
|---------|---------|------|
| [pytest](https://docs.pytest.org/) | >= 8.0 | Test runner |
| [pytest-asyncio](https://pytest-asyncio.readthedocs.io/) | >= 0.24 | Async test support |
| [aiosqlite](https://aiosqlite.omnilib.dev/) | >= 0.20 | In-memory SQLite for database tests |

### External services

| Service | Required for | Auth |
|---------|-------------|------|
| **PostgreSQL** (>= 14 recommended) | Persistent storage | Connection string in `DATABASE_URL` |
| **TextIn xParse API** | Document parsing / OCR | `TEXTIN_APP_ID` + `TEXTIN_SECRET_CODE` |
| **Google Drive API v3** | Remote file source | OAuth2 `credentials.json` or service account key |

## Setup

### Prerequisites

- Python >= 3.11
- PostgreSQL (running and accessible)
- TextIn API credentials ([textin.com](https://www.textin.com/))
- Google Drive OAuth2 or service account credentials (only if using Drive)

### Install

```bash
# Clone and install in editable mode
git clone <repo-url> && cd doc_parser
python -m venv .venv && source .venv/bin/activate
pip install -e .
```

### Configure

```bash
cp .env.example .env
# Edit .env with your credentials:
#   TEXTIN_APP_ID, TEXTIN_SECRET_CODE, DATABASE_URL,
#   GOOGLE_CREDENTIALS_FILE, etc.
```

All settings can also be passed as environment variables.  See
`src/doc_parser/config.py` for the full list and defaults.

### Create database tables

```bash
doc-parser init-db
```

This runs `alembic upgrade head` under the hood.

## Usage

```bash
# Parse all supported files in a Google Drive folder
doc-parser parse-folder <FOLDER_ID>

# Parse a single file from Google Drive
doc-parser parse-file <FILE_ID>

# Parse a local file (no Drive needed)
doc-parser parse-local /path/to/report.pdf

# List files in a Drive folder
doc-parser list-files <FOLDER_ID>

# Show database statistics
doc-parser status

# Enable debug logging
doc-parser -v parse-local report.pdf
```

### Common flags

| Flag | Description |
|------|-------------|
| `--reparse` | Re-parse files that already have a completed parse |
| `--parse-mode MODE` | Override TextIn parse mode (default: `auto`) |
| `--no-excel` | Skip Excel table extraction |
| `--no-chart` | Skip chart recognition |
| `-v` / `--verbose` | Debug-level logging |

## Testing

Tests run entirely offline — no PostgreSQL, no TextIn API, no Google Drive
credentials.  Database tests use async SQLite in-memory, HTTP calls are mocked,
and filesystem operations use pytest's `tmp_path`.

```bash
# Install test dependencies
pip install -e ".[test]"

# Run the full suite (77 tests, < 1 second)
pytest -v

# Run a single module
pytest tests/test_pipeline.py

# Run by keyword
pytest -k "dedup"
```

## Project structure

```
doc_parser/
├── src/doc_parser/         # Application source
├── tests/                  # Test suite (77 tests)
│   ├── conftest.py         #   Shared fixtures, JSONB→JSON SQLite shim
│   ├── test_hasher.py      #   SHA-256 hashing
│   ├── test_config.py      #   Settings + computed fields
│   ├── test_storage.py     #   Filesystem writes
│   ├── test_textin_client.py  # TextIn client + retry logic
│   ├── test_google_drive.py   # Drive client (mocked service)
│   ├── test_db.py          #   Engine + session lifecycle
│   ├── test_models.py      #   ORM round-trips
│   ├── test_pipeline.py    #   End-to-end orchestration
│   └── test_cli.py         #   CLI commands (Click CliRunner)
├── alembic/                # Database migrations
│   ├── env.py
│   └── versions/
│       └── 001_initial_schema.py
├── alembic.ini
├── pyproject.toml
├── .env.example
└── README.md
```

## License

Private / internal use.
