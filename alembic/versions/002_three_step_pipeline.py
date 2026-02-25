"""Two-step pipeline: ParseX and entity extraction.

- Convert DateTime columns to BigInteger (epoch seconds)
- Add new columns to doc_file and doc_parse
- Create doc_extraction table

Revision ID: 002
Revises: 001
Create Date: 2026-02-24
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "002"
down_revision: Union[str, None] = "001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # -----------------------------------------------------------------------
    # 1. Convert DateTime columns to BigInteger (epoch seconds) on doc_file
    # -----------------------------------------------------------------------
    op.alter_column(
        "doc_file", "publish_date",
        type_=sa.BigInteger(),
        existing_type=sa.DateTime(timezone=True),
        postgresql_using="EXTRACT(EPOCH FROM publish_date)::bigint",
    )
    # Drop server defaults before type conversion
    op.alter_column("doc_file", "created_at", server_default=None, existing_type=sa.DateTime(timezone=True))
    op.alter_column(
        "doc_file", "created_at",
        type_=sa.BigInteger(),
        existing_type=sa.DateTime(timezone=True),
        postgresql_using="EXTRACT(EPOCH FROM created_at)::bigint",
    )
    op.alter_column("doc_file", "updated_at", server_default=None, existing_type=sa.DateTime(timezone=True))
    op.alter_column(
        "doc_file", "updated_at",
        type_=sa.BigInteger(),
        existing_type=sa.DateTime(timezone=True),
        postgresql_using="EXTRACT(EPOCH FROM updated_at)::bigint",
    )

    # -----------------------------------------------------------------------
    # 2. Convert DateTime columns to BigInteger (epoch seconds) on doc_parse
    # -----------------------------------------------------------------------
    op.alter_column(
        "doc_parse", "started_at",
        type_=sa.BigInteger(),
        existing_type=sa.DateTime(timezone=True),
        postgresql_using="EXTRACT(EPOCH FROM started_at)::bigint",
    )
    op.alter_column(
        "doc_parse", "completed_at",
        type_=sa.BigInteger(),
        existing_type=sa.DateTime(timezone=True),
        postgresql_using="EXTRACT(EPOCH FROM completed_at)::bigint",
    )

    # -----------------------------------------------------------------------
    # 3. Add new columns to doc_file (for query convenience from Step 3)
    # -----------------------------------------------------------------------
    op.add_column("doc_file", sa.Column("market", sa.String(255)))
    op.add_column("doc_file", sa.Column("sector", sa.String(255)))
    op.add_column("doc_file", sa.Column("document_type", sa.String(255)))
    op.add_column("doc_file", sa.Column("target_company", sa.String(255)))
    op.add_column("doc_file", sa.Column("ticker_symbol", sa.String(50)))
    op.add_column("doc_file", sa.Column("authors", sa.String(1024)))

    # -----------------------------------------------------------------------
    # 4. Add new column to doc_parse
    # -----------------------------------------------------------------------
    op.add_column("doc_parse", sa.Column("src_page_count", sa.Integer()))

    # -----------------------------------------------------------------------
    # 5. Create doc_extraction table
    # -----------------------------------------------------------------------
    op.create_table(
        "doc_extraction",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("doc_file_id", sa.Integer(), sa.ForeignKey("doc_file.id", ondelete="CASCADE"), nullable=False),
        sa.Column("doc_parse_id", sa.Integer(), sa.ForeignKey("doc_parse.id", ondelete="SET NULL")),
        sa.Column("status", sa.String(20), nullable=False, server_default="pending"),
        sa.Column("started_at", sa.BigInteger()),
        sa.Column("completed_at", sa.BigInteger()),
        sa.Column("duration_ms", sa.Integer()),
        sa.Column("textin_request_id", sa.String(255)),
        sa.Column("title", sa.String(1024)),
        sa.Column("broker", sa.String(255)),
        sa.Column("authors", sa.String(1024)),
        sa.Column("publish_date", sa.BigInteger()),
        sa.Column("market", sa.String(255)),
        sa.Column("sector", sa.String(255)),
        sa.Column("document_type", sa.String(255)),
        sa.Column("target_company", sa.String(255)),
        sa.Column("ticker_symbol", sa.String(50)),
        sa.Column("extraction_json_path", sa.String(1024)),
        sa.Column("extraction_config", JSONB()),
        sa.Column("error_message", sa.Text()),
    )


def downgrade() -> None:
    op.drop_table("doc_extraction")

    op.drop_column("doc_parse", "src_page_count")

    op.drop_column("doc_file", "authors")
    op.drop_column("doc_file", "ticker_symbol")
    op.drop_column("doc_file", "target_company")
    op.drop_column("doc_file", "document_type")
    op.drop_column("doc_file", "sector")
    op.drop_column("doc_file", "market")

    # Revert BigInteger back to DateTime on doc_parse
    op.alter_column(
        "doc_parse", "completed_at",
        type_=sa.DateTime(timezone=True),
        existing_type=sa.BigInteger(),
        postgresql_using="to_timestamp(completed_at)",
    )
    op.alter_column(
        "doc_parse", "started_at",
        type_=sa.DateTime(timezone=True),
        existing_type=sa.BigInteger(),
        postgresql_using="to_timestamp(started_at)",
    )

    # Revert BigInteger back to DateTime on doc_file
    op.alter_column(
        "doc_file", "updated_at",
        type_=sa.DateTime(timezone=True),
        existing_type=sa.BigInteger(),
        postgresql_using="to_timestamp(updated_at)",
    )
    op.alter_column(
        "doc_file", "created_at",
        type_=sa.DateTime(timezone=True),
        existing_type=sa.BigInteger(),
        postgresql_using="to_timestamp(created_at)",
    )
    op.alter_column(
        "doc_file", "publish_date",
        type_=sa.DateTime(timezone=True),
        existing_type=sa.BigInteger(),
        postgresql_using="to_timestamp(publish_date)",
    )
