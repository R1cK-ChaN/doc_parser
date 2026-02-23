"""Initial schema: doc_file, doc_parse, doc_element.

Revision ID: 001
Revises:
Create Date: 2026-02-23
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "doc_file",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("file_id", sa.String(255), unique=True, nullable=False),
        sa.Column("sha256", sa.String(64), nullable=False, index=True),
        sa.Column("source", sa.String(50), nullable=False),
        sa.Column("mime_type", sa.String(127)),
        sa.Column("file_name", sa.String(512), nullable=False),
        sa.Column("file_size_bytes", sa.Integer()),
        sa.Column("publish_date", sa.DateTime(timezone=True)),
        sa.Column("broker", sa.String(255)),
        sa.Column("title", sa.String(1024)),
        sa.Column("drive_folder_id", sa.String(255)),
        sa.Column("local_path", sa.String(1024)),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    op.create_table(
        "doc_parse",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("doc_file_id", sa.Integer(), sa.ForeignKey("doc_file.id", ondelete="CASCADE"), nullable=False),
        sa.Column("parse_mode", sa.String(50), nullable=False, server_default="auto"),
        sa.Column("status", sa.String(20), nullable=False, server_default="pending"),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("duration_ms", sa.Integer()),
        sa.Column("textin_request_id", sa.String(255)),
        sa.Column("markdown_path", sa.String(1024)),
        sa.Column("detail_json_path", sa.String(1024)),
        sa.Column("pages_json_path", sa.String(1024)),
        sa.Column("excel_path", sa.String(1024)),
        sa.Column("has_excel", sa.Boolean(), server_default="false"),
        sa.Column("has_chart", sa.Boolean(), server_default="false"),
        sa.Column("page_count", sa.Integer()),
        sa.Column("valid_page_count", sa.Integer()),
        sa.Column("error_message", sa.Text()),
        sa.Column("parse_config", JSONB()),
    )

    op.create_table(
        "doc_element",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("doc_parse_id", sa.Integer(), sa.ForeignKey("doc_parse.id", ondelete="CASCADE"), nullable=False),
        sa.Column("page_number", sa.Integer()),
        sa.Column("element_type", sa.String(50)),
        sa.Column("sub_type", sa.String(50)),
        sa.Column("text", sa.Text()),
        sa.Column("position", JSONB()),
        sa.Column("char_pos_start", sa.Integer()),
        sa.Column("char_pos_end", sa.Integer()),
        sa.Column("outline_level", sa.Integer()),
        sa.Column("content_flag", sa.String(50)),
        sa.Column("image_url", sa.String(1024)),
        sa.Column("table_cells", JSONB()),
    )

    op.create_index("ix_doc_element_parse_page", "doc_element", ["doc_parse_id", "page_number"])


def downgrade() -> None:
    op.drop_table("doc_element")
    op.drop_table("doc_parse")
    op.drop_table("doc_file")
