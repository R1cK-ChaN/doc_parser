"""Add chart enhancement columns to doc_parse.

- ADD enhanced_markdown_path String(1024) to doc_parse
- ADD chart_count Integer to doc_parse

Revision ID: 004
Revises: 003
Create Date: 2026-02-25
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "004"
down_revision: Union[str, None] = "003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("doc_parse", sa.Column("enhanced_markdown_path", sa.String(1024)))
    op.add_column("doc_parse", sa.Column("chart_count", sa.Integer()))


def downgrade() -> None:
    op.drop_column("doc_parse", "chart_count")
    op.drop_column("doc_parse", "enhanced_markdown_path")
