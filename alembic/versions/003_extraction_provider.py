"""Add extraction provider columns to doc_extraction.

- ADD provider String(50) to doc_extraction
- ADD llm_model String(255) to doc_extraction

Revision ID: 003
Revises: 002
Create Date: 2026-02-24
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "003"
down_revision: Union[str, None] = "002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("doc_extraction", sa.Column("provider", sa.String(50)))
    op.add_column("doc_extraction", sa.Column("llm_model", sa.String(255)))


def downgrade() -> None:
    op.drop_column("doc_extraction", "llm_model")
    op.drop_column("doc_extraction", "provider")
