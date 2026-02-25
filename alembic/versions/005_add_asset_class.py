"""Add asset_class column to doc_file and doc_extraction.

Revision ID: 005
Revises: 004
Create Date: 2026-02-25
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "005"
down_revision: Union[str, None] = "004"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("doc_file", sa.Column("asset_class", sa.String(255)))
    op.add_column("doc_extraction", sa.Column("asset_class", sa.String(255)))


def downgrade() -> None:
    op.drop_column("doc_extraction", "asset_class")
    op.drop_column("doc_file", "asset_class")
