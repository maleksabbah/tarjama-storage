"""initial schema

Revision ID: 0001_initial
Revises:
Create Date: 2026-05-31

"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "0001_initial"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "files",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("job_id", sa.String(length=36), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("category", sa.String(length=50), nullable=False),
        sa.Column("type", sa.String(length=50), nullable=False),
        sa.Column("path", sa.String(), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=True),
        sa.Column("mime_type", sa.String(length=100), nullable=True),
        sa.Column("idempotency_key", sa.String(length=200), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("expires_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_files_job_id", "files", ["job_id"])
    op.create_index("ix_files_user_id", "files", ["user_id"])
    op.create_index("ix_files_category", "files", ["category"])
    op.create_index("ix_files_idempotency_key", "files", ["idempotency_key"], unique=True)


def downgrade() -> None:
    op.drop_index("ix_files_idempotency_key", table_name="files")
    op.drop_index("ix_files_category", table_name="files")
    op.drop_index("ix_files_user_id", table_name="files")
    op.drop_index("ix_files_job_id", table_name="files")
    op.drop_table("files")