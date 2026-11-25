"""Persist distributed trace context with outbox rows.

Revision ID: e91c420cfa33
Revises: 3a0f1c9d2e7b
"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "e91c420cfa33"
down_revision: str | Sequence[str] | None = "3a0f1c9d2e7b"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "outbox_messages",
        sa.Column("trace_context", sa.JSON(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("outbox_messages", "trace_context")
