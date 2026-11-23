"""Allow processing delivery state."""
from alembic import op
revision = "3a0f1c9d2e7b"
down_revision = "6085221658e4"
branch_labels = None
depends_on = None

def upgrade() -> None:
    op.drop_constraint("ck_deliveries_status", "deliveries", type_="check")
    op.create_check_constraint("ck_deliveries_status", "deliveries", "status IN ('queued', 'processing', 'retrying', 'delivered', 'dead', 'permanent_failure', 'paused')")

def downgrade() -> None:
    op.drop_constraint("ck_deliveries_status", "deliveries", type_="check")
    op.create_check_constraint("ck_deliveries_status", "deliveries", "status IN ('queued', 'retrying', 'delivered', 'dead', 'permanent_failure', 'paused')")
