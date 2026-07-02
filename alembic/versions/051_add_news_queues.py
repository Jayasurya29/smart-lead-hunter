"""add news action queues (hotel-lead approval + person job-change review)

Two human-in-the-loop queues fed by the news scan (same pattern as
pending_names/pending_moves — nothing enters the pipeline unreviewed):

  news_lead_queue     A qualified NEW org (hotel/edu/healthcare) surfaced in the
                      news that is NOT already in existing_hotels/potential_leads.
                      Held for approval; on approve -> save_lead_to_db() creates a
                      potential_lead (status='new') which auto_smart_fill enriches.

  news_person_review  A person named in an appointment/mgmt-change story who
                      already exists in our contacts/lead_contacts (a job-change
                      signal). Deduped per person+move; self-matches (same place)
                      are never queued. Reviewed -> actioned/dismissed.

Revision ID: 051
Revises: 050
"""
from alembic import op
import sqlalchemy as sa

revision = "051"
down_revision = "050"
branch_labels = None
depends_on = None


def _has_table(conn, name: str) -> bool:
    return sa.inspect(conn).has_table(name)


def upgrade() -> None:
    conn = op.get_bind()

    if not _has_table(conn, "news_lead_queue"):
        op.create_table(
            "news_lead_queue",
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("news_id", sa.Integer),                       # hotel_news.id
            sa.Column("hotel_name", sa.Text, nullable=False),
            sa.Column("hotel_name_normalized", sa.Text, nullable=False),
            sa.Column("brand", sa.Text),
            sa.Column("city", sa.Text),
            sa.Column("region", sa.String(20)),
            sa.Column("vertical", sa.String(20)),
            sa.Column("category", sa.String(40)),
            sa.Column("luxury", sa.Boolean, server_default=sa.text("false")),
            sa.Column("source", sa.String(160)),
            sa.Column("url", sa.Text),
            sa.Column("status", sa.Text, nullable=False, server_default="pending"),
            sa.Column("created_lead_id", sa.Integer),               # set on approve
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
            sa.Column("reviewed_at", sa.DateTime(timezone=True)),
            sa.CheckConstraint(
                "status IN ('pending','approved','rejected')",
                name="ck_news_lead_queue_status",
            ),
        )
        op.execute(
            "CREATE INDEX IF NOT EXISTS ix_news_lead_queue_status "
            "ON news_lead_queue(status)"
        )
        # one open row per distinct new org (dedups repeat stories of same hotel)
        op.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_news_lead_queue_open "
            "ON news_lead_queue(hotel_name_normalized) WHERE status='pending'"
        )

    if not _has_table(conn, "news_person_review"):
        op.create_table(
            "news_person_review",
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("news_id", sa.Integer),                       # hotel_news.id
            sa.Column("person_name", sa.Text, nullable=False),
            sa.Column("person_title", sa.Text),
            sa.Column("new_hotel", sa.Text),                        # where the news puts them
            sa.Column("new_org", sa.Text),
            sa.Column("match_strength", sa.String(20)),             # email-exact | name-match
            sa.Column("known_contact_id", sa.Integer),             # contacts.id
            sa.Column("known_lead_contact_id", sa.Integer),        # lead_contacts.id
            sa.Column("known_account", sa.Text),                    # where we knew them
            sa.Column("status", sa.Text, nullable=False, server_default="pending"),
            sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
            sa.Column("reviewed_at", sa.DateTime(timezone=True)),
            sa.CheckConstraint(
                "status IN ('pending','actioned','dismissed')",
                name="ck_news_person_review_status",
            ),
        )
        op.execute(
            "CREATE INDEX IF NOT EXISTS ix_news_person_review_status "
            "ON news_person_review(status)"
        )
        # one open row per person + destination (dedups David Lang x4)
        op.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_news_person_review_open "
            "ON news_person_review(LOWER(person_name), LOWER(COALESCE(new_hotel,''))) "
            "WHERE status='pending'"
        )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS news_person_review")
    op.execute("DROP TABLE IF EXISTS news_lead_queue")
