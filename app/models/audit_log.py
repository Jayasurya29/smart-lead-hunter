"""
SMART LEAD HUNTER — Audit Log Model
=====================================
Tracks who did what to which lead, with before/after snapshots.

Used by sales team accountability ("who rejected that lead?")
and debugging ("why did the score change?").
"""

from sqlalchemy import Column, String, Integer, DateTime, Text
from sqlalchemy.dialects.postgresql import JSONB

from app.database import Base
from app.services.utils import local_now


class AuditLog(Base):
    """Immutable audit trail for all lead state changes."""

    __tablename__ = "audit_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # Who
    user_email = Column(String(255))  # From JWT cookie or "system" for automated
    user_id = Column(Integer, nullable=True)

    # What
    action = Column(
        String(50), nullable=False, index=True
    )  # approve, reject, restore, delete, edit, enrich, scrape_save

    # Which lead
    lead_id = Column(Integer, nullable=True, index=True)
    hotel_name = Column(String(255))  # Snapshot — readable even if lead is deleted

    # Before/after snapshots (only changed fields for edits)
    old_values = Column(JSONB, default=dict)
    new_values = Column(JSONB, default=dict)

    # Extra context
    detail = Column(Text)  # e.g. "Rejected: budget_brand", "CRM push: 3 contacts"

    # When
    created_at = Column(
        DateTime(timezone=True), default=lambda: local_now(), index=True
    )

    def __repr__(self):
        return f"<AuditLog {self.action} lead={self.lead_id} by={self.user_email}>"

    def to_dict(self):
        return {
            "id": self.id,
            "user_email": self.user_email,
            "action": self.action,
            "lead_id": self.lead_id,
            "hotel_name": self.hotel_name,
            "old_values": self.old_values,
            "new_values": self.new_values,
            "detail": self.detail,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }
