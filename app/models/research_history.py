"""ResearchHistory — persisted output of the outreach pipeline.

One row per generated outreach. Links back to the source
contact/lead/hotel via `lead_contact_id` and `lead_id` /
`existing_hotel_id` (whichever side this contact came from).

Approval workflow:
  - Status 'pending'  — generated, awaiting human review
  - Status 'approved' — operator has approved
  - Status 'rejected' — operator has rejected
  - Status 'sent'     — manually marked as sent (Phase 1 sales reps copy
                        the email body and send from their own inbox).

Phase 2 (real Gemini → Resend send) would add 'sent_at' and 'send_id'
columns plus auto-flip approved → sent on /approve.
"""

from __future__ import annotations

from sqlalchemy import (
    Boolean,
    Column,
    Integer,
    String,
    Text,
    DateTime,
    ForeignKey,
    Index,
)
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.sql import func

from app.database import Base


class ResearchHistory(Base):
    __tablename__ = "research_history"

    id = Column(Integer, primary_key=True, index=True)

    # ── Source linkage (nullable so manual entries with no SLH parent
    #    still work — though in practice the UI always links back) ──
    lead_id = Column(
        Integer,
        ForeignKey("potential_leads.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    existing_hotel_id = Column(
        Integer,
        ForeignKey("existing_hotels.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    lead_contact_id = Column(
        Integer,
        ForeignKey("lead_contacts.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    # ── Snapshot inputs (frozen at research time so deleting the parent
    #    contact doesn't blow away the outreach record) ──
    contact_name = Column(String(255), nullable=False)
    contact_title = Column(String(255), nullable=True)
    hotel_name = Column(String(255), nullable=False)
    hotel_location = Column(String(255), nullable=True)
    linkedin_url = Column(String(500), nullable=True)
    email = Column(String(255), nullable=True)

    # ── Researcher output ──
    company_summary = Column(Text, nullable=True)
    contact_summary = Column(Text, nullable=True)
    pain_points = Column(ARRAY(Text), nullable=True)
    signals = Column(ARRAY(Text), nullable=True)
    outreach_angle = Column(String(500), nullable=True)
    personalization_hook = Column(Text, nullable=True)
    hotel_tier = Column(String(50), nullable=True)
    hiring_signals = Column(ARRAY(Text), nullable=True)
    recent_news = Column(ARRAY(Text), nullable=True)

    # ── Analyst output ──
    fit_score = Column(Integer, nullable=True, index=True)
    value_props = Column(ARRAY(Text), nullable=True)

    # ── Writer output ──
    email_subject = Column(String(500), nullable=True)
    email_body = Column(Text, nullable=True)
    linkedin_message = Column(Text, nullable=True)

    # ── Critic output ──
    quality_approved = Column(Boolean, nullable=True)
    quality_feedback = Column(Text, nullable=True)

    # ── Scheduler output ──
    send_time = Column(String(255), nullable=True)
    follow_up_sequence = Column(ARRAY(Text), nullable=True)

    # ── Operator workflow ──
    # 'pending' (default) | 'approved' | 'rejected' | 'sent'
    approval_status = Column(String(50), nullable=False, default="pending", index=True)
    approval_notes = Column(Text, nullable=True)

    # ── Timestamps ──
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
    sent_at = Column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("ix_research_history_status_created", "approval_status", "created_at"),
    )

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "lead_id": self.lead_id,
            "existing_hotel_id": self.existing_hotel_id,
            "lead_contact_id": self.lead_contact_id,
            "contact_name": self.contact_name,
            "contact_title": self.contact_title,
            "hotel_name": self.hotel_name,
            "hotel_location": self.hotel_location,
            "linkedin_url": self.linkedin_url,
            "email": self.email,
            "company_summary": self.company_summary,
            "contact_summary": self.contact_summary,
            "pain_points": list(self.pain_points or []),
            "signals": list(self.signals or []),
            "outreach_angle": self.outreach_angle,
            "personalization_hook": self.personalization_hook,
            "hotel_tier": self.hotel_tier,
            "hiring_signals": list(self.hiring_signals or []),
            "recent_news": list(self.recent_news or []),
            "fit_score": self.fit_score,
            "value_props": list(self.value_props or []),
            "email_subject": self.email_subject,
            "email_body": self.email_body,
            "linkedin_message": self.linkedin_message,
            "quality_approved": self.quality_approved,
            "quality_feedback": self.quality_feedback,
            "send_time": self.send_time,
            "follow_up_sequence": list(self.follow_up_sequence or []),
            "approval_status": self.approval_status,
            "approval_notes": self.approval_notes,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "sent_at": self.sent_at.isoformat() if self.sent_at else None,
        }

    def __repr__(self) -> str:
        return (
            f"<ResearchHistory id={self.id} "
            f"contact={self.contact_name!r} status={self.approval_status}>"
        )
