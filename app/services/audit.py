"""
SMART LEAD HUNTER — Audit Service
===================================
Simple helper to write audit log entries from any route.

Usage:
    from app.services.audit import log_action

    await log_action(
        session=db,
        action="approve",
        lead=lead,
        user_email="nico@jauniforms.com",
        detail="CRM push: 3 contacts",
    )

    # For edits with before/after:
    await log_action(
        session=db,
        action="edit",
        lead=lead,
        user_email=user["email"],
        old_values={"lead_score": 65, "brand_tier": "tier3_upper_upscale"},
        new_values={"lead_score": 80, "brand_tier": "tier2_luxury"},
    )
"""

import logging
from typing import Optional

from sqlalchemy.ext.asyncio import AsyncSession

from app.models.audit_log import AuditLog

logger = logging.getLogger(__name__)


async def log_action(
    session: AsyncSession,
    action: str,
    lead=None,
    lead_id: Optional[int] = None,
    hotel_name: Optional[str] = None,
    user_email: str = "system",
    user_id: Optional[int] = None,
    old_values: Optional[dict] = None,
    new_values: Optional[dict] = None,
    detail: Optional[str] = None,
) -> None:
    """Write an audit log entry. Never raises — logs errors instead."""
    try:
        entry = AuditLog(
            user_email=user_email,
            user_id=user_id,
            action=action,
            lead_id=lead_id or (lead.id if lead else None),
            hotel_name=hotel_name or (lead.hotel_name if lead else None),
            old_values=old_values or {},
            new_values=new_values or {},
            detail=detail,
        )
        session.add(entry)
        # Don't commit here — let the caller's commit include this
    except Exception as e:
        # Never break the main operation because of audit logging
        logger.error(f"Audit log failed: {e}")
