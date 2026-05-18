"""app/services/contact_dedup.py

UPSERT + smart-merge logic for the `contacts` table, plus read/state-change
helpers used by the API routes (Phase 3).

Design rules:
  - email is the natural dedup key (UNIQUE constraint on contacts.email)
  - Never overwrite a non-null field with a null/empty value
  - interaction_count increments by `interaction_increment` (default 1) so
    callers can record real per-run interaction deltas from the Gmail header
    scan rather than always counting "+1 per sync".
  - Always update last_seen
  - source_mailboxes is a Postgres ARRAY — append without duplicating
  - sync_history is JSONB — append a small event record (capped at 50 entries)
  - procurement_priority and brand enrichment always refresh (they come from
    BrandRegistry which may improve over time)
  - approval_status: never downgrade (pending → approved is fine; approved →
    pending is not). Reject = hard delete (no 'rejected' state).

FIX 2026-05-18: All ``:param::type`` casts replaced with
``CAST(:param AS type)`` — asyncpg translates named params to positional
``$N`` and chokes on the ``::`` shorthand (it sees ``:param::jsonb`` as
two separate named params).

FIX 2026-05-18: bulk_upsert_contacts uses a SAVEPOINT per contact so a
single failure doesn't poison the transaction for all remaining rows.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

# Maximum number of sync_history entries to keep per contact.
SYNC_HISTORY_MAX = 50

# Valid approval_status values (matches CHECK constraint in migration 025)
VALID_APPROVAL_STATUSES = ("pending", "approved", "pushed_to_insightly")

# Valid procurement_priority values (matches CHECK constraint)
VALID_PRIORITIES = ("P1", "P2", "P3", "P4", "P_unknown")


# ──────────────────────────────────────────────────────────────────────
# Internal helpers
# ──────────────────────────────────────────────────────────────────────


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _coerce_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def _make_sync_event(
    action: str,
    source_mailbox: Optional[str],
    ts: datetime,
) -> dict:
    return {
        "action": action,
        "mailbox": source_mailbox,
        "ts": ts.isoformat(),
    }


# ──────────────────────────────────────────────────────────────────────
# UPSERT — the heart of dedup
# ──────────────────────────────────────────────────────────────────────


async def upsert_contact(
    session: AsyncSession,
    *,
    email: str,
    first_name: Optional[str] = None,
    last_name: Optional[str] = None,
    display_name: Optional[str] = None,
    title: Optional[str] = None,
    organization: Optional[str] = None,
    phone: Optional[str] = None,
    address: Optional[str] = None,
    linkedin_url: Optional[str] = None,
    org_source: Optional[str] = None,
    has_signature: bool = False,
    confidence: Optional[float] = None,
    # Hospitality enrichment
    parent_company: Optional[str] = None,
    brand_tier: Optional[str] = None,
    operating_model: Optional[str] = None,
    gpo: Optional[str] = None,
    procurement_priority: str = "P_unknown",
    priority_reason: Optional[str] = None,
    opportunity_level: Optional[str] = None,
    opportunity_score: Optional[float] = None,
    management_company: Optional[str] = None,
    # Interaction tracking
    source_mailbox: Optional[str] = None,
    interaction_increment: int = 1,
    # Pipeline linkage
    matched_lead_id: Optional[int] = None,
    matched_hotel_id: Optional[int] = None,
) -> tuple[str, int]:
    """Insert or merge a contact row.

    Args:
        interaction_increment: How many emails the caller observed this
            contact in during this sync run. On INSERT this becomes the
            initial interaction_count value. On UPDATE this is ADDED to
            the existing count. Default 1.

    Returns:
        (action, contact_id) where action is "inserted" | "updated".
    """
    email = email.lower().strip()
    if not email or "@" not in email:
        raise ValueError(f"Invalid email: {email!r}")

    if interaction_increment < 1:
        interaction_increment = 1

    if procurement_priority not in VALID_PRIORITIES:
        procurement_priority = "P_unknown"

    now = _now_utc()

    # ── Fetch existing row with lock ─────────────────────────────────
    result = await session.execute(
        text("SELECT * FROM contacts WHERE email = :email FOR UPDATE"),
        {"email": email},
    )
    existing = result.mappings().first()

    if existing is None:
        # ── INSERT new row ──────────────────────────────────────────
        sync_event = _make_sync_event("inserted", source_mailbox, now)
        mailboxes = [source_mailbox] if source_mailbox else []

        insert_result = await session.execute(
            text("""
                INSERT INTO contacts (
                    email, first_name, last_name, display_name,
                    title, organization, phone, address, linkedin_url,
                    org_source, has_signature, confidence,
                    parent_company, brand_tier, operating_model, gpo,
                    procurement_priority, priority_reason,
                    opportunity_level, opportunity_score, management_company,
                    interaction_count, source_mailboxes,
                    first_seen, last_seen,
                    approval_status, matched_lead_id, matched_hotel_id,
                    sync_history, created_at, updated_at
                ) VALUES (
                    :email, :first_name, :last_name, :display_name,
                    :title, :organization, :phone, :address, :linkedin_url,
                    :org_source, :has_signature, :confidence,
                    :parent_company, :brand_tier, :operating_model, :gpo,
                    :procurement_priority, :priority_reason,
                    :opportunity_level, :opportunity_score, :management_company,
                    :interaction_count, :source_mailboxes,
                    :now, :now,
                    'pending', :matched_lead_id, :matched_hotel_id,
                    CAST(:sync_history AS jsonb), :now, :now
                )
                RETURNING id
            """),
            {
                "email": email,
                "first_name": _coerce_str(first_name),
                "last_name": _coerce_str(last_name),
                "display_name": _coerce_str(display_name),
                "title": _coerce_str(title),
                "organization": _coerce_str(organization),
                "phone": _coerce_str(phone),
                "address": _coerce_str(address),
                "linkedin_url": _coerce_str(linkedin_url),
                "org_source": _coerce_str(org_source),
                "has_signature": has_signature,
                "confidence": confidence,
                "parent_company": _coerce_str(parent_company),
                "brand_tier": _coerce_str(brand_tier),
                "operating_model": _coerce_str(operating_model),
                "gpo": _coerce_str(gpo),
                "procurement_priority": procurement_priority,
                "priority_reason": _coerce_str(priority_reason),
                "opportunity_level": _coerce_str(opportunity_level),
                "opportunity_score": opportunity_score,
                "management_company": _coerce_str(management_company),
                "interaction_count": interaction_increment,
                "source_mailboxes": mailboxes,
                "now": now,
                "matched_lead_id": matched_lead_id,
                "matched_hotel_id": matched_hotel_id,
                "sync_history": json.dumps([sync_event], default=str),
            },
        )
        contact_id = insert_result.scalar_one()
        logger.debug(
            f"contact_dedup: inserted {email} → id={contact_id} "
            f"(interaction_count={interaction_increment})"
        )
        return "inserted", contact_id

    # ── UPDATE (smart merge) ─────────────────────────────────────────
    contact_id = existing["id"]
    updates: dict[str, Any] = {}

    def _fill(field: str, new_val: Any):
        """Fill-empty rule — only set if existing is null/empty AND new is truthy."""
        if new_val and not existing[field]:
            updates[field] = (
                _coerce_str(new_val) if isinstance(new_val, str) else new_val
            )

    _fill("first_name", first_name)
    _fill("last_name", last_name)
    _fill("display_name", display_name)
    _fill("title", title)
    _fill("organization", organization)
    _fill("phone", phone)
    _fill("address", address)
    _fill("linkedin_url", linkedin_url)
    _fill("org_source", org_source)
    _fill("matched_lead_id", matched_lead_id)
    _fill("matched_hotel_id", matched_hotel_id)

    # has_signature: once true, stays true
    if has_signature and not existing["has_signature"]:
        updates["has_signature"] = True

    # confidence: take the higher value
    if confidence is not None:
        existing_conf = existing["confidence"] or 0.0
        if confidence > existing_conf:
            updates["confidence"] = confidence

    # Hospitality enrichment — always refresh
    updates["procurement_priority"] = procurement_priority
    if priority_reason:
        updates["priority_reason"] = priority_reason
    if parent_company is not None:
        updates["parent_company"] = _coerce_str(parent_company)
    if brand_tier is not None:
        updates["brand_tier"] = _coerce_str(brand_tier)
    if operating_model is not None:
        updates["operating_model"] = _coerce_str(operating_model)
    if gpo is not None:
        updates["gpo"] = _coerce_str(gpo)
    if opportunity_level is not None:
        updates["opportunity_level"] = _coerce_str(opportunity_level)
    if opportunity_score is not None:
        updates["opportunity_score"] = opportunity_score
    if management_company is not None:
        updates["management_company"] = _coerce_str(management_company)

    # Always update
    updates["last_seen"] = now
    updates["updated_at"] = now
    updates["interaction_count"] = (
        existing["interaction_count"] or 0
    ) + interaction_increment

    # source_mailboxes — append without duplicating
    if source_mailbox:
        existing_mailboxes: list[str] = list(existing["source_mailboxes"] or [])
        if source_mailbox not in existing_mailboxes:
            updates["source_mailboxes"] = existing_mailboxes + [source_mailbox]

    # sync_history — append event, cap at SYNC_HISTORY_MAX
    sync_event = _make_sync_event("updated", source_mailbox, now)
    existing_history: list = list(existing["sync_history"] or [])
    existing_history.append(sync_event)
    if len(existing_history) > SYNC_HISTORY_MAX:
        existing_history = existing_history[-SYNC_HISTORY_MAX:]
    updates["sync_history"] = existing_history

    # Build dynamic SET clause
    # FIX 2026-05-18: Use CAST(:param AS type) instead of :param::type
    # because asyncpg's named-param translation chokes on the :: shorthand.
    set_clauses = []
    params: dict[str, Any] = {"contact_id": contact_id}
    for col, val in updates.items():
        param_name = f"p_{col}"
        if col == "sync_history":
            set_clauses.append(f"{col} = CAST(:{param_name} AS jsonb)")
            params[param_name] = json.dumps(val, default=str)
        elif col == "source_mailboxes":
            set_clauses.append(f"{col} = CAST(:{param_name} AS text[])")
            params[param_name] = val
        else:
            set_clauses.append(f"{col} = :{param_name}")
            params[param_name] = val

    sql = f"UPDATE contacts SET {', '.join(set_clauses)} WHERE id = :contact_id"
    await session.execute(text(sql), params)
    logger.debug(
        f"contact_dedup: updated {email} (id={contact_id}) "
        f"+{interaction_increment} interactions"
    )
    return "updated", contact_id


async def bulk_upsert_contacts(
    session: AsyncSession,
    contacts: list[dict],
    source_mailbox: Optional[str] = None,
) -> dict[str, int]:
    """Upsert a batch of contact dicts.

    Each dict's `interaction_count` key (from inbox_sync header counting)
    is passed through as `interaction_increment` so per-run deltas land
    correctly in the DB.

    FIX 2026-05-18: Uses a SAVEPOINT per contact so a single failure
    doesn't poison the transaction for all remaining rows.

    Returns: {"inserted": N, "updated": N, "errors": N}
    """
    inserted = updated = errors = 0
    for c in contacts:
        try:
            async with session.begin_nested():
                mailbox = source_mailbox or c.get("source_mailbox")
                increment = max(1, int(c.get("interaction_count") or 1))
                action, _ = await upsert_contact(
                    session,
                    email=c["email"],
                    first_name=c.get("first_name"),
                    last_name=c.get("last_name"),
                    display_name=c.get("display_name"),
                    title=c.get("title"),
                    organization=c.get("organization"),
                    phone=c.get("phone"),
                    address=c.get("address"),
                    linkedin_url=c.get("linkedin_url"),
                    org_source=c.get("org_source"),
                    has_signature=c.get("has_signature", False),
                    confidence=c.get("confidence"),
                    parent_company=c.get("parent_company"),
                    brand_tier=c.get("brand_tier"),
                    operating_model=c.get("operating_model"),
                    gpo=c.get("gpo"),
                    procurement_priority=c.get("procurement_priority", "P_unknown"),
                    priority_reason=c.get("priority_reason"),
                    opportunity_level=c.get("opportunity_level"),
                    opportunity_score=c.get("opportunity_score"),
                    management_company=c.get("management_company"),
                    source_mailbox=mailbox,
                    interaction_increment=increment,
                    matched_lead_id=c.get("matched_lead_id"),
                    matched_hotel_id=c.get("matched_hotel_id"),
                )
            if action == "inserted":
                inserted += 1
            else:
                updated += 1
        except Exception as e:
            errors += 1
            logger.warning(
                f"contact_dedup: upsert failed for {c.get('email', '?')}: {e}"
            )

    logger.info(
        f"contact_dedup: bulk_upsert done — "
        f"inserted={inserted} updated={updated} errors={errors}"
    )
    return {"inserted": inserted, "updated": updated, "errors": errors}


# ──────────────────────────────────────────────────────────────────────
# READ helpers (used by API routes / UI)
# ──────────────────────────────────────────────────────────────────────


async def get_contact_by_id(
    session: AsyncSession,
    contact_id: int,
) -> Optional[dict]:
    """Fetch one contact row by id. Returns dict or None."""
    result = await session.execute(
        text("SELECT * FROM contacts WHERE id = :id"),
        {"id": contact_id},
    )
    row = result.mappings().first()
    return dict(row) if row else None


async def get_contact_by_email(
    session: AsyncSession,
    email: str,
) -> Optional[dict]:
    """Fetch one contact row by email. Returns dict or None."""
    email = email.lower().strip()
    result = await session.execute(
        text("SELECT * FROM contacts WHERE email = :email"),
        {"email": email},
    )
    row = result.mappings().first()
    return dict(row) if row else None


async def list_contacts(
    session: AsyncSession,
    *,
    procurement_priority: Optional[str] = None,
    approval_status: Optional[str] = None,
    brand_tier: Optional[str] = None,
    gpo: Optional[str] = None,
    source_mailbox: Optional[str] = None,
    has_signature: Optional[bool] = None,
    organization: Optional[str] = None,
    search: Optional[str] = None,
    matched_only: Optional[bool] = None,
    limit: int = 100,
    offset: int = 0,
    order_by: str = "priority_score",
) -> tuple[list[dict], int]:
    """List contacts with filters.

    Filter args (all optional):
        procurement_priority: 'P1' / 'P2' / 'P3' / 'P4' / 'P_unknown'
        approval_status: 'pending' / 'approved' / 'pushed_to_insightly'
        brand_tier, gpo: exact match
        source_mailbox: matches any in source_mailboxes[] array
        has_signature: True | False
        organization: ILIKE wildcard
        search: ILIKE across email/name/org/title
        matched_only: True = has matched_lead_id or matched_hotel_id,
                      False = neither
        order_by: 'priority_score' | 'last_seen' | 'first_seen' | 'name'

    Returns: (rows, total_count) — total_count is BEFORE limit/offset.
    """
    where_clauses: list[str] = []
    params: dict[str, Any] = {}

    if procurement_priority:
        if procurement_priority not in VALID_PRIORITIES:
            raise ValueError(f"Invalid priority: {procurement_priority}")
        where_clauses.append("procurement_priority = :priority")
        params["priority"] = procurement_priority

    if approval_status:
        if approval_status not in VALID_APPROVAL_STATUSES:
            raise ValueError(f"Invalid status: {approval_status}")
        where_clauses.append("approval_status = :status")
        params["status"] = approval_status

    if brand_tier:
        where_clauses.append("brand_tier = :brand_tier")
        params["brand_tier"] = brand_tier

    if gpo:
        where_clauses.append("gpo = :gpo")
        params["gpo"] = gpo

    if source_mailbox:
        where_clauses.append(":mailbox = ANY(source_mailboxes)")
        params["mailbox"] = source_mailbox

    if has_signature is not None:
        where_clauses.append("has_signature = :has_sig")
        params["has_sig"] = has_signature

    if organization:
        where_clauses.append("organization ILIKE :org_pattern")
        params["org_pattern"] = f"%{organization}%"

    if search:
        where_clauses.append(
            "(email ILIKE :search OR "
            "COALESCE(first_name, '') ILIKE :search OR "
            "COALESCE(last_name, '') ILIKE :search OR "
            "COALESCE(display_name, '') ILIKE :search OR "
            "COALESCE(organization, '') ILIKE :search OR "
            "COALESCE(title, '') ILIKE :search)"
        )
        params["search"] = f"%{search}%"

    if matched_only is True:
        where_clauses.append(
            "(matched_lead_id IS NOT NULL OR matched_hotel_id IS NOT NULL)"
        )
    elif matched_only is False:
        where_clauses.append("matched_lead_id IS NULL AND matched_hotel_id IS NULL")

    where_sql = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    # Total count for pagination
    count_result = await session.execute(
        text(f"SELECT COUNT(*) FROM contacts{where_sql}"),
        params,
    )
    total = count_result.scalar_one()

    # Order clause
    if order_by == "priority_score":
        order_sql = """
            ORDER BY
                CASE procurement_priority
                    WHEN 'P1' THEN 1
                    WHEN 'P2' THEN 2
                    WHEN 'P3' THEN 3
                    WHEN 'P_unknown' THEN 4
                    WHEN 'P4' THEN 5
                    ELSE 6
                END,
                opportunity_score DESC NULLS LAST,
                last_seen DESC NULLS LAST
        """
    elif order_by == "last_seen":
        order_sql = "ORDER BY last_seen DESC NULLS LAST"
    elif order_by == "first_seen":
        order_sql = "ORDER BY first_seen DESC NULLS LAST"
    elif order_by == "name":
        order_sql = "ORDER BY last_name NULLS LAST, first_name NULLS LAST, email"
    else:
        raise ValueError(f"Invalid order_by: {order_by}")

    params["limit"] = limit
    params["offset"] = offset

    rows_result = await session.execute(
        text(
            f"SELECT * FROM contacts{where_sql} {order_sql} LIMIT :limit OFFSET :offset"
        ),
        params,
    )
    rows = [dict(r) for r in rows_result.mappings().all()]
    return rows, total


# ──────────────────────────────────────────────────────────────────────
# STATE-CHANGE helpers (approve, reject/delete, push, link)
# ──────────────────────────────────────────────────────────────────────


async def update_approval_status(
    session: AsyncSession,
    contact_id: int,
    new_status: str,
) -> Optional[dict]:
    """Change a contact's approval_status.

    Returns updated contact dict, or None if contact not found.
    Raises ValueError for invalid status.
    """
    if new_status not in VALID_APPROVAL_STATUSES:
        raise ValueError(
            f"Invalid status: {new_status!r} "
            f"(must be one of {VALID_APPROVAL_STATUSES})"
        )

    now = _now_utc()
    sync_event = _make_sync_event(f"status_changed:{new_status}", None, now)

    result = await session.execute(
        text("""
            UPDATE contacts
            SET approval_status = :status,
                updated_at = :now,
                sync_history = COALESCE(sync_history, '[]'::jsonb) || CAST(:event AS jsonb)
            WHERE id = :id
            RETURNING *
        """),
        {
            "id": contact_id,
            "status": new_status,
            "now": now,
            "event": json.dumps([sync_event], default=str),
        },
    )
    row = result.mappings().first()
    if not row:
        return None
    logger.info(f"contact_dedup: contact #{contact_id} → status={new_status}")
    return dict(row)


async def delete_contact(
    session: AsyncSession,
    contact_id: int,
) -> bool:
    """Hard delete a contact by id. This is what 'Reject' does in the UI.

    Returns True if a row was deleted, False if it didn't exist.
    """
    result = await session.execute(
        text("DELETE FROM contacts WHERE id = :id RETURNING id"),
        {"id": contact_id},
    )
    deleted_id = result.scalar()
    if deleted_id:
        logger.info(f"contact_dedup: deleted contact #{contact_id}")
        return True
    return False


async def mark_pushed_to_insightly(
    session: AsyncSession,
    contact_id: int,
    insightly_contact_id: str,
) -> Optional[dict]:
    """After a successful push to Insightly, stamp the contact.

    Sets approval_status='pushed_to_insightly', insightly_contact_id,
    and pushed_to_insightly_at. Returns updated dict or None.
    """
    now = _now_utc()
    sync_event = _make_sync_event(
        f"pushed_to_insightly:{insightly_contact_id}",
        None,
        now,
    )

    result = await session.execute(
        text("""
            UPDATE contacts
            SET approval_status = 'pushed_to_insightly',
                insightly_contact_id = :insightly_id,
                pushed_to_insightly_at = :now,
                updated_at = :now,
                sync_history = COALESCE(sync_history, '[]'::jsonb) || CAST(:event AS jsonb)
            WHERE id = :id
            RETURNING *
        """),
        {
            "id": contact_id,
            "insightly_id": insightly_contact_id,
            "now": now,
            "event": json.dumps([sync_event], default=str),
        },
    )
    row = result.mappings().first()
    if not row:
        return None
    logger.info(
        f"contact_dedup: contact #{contact_id} pushed to Insightly "
        f"(insightly_id={insightly_contact_id})"
    )
    return dict(row)


async def link_to_lead(
    session: AsyncSession,
    contact_id: int,
    lead_id: Optional[int],
) -> Optional[dict]:
    """Link a contact to a potential_leads row, or unlink (lead_id=None)."""
    now = _now_utc()
    action = f"linked_to_lead:{lead_id}" if lead_id else "unlinked_from_lead"
    sync_event = _make_sync_event(action, None, now)

    result = await session.execute(
        text("""
            UPDATE contacts
            SET matched_lead_id = :lead_id,
                updated_at = :now,
                sync_history = COALESCE(sync_history, '[]'::jsonb) || CAST(:event AS jsonb)
            WHERE id = :id
            RETURNING *
        """),
        {
            "id": contact_id,
            "lead_id": lead_id,
            "now": now,
            "event": json.dumps([sync_event], default=str),
        },
    )
    row = result.mappings().first()
    return dict(row) if row else None


async def link_to_hotel(
    session: AsyncSession,
    contact_id: int,
    hotel_id: Optional[int],
) -> Optional[dict]:
    """Link a contact to an existing_hotels row, or unlink (hotel_id=None)."""
    now = _now_utc()
    action = f"linked_to_hotel:{hotel_id}" if hotel_id else "unlinked_from_hotel"
    sync_event = _make_sync_event(action, None, now)

    result = await session.execute(
        text("""
            UPDATE contacts
            SET matched_hotel_id = :hotel_id,
                updated_at = :now,
                sync_history = COALESCE(sync_history, '[]'::jsonb) || CAST(:event AS jsonb)
            WHERE id = :id
            RETURNING *
        """),
        {
            "id": contact_id,
            "hotel_id": hotel_id,
            "now": now,
            "event": json.dumps([sync_event], default=str),
        },
    )
    row = result.mappings().first()
    return dict(row) if row else None


# ──────────────────────────────────────────────────────────────────────
# STATS helpers (for UI header bar)
# ──────────────────────────────────────────────────────────────────────


async def get_contact_stats(session: AsyncSession) -> dict:
    """Return counts for the Contacts page header bar.

    Example return:
    {
      'total': 1035, 'p1': 43, 'p2': 50, 'p3': 30, 'p4': 9,
      'p_unknown': 903, 'pending': 1035, 'approved': 0,
      'pushed_to_insightly': 0, 'new_today': 27,
      'with_signature': 531, 'with_phone': 382,
      'last_sync_at': '2026-05-14T09:45:13+00:00',
    }
    """
    result = await session.execute(
        text("""
            SELECT
                COUNT(*)::int                                                            AS total,
                COUNT(*) FILTER (WHERE procurement_priority = 'P1')::int                AS p1,
                COUNT(*) FILTER (WHERE procurement_priority = 'P2')::int                AS p2,
                COUNT(*) FILTER (WHERE procurement_priority = 'P3')::int                AS p3,
                COUNT(*) FILTER (WHERE procurement_priority = 'P4')::int                AS p4,
                COUNT(*) FILTER (WHERE procurement_priority = 'P_unknown')::int         AS p_unknown,
                COUNT(*) FILTER (WHERE approval_status = 'pending')::int                AS pending,
                COUNT(*) FILTER (WHERE approval_status = 'approved')::int               AS approved,
                COUNT(*) FILTER (WHERE approval_status = 'pushed_to_insightly')::int    AS pushed_to_insightly,
                COUNT(*) FILTER (WHERE first_seen >= NOW() - INTERVAL '24 hours')::int  AS new_today,
                COUNT(*) FILTER (WHERE has_signature = TRUE)::int                       AS with_signature,
                COUNT(*) FILTER (WHERE phone IS NOT NULL)::int                          AS with_phone
            FROM contacts
        """)
    )
    row = result.mappings().first() or {}

    sync_result = await session.execute(
        text("SELECT MAX(last_synced_at) AS last_sync FROM mailbox_sync_state")
    )
    last_sync = sync_result.scalar()

    return {
        "total": row.get("total", 0),
        "p1": row.get("p1", 0),
        "p2": row.get("p2", 0),
        "p3": row.get("p3", 0),
        "p4": row.get("p4", 0),
        "p_unknown": row.get("p_unknown", 0),
        "pending": row.get("pending", 0),
        "approved": row.get("approved", 0),
        "pushed_to_insightly": row.get("pushed_to_insightly", 0),
        "new_today": row.get("new_today", 0),
        "with_signature": row.get("with_signature", 0),
        "with_phone": row.get("with_phone", 0),
        "last_sync_at": last_sync.isoformat() if last_sync else None,
    }
