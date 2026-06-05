"""Relationship intelligence: when a contact surfaces on a new lead, find
every place we already know this person from — the inbox archive (real
correspondence) and other accounts in the pipeline. The sales gold mine:

    "Maria Lopez, incoming GM at <new hotel> — we exchanged 47 emails with
     her when she ran purchasing at Loews Miami Beach (last: 2026-03-02)."

Match tiers (strongest first):
  email-exact   same address in the archive / on another account — certainty
  name-match    same first+last on a DIFFERENT email/account — the job-change
                signal; surfaced as "verify", never auto-claimed

Read-time only: no migrations, no pipeline writes. Designed to back
GET /api/dashboard/leads/{id}/contacts/{cid}/relationships and the bulk
per-lead endpoint.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


def _split_name(name: str) -> tuple[str, str]:
    parts = [p for p in re.split(r"\s+", (name or "").strip()) if p]
    if len(parts) < 2:
        return "", ""
    return parts[0].lower(), " ".join(parts[1:]).lower()


def _fmt_last(dt) -> str:
    try:
        return f", last {dt:%Y-%m-%d}" if dt else ""
    except Exception:
        return ""


async def find_known_relationships(
    db: AsyncSession,
    *,
    name: str | None,
    email: str | None,
    exclude_lead_id: int | None = None,
    exclude_hotel_id: int | None = None,
    exclude_lead_contact_id: int | None = None,
) -> list[dict[str, Any]]:
    """All known-relationship hits for a person, strongest evidence first."""
    hits: list[dict[str, Any]] = []
    email_l = (email or "").strip().lower()
    fn, ln = _split_name(name or "")

    # ── Tier 1: inbox archive, exact email ──────────────────────────
    if email_l:
        rows = (
            (
                await db.execute(
                    text(
                        "SELECT id, display_name, title, organization, "
                        "parent_company, management_company, interaction_count, "
                        "last_seen, matched_lead_id, matched_hotel_id "
                        "FROM contacts "
                        "WHERE LOWER(email) = :em "
                        "AND contact_category IS DISTINCT FROM 'junk'"
                    ),
                    {"em": email_l},
                )
            )
            .mappings()
            .all()
        )
        for r in rows:
            n = r["interaction_count"] or 0
            org = r["organization"] or r["parent_company"] or "unknown org"
            hits.append(
                {
                    "strength": "email-exact",
                    "source": "inbox_archive",
                    "contact_id": r["id"],
                    "person": r["display_name"],
                    "title": r["title"],
                    "organization": r["organization"],
                    "parent_company": r["parent_company"],
                    "interaction_count": n,
                    "last_seen": r["last_seen"].isoformat() if r["last_seen"] else None,
                    "summary": (
                        f"In our email archive: {n} message(s) with this exact "
                        f"address at '{org}'{_fmt_last(r['last_seen'])}."
                    ),
                }
            )

    # ── Tier 1b: other accounts in the pipeline, exact email ────────
    if email_l:
        rows = (
            (
                await db.execute(
                    text(
                        "SELECT lc.id, lc.name, lc.title, lc.is_saved, "
                        "lc.lead_id, lc.existing_hotel_id, "
                        "pl.hotel_name AS lead_name, eh.hotel_name AS hotel_name "
                        "FROM lead_contacts lc "
                        "LEFT JOIN potential_leads pl ON pl.id = lc.lead_id "
                        "LEFT JOIN existing_hotels eh ON eh.id = lc.existing_hotel_id "
                        "WHERE (LOWER(lc.email) = :em OR LOWER(lc.secondary_email) = :em) "
                        "AND lc.id IS DISTINCT FROM :self_id "
                        "AND (lc.lead_id IS NULL OR lc.lead_id IS DISTINCT FROM :xl) "
                        "AND (lc.existing_hotel_id IS NULL "
                        "     OR lc.existing_hotel_id IS DISTINCT FROM :xh)"
                    ),
                    {
                        "em": email_l,
                        "self_id": exclude_lead_contact_id,
                        "xl": exclude_lead_id,
                        "xh": exclude_hotel_id,
                    },
                )
            )
            .mappings()
            .all()
        )
        for r in rows:
            account = r["lead_name"] or r["hotel_name"] or "another account"
            kind = "lead" if r["lead_id"] else "existing hotel"
            saved = " (saved contact)" if r["is_saved"] else ""
            hits.append(
                {
                    "strength": "email-exact",
                    "source": "pipeline_account",
                    "lead_contact_id": r["id"],
                    "person": r["name"],
                    "title": r["title"],
                    "account": account,
                    "account_kind": kind,
                    "account_lead_id": r["lead_id"],
                    "account_hotel_id": r["existing_hotel_id"],
                    "is_saved": bool(r["is_saved"]),
                    "summary": (
                        f"Already on our {kind} '{account}'{saved} with this " f"exact address."
                    ),
                }
            )

    # ── Tier 2: same name, DIFFERENT email — the job-change signal ──
    if fn and len(ln) >= 3:
        rows = (
            (
                await db.execute(
                    text(
                        "SELECT id, email, display_name, title, organization, "
                        "parent_company, interaction_count, last_seen "
                        "FROM contacts "
                        "WHERE LOWER(first_name) = :fn AND LOWER(last_name) = :ln "
                        "AND LOWER(email) IS DISTINCT FROM :em "
                        "AND contact_category IS DISTINCT FROM 'junk' "
                        "LIMIT 5"
                    ),
                    {"fn": fn, "ln": ln, "em": email_l},
                )
            )
            .mappings()
            .all()
        )
        for r in rows:
            n = r["interaction_count"] or 0
            org = r["organization"] or r["parent_company"] or "unknown org"
            hits.append(
                {
                    "strength": "name-match",
                    "source": "inbox_archive",
                    "contact_id": r["id"],
                    "person": r["display_name"],
                    "title": r["title"],
                    "organization": r["organization"],
                    "known_email": r["email"],
                    "interaction_count": n,
                    "last_seen": r["last_seen"].isoformat() if r["last_seen"] else None,
                    "summary": (
                        f"Same name in our archive at '{org}' "
                        f"({r['email']}, {n} message(s)"
                        f"{_fmt_last(r['last_seen'])}) — possible job change, "
                        f"verify it's the same person."
                    ),
                }
            )

        rows = (
            (
                await db.execute(
                    text(
                        "SELECT lc.id, lc.name, lc.email, lc.title, lc.is_saved, "
                        "lc.lead_id, lc.existing_hotel_id, "
                        "pl.hotel_name AS lead_name, eh.hotel_name AS hotel_name "
                        "FROM lead_contacts lc "
                        "LEFT JOIN potential_leads pl ON pl.id = lc.lead_id "
                        "LEFT JOIN existing_hotels eh ON eh.id = lc.existing_hotel_id "
                        "WHERE LOWER(lc.name) = :full "
                        "AND LOWER(COALESCE(lc.email,'')) IS DISTINCT FROM :em "
                        "AND lc.id IS DISTINCT FROM :self_id "
                        "AND (lc.lead_id IS NULL OR lc.lead_id IS DISTINCT FROM :xl) "
                        "AND (lc.existing_hotel_id IS NULL "
                        "     OR lc.existing_hotel_id IS DISTINCT FROM :xh) "
                        "LIMIT 5"
                    ),
                    {
                        "full": f"{fn} {ln}",
                        "em": email_l,
                        "self_id": exclude_lead_contact_id,
                        "xl": exclude_lead_id,
                        "xh": exclude_hotel_id,
                    },
                )
            )
            .mappings()
            .all()
        )
        for r in rows:
            account = r["lead_name"] or r["hotel_name"] or "another account"
            kind = "lead" if r["lead_id"] else "existing hotel"
            hits.append(
                {
                    "strength": "name-match",
                    "source": "pipeline_account",
                    "lead_contact_id": r["id"],
                    "person": r["name"],
                    "title": r["title"],
                    "account": account,
                    "account_kind": kind,
                    "account_lead_id": r["lead_id"],
                    "account_hotel_id": r["existing_hotel_id"],
                    "is_saved": bool(r["is_saved"]),
                    "summary": (
                        f"Same name on our {kind} '{account}'"
                        f"{' (saved)' if r['is_saved'] else ''} under a "
                        f"different email — possible job change, verify."
                    ),
                }
            )

    order = {"email-exact": 0, "name-match": 1}
    hits.sort(key=lambda h: order.get(h["strength"], 9))
    return hits
