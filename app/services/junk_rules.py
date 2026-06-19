"""
app/services/junk_rules.py
==========================
The learning junk system's data layer.

Two levels, both reversible, neither touched by the classifier/sync:

  manual_category  — a rep's override on ONE contact. Wins over the AI category
                     everywhere (effective category = COALESCE(manual_category,
                     contact_category)). 'junk' to dismiss, NULL to restore.

  junk_domains     — rep-curated domains that auto-junk. Pass 1 of run_tier1
                     consults `load_junk_domains()` / `is_junk_domain()` and
                     resolves matches to junk with no LLM call. Adding a domain
                     also flips every existing contact from it to manual junk;
                     removing it clears those overrides.

Junk never infiltrates the real contact count — the count uses the effective
category, so a manual or domain junk drops out immediately.
"""

from __future__ import annotations

from typing import Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


def _domain_of(email: str) -> str:
    e = (email or "").strip().lower()
    return e.split("@", 1)[1] if "@" in e else ""


# ── domain-set loader + check (used by the classifier Pass 1) ──────────────
async def load_junk_domains(session: AsyncSession) -> set[str]:
    rows = (await session.execute(text("SELECT domain FROM junk_domains"))).all()
    return {r[0] for r in rows if r[0]}


def is_junk_domain(email: str, junk_set: set[str]) -> bool:
    d = _domain_of(email)
    return bool(d) and d in junk_set


# ── per-contact manual junk / restore ──────────────────────────────────────
async def junk_contact(session: AsyncSession, contact_id: int) -> bool:
    res = await session.execute(
        text(
            "UPDATE contacts SET manual_category = 'junk', manual_category_at = now() "
            "WHERE id = :id"
        ),
        {"id": contact_id},
    )
    await session.commit()
    return (res.rowcount or 0) > 0


async def unjunk_contact(session: AsyncSession, contact_id: int) -> bool:
    res = await session.execute(
        text(
            "UPDATE contacts SET manual_category = NULL, manual_category_at = NULL "
            "WHERE id = :id"
        ),
        {"id": contact_id},
    )
    await session.commit()
    return (res.rowcount or 0) > 0


async def junk_contacts_bulk(session: AsyncSession, contact_ids: list[int]) -> int:
    if not contact_ids:
        return 0
    res = await session.execute(
        text(
            "UPDATE contacts SET manual_category = 'junk', manual_category_at = now() "
            "WHERE id = ANY(:ids)"
        ),
        {"ids": contact_ids},
    )
    await session.commit()
    return res.rowcount or 0


# ── domain-level junk / restore (the learning part) ─────────────────────────
async def junk_domain(
    session: AsyncSession,
    domain: str,
    *,
    added_by: Optional[str] = None,
    reason: Optional[str] = None,
) -> dict:
    """Add a domain to the auto-junk list AND flip its existing contacts to junk."""
    d = (domain or "").strip().lower().lstrip("@")
    if not d or "." not in d:
        raise ValueError(f"not a valid domain: {domain!r}")
    # how many existing contacts this affects (for the audit + UI feedback)
    n = (
        await session.execute(
            text("SELECT count(*) FROM contacts WHERE lower(split_part(email,'@',2)) = :d"),
            {"d": d},
        )
    ).scalar() or 0
    await session.execute(
        text(
            "INSERT INTO junk_domains (domain, added_by, reason, contacts_at_add) "
            "VALUES (:d, :by, :rsn, :n) "
            "ON CONFLICT (domain) DO UPDATE SET added_by = EXCLUDED.added_by, "
            "reason = EXCLUDED.reason, contacts_at_add = EXCLUDED.contacts_at_add"
        ),
        {"d": d, "by": added_by, "rsn": reason, "n": n},
    )
    flipped = await session.execute(
        text(
            "UPDATE contacts SET manual_category = 'junk', manual_category_at = now() "
            "WHERE lower(split_part(email,'@',2)) = :d "
            "AND (manual_category IS NULL OR manual_category <> 'junk')"
        ),
        {"d": d},
    )
    await session.commit()
    return {"domain": d, "existing_contacts": n, "flipped_to_junk": flipped.rowcount or 0}


async def unjunk_domain(session: AsyncSession, domain: str) -> dict:
    """Remove a domain from auto-junk AND release its contacts:
    clear the manual junk override, and reset rows the Pass-1 rule auto-junked
    (category_source='junk_domain') so they re-classify on the next run."""
    d = (domain or "").strip().lower().lstrip("@")
    await session.execute(text("DELETE FROM junk_domains WHERE domain = :d"), {"d": d})
    restored = await session.execute(
        text(
            "UPDATE contacts SET manual_category = NULL, manual_category_at = NULL "
            "WHERE lower(split_part(email,'@',2)) = :d AND manual_category = 'junk'"
        ),
        {"d": d},
    )
    await session.execute(
        text(
            "UPDATE contacts SET contact_category = NULL, category_source = NULL, "
            "enriched_at = NULL "
            "WHERE lower(split_part(email,'@',2)) = :d AND category_source = 'junk_domain'"
        ),
        {"d": d},
    )
    await session.commit()
    return {"domain": d, "restored": restored.rowcount or 0}


async def list_junk_domains(session: AsyncSession) -> list[dict]:
    rows = (
        (
            await session.execute(
                text(
                    "SELECT domain, added_at, added_by, reason, contacts_at_add "
                    "FROM junk_domains ORDER BY added_at DESC"
                )
            )
        )
        .mappings()
        .all()
    )
    return [dict(r) for r in rows]


# ── Tier 3: suggest domains the rep keeps junking by hand ───────────────────
async def junk_domain_suggestions(session: AsyncSession, threshold: int = 3) -> list[dict]:
    """Domains where the rep has manually junked >= threshold contacts and which
    are NOT already auto-junked — offer one-click 'junk the whole domain'."""
    rows = (
        (
            await session.execute(
                text(
                    "SELECT lower(split_part(email,'@',2)) AS domain, count(*) AS manual_junked "
                    "FROM contacts "
                    "WHERE manual_category = 'junk' "
                    "AND lower(split_part(email,'@',2)) NOT IN (SELECT domain FROM junk_domains) "
                    "GROUP BY 1 HAVING count(*) >= :t ORDER BY 2 DESC"
                ),
                {"t": threshold},
            )
        )
        .mappings()
        .all()
    )
    return [dict(r) for r in rows if r["domain"]]
