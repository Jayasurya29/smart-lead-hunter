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

import re as _re

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


# ═════════════════════════════════════════════════════════════════════════════
# Tier 0: deterministic PATTERN rules (2026-07-06, built from the real 32k
# export). Runs before any LLM. Three conservative classes only — the ambiguous
# one-email long tail is deliberately left for the classifier/human:
#   machine locals        -> junk        (hex/uuid blobs, sourcing+hash, digit IDs)
#   automated locals      -> junk        (noreply/notifications/newsletter/...)
#   SaaS/carrier domains  -> junk        (ups/dhl/adp/intuit/docusign/... ANCHORED:
#                                         'stripe.com' must not catch 'pinstripes.com')
#   role-inbox locals     -> operational (sales@/admin@/accounting@/ap@/hr@/...;
#                                         purchasing@/procurement@ EXCLUDED — buying
#                                         inboxes stay buyer by policy)
# Human overrides always win: rows with manual_category set are never touched.
# ═════════════════════════════════════════════════════════════════════════════


_MACHINE_LOCAL = _re.compile(
    r"^(?:"
    r"[a-f0-9]{12,}"
    r"|\d{6,}.*"
    r"|.{30,}"
    r"|(?:bounce|prvs=|msprvs|btv1|srs0|srs1)\S*"
    r"|\S+\+[a-f0-9]{10,}"
    r")$"
)

_AUTOMATED_LOCAL = _re.compile(
    r"^(?:no-?reply|donotreply|do-not-reply|notifications?|alerts?|newsletters?"
    r"|marketing|mailer(?:-daemon)?|bounces?|auto(?:mated|reply)?|system"
    r"|updates?|digest|confirm(?:ations?)?|receipts?|broadcast"
    r"|campaigns?|promo(?:tions?)?|unsubscribe|listserv|majordomo|postmaster"
    r"|webmaster|daemon)"
    r"(?:[._\-\d].*)?$"
)

_JUNK_DOMAINS_STATIC = {
    # NOTE: carriers (ups/dhl/fedex/usps) are deliberately NOT here — their
    # staff (aviation purchasing, ops) are uniform BUYERS; carrier notification
    # noise comes from automated locals, which the local rules catch.
    "adp.com",
    "paychex.com",
    "gusto.com",
    "paylocity.com",
    "ukg.com",
    "bamboohr.com",
    "workday.com",
    "intuit.com",
    "quickbooks.com",
    "bill.com",
    "avidbill.com",
    "docusign.com",
    "docusign.net",
    "stripe.com",
    "paypal.com",
    "squareup.com",
    "shopify.com",
    "concursolutions.com",
    "expensify.com",
    "mailchimp.com",
    "mailchimpapp.com",
    "sendgrid.net",
    "sendgrid.com",
    "constantcontact.com",
    "klaviyo.com",
    "hubspot.com",
    "hubspotemail.net",
    "salesforce.com",
    "marketo.com",
    "pardot.com",
    "braze.com",
    "zendesk.com",
    "freshdesk.com",
    "surveymonkey.com",
    "typeform.com",
    "eventbrite.com",
    "calendly.com",
    "zoominfo.com",
    "apollo.io",
    "linkedin.com",
    "facebookmail.com",
    "amazonses.com",
    "mandrillapp.com",
    "postmarkapp.com",
    "mailgun.org",
    "mailgun.net",
    "iterable.com",
    "glassdoor.com",
    "indeed.com",
    "ziprecruiter.com",
}

_OPERATIONAL_LOCAL = _re.compile(
    r"^(?:sales|admin|accounting|accounts?|ap|ar|payroll|billing|invoices?"
    r"|orders?|office|frontdesk|reception|reservations?|bookings?|events?"
    r"|catering|banquets?|hr|humanresources|careers?|jobs|recruiting"
    r"|helpdesk|support|service|customerservice|customercare|feedback"
    r"|info|contact|hello|team|mail|email|enquir(?:y|ies)|inquir(?:y|ies)"
    r"|security|facilities|maintenance|housekeeping|engineering|it"
    r"|press|media|pr|concierge|guestservices?|frontoffice)"
    r"(?:[._\-\d].*)?$"
)


def _registrable(domain: str) -> str:
    parts = (domain or "").lower().strip().split(".")
    return ".".join(parts[-2:]) if len(parts) >= 2 else (domain or "").lower()


def classify_by_rule(email: str) -> Optional[str]:
    """'junk' | 'operational' | None — deterministic, no DB, no LLM."""
    em = (email or "").strip().lower()
    if "@" not in em:
        return None
    local, _, domain = em.partition("@")
    if _registrable(domain) in _JUNK_DOMAINS_STATIC or domain in _JUNK_DOMAINS_STATIC:
        return "junk"
    if _MACHINE_LOCAL.match(local):
        return "junk"
    if _AUTOMATED_LOCAL.match(local):
        return "junk"
    if _OPERATIONAL_LOCAL.match(local):
        return "operational"
    return None


async def apply_junk_rules(dry_run: bool = True, limit: int = 200_000) -> dict:
    """Backfill existing contacts through the pattern rules.

    Skips rows with manual_category (human wins) and rows already
    junk/operational. Writes contact_category + category_source='rule'.
    """
    from app.database import async_session

    stats: dict = {"scanned": 0, "junk": 0, "operational": 0, "dry_run": dry_run, "samples": []}
    async with async_session() as session:
        rows = (
            await session.execute(
                text(
                    "SELECT id, email FROM contacts "
                    "WHERE manual_category IS NULL "
                    "AND COALESCE(contact_category,'') NOT IN ('junk','operational') "
                    "ORDER BY id LIMIT :lim"
                ),
                {"lim": limit},
            )
        ).all()
        to_junk: list[int] = []
        to_oper: list[int] = []
        for r in rows:
            stats["scanned"] += 1
            v = classify_by_rule(r.email or "")
            if v == "junk":
                to_junk.append(r.id)
                if len(stats["samples"]) < 30:
                    stats["samples"].append(f"junk: {r.email}")
            elif v == "operational":
                to_oper.append(r.id)
                if len(stats["samples"]) < 30:
                    stats["samples"].append(f"oper: {r.email}")
        stats["junk"] = len(to_junk)
        stats["operational"] = len(to_oper)
        if not dry_run:
            if to_junk:
                await session.execute(
                    text(
                        "UPDATE contacts SET contact_category='junk', "
                        "category_source='rule', updated_at=now() WHERE id = ANY(:ids)"
                    ),
                    {"ids": to_junk},
                )
            if to_oper:
                await session.execute(
                    text(
                        "UPDATE contacts SET contact_category='operational', "
                        "category_source='rule', updated_at=now() WHERE id = ANY(:ids)"
                    ),
                    {"ids": to_oper},
                )
            await session.commit()
    return stats
