"""role_learning.py — learn buyer ROLE knowledge from observed buying behavior.

When the Buying Signal Engine labels a contact `buyer_evidence` (we can SEE
them purchasing — approving quotes, placing orders), their job title is real
buyer-role knowledge. This service records that title into the contact_roles
dictionary (source='learned', is_relevant=true) so the classifier recognizes
the SAME role on future contacts automatically.

Deliberately conservative:
  - fires only on the strongest label (buyer_evidence), not mere buying verbs
  - skips role inboxes (ap@/accounting@/invoice@ ...) — a mailbox isn't a role
  - skips empty/one-word-junk titles
  - idempotent: existing normalized role -> contact_count += 1 (evidence
    accumulates), never duplicated
Priority is set from the canonical sap_title_classifier tier when it knows the
title; otherwise stays P_unknown for the human review queue.
"""

from __future__ import annotations

import re

import logging

from sqlalchemy import text

logger = logging.getLogger(__name__)

# local parts that are shared/role inboxes, never a person's role
_ROLE_INBOXES = {
    "accounting",
    "ap",
    "ar",
    "invoice",
    "invoices",
    "billing",
    "payables",
    "receivables",
    "info",
    "sales",
    "orders",
    "office",
    "admin",
    "hr",
    "frontdesk",
    "reservations",
    "noreply",
    "no-reply",
    "support",
}

_TIER_TO_PRIORITY = {
    "TIER1_UNIFORM_DIRECT": "P1",
    "TIER2_PURCHASING": "P1",
    "TIER3_GM_OPS": "P2",
    "TIER4_FB": "P2",
    "TIER5_HR": "P3",
    "TIER6_FINANCE": "P4",
}


def _normalize(title: str) -> str:
    """Match the contact_roles join normalization used across the app:
    '&' -> ' and ', strip non-alnum, collapse spaces, lowercase."""
    s = (title or "").replace("&", " and ")
    s = re.sub(r"[^a-zA-Z0-9 ]+", " ", s)
    return " ".join(s.lower().split())


def _priority_for(title: str) -> str:
    try:
        from app.config.sap_title_classifier import title_classifier

        res = title_classifier.classify(title)
        tier = getattr(res, "tier", None)
        name = getattr(tier, "name", "") or ""
        return _TIER_TO_PRIORITY.get(name, "P_unknown")
    except Exception:
        return "P_unknown"


async def learn_role_from_buyer(session, email: str) -> bool:
    """Record the contact's title as a learned buyer role. Returns True if a
    dictionary row was created or reinforced. Caller commits."""
    em = (email or "").strip().lower()
    if not em or "@" not in em:
        return False
    if em.split("@", 1)[0] in _ROLE_INBOXES:
        return False

    row = (
        await session.execute(
            text(
                "SELECT COALESCE(NULLIF(title,''), inferred_role, '') AS t "
                "FROM contacts WHERE lower(email) = :em LIMIT 1"
            ),
            {"em": em},
        )
    ).first()
    title = (row.t if row else "").strip()
    norm = _normalize(title)
    # need a real multi-word-ish role; single junk tokens teach nothing
    if not norm or len(norm) < 4:
        return False

    existing = (
        await session.execute(
            text("SELECT id FROM contact_roles WHERE role_normalized = :n LIMIT 1"),
            {"n": norm},
        )
    ).first()
    if existing:
        await session.execute(
            text(
                "UPDATE contact_roles SET contact_count = contact_count + 1, "
                "is_relevant = true, updated_at = now() WHERE id = :id"
            ),
            {"id": existing.id},
        )
        logger.info(f"role_learning: reinforced learned role {norm!r} (+1) from {em}")
        return True

    await session.execute(
        text(
            "INSERT INTO contact_roles "
            "(role_raw, role_normalized, vertical, priority, is_relevant, "
            " source, reviewed, contact_count, confidence, notes) "
            "VALUES (:raw, :norm, 'unknown', :prio, true, "
            " 'learned', false, 1, 0.7, :notes)"
        ),
        {
            "raw": title,
            "norm": norm,
            "prio": _priority_for(title),
            "notes": f"Learned from buyer_evidence on {em}",
        },
    )
    logger.info(f"role_learning: NEW learned buyer role {norm!r} from {em}")
    return True
