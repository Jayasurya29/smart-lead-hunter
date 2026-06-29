"""
contact_freshness.py
===================
Shared, bounded "check-status" sweep over STALE contacts. One async function so
the manual CLI (batch_check_status.py) and the scheduled Celery task
(freshness_sweep) run the IDENTICAL logic and can never diverge.

Staleness = the most recent real interaction (last_inbound_at / last_outbound_at,
falling back to last_seen) is older than `stale_days`. For each stale buyer with
a real name at a real hotel (not a role inbox, not already flagged 'former',
not re-checked since the last sweep), it runs the same two functions the
"Check status" button runs:

  1. enrich_contact_deep(id)            -> detect move, re-file, flag former
  2. apply_seat_successor(session, id)  -> on a move, find/file the successor

Then stamps enrichment_source='status_checked' so the next sweep skips it
(making the campaign resumable and self-advancing).
"""

import logging

from sqlalchemy import text

from app.database import async_session
from app.services.name_validation import is_role_inbox

logger = logging.getLogger("contact_freshness")

# Bare multi-property PARENT-BRAND domains. Contacts here can't be safely
# auto-resolved: common names (jose.martinez@marriott.com) collide with
# namesakes so move-detection produces FALSE moves, and even a real move can't
# resolve a successor (no single property). These are left for the property-
# resolution work / human review, NOT the automated sweep.
_BRAND_DOMAINS = {
    "marriott.com",
    "hilton.com",
    "hyatt.com",
    "ihg.com",
    "accor.com",
    "wyndham.com",
    "choicehotels.com",
    "radisson.com",
    "bestwestern.com",
    "fourseasons.com",
    "loewshotels.com",
    "ritzcarlton.com",
    "fairmont.com",
    "sheraton.com",
    "westin.com",
    "hyatt.net",
    "marriotthotels.com",
}


def _is_brand_domain(email: str) -> bool:
    dom = email.split("@", 1)[1].lower().strip() if "@" in email else ""
    return dom in _BRAND_DOMAINS


_TARGET_SQL = text(
    "SELECT c.id, c.email, c.first_name, c.last_name, c.organization, "
    "  GREATEST(COALESCE(c.last_inbound_at, c.last_outbound_at, c.last_seen), "
    "           COALESCE(c.last_outbound_at, c.last_inbound_at, c.last_seen)) AS last_touch "
    "FROM contacts c "
    "WHERE (c.first_name IS NOT NULL OR c.display_name IS NOT NULL) "
    "  AND c.organization IS NOT NULL "
    "  AND c.contact_category = 'buyer' "
    "  AND c.email LIKE '%@%' "
    "  AND NOT EXISTS (SELECT 1 FROM contact_affiliations a "
    "       WHERE a.person_type='contact' AND a.person_id=c.id AND a.relationship='former') "
    "  AND c.enrichment_source IS DISTINCT FROM 'status_checked' "
    "  AND GREATEST(COALESCE(c.last_inbound_at, c.last_outbound_at, c.last_seen), "
    "               COALESCE(c.last_outbound_at, c.last_inbound_at, c.last_seen)) "
    "      < now() - make_interval(days => :days) "
    "ORDER BY last_touch ASC NULLS LAST "
    "LIMIT :lim"
)

_STAMP_SQL = text(
    "UPDATE contacts SET enrichment_source='status_checked', updated_at=now() "
    "WHERE id=:id AND enrichment_source IS DISTINCT FROM 'grounded_name'"
)


async def select_stale_contacts(session, stale_days: int, limit: int) -> list[dict]:
    """Return up to `limit` stale buyer contacts (stalest first). Role inboxes and
    bare parent-brand domains (Marriott/Hilton/IHG...) are removed -- the latter
    can't be auto-resolved without false moves, so they're left for human review."""
    rows = (
        (await session.execute(_TARGET_SQL, {"lim": limit * 4, "days": stale_days}))
        .mappings()
        .all()
    )
    keep = [
        dict(r) for r in rows if not is_role_inbox(r["email"]) and not _is_brand_domain(r["email"])
    ]
    return keep[:limit]


async def run_freshness_sweep(stale_days: int = 365, limit: int = 30, apply: bool = False) -> dict:
    """Bounded check-status sweep. apply=False is a dry-run (no API calls/writes)."""
    async with async_session() as s:
        rows = await select_stale_contacts(s, stale_days, limit)
        stats = {
            "targets": len(rows),
            "moved": 0,
            "still_current": 0,
            "successors": 0,
            "errors": 0,
            "applied": apply,
            "stale_days": stale_days,
        }
        if not apply:
            stats["preview"] = [
                {
                    "email": r["email"],
                    "last_touch": r["last_touch"].date().isoformat() if r["last_touch"] else None,
                    "org": r["organization"],
                }
                for r in rows
            ]
            return stats

        from app.services.contact_tier2_enrichment import enrich_contact_deep
        from app.services.current_employer import apply_seat_successor

        for r in rows:
            cid = r["id"]
            try:
                res = await enrich_contact_deep(cid)
                changed = bool(res.get("employer_changed"))
                left = bool(res.get("left_industry"))
                if changed or left:
                    stats["moved"] += 1
                    if changed:
                        try:
                            suc = await apply_seat_successor(s, cid)
                            if suc.get("found") or suc.get("successor_name"):
                                stats["successors"] += 1
                        except Exception as se:
                            logger.warning(f"freshness: successor skipped for {cid}: {se}")
                else:
                    stats["still_current"] += 1
                await s.execute(_STAMP_SQL, {"id": cid})
                await s.commit()
            except Exception as e:
                stats["errors"] += 1
                await s.rollback()
                logger.warning(f"freshness: check-status failed for {cid}: {e}")
        logger.info(
            f"freshness_sweep: targets={stats['targets']} moved={stats['moved']} "
            f"current={stats['still_current']} successors={stats['successors']} "
            f"errors={stats['errors']}"
        )
        return stats
