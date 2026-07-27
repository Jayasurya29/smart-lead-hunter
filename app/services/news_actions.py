"""News -> actions: turn scan output into human-in-the-loop queues.

Two queues (see migration 051):
  news_lead_queue     new orgs to (maybe) add as leads — approval-gated
  news_person_review  known people who changed roles — review-gated

This module is the SINGLE source of truth for the qualify gate, the self-match
filter, dedup, and the approve/reject transitions. Both run_news_scan (live)
and scripts/ops/backfill_news_queues.py (one-shot over existing hotel_news)
call these, so the rule can't drift between them.

Nothing here auto-creates a lead. A queued hotel only becomes a potential_lead
when a human approves it — at which point save_lead_to_db() runs its own dedup,
so approving a hotel that already snuck into the pipeline is safe (it links to
the existing row instead of duplicating).
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.lead_factory import _normalize_for_dedup, save_lead_to_db

logger = logging.getLogger(__name__)

# A story is worth queuing when it's a real property/facility EVENT (not generic
# industry chatter) in our sell-to geography and a vertical we service.
PROPERTY_EVENTS = {
    "opening",
    "appointment",
    "management_change",
    "renovation",
    "acquisition",
    "rebrand",
}
TARGET_REGIONS = {"usa", "caribbean"}
# HOTELS ONLY for now: enrich_lead_data / lead_score / revenue / brand-tier /
# procurement grounding are all hotel-shaped, so a queued hospital or university
# would smart-fill into a half-empty, useless lead. When the account model gives
# us non-hotel enrichment, add "education", "healthcare" back here (one line).
LEAD_VERTICALS = {"hotel"}


def _norm(s: str | None) -> str:
    return _normalize_for_dedup(s or "")


def qualifies_as_lead(
    *, vertical: str | None, region: str | None, category: str | None, luxury: bool | None
) -> bool:
    """Second gate (after the scan's relevance classifier) before we queue a
    new org as a lead candidate. Hotels qualify on luxury OR a property event;
    education/healthcare qualify on a property event (luxury doesn't apply)."""
    vert = (vertical or "hotel").lower()
    reg = (region or "other").lower()
    cat = (category or "other").lower()
    if vert not in LEAD_VERTICALS or reg not in TARGET_REGIONS:
        return False
    return bool(luxury) or cat in PROPERTY_EVENTS


def is_self_match(known_account: str | None, new_hotel: str | None) -> bool:
    """A 'job change' where the person is already AT that place is not a move —
    it's the same record re-surfacing. Never queue those. Containment only
    counts as 'same place' when the shorter side is specific (>=2 tokens), so a
    bare brand ('Marriott') moving to 'Marriott Marquis NYC' is still a move."""
    a, b = _norm(known_account), _norm(new_hotel)
    if not a or not b:
        return False
    if a == b:
        return True
    short, long = (a, b) if len(a) <= len(b) else (b, a)
    return len(short.split()) >= 2 and short in long


# Non-hotel words the extractor sometimes mislabels as a property.
_BAD_TOKENS = re.compile(
    r"\b(zoo|aquarium|museum|stadium|arena|ballpark|airport|hospital|clinic|"
    r"university|college|campus|headquarters)\b",
    re.IGNORECASE,
)


def looks_like_real_hotel(name: str | None) -> bool:
    """Cheap guard against the name-extractor's junk before it hits the queue:
    sentence fragments ('this historic Hawaiian hotel'), non-hotels ('Columbus
    Zoo'), descriptor blobs ('Delano-branded tower'). Real property names are
    proper nouns, so the first letter is capitalized and there's no non-hotel
    token. Errs toward keeping — only the obvious junk is dropped."""
    s = (name or "").strip()
    if len(s) < 4:
        return False
    first_alpha = next((c for c in s if c.isalpha()), "")
    if first_alpha and first_alpha.islower():  # fragments start lowercase
        return False
    if "-branded" in s.lower():  # "Delano-branded tower"
        return False
    if _BAD_TOKENS.search(s):
        return False
    return True


# ── QUEUE WRITERS ─────────────────────────────────────────────────────────


async def queue_new_hotel(
    db: AsyncSession,
    *,
    news_id: int | None,
    hotel_name: str,
    brand: str | None = None,
    city: str | None = None,
    region: str | None = None,
    vertical: str | None = None,
    category: str | None = None,
    luxury: bool | None = None,
    source: str | None = None,
    url: str | None = None,
    in_pipeline: bool = False,
) -> str:
    """Queue one new-org candidate. Returns a short outcome string.
    Idempotent: the partial unique index collapses repeat stories of one hotel."""
    nk = _norm(hotel_name)
    if len(nk) < 4:
        return "skipped_no_name"
    if in_pipeline:
        return "skipped_in_pipeline"
    if not looks_like_real_hotel(hotel_name):
        return "skipped_bad_name"
    if not qualifies_as_lead(vertical=vertical, region=region, category=category, luxury=luxury):
        return "skipped_unqualified"
    res = await db.execute(
        text(
            "INSERT INTO news_lead_queue (news_id, hotel_name, "
            "hotel_name_normalized, brand, city, region, vertical, category, "
            "luxury, source, url, status) VALUES (:nid, :hn, :nk, :brand, "
            ":city, :region, :vert, :cat, :lux, :src, :url, 'pending') "
            "ON CONFLICT (hotel_name_normalized) WHERE status='pending' "
            "DO NOTHING RETURNING id"
        ),
        {
            "nid": news_id,
            "hn": hotel_name[:500],
            "nk": nk,
            "brand": (brand or None),
            "city": (city or None),
            "region": region,
            "vert": vertical,
            "cat": category,
            "lux": bool(luxury),
            "src": (source or None)[:160] if source else None,
            "url": url,
        },
    )
    return "queued" if res.first() else "skipped_dup"


async def queue_person_flag(
    db: AsyncSession,
    *,
    news_id: int | None,
    person_name: str | None,
    person_title: str | None,
    new_hotel: str | None,
    new_org: str | None,
    hits: list[dict[str, Any]] | None,
    region: str | None = None,
) -> str:
    """Queue one known-person job-change flag. Picks the strongest hit that is
    NOT a self-match. Deduped per person+destination by the partial index.
    Region-gated to our sell-to geography (USA / Caribbean)."""
    if (region or "other").lower() not in TARGET_REGIONS:
        return "skipped_out_of_region"
    if not person_name or " " not in person_name.strip() or not hits:
        return "skipped_no_person"
    # strongest first: email-exact beats name-match
    order = {"email-exact": 0, "name-match": 1}
    ranked = sorted(hits, key=lambda h: order.get(h.get("strength"), 9))
    hit = next(
        (
            h
            for h in ranked
            if not is_self_match(h.get("account") or h.get("organization"), new_hotel)
        ),
        None,
    )
    if hit is None:
        return "skipped_self_match"
    res = await db.execute(
        text(
            "INSERT INTO news_person_review (news_id, person_name, person_title, "
            "new_hotel, new_org, match_strength, known_contact_id, "
            "known_lead_contact_id, known_account, status) VALUES (:nid, :pn, "
            ":pt, :nh, :no, :ms, :kcid, :klcid, :ka, 'pending') "
            "ON CONFLICT (LOWER(person_name), LOWER(COALESCE(new_hotel,''))) "
            "WHERE status='pending' DO NOTHING RETURNING id"
        ),
        {
            "nid": news_id,
            "pn": person_name[:300],
            "pt": person_title,
            "nh": new_hotel,
            "no": new_org,
            "ms": hit.get("strength"),
            "kcid": hit.get("contact_id"),
            "klcid": hit.get("lead_contact_id"),
            "ka": hit.get("account") or hit.get("organization"),
        },
    )
    return "queued" if res.first() else "skipped_dup"


def _looks_like_person(name: str | None) -> bool:
    """Cheap sanity gate: a real 'First Last' human name, not an org blob."""
    n = (name or "").strip()
    if " " not in n or len(n) > 60:
        return False
    parts = [p for p in re.split(r"\s+", n) if p]
    if len(parts) < 2:
        return False
    for p in parts[:2]:  # first two tokens each ≥2 letters
        if sum(c.isalpha() for c in p) < 2:
            return False
    alpha = [c for c in n if c.isalpha()]  # reject ALL-CAPS org strings
    if alpha and all(c.isupper() for c in alpha):
        return False
    return True


def _tier_for_title(title: str | None) -> str | None:
    """Map a raw title to a BuyerTier name (e.g. 'TIER3_GM_OPS') using the
    canonical synonym-aware classifier, so news contacts get the right sales
    priority immediately instead of sitting at UNKNOWN. None if unplaceable."""
    if not title or not title.strip():
        return None
    try:
        from app.config.sap_title_classifier import title_classifier

        c = title_classifier.classify(title)
        return c.tier.name if c and getattr(c, "tier", None) else None
    except Exception:
        return None


async def queue_existing_contact(
    db: AsyncSession,
    *,
    news_id: int | None,
    hotel_name: str | None,
    person_name: str | None,
    person_title: str | None,
    region: str | None = None,
    category: str | None = None,
) -> str:
    """A story named a person at a hotel we ALREADY own. Queue them for review
    so they can be attached to the existing account as a new contact. Skipped
    if out of region, not a real name, unresolvable, or already a contact."""
    if (region or "other").lower() not in TARGET_REGIONS:
        return "skipped_out_of_region"
    if not _looks_like_person(person_name):
        return "skipped_no_person"
    acct_type, acct_id, _ = await _resolve_destination(db, hotel_name)
    if not (acct_type and acct_id):
        return "skipped_no_account"
    fk = "lead_id" if acct_type == "potential_lead" else "existing_hotel_id"
    exists = (
        await db.execute(
            text(f"SELECT 1 FROM lead_contacts WHERE {fk}=:aid " "AND lower(name)=:n LIMIT 1"),
            {"aid": acct_id, "n": person_name.strip().lower()},
        )
    ).first()
    if exists:
        return "skipped_already_contact"
    res = await db.execute(
        text(
            "INSERT INTO news_contact_review (news_id, hotel_name, person_name, "
            "person_title, account_type, account_id, account_name, region, "
            "event_type, status) VALUES (:nid, :hn, :pn, :pt, :at, :aid, :an, "
            ":reg, :ev, 'pending') "
            "ON CONFLICT (lower(person_name), account_type, account_id) "
            "WHERE status='pending' DO NOTHING RETURNING id"
        ),
        {
            "nid": news_id,
            "hn": hotel_name,
            "pn": person_name.strip()[:300],
            "pt": person_title,
            "at": acct_type,
            "aid": acct_id,
            "an": hotel_name,
            "reg": region,
            "ev": category,
        },
    )
    return "queued" if res.first() else "skipped_dup"


# ── AUDIT LOG + REVERT ────────────────────────────────────────────────────
# Every mutating action records an `undo` recipe (list of inverse ops) so a
# bad one can be rolled back. Tables that revert is allowed to touch — a
# whitelist so a malformed recipe can never delete from an arbitrary table.
_REVERTABLE_TABLES = {
    "lead_contacts",
    "potential_leads",
    "existing_hotels",
    "contact_affiliations",
}


async def log_action(
    db: AsyncSession,
    *,
    action: str,
    summary: str,
    undo: list[dict[str, Any]],
    source_ref: str | None = None,
) -> int | None:
    """Record one mutating action + its undo recipe. Caller commits."""
    r = await db.execute(
        text(
            "INSERT INTO news_action_log (action, summary, source_ref, undo) "
            "VALUES (:a, :s, :r, CAST(:u AS jsonb)) RETURNING id"
        ),
        {"a": action[:40], "s": summary, "r": source_ref, "u": json.dumps(undo or [])},
    )
    return r.scalar()


async def revert_action(db: AsyncSession, log_id: int) -> dict[str, Any]:
    """Replay a logged action's inverse ops to undo it (for bad data)."""
    row = (
        (
            await db.execute(
                text("SELECT * FROM news_action_log WHERE id=:i AND NOT reverted"),
                {"i": log_id},
            )
        )
        .mappings()
        .first()
    )
    if not row:
        return {"status": "not_found_or_already_reverted"}
    done: list[str] = []
    for op in row["undo"] or []:
        kind = op.get("op")
        if kind == "delete" and op.get("table") in _REVERTABLE_TABLES:
            await db.execute(text(f"DELETE FROM {op['table']} WHERE id=:i"), {"i": op["id"]})
            done.append(f"deleted {op['table']}#{op['id']}")
        elif kind == "set_lead_rejected":
            await db.execute(
                text("UPDATE potential_leads SET status='rejected' WHERE id=:i"),
                {"i": op["id"]},
            )
            done.append(f"rejected potential_lead#{op['id']}")
        elif kind == "restore_affiliation":
            await db.execute(
                text(
                    "UPDATE contact_affiliations SET relationship=:r, updated_at=now() "
                    "WHERE id=:i"
                ),
                {"r": op.get("to", "employed_by"), "i": op["id"]},
            )
            done.append(f"restored affiliation#{op['id']} → {op.get('to')}")
        elif kind == "reopen_queue":
            await db.execute(
                text(
                    "UPDATE news_lead_queue SET status='pending', "
                    "created_lead_id=NULL, reviewed_at=NULL WHERE id=:i"
                ),
                {"i": op["queue_id"]},
            )
            done.append(f"reopened lead-queue#{op['queue_id']}")
        elif kind == "reopen_review":
            await db.execute(
                text(
                    "UPDATE news_person_review SET status='pending', "
                    "reviewed_at=NULL WHERE id=:i"
                ),
                {"i": op["review_id"]},
            )
            done.append(f"reopened person-review#{op['review_id']}")
        elif kind == "reopen_contact_review":
            await db.execute(
                text(
                    "UPDATE news_contact_review SET status='pending', "
                    "reviewed_at=NULL WHERE id=:i"
                ),
                {"i": op["review_id"]},
            )
            done.append(f"reopened contact-review#{op['review_id']}")
    await db.execute(
        text("UPDATE news_action_log SET reverted=true, reverted_at=now() WHERE id=:i"),
        {"i": log_id},
    )
    await db.commit()
    return {"status": "reverted", "log_id": log_id, "did": done}


async def list_actions(db: AsyncSession, days: int = 7, limit: int = 200):
    """Recent news actions, newest first (for the daily/weekly activity view)."""
    return (
        (
            await db.execute(
                text(
                    "SELECT id, action, summary, source_ref, reverted, created_at, "
                    "reverted_at FROM news_action_log "
                    "WHERE created_at > NOW() - make_interval(days => :d) "
                    "ORDER BY created_at DESC LIMIT :lim"
                ),
                {"d": days, "lim": limit},
            )
        )
        .mappings()
        .all()
    )


# ── REVIEW TRANSITIONS ────────────────────────────────────────────────────


async def approve_lead(db: AsyncSession, queue_id: int) -> dict[str, Any]:
    """Approve a queued hotel -> save_lead_to_db (its own dedup runs) -> mark
    the queue row approved and link the created/matched lead."""
    row = (
        (
            await db.execute(
                text("SELECT * FROM news_lead_queue WHERE id=:i AND status='pending'"),
                {"i": queue_id},
            )
        )
        .mappings()
        .first()
    )
    if not row:
        return {"status": "not_found"}
    # The news scan gives us region (usa/caribbean) but no city, and
    # save_lead_to_db's hard location gate needs state OR country. Map the
    # region we already classified into a country the scorer recognizes
    # (US at scorer:1041, "Caribbean" via CARIBBEAN_KEYWORDS at scorer:1093).
    region = (row["region"] or "").lower()
    country = "United States" if region == "usa" else "Caribbean" if region == "caribbean" else None
    lead_dict = {
        "hotel_name": row["hotel_name"],
        "city": row["city"],
        "brand": row["brand"],
        "country": country,
        "data_source": "news",
    }
    result = await save_lead_to_db(lead_dict, db, commit=False)
    lead_id = result.get("id")
    if lead_id is None:
        # The hotel wasn't created — but WHY matters. If it fuzzy/exact-matched a
        # hotel we already own, don't throw away the story's named person: attach
        # them to that existing hotel as a new contact (the same value the
        # contact-review queue provides, salvaged from a would-be dead end).
        eh_id = result.get("matched_existing_hotel_id")
        pname = ""
        if eh_id and row["news_id"]:
            story = (
                (
                    await db.execute(
                        text("SELECT person_name, person_title, url FROM hotel_news WHERE id=:i"),
                        {"i": row["news_id"]},
                    )
                )
                .mappings()
                .first()
            )
            pname = (story["person_name"] or "").strip() if story else ""
        if eh_id and _looks_like_person(pname):
            already = (
                await db.execute(
                    text(
                        "SELECT 1 FROM lead_contacts WHERE existing_hotel_id=:h "
                        "AND lower(name)=:n LIMIT 1"
                    ),
                    {"h": eh_id, "n": pname.lower()},
                )
            ).first()
            lc_id = None
            if not already:
                lc_id = (
                    await db.execute(
                        text(
                            "INSERT INTO lead_contacts (existing_hotel_id, name, title, "
                            "tier, organization, found_via, source_detail, confidence, "
                            "is_saved, scope) VALUES "
                            "(:h,:n,:t,:tier,:org,'news',:sd,'high',true,'hotel_specific') "
                            "RETURNING id"
                        ),
                        {
                            "h": eh_id,
                            "n": pname,
                            "t": (story["person_title"] or None),
                            "tier": _tier_for_title(story["person_title"]),
                            "org": row["hotel_name"],
                            "sd": f"Named in news story: {story['url']}",
                        },
                    )
                ).scalar()
            await db.execute(
                text(
                    "UPDATE news_lead_queue SET status='rejected', reviewed_at=now() " "WHERE id=:i"
                ),
                {"i": queue_id},
            )
            undo: list[dict[str, Any]] = []
            if lc_id:
                undo.append({"op": "delete", "table": "lead_contacts", "id": lc_id})
            undo.append({"op": "reopen_queue", "queue_id": queue_id})
            await log_action(
                db,
                action="add_contact",
                summary=(f"{pname} → existing EH#{eh_id} " f"(hotel dup of '{row['hotel_name']}')"),
                undo=undo,
                source_ref=f"queue#{queue_id}",
            )
            await db.commit()
            return {
                "status": "merged_contact",
                "existing_hotel_id": eh_id,
                "attached_contact": pname if lc_id else None,
                "reason": result.get("reason"),
            }
        # genuinely nothing to save (location gate, bad data) — leave pending.
        await db.rollback()
        return {"status": "not_created", "reason": result.get("reason"), "lead_result": result}

    # Attach the person the STORY named (e.g. the new GM in an appointment
    # article) as the first contact — the news already told us who, no need to
    # wait for smart-fill to re-discover them. Route to the correct parent:
    # a graduated hotel lands in existing_hotels, everything else is a lead.
    to_existing = result.get("status") in ("saved_to_existing", "merged_to_existing")
    fk = "existing_hotel_id" if to_existing else "lead_id"
    attached = None
    attached_contact_id = None
    if row["news_id"]:
        story = (
            (
                await db.execute(
                    text("SELECT person_name, person_title FROM hotel_news WHERE id=:i"),
                    {"i": row["news_id"]},
                )
            )
            .mappings()
            .first()
        )
        pname = (story["person_name"] or "").strip() if story else ""
        if pname and " " in pname:
            exists = (
                await db.execute(
                    text(
                        f"SELECT 1 FROM lead_contacts WHERE {fk}=:aid " "AND lower(name)=:n LIMIT 1"
                    ),
                    {"aid": lead_id, "n": pname.lower()},
                )
            ).first()
            if not exists:
                attached_contact_id = (
                    await db.execute(
                        text(
                            f"INSERT INTO lead_contacts ({fk}, name, title, tier, "
                            "organization, found_via, source_detail, confidence, "
                            "is_saved, scope) VALUES "
                            "(:aid,:n,:t,:tier,:org,'news',:sd,'high',true,'hotel_specific') "
                            "RETURNING id"
                        ),
                        {
                            "aid": lead_id,
                            "n": pname,
                            "t": (story["person_title"] or None),
                            "tier": _tier_for_title(story["person_title"]),
                            "org": row["hotel_name"],
                            "sd": f"Named in news story: {row['url']}",
                        },
                    )
                ).scalar()
                attached = pname

    await db.execute(
        text(
            "UPDATE news_lead_queue SET status='approved', created_lead_id=:lid, "
            "reviewed_at=now() WHERE id=:i"
        ),
        {"lid": lead_id, "i": queue_id},
    )

    # audit + undo recipe. Only delete the hotel row if WE created it (a fresh
    # potential_lead / existing_hotel); never delete a pre-existing one we
    # merged into. Order: contact before parent (FK-safe).
    undo: list[dict[str, Any]] = []
    if attached_contact_id:
        undo.append({"op": "delete", "table": "lead_contacts", "id": attached_contact_id})
    st = result.get("status")
    if st in ("saved", "new"):
        undo.append({"op": "set_lead_rejected", "id": lead_id})
    elif st == "saved_to_existing":
        undo.append({"op": "delete", "table": "existing_hotels", "id": lead_id})
    undo.append({"op": "reopen_queue", "queue_id": queue_id})
    summary = (
        f"Approved '{row['hotel_name']}' → {'EH' if to_existing else 'lead'} "
        f"#{lead_id}" + (f"; attached {attached}" if attached else "")
    )
    await log_action(
        db, action="approve_lead", summary=summary, undo=undo, source_ref=f"queue#{queue_id}"
    )

    await db.commit()
    return {
        "status": "approved",
        "lead_id": lead_id,
        "attached_contact": attached,
        "lead_result": result,
    }


async def reset_stuck_leads(db: AsyncSession) -> int:
    """Re-open hotel rows that were marked 'approved' but never produced a lead
    (created_lead_id IS NULL) — e.g. rows consumed by the earlier approve bug."""
    r = await db.execute(
        text(
            "UPDATE news_lead_queue SET status='pending', reviewed_at=NULL "
            "WHERE status='approved' AND created_lead_id IS NULL"
        )
    )
    await db.commit()
    return r.rowcount


async def reject_lead(db: AsyncSession, queue_id: int) -> bool:
    r = await db.execute(
        text(
            "UPDATE news_lead_queue SET status='rejected', reviewed_at=now() "
            "WHERE id=:i AND status='pending'"
        ),
        {"i": queue_id},
    )
    await db.commit()
    return r.rowcount > 0


async def set_person_status(db: AsyncSession, review_id: int, status: str) -> bool:
    if status not in ("actioned", "dismissed"):
        return False
    r = await db.execute(
        text(
            "UPDATE news_person_review SET status=:s, reviewed_at=now() "
            "WHERE id=:i AND status='pending'"
        ),
        {"s": status, "i": review_id},
    )
    await db.commit()
    return r.rowcount > 0


# ── PERSON MOVE: turn a flag into real data ───────────────────────────────


async def _resolve_destination(
    db: AsyncSession, hotel_name: str | None
) -> tuple[str | None, int | None, int | None]:
    """Find the lead/hotel the person moved TO, so we can attach them.
    Returns (account_type, account_id, potential_lead_id_for_contact)."""
    nk = _norm(hotel_name)
    low = (hotel_name or "").strip().lower()
    if not nk:
        return (None, None, None)
    # 1) a news hotel we already approved into a lead (exact link, same source)
    lid = (
        await db.execute(
            text(
                "SELECT created_lead_id FROM news_lead_queue "
                "WHERE hotel_name_normalized=:nk AND status='approved' "
                "AND created_lead_id IS NOT NULL ORDER BY reviewed_at DESC LIMIT 1"
            ),
            {"nk": nk},
        )
    ).scalar()
    if lid:
        return ("potential_lead", lid, lid)
    # 2) exact name already a lead
    lid = (
        await db.execute(
            text(
                "SELECT id FROM potential_leads WHERE lower(hotel_name)=:h "
                "AND status NOT IN ('rejected','duplicate') LIMIT 1"
            ),
            {"h": low},
        )
    ).scalar()
    if lid:
        return ("potential_lead", lid, lid)
    # 3) exact name is an existing hotel
    hid = (
        await db.execute(
            text("SELECT id FROM existing_hotels WHERE lower(hotel_name)=:h LIMIT 1"),
            {"h": low},
        )
    ).scalar()
    if hid:
        return ("existing_hotel", hid, None)
    return (None, None, None)


async def apply_person_move(db: AsyncSession, review_id: int) -> dict[str, Any]:
    """Action a job-change flag = make it real, non-destructively:
      1. mark the person's prior role 'former' in contact_affiliations
      2. add an 'employed_by' edge to the new place
      3. if the new place is a lead/hotel, attach them as a warm lead_contact
         (a decision-maker you already know, on a new account)
    The contact's own record is never overwritten — only edges are added."""
    row = (
        (
            await db.execute(
                text("SELECT * FROM news_person_review WHERE id=:i AND status='pending'"),
                {"i": review_id},
            )
        )
        .mappings()
        .first()
    )
    if not row:
        return {"status": "not_found"}

    pt = "contact" if row["known_contact_id"] else "lead_contact"
    pid = row["known_contact_id"] or row["known_lead_contact_id"]
    done: list[str] = []

    # Resolve the destination FIRST. If it's not a lead/hotel yet, do NOTHING
    # and leave the flag pending — so the action is atomic and re-runnable once
    # you've approved the hotel. No half-applied state.
    acct_type, acct_id, lead_id = await _resolve_destination(db, row["new_hotel"])
    fk_col = (
        "lead_id"
        if acct_type == "potential_lead"
        else ("existing_hotel_id" if acct_type == "existing_hotel" else None)
    )
    # [patch_news_autocreate] Destination not in the DB yet — create it as a
    # lead automatically instead of dead-ending, then re-resolve. This is what
    # "Apply move" visually promises: one click records the move, standing up
    # the hotel if needed. Uses save_lead_to_db so the new lead still gets full
    # dedup / scoring / region-gating.
    if not (fk_col and acct_id):
        try:
            from app.services.lead_factory import save_lead_to_db

            # [patch_news_seed_country] news_person_review has no location
            # columns, and the lead_factory gate rejects a lead with no
            # state/country/known-city. The feed is region-filtered upstream
            # and appointments are overwhelmingly US, so default country=USA to
            # clear the gate. Hotel-name dedup still matches any existing
            # Caribbean property first, so none is lost.
            seed = {
                "hotel_name": row["new_hotel"],
                "country": "USA",
                "source_site": "Hospitality News",
                "hotel_type": "hotel",
                "description": (
                    f"Auto-created from a news appointment: {row['person_name']}"
                    f" -> {row['new_hotel']}."
                ),
            }
            res = await save_lead_to_db(seed, db, commit=False)
            if res.get("status") in ("saved", "duplicate") and res.get("id"):
                acct_type, acct_id, lead_id = await _resolve_destination(db, row["new_hotel"])
                fk_col = (
                    "lead_id"
                    if acct_type == "potential_lead"
                    else ("existing_hotel_id" if acct_type == "existing_hotel" else None)
                )
        except Exception as _e:  # noqa: BLE001
            fk_col = fk_col  # fall through to the block below

    if not (fk_col and acct_id):
        return {
            "status": "blocked",
            "person": row["person_name"],
            "reason": (
                f"Couldn't stand up '{row['new_hotel']}' as a lead automatically "
                "(likely out of region or a junk name). Add it manually, then "
                "re-run. Flag left pending."
            ),
        }

    # 1) prior role -> former (only the place we knew them). Capture which
    # rows we flipped so revert can restore them.
    former_ids: list[int] = []
    if pid and row["known_account"]:
        r = await db.execute(
            text(
                "UPDATE contact_affiliations SET relationship='former', updated_at=now() "
                "WHERE person_type=:pt AND person_id=:pid AND relationship='employed_by' "
                "AND account_name ILIKE :ka RETURNING id"
            ),
            {"pt": pt, "pid": pid, "ka": f"%{row['known_account']}%"},
        )
        former_ids = [x[0] for x in r.fetchall()]
        if former_ids:
            done.append(f"marked {len(former_ids)} prior role(s) former")

    # 2) new employed_by edge to the destination lead/hotel
    new_affil_id = None
    if pid:
        new_affil_id = (
            await db.execute(
                text(
                    "INSERT INTO contact_affiliations (person_type, person_id, "
                    "account_type, account_id, account_name, relationship, source, "
                    "confidence, notes) VALUES (:pt,:pid,:at,:aid,NULL,'employed_by',"
                    "'derived',0.7,:notes) RETURNING id"
                ),
                {
                    "pt": pt,
                    "pid": pid,
                    "at": acct_type,
                    "aid": acct_id,
                    "notes": (
                        f"News job-change: {row['person_title'] or 'new role'} at "
                        f"{row['new_hotel']} (was at {row['known_account']})"
                    ),
                },
            )
        ).scalar()
        done.append(f"added employed_by → {row['new_hotel']}")

    # 3) attach as a warm lead_contact on the destination
    lc_id = None
    exists = (
        await db.execute(
            text(f"SELECT 1 FROM lead_contacts WHERE {fk_col}=:aid " "AND lower(name)=:n LIMIT 1"),
            {"aid": acct_id, "n": (row["person_name"] or "").lower()},
        )
    ).first()
    if exists:
        done.append(f"already on {acct_type} #{acct_id}")
    else:
        lc_id = (
            await db.execute(
                text(
                    f"INSERT INTO lead_contacts ({fk_col}, name, title, tier, "
                    "organization, found_via, source_detail, confidence, "
                    "is_saved, scope) VALUES "
                    "(:aid,:n,:t,:tier,:org,'news_move',:sd,'high',true,'hotel_specific') "
                    "RETURNING id"
                ),
                {
                    "aid": acct_id,
                    "n": row["person_name"],
                    "t": row["person_title"],
                    "tier": _tier_for_title(row["person_title"]),
                    "org": row["new_hotel"],
                    "sd": f"Known from {row['known_account']}; moved here per news",
                },
            )
        ).scalar()
        done.append(f"attached warm contact to {acct_type} #{acct_id}")

    await db.execute(
        text("UPDATE news_person_review SET status='actioned', reviewed_at=now() " "WHERE id=:i"),
        {"i": review_id},
    )

    # audit + undo: remove the contact + new edge, restore the former flips,
    # reopen the review.
    undo: list[dict[str, Any]] = []
    if lc_id:
        undo.append({"op": "delete", "table": "lead_contacts", "id": lc_id})
    if new_affil_id:
        undo.append({"op": "delete", "table": "contact_affiliations", "id": new_affil_id})
    for aid in former_ids:
        undo.append({"op": "restore_affiliation", "id": aid, "to": "employed_by"})
    undo.append({"op": "reopen_review", "review_id": review_id})
    await log_action(
        db,
        action="person_move",
        summary=(f"{row['person_name']} → {row['new_hotel']} " f"(was {row['known_account']})"),
        undo=undo,
        source_ref=f"review#{review_id}",
    )

    await db.commit()
    return {"status": "actioned", "person": row["person_name"], "did": done}


async def reopen_person(db: AsyncSession, review_id: int) -> bool:
    """Reset a person flag to pending (e.g. it was actioned before the
    destination hotel was a lead). Also removes any stray 'management_company'
    fallback edge a prior run may have written, so re-actioning is clean."""
    row = (
        (
            await db.execute(
                text(
                    "SELECT known_contact_id, known_lead_contact_id "
                    "FROM news_person_review WHERE id=:i"
                ),
                {"i": review_id},
            )
        )
        .mappings()
        .first()
    )
    if not row:
        return False
    pt = "contact" if row["known_contact_id"] else "lead_contact"
    pid = row["known_contact_id"] or row["known_lead_contact_id"]
    if pid:
        await db.execute(
            text(
                "DELETE FROM contact_affiliations WHERE person_type=:pt "
                "AND person_id=:pid AND source='derived' "
                "AND account_type='management_company' "
                "AND notes LIKE 'News job-change%'"
            ),
            {"pt": pt, "pid": pid},
        )
    await db.execute(
        text("UPDATE news_person_review SET status='pending', reviewed_at=NULL " "WHERE id=:i"),
        {"i": review_id},
    )
    await db.commit()
    return True


# ── CONTACT REVIEWS (new person at a hotel we already own) ─────────────────


async def approve_contact(db: AsyncSession, review_id: int) -> dict[str, Any]:
    """Attach a queued person to the existing account as a new saved contact."""
    row = (
        (
            await db.execute(
                text("SELECT * FROM news_contact_review WHERE id=:i AND status='pending'"),
                {"i": review_id},
            )
        )
        .mappings()
        .first()
    )
    if not row:
        return {"status": "not_found"}
    fk = "lead_id" if row["account_type"] == "potential_lead" else "existing_hotel_id"
    url = None
    if row["news_id"]:
        url = (
            await db.execute(text("SELECT url FROM hotel_news WHERE id=:i"), {"i": row["news_id"]})
        ).scalar()
    exists = (
        await db.execute(
            text(f"SELECT 1 FROM lead_contacts WHERE {fk}=:aid AND lower(name)=:n LIMIT 1"),
            {"aid": row["account_id"], "n": (row["person_name"] or "").lower()},
        )
    ).first()
    lc_id = None
    if exists:
        did = "already a contact"
    else:
        lc_id = (
            await db.execute(
                text(
                    f"INSERT INTO lead_contacts ({fk}, name, title, tier, "
                    "organization, found_via, source_detail, confidence, "
                    "is_saved, scope) VALUES "
                    "(:aid,:n,:t,:tier,:org,'news',:sd,'high',true,'hotel_specific') "
                    "RETURNING id"
                ),
                {
                    "aid": row["account_id"],
                    "n": row["person_name"],
                    "t": row["person_title"],
                    "tier": _tier_for_title(row["person_title"]),
                    "org": row["hotel_name"],
                    "sd": f"Named in news story{f': {url}' if url else ''}",
                },
            )
        ).scalar()
        did = f"attached to {row['account_type']} #{row['account_id']}"
    await db.execute(
        text("UPDATE news_contact_review SET status='actioned', reviewed_at=now() " "WHERE id=:i"),
        {"i": review_id},
    )
    undo: list[dict[str, Any]] = []
    if lc_id:
        undo.append({"op": "delete", "table": "lead_contacts", "id": lc_id})
    undo.append({"op": "reopen_contact_review", "review_id": review_id})
    await log_action(
        db,
        action="add_contact",
        summary=f"{row['person_name']} → {row['hotel_name']} (existing account)",
        undo=undo,
        source_ref=f"contact-review#{review_id}",
    )
    await db.commit()
    return {"status": "actioned", "person": row["person_name"], "did": did}


async def set_contact_status(db: AsyncSession, review_id: int, status: str) -> bool:
    r = await db.execute(
        text(
            "UPDATE news_contact_review SET status=:s, reviewed_at=now() "
            "WHERE id=:i AND status='pending'"
        ),
        {"s": status, "i": review_id},
    )
    await db.commit()
    return bool(r.rowcount)


async def reopen_contact(db: AsyncSession, review_id: int) -> bool:
    r = await db.execute(
        text("UPDATE news_contact_review SET status='pending', reviewed_at=NULL " "WHERE id=:i"),
        {"i": review_id},
    )
    await db.commit()
    return bool(r.rowcount)
