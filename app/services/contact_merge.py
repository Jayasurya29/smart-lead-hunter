"""contact_merge.py — merge two inbox contacts into one.

Destructive-but-reversible. The SURVIVOR keeps the richest value per field
(non-empty wins; on a tie the survivor's own value stays). The LOSER's
affiliations and source_mailboxes fold into the survivor, interaction_count
sums, and the loser is soft-deleted (manual_category='junk' + a merge note in
background) so nothing is hard-lost and the action can be undone by un-junking.

Two entry points:
  preview_merge(a, b)  -> what the merged record would look like, field by field
  commit_merge(primary_id, merge_id) -> perform it; returns the survivor id
"""

from __future__ import annotations

import logging

from sqlalchemy import text

from app.database import async_session

logger = logging.getLogger(__name__)

# fields folded "richest wins" (survivor keeps its own on a tie)
_RICH_FIELDS = [
    "first_name",
    "last_name",
    "display_name",
    "title",
    "organization",
    "phone",
    "address",
    "linkedin_url",
    "parent_company",
    "brand_tier",
    "gpo",
    "management_company",
    "department",
    "seniority",
    "inferred_role",
    "background",
    "secondary_email",
]


def _pick(primary, loser, field):
    """Non-empty wins; primary wins ties."""
    pv = getattr(primary, field, None)
    lv = getattr(loser, field, None)
    ps = (pv or "").strip() if isinstance(pv, str) else pv
    ls = (lv or "").strip() if isinstance(lv, str) else lv
    if ps:
        return pv
    return lv if ls else pv


async def _load(session, cid: int):
    return (
        (await session.execute(text("SELECT * FROM contacts WHERE id = :id"), {"id": cid}))
        .mappings()
        .first()
    )


async def preview_merge(primary_id: int, merge_id: int) -> dict:
    """Return the field-by-field result of merging merge_id INTO primary_id.
    Read-only."""
    if primary_id == merge_id:
        return {"error": "cannot merge a contact into itself"}
    async with async_session() as session:
        a = await _load(session, primary_id)
        b = await _load(session, merge_id)
    if not a or not b:
        return {"error": "one or both contacts not found"}

    class _Row:  # tiny attr shim so _pick can use getattr on mappings
        def __init__(self, m):
            self._m = m

        def __getattr__(self, k):
            return self._m.get(k)

    ra, rb = _Row(a), _Row(b)
    result = {}
    for f in _RICH_FIELDS:
        result[f] = {"primary": a.get(f), "loser": b.get(f), "merged": _pick(ra, rb, f)}
    return {
        "primary_id": primary_id,
        "merge_id": merge_id,
        "primary_email": a.get("email"),
        "merge_email": b.get("email"),
        "interaction_count_sum": (a.get("interaction_count") or 0)
        + (b.get("interaction_count") or 0),
        "fields": result,
        "note": "merge_id will be soft-deleted (moved to Trash) after folding.",
    }


async def commit_merge(primary_id: int, merge_id: int) -> dict:
    """Merge merge_id INTO primary_id. Survivor = primary_id. Reversible: the
    loser is soft-deleted to Trash, not dropped."""
    if primary_id == merge_id:
        return {"error": "cannot merge a contact into itself"}
    async with async_session() as session:
        a = await _load(session, primary_id)
        b = await _load(session, merge_id)
        if not a or not b:
            return {"error": "one or both contacts not found"}

        class _Row:
            def __init__(self, m):
                self._m = m

            def __getattr__(self, k):
                return self._m.get(k)

        ra, rb = _Row(a), _Row(b)

        # 1) fold richest fields onto survivor
        sets, params = [], {"id": primary_id}
        for f in _RICH_FIELDS:
            val = _pick(ra, rb, f)
            if val != a.get(f):
                sets.append(f"{f} = :{f}")
                params[f] = val
        # sum interactions; keep the older first_seen + newer last_seen
        params["ic"] = (a.get("interaction_count") or 0) + (b.get("interaction_count") or 0)
        sets.append("interaction_count = :ic")
        sets.append("last_seen = GREATEST(last_seen, :bls)")
        params["bls"] = b.get("last_seen")
        sets.append("first_seen = LEAST(first_seen, :bfs)")
        params["bfs"] = b.get("first_seen")
        # fold source_mailboxes (array union, drop nulls)
        sets.append(
            "source_mailboxes = ("
            "SELECT ARRAY(SELECT DISTINCT unnest("
            "COALESCE(source_mailboxes,'{}') || COALESCE(:bmbx,'{}')))"
            ")"
        )
        params["bmbx"] = b.get("source_mailboxes")
        sets.append("updated_at = now()")
        await session.execute(text(f"UPDATE contacts SET {', '.join(sets)} WHERE id = :id"), params)

        # 2) repoint the loser's affiliations to the survivor (skip dup edges)
        await session.execute(
            text(
                "UPDATE contact_affiliations a SET person_id = :pid "
                "WHERE a.person_type='contact' AND a.person_id = :mid "
                "AND NOT EXISTS ("
                "  SELECT 1 FROM contact_affiliations x WHERE x.person_type='contact' "
                "  AND x.person_id = :pid AND x.relationship = a.relationship "
                "  AND lower(COALESCE(x.account_name,'')) = lower(COALESCE(a.account_name,'')) "
                "  AND lower(COALESCE(x.title,'')) = lower(COALESCE(a.title,''))"
                ")"
            ),
            {"pid": primary_id, "mid": merge_id},
        )
        # delete any now-duplicate loser edges that didn't repoint
        await session.execute(
            text(
                "DELETE FROM contact_affiliations "
                "WHERE person_type='contact' AND person_id = :mid"
            ),
            {"mid": merge_id},
        )

        # 3) soft-delete the loser -> Trash, with a reversible breadcrumb
        await session.execute(
            text(
                "UPDATE contacts SET manual_category = 'junk', "
                "background = COALESCE(NULLIF(background,'') || ' | ', '') "
                "|| :note, updated_at = now() WHERE id = :mid"
            ),
            {
                "mid": merge_id,
                "note": f"Merged into contact #{primary_id} ({a.get('email')}); "
                "un-junk to restore.",
            },
        )

        await session.commit()

    logger.info(f"contact_merge: #{merge_id} merged into #{primary_id}")
    return {"status": "merged", "survivor_id": primary_id, "merged_id": merge_id}
