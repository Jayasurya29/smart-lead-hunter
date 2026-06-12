#!/usr/bin/env python3
"""build_role_dictionary.py -- populate & maintain the role dictionary.

Mines every DISTINCT role string across contacts into contact_roles (migration
036), classifying each via rules first and the LLM for what rules can't place.
Once populated, classification is a lookup -- fast, vertical-aware, cumulative.

MODES
  (default / --mine)   Harvest distinct roles, rule-classify, upsert rows.
                       Adds new roles, refreshes contact_count, never touches
                       reviewed=true rows. Prints the unknown backlog.
  --llm N              After mining, send up to N still-unknown roles (most
                       frequent first) to the LLM and store its labels.
  --review N           Print the top-N highest-frequency UNREVIEWED roles as a
                       checklist to eyeball (no writes).
  --set "role=P1"      Manually set a role's priority (marks reviewed=true,
       --vertical V    source=human -- locks it). Optional --vertical,
       --relevant Y/N  --relevant, --irrelevant.
  --apply              Push the dictionary back onto contacts: for every contact
                       whose role is in the dictionary, set procurement_priority
                       (only where currently NULL/P_unknown -- never clobbers a
                       human/scrape P1..P4) from the dictionary. Buyers first.
  --dry-run            With --apply: show what would change, write nothing.

TYPICAL FLOW
  python scripts/build_role_dictionary.py                 # mine + rule-classify
  python scripts/build_role_dictionary.py --llm 150       # label frequent unknowns
  python scripts/build_role_dictionary.py --review 40     # eyeball the big ones
  python scripts/build_role_dictionary.py --set "registered agent=P4" --irrelevant
  python scripts/build_role_dictionary.py --apply --dry-run
  python scripts/build_role_dictionary.py --apply
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

from sqlalchemy import text  # noqa: E402

from app.database import async_session  # noqa: E402
from app.services.role_intelligence import (  # noqa: E402
    classify_role_rule,
    label_roles_llm,
    normalize_role,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
logger = logging.getLogger("role_dict")

# Distinct roles + a representative org hint + how many contacts carry each.
MINE_SQL = text(
    """
    SELECT role_raw, MAX(org_hint) AS org_hint, COUNT(*)::int AS n
    FROM (
        SELECT COALESCE(NULLIF(title,''), NULLIF(inferred_role,'')) AS role_raw,
               COALESCE(organization,'') AS org_hint
        FROM contacts
        WHERE COALESCE(NULLIF(title,''), NULLIF(inferred_role,'')) IS NOT NULL
          AND (contact_category IS NULL OR contact_category NOT IN
               ('junk','seller','competitor','personal','operational'))
          AND COALESCE(is_shared_mailbox,false) = false
    ) t
    GROUP BY role_raw
    """
)

UPSERT_SQL = text(
    """
    INSERT INTO contact_roles
        (role_raw, role_normalized, vertical, priority, is_relevant,
         seniority, source, reviewed, contact_count, confidence, updated_at)
    VALUES
        (:raw, :norm, :vert, :pri, :rel, :sen, :src, false, :n, :conf, now())
    ON CONFLICT (role_normalized) DO UPDATE SET
        contact_count = EXCLUDED.contact_count,
        updated_at = now(),
        -- only refresh the classification if a human hasn't locked the row
        vertical  = CASE WHEN contact_roles.reviewed THEN contact_roles.vertical  ELSE EXCLUDED.vertical  END,
        priority  = CASE WHEN contact_roles.reviewed THEN contact_roles.priority  ELSE EXCLUDED.priority  END,
        is_relevant = CASE WHEN contact_roles.reviewed THEN contact_roles.is_relevant ELSE EXCLUDED.is_relevant END,
        seniority = CASE WHEN contact_roles.reviewed THEN contact_roles.seniority ELSE EXCLUDED.seniority END,
        source    = CASE WHEN contact_roles.reviewed THEN contact_roles.source    ELSE EXCLUDED.source    END,
        confidence = CASE WHEN contact_roles.reviewed THEN contact_roles.confidence ELSE EXCLUDED.confidence END
    """
)


async def mine(llm_limit: int) -> None:
    async with async_session() as session:
        rows = (await session.execute(MINE_SQL)).all()

    seen: dict[str, dict] = {}
    for r in rows:
        norm = normalize_role(r.role_raw)
        if not norm:
            continue
        # collapse case/punctuation variants, summing their contact counts
        if norm in seen:
            seen[norm]["n"] += r.n
            continue
        c = classify_role_rule(r.role_raw, r.org_hint or "")
        seen[norm] = {
            "raw": r.role_raw.strip(), "norm": norm, "n": r.n,
            "vert": c["vertical"], "pri": c["priority"], "rel": c["is_relevant"],
            "sen": c["seniority"], "src": "rule", "conf": c["confidence"],
        }

    async with async_session() as session:
        for i, v in enumerate(seen.values(), 1):
            await session.execute(UPSERT_SQL, v)
            if i % 500 == 0:
                await session.commit()
        await session.commit()

    total = len(seen)
    unknown = [v for v in seen.values() if v["pri"] == "P_unknown"]
    by_pri = {}
    for v in seen.values():
        by_pri[v["pri"]] = by_pri.get(v["pri"], 0) + 1
    print(f"\nMined {total:,} distinct roles "
          f"({sum(v['n'] for v in seen.values()):,} contacts).")
    print("  By priority: " + "  ".join(f"{k}={by_pri.get(k,0):,}" for k in
          ("P1", "P2", "P3", "P4", "P_unknown")))
    print(f"  Rule-unknown roles: {len(unknown):,} "
          f"(covering {sum(v['n'] for v in unknown):,} contacts)")
    top = sorted(unknown, key=lambda v: v["n"], reverse=True)[:15]
    if top:
        print("\n  Most common UNKNOWN roles (label via --llm N or --set):")
        for v in top:
            print(f"    {v['n']:>4}x  {v['raw'][:48]}")

    if llm_limit > 0 and unknown:
        await _llm_pass(unknown, llm_limit)


# Strings that are org-descriptors / placeholders, not job titles -- the
# scraper sometimes drops a company-type or filler into the role field
# ("hotel staff", "parking services", "condo staff", bare "hotel"). Sending
# these to the LLM wastes credits and yields non-answers, so the LLM pass
# skips anything that doesn't look like an actual title.
_ORG_NOISE_WORDS = frozenset((
    "hotel", "resort", "staff", "parking", "condo", "property", "management",
    "services", "hospitality", "executive", "team", "unknown", "inn", "suites",
    "spa", "club", "restaurant", "company", "llc", "group", "operator",
    "operations", "front", "desk",
))
_TITLE_ANCHORS = (
    "manager", "director", "coordinator", "supervisor", "officer", "chief",
    "president", "vp", "head", "lead", "specialist", "agent", "administrator",
    "buyer", "purchaser", "executive", "owner", "founder", "principal", "chef",
    "engineer", "controller", "superintendent", "representative", "captain",
    "housekeep", "concierge", "steward", "purchas", "procure",
)


def _looks_like_title(norm: str) -> bool:
    """True if the normalized string plausibly names a job (worth LLM labeling),
    False for org-descriptors / placeholders. Errs toward keeping ambiguous
    multi-word strings -- a few extra real titles beat paying to label noise."""
    if not norm or norm == "unknown":
        return False
    words = norm.split()
    if len(words) <= 1 and norm in _ORG_NOISE_WORDS:
        return False
    if (len(words) == 2 and words[0] in _ORG_NOISE_WORDS
            and words[1] in {"staff", "services", "management", "team",
                             "executive", "operator", "operations"}):
        return False
    if any(a in norm for a in _TITLE_ANCHORS):
        return True
    # multi-word and not obvious noise -> let the LLM judge it
    return len(words) >= 2 and not all(w in _ORG_NOISE_WORDS for w in words)


async def _llm_pass(unknown: list[dict], limit: int) -> None:
    import httpx
    # only spend the LLM on strings that actually look like titles
    real = [v for v in unknown if _looks_like_title(v["norm"])]
    skipped = len(unknown) - len(real)
    targets = sorted(real, key=lambda v: v["n"], reverse=True)[:limit]
    print(f"\nLLM labeling {len(targets)} title-like unknown role(s) "
          f"(skipped {skipped} org-descriptor/noise string(s))...")
    client = httpx.AsyncClient(timeout=90)
    labeled = 0
    try:
        BATCH = 25
        async with async_session() as session:
            for i in range(0, len(targets), BATCH):
                chunk = targets[i:i + BATCH]
                items = [{"role": v["raw"], "org_hint": ""} for v in chunk]
                out = await label_roles_llm(client, items)
                for v in chunk:
                    lab = out.get(v["norm"])
                    if not lab:
                        continue
                    await session.execute(
                        text(
                            "UPDATE contact_roles SET vertical=:vert, priority=:pri, "
                            "is_relevant=:rel, seniority=:sen, source='llm', "
                            "confidence=0.75, updated_at=now() "
                            "WHERE role_normalized=:norm AND reviewed=false"
                        ),
                        {"vert": lab["vertical"], "pri": lab["priority"],
                         "rel": lab["is_relevant"], "sen": lab["seniority"],
                         "norm": v["norm"]},
                    )
                    labeled += 1
                await session.commit()
                print(f"  ...{min(i + BATCH, len(targets))}/{len(targets)}")
    finally:
        await client.aclose()
    print(f"LLM labeled {labeled} role(s). Re-run --review to eyeball them.")


async def review(n: int) -> None:
    async with async_session() as session:
        rows = (await session.execute(
            text("SELECT role_raw, vertical, priority, is_relevant, source, "
                 "contact_count FROM contact_roles WHERE reviewed=false "
                 "ORDER BY contact_count DESC LIMIT :n"),
            {"n": n},
        )).all()
    print(f"\nTop {len(rows)} unreviewed roles (by contact count):\n")
    print(f"  {'cnt':>5}  {'pri':4}  {'vertical':14}  {'rel':3}  {'src':5}  role")
    print("  " + "-" * 72)
    for r in rows:
        rel = "?" if r.is_relevant is None else ("yes" if r.is_relevant else "no")
        print(f"  {r.contact_count:>5}  {r.priority:4}  {r.vertical:14}  "
              f"{rel:3}  {r.source:5}  {r.role_raw[:38]}")
    print("\n  Correct any with:  --set \"<role>=<P1|P2|P3|P4>\" "
          "[--vertical V] [--relevant|--irrelevant]")


async def set_role(spec: str, vertical: str, relevant: Optional[bool]) -> None:
    if "=" not in spec:
        print("--set needs the form  \"role text=P1\"")
        return
    role, pri = spec.rsplit("=", 1)
    pri = pri.strip().upper().replace("PUNKNOWN", "P_unknown")
    if pri not in {"P1", "P2", "P3", "P4", "P_unknown"}:
        print(f"bad priority {pri!r}; use P1|P2|P3|P4|P_unknown")
        return
    norm = normalize_role(role)
    sets = ["priority=:pri", "reviewed=true", "source='human'", "updated_at=now()"]
    params = {"pri": pri, "norm": norm}
    if vertical:
        sets.append("vertical=:vert")
        params["vert"] = vertical
    if relevant is not None:
        sets.append("is_relevant=:rel")
        params["rel"] = relevant
    async with async_session() as session:
        res = await session.execute(
            text(f"UPDATE contact_roles SET {', '.join(sets)} "
                 "WHERE role_normalized=:norm"),
            params,
        )
        if res.rowcount == 0:
            # role not mined yet -- insert it as a human row
            await session.execute(
                text("INSERT INTO contact_roles (role_raw, role_normalized, "
                     "vertical, priority, is_relevant, source, reviewed, "
                     "contact_count, confidence) VALUES (:raw,:norm,:vert,:pri,"
                     ":rel,'human',true,0,1.0) ON CONFLICT (role_normalized) "
                     "DO NOTHING"),
                {"raw": role.strip(), "norm": norm,
                 "vert": vertical or "unknown", "pri": pri,
                 "rel": relevant},
            )
        await session.commit()
    print(f"Set {norm!r} -> {pri}"
          + (f", vertical={vertical}" if vertical else "")
          + ("" if relevant is None else f", relevant={relevant}")
          + " (locked: reviewed=human).")


async def apply(dry_run: bool) -> None:
    """Push dictionary priorities onto contacts at P_unknown."""
    sel = text(
        """
        SELECT c.id, COALESCE(NULLIF(c.title,''), NULLIF(c.inferred_role,'')) AS role,
               r.priority
        FROM contacts c
        JOIN contact_roles r
          ON r.role_normalized = lower(regexp_replace(
                 regexp_replace(COALESCE(NULLIF(c.title,''), NULLIF(c.inferred_role,'')),
                 '&', ' and ', 'g'), '[^a-zA-Z0-9 ]+', ' ', 'g'))
        WHERE (c.procurement_priority IS NULL OR c.procurement_priority = 'P_unknown')
          AND r.priority <> 'P_unknown'
          AND (c.contact_category IS NULL OR c.contact_category NOT IN
               ('junk','seller','competitor','personal','operational'))
          AND COALESCE(c.is_shared_mailbox,false) = false
        ORDER BY (c.contact_category='buyer') DESC, c.id
        """
    )
    async with async_session() as session:
        rows = (await session.execute(sel)).all()
    by_pri = {}
    for r in rows:
        by_pri[r.priority] = by_pri.get(r.priority, 0) + 1
    print(f"\nDictionary would set priority on {len(rows):,} contact(s) now at "
          f"P_unknown:  " + "  ".join(f"{k}={by_pri.get(k,0)}" for k in
          ("P1", "P2", "P3", "P4")))
    if dry_run:
        print("DRY RUN -- nothing written.")
        return
    if not rows:
        print("Nothing to apply.")
        return
    async with async_session() as session:
        for i, r in enumerate(rows, 1):
            await session.execute(
                text("UPDATE contacts SET procurement_priority=:p, "
                     "priority_reason='role dictionary' WHERE id=:id AND "
                     "(procurement_priority IS NULL OR procurement_priority='P_unknown')"),
                {"p": r.priority, "id": r.id},
            )
            if i % 500 == 0:
                await session.commit()
        await session.commit()
    print(f"Applied to {len(rows):,} contact(s). Priority facet now reflects the dictionary.")


def main() -> None:
    p = argparse.ArgumentParser(
        description=__doc__.splitlines()[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--mine", action="store_true", help="Harvest + rule-classify (default)")
    p.add_argument("--llm", type=int, default=0, help="LLM-label up to N frequent unknowns")
    p.add_argument("--review", type=int, default=0, help="Print top-N unreviewed roles")
    p.add_argument("--set", type=str, default="", help='Lock a role: "role=P1"')
    p.add_argument("--vertical", type=str, default="", help="With --set: vertical")
    p.add_argument("--relevant", action="store_true", help="With --set: mark relevant")
    p.add_argument("--irrelevant", action="store_true", help="With --set: mark not relevant")
    p.add_argument("--apply", action="store_true", help="Push dictionary onto contacts")
    p.add_argument("--dry-run", action="store_true", help="With --apply: plan only")
    args = p.parse_args()

    if args.set:
        rel = True if args.relevant else (False if args.irrelevant else None)
        asyncio.run(set_role(args.set, args.vertical, rel))
    elif args.review:
        asyncio.run(review(args.review))
    elif args.apply:
        asyncio.run(apply(args.dry_run))
    else:
        asyncio.run(mine(args.llm))


if __name__ == "__main__":
    main()
