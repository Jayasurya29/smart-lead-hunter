"""
merge_hotel_dupes.py
====================
Merges existing_hotels rows that are the SAME property spelled differently,
repointing all references and marking the loser as a duplicate (never deletes).

    python -m scripts.ops.merge_hotel_dupes                 # DRY RUN, safe tier only
    python -m scripts.ops.merge_hotel_dupes --apply         # apply safe tier
    python -m scripts.ops.merge_hotel_dupes --show-review   # also list REVIEW tier

TIERING (a merge only happens if the two names are the same property)
  SAFE   : identical after lowercase / '&'->'and' / punctuation / 'The'
           removal, OR the same set of non-filler "core" words with only
           filler (Resort/Hotel/Spa/Suites/Inn/Club...) differing.
           e.g. "Four Seasons Resort Palm Beach" == "Four Seasons Palm Beach"
  REVIEW : one name has extra NON-filler words — listed, never auto-merged.
           e.g. "Cocoa Beach Suites Hotel" vs "The Inn at Cocoa Beach"
  Only SAFE is ever written, and only with --apply.

WINNER = the row with the richer data (client status, then more filled fields,
then longer name). The winner also ADOPTS the better brand_tier of the pair
(tier1 > tier2 > ... ) and any field it is missing that the loser has.

REFERENCES REPOINTED to the winner before the loser is retired:
  - contacts.matched_hotel_id
  - lead_contacts.existing_hotel_id
  - research_history.existing_hotel_id
Loser is then flagged existing_hotels.duplicate_of_id = winner (kept, not
deleted — fully reversible).

SAFE: dry run writes nothing; --apply runs each merge in its own transaction.
"""

import argparse
import asyncio
import re
import sys
from collections import defaultdict

from sqlalchemy import text

from app.database import async_session

FILLER = {"resort", "resorts", "hotel", "hotels", "spa", "spas", "suites",
          "inn", "inns", "club", "clubs", "collection", "villas", "residences",
          "lodge", "lodges", "the", "and", "a", "at", "by", "of", "an"}

BARE_BRANDS = {
    "doubletree", "embassy suites", "hilton", "marriott", "hyatt", "hyatt regency",
    "crowne plaza", "ritz-carlton", "ritz carlton", "the ritz-carlton", "sheraton",
    "westin", "hilton garden inn", "hampton inn", "holiday inn", "courtyard",
    "residence inn", "fairfield inn", "delta hotels", "sonesta es suites",
    "sonesta", "aloft", "renaissance", "w hotel", "conrad", "waldorf astoria",
    "four seasons", "st. regis", "st regis", "intercontinental", "kimpton",
    "hotel indigo", "le meridien", "autograph collection", "curio", "canopy",
    "tribute portfolio", "moxy", "ac hotel", "element", "wyndham", "ramada",
    "days inn", "la quinta", "best western", "radisson", "sofitel", "novotel",
    "park plaza hotel", "park plaza",
}


def _is_bare_brand(name: str) -> bool:
    """True if the name is just a brand with no city/distinguisher — many
    different properties share it, so it must never auto-merge."""
    n = _norm_full(name)
    n = re.sub(r"^the\\s+", "", n).strip()
    return n in BARE_BRANDS


TIER_RANK = {"tier1_ultra_luxury": 5, "tier2_luxury": 4, "tier3_upper_upscale": 3,
             "tier4_upscale": 2, "tier5_skip": 1, "unknown": 0, None: 0, "": 0}


def _norm_full(n: str) -> str:
    s = (n or "").lower().replace("&", "and")
    s = re.sub(r"[.,'\"]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _core(n: str) -> frozenset:
    return frozenset(w for w in _norm_full(n).split() if w not in FILLER)


def _key(n: str) -> str:
    """Grouping key: core words joined. Guard: if <2 core words, keep all
    non-'the' words so single-location names don't collapse together."""
    words = [w for w in _norm_full(n).split() if w]
    core = [w for w in words if w not in FILLER]
    if len(core) < 2:
        core = [w for w in words if w != "the"]
    return "".join(sorted(core))


def _trivial(n: str) -> str:
    """Strip ONLY case, &->and, punctuation, and a leading 'the'. No filler
    removal — so this cannot conflate two different properties in one city."""
    s = _norm_full(n)
    s = re.sub(r"^the\\s+", "", s)
    return s.replace(" ", "")


def classify(a: str, b: str) -> str:
    # SAFE only when the names are the SAME string once case/&/punct/'the'
    # are normalized. Any real word difference (Resort vs none, Inn vs Suites)
    # is REVIEW — a human decides, because string rules cannot tell
    # "same hotel, cosmetic" from "two hotels sharing a city".
    if _trivial(a) == _trivial(b):
        return "SAFE"
    ca, cb = _core(a), _core(b)
    if ca == cb or ca < cb or cb < ca:
        return "REVIEW"
    return "REJECT"


def _richness(h: dict) -> tuple:
    filled = sum(1 for k in ("brand_tier", "address", "room_count", "city",
                             "state", "management_company", "hotel_website")
                 if (h.get(k) not in (None, "", 0)))
    return (1 if h.get("is_client") else 0, filled, len((h.get("hotel_name") or "")))


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--show-review", action="store_true")
    args = ap.parse_args()

    async with async_session() as s:
        hotels = [dict(r._mapping) for r in (await s.execute(text(
            "SELECT id, hotel_name, brand_tier, address, city, state, country, "
            "room_count, management_company, hotel_website, is_client, "
            "duplicate_of_id FROM existing_hotels WHERE duplicate_of_id IS NULL"
        ))).all()]

    groups = defaultdict(list)
    for h in hotels:
        groups[_key(h["hotel_name"])].append(h)

    safe_merges = []   # (winner, loser)
    review_pairs = []
    for k, members in groups.items():
        if len(members) < 2:
            continue
        members.sort(key=_richness, reverse=True)
        winner = members[0]
        for loser in members[1:]:
            verdict = classify(winner["hotel_name"], loser["hotel_name"])
            if verdict == "SAFE" and not (
                _is_bare_brand(winner["hotel_name"]) or _is_bare_brand(loser["hotel_name"])
            ):
                safe_merges.append((winner, loser))
            elif verdict == "SAFE":
                # identical strings but a bare brand — different properties that
                # merely lost their city. Never auto-merge; send to review.
                review_pairs.append((winner, loser))
            elif verdict == "REVIEW":
                review_pairs.append((winner, loser))

    print(f"\nHOTEL MERGE  ({'APPLYING' if args.apply else 'DRY RUN'})\n" + "=" * 72)
    print(f"  SAFE merges (auto)   : {len(safe_merges)}")
    print(f"  REVIEW pairs (manual): {len(review_pairs)}")

    print("\n  SAFE — will merge (loser -> winner), keeping the better tier:")
    for w, lo in safe_merges[:80]:
        wt, lt = w.get("brand_tier"), lo.get("brand_tier")
        tier_note = ""
        if TIER_RANK.get(lt, 0) > TIER_RANK.get(wt, 0):
            tier_note = f"  [tier -> {lt}]"
        print(f"    #{lo['id']} {lo['hotel_name']!r}")
        print(f"       -> #{w['id']} {w['hotel_name']!r}{tier_note}")

    if args.show_review and review_pairs:
        print("\n  REVIEW — NOT merged (decide by hand):")
        for w, lo in review_pairs[:60]:
            print(f"    ? {w['hotel_name']!r}  vs  {lo['hotel_name']!r}")

    if not args.apply:
        print("\n  Dry run — nothing written. --apply to merge SAFE tier; "
              "--show-review to see the manual list.\n")
        return 0

    merged = 0
    async with async_session() as s:
        for w, lo in safe_merges:
            wid, lid = w["id"], lo["id"]
            if wid == lid:
                continue
            # winner adopts better tier + any field it lacks
            sets = []
            params = {"wid": wid}
            if TIER_RANK.get(lo.get("brand_tier"), 0) > TIER_RANK.get(w.get("brand_tier"), 0):
                sets.append("brand_tier = :bt")
                params["bt"] = lo["brand_tier"]
            for col in ("address", "city", "state", "room_count",
                        "management_company", "hotel_website"):
                if not (w.get(col) not in (None, "", 0)) and (lo.get(col) not in (None, "", 0)):
                    sets.append(f"{col} = :{col}")
                    params[col] = lo[col]
            if sets:
                await s.execute(text(
                    f"UPDATE existing_hotels SET {', '.join(sets)}, updated_at=NOW() "
                    "WHERE id = :wid"), params)
            # repoint references
            await s.execute(text("UPDATE contacts SET matched_hotel_id=:wid "
                                 "WHERE matched_hotel_id=:lid"), {"wid": wid, "lid": lid})
            await s.execute(text("UPDATE lead_contacts SET existing_hotel_id=:wid "
                                 "WHERE existing_hotel_id=:lid"), {"wid": wid, "lid": lid})
            await s.execute(text("UPDATE research_history SET existing_hotel_id=:wid "
                                 "WHERE existing_hotel_id=:lid"), {"wid": wid, "lid": lid})
            # retire loser (reversible)
            await s.execute(text("UPDATE existing_hotels SET duplicate_of_id=:wid, "
                                 "status='duplicate', updated_at=NOW() WHERE id=:lid"),
                            {"wid": wid, "lid": lid})
            await s.commit()
            merged += 1

    print(f"\n  MERGED {merged} duplicate hotels into their canonical row.")
    print("  Losers flagged duplicate_of_id (kept, reversible). References repointed.\n")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        sys.exit(130)
