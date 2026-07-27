"""resolve_names_serper.py — CHEAP name resolution for blank contacts.

NO Gemini, NO grounding. One or two Serper queries per contact
(~$0.001-0.002 each vs ~$0.10 grounded).

How it works per contact (blank display_name, non-role-inbox, has org):
  1. Decompose the email local-part into a surname candidate:
       apaxton    -> initial 'a' + surname 'paxton'
       jsmith22   -> 'j' + 'smith'
       paxtona    -> surname 'paxton' + trailing initial (also tried)
  2. Serper: site:linkedin.com/in <surname> "<org>"
     (falls back to the email domain core if org is blank)
  3. Parse LinkedIn result titles ("Amanda Paxton - Director - SPI | LinkedIn")
     and accept a candidate ONLY if the name structurally matches the local:
       flatten(name) reproduces the local under one of the standard shapes
       (f+last, first+l, first, glued firstlast, first.last, last+f).
  4. Write first/last/display + linkedin_url, enrichment_source='serper_name'.

Never guesses: no structural email match -> no write.

DRY-RUN by default. Buyers first.
    python scripts/ops/resolve_names_serper.py --limit 50
    python scripts/ops/resolve_names_serper.py --apply --limit 500
    python scripts/ops/resolve_names_serper.py --revert
"""

import argparse
import asyncio
import csv
import os
import re
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from sqlalchemy import text  # noqa: E402

from app.database import async_session  # noqa: E402
from app.services.name_validation import is_role_inbox  # noqa: E402
from app.services.smart_fill import _norm_linkedin, _serper_linkedin_raw  # noqa: E402

BACKUP = "serper_name_backup.csv"

SQL = """
SELECT id, email, organization,
       COALESCE(manual_category, contact_category) AS category
FROM contacts
WHERE (display_name IS NULL OR display_name = '')
  AND first_name IS NULL
  AND email IS NOT NULL
  AND (COALESCE(manual_category, contact_category)
       NOT IN ('junk', 'operational')
   OR COALESCE(manual_category, contact_category) IS NULL)
ORDER BY
  last_inbound_at DESC NULLS LAST,
  CASE COALESCE(manual_category, contact_category)
       WHEN 'buyer' THEN 0 ELSE 1 END,
  id
"""

# "Amanda Paxton - Director of Ops - SPI Health | LinkedIn"
RX_TITLE_NAME = re.compile(r"^([A-Z][\w'.\-]+(?:\s+[A-Z][\w'.\-]+){1,2})\s*[-|\u2013]")
RX_ALPHA = re.compile(r"[^a-z]")


def _flat(s: str) -> str:
    return RX_ALPHA.sub("", (s or "").lower())


def _local(email: str) -> str:
    local = email.split("@")[0].lower()
    return re.sub(r"\d+$", "", local)          # strip trailing digits (jsmith22)


def _surname_candidates(local: str) -> list[str]:
    """Possible surnames encoded in a glued local."""
    l = RX_ALPHA.sub("", local)
    out = []
    if len(l) >= 4:
        out.append(l[1:])          # apaxton  -> paxton (most common shape)
    if len(l) >= 3:
        out.append(l)              # bare surname local (fracassa@)
    if len(l) >= 4:
        out.append(l[:-1])         # paxtona  -> paxton (rare, 3rd choice)
    return [s for s in dict.fromkeys(out) if len(s) >= 3]


def _name_matches_local(first: str, last: str, local: str) -> bool:
    f, l, loc = _flat(first), _flat(last), _flat(local)
    if not f or not l or not loc:
        return False
    shapes = {
        f[0] + l,      # apaxton
        f + l[0],      # amandap
        f + l,         # amandapaxton
        l + f[0],      # paxtona
        f,             # amanda@   (only if local == first name exactly)
        l,             # paxton@
    }
    return loc in shapes


def _extract_candidates(results: list[str]) -> list[tuple[str, str, str]]:
    """(first, last, linkedin_url) candidates from Serper result lines."""
    out = []
    for line in results:
        title = line.split("::")[0].strip()
        url = _norm_linkedin(line) or ""
        m = RX_TITLE_NAME.match(title)
        if not m:
            continue
        toks = m.group(1).split()
        if len(toks) < 2:
            continue
        first, last = toks[0], toks[-1]
        if first.lower() == "linkedin" or len(_flat(last)) < 2:
            continue
        # normalize shouting caps from LinkedIn titles ("Shabber ALI")
        if first.isupper() and len(first) > 1:
            first = first[0] + first[1:].lower()
        if last.isupper() and len(last) > 1:
            last = last[0] + last[1:].lower()
        out.append((first, last, url))
    return out


def resolve_one(email: str, org: str) -> tuple[str, str, str] | None:
    local = _local(email)
    if not local or is_role_inbox(email):
        return None
    surnames = _surname_candidates(local)
    if not surnames:
        return None
    domain_core = email.split("@")[1].split(".")[0] if "@" in email else ""
    # marketing-sender style domains: marriott@email-marriott.com
    dom_words = set(re.split(r"[.-]", email.split("@")[1].lower())) if "@" in email else set()
    if RX_ALPHA.sub("", local) in dom_words or local == domain_core:
        return None                     # local IS the brand, not a person
    anchor = (org or "").strip() or domain_core
    if not anchor:
        return None

    tried = 0
    for surname in surnames[:2]:               # max 2 queries per contact
        tried += 1
        q = f'site:linkedin.com/in {surname} "{anchor}"'
        results = _serper_linkedin_raw(q)
        for first, last, url in _extract_candidates(results):
            # reject org-account "names" whose surname is the company/domain
            if _flat(last) in dom_words or _flat(last) == _flat(anchor):
                continue
            if _name_matches_local(first, last, local):
                return (first, last, url)
    return None


async def run(apply: bool, limit: int) -> None:
    async with async_session() as session:
        rows = (await session.execute(text(SQL))).mappings().all()
    print(f"eligible blank contacts: {len(rows)} (processing first {limit})")

    resolved, misses = [], 0
    t0 = time.time()
    for r in rows[:limit]:
        hit = resolve_one(r["email"], r["organization"])
        if hit:
            first, last, url = hit
            display = f"{first} {last}"
            resolved.append((r["id"], display, first, last, url, r["email"], r["category"]))
            print(f"  HIT  #{r['id']}: {display}  {url or '(no url)'}  <{r['email']}>")
        else:
            misses += 1
    dt = time.time() - t0
    n = min(limit, len(rows))
    print(f"\nresolved {len(resolved)}/{n} ({misses} no-match) in {dt:.0f}s")
    print(f"serper queries used: ~{n * 2} max (2/contact cap)")

    if not apply:
        print("Dry run — nothing written. Re-run with --apply.")
        return
    if not resolved:
        print("nothing to write")
        return

    exists = os.path.exists(BACKUP)
    with open(BACKUP, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not exists:
            w.writerow(["id", "new_display", "linkedin_url", "email"])
        for cid, display, _f, _l, url, email, _cat in resolved:
            w.writerow([cid, display, url, email])

    async with async_session() as session:
        for cid, display, first, last, url, _email, _cat in resolved:
            await session.execute(
                text(
                    "UPDATE contacts SET display_name=:d, first_name=:f, "
                    "last_name=:l, "
                    "linkedin_url = COALESCE(NULLIF(linkedin_url,''), :li), "
                    "enrichment_source='serper_name', updated_at=now() "
                    "WHERE id=:id AND (display_name IS NULL OR display_name='')"
                ),
                {"d": display, "f": first, "l": last, "li": url or None, "id": cid},
            )
        await session.commit()
    print(f"APPLIED {len(resolved)} names. Backup appended: {BACKUP}")


async def revert() -> None:
    if not os.path.exists(BACKUP):
        print(f"no backup file {BACKUP}")
        return
    with open(BACKUP, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    async with async_session() as session:
        for r in rows:
            await session.execute(
                text(
                    "UPDATE contacts SET display_name=NULL, first_name=NULL, "
                    "last_name=NULL, enrichment_source=NULL, updated_at=now() "
                    "WHERE id=:id AND enrichment_source='serper_name'"
                ),
                {"id": int(r["id"])},
            )
        await session.commit()
    print(f"reverted {len(rows)} contacts")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--limit", type=int, default=50)
    ap.add_argument("--revert", action="store_true")
    args = ap.parse_args()
    if args.revert:
        asyncio.run(revert())
    else:
        asyncio.run(run(args.apply, args.limit))
