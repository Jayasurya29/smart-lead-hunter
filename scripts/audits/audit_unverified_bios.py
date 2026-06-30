"""
audit_unverified_bios.py
======================
Find and clean namesake / "couldn't-verify" bios stored in contacts.background
for contacts that have NO verified LinkedIn slug. These are the Meredith-Haley
pattern: a stranger's life story (or the LLM narrating its own failure to ID
the person) shown in the AI panel as if it were fact.

Three buckets:
  JUNK     - the bio is the LLM admitting it couldn't identify the person
             ("multiple individuals named...", "no clear match", "do not contain
             information relevant..."). Worthless. Cleared on --apply.
  NAMESAKE - the bio names an employer/field that contradicts the contact's org
             (heuristic: a 'works at / associated with X' where X is clearly a
             different company than the on-file org). Cleared on --apply.
  KEEP     - bio reads as a real description consistent with the org. Left alone.

Clearing = set background, title, inferred_role, seniority, enrichment_confidence
to NULL (same fields we hand-cleared for Meredith). Name/org/email untouched.

    python audit_unverified_bios.py              # dry-run: show the 3 buckets
    python audit_unverified_bios.py --apply       # clear JUNK + NAMESAKE
    python audit_unverified_bios.py --apply --junk-only   # clear only JUNK (safer)

Read-only without --apply. Run from repo root, venv active, DATABASE_URL set.
"""

import asyncio
import re
import sys

from sqlalchemy import text

from app.database import async_session

APPLY = "--apply" in sys.argv

# Phrases that mark a bio as the LLM failing to identify the person.
_JUNK_SIGNALS = (
    "multiple individuals named",
    "several individuals named",
    "several linkedin profiles",
    "several people named",
    "various mentions of individuals",
    "various individuals named",
    "no clear match",
    "none clearly match",
    "none clearly",
    "impossible to",
    "snippets provide no",
    "provide no professional",
    "do not contain information",
    "does not contain information",
    "no professional information",
    "not contain information relevant",
    "primarily discuss",           # "...primarily discuss Tim Robinson, the American comedian"
    "the american comedian",
)


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def _is_junk(bio: str) -> bool:
    low = (bio or "").lower()
    return any(sig in low for sig in _JUNK_SIGNALS)


def _namesake_company(bio: str, org: str):
    """If the bio says the person works at / is associated with a company that
    is clearly NOT their on-file org, return that company string (namesake
    signal). Heuristic and CONSERVATIVE -- this bucket is review-only, never
    auto-cleared, because 'Wymara Ltd' vs 'Wymara Resort' looks like a mismatch
    but isn't. We only surface it for your eyes."""
    if not bio or not org:
        return None
    org_flat = _norm(org)
    pats = [
        r"(?:owner|ceo|founder|co-founder)\s+of\s+([A-Z][\w&'.\- ]{2,40})",
        r"currently associated with ([A-Z][\w&'.\- ]{2,40})",
    ]
    for pat in pats:
        for m in re.finditer(pat, bio):
            cand = m.group(1).strip().rstrip(".,")
            cand_flat = _norm(cand)
            if len(cand_flat) < 4:
                continue
            # require NO shared word stem at all (kills Wymara/Wymara, Carr/Carr)
            org_words = set(re.findall(r"[a-z]{4,}", (org or "").lower()))
            cand_words = set(re.findall(r"[a-z]{4,}", cand.lower()))
            if org_words & cand_words:
                continue
            if cand_flat in org_flat or org_flat in cand_flat:
                continue
            return cand
    return None


async def main() -> None:
    async with async_session() as s:
        rows = (await s.execute(text(
            "SELECT id, first_name, last_name, organization, title, background, "
            "enrichment_confidence AS conf "
            "FROM contacts "
            "WHERE background IS NOT NULL AND background <> '' "
            "AND (linkedin_url IS NULL OR linkedin_url NOT LIKE '%linkedin.com/in%') "
            "AND (enrichment_confidence IS NULL OR enrichment_confidence <= 0.75) "
            "ORDER BY enrichment_confidence NULLS FIRST"
        ))).mappings().all()

        junk, namesake, keep = [], [], []
        for r in rows:
            bio = r["background"] or ""
            if _is_junk(bio):
                junk.append(r)
            elif (ns := _namesake_company(bio, r["organization"] or "")):
                namesake.append((r, ns))
            else:
                keep.append(r)

        def _name(r):
            return f"{(r['first_name'] or '').strip()} {(r['last_name'] or '').strip()}".strip() or "?"

        print("=" * 80)
        print(f" UNVERIFIED BIOS (no slug, conf<=0.75): {len(rows)} contacts")
        print("=" * 80)
        print(f"\n JUNK — LLM couldn't identify the person ({len(junk)}):")
        for r in junk:
            print(f"  #{r['id']:<6} {_name(r):<22} @ {(r['organization'] or '?')[:28]}")
        print(f"\n NAMESAKE — bio names a different employer ({len(namesake)}):")
        for r, ns in namesake:
            print(f"  #{r['id']:<6} {_name(r):<22} @ {(r['organization'] or '?')[:24]} -> bio says {ns!r}")
        print(f"\n KEEP — bio fits the org, looks like the real person ({len(keep)}):")
        for r in keep:
            print(f"  #{r['id']:<6} {_name(r):<22} @ {(r['organization'] or '?')[:28]}")

        if not APPLY:
            print("\n  DRY-RUN. --apply clears the JUNK bucket (reliable).")
            print("  NAMESAKE is review-only (heuristic isn't safe to auto-clear).")
            print("  To also clear specific namesake ids: --apply --also-clear \"id,id\"")
            return

        to_clear = [r["id"] for r in junk]
        # explicit ids the human approved from the NAMESAKE review list
        if "--also-clear" in sys.argv:
            try:
                extra = [int(x) for x in sys.argv[sys.argv.index("--also-clear") + 1].split(",")
                         if x.strip().isdigit()]
                to_clear += extra
            except Exception:
                pass
        to_clear = sorted(set(to_clear))
        if not to_clear:
            print("\n  Nothing to clear.")
            return
        res = await s.execute(text(
            "UPDATE contacts SET background=NULL, title=NULL, inferred_role=NULL, "
            "seniority=NULL, enrichment_confidence=NULL "
            "WHERE id = ANY(:ids)"), {"ids": to_clear})
        await s.commit()
        print(f"\n  CLEARED {res.rowcount} contacts' unverified bios. KEEP set left intact.")
        print(f"  ids: {to_clear}")


if __name__ == "__main__":
    asyncio.run(main())
