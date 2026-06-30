"""
audit_personal_contacts.py
========================
Marks genuinely personal contacts (tennis partners, models, family, coaches)
as manual_category='personal' so they drop out of the buyer list -- WITHOUT
nuking real businesses that share a keyword ('10-S Tennis Supply', 'Tennis
Plaza', 'Model Linen Service', municipal parks).

The tell: a personal contact has a personal-ish org label ('Tennis - Fred
Pollack', 'Model - Emillia', "Ricardo's Father") AND a consumer email domain
(gmail/yahoo/hotmail/aol/icloud/mac/live/msn/outlook). A real business is on a
company domain or a .gov/.edu, so it's KEPT even if it contains 'tennis'.

Buckets:
  PERSONAL  - personal label + consumer email -> manual_category='personal'
  KEEP      - business domain, or label doesn't look personal -> leave alone
              (shown so you can spot-check nothing real is being dropped)

    python -m scripts.ops.audit_personal_contacts            # dry-run
    python -m scripts.ops.audit_personal_contacts --apply     # mark PERSONAL

Read-only without --apply. Run from repo root, venv active, DATABASE_URL set.
"""

import asyncio
import re
import sys

from sqlalchemy import text

from app.database import async_session

APPLY = "--apply" in sys.argv

# consumer / personal email providers -> a person, not a business inbox
_CONSUMER_DOMAINS = {
    "gmail.com", "yahoo.com", "hotmail.com", "aol.com", "icloud.com",
    "me.com", "mac.com", "live.com", "msn.com", "outlook.com", "comcast.net",
    "bellsouth.net", "att.net", "ymail.com", "sbcglobal.net", "verizon.net",
}

# org labels that signal a personal relationship (word-bounded where it matters)
_PERSONAL_PATTERNS = [
    r"^\s*tennis\s*[-:]",          # 'Tennis - Fred Pollack'
    r"^\s*tennis\s+pro\b",         # 'Tennis Pro - Leandro'
    r"^\s*model\s*[-:]",           # 'Model - Emillia'
    r"\bpickleball\b.*\b\d\.\d\b",
    r"\b(mom|dad|father|mother)\b",  # "Ricardo's Father"
    r"^\s*(soccer|golf)\s+coach\b",
    r"\bmake\s?up artist\b",
    r"\bfashion designer\b",
    r"\d\.\d\b",                    # a skill rating like 4.5 / 5.0 in the label
]
_PERSONAL_RE = re.compile("|".join(_PERSONAL_PATTERNS), re.I)

# real-business signals that OVERRIDE a personal keyword
_BUSINESS_WORDS = ("supply", "supplies", "shop", "store", "plaza", "point",
                   "center", "centre", "company", "inc", "llc", "corp", "group",
                   "service", "services", "park", "department", "academy",
                   "club", "association", "rentals", "linen", "uniform")


def _classify(display_name: str, org: str, email: str):
    org = (org or "").strip()
    dom = (email or "").split("@")[-1].lower()
    low = org.lower()

    # business domain (not a consumer provider) -> KEEP regardless of keyword
    if dom and dom not in _CONSUMER_DOMAINS and not dom.endswith((".gov", ".edu")):
        # but a consumer person mislabeled on a company domain is rare; keep.
        # exception: org literally starts 'Tennis - <person>' is still personal
        if not re.match(r"^\s*tennis\s*[-:]", low):
            return "KEEP"
    # explicit business words override the personal keyword
    if any(re.search(rf"\b{re.escape(w)}\b", low) for w in _BUSINESS_WORDS):
        # ...unless it's the 'Tennis - Person' personal pattern with a rating
        if not (re.match(r"^\s*tennis\s*[-:]", low) and re.search(r"\d\.\d", low)):
            return "KEEP"
    # personal label + (consumer email OR no business signal) -> PERSONAL
    if _PERSONAL_RE.search(org):
        return "PERSONAL"
    return "KEEP"


async def main() -> None:
    async with async_session() as s:
        rows = (await s.execute(text(
            "SELECT id, display_name, organization, email "
            "FROM contacts "
            "WHERE (organization ~* '(\\mtennis\\M|\\mmodel\\M|\\mmom\\M|\\mdad\\M|"
            "\\mfather\\M|\\mmother\\M|\\mcoach\\M|\\msoccer\\M|\\mpickleball\\M|"
            "makeup|make up|fashion designer|roofing)') "
            "AND (manual_category IS NULL OR manual_category <> 'personal') "
            "ORDER BY organization"
        ))).mappings().all()

        personal, keep = [], []
        for r in rows:
            (personal if _classify(r["display_name"], r["organization"], r["email"]) == "PERSONAL"
             else keep).append(r)

        print("=" * 82)
        print(f" PERSONAL-CONTACT AUDIT: {len(rows)} keyword matches")
        print("=" * 82)
        print(f"\n PERSONAL ({len(personal)}) -> manual_category='personal' (drops from buyer list)")
        for r in personal:
            print(f"   #{r['id']:<6} {(r['organization'] or '')[:46]:<46} {r['email']}")
        print(f"\n KEEP ({len(keep)}) -> real business / ambiguous, LEFT ALONE")
        for r in keep:
            print(f"   #{r['id']:<6} {(r['organization'] or '')[:46]:<46} {r['email']}")

        if not APPLY:
            print("\n  DRY-RUN. --apply to mark the PERSONAL bucket. KEEP is untouched.")
            return

        ids = [r["id"] for r in personal]
        if ids:
            await s.execute(text(
                "UPDATE contacts SET manual_category='personal' WHERE id = ANY(:ids)"),
                {"ids": ids})
            await s.commit()
        print(f"\n  DONE — marked {len(ids)} contacts personal. KEEP ({len(keep)}) untouched.")


if __name__ == "__main__":
    asyncio.run(main())
