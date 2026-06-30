"""
audit_contact_filters.py
========================
Answers one question against your LIVE data: is the buyer / seller / junk
classification doing what we intended, or is spam slipping through as "buyer"?

It reports:
  1. contact_category distribution (buyer / seller / junk / NULL=unclassified)
  2. category_source breakdown (who assigned it: llm / lead_generator / ...)
  3. how many contacts are still UNCLASSIFIED (NULL) — these show as "role
     unknown" with no badge in the UI and are NOT a filter failure, just
     not-yet-classified (classification is event-driven off the sync tail)
  4. a spot-check of suspicious cold-pitch domains: what category did each get?
       junk   -> filter WORKING (just visible because the view = "All people")
       buyer  -> filter MISS (a spam sender labelled as a buyer)
       NULL   -> not classified yet
  5. for contrast, known-good buyer/seller domains (parking op, textile supplier)

Run from repo root, venv active, DATABASE_URL set:
    python audit_contact_filters.py
    python audit_contact_filters.py extradomain.com other.co   # add domains to check
"""

import asyncio
import sys

from sqlalchemy import text

from app.database import async_session

# Cold-pitch / spammy-looking domains seen in the Contacts list (override via argv)
SUSPECT_DOMAINS = [
    "youcouponpro.co",
    "runinfluencespace.com",
    "leadroister.co",
    "scalecheckoutexpert.com",
    "appsnichecap.com",
    "luciditycohesion.org",
    "tlyntcontent.co",
    "peoevolve.com",
]
# Domains we EXPECT to be legitimate buyers/sellers — sanity contrast
KNOWN_GOOD = [
    ("townepark.com", "buyer (parking operator)"),
    ("premiumparking.com", "buyer (parking operator)"),
    ("pinnacletextile.com", "seller (apparel supplier)"),
]


async def main() -> None:
    extra = [a.strip() for a in sys.argv[1:] if a.strip()]
    suspects = extra or SUSPECT_DOMAINS

    async with async_session() as s:
        total = (await s.execute(text("SELECT COUNT(*) FROM contacts"))).scalar() or 0
        print("=" * 70)
        print(f" CONTACT FILTER AUDIT — {total:,} total contacts")
        print("=" * 70)

        print("\n1) contact_category distribution")
        rows = (
            await s.execute(
                text(
                    "SELECT COALESCE(contact_category,'(NULL/unclassified)') AS cat, COUNT(*) n "
                    "FROM contacts GROUP BY 1 ORDER BY n DESC"
                )
            )
        ).all()
        for cat, n in rows:
            pct = (n / total * 100) if total else 0
            print(f"   {cat:<26} {n:>7,}  ({pct:4.1f}%)")

        print("\n2) category_source (who assigned the label)")
        rows = (
            await s.execute(
                text(
                    "SELECT COALESCE(category_source,'(none)') src, COUNT(*) n "
                    "FROM contacts WHERE contact_category IS NOT NULL GROUP BY 1 ORDER BY n DESC"
                )
            )
        ).all()
        for src, n in rows:
            print(f"   {src:<26} {n:>7,}")

        unclassified = (
            await s.execute(text("SELECT COUNT(*) FROM contacts WHERE contact_category IS NULL"))
        ).scalar() or 0
        print(f"\n3) UNCLASSIFIED (contact_category IS NULL): {unclassified:,}")
        print("   ^ shown as 'role unknown' / no badge in the UI. Not a filter failure —")
        print("     just not yet classified. If this is huge, classification isn't running.")

        print("\n4) SUSPECT domains — what did the classifier decide?")
        print(f"   {'domain':<28}{'category':<14}{'source':<16}count")
        for dom in suspects:
            rows = (
                await s.execute(
                    text(
                        "SELECT COALESCE(contact_category,'NULL') cat, "
                        "COALESCE(category_source,'-') src, COUNT(*) n "
                        "FROM contacts WHERE email ILIKE :pat GROUP BY 1,2 ORDER BY n DESC"
                    ),
                    {"pat": f"%@{dom}"},
                )
            ).all()
            if not rows:
                print(f"   {dom:<28}{'(not in DB)':<14}")
                continue
            for cat, src, n in rows:
                flag = ""
                if cat == "buyer":
                    flag = "   <-- ⚠ MISS? spam domain labelled buyer"
                elif cat == "junk":
                    flag = "   <-- ✅ caught"
                elif cat == "NULL":
                    flag = "   <-- not classified yet"
                print(f"   {dom:<28}{cat:<14}{src:<16}{n}{flag}")

        print("\n5) KNOWN-GOOD domains — sanity contrast (should NOT be junk)")
        print(f"   {'domain':<28}{'category':<14}{'expected'}")
        for dom, expected in KNOWN_GOOD:
            row = (
                await s.execute(
                    text(
                        "SELECT COALESCE(contact_category,'NULL') cat, COUNT(*) n "
                        "FROM contacts WHERE email ILIKE :pat GROUP BY 1 ORDER BY n DESC LIMIT 1"
                    ),
                    {"pat": f"%@{dom}"},
                )
            ).first()
            cat = row[0] if row else "(not in DB)"
            warn = "   <-- ⚠ expected a buyer/seller" if cat in ("junk", "NULL") else ""
            print(f"   {dom:<28}{cat:<14}{expected}{warn}")

        print("\n" + "=" * 70)
        print(" Read: junk on the spam domains = filter WORKING (view just shows all).")
        print("       buyer on spam domains    = real MISS, worth tightening.")
        print("       large NULL count         = classifier not running on new syncs.")
        print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
