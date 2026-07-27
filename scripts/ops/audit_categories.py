"""
audit_categories.py
===================
Read-only. Answers the original question: are contacts in the WRONG category —
sellers stuck in buyers, buyers marked junk, etc.

    python -m scripts.ops.audit_categories
    python -m scripts.ops.audit_categories --sample 20

We cannot know the "true" category for sure, but we can find DISAGREEMENTS
between the stored category and three independent signals:

  1. category_source   — how confident the labelling was (SAP client = strong,
                         domain guess = weak).
  2. buying_signal_*   — the buying-signal engine's read of the actual email
                         content (did this person talk about ordering uniforms?).
  3. VENDOR_SEEDS      — a maintained list of real uniform SUPPLIERS. A contact
                         whose org is on it is a seller, whatever the label says.

Each disagreement is a candidate misclassification to review. This does not
change anything — it produces the worklist.
"""

import argparse
import asyncio
import sys
from collections import Counter

from sqlalchemy import text

from app.database import async_session

try:
    from app.services.client_resolver import VENDOR_SEEDS
except Exception:
    VENDOR_SEEDS = set()

SQL = """
    SELECT id, email, organization,
           COALESCE(manual_category, contact_category, '') AS cat,
           manual_category, contact_category, category_source,
           buying_signal_score, buying_signal_label, buying_signal_products
    FROM contacts
    WHERE email LIKE '%@%'
"""


def is_vendor(org: str) -> bool:
    o = (org or "").lower()
    return bool(o) and any(v in o for v in VENDOR_SEEDS)


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=int, default=12)
    args = ap.parse_args()
    S = args.sample

    async with async_session() as s:
        rows = [dict(r._mapping) for r in (await s.execute(text(SQL))).all()]
    total = len(rows)

    print(f"\nCATEGORY AUDIT  ({total:,} contacts)\n" + "=" * 70)

    # ── overall distribution + provenance ──
    cats = Counter(r["cat"] or "(none)" for r in rows)
    print("\nCURRENT CATEGORIES:")
    for k, v in cats.most_common():
        print(f"  {k:<14} {v:>7,}  ({100.0*v/total:.0f}%)")

    print("\nHOW EACH WAS DECIDED (category_source):")
    for k, v in Counter(r["category_source"] or "(none)" for r in rows).most_common():
        print(f"  {k:<20} {v:>7,}")

    # ── disagreement 1: seller/vendor stuck as buyer ──
    d1 = [r for r in rows if r["cat"] == "buyer" and is_vendor(r["organization"])]
    print(f"\n1. Marked BUYER but org is a known SUPPLIER: {len(d1)}")
    for r in d1[:S]:
        print(f"     {r['email'][:38]:<40} org={r['organization']}")

    # ── disagreement 2: buyer on a supplier-ish domain, weak source ──
    # buyer whose category came from a WEAK source and who has NO buying signal
    d2 = [r for r in rows if r["cat"] == "buyer"
          and (r["category_source"] or "") in ("domain_inferred", "", None)
          and not r["buying_signal_score"]]
    print(f"\n2. BUYER from a weak source with NO buying signal: {len(d2)}")
    print("   (may be fine — but these are the least-supported buyer labels)")
    for r in d2[:S]:
        print(f"     {r['email'][:38]:<40} src={r['category_source'] or '-'}")

    # ── disagreement 3: strong buying signal but NOT marked buyer ──
    d3 = [r for r in rows if r["cat"] not in ("buyer",)
          and (r["buying_signal_score"] or 0) >= 5
          and r["cat"] not in ("seller", "competitor")]
    print(f"\n3. Strong BUYING SIGNAL but NOT marked buyer: {len(d3)}")
    print("   (these may be buyers we mislabelled or left unstatused)")
    for r in d3[:S]:
        print(f"     {r['email'][:38]:<40} cat={r['cat'] or '(none)':<12} "
              f"signal={r['buying_signal_score']} {r['buying_signal_label'] or ''}")

    # ── disagreement 4: seller/vendor NOT marked seller ──
    d4 = [r for r in rows if r["cat"] not in ("seller", "junk")
          and is_vendor(r["organization"])]
    print(f"\n4. Org is a known SUPPLIER but NOT marked seller: {len(d4)}")
    for r in d4[:S]:
        print(f"     {r['email'][:38]:<40} cat={r['cat'] or '(none)':<12} org={r['organization']}")

    # ── disagreement 5: buying signal but sitting in junk/trash ──
    d5 = [r for r in rows if r["cat"] == "junk"
          and (r["buying_signal_score"] or 0) >= 5]
    print(f"\n5. In JUNK but has a real buying signal: {len(d5)}")
    for r in d5[:S]:
        print(f"     {r['email'][:38]:<40} signal={r['buying_signal_score']} "
              f"{r['buying_signal_label'] or ''}")

    total_suspect = len(d1) + len(d3) + len(d4) + len(d5)
    print("\n" + "=" * 70)
    print(f"  HIGH-CONFIDENCE review worklist: {total_suspect:,}")
    print(f"    seller-as-buyer      {len(d1):>5}")
    print(f"    buyer-signal, mislabeled {len(d3):>5}")
    print(f"    supplier-not-seller  {len(d4):>5}")
    print(f"    buyer stuck in junk  {len(d5):>5}")
    print(f"  (bucket 2, weak buyers, is lower confidence — {len(d2):,} to spot-check)")
    print("\n  Read-only. This is a worklist, not a change.\n")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        sys.exit(130)
