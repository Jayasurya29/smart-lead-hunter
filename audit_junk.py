"""Audit junk contacts (2026-06-08) — READ ONLY, writes nothing.

Re-runs the real relevance() rules on every contact currently marked
'junk' and sorts the likely MIS-FILES to the top:

  SUSPICIOUS — junked, but the rule engine would NOT junk it today
               (verdict relevant/unknown) → probably the tier1 LLM junked
               it. Plus any ★decision-maker or actively-emailing contact
               sitting in junk.
  CLEAR JUNK — the rules agree it's junk (shows which signals fired).

Use it to decide what to recover; a separate picker will do the writes.

Run:
    python audit_junk.py            # suspicious + summary (+ first 30 clear)
    python audit_junk.py --all      # also list every clear-junk row
"""

import asyncio
import sys

from sqlalchemy import text

from app.database import async_session
from app.services.contact_intelligence import relevance


def _domain(email: str) -> str:
    return (email or "").split("@", 1)[1].lower() if "@" in (email or "") else ""


async def main(show_all: bool, show_more: bool) -> None:
    async with async_session() as db:
        rows = (
            await db.execute(
                text(
                    "SELECT id, email, display_name, first_name, last_name, title, "
                    "       inferred_role, organization, is_decision_maker, phone, "
                    "       interaction_count "
                    "FROM contacts WHERE contact_category = 'junk' "
                    "ORDER BY interaction_count DESC NULLS LAST, id"
                )
            )
        ).mappings().all()

    suspicious, clear = [], []
    for r in rows:
        title = r["title"] or r["inferred_role"]
        rel = relevance(r["organization"], title, r["email"] or "")
        is_dm = bool(r["is_decision_maker"])
        active = (r["interaction_count"] or 0) >= 3
        rule_would_junk = rel["verdict"] == "junk"
        flag = (not rule_would_junk) or is_dm or active
        rec = {
            "id": r["id"],
            "name": (r["display_name"] or f"{r['first_name'] or ''} {r['last_name'] or ''}".strip() or "—"),
            "title": title or "—",
            "email": r["email"] or "—",
            "org": r["organization"] or "—",
            "dm": is_dm,
            "emails": r["interaction_count"] or 0,
            "verdict": rel["verdict"],
            "reasons": "; ".join(rel["reasons"]),
        }
        (suspicious if flag else clear).append(rec)

    def line(rec):
        tag = []
        if rec["dm"]:
            tag.append("★DM")
        if rec["emails"]:
            tag.append(f"{rec['emails']}em")
        tagstr = (" [" + " ".join(tag) + "]") if tag else ""
        return (f"  #{rec['id']:>5}  {rec['email']:<40} {rec['name'][:22]:<22} "
                f"{rec['title'][:24]:<24} → rules say {rec['verdict']}"
                f" ({rec['reasons']}){tagstr}\n        org: {rec['org']}")

    print(f"TOTAL junk contacts: {len(rows)}")
    print(f"  likely MIS-FILED (review these): {len(suspicious)}")
    print(f"  rules agree it's junk:           {len(clear)}\n")

    suspicious.sort(key=lambda x: (not x["dm"], -x["emails"], x["verdict"] == "junk"))
    dms = [r for r in suspicious if r["dm"]]
    rest = [r for r in suspicious if not r["dm"]]

    print("=" * 70)
    print(f"★ DECISION-MAKERS sitting in junk — review FIRST ({len(dms)})")
    print("=" * 70)
    for rec in dms:
        print(line(rec))

    print("\n" + "=" * 70)
    print(f"OTHER suspicious (rules wouldn't junk / active): {len(rest)}")
    print("=" * 70)
    if show_more or show_all:
        for rec in rest:
            print(line(rec))
    else:
        for rec in rest[:15]:
            print(line(rec))
        if len(rest) > 15:
            print(f"\n  … +{len(rest) - 15} more suspicious. Run with --more to see them.")

    print(f"\nCLEAR JUNK — rules confirm: {len(clear)} (hidden; run --all to list)")
    if show_all:
        print("=" * 70)
        for rec in clear:
            print(line(rec))


if __name__ == "__main__":
    asyncio.run(main(show_all="--all" in sys.argv, show_more="--more" in sys.argv))
