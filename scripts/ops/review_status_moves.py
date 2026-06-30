"""
review_status_moves.py
=====================
Lists the MOVES the check-status batch recorded (the 'former' affiliations it
created), newest first, so you can eyeball which are real vs namesake garbage
(e.g. 'William Graham @ Marriott -> Rowing Sport' is a false move).

For each mover it shows: former org -> current org (the new employer it re-filed
to), the email, and a [BRAND] tag when the email is a bare parent-brand domain
(the namesake-prone ones).

DRY-RUN / read-only by default.
--since-hours N : only moves recorded in the last N hours (default 24)
--rollback "12,45,99" : revert THOSE contact ids -- restore organization to the
    former org, delete the 'former' affiliation, and clear the move's
    enrichment marker. (Successor stubs are listed separately; delete by id with
    --drop-successors "id,id" if you want.)

Run from repo root, venv active, DATABASE_URL set:
    python review_status_moves.py
    python review_status_moves.py --since-hours 6
    python review_status_moves.py --rollback "1234,5678"
"""

import asyncio
import re
import sys

from sqlalchemy import text

from app.database import async_session

try:
    from app.services.contact_freshness import _BRAND_DOMAINS
except Exception:
    _BRAND_DOMAINS = set()


def _arg(flag, default):
    if flag in sys.argv:
        try:
            return sys.argv[sys.argv.index(flag) + 1]
        except Exception:
            return default
    return default


SINCE_H = int(_arg("--since-hours", "24"))
ROLLBACK = [int(x) for x in (_arg("--rollback", "") or "").split(",") if x.strip().isdigit()]
DROP_SUC = [int(x) for x in (_arg("--drop-successors", "") or "").split(",") if x.strip().isdigit()]


def _is_brand(email: str) -> bool:
    dom = email.split("@", 1)[1].lower().strip() if email and "@" in email else ""
    return dom in _BRAND_DOMAINS


async def main() -> None:
    async with async_session() as s:
        if DROP_SUC:
            res = await s.execute(text(
                "DELETE FROM lead_contacts WHERE id = ANY(:ids) "
                "AND found_via='successor_discovery'"), {"ids": DROP_SUC})
            await s.commit()
            print(f"  dropped {res.rowcount} successor stub(s): {DROP_SUC}")
            if not ROLLBACK:
                return

        if ROLLBACK:
            n = 0
            for cid in ROLLBACK:
                row = (await s.execute(text(
                    "SELECT email, organization FROM contacts WHERE id=:id"),
                    {"id": cid})).mappings().one_or_none()
                if not row:
                    print(f"  #{cid}: contact not found, skipped")
                    continue
                formers = [r[0] for r in (await s.execute(text(
                    "SELECT account_name FROM contact_affiliations "
                    "WHERE person_type='contact' AND person_id=:id AND relationship='former' "
                    "ORDER BY created_at DESC"), {"id": cid})).fetchall() if r[0]]
                email = row["email"] or ""
                cur = (row["organization"] or "")
                dom_label = (email.split("@", 1)[1].split(".")[0].lower()
                             if "@" in email else "")
                cur_flat = re.sub(r"[^a-z]", "", cur.lower())
                # is the CURRENT org consistent with the email domain?
                org_ok = bool(dom_label) and dom_label in cur_flat
                if not org_ok and formers:
                    # org was re-filed AWAY from the domain -> restore. Prefer a
                    # former that matches the domain, else the most recent.
                    restore = next(
                        (f for f in formers
                         if dom_label and dom_label in re.sub(r"[^a-z]", "", f.lower())),
                        formers[0])
                    await s.execute(text(
                        "UPDATE contacts SET organization=:org, enrichment_source=NULL, "
                        "parent_company=NULL, brand_tier=NULL WHERE id=:id"),
                        {"org": restore, "id": cid})
                    print(f"  reverted #{cid}: org restored '{cur}' -> '{restore}', "
                          f"{len(formers)} former edge(s) deleted")
                else:
                    print(f"  reverted #{cid}: org kept '{cur}' (matches domain), "
                          f"{len(formers)} spurious former edge(s) deleted")
                # always clear the bogus former edges
                await s.execute(text(
                    "DELETE FROM contact_affiliations WHERE person_type='contact' "
                    "AND person_id=:id AND relationship='former'"), {"id": cid})
                n += 1
            await s.commit()
            print(f"\n  DONE — cleaned {n} contacts. "
                  f"(Successor stubs untouched; use --drop-successors.)")
            return

        rows = (await s.execute(text(
            "SELECT a.person_id AS id, a.account_name AS former_org, a.created_at, "
            "  c.email, c.organization AS current_org, c.first_name, c.last_name "
            "FROM contact_affiliations a JOIN contacts c ON c.id=a.person_id "
            "WHERE a.person_type='contact' AND a.relationship='former' "
            "  AND a.created_at > now() - make_interval(hours => :h) "
            "ORDER BY a.created_at DESC"), {"h": SINCE_H})).mappings().all()

        print("=" * 78)
        print(f" CHECK-STATUS MOVES — recorded in last {SINCE_H}h: {len(rows)}")
        print(" Review for namesake/false moves. [BRAND] = bare parent-brand domain (risky).")
        print("=" * 78)
        for r in rows:
            nm = " ".join(x for x in (r["first_name"], r["last_name"]) if x) or "?"
            tag = " [BRAND]" if _is_brand(r["email"]) else ""
            print(f"  #{r['id']:<7} {nm:<22} {r['former_org']:<22} -> "
                  f"{r['current_org']}{tag}")
            print(f"           {r['email']}")

        # successor stubs filed in the same window
        sucs = (await s.execute(text(
            "SELECT id, name, organization, created_at FROM lead_contacts "
            "WHERE found_via='successor_discovery' "
            "  AND created_at > now() - make_interval(hours => :h) "
            "ORDER BY created_at DESC"), {"h": SINCE_H})).mappings().all()
        if sucs:
            print(f"\n SUCCESSOR STUBS filed in last {SINCE_H}h: {len(sucs)}")
            for r in sucs:
                print(f"   stub #{r['id']:<7} {r['name']:<24} @ {r['organization']}")

        print("\n  Read-only. To revert bad moves: "
              "--rollback \"id,id\"  (restores org, deletes former edge).")


if __name__ == "__main__":
    asyncio.run(main())
