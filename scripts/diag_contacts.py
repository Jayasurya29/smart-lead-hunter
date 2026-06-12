#!/usr/bin/env python3
"""diag_contacts.py -- READ-ONLY diagnosis of contact-quality bugs.

Dumps the exact data behind three problems so the fixes are surgical:

  A. PERSONAL-EMAIL CONTACTS CARRYING A HOTEL ORG
     (e.g. adam.marquardt14@gmail.com shown under "Rosen") -- a freemail
     address should not inherit a hotel/operator account.

  B. SPAM / THROWAWAY-DOMAIN CONTACTS STILL IN THE DIRECTORY
     (e.g. *.cfd, eocworksprime.com, "Email Outreach Company") -- cold-
     outreach junk the inbox filter missed; should be category 'junk'.

  C. SPLIT PEOPLE -- the same human as two+ rows that didn't merge, usually
     because one row has no email to match on (e.g. Adam Stewart: one row with
     sri.sandals.com, one "Executive Chairman" with no email).

Plus a targeted look at any name you pass with --name "Adam Rosen".

Usage (repo root):
    python scripts/diag_contacts.py
    python scripts/diag_contacts.py --name "Adam Stewart"
    python scripts/diag_contacts.py --limit 40
"""

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

from sqlalchemy import text  # noqa: E402

from app.database import async_session  # noqa: E402

FREEMAIL = ("gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "aol.com",
            "icloud.com", "me.com", "live.com", "msn.com", "comcast.net",
            "ymail.com", "protonmail.com", "proton.me")

# Throwaway / cold-outreach TLDs and spam-y second-level patterns.
SUSPECT_TLD = (".cfd", ".info", ".help", ".click", ".sbs", ".top", ".xyz",
               ".online", ".site", ".live", ".shop", ".store", ".icu", ".buzz",
               ".rest", ".monster", ".quest", ".bond", ".cyou")


def _t(v, n):
    s = "" if v is None else str(v).replace("\n", " ").strip()
    return s if len(s) <= n else s[: n - 1] + "."


async def section_a(session):
    print("\n" + "=" * 78)
    print("A.  PERSONAL-EMAIL CONTACTS CARRYING A COMPANY/HOTEL ORG")
    print("    (freemail address but organization is set -- likely mis-assigned)")
    print("=" * 78)
    free_clause = " OR ".join(f"lower(email) LIKE '%@{d}'" for d in FREEMAIL)
    rows = (await session.execute(text(
        f"""
        SELECT id, display_name, email, organization, parent_company,
               management_company, contact_category, org_source, matched_hotel_id
        FROM contacts
        WHERE ({free_clause})
          AND COALESCE(organization,'') <> ''
          AND (contact_category IS NULL OR contact_category NOT IN
               ('junk','personal','seller','competitor'))
        ORDER BY organization, id
        LIMIT 60
        """
    ))).all()
    print(f"  {len(rows)} contact(s):\n")
    print(f"  {'id':>6}  {'name':20}  {'email':32}  {'org (suspect)':22}  {'org_source':12}")
    print("  " + "-" * 96)
    for r in rows:
        print(f"  {r.id:>6}  {_t(r.display_name,20):20}  {_t(r.email,32):32}  "
              f"{_t(r.organization,22):22}  {_t(r.org_source,12):12}")
    return [r.id for r in rows]


async def section_b(session):
    print("\n" + "=" * 78)
    print("B.  SPAM / THROWAWAY-DOMAIN CONTACTS NOT MARKED JUNK")
    print("    (newer junk TLDs the inbox filter's .info/.help/.click list missed)")
    print("=" * 78)
    tld_clause = " OR ".join(f"lower(email) LIKE '%{t}'" for t in SUSPECT_TLD)
    rows = (await session.execute(text(
        f"""
        SELECT id, display_name, email, organization, contact_category
        FROM contacts
        WHERE ({tld_clause})
          AND (contact_category IS NULL OR contact_category <> 'junk')
        ORDER BY email
        LIMIT 80
        """
    ))).all()
    print(f"  {len(rows)} contact(s) on suspect TLDs, NOT yet junk:\n")
    print(f"  {'id':>6}  {'name':20}  {'email':38}  {'org':22}  {'category':10}")
    print("  " + "-" * 100)
    for r in rows:
        print(f"  {r.id:>6}  {_t(r.display_name,20):20}  {_t(r.email,38):38}  "
              f"{_t(r.organization,22):22}  {_t(r.contact_category,10):10}")
    # also flag orgs literally named like outreach spam
    spam_org = (await session.execute(text(
        """
        SELECT id, display_name, email, organization, contact_category
        FROM contacts
        WHERE lower(organization) LIKE '%email outreach%'
           OR lower(organization) LIKE '%outreach company%'
           OR lower(organization) LIKE '%lead gen%'
        ORDER BY id LIMIT 40
        """
    ))).all()
    if spam_org:
        print(f"\n  + {len(spam_org)} contact(s) with outreach-spam ORG names:")
        for r in spam_org:
            print(f"    {r.id:>6}  {_t(r.display_name,20):20}  {_t(r.email,32):32}  "
                  f"{_t(r.organization,24):24}  [{_t(r.contact_category,8)}]")
    return [r.id for r in rows]


async def section_c(session):
    print("\n" + "=" * 78)
    print("C.  SPLIT PEOPLE -- same name, 2+ rows, NOT merged into one person_id")
    print("    (usually because one row has no email to match on)")
    print("=" * 78)
    rows = (await session.execute(text(
        """
        SELECT lower(trim(coalesce(first_name,'')||' '||coalesce(last_name,''))) AS nm,
               COUNT(*) AS n,
               COUNT(*) FILTER (WHERE email IS NULL OR email='') AS no_email,
               COUNT(DISTINCT organization) AS orgs_n,
               string_agg(DISTINCT coalesce(organization,'?'), ' | ') AS orgs
        FROM contacts
        WHERE coalesce(first_name,'')<>'' AND coalesce(last_name,'')<>''
          AND (contact_category IS NULL OR contact_category NOT IN ('junk','personal','seller','competitor'))
        GROUP BY nm
        HAVING COUNT(*) > 1
        ORDER BY (COUNT(*) FILTER (WHERE email IS NULL OR email='')) DESC, COUNT(*) DESC
        LIMIT 40
        """
    ))).all()
    print(f"  {len(rows)} name(s) with multiple rows (no_email>0 first -- the "
          "mergeable ones):\n")
    print(f"  {'rows':>4} {'noEml':>5} {'orgs':>4}  {'name':24}  organizations")
    print("  " + "-" * 92)
    for r in rows:
        print(f"  {r.n:>4} {r.no_email:>5} {r.orgs_n:>4}  {_t(r.nm,24):24}  {_t(r.orgs,40)}")


async def by_name(session, name):
    print("\n" + "=" * 78)
    print(f"DETAIL: every row matching name ~ {name!r}")
    print("=" * 78)
    rows = (await session.execute(text(
        """
        SELECT id, display_name, email, organization, inferred_role, title,
               seniority, contact_category, is_decision_maker,
               matched_hotel_id, interaction_count, org_source
        FROM contacts
        WHERE lower(coalesce(first_name,'')||' '||coalesce(last_name,'')) LIKE :p
           OR lower(coalesce(display_name,'')) LIKE :p
        ORDER BY id
        """
    ), {"p": f"%{name.lower()}%"})).all()
    print(f"  {len(rows)} row(s):\n")
    for r in rows:
        print(f"  id={r.id}  cat={r.contact_category}  dm={r.is_decision_maker}  "
              f"org_source={r.org_source}")
        print(f"     name={r.display_name!r}  email={r.email!r}")
        print(f"     org={r.organization!r}  role={(r.title or r.inferred_role)!r}  "
              f"sen={r.seniority}  hotel_id={r.matched_hotel_id}  "
              f"interactions={r.interaction_count}")
        # affiliations are keyed by contact id (person_type='contact', person_id=id)
        aff = (await session.execute(text(
            "SELECT relationship, account_type, account_name, scope, source "
            "FROM contact_affiliations WHERE person_type='contact' AND person_id=:id"
        ), {"id": r.id})).all()
        if aff:
            for a in aff:
                print(f"       aff: {a.relationship} {a.account_type} "
                      f"{a.account_name or '(id)'} scope={a.scope} src={a.source}")
        print()


async def run(name, limit):
    async with async_session() as session:
        if name:
            await by_name(session, name)
            return
        await section_a(session)
        await section_b(session)
        await section_c(session)
    print("\n" + "=" * 78)
    print("Read-only -- nothing changed. Share this output and we fix each root cause:")
    print("  A -> stop freemail contacts inheriting a hotel org")
    print("  B -> extend the spam-TLD/outreach filter + reclassify these as junk")
    print("  C -> merge name+org matches even when one side has no email")
    print("=" * 78)


def main():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--name", type=str, default="", help="Dump every row for one person")
    p.add_argument("--limit", type=int, default=60)
    args = p.parse_args()
    asyncio.run(run(args.name, args.limit))


if __name__ == "__main__":
    main()
