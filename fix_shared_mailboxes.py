"""Shared-mailbox fix v3 (2026-06-05): role inboxes posing as people.

v2 DRY-RUN caught a trap before it shipped: bare ap/ar suffixes matched
real surnames (Baraskar, Dunlap, Salazar...) whose first.last@ emails
equal their own names. v3 rules:

  - bare 2-letter ap/ar ONLY when separated as their own token
    (fllci_ap, no.reply) OR when the prefix is the brand itself
    (rosenAP @ rosenhotels.com, eastmiamiAP @ East Miami Hotel)
  - display names equal to the FULL email, or the literal string
    'None', now count as nameless (catches rosenpurchasing@,
    ap@nationalhotel.com, raorders@ that v1/v2 missed)
  - ambiguous brandAP/AR leftovers go to a REVIEW list, never applied

Idempotent. Requires migration 031.

Run:
    python fix_shared_mailboxes.py            # dry-run
    python fix_shared_mailboxes.py --apply
"""

import asyncio
import re
import sys

from sqlalchemy import text

from app.database import async_session

ROLE_LOCALS = {
    "ap": "Accounts Payable inbox",
    "ar": "Accounts Receivable inbox",
    "accountspayable": "Accounts Payable inbox",
    "payables": "Accounts Payable inbox",
    "accounting": "Accounting inbox",
    "payroll": "Payroll inbox",
    "billing": "Billing inbox",
    "invoices": "Billing inbox",
    "csr": "Customer Service inbox",
    "customerservice": "Customer Service inbox",
    "service": "Customer Service inbox",
    "info": "General inbox",
    "contact": "General inbox",
    "hello": "General inbox",
    "office": "Office inbox",
    "admin": "Admin inbox",
    "sales": "Sales inbox",
    "orders": "Orders inbox",
    "order": "Orders inbox",
    "purchasing": "Purchasing inbox",
    "procurement": "Procurement inbox",
    "receiving": "Receiving inbox",
    "warehouse": "Warehouse inbox",
    "frontdesk": "Front Desk inbox",
    "frontdeskmanager": "Front Desk inbox",
    "frontoffice": "Front Office inbox",
    "reservations": "Reservations inbox",
    "reservation": "Reservations inbox",
    "concierge": "Concierge inbox",
    "housekeeping": "Housekeeping inbox",
    "engineering": "Engineering inbox",
    "security": "Security inbox",
    "hr": "HR inbox",
    "humanresources": "HR inbox",
    "careers": "HR inbox",
    "jobs": "HR inbox",
    "marketing": "Marketing inbox",
    "events": "Events inbox",
    "banquets": "Banquets inbox",
    "catering": "Catering inbox",
    "spa": "Spa inbox",
    "press": "Press inbox",
    "media": "Press inbox",
    "pr": "Press inbox",
    "support": "Support inbox",
    "help": "Support inbox",
    "pm": "Property Management inbox",
    "apm": "Property Management inbox",
    "om": "Operations inbox",
    "ops": "Operations inbox",
    "fd": "Front Desk inbox",
    "fo": "Front Office inbox",
    "quotes": "Quotes inbox",
    "inquiry": "General inbox",
    "inquiries": "General inbox",
    "noreply": "No-reply sender",
    "donotreply": "No-reply sender",
    "gm": "General Manager inbox",  # keeps DM - see below
}

ROLE_SUFFIXES = {
    "procurement": "Procurement inbox",
    "purchasing": "Purchasing inbox",
    "orders": "Orders inbox",
    "reservations": "Reservations inbox",
    "frontdesk": "Front Desk inbox",
    "accounting": "Accounting inbox",
    "accountspayable": "Accounts Payable inbox",
    "payables": "Accounts Payable inbox",
    "sales": "Sales inbox",
    "service": "Customer Service inbox",
    "billing": "Billing inbox",
    "marketing": "Marketing inbox",
    "events": "Events inbox",
    "info": "General inbox",
    "accounts": "Accounting inbox",
}

# bare ap/ar: ONLY as a separated token or with a brand prefix
SHORT_TOKENS = {
    "ap": "Accounts Payable inbox",
    "ar": "Accounts Receivable inbox",
}

FREEMAIL_DOMAINS = {
    "yahoo.com", "gmail.com", "aol.com", "hotmail.com", "outlook.com",
    "icloud.com", "msn.com", "live.com", "bellsouth.net", "comcast.net",
}

COMPETITOR_DOMAINS = {"reflectiveapparel.com"}

_CLEAN = re.compile(r"[^a-z0-9]")
_SEP = re.compile(r"[._\-]+")


def classify(local):
    """Exact + long-suffix detection (safe without context)."""
    key = _CLEAN.sub("", local.lower())
    if not key:
        return None
    if key in ROLE_LOCALS:
        return ROLE_LOCALS[key]
    for suf, label in ROLE_SUFFIXES.items():
        if key.endswith(suf) and len(key) > len(suf):
            return label
    return None


def classify_full(local, domain, org):
    """classify() plus context-guarded bare ap/ar:
    - separated token: fllci_ap, accounts.AP
    - brand prefix: rosenAP when 'rosen' is in the domain or org
    Never matches first.last surnames (baraskAR, dunlAP)."""
    base = classify(local)
    if base:
        return base, False
    key = _CLEAN.sub("", local.lower())
    tokens = [t for t in _SEP.split(local.lower()) if t]
    if len(tokens) >= 2 and tokens[-1] in SHORT_TOKENS and len(tokens[0]) >= 2:
        # separated form — but not a person's initials pattern like j.ap? require
        # the non-suffix part to not be a single letter
        if all(len(t) == 1 for t in tokens[:-1]):
            return None, False
        return SHORT_TOKENS[tokens[-1]], False
    for suf, label in SHORT_TOKENS.items():
        if key.endswith(suf) and len(key) - len(suf) >= 4:
            prefix = key[: -len(suf)]
            root = (domain or "").lower()
            if root in FREEMAIL_DOMAINS:
                return label, True  # personal provider — never auto-flag
            haystack = _CLEAN.sub("", root) + _CLEAN.sub("", (org or "").lower())
            if prefix and prefix in haystack:
                return label, False
            return label, True  # review-only candidate
    return None, False


_NULLISH = ("none", "null", "n/a", "-")


def _clean_field(v):
    v = (v or "").strip()
    return "" if v.lower() in _NULLISH else v


def name_is_derived(display, first, last, local, domain=""):
    disp = _clean_field(display)
    first = _clean_field(first)
    last = _clean_field(last)
    lkey = _CLEAN.sub("", local.lower())
    ekey = _CLEAN.sub("", (local + domain).lower())
    if (first or "").strip() or (last or "").strip():
        joined = _CLEAN.sub("", ((first or "") + (last or "")).lower())
        return joined in (lkey, ekey)
    d = _CLEAN.sub("", disp.lower())
    return (not d) or d in (lkey, ekey)


async def main(apply):
    async with async_session() as db:
        rows = (
            await db.execute(
                text(
                    "SELECT id, email, display_name, first_name, last_name, "
                    "       inferred_role, is_decision_maker, contact_category, organization "
                    "FROM contacts WHERE email IS NOT NULL"
                )
            )
        ).mappings().all()

        flagged, competitors, kept_named, review = [], [], [], []
        for r in rows:
            email = (r["email"] or "").strip().lower()
            if "@" not in email:
                continue
            local, _, domain = email.partition("@")

            if domain in COMPETITOR_DOMAINS and (r["contact_category"] or "") != "competitor":
                competitors.append(r)

            label, review_only = classify_full(local, domain, r["organization"])
            if not label:
                continue
            derived = name_is_derived(
                r["display_name"], r["first_name"], r["last_name"], local, domain
            )
            if review_only:
                if derived:
                    review.append((r, label))
                continue
            if derived:
                keep_dm = _CLEAN.sub("", local) == "gm"
                flagged.append((r, label, keep_dm))
            else:
                kept_named.append((r, label))

        print(f"shared mailboxes detected: {len(flagged)}")
        for r, label, keep_dm in flagged:
            dm = " [keeps DM]" if keep_dm else (" [DM cleared]" if r["is_decision_maker"] else "")
            print(f"  #{r['id']:>5}  {r['email']:<45} '{r['display_name']}' -> '{label}'{dm}"
                  f"  ({r['organization'] or '-'})")

        print(f"\nREVIEW ONLY (ambiguous brand+ap/ar, NOT applied): {len(review)}")
        for r, label in review:
            print(f"  #{r['id']:>5}  {r['email']:<45} '{r['display_name']}' ({label}?)")

        print(f"\nrole addresses KEPT as people (real name attached): {len(kept_named)}")
        for r, label in kept_named:
            print(f"  #{r['id']:>5}  {r['email']:<45} '{r['display_name']}'")

        print(f"\ncompetitor flips: {len(competitors)}")
        for r in competitors:
            print(f"  #{r['id']:>5}  {r['email']:<45} '{r['contact_category']}' -> 'competitor'")

        if not apply:
            print("\nDRY RUN - re-run with --apply to write.")
            return

        for r, label, keep_dm in flagged:
            await db.execute(
                text(
                    "UPDATE contacts SET is_shared_mailbox = TRUE, "
                    "display_name = :label, inferred_role = :label, "
                    "first_name = NULL, last_name = NULL, "
                    "is_decision_maker = :dm WHERE id = :cid"
                ),
                {"label": label, "dm": keep_dm and bool(r["is_decision_maker"]), "cid": r["id"]},
            )
        for r in competitors:
            await db.execute(
                text("UPDATE contacts SET contact_category = 'competitor' WHERE id = :cid"),
                {"cid": r["id"]},
            )
        await db.commit()
        print(f"\nAPPLIED: {len(flagged)} shared mailboxes, {len(competitors)} competitor flips.")


if __name__ == "__main__":
    asyncio.run(main(apply="--apply" in sys.argv))
