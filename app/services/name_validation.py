"""name_validation.py -- decide whether a personal name may attach to an email.

ONE pure function, used in TWO places so sync and backfill can never diverge:
  * app/services/inbox_sync.py finalizes scraped names through name_fits_email()
    so the bug never enters the DB again.
  * backfill_bad_names.py runs the SAME function over existing rows.

The problem it solves: during sync, a personal display name scraped from a
forwarded thread gets attached to the wrong address -- a shared role inbox
(ap@, accountspayable@, ssc.apcustomersrvc) ends up "named" after a person who
merely appeared in the thread, and occasionally a first.last@ address gets a
name that isn't theirs.

Design notes:
  - PURE: no DB, no network, deterministic. Same input -> same verdict anywhere.
  - CONSERVATIVE: only two failure verdicts; everything ambiguous is OK (kept).
  - INITIAL-AWARE: jcavaliere ~ "Luis Cavalieri", smcpherson ~ "Sandy McPhearson"
    are treated as MATCHES (flast / finitial+last), never flagged.
  - Role inboxes lose only the STRUCTURED identity (first/last); a human label
    survives as display context, so a real sales rep is not destroyed.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

# Role / shared mailbox local-parts. Matched at token boundaries (split on
# . _ -) OR as a known glued substring -- so "salazar" never matches "sales"
# but "ssc.apcustomersrvc" and "ap.inquiry" do.
_ROLE_TOKENS = {
    "ap",
    "ar",
    "accounting",
    "accountspayable",
    "accountsreceivable",
    "accounts",
    "payable",
    "payables",
    "receivable",
    "receivables",
    "purchasing",
    "procurement",
    "billing",
    "invoice",
    "invoices",
    "info",
    "sales",
    "marketing",
    "noreply",
    "donotreply",
    "reservations",
    "reservation",
    "booking",
    "bookings",
    "frontdesk",
    "concierge",
    "events",
    "catering",
    "hr",
    "careers",
    "jobs",
    "recruiting",
    "support",
    "help",
    "helpdesk",
    "service",
    "customerservice",
    "custsvc",
    "admin",
    "administrator",
    "office",
    "mail",
    "contact",
    "team",
    "hello",
    "orders",
    "order",
    "ssc",
    "apcustomersrvc",
    "apcustomerservice",
    "webmaster",
    "postmaster",
    "do_not_reply",
    "no_reply",
    "quotes",
    "quote",
    "enquiries",
    "enquiry",
    "inquiry",
    "inquiries",
    "feedback",
    "notifications",
}
_ROLE_GLUED = (
    "accountspayable",
    "accountsreceivable",
    "customerservice",
    "customersrvc",
    "apcustomer",
    "noreply",
    "donotreply",
    "frontdesk",
    "helpdesk",
    "purchasing",
    "procurement",
    "reservations",
    "donotreplay",
)

# Words that, if they appear in the "name", mean the name is NOT a person --
# it's an echo of the role/company (e.g. "ACCTS. PAYABLE", "AP Lombardy",
# "FreightCenter Team"). Used to decide a role inbox's label is also junk.
_NONPERSONAL_NAME = re.compile(
    r"\b(accounts?|payable|receivable|a/?p|a/?r|accts?|dept|department|team|"
    r"administrator|admin|reception|frontdesk|front desk|billing|invoice|"
    r"purchasing|procurement|reservations?|noreply|no reply|do not reply|"
    r"customer service|support|sales|office|llc|inc|corp|co\.?$)\b",
    re.I,
)


@dataclass(frozen=True)
class NameVerdict:
    code: str  # 'OK' | 'ROLE' | 'MISMATCH'
    role_inbox: bool  # local part is a shared/role mailbox
    nonpersonal: bool  # the stored "name" is itself a role/company echo
    reason: str


def _local(email: str) -> str:
    return (email or "").split("@")[0].lower().strip()


def _flat(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def _local_tokens(local: str) -> list[str]:
    base = local.split("+")[0]
    return [re.sub(r"\d+", "", t) for t in re.split(r"[._\-]+", base) if t]


def is_role_inbox(email: str) -> bool:
    """True when the address is a shared/role mailbox, not a single human."""
    local = _local(email)
    if not local:
        return False
    toks = re.split(r"[._\-]+", local.split("+")[0])
    if any(t in _ROLE_TOKENS for t in toks):
        return True
    flat = _flat(local)
    return any(g in flat for g in _ROLE_GLUED)


def name_tokens(first: str, last: str, display: str) -> list[str]:
    src = " ".join([first or "", last or "", display or ""])
    return [t for t in re.split(r"[^a-z0-9]+", src.lower()) if len(t) >= 2]


def looks_personal_name(first: str, last: str, display: str) -> bool:
    """The stored name resembles a human (>=1 alpha token, not a role echo)."""
    toks = name_tokens(first, last, display)
    if not any(t.isalpha() for t in toks):
        return False
    label = " ".join([first or "", last or "", display or ""]).strip()
    if _NONPERSONAL_NAME.search(label):
        return False
    return True


def _is_clean_personal_local(local: str) -> bool:
    """local is a dotted first.last with both parts alpha and >=3 chars."""
    if is_role_inbox(local + "@x"):
        return False
    parts = local.split("+")[0].split(".")
    alpha = [p for p in parts if p.isalpha()]
    return len(alpha) >= 2 and all(len(p) >= 3 for p in alpha)


def _name_overlaps_local(local: str, ntoks: list[str]) -> bool:
    """Initial-aware overlap: any name token matches a local token, OR the
    glued local contains a name token, OR a flast/finitial pattern lines up."""
    if not ntoks:
        return True
    flat = _flat(local)
    ltoks = [t for t in _local_tokens(local) if t.isalpha() and len(t) >= 2]
    for nt in ntoks:
        if nt in flat or flat in nt:
            return True
        for lt in ltoks:
            if nt == lt or nt.startswith(lt) or lt.startswith(nt):
                return True
    # flast / finitial+last: a single glued local token whose tail is a surname
    if len(ltoks) == 1 and len(ltoks[0]) >= 4:
        glue = ltoks[0]
        for nt in ntoks:
            if len(nt) >= 4 and glue.endswith(
                nt
            ):  # smcpherson endswith mcpherson? close enough via tail
                return True
            if len(nt) >= 4 and nt in glue:
                return True
    return False


def name_fits_email(first: str, last: str, display: str, email: str) -> NameVerdict:
    """Single source of truth: may this personal name attach to this address?

    OK       -> keep the name as-is.
    ROLE     -> address is a shared/role inbox; do NOT store a structured
                personal identity (first/last). Caller keeps display label only,
                or clears it too if `nonpersonal` is True.
    MISMATCH -> personal address whose name shares nothing with it; the name was
                almost certainly scraped from the wrong thread -> clear it so it
                can be re-resolved. Never fabricate a replacement here.
    """
    role = is_role_inbox(email)
    personal = looks_personal_name(first, last, display)
    nonpersonal = bool(
        (first or last or display)
        and _NONPERSONAL_NAME.search(" ".join([first or "", last or "", display or ""]))
    )

    if role:
        # role inbox should never carry a structured personal identity
        return NameVerdict(
            "ROLE", True, nonpersonal, "shared/role inbox; personal identity not allowed"
        )

    if personal:
        local = _local(email)
        if _is_clean_personal_local(local):
            ntoks = name_tokens(first, last, display)
            if not _name_overlaps_local(local, ntoks):
                return NameVerdict(
                    "MISMATCH", False, False, "personal address; name shares no token with it"
                )

    return NameVerdict("OK", False, nonpersonal, "name plausible for this address")
