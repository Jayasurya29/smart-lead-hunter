"""known_hotel_domains -- derive the set of domains we KNOW belong to hotels.

The contact classifier (contact_intelligence.relevance / classify) already
accepts a `known_hotel_domains` set and short-circuits a contact to "relevant"
when its email domain is in it. That parameter was never populated, so real
properties whose NAME lacks a hospitality word ("The Elene", "Mohonk", "Oil Nut
Bay") were wrongly junked.

This helper builds that set from data we already trust: the hotel/lead/client
records' own websites + contact emails. Every domain we have already confirmed
as a hotel is, by definition, a known-hotel domain. Self-maintaining -- no
hand-typed list to rot.

Sources (all confirmed-hotel):
  potential_leads.hotel_website + .contact_email
  existing_hotels.hotel_website + .contact_email
  sap_clients.hotel_website + .email

Generic / personal / parent-brand domains are excluded so we never bless
gmail.com or a management-company parent as "a hotel".
"""

from __future__ import annotations

import re
from urllib.parse import urlparse

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

# Domains that are NEVER a specific hotel even if they appear in a record:
# free/personal mail, and big parent-brand domains that span many properties
# (a person on hilton.com is not necessarily "a hotel" -- the inverse-domain
# rule from the org cleanup). Keep this tight and conservative.
_EXCLUDE_DOMAINS = {
    "gmail.com",
    "yahoo.com",
    "hotmail.com",
    "outlook.com",
    "icloud.com",
    "aol.com",
    "live.com",
    "msn.com",
    "me.com",
    "comcast.net",
    "att.net",
    "proton.me",
    "protonmail.com",
    "googlemail.com",
    # consumer ISP mail domains -- a contact using these is a person, not a hotel
    "bellsouth.net",
    "sbcglobal.net",
    "verizon.net",
    "mac.com",
    "earthlink.net",
    "cox.net",
    "charter.net",
    "roadrunner.com",
    "rr.com",
    "ymail.com",
    "mail.com",
    "gmx.com",
    "zoho.com",
    "yandex.com",
    "qq.com",
    # industry media / booking aggregators -- not a specific property
    "hotel-online.com",
    "hotelplanner.com",
    "tripadvisor.com",
    "booking.com",
    "expedia.com",
    # broad parent-brand mail domains (sub-brands live on their own domains)
    "hilton.com",
    "marriott.com",
    "ihg.com",
    "hyatt.com",
    "accor.com",
    "wyndham.com",
    "choicehotels.com",
    "bestwestern.com",
    "radissonhotels.com",
}

_CACHE: dict[str, set[str]] = {}


def _domain_from_url(value: str | None) -> str:
    """Extract a bare registrable-ish domain from a website URL or email."""
    if not value:
        return ""
    v = value.strip().lower()
    if not v:
        return ""
    if "@" in v and "/" not in v:
        # looks like an email
        return v.split("@")[-1].strip()
    # website URL -- ensure urlparse sees a scheme
    if not re.match(r"^[a-z]+://", v):
        v = "http://" + v
    host = urlparse(v).netloc or ""
    host = host.split(":")[0]  # strip port
    if host.startswith("www."):
        host = host[4:]
    return host.strip()


async def get_known_hotel_domains(session: AsyncSession, *, use_cache: bool = True) -> set[str]:
    """Return the set of domains confirmed to belong to a hotel/lead/client.

    Cached process-wide (set use_cache=False to force a rebuild).
    """
    if use_cache and "domains" in _CACHE:
        return _CACHE["domains"]

    domains: set[str] = set()
    # IMPORTANT: harvest ONLY from the property's own WEBSITE columns, never
    # from contact_email. A website domain IS the property; a contact's email
    # domain is just whoever corresponded -- people use personal ISP mail
    # (bellsouth.net, mac.com), which would wrongly bless those as "hotels".
    queries = [
        "SELECT hotel_website FROM potential_leads",
        "SELECT hotel_website FROM existing_hotels",
        "SELECT hotel_website FROM sap_clients",
    ]
    for q in queries:
        try:
            rows = (await session.execute(text(q))).all()
        except Exception:
            continue  # table/column may not exist in every environment
        for row in rows:
            for cell in row:
                d = _domain_from_url(cell)
                if d and d not in _EXCLUDE_DOMAINS and "." in d and len(d) >= 4:
                    domains.add(d)

    if use_cache:
        _CACHE["domains"] = domains
    return domains


def clear_cache() -> None:
    _CACHE.pop("domains", None)
