"""Client / competitor resolution for contact categorization.

The CLIENT vs PROSPECT distinction is NOT a stored guess — it is a live lookup
against your real customer data (the `sap_clients` table, imported from SAP
Business One). A contact is a CLIENT iff their email domain or company name
matches a current SAP customer. The moment a prospect starts buying and lands
in SAP, the next enrichment pass re-derives them as CLIENT automatically — no
manual relabeling.

COMPETITOR is matched against a maintained seed list of hotel-uniform / linen /
workwear companies, so JA never pitches a competitor.

Both lookups are cheap (in-memory sets) so they can re-run on every pass.
"""

import re
from typing import Optional

from sqlalchemy import text

# Hotel-uniform / hospitality-linen / workwear competitors (seed list — extend
# freely). Matched by domain root or normalized name. These are companies that
# SELL uniforms to hotels, i.e. JA's competitors — flag, never pitch.
COMPETITOR_SEEDS = {
    "unifirst",
    "cintas",
    "aramark",
    "landsend",
    "lands end",
    "cleanuniform",
    "clean uniform",
    "noelasmar",
    "noel asmar",
    "tilit",
    "tilitnyc",
    "stockmfgco",
    "stock mfg",
    "fashionizer",
    "gadol",
    "gadolcisa",
    "themadisoncollection",
    "madison collection",
    "spikysport",
    "spiky",
    "lazzarusa",
    "alsco",
    "mission linen",
    "missionlinen",
    "g&k",
    "g and k",
    "superioruniform",
    "superior uniform",
    "fechheimer",
    "uniformadvantage",
    "uniform advantage",
    "sharperuniforms",
    "sharper uniforms",
    "averills",
    "edwardsgarment",
    "edwards garment",
    "chefworks",
    "chef works",
}

# Personal / free email providers — a contact here with no company is PERSONAL.
PERSONAL_EMAIL_DOMAINS = {
    "gmail.com",
    "yahoo.com",
    "hotmail.com",
    "outlook.com",
    "aol.com",
    "icloud.com",
    "me.com",
    "msn.com",
    "live.com",
    "proton.me",
    "protonmail.com",
    "ymail.com",
    "comcast.net",
    "att.net",
    "verizon.net",
    "sbcglobal.net",
    "bellsouth.net",
    "cox.net",
    "mac.com",
    "mail.com",
    "googlemail.com",
}


def normalize_name(name: str) -> str:
    """Mirror of sap_import._normalize_name so matches line up with stored
    customer_name_normalized values."""
    if not name:
        return ""
    n = name.lower().strip()
    n = re.sub(r"\s+", " ", n)
    n = re.sub(r"\b(llc|inc|corp|ltd|co)\b\.?", "", n)
    return n.strip()


def _domain(email: str) -> str:
    return email.split("@")[-1].lower().strip() if email and "@" in email else ""


def _domain_root(domain: str) -> str:
    """'mail.townepark.com' -> 'townepark'. Used for fuzzy seed/domain match."""
    if not domain:
        return ""
    parts = domain.split(".")
    return parts[-2] if len(parts) >= 2 else parts[0]


class ClientResolver:
    """Loads SAP client domains + names once, then resolves categories in
    memory. Rebuild it (or call load()) after a SAP import to pick up new
    clients."""

    def __init__(self):
        self.client_domains: set[str] = set()
        self.client_name_roots: set[str] = set()

    async def load(self, session) -> "ClientResolver":
        rows = (
            await session.execute(
                text("SELECT customer_name_normalized, email, hotel_website " "FROM sap_clients")
            )
        ).all()
        for norm_name, email, website in rows:
            if norm_name:
                self.client_name_roots.add(norm_name)
            for src in (email, website):
                full = _domain(src or "")
                # Skip freemail/ISP providers. A customer entered with a gmail/
                # bellsouth/yahoo address must NOT turn that whole provider into
                # a "client domain" — that would match every personal email.
                if full in PERSONAL_EMAIL_DOMAINS:
                    continue
                d = full or _domain_root(
                    (website or "").replace("https://", "").replace("http://", "")
                )
                if d:
                    self.client_domains.add(_domain_root(d) if "." in d else d)
        return self

    def is_client(self, organization: Optional[str], email: str) -> bool:
        d_root = _domain_root(_domain(email))
        if d_root and d_root in self.client_domains:
            return True
        norm = normalize_name(organization or "")
        if norm and norm in self.client_name_roots:
            return True
        return False


def is_competitor(organization: Optional[str], email: str) -> bool:
    d_root = _domain_root(_domain(email))
    norm = normalize_name(organization or "")
    if d_root and any(d_root == s.replace(" ", "") for s in COMPETITOR_SEEDS):
        return True
    if norm and any(s in norm for s in COMPETITOR_SEEDS):
        return True
    return False


def is_personal(organization: Optional[str], email: str) -> bool:
    return _domain(email) in PERSONAL_EMAIL_DOMAINS and not (organization or "").strip()


# JA's OWN uniform/apparel/textile SUPPLIERS (SAP vendor master, hand-filtered
# to real apparel suppliers). A contact here SELLS to JA -> seller. Matched by
# domain root or substring of the normalized org name, like COMPETITOR_SEEDS.
VENDOR_SEEDS = {
    "4imprint",
    "a+ career apparel",
    "ad products bazaar",
    "adar medical uniforms",
    "alessa wholesale",
    "alphabroder",
    "american seashore",
    "anchor uniforms",
    "atlas levy sewing machine",
    "augusta sportswear",
    "badger sportswear",
    "bandanna",
    "berne apparel",
    "bows-n-ties",
    "brook taverner",
    "buttonology",
    "c i iblu",
    "careismatic brands",
    "carr textile corporation",
    "cawley",
    "charles river apparel",
    "chef works",
    "cobmex",
    "collars and co",
    "cruz label",
    "cutter & buck",
    "d zinman textiles ltee",
    "daystar",
    "daytona trimming",
    "dorfman milano",
    "east coast umbrella",
    "edwards garment",
    "eiseman-ludmar",
    "elegancia tropical genuine panama hats",
    "embroservice",
    "emmaline bags",
    "erb safety",
    "excel labels plus",
    "fabian couture",
    "fabric base",
    "fashion biz",
    "first class uniforms",
    "footwear specialties",
    "game apparel",
    "game sportswear",
    "global industrial",
    "goldstar tool",
    "hamburger woolen",
    "happy chef",
    "hero's pride",
    "huzhou ruibang glove",
    "id identity",
    "j & d sewing",
    "j & f promotions",
    "j r embroidery",
    "jaazer apparel",
    "jazun embroidery",
    "jiangsu pinytex textile dyeing & finishing",
    "karlowsky",
    "kati sportcap & bag",
    "kufner textile corporation",
    "l c tex",
    "landau",
    "liberty uniforms",
    "madeira",
    "midway cap",
    "occunomix",
    "omni apparel",
    "paragon",
    "peak premium",
    "philips uniform mfg",
    "pinnacle textile",
    "prenusac",
    "products from abroad",
    "ralawise",
    "reflective apparel factory",
    "richard tie fabrics",
    "richpeace",
    "robert kaufman",
    "rosa fabrics",
    "rothco",
    "s&s activewear",
    "samuel broome",
    "sanmar",
    "scott notions",
    "shoes for crews",
    "simon jersey",
    "superior uniform",
    "texcauca s a",
    "textiles south",
    "the tie bar",
    "timeless trims",
    "tom gutie",
    "tootal-filtex",
    "transfer express",
    "tri-mountain",
    "tsf sportswear",
    "uline",
    "uncommon chef",
    "uniforms unlimited",
    "uniqlo",
    "united thread",
    "universal sewing",
    "vantage apparel",
    "watermen",
    "wawak",
    "wet effect",
    "white swan brands",
    "wolfmark neckwear",
    "workwear outfitters",
    "yarrington mills",
}


# Generated vendor seeds from VENDOR_LIST.xlsx (via import_vendor_list.py).
# Loaded if present; falls back to the in-code VENDOR_SEEDS below otherwise.
try:
    from app.config.vendor_seeds import (
        VENDOR_NAME_SEEDS as _GEN_VENDOR_NAME_SEEDS,
        VENDOR_DOMAIN_SEEDS as _GEN_VENDOR_DOMAIN_SEEDS,
    )
except Exception:  # noqa: BLE001 - generated file may not exist yet
    _GEN_VENDOR_NAME_SEEDS = set()
    _GEN_VENDOR_DOMAIN_SEEDS = set()


def is_vendor(organization, email):
    """True if the contact is at one of JA's known apparel suppliers.

    Uses the generated VENDOR_LIST.xlsx seeds (domain exact-match + name
    substring) plus the in-code VENDOR_SEEDS as a fallback so HUAFANG and the
    rest of the real vendor master are recognized."""
    d = _domain(email)
    d_root = _domain_root(d)
    norm = normalize_name(organization or "")
    # 1) exact domain against generated supplier domains (full domain or root)
    if d and (d in _GEN_VENDOR_DOMAIN_SEEDS or d_root in _GEN_VENDOR_DOMAIN_SEEDS):
        return True
    # 2) name substring against generated name seeds + in-code fallback
    name_seeds = _GEN_VENDOR_NAME_SEEDS or VENDOR_SEEDS
    if norm and any(s in norm for s in name_seeds):
        return True
    # 3) legacy in-code domain-root match (kept for safety)
    if d_root and any(d_root == s.replace(" ", "") for s in VENDOR_SEEDS):
        return True
    return False
