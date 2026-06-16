"""buying_signal_engine.py -- Phase 1 content-based opportunity engine (standalone).

Scores an email THREAD by what's being SAID and attributes it to the EXTERNAL
buyer, not email volume and not JA's own staff. No DB, no network, deterministic.

Proven thesis (2026-06-15): 4 real JA threads (Sedano's, Towne Park, Kimpton,
Ritz Grand Cayman) all score ~100 here; the old volume formula scored them ~3.

Three parts:
  1. SIGNALS  -- tiered keyword extraction (order-confirmed > buying > project >
                 dialogue > service > noise), EN + ES, deal-size, sales-stage.
  2. ATTRIBUTION -- given a thread's messages, find the single external
                 counterparty it is WITH: exclude JA (OWN_DOMAINS) and CC'd
                 vendors, collapse the buyer's multiple domains to one person via
                 their signature, read THEIR org/property from the signature.
  3. SCORE    -- blend strongest signal + stage + who-they-are bumps into 0-100
                 with an explainable reason and a sales-stage label.

Public API:
  score_thread(messages, *, own_domains, vendor_domains=None) -> ThreadScore
  score_text(body, **ctx) -> SignalResult        (single-blob convenience)
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

# -- JA's own domains (buyer is never one of these) --------------------------
DEFAULT_OWN_DOMAINS = {"jauniforms.com", "jauniforms.org"}

# -- signal phrase banks (EN + ES), word-boundary, case-insensitive ----------
# ORDER-CONFIRMED outranks everything: the deal is won/closing.
_ORDER_CONFIRMED = [
    r"\bapproved\b",
    r"\bproof (?:is )?approved\b",
    r"\bproceed with (?:the )?order\b",
    r"\bplace (?:an|the) order\b",
    r"\bplacing an order\b",
    r"\btrying to place an order\b",
    r"\border right away\b",
    r"\bwe selected\b",
    r"\bwe chose\b",
    r"\bgo ahead\b",
    r"\bformal approval\b",
    r"\bmove forward\b",
    r"\bconfirmed\b",
    r"\bgreen ?light\b",
    # ES
    r"\baprobado\b",
    r"\baprobada\b",
    r"\bproceder con (?:el|la) (?:orden|pedido)\b",
    r"\bhacer el pedido\b",
    r"\bconfirmar (?:la )?orden\b",
]
_BUYING = [
    r"\bquote\b",
    r"\brfq\b",
    r"\bpurchase order\b",
    r"\bp\.?o\.?\s*#",
    r"\bpricing\b",
    r"\bprice list\b",
    r"\blead time\b",
    r"\breorder\b",
    r"\bre-order\b",
    r"\bsize run\b",
    r"\bsample(s)?\b",
    r"\bproforma\b",
    r"\binvoice\b",
    r"\bwe need\b",
    r"\blooking for\b",
    r"\border form\b",
    r"\bquantit(y|ies)\b",
    r"\bcost (per|each)\b",
    r"\bwhat(?:'s| is| would be) the price\b",
    # ES
    r"\bcotizaci[o\u00f3]n\b",
    r"\bpedido\b",
    r"\borden\b",
    r"\bfactura\b",
    r"\bprecio\b",
    r"\bmuestra(s)?\b",
    r"\bnecesitamos\b",
    r"\bcantidad(es)?\b",
]
_PROJECT = [
    r"\bopening\b",
    r"\bpre-?opening\b",
    r"\brenovat",
    r"\brebrand",
    r"\bnew property\b",
    r"\buniform program\b",
    r"\brollout\b",
    r"\blaunch\b",
    r"\bgrand opening\b",
    r"\brefresh\b",
    r"\bconversion\b",
    r"\bnew outlet\b",
    r"\bnew restaurant\b",
    r"\bcore program\b",
    r"\bstandardization\b",
]
_DIALOGUE = [
    r"\bproposal\b",
    r"\bcatalog",
    r"\blookbook\b",
    r"\bspec sheet\b",
    r"\bfollow(ing)? up\b",
    r"\bmeeting\b",
    r"\bschedule\b",
    r"\bpresentation\b",
    r"\bdeck\b",
    r"\bintro(duction)?\b",
    r"\bnext steps\b",
    r"\bcircle back\b",
    r"\bzoom\b",
    r"\b(?:a|the|our|phone|schedule a|jump on a|hop on a) call\b",
]
_SERVICE = [
    r"\bshipment\b",
    r"\btracking\b",
    r"\breturn\b",
    r"\bexchange\b",
    r"\bdamaged\b",
    r"\bcomplaint\b",
    r"\bdefect",
    r"\bwrong size\b",
    r"\bbackorder",
    r"\bstatus of\b",
    r"\bdelivery\b",
    r"\bcredit memo\b",
]
_NOISE = [
    r"\bunsubscribe\b",
    r"\bview (this email )?in (your )?browser\b",
    r"\bnewsletter\b",
    r"\bno-?reply\b",
    r"\bdo not reply\b",
    r"\bwebinar\b",
    r"\bfollow us on\b",
    r"\bprivacy policy\b",
    r"\bterms of service\b",
    # vendor-spam / promo markers (someone selling a SaaS/tool TO us)
    r"\b\d{1,2}%\s*off\b",
    r"\bpromo code\b",
    r"\bsubscription\b",
    r"\bsign up\b",
    r"\blast call\b",
    r"\bvalid only for\b",
    r"\blimited time\b",
    r"\bact now\b",
    r"\bfree trial\b",
    r"\bbook a demo\b",
    r"\bhappy prospecting\b",
    r"\b\d{3,}m\+?\s*profiles\b",
    r"\bcopyright \u00a9\b",
    r"\ball rights reserved\b",
]

_BANKS = [
    ("order_confirmed", _ORDER_CONFIRMED),
    ("buying", _BUYING),
    ("project", _PROJECT),
    ("dialogue", _DIALOGUE),
    ("service", _SERVICE),
    ("noise", _NOISE),
]
_BASE = {
    "order_confirmed": 96,
    "buying": 88,
    "project": 74,
    "dialogue": 55,
    "service": 40,
    "noise": 5,
    "none": 15,
}

# sales-stage label derived from the strongest signal present
_STAGE = {
    "order_confirmed": "approved/ordered",
    "buying": "proposal/quote",
    "project": "prospecting",
    "dialogue": "dialogue",
    "service": "existing-service",
    "noise": "noise",
    "none": "unknown",
}

# deal-size patterns: "Total: 172 aprons", "60 employees", "3 sets each", "$48.08"
_RE_TOTAL = re.compile(r"\btotal[:\s]+(\d{2,5})\b", re.I)
_RE_EMP = re.compile(r"\b(\d{2,5})\s*(?:employees?|emp|staff|associates?|team members?)\b", re.I)
_RE_SETS = re.compile(
    r"\b(\d{1,3})\s*sets?\s*(?:of\s+)?(?:bottom\+?top|per (?:employee|person)|each)\b", re.I
)
_RE_MONEY = re.compile(r"\$\s?(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)")
_RE_QTY_TABLE = re.compile(
    r"(?:^|\n)\s*\d{2,4}\s*[\u2013\-]\s*\d{1,3}\s*(?:\n|$)"
)  # "201 - 6" lines

# Internal "deal substance" markers: JA actively building/specifying the item =
# the deal is real work in progress, not idle. style#, vendor code, SKU activity,
# color/spec decisions, item creation. These corroborate a real buyer's intent.
_SUBSTANCE = [
    r"\bvendor (?:code|style)\b",
    r"\bstyle (?:name|#|number)\b",
    r"\bv\d{3}\b",
    r"\bsku\b",
    r"\bsku generate\b",
    r"\bitem (?:#|number|code|ready)\b",
    r"\badd (?:the )?color\b",
    r"\bcreate (?:the )?items?\b",
    r"\bdeactivate\b",
    r"\blookbook\b",
    r"\blayout\b",
    r"\bproof\b",
    r"\bsample order\b",
    r"\bquote\s*#?\s*\d+\b",
    r"\bcost\b",
    r"\bsizes?\b",
]


_PRODUCTS = [
    ("apron", r"\baprons?\b|\bdelantal(?:es)?\b"),
    ("polo", r"\bpolos?\b"),
    ("dress", r"\bdress(?:es)?\b|\bvestidos?\b"),
    ("vest", r"\bvests?\b"),
    ("jacket", r"\bjackets?\b|\bchaquetas?\b|\bblazers?\b|\bwindbreakers?\b"),
    ("shirt", r"\bshirts?\b|\bblouses?\b|\bcamisas?\b"),
    ("pants", r"\bpants?\b|\btrousers?\b"),
    ("skirt", r"\bskirts?\b"),
    ("scrub", r"\bscrubs?\b"),
    ("chef coat", r"\bchef\s*coats?\b"),
    ("tunic", r"\btunics?\b"),
    ("coverall", r"\bcoveralls?\b"),
    ("smock", r"\bsmocks?\b"),
    ("tie", r"\bneck\s*ties?\b"),
    ("cap", r"\bcaps?\b|\bhats?\b"),
    ("FOH uniform program", r"\bfoh\s+uniforms?\b"),
    ("uniform program", r"\buniform program\b|\bcore program\b"),
    ("uniforms", r"\buniforms?\b|\buniformes?\b"),
]


def extract_products(text: str) -> list[str]:
    """Product nouns named in the thread (specific garments first), deduped."""
    text = text or ""
    hits = [label for label, pat in _PRODUCTS if re.search(pat, text, re.I)]
    specific = [h for h in hits if h not in ("uniforms", "uniform program")]
    if specific:
        hits = specific + [h for h in hits if h == "uniform program"][:1]
    return hits[:4]


def substance_score(internal_text: str) -> tuple[int, list[str]]:
    """How much real product/spec/production work is in the internal thread.
    Returns (0-15 bump, matched markers). More distinct substance = more real."""
    hits = _find(internal_text or "", _SUBSTANCE)
    if not hits:
        return 0, []
    # 3 markers -> deal is being actively built; cap the bump so buyer intent
    # stays the lead signal.
    bump = min(15, 4 + 3 * len(hits))
    return bump, hits


@dataclass
class SignalResult:
    bucket: str
    score: int
    stage: str
    reason: str
    tags: dict = field(default_factory=dict)
    deal_size: str | None = None


@dataclass
class ThreadScore:
    score: int
    stage: str
    bucket: str
    buyer_email: str | None
    buyer_name: str | None
    buyer_org: str | None
    reason: str
    deal_size: str | None
    tags: dict = field(default_factory=dict)


def _find(body: str, pats: list[str]) -> list[str]:
    out = []
    for p in pats:
        m = re.search(p, body, re.I)
        if m:
            out.append(m.group(0).strip().lower())
    return sorted(set(out))


def _domain(email: str) -> str:
    return (email or "").split("@")[-1].lower().strip()


def extract_signals(body: str) -> dict:
    tags = {}
    for name, pats in _BANKS:
        hits = _find(body or "", pats)
        if hits:
            tags[name] = hits
    return tags


def extract_deal_size(body: str) -> str | None:
    body = body or ""
    parts = []
    m = _RE_TOTAL.search(body)
    if m:
        parts.append(f"total {m.group(1)} units")
    m = _RE_EMP.search(body)
    if m:
        parts.append(f"{m.group(1)} employees")
    m = _RE_SETS.search(body)
    if m:
        parts.append(f"{m.group(1)} sets each (par)")
    qty_rows = len(_RE_QTY_TABLE.findall(body))
    if qty_rows >= 3:
        parts.append(f"{qty_rows}-line quantity table")
    moneys = _RE_MONEY.findall(body)
    if moneys:
        parts.append("$" + "/$".join(moneys[:3]))
    return "; ".join(parts) if parts else None


def _strongest_bucket(tags: dict) -> str:
    # Noise veto: >=2 spam markers (unsubscribe + promo/% off + "last call"...)
    # means this is a marketing blast TO us -- override weak dialogue/buying
    # words that slipped in ("Last call" matching call, "$100k account" hype).
    # A genuine buyer email does not carry multiple unsubscribe/promo markers.
    if len(tags.get("noise", [])) >= 2 and "order_confirmed" not in tags:
        strong_buying = len(tags.get("buying", [])) >= 2 or len(tags.get("project", [])) >= 1
        if not strong_buying:
            return "noise"
    for name in ("order_confirmed", "buying", "project", "dialogue", "service"):
        if name in tags:
            return name
    return "noise" if "noise" in tags else "none"


def score_text(
    body: str,
    *,
    is_decision_maker: bool = False,
    contact_category: str | None = None,
    brand_tier: str | None = None,
    days_since: int | None = None,
    repeat_buyer: bool = False,
) -> SignalResult:
    """Score a single blob of text (one message, or a buyer's collected lines)."""
    tags = extract_signals(body)
    bucket = _strongest_bucket(tags)
    score = _BASE[bucket]
    why = []

    if bucket in ("order_confirmed", "buying", "project", "dialogue", "service"):
        why.append(f"{bucket.replace('_', ' ')}: {', '.join(tags[bucket][:3])}")
    elif bucket == "noise":
        why.append("only marketing/automated content")
    else:
        why.append("no buying signal")

    if bucket == "buying" and len(tags.get("buying", [])) >= 2:
        score += 4
        why.append(f"+{len(tags['buying'])} buying phrases")
    if bucket in ("order_confirmed", "buying") and "project" in tags:
        score += 3
        why.append("project context")
    if repeat_buyer:
        score += 4
        why.append("repeat buyer")
    if is_decision_maker:
        score += 3
        why.append("decision-maker")
    if contact_category == "buyer":
        score += 2
        why.append("buyer")
    if brand_tier and re.search(r"tier1|tier2|luxury", brand_tier, re.I):
        score += 3
        why.append("tier1/2")
    if days_since is not None:
        if days_since <= 30:
            score += 2
            why.append("active <30d")
        elif days_since > 365:
            score -= 6
            why.append("stale >1y")

    if bucket == "noise":
        score = min(score, 10)
    score = max(0, min(100, score))

    return SignalResult(
        bucket=bucket,
        score=score,
        stage=_STAGE[bucket],
        reason="; ".join(why),
        tags=tags,
        deal_size=extract_deal_size(body),
    )


# -- ATTRIBUTION -------------------------------------------------------------
# A message dict: {from_email, from_name, body, sig_org, days_since}
# (sig_org = organization parsed from THAT sender's signature, may be None)

_PERSONAL_DOMAINS = {
    "gmail.com",
    "yahoo.com",
    "outlook.com",
    "hotmail.com",
    "aol.com",
    "icloud.com",
    "me.com",
    "live.com",
    "msn.com",
}


def find_buyer(
    messages: list[dict], own_domains: set[str], vendor_domains: set[str] | None = None
) -> dict | None:
    """Pick the single external counterparty the thread is WITH.

    Rules (spec refinement 1, 8):
      - exclude JA's own staff (own_domains)
      - exclude CC'd vendor domains (if provided)
      - among remaining external senders, pick the one who SENT the most
        messages (the active counterparty), tie-break by most recent.
      - collapse their identity via signature org when present.
    """
    vendor_domains = vendor_domains or set()
    ext = {}
    for m in messages:
        em = (m.get("from_email") or "").lower().strip()
        if not em or "@" not in em:
            continue
        dom = _domain(em)
        if dom in own_domains or dom in vendor_domains:
            continue
        e = ext.setdefault(
            em,
            {"email": em, "name": m.get("from_name"), "sig_org": None, "count": 0, "recency": None},
        )
        e["count"] += 1
        if m.get("sig_org") and not e["sig_org"]:
            e["sig_org"] = m["sig_org"]
        ds = m.get("days_since")
        if ds is not None and (e["recency"] is None or ds < e["recency"]):
            e["recency"] = ds
        if m.get("from_name") and not e["name"]:
            e["name"] = m["from_name"]
    if not ext:
        return None
    # most messages sent, then most recent
    best = sorted(
        ext.values(),
        key=lambda x: (x["count"], -(x["recency"] if x["recency"] is not None else 9999)),
        reverse=True,
    )[0]
    return best


def thread_participants(
    messages: list[dict], own_domains: set[str], vendor_domains: set[str] | None = None
) -> list[dict]:
    """All EXTERNAL people in the thread (non-JA, non-vendor), most-active first.

    The first entry is the buyer (same pick as find_buyer); the rest are the
    other people on the client side in the conversation -- their colleagues /
    the buying committee. Each: {email, name, sig_org, count, recency}.
    """
    vendor_domains = vendor_domains or set()
    _INFRA = (
        "postmaster",
        "mailer-daemon",
        "mailerdaemon",
        "bounce",
        "bounces",
        "no-reply",
        "noreply",
        "donotreply",
        "do-not-reply",
        "notifications",
    )
    ext = {}
    for m in messages:
        em = (m.get("from_email") or "").lower().strip()
        if not em or "@" not in em:
            continue
        dom = _domain(em)
        if dom in own_domains or dom in vendor_domains:
            continue
        _loc = em.split("@")[0]
        if _loc in _INFRA or any(_loc.startswith(p) for p in _INFRA):
            continue  # mail-server / automated address, not a person
        e = ext.setdefault(
            em,
            {"email": em, "name": m.get("from_name"), "sig_org": None, "count": 0, "recency": None},
        )
        e["count"] += 1
        if m.get("sig_org") and not e["sig_org"]:
            e["sig_org"] = m["sig_org"]
        ds = m.get("days_since")
        if ds is not None and (e["recency"] is None or ds < e["recency"]):
            e["recency"] = ds
        if m.get("from_name") and not e["name"]:
            e["name"] = m["from_name"]
    return sorted(
        ext.values(),
        key=lambda x: (x["count"], -(x["recency"] if x["recency"] is not None else 9999)),
        reverse=True,
    )


def classify_relationship(
    messages: list[dict],
    *,
    own_domains: set[str] | None = None,
    vendor_domains: set[str] | None = None,
    known_category: str | None = None,
) -> dict:
    """Read a thread and return a LABEL + a RELATIONSHIP CARD (not a score).

    LABEL (body-evidence, maps to contact_category):
      buyer_evidence -- we can SEE them purchasing from us (they approve/order/
                        ask price/request samples for THEIR property)
      vendor         -- they're selling TO us (spam/promo markers, no buyer signal)
      noise          -- newsletters / automated / nothing
      internal       -- no external party at all
      contact        -- external person, conversational, no clear buying yet

    CARD:
      buyer  = {name, email, org}              the person purchasing
      team   = [{name, email, org}, ...]       their colleagues in the thread
      stage  = furthest sales stage reached
    """
    own = own_domains or DEFAULT_OWN_DOMAINS
    parts = thread_participants(messages, own, vendor_domains)
    if not parts:
        return {
            "label": "internal",
            "buyer": None,
            "team": [],
            "stage": "internal",
            "evidence": "no external party in thread",
        }

    buyer = parts[0]
    team = parts[1:]
    _team_card = [
        {"name": p.get("name"), "email": p["email"], "org": p.get("sig_org")} for p in team
    ]

    # Defer to a confident existing category: if the DB already classified this
    # contact as a seller/vendor/competitor (via SAP / VENDOR_SEEDS), TRUST it.
    # The body shows buying verbs only because WE buy from suppliers; the body-
    # label must not relabel a known supplier as our buyer. (Empty/unknown/junk/
    # prospect do NOT block -- there the body-label adds value.)
    if (known_category or "").strip().lower() in ("seller", "vendor", "competitor"):
        return {
            "label": "vendor_or_noise",
            "buyer": None,
            "team": _team_card,
            "stage": "noise",
            "evidence": f"known {known_category.lower()} in our system -- not a buyer",
        }

    # Role-inbox guard: accounting/AP/invoice/payables/no-reply addresses are
    # transactional paperwork (they send US invoices), NEVER a sales buyer --
    # even if "invoice"/"payment" trips the buying bank. Label them noise.
    _local = buyer["email"].split("@")[0].lower()
    _ROLE_NONBUYER = (
        "accounting",
        "accountspayable",
        "accounts.payable",
        "ap",
        "ar",
        "invoice",
        "invoices",
        "payables",
        "payable",
        "billing",
        "no-reply",
        "noreply",
        "donotreply",
        "do-not-reply",
    )
    if _local in _ROLE_NONBUYER or any(
        _local.startswith(p + ".") or _local.startswith(p + "_") for p in _ROLE_NONBUYER
    ):
        return {
            "label": "vendor_or_noise",
            "buyer": None,
            "team": [
                {"name": p.get("name"), "email": p["email"], "org": p.get("sig_org")} for p in team
            ],
            "stage": "noise",
            "evidence": f"transactional role inbox ({_local}@) -- not a sales buyer",
        }

    # Vendor guard: an apparel manufacturer/supplier sells uniforms TO us. The
    # thread carries buying verbs because WE buy from THEM, but in our system
    # they are a seller, not a sales opportunity. Detect via domain/org tokens.
    _dom = buyer["email"].split("@")[-1].lower()
    _orgl = (buyer.get("sig_org") or "").lower()
    _VENDOR_TOK = (
        "clothes",
        "clothing",
        "uniform",
        "uniforms",
        "garment",
        "garments",
        "textile",
        "textiles",
        "apparel",
        "embroidery",
        "screenprint",
        "knit",
        "knitwear",
        "manufactur",
        "factory",
        "supply",
        "supplies",
        "wholesale",
        "import",
        "export",
    )
    if any(t in _dom for t in _VENDOR_TOK) or any(t in _orgl for t in _VENDOR_TOK):
        return {
            "label": "vendor_or_noise",
            "buyer": None,
            "team": [
                {"name": p.get("name"), "email": p["email"], "org": p.get("sig_org")} for p in team
            ],
            "stage": "noise",
            "evidence": f"apparel supplier/manufacturer ({_dom}) -- sells to us, not a buyer",
        }

    # score only the buyer's own words for intent (same model as score_thread)
    buyer_email = buyer["email"].lower()
    buyer_text = "\n".join(
        m.get("body") or "" for m in messages if (m.get("from_email") or "").lower() == buyer_email
    )
    res = score_text(buyer_text)

    # vendor detection: their messages are spam/promo, no real buying intent
    if res.bucket == "noise":
        label = "vendor_or_noise"
        evidence = res.reason
    elif res.bucket in ("order_confirmed", "buying"):
        label = "buyer_evidence"
        evidence = f"buyer wrote: {res.reason.split(';')[0]}"
    elif res.bucket in ("project", "dialogue"):
        label = "active_contact"
        evidence = f"engaged: {res.reason.split(';')[0]}"
    else:
        label = "contact"
        evidence = "external contact, no clear buying signal in their messages"

    return {
        "label": label,
        "buyer": {"name": buyer.get("name"), "email": buyer["email"], "org": buyer.get("sig_org")},
        "team": [
            {"name": p.get("name"), "email": p["email"], "org": p.get("sig_org")} for p in team
        ],
        "stage": res.stage,
        "evidence": evidence,
        "products": extract_products("\n".join(m.get("body") or "" for m in messages)),
    }


# stages, ranked, for taking the FURTHEST point a thread reached
_STAGE_RANK = {
    "unknown": 0,
    "noise": 0,
    "existing-service": 1,
    "dialogue": 2,
    "prospecting": 3,
    "proposal/quote": 4,
    "approved/ordered": 5,
}


def score_thread(
    messages: list[dict],
    *,
    own_domains: set[str] | None = None,
    vendor_domains: set[str] | None = None,
    is_decision_maker: bool = False,
    contact_category: str | None = None,
    brand_tier: str | None = None,
    repeat_buyer: bool = False,
) -> ThreadScore:
    """Score a thread by what the BUYER said; use internal msgs for stage only.

    Real threads are mostly JA-internal colleague chatter (order processing, art,
    IT, production passing info) that carries buying VERBS but is NOT the client.
    So we DON'T score the whole joined thread -- we:
      - score the BUYER's own messages as the PRIMARY intent signal,
      - read JA-internal messages ONLY for stage evidence ('PROCEED WITH ORDER'
        from JA = the deal advanced), never to manufacture buyer intent,
      - so a thread where JA pushes but the client stays quiet scores LOW.
    Deal-size is read from the whole thread (the numbers are facts wherever
    they appear). Attribution = the external counterparty (find_buyer).
    """
    own = own_domains or DEFAULT_OWN_DOMAINS
    vendor_domains = vendor_domains or set()
    buyer = find_buyer(messages, own, vendor_domains)

    if not buyer:
        return ThreadScore(
            score=0,
            stage="internal",
            bucket="none",
            buyer_email=None,
            buyer_name=None,
            buyer_org=None,
            reason="no external buyer found (internal-only thread)",
            deal_size=extract_deal_size("\n".join(m.get("body") or "" for m in messages)),
            tags={},
        )

    buyer_email = buyer["email"].lower()
    buyer_msgs, internal_msgs = [], []
    for m in messages:
        em = (m.get("from_email") or "").lower()
        dom = _domain(em)
        if em == buyer_email:
            buyer_msgs.append(m)
        elif dom in own:
            internal_msgs.append(m)
        # other-external (vendors/cc) ignored for scoring

    buyer_text = "\n".join(m.get("body") or "" for m in buyer_msgs)
    internal_text = "\n".join(m.get("body") or "" for m in internal_msgs)
    full_text = "\n".join(m.get("body") or "" for m in messages)

    # PRIMARY: score the buyer's own words
    res = score_text(
        buyer_text,
        is_decision_maker=is_decision_maker,
        contact_category=contact_category,
        brand_tier=brand_tier,
        days_since=buyer["recency"],
        repeat_buyer=repeat_buyer,
    )

    # STAGE: a thread reaches the FURTHEST stage either party evidences. If JA
    # internally marked it approved/ordered, the DEAL advanced. Take max stage.
    internal_tags = extract_signals(internal_text)
    internal_bucket = _strongest_bucket(internal_tags)
    internal_stage = _STAGE.get(internal_bucket, "unknown")
    stage = res.stage
    if _STAGE_RANK.get(internal_stage, 0) > _STAGE_RANK.get(stage, 0):
        stage = internal_stage

    # SUBSTANCE: the internal colleague work is PURPOSEFUL -- style#, colors,
    # quantities, costs, SKU generation = JA actively building the item. That
    # corroborates a real deal and adds to the score (it is NOT chitchat).
    sub_bump, sub_hits = substance_score(internal_text)
    score = min(100, res.score + sub_bump)

    # Asymmetry guard: buyer quiet on INTENT but JA pushing AND no real
    # substance work -> we're chasing air. Keep modest. But if there's heavy
    # substance (specs/qty/cost being built), the deal IS real -> don't suppress.
    buyer_quiet = res.bucket in ("none", "noise", "dialogue")
    ja_pushing = internal_bucket in ("order_confirmed", "buying")
    note = None
    if buyer_quiet and ja_pushing and sub_bump < 10:
        score = min(max(score, 45), 60)
        note = "JA advancing but buyer not vocal -- confirm client intent"

    reason = res.reason
    if sub_hits:
        reason += f"; deal substance: {', '.join(sub_hits[:3])}"
    if note:
        reason += f"; {note}"
    label = buyer.get("name") or buyer["email"]
    reason += f"  |  buyer: {label}"
    if buyer.get("sig_org"):
        reason += f" @ {buyer['sig_org']}"

    return ThreadScore(
        score=score,
        stage=stage,
        bucket=res.bucket,
        buyer_email=buyer["email"],
        buyer_name=buyer.get("name"),
        buyer_org=buyer.get("sig_org"),
        reason=reason,
        deal_size=extract_deal_size(full_text),
        tags=res.tags,
    )
