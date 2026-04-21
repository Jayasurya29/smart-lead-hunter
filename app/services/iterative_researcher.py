"""
Iterative Contact Researcher (v5)
==================================

Replaces the fixed-query enrichment pipeline with an iterative researcher
that asks ~3 queries, learns what kind of lead this is, then asks smarter
follow-up queries based on what it learned.

Mimics how a human researcher would dig in:
  Iteration 1 (DISCOVERY): "What kind of project is this? Who owns it?"
  Iteration 2 (GM HUNT):   "Is there a named GM? Try TripAdvisor too."
  Iteration 3 (CORPORATE): "Find decision-makers at the owner + brand parent."
  Iteration 4 (LINKEDIN):  "For every name found, get their LinkedIn URL."

Each iteration uses the facts learned in previous iterations to construct
better, more targeted queries. Stops early if no new info.

Reuses the existing helpers from contact_enrichment.py:
  _search_web, _scrape_url, _extract_contacts_with_gemini,
  _verify_contacts_with_gemini, _is_corporate_title, etc.

The output is the same EnrichmentResult shape as the legacy pipeline,
so callers (routes, tasks) don't need to change.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Optional

from app.config.brand_registry import BrandRegistry

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
# RESEARCH STATE — the "memory" carried across iterations
# ═══════════════════════════════════════════════════════════════


@dataclass
class ResearchState:
    """What the researcher has learned so far about the lead."""

    # ── Lead facts (input, immutable) ──
    hotel_name: str
    brand: Optional[str] = None
    management_company: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    opening_date: Optional[str] = None
    timeline_label: Optional[str] = None  # URGENT | HOT | WARM | COOL | EXPIRED
    project_type: Optional[str] = (
        None  # new_opening | renovation | rebrand | ownership_change
    )
    search_name: Optional[str] = None  # Stripped name for queries ("Kali Hotel")
    former_names: Optional[list] = None  # Previous names ["Montage Kapalua Bay"]
    description: Optional[str] = None  # DB description field — richer classifier input

    # ── Discovered facts (filled in as iterations run) ──
    operator_parent: Optional[str] = None  # "Hyatt Inclusive Collection"
    owner_company: Optional[str] = None  # "Playa Hotels & Resorts"
    region_term: Optional[str] = None  # "Caribbean"
    cluster_siblings: list[str] = field(default_factory=list)
    project_stage: Optional[str] = (
        None  # "greenfield" | "renovation" | "reopening" | "conversion"
    )
    has_named_gm: bool = False
    discovered_names: list[dict] = field(
        default_factory=list
    )  # {name, title, source_url, scope}

    # ── Company verification (Shift A) ──
    verified_current_companies: list[str] = field(
        default_factory=list
    )  # companies confirmed currently in charge
    historical_companies: list[str] = field(
        default_factory=list
    )  # past owners/operators to skip

    # ── Bookkeeping ──
    queries_run: list[str] = field(default_factory=list)
    urls_scraped: list[str] = field(default_factory=list)
    iterations_done: int = 0

    # ── D1: Existing-hotels + GM-missing cascade flags ──
    # Set to True when the researcher is invoked against an already-operating
    # hotel (is_client=True SAP client, or a scraped existing hotel). When
    # True, Iter 2 uses a slim 2-query set (skips appointment/rebrand noise
    # that only applies to pre-opening leads).
    is_existing_hotel: bool = False

    # Flipped to True by Iter 2 when no GM was found AND the timeline bucket
    # is HOT/URGENT/WARM (i.e. active pre-opening where a GM SHOULD exist).
    # Triggers the cascade queries (DOSM, Dir Rev Mgmt, Area/Regional GM,
    # Task Force for Marriott-family brands) and also reweights Iter 6's
    # strategist prompt so corporate/regional VPs become P1 (not P2)
    # because they own the vendor decision until a GM is hired.
    gm_search_cascade_active: bool = False

    # ── Phase B: Project-type classification (from project_type_intelligence) ──
    # Set at the top of run_iterative_research(), BEFORE any iteration runs.
    # Used to route iter 2/3/6 behaviour:
    #   - residences_only → should_reject=True, skip all iterations
    #   - reopening → Iter 2 targets corporate, skips property-GM hunt
    #   - conversion → Iter 2 runs standard GM hunt but also targets operator corp
    #   - renovation → Iter 2 starts with current GM (already on-site)
    #   - new_opening → existing cascade (GM hunt → corporate fallback)
    #   - rebrand → urgent uniform replacement, all phases relevant
    project_confidence: Optional[str] = None  # high | medium | low
    project_signals: list[str] = field(default_factory=list)
    phase_reason: Optional[str] = None  # Human-readable routing explanation
    should_reject: bool = False
    rejection_reason: Optional[str] = None  # e.g. 'residences_only_not_hotel'


# ═══════════════════════════════════════════════════════════════
# ITERATION 1 — DISCOVERY
# Figure out the lead's situation. Owner, operator parent, stage.
# ═══════════════════════════════════════════════════════════════


async def iteration_1_discovery(state: ResearchState) -> int:
    """
    Run discovery queries. NO site:linkedin.com restriction — we want
    trade press articles where owners and operators get named.

    Returns: count of new facts learned (used for early-stop decision)
    """
    from app.services import (
        contact_enrichment as ce,
    )  # delayed import to avoid circularity

    # Resolve operator_parent from the brand registry (if known)
    if not state.operator_parent and state.brand:
        try:
            bi = BrandRegistry.lookup(state.brand)
            if bi and bi.parent_company:
                state.operator_parent = bi.parent_company.split("(")[0].strip()
        except Exception:
            pass

    # Resolve region term from country
    if not state.region_term and state.country:
        try:
            from app.config.region_map import primary_region

            state.region_term = primary_region(state.country)
        except Exception:
            pass

    # ── Query batch 1: project announcement / management agreement ──
    # Use short name for queries — long names like "Royalton Vessence Barbados,
    # An Autograph Collection All-Inclusive Resort – Adults Only" return zero results.
    discovery_name = state.search_name or _shorten_hotel_name(state.hotel_name)
    queries = [
        # Trade press articles announcing the project / signing
        f'"{discovery_name}" announcement OR opening OR "management agreement"',
        f'"{discovery_name}" developer OR owner OR partnership',
        # Project type signals — picks up reopening, conversion, renovation
        f'"{discovery_name}" reopening OR conversion OR renovation OR rebrand',
    ]

    facts_before = _fact_count(state)
    await _run_queries_and_extract(state, queries, ce, scrape_limit=5)

    # ── Mine the scraped articles to fill in owner_company + project_stage ──
    # The Gemini-extracted contacts include `organization` field which often
    # IS the owner company name (e.g. "Playa Resorts Management").
    if not state.owner_company:
        state.owner_company = _guess_owner_from_state(state)

    if not state.project_stage:
        state.project_stage = _guess_stage_from_state(state)

    # ── SHIFT A: verify which operating companies are CURRENTLY in charge ──
    # Some companies we find in iter 1 are historical (e.g. "Playa Hotels"
    # was acquired by Hyatt in June 2025 — so Playa execs are now ex-execs
    # for Hyatt-managed properties). Verify before we hunt inside them.
    await _verify_operating_companies(state)

    state.iterations_done = 1
    return _fact_count(state) - facts_before


async def _verify_operating_companies(state: ResearchState) -> None:
    """
    SHIFT A: For each candidate operating company (operator_parent,
    owner_company, management_company), run a quick search to verify
    they are CURRENTLY associated with the property, not historically.
    Populates state.verified_current_companies and state.historical_companies.
    """
    from app.services import contact_enrichment as ce

    candidates: list[str] = []
    for c in [state.operator_parent, state.owner_company, state.management_company]:
        if c and c not in candidates:
            candidates.append(c)

    for company in candidates:
        # Skip if already classified
        if (
            company in state.verified_current_companies
            or company in state.historical_companies
        ):
            continue

        verify_name = state.search_name or _shorten_hotel_name(state.hotel_name)
        query = f'"{company}" "{verify_name}" 2025 OR 2026 management OR operator'
        if query in state.queries_run:
            continue
        state.queries_run.append(query)

        try:
            results = await ce._search_web(query, max_results=5)
        except Exception as ex:
            logger.warning(f"[SHIFT A] Verification search failed for {company}: {ex}")
            state.verified_current_companies.append(company)  # default to keep
            continue

        if not results:
            logger.info(
                f"[SHIFT A] No recent signal for {company} — keeping as candidate"
            )
            state.verified_current_companies.append(company)
            continue

        # Build a snippet blob and let Gemini decide
        snippets = "\n\n".join(
            f"[{r.get('url','')}] {r.get('snippet') or r.get('title') or ''}"
            for r in results[:5]
        )
        verdict = await _check_company_currency_with_gemini(
            company=company,
            target_hotel=state.hotel_name,
            evidence=snippets[:6000],
        )

        if verdict == "current":
            state.verified_current_companies.append(company)
            logger.info(
                f"[SHIFT A] {company!r} verified as CURRENT operator of {state.hotel_name}"
            )
        elif verdict == "historical":
            state.historical_companies.append(company)
            logger.info(
                f"[SHIFT A] {company!r} marked HISTORICAL — skipping contact hunt inside"
            )
        else:
            # Unknown / ambiguous — default to keep (don't drop real data on weak signal)
            state.verified_current_companies.append(company)
            logger.info(f"[SHIFT A] {company!r} ambiguous — keeping as candidate")


async def _check_company_currency_with_gemini(
    company: str, target_hotel: str, evidence: str
) -> str:
    """Return 'current', 'historical', or 'unknown'."""
    from app.services import contact_enrichment as ce
    import json

    if not evidence.strip():
        return "unknown"

    prompt = f"""Evidence snippets below describe the relationship between a company
and a hotel. Current date: April 2026.

COMPANY: {company}
HOTEL: {target_hotel}

Decide ONE of:
- "current"    = {company} is CURRENTLY (2025-2026) the owner, operator, or
                 management company of {target_hotel}
- "historical" = {company} WAS once associated with {target_hotel} but has
                 since sold it, exited, been acquired, or been replaced
- "unknown"    = evidence is unclear or absent

Acquisitions matter: if {company} was acquired by another company, and the new
owner manages {target_hotel} now, then {company} is "historical" even though
its name still appears in old articles.

Respond with a single JSON object, no prose:
{{"verdict": "current" | "historical" | "unknown", "reason": "1 short sentence"}}

EVIDENCE:
{evidence}
"""
    resp = await ce._call_gemini(prompt)
    if not resp:
        return "unknown"
    if isinstance(resp, dict):
        return (resp.get("verdict") or "unknown").lower()
    if isinstance(resp, str):
        try:
            parsed = json.loads(resp)
            return (parsed.get("verdict") or "unknown").lower()
        except json.JSONDecodeError:
            return "unknown"
    return "unknown"


# ═══════════════════════════════════════════════════════════════
# ITERATION 2 — GM HUNT
# Find a named property GM if one exists. Search TripAdvisor too —
# guest reviews from the last year often name the GM directly.
# ═══════════════════════════════════════════════════════════════


async def iteration_2_gm_hunt(state: ResearchState) -> int:
    from app.services import contact_enrichment as ce

    # Short name for search (strips "Resort & Spa" etc.)
    short_name = state.search_name or _shorten_hotel_name(state.hotel_name)

    # ── D1: SLIM BRANCH for already-operating hotels ──
    # For existing hotels the GM (if any) is already named publicly on
    # LinkedIn / TripAdvisor / hotel website. We don't need appointment
    # press-release queries ("appointed", "hired") or former-name rebrand
    # queries — those are noise on an open property and waste credits.
    if state.is_existing_hotel:
        queries = [
            f'"{short_name}" "general manager"',
            f'"{short_name}" "general manager" OR "hotel manager" site:linkedin.com',
        ]

        facts_before = _fact_count(state)
        await _run_queries_and_extract(state, queries, ce, scrape_limit=5)

        state.has_named_gm = any(
            _looks_like_gm(n.get("title", "")) for n in state.discovered_names
        )

        state.iterations_done = 2
        return _fact_count(state) - facts_before

    # ── DEFAULT BRANCH — pre-opening leads (URGENT/HOT/WARM/COOL) ──
    queries = [
        # ── SIMPLE QUERY FIRST — the way a human would search ──
        # This is the #1 most important query. No qualifiers.
        # Catches LinkedIn profiles, RocketReach pages, TripAdvisor reviews,
        # press releases — anything Google surfaces with name + title.
        # Snippet extraction will read the name from the result text.
        f'"{short_name}" "general manager"',
        # LinkedIn-specific — catches GMs who list the parent brand, not property name
        f'"{short_name}" "general manager" OR "hotel manager" site:linkedin.com',
        # Press releases with appointment announcements
        f'"{short_name}" "general manager" appointed OR named OR hired',
        # Generic GM mention with year filter
        f'"{short_name}" general manager 2025 OR 2026',
    ]

    # If this is a cluster reopening, also search for a cluster GM
    if state.project_stage in ("reopening", "renovation") and state.operator_parent:
        queries.append(
            f'"{state.operator_parent}" "cluster general manager" OR "area general manager" {state.region_term or ""}'
        )

        # CLUSTER SIBLING PROBE: search sibling properties for the cluster GM
        if state.region_term and state.city:
            queries.append(
                f'site:tripadvisor.com "{state.operator_parent}" {state.city} "general manager"'
            )

    # ── FORMER NAME QUERIES (for rebranded properties) ──
    # If SmartFill found former names, search those too.
    # This catches Robert Friedl (LinkedIn says "Montage Kapalua Bay",
    # not "St. Regis Kapalua Bay") and similar rebrand cases.
    if state.former_names:
        for old_name in state.former_names[:3]:  # cap at 3
            old_short = _shorten_hotel_name(old_name)
            queries.append(f'"{old_short}" "general manager"')

    # ── REBRAND-PROOF LOCATION QUERY ──
    # GMs at rebranded properties often list the parent brand as employer.
    # Search by LOCATION + ROLE + any-brand-in-history.
    location_short = state.city or ""
    if not location_short:
        name_parts = (state.hotel_name or "").split()
        drop = {"resort", "resorts", "spa", "hotel", "hotels", "&", "the", "by"}
        if state.brand:
            drop.update(w.lower() for w in state.brand.split())
        location_words = [w for w in name_parts if w.lower() not in drop]
        location_short = " ".join(location_words[:3])

    if location_short and len(location_short) > 3:
        brand_variants = set()
        if state.operator_parent:
            brand_variants.add(state.operator_parent.split()[0])
        if state.brand:
            brand_variants.add(state.brand.split()[0])
        brand_variants.update(["Hilton", "Marriott", "IHG", "Playa"])
        brand_or = " OR ".join(brand_variants)
        queries.append(
            f'"{location_short}" {state.country or ""} "general manager" OR "hotel manager" '
            f'{brand_or} site:linkedin.com'.strip()
        )

    facts_before = _fact_count(state)
    await _run_queries_and_extract(state, queries, ce, scrape_limit=5)

    # Did any of the names found look like a GM?
    state.has_named_gm = any(
        _looks_like_gm(n.get("title", "")) for n in state.discovered_names
    )

    # ── D1: GM-MISSING CASCADE ──
    # No GM found yet but timeline says one SHOULD be hired by now.
    # Fire a second round of queries for the surrounding staff (DOSM,
    # Revenue Director, Area/Regional GM) plus Task Force for
    # Marriott-family brands. Also flip the cascade flag so Iter 6's
    # strategist prompt reweights corporate/regional VPs to P1.
    tl = (state.timeline_label or "").upper()
    if (not state.has_named_gm) and tl in ("HOT", "URGENT", "WARM"):
        cascade_queries = [
            f'"{short_name}" "director of sales" OR "DOSM" OR "director of sales and marketing"',
            f'"{short_name}" "director of revenue" OR "revenue management"',
            f'"{short_name}" "area general manager" OR "regional general manager"',
        ]

        # Marriott-family brands use Task Force GMs for pre-opening.
        # Check parent_company from brand registry.
        is_marriott_family = False
        try:
            if state.brand:
                bi = BrandRegistry.lookup(state.brand)
                if bi and "marriott" in (bi.parent_company or "").lower():
                    is_marriott_family = True
        except Exception as ex:
            logger.debug(f"[ITER 2 CASCADE] brand registry lookup failed: {ex}")

        if is_marriott_family:
            cascade_queries.append(
                f'"{short_name}" "task force" "general manager" OR "task force GM"'
            )

        logger.info(
            f"[ITER 2 CASCADE] No GM found for {state.hotel_name!r} "
            f"(timeline={tl}). Firing {len(cascade_queries)} cascade queries "
            f"(DOSM / Rev Mgmt / Area GM"
            f"{' / Task Force' if is_marriott_family else ''})."
        )

        await _run_queries_and_extract(state, cascade_queries, ce, scrape_limit=3)
        state.gm_search_cascade_active = True

    state.iterations_done = 2
    return _fact_count(state) - facts_before


# ═══════════════════════════════════════════════════════════════
# ITERATION 2.5 — PROPERTY DEPT HEADS HUNT
# The old v4 pipeline's "Phase 3" covered this and the new v5
# pipeline lost it. Now restored properly:
# For HOT/URGENT leads (hotel opening in 3-11 months OR already open),
# the property-level department heads exist and are real buyers.
# Queries each major uniform-relevant role by property name.
#
# Skipped for WARM/COOL leads (12+ months out) because those depts
# aren't staffed yet — wastes queries.
# ═══════════════════════════════════════════════════════════════


async def iteration_2_5_property_staff(state: ResearchState) -> int:
    """
    Hunt for on-property department heads — the day-to-day uniform buyers
    at THIS specific hotel. Titles sourced from the brand registry's
    property_team_titles list so each brand hunts for the roles that
    actually matter for its operating model.

    Timeline-aware: skipped for WARM/COOL leads (12+ months out) where
    most of these roles haven't been hired yet. Runs for HOT/URGENT/
    EXPIRED/TBD.
    """
    from app.services import contact_enrichment as ce

    # Timeline gate
    tl = (state.timeline_label or "").upper()
    if tl in ("WARM", "COOL"):
        logger.info(
            f"[ITER 2.5/STAFF] Skipping dept-head hunt for {tl} lead "
            f"(property dept heads not hired yet at this timeline)"
        )
        return 0

    hotel = state.search_name or _shorten_hotel_name(state.hotel_name)

    # ── Pull property titles from brand registry ──
    # Dynamic, per-brand. Falls back to universal baseline if no brand match.
    property_titles: list[str] = []
    bi = BrandRegistry.lookup(state.brand) if state.brand else None
    if bi and bi.property_team_titles:
        property_titles = list(bi.property_team_titles)
    else:
        # Safe universal baseline — every hotel has these roles when staffed
        property_titles = [
            "General Manager",
            "Hotel Manager",
            "Resort Manager",
            "Director of Operations",
            "Director of Rooms",
            "Director of Housekeeping",
            "Director of Food and Beverage",
            "Executive Chef",
            "Director of Sales",
            "Director of Events",
            "Human Resources Director",
            "Director of Finance",
            "Controller",
        ]

    # ── Group titles into 6-7 efficient queries rather than 1-per-title ──
    # Groups = related roles that would appear on the same LinkedIn/press page.
    # Each group becomes a single OR'd Serper query. Preserves abundance
    # (every title gets searched) without multiplying query count.
    query_groups = [
        # Group 1 — GM + Hotel Manager + Resort Manager
        [
            t
            for t in property_titles
            if t.lower() in ("general manager", "hotel manager", "resort manager")
        ],
        # Group 2 — Operations + Rooms + Housekeeping
        [
            t
            for t in property_titles
            if any(
                k in t.lower()
                for k in (
                    "director of operations",
                    "director of rooms",
                    "director of housekeeping",
                    "executive housekeeper",
                )
            )
        ],
        # Group 3 — F&B + Chef
        [
            t
            for t in property_titles
            if any(
                k in t.lower()
                for k in (
                    "food and beverage",
                    "f&b",
                    "executive chef",
                )
            )
        ],
        # Group 4 — Sales + Events + Banquets
        [
            t
            for t in property_titles
            if any(
                k in t.lower()
                for k in (
                    "director of sales",
                    "director of events",
                    "director of banquets",
                    "sales and marketing",
                )
            )
        ],
        # Group 5 — HR / People & Culture
        [
            t
            for t in property_titles
            if any(
                k in t.lower()
                for k in (
                    "human resources",
                    "people and culture",
                )
            )
        ],
        # Group 6 — Finance + Purchasing
        [
            t
            for t in property_titles
            if any(
                k in t.lower()
                for k in (
                    "finance",
                    "controller",
                    "purchasing",
                )
            )
        ],
    ]

    queries = []
    for group in query_groups:
        if not group:
            continue
        # De-dup, cap 4 titles per query to keep Serper query strings sane
        uniq: list[str] = []
        seen = set()
        for t in group:
            k = t.lower()
            if k not in seen:
                seen.add(k)
                uniq.append(t)
        uniq = uniq[:4]
        or_clause = " OR ".join(f'"{t}"' for t in uniq)
        queries.append(f'"{hotel}" {or_clause}')

    facts_before = _fact_count(state)
    await _run_queries_and_extract(state, queries, ce, scrape_limit=8)

    state.iterations_done = max(state.iterations_done, 2)
    new_facts = _fact_count(state) - facts_before
    return new_facts


# ═══════════════════════════════════════════════════════════════
# ITERATION 3 — CLUSTER + REGIONAL / OWNER HUNT
# Queries target decision-maker roles at the operating/owner company:
#   - Cluster/Area GM and Complex Director (cluster tier)
#   - Regional VP/SVP Operations, VP Commercial Services,
#     Director of Procurement (regional tier)
# Pulls tier-specific titles from the brand registry so each brand's
# actual buyer hierarchy is used, not generic keywords.
# ═══════════════════════════════════════════════════════════════


async def iteration_3_corporate_hunt(state: ResearchState) -> int:
    """
    Hunt for cluster and regional decision-makers. Pulls target titles from
    the brand registry's `cluster_team_titles` and `regional_team_titles`
    lists so each brand is hunted for its actual buyer hierarchy.

    This iteration always runs (abundance principle). The Iter 6 strategist
    later decides whether each contact is P1-P4 based on tier + regional fit.
    """
    from app.services import contact_enrichment as ce

    # Use specific location instead of broad region term.
    # "Marriott" "Cluster GM" "North America" returns contacts from 50 states.
    # "Marriott" "Cluster GM" "California" or "Jamaica" is much more targeted.
    location_specific = state.state or state.city or state.country or ""
    region = location_specific if location_specific else (state.region_term or "")

    # Decide which companies to hunt at.
    # PRIORITY: management_company (actual operator like Crescent) comes FIRST.
    # Brand flag (Marriott for Autograph Collection) comes second.
    # This prevents searching "Marriott" Area GM when Crescent is the real operator.
    hunt_companies: list[str] = []

    # 1. Management company from SmartFill (most reliable — actual operator)
    if (
        state.management_company
        and state.management_company not in state.historical_companies
    ):
        hunt_companies.append(state.management_company)

    # 2. Verified current companies from Shift A
    if state.verified_current_companies:
        for vc in state.verified_current_companies:
            if vc not in hunt_companies:
                hunt_companies.append(vc)
    elif state.operator_parent and state.operator_parent not in hunt_companies:
        hunt_companies.append(state.operator_parent)

    # 3. Owner company (if distinct)
    if (
        state.owner_company
        and state.owner_company not in hunt_companies
        and state.owner_company not in state.historical_companies
        and state.owner_company.lower() not in (state.hotel_name or "").lower()
    ):
        hunt_companies.append(state.owner_company)

    # Pull tiered title lists from the brand registry. These are the exact
    # titles we want to find for this brand's buyer hierarchy.
    cluster_titles: list[str] = []
    regional_titles: list[str] = []
    bi = BrandRegistry.lookup(state.brand) if state.brand else None
    if bi:
        cluster_titles = list(bi.cluster_team_titles or [])
        regional_titles = list(bi.regional_team_titles or [])

    # Sensible fallback when brand isn't in registry
    if not cluster_titles:
        cluster_titles = [
            "Cluster General Manager",
            "Complex General Manager",
            "Area General Manager",
            "Complex Director of Operations",
        ]
    if not regional_titles:
        regional_titles = [
            "VP Operations",
            "SVP Operations",
            "Regional VP Operations",
            "Director of Procurement",
            "VP Procurement",
            "Pre-Opening Director",
        ]

    def _group_or_query(company: str, titles: list[str], region_term: str) -> str:
        """Build a single Serper query OR'ing up to 4 titles for one company."""
        if not titles:
            return ""
        uniq: list[str] = []
        seen = set()
        for t in titles:
            k = (t or "").lower().strip()
            if k and k not in seen:
                seen.add(k)
                uniq.append(t)
        uniq = uniq[:4]
        or_clause = " OR ".join(f'"{t}"' for t in uniq)
        return f'"{company}" {or_clause} {region_term}'.strip()

    queries: list[str] = []

    # Cap at top 2 companies to keep query count reasonable while covering
    # both operator and owner when distinct.
    for company in hunt_companies[:2]:
        # ── Cluster tier queries (1-2 OR'd groups) ──
        q = _group_or_query(company, cluster_titles[:4], region)
        if q:
            queries.append(q)
        if len(cluster_titles) > 4:
            q = _group_or_query(company, cluster_titles[4:8], region)
            if q:
                queries.append(q)

        # ── Regional tier queries (1-2 OR'd groups) ──
        q = _group_or_query(company, regional_titles[:4], region)
        if q:
            queries.append(q)
        if len(regional_titles) > 4:
            q = _group_or_query(company, regional_titles[4:8], region)
            if q:
                queries.append(q)

        # ── Pre-opening / task force (signals incoming ops team) ──
        is_pre_opening = (state.timeline_label or "").upper() in (
            "HOT",
            "WARM",
            "URGENT",
        )
        if is_pre_opening:
            queries.append(
                f'"{company}" "pre-opening" OR "task force" OR "opening team" {region}'.strip()
            )

    # ── Independent / boutique brand: founder IS the buyer ──
    brand_lower = (state.brand or "").lower().strip()
    is_indie = (
        (bi and (bi.uniform_freedom or "").lower() in ("high", "full"))
        or not brand_lower
        or brand_lower in ("independent", "boutique", "lifestyle")
        or (brand_lower and not bi)  # brand name exists but not in registry
    )
    if is_indie:
        # Search for founders/principals at the owner/operator company
        search_company = (
            state.management_company
            or state.owner_company
            or state.operator_parent
            or state.hotel_name
        )
        queries.append(
            f'"{search_company}" founder OR "co-founder" OR "chief executive" OR "managing director"'
        )

    # Dedupe
    seen = set()
    deduped: list[str] = []
    for q in queries:
        q = (q or "").strip()
        if q and q not in seen:
            seen.add(q)
            deduped.append(q)

    if not deduped:
        state.iterations_done = 3
        return 0

    facts_before = _fact_count(state)
    # Cap at 8 queries — covers cluster + regional for 2 companies + extras
    await _run_queries_and_extract(state, deduped[:8], ce, scrape_limit=6)

    state.iterations_done = 3
    return _fact_count(state) - facts_before


# ═══════════════════════════════════════════════════════════════
# ITERATION 4 — LINKEDIN LOOKUP
# For every name discovered, find their LinkedIn profile URL.
# This is content-blind — works for any name, any company.
# ═══════════════════════════════════════════════════════════════


async def iteration_4_linkedin_lookup(state: ResearchState) -> int:
    from app.services import contact_enrichment as ce

    facts_before = _fact_count(state)
    short_name = state.search_name or _shorten_hotel_name(state.hotel_name)

    for contact in state.discovered_names:
        # Skip if we already have a LinkedIn URL or no usable name
        if contact.get("linkedin"):
            continue
        name = contact.get("name", "").strip()
        if not name or len(name.split()) < 2:
            continue

        # Choose the right search context based on contact type.
        scope = (contact.get("scope") or "").lower()
        source_type = (contact.get("source_type") or "").lower()

        if scope == "hotel_specific" or source_type == "snippet":
            query = f'"{name}" "{short_name}" OR "{state.brand or ""}" linkedin'.strip()
        else:
            # Use the contact's OWN organization first — this is the most
            # specific context. "Elie Khoury" + "Crescent Hotels" finds the
            # COO. "Elie Khoury" + "Marriott" finds the wrong person (a GM
            # at Oakland Marriott with the same name).
            contact_org = (contact.get("organization") or "").strip()
            company_ctx = (
                contact_org  # e.g. "Crescent Hotels & Resorts"
                or state.management_company  # SmartFill-discovered operator
                or state.operator_parent  # Shift A-discovered operator
                or state.owner_company
                or state.brand
                or ""
            )
            query = f'"{name}" "{company_ctx}" linkedin'.strip()

        # Try qualified query first, then simple name-only fallback
        queries_to_try = [query, f'"{name}" linkedin']
        found_url = False

        for q in queries_to_try:
            if found_url:
                break
            if q in state.queries_run:
                continue
            state.queries_run.append(q)

            results = await ce._search_web(q, max_results=3)
            for r in results:
                r_url = r.get("url", "")
                if "linkedin.com/in/" not in r_url:
                    continue
                # Verify the URL slug contains the person's name parts.
                # Without this, "Sean Verney" could get Debbie Riga's URL
                # just because it appeared in the same search results.
                slug = (
                    r_url.lower()
                    .split("linkedin.com/in/")[-1]
                    .split("?")[0]
                    .split("/")[0]
                )
                name_parts = name.lower().split()
                # At least the last name (longest part) must appear in slug
                last_name = max(name_parts, key=len) if name_parts else ""
                # Normalize: remove accents for comparison
                import unicodedata

                slug_clean = (
                    unicodedata.normalize("NFKD", slug)
                    .encode("ascii", "ignore")
                    .decode()
                )
                last_clean = (
                    unicodedata.normalize("NFKD", last_name)
                    .encode("ascii", "ignore")
                    .decode()
                )
                if last_clean and last_clean.replace(".", "").replace(
                    "-", ""
                ) in slug_clean.replace("-", ""):
                    contact["linkedin"] = r_url
                    logger.info(f"LinkedIn URL found for {name}: {r_url}")
                    found_url = True
                    break
                else:
                    logger.debug(
                        f"LinkedIn URL rejected for {name}: slug '{slug}' "
                        f"doesn't match name part '{last_name}'"
                    )

    state.iterations_done = 4
    return _fact_count(state) - facts_before


# ═══════════════════════════════════════════════════════════════
# ITERATION 5 — VERIFY_CURRENT_ROLE
# Before we label anyone "hotel_specific", CONFIRM via a fresh
# search that their CURRENT role is at the target property.
# LinkedIn profiles are CVs — they list past jobs — and our previous
# "LinkedIn mentions this hotel" signal was catching ex-employees
# from 5+ years ago. This iteration asks:
#   "Is {name} at {hotel_name} RIGHT NOW, in 2025-2026?"
# and reassigns scope based on the answer.
# ═══════════════════════════════════════════════════════════════


async def iteration_5_verify_current_role(state: ResearchState) -> int:
    """
    For every contact marked hotel_specific (or unknown), run a confirmation
    search to verify they are CURRENTLY at the property. Downgrade scope
    if the person is an ex-employee or works at a sibling property.
    """
    from app.services import contact_enrichment as ce

    facts_before = _fact_count(state)

    for contact in state.discovered_names:
        name = (contact.get("name") or "").strip()
        if not name or len(name.split()) < 2:
            continue

        # Only verify contacts that the earlier iterations tagged
        # hotel_specific or left ambiguous. Corporate/chain_area contacts
        # already carry correct scope from their source articles.
        scope = (contact.get("scope") or "unknown").lower()
        if scope not in ("hotel_specific", "unknown"):
            continue

        # Confirmation query — forces Google to show RECENT mentions
        # Use SHORT name — "Royalton Vessence Barbados, An Autograph Collection
        # All-Inclusive Resort – Adults Only" returns ZERO Google results.
        # "Royalton Vessence Barbados" works perfectly.
        verify_name = state.search_name or _shorten_hotel_name(state.hotel_name)
        query = f'"{name}" "{verify_name}" 2025 OR 2026 OR current OR present'
        if query in state.queries_run:
            continue
        state.queries_run.append(query)

        logger.info(f"[ITER 5/VERIFY] Query: {query}")
        try:
            results = await ce._search_web(query, max_results=5)
        except Exception as ex:
            logger.warning(f"[ITER 5] search failed for {name}: {ex}")
            continue

        if not results:
            # No recent mentions found — likely stale or fabricated. Downgrade.
            contact["scope"] = "chain_area"
            contact["_verification_result"] = "no_recent_mentions"
            contact["source_detail"] = (
                f"⚠ Could not verify {name} is currently at {state.hotel_name}. "
                f"No recent (2025-2026) mentions found. Verify before outreach."
            )
            logger.info(
                f"[ITER 5] {name}: no recent mentions → downgraded to chain_area"
            )
            continue

        # Scrape top 2 results to let Gemini extract current role + dates
        scraped_texts = []
        for r in results[:2]:
            url = r.get("url", "")
            try:
                text = await ce._scrape_url(url)
            except Exception:
                text = r.get("snippet", "")
            if text:
                scraped_texts.append({"url": url, "text": text[:3000]})

        # Also include the search result snippets (they often have enough signal
        # even when full-page scraping fails)
        snippets_blob = "\n\n".join(
            f"[{r.get('url','')}] {r.get('snippet','') or r.get('title','')}"
            for r in results[:5]
        )

        combined = (
            snippets_blob
            + "\n\n"
            + "\n\n".join(f"[{s['url']}] {s['text']}" for s in scraped_texts)
        )

        verdict = await _verify_role_with_gemini(
            name=name,
            candidate_title=contact.get("title") or "",
            target_hotel=state.hotel_name,
            operator_parent=state.operator_parent or "",
            evidence_blob=combined[:12000],
        )

        if not verdict:
            # Gemini couldn't decide — safer to downgrade
            contact["scope"] = "chain_area"
            contact["_verification_result"] = "inconclusive"
            contact["source_detail"] = (
                f"Role at {state.hotel_name} could not be verified. "
                f"Listed title: {contact.get('title') or 'unknown'}. "
                f"Verify current employment before outreach."
            )
            continue

        # Apply the verdict
        contact["_verification_result"] = verdict.get("status", "inconclusive")
        contact["_current_employer"] = verdict.get("current_employer")
        contact["_current_title"] = verdict.get("current_title")
        contact["_role_period"] = verdict.get("role_period")

        if verdict.get("status") == "currently_at_property":
            # Confirmed current — keep hotel_specific
            contact["scope"] = "hotel_specific"
            contact["source_detail"] = (
                f"✓ Current: {verdict.get('current_title') or contact.get('title')} "
                f"at {state.hotel_name}"
                + (
                    f" ({verdict.get('role_period')})"
                    if verdict.get("role_period")
                    else ""
                )
            )
        elif verdict.get("status") == "currently_at_sibling":
            # At a sibling property in the same cluster → chain_area
            contact["scope"] = "chain_area"
            sibling = verdict.get("current_employer") or "a sibling property"
            contact["source_detail"] = (
                f"Cluster contact: currently {verdict.get('current_title') or 'employed'} "
                f"at {sibling}. Not at {state.hotel_name}."
            )
        elif verdict.get("status") == "corporate":
            contact["scope"] = "chain_corporate"
            contact["source_detail"] = (
                f"Corporate: {verdict.get('current_title') or contact.get('title')} "
                f"at {verdict.get('current_employer') or state.operator_parent}"
            )
        elif verdict.get("status") == "former_employee":
            # Used to work at the property — downgrade with clear warning
            past = verdict.get("role_period") or "previously"
            contact["scope"] = "chain_area"
            contact["source_detail"] = (
                f"⚠ Former employee of {state.hotel_name} ({past}). "
                f"Currently: {verdict.get('current_title') or 'unknown role'} "
                f"at {verdict.get('current_employer') or 'unknown company'}. "
                f"Historical connection — confirm interest before outreach."
            )
            logger.info(
                f"[ITER 5] {name}: FORMER employee ({past}) → downgraded, current role: "
                f"{verdict.get('current_title')} @ {verdict.get('current_employer')}"
            )
        else:
            # Unknown / ambiguous
            contact["scope"] = "chain_area"
            contact["source_detail"] = (
                f"Role at {state.hotel_name} unclear. "
                f"Listed: {contact.get('title') or 'unknown'}. Verify before outreach."
            )

    # ── COMPANY-LEVEL VERIFICATION for corporate contacts ──
    # Juan Pablo Puerta left Hyatt → now CFO at Vitro Glass.
    # Property-level check doesn't catch this because he was never AT
    # the property. Check: is this person still at the COMPANY?
    operator = state.operator_parent or state.management_company or ""
    if operator:
        for contact in state.discovered_names:
            scope = (contact.get("scope") or "").lower()
            if scope not in ("chain_area", "chain_corporate"):
                continue
            # Skip if already verified/downgraded
            if contact.get("_verification_result"):
                continue
            name = (contact.get("name") or "").strip()
            if not name or len(name.split()) < 2:
                continue

            # Quick search: is this person still at the operator company?
            query = f'"{name}" "{operator}" 2025 OR 2026 OR current'
            if query in state.queries_run:
                continue
            state.queries_run.append(query)

            try:
                results = await ce._search_web(query, max_results=3)
            except Exception:
                continue

            if not results:
                # No recent mentions at this company — could have left
                contact["source_detail"] = (
                    f"⚠ No recent mentions of {name} at {operator}. "
                    f"May have left. Verify before outreach."
                )
                contact["_verification_result"] = "company_unverified"
                continue

            # Check snippets for red flags: "former", "left", "joined [other company]"
            blob = " ".join(
                (r.get("snippet") or "") + " " + (r.get("title") or "")
                for r in results[:3]
            ).lower()

            left_signals = [
                "former",
                "previously",
                "ex-",
                "departed",
                "has left",
                "moved to",
                "joined",
                "now at",
                "no longer",
                "vitro",
                "resigned",
            ]
            found_left = [s for s in left_signals if s in blob]
            if found_left:
                contact["source_detail"] = (
                    f"⚠ May have LEFT {operator}. "
                    f"Signals: {', '.join(found_left[:3])}. Verify before outreach."
                )
                contact["_verification_result"] = "possibly_departed"
                logger.info(
                    f"[ITER 5] {name}: possibly left {operator} — "
                    f"signals: {', '.join(found_left[:3])}"
                )

    state.iterations_done = 5
    return _fact_count(state) - facts_before


# ═══════════════════════════════════════════════════════════════
# ITERATION 5.5 — REGIONAL FIT VERIFICATION
# For contacts whose title says "Global" or has no clear region,
# run a quick search to discover which region they actually cover.
# The results get attached to the contact and feed Iter 6's reasoning.
# This prevents "SVP Global HIC Growth" (actually EMEA) from being
# misread as a Caribbean-property contact.
# ═══════════════════════════════════════════════════════════════
# For contacts whose title says "Global" or has no clear region,
# run a quick search to discover which region they actually cover.
# The results get attached to the contact and feed Iter 6's reasoning.
# This prevents "SVP Global HIC Growth" (actually EMEA) from being
# misread as a Caribbean-property contact.
# ═══════════════════════════════════════════════════════════════

# Region markers — titles containing these are considered regionally clear
_REGION_MARKERS = (
    "latam",
    "latin america",
    "caribbean",
    "americas",
    "north america",
    "south america",
    "usa",
    "us & canada",
    "emea",
    "europe",
    "eame",
    "mea",
    "middle east",
    "africa",
    "apac",
    "asia pacific",
    "asia",
    "oceania",
    "mexico",
    "central america",
)


async def iteration_5_5_regional_fit(state: ResearchState) -> int:
    """
    For contacts whose title says "Global" or lacks a clear regional qualifier,
    search for evidence of where they actually work. Attaches a
    `_region_evidence` field to each contact for Iter 6 to reason with.
    """
    from app.services import contact_enrichment as ce

    (state.region_term or "").lower()  # region used via _REGION_MARKERS
    (state.country or "").lower()

    checked = 0
    for contact in state.discovered_names:
        title = (contact.get("title") or "").lower()
        # Skip if title already has a clear regional marker
        if any(m in title for m in _REGION_MARKERS):
            continue
        # Skip if this is an on-property person (already regionally fit)
        if (contact.get("scope") or "") == "hotel_specific":
            continue
        # Skip C-suite — Presidents, CEOs, Chairmen are GLOBAL by definition.
        # Region-filtering them produces wrong results (e.g. "Jordi Pelfort,
        # President" flagged as "MEA" because his office is in Barbados).
        _CSUITE_SKIP = {
            "president",
            "ceo",
            "chief executive",
            "chairman",
            "group president",
            "executive chairman",
            "founder",
        }
        if any(cs in title for cs in _CSUITE_SKIP):
            contact["_region_evidence"] = "global (c-suite exempt)"
            continue
        # Skip if it's already flagged as former/historical
        if contact.get("_verification_result") in (
            "former_employee",
            "no_recent_mentions",
        ):
            continue

        name = (contact.get("name") or "").strip()
        if not name or len(name.split()) < 2:
            continue

        # One query per candidate — look up which region they cover
        query = f'"{name}" {state.operator_parent or ""} region OR based OR located OR covers'.strip()
        if query in state.queries_run:
            continue
        state.queries_run.append(query)

        try:
            results = await ce._search_web(query, max_results=4)
        except Exception as ex:
            logger.debug(f"[ITER 5.5] region search failed for {name}: {ex}")
            continue

        if not results:
            continue

        # Extract region hints from the snippet blob
        blob = " ".join(
            (r.get("snippet") or "") + " " + (r.get("title") or "") for r in results[:4]
        ).lower()

        found_regions: list[str] = []
        for m in _REGION_MARKERS:
            if m in blob:
                found_regions.append(m)

        if found_regions:
            contact["_region_evidence"] = ", ".join(sorted(set(found_regions)))
            logger.info(
                f"[ITER 5.5/REGION] {name}: found region evidence → {contact['_region_evidence']}"
            )
        checked += 1

    logger.info(f"[ITER 5.5/REGION] Checked {checked} ambiguous-region contacts")
    return checked


async def _verify_role_with_gemini(
    name: str,
    candidate_title: str,
    target_hotel: str,
    operator_parent: str,
    evidence_blob: str,
) -> dict | None:
    """
    Ask Gemini: given this evidence, is {name} currently at {target_hotel}?

    Returns: dict with keys:
        status:            currently_at_property | currently_at_sibling |
                           corporate | former_employee | unknown
        current_employer:  company name or property name
        current_title:     e.g. "Hotel Manager"
        role_period:       e.g. "Jul 2023–present" or "2017–2020"
    """
    from app.services import contact_enrichment as ce
    import json

    if not evidence_blob.strip():
        return None

    today_year = 2026  # matches the server's current-date reference
    prompt = f"""You are verifying whether a hospitality contact is CURRENTLY employed
at a specific hotel, based on evidence from LinkedIn, press releases, or news
articles. Current year: {today_year}.

CONTACT: {name}
LISTED TITLE (unverified): {candidate_title or 'unknown'}
TARGET HOTEL: {target_hotel}
OPERATOR PARENT: {operator_parent or 'unknown'}

TASK: Determine this person's CURRENT role ({today_year}), not past roles.
LinkedIn profiles list many jobs — only the one with "Present" or the most
recent end date counts as current.

Categorize the contact into ONE of these statuses:
- "currently_at_property"  → current job is AT {target_hotel} (or clearly covers it)
- "currently_at_sibling"   → current job is at a different property in the same brand/cluster
- "corporate"              → current job is at a corporate/regional office
- "former_employee"        → had a past role at {target_hotel} but that role has ENDED
- "unknown"                → cannot tell from the evidence

IMPORTANT:
- If evidence only mentions {target_hotel} in a past/historical context (e.g. a
  job that ended in 2020, 2021, 2022, 2023), status = "former_employee".
- If the current role is "Complex GM" or "Area GM" covering multiple properties,
  use "currently_at_sibling" unless {target_hotel} is explicitly in that scope.
- Be STRICT. "Could be at the property" is not good enough — require clear
  current-role evidence.

Respond with JSON only, no prose:
{{
  "status": "currently_at_property" | "currently_at_sibling" | "corporate" | "former_employee" | "unknown",
  "current_employer": "company or property where they work NOW",
  "current_title": "their current job title",
  "role_period": "Jul 2023–present" or "2017–2020" (target hotel role dates if known),
  "reasoning": "1 sentence"
}}

EVIDENCE:
{evidence_blob}
"""

    resp = await ce._call_gemini(prompt)
    if not resp:
        return None

    # Gemini sometimes returns the dict directly, sometimes wraps in {"candidates":...}
    if isinstance(resp, dict) and "status" in resp:
        return resp
    if isinstance(resp, str):
        try:
            return json.loads(resp)
        except json.JSONDecodeError:
            return None
    return None


# ═══════════════════════════════════════════════════════════════
# THE ITERATION CONTROLLER — orchestrates the 4 stages
# ═══════════════════════════════════════════════════════════════

# How many "good" contacts we want before we stop early
# Early stopping REMOVED — abundance principle. All iterations always run.

# Don't run more than this many iterations even if we have budget
_MAX_ITERATIONS = 4


# ═══════════════════════════════════════════════════════════════
# ITERATION 6 — REASONING PASS (SHIFT D)
# The final, most important step. After all hunting and verification,
# Gemini reads the full lead context + every discovered candidate and
# REASONS like a senior hospitality sales strategist:
#   "This is Dreams Rose Hall, reopening Q1 2027 as part of a 7-property
#    HIC Jamaica cluster after Hurricane Melissa. Who is actually running
#    operations and procurement for this property RIGHT NOW?"
# For each candidate, Gemini assigns:
#   - final priority (P1/P2/P3/P4)
#   - reasoning sentence explaining why
#   - optional scope correction
# ═══════════════════════════════════════════════════════════════


async def iteration_6_reasoning_pass(state: ResearchState) -> int:
    """
    Feed Gemini the whole lead context + all discovered candidates and
    have it reason about who is ACTUALLY handling operations right now.
    Enriches each contact with:
      - final_priority (P1/P2/P3/P4)
      - final_reasoning (one-sentence why)
      - scope may be corrected if Gemini sees a better fit
    """
    from app.services import contact_enrichment as ce
    import json

    if not state.discovered_names:
        return 0

    # Build compact contact payload for the prompt
    contacts_payload = []
    for i, c in enumerate(state.discovered_names):
        contacts_payload.append(
            {
                "idx": i,
                "name": c.get("name", ""),
                "title": c.get("title", ""),
                "organization": c.get("organization", ""),
                "scope": c.get("scope", "unknown"),
                "current_employer": c.get("_current_employer"),
                "current_title": c.get("_current_title"),
                "role_period": c.get("_role_period"),
                "verification": c.get("_verification_result"),
                "source_detail": c.get("source_detail", ""),
                "found_via_iteration": c.get("_iteration_found"),
                "region_evidence": c.get("_region_evidence"),  # Iter 5.5 finding
            }
        )

    # Timeline context for the prompt
    tl = (state.timeline_label or "").upper()
    timeline_hint = {
        "URGENT": "Opens in 3-5 months. On-site team is being hired now. GM and dept heads are critical.",
        "HOT": "Opens in 6-11 months — SWEET SPOT for uniform buying. Pre-opening team active. Incoming GM = gold.",
        "WARM": "Opens in 12-17 months. Pre-opening planning underway. Regional/corporate execs decide.",
        "COOL": "Opens 18+ months out. Corporate/regional execs own planning. GM may not be hired yet.",
        "EXPIRED": "Already opened or expired. Standard operational contacts apply.",
        "TBD": "Timeline unknown. Default to corporate/regional decision-makers.",
    }.get(tl, "Timeline unknown.")

    # ── Brand registry intel — THIS IS CRITICAL ──
    # ── Brand registry tiered context (v2) ──
    # Feed the 3-tier buyer hierarchy directly into the prompt so Gemini
    # maps each candidate to the correct tier (property/cluster/regional)
    # and assigns P1/P2/P3/P4 accordingly. This is the "abundance" principle:
    # we hunt for EVERY tier, and the strategist decides priority.
    brand_tier_block = ""
    brand_model_block = ""
    try:
        bi = BrandRegistry.lookup(state.brand) if state.brand else None
        if bi:
            prop_titles = bi.property_team_titles or []
            cluster_titles = bi.cluster_team_titles or []
            regional_titles = bi.regional_team_titles or []

            parts: list[str] = []
            if prop_titles:
                t_str = ", ".join(prop_titles[:12])
                parts.append(
                    f"PROPERTY TEAM (P1 candidates — on-site at THIS hotel):\n"
                    f"  Titles include: {t_str}\n"
                    f"  These are the day-to-day uniform buyers. Always P1 if\n"
                    f"  they currently work at {state.hotel_name}."
                )
            if cluster_titles:
                t_str = ", ".join(cluster_titles[:8])
                parts.append(
                    f"CLUSTER / AREA TEAM (P2 candidates — multi-property roles):\n"
                    f"  Titles include: {t_str}\n"
                    f"  These cover clusters of properties — strong secondary\n"
                    f"  contact when the on-property team is partially hired,\n"
                    f"  or for master contracts across the cluster."
                )
            if regional_titles:
                t_str = ", ".join(regional_titles[:10])
                parts.append(
                    f"REGIONAL TEAM (P2-P3 candidates — regional execs for {state.region_term or 'this region'}):\n"
                    f"  Titles include: {t_str}\n"
                    f"  These are the escalation path + master-contract signers.\n"
                    f"  P2 if their patch explicitly covers this property's\n"
                    f"  country. P3 if covering a broader region that includes\n"
                    f"  this area. P4 if wrong region (EMEA for Caribbean, etc.)."
                )

            if parts:
                brand_tier_block = (
                    f"\nBUYER HIERARCHY FOR {state.brand!r} "
                    f"(from JA's brand registry):\n\n" + "\n\n".join(parts) + "\n"
                )

            uf = (bi.uniform_freedom or "").lower()
            pm = (bi.procurement_model or "").lower()
            if uf in ("high", "full") or pm in (
                "fully_open",
                "independent",
                "owner_decides",
                "open",
            ):
                brand_model_block = (
                    "\nBRAND PROCUREMENT MODEL: INDEPENDENT / BOUTIQUE\n"
                    "For this brand, founders, principals, presidents, and COOs ARE the\n"
                    "uniform buyers. Do NOT downgrade them as 'too senior'.\n"
                )
            elif pm in ("avendra_gpo",) or (bi.gpo or ""):
                brand_model_block = (
                    f"\nBRAND PROCUREMENT MODEL: GPO-LOCKED ({bi.gpo or 'Avendra'})\n"
                    f"This brand uses a GPO. Regional VPs exist but have LESS direct\n"
                    f"vendor selection authority. On-property GM + Dir Housekeeping\n"
                    f"are the real buyers within approved-vendor lists.\n"
                )
            elif pm == "brand_managed":
                brand_model_block = (
                    "\nBRAND PROCUREMENT MODEL: BRAND-MANAGED (regional corporate IS reachable)\n"
                    "This brand's regional VP/SVP and Cluster GM roles are real\n"
                    "uniform buyers. Treat them as P1/P2 for properties in their region.\n"
                )
    except Exception as ex:
        logger.debug(f"Brand registry lookup in Iter 6 prompt failed: {ex}")

    # ── INDEPENDENT / BOUTIQUE FALLBACK ──
    # If no brand registry match and brand signals independence,
    # tell the strategist that founders ARE the buyers.
    if not brand_tier_block and not brand_model_block:
        brand_lower = (state.brand or "").lower().strip()
        if not brand_lower or brand_lower in ("independent", "boutique", "lifestyle"):
            brand_model_block = (
                "\nBRAND PROCUREMENT MODEL: INDEPENDENT / BOUTIQUE\n"
                "This is an independent hotel — NOT part of a major chain.\n"
                "Founders, CEOs, Managing Directors, Principals, and COOs ARE the\n"
                "uniform buyers. There is NO corporate procurement layer or GPO.\n"
                "The owner/operator makes every vendor decision directly.\n"
                "Do NOT downgrade founders or C-suite as 'too senior' — they are P1.\n"
            )

    # ── D1: GM-MISSING CASCADE CONTEXT ──
    # Iter 2 flipped this flag because no GM was found despite timeline
    # saying one SHOULD be hired by now. Tell the strategist to reweight
    # corporate procurement + regional VPs UP (P1, not P2) because they
    # own the vendor decision until the GM is hired. DOSM and Revenue
    # Director are warm intro paths, not decision-makers — P3.
    cascade_block = ""
    if state.gm_search_cascade_active:
        cascade_block = (
            "\nGM-MISSING CASCADE ACTIVE (CRITICAL PRIORITY REWEIGHT):\n"
            "No on-property GM has been announced for this property yet, but\n"
            "the timeline says one SHOULD be hired by now. Iter 2 fired cascade\n"
            "queries for DOSM, Revenue Director, and Area/Regional GM candidates.\n"
            "\n"
            "Reweight priorities as follows for this specific lead:\n"
            "  • Corporate procurement directors = P1 (NOT P3). They own the\n"
            "    vendor decision until the GM is hired and will sign the\n"
            "    opening contract.\n"
            "  • Regional VPs whose patch explicitly covers this property's\n"
            "    region = P1 (NOT P2). Same reasoning — decision sits with\n"
            "    them right now.\n"
            "  • Area/Cluster/Regional GM candidates = P1. They are the\n"
            "    interim decision-maker until a property GM is named.\n"
            "  • Task Force GM (Marriott-family brands) = P1 if found.\n"
            "  • DOSM (Director of Sales & Marketing) = P3. Warm intro path,\n"
            "    NOT a buyer. They know the organization but don't sign.\n"
            "  • Director of Revenue Management = P3. Same — not a buyer.\n"
            "\n"
            "This reweight only applies because the GM slot is unfilled. Do\n"
            "NOT apply it in the reasoning for other leads.\n"
        )

    # ── Phase B: Project-type context block ──
    # Tell the strategist what KIND of project this is, so contact
    # prioritization reflects procurement reality (e.g. reopening →
    # corporate wins, conversion → pre-opening GM + operator, etc.)
    phase_b_block = ""
    if state.project_type and state.project_type != "unknown":
        ptype_advice = {
            "reopening": (
                "REOPENING CONTEXT: This property was previously operating, closed, and is now returning.\n"
                "  → Property staff were redeployed or laid off; corporate owns the reopening procurement decision.\n"
                "  → Regional VP Operations and corporate procurement execs are the REAL buyers → P1.\n"
                "  → If a 'returning GM' is named (someone who ran this hotel before), P1 for them too.\n"
                "  → New F&B concepts usually debut with reopenings (new bars, restaurants) = NEW uniform SKUs.\n"
                "  → Don't over-index on property-level titles unless evidence says they're staying with THIS reopening."
            ),
            "conversion": (
                "CONVERSION CONTEXT: An existing building is being gutted and re-flagged under a new brand.\n"
                "  → Unlike a rebrand (staff retained), conversions rebuild operations: staff hired fresh, FF&E replaced.\n"
                "  → Operator has existing corporate procurement team — regional VP Ops / Procurement → P1.\n"
                "  → Pre-opening GM hired earlier than for ground-up new construction — if named, → P1.\n"
                "  → Old operator's staff are IRRELEVANT — don't prioritize anyone from the former flag."
            ),
            "rebrand": (
                "REBRAND CONTEXT: Existing hotel, SAME staff, new brand flag.\n"
                "  → MANDATORY uniform replacement — highest urgency scenario.\n"
                "  → Existing property team stays → on-site GM, Director of Housekeeping, HR, F&B = P1.\n"
                "  → Contact immediately — no ramp-up time needed."
            ),
            "renovation": (
                "RENOVATION-WHILE-OPERATING CONTEXT: Hotel stays open; phased updates.\n"
                "  → Current GM is ON-SITE and involved in procurement → P1.\n"
                "  → Dept heads (Housekeeping, F&B, HR) actively buying during the phased work → P1.\n"
                "  → Corporate is supportive but not the primary decision-maker."
            ),
            "new_opening": (
                "NEW-OPENING CONTEXT: Brand-new ground-up construction, no existing staff.\n"
                "  → Phase depends on timeline — see timeline_hint above.\n"
                "  → If HOT/URGENT: incoming GM (P1), dept heads being hired (P1/P2).\n"
                "  → If WARM/COOL: management-company corporate procurement (P1) — GM not yet hired."
            ),
            "ownership_change": (
                "OWNERSHIP-CHANGE CONTEXT: Property sold to new owner.\n"
                "  → If management changes too → treat like new_opening.\n"
                "  → If same management retained → existing GM is still the P1 buyer."
            ),
        }.get(state.project_type, "")

        if ptype_advice:
            phase_b_block = (
                f"\nPROJECT-TYPE ROUTING (classifier confidence: {state.project_confidence or 'unknown'}):\n"
                f"{ptype_advice}\n"
            )

    prompt = f"""You are a senior hospitality sales strategist for JA Uniforms,
a hotel uniform supplier. JA Uniforms needs 6 months of lead time to deliver
uniforms for a new opening or major renovation. Current date: April 2026.

LEAD CONTEXT:
- Hotel: {state.hotel_name}
- Brand: {state.brand or "unknown"}
- Location: {state.city or ""}, {state.state or ""}, {state.country or ""}
- Opening: {state.opening_date or "unknown"}
- Timeline label: {tl} — {timeline_hint}
- Project stage: {state.project_stage or "unknown"} (greenfield / reopening / renovation / conversion)
- Project TYPE (Phase A classifier): {state.project_type or "unknown"}
- Operator parent: {state.operator_parent or "unknown"}
- Owner company: {state.owner_company or "unknown"}
- Verified-current companies: {', '.join(state.verified_current_companies) or 'none confirmed'}
- Historical (skip) companies: {', '.join(state.historical_companies) or 'none'}
{phase_b_block}{brand_tier_block}{brand_model_block}{cascade_block}
YOUR JOB:
For each candidate below, decide (a) the FINAL priority and (b) a one-sentence
reasoning. Think like a salesperson: who is ACTUALLY handling operations and
procurement for THIS property, in THIS phase, RIGHT NOW?

CRITICAL RULE 1 — TITLE SENIORITY ≠ PRIORITY:
A "Senior Vice President" or "Vice President" title is NOT automatically P4.
It depends on WHAT role they cover. A "VP of Commercial Services for
LATAM/Caribbean" is THE buyer for a Caribbean property — P1. A "Group
President Americas" covering all North/South America is too senior for
direct outreach — P4. Use REGIONAL FIT, not seniority, to decide.

CRITICAL RULE 2 — REGIONAL FIT IS MANDATORY:
For this lead in {state.country or "unknown country"}, {state.region_term or "unknown region"}:
- A contact whose title says "LATAM", "Latin America", "Caribbean", "Americas" → fits a Caribbean lead
- A contact whose title says "EMEA", "Europe", "EAME", "MEA", "Asia Pacific", "APAC" → does NOT fit a Caribbean lead → P4
- A contact whose title says "Global" is AMBIGUOUS — look at their evidence
  text. If evidence places them in Europe or Asia, downgrade to P4. If
  evidence places them in the Americas, keep them.
- A contact with NO regional qualifier in their title — check their
  current_employer and source_detail. If location evidence points elsewhere,
  downgrade.

A "Global Growth and Owner Relations" SVP whose actual patch is Europe is
NOT a Caribbean property contact — make them P4 with reasoning that names
the mismatch.

CRITICAL RULE 3 — F&B TITLES: DECISION-MAKER vs LINE STAFF:
Kitchen and F&B titles vary hugely. Uniform buyers are the DECISION-MAKERS.
Line and supervisor roles wear uniforms but don't buy them.

  UNIFORM BUYERS (P1 or P2 if on-property):
  • Executive Chef, Head Chef, Corporate Executive Chef
  • F&B Director, Director of Food and Beverage
  • Director of Culinary, VP Culinary
  • Outlet/Restaurant General Manager (if named)

  NOT uniform buyers (P3 or P4):
  • Kitchen Supervisor, Line Cook, Sous Chef (unless Executive Sous Chef
    acting as #2 to the Executive Chef at a large property)
  • Culinary Chef Management (vague — treat as line role unless evidence
    clearly says department head)
  • Bartender, Server, Host, Steward, Dishwasher
  • Pastry Chef (unless Executive Pastry Chef at a flagship property)

When a title is ambiguous (e.g. "Culinary Chef Management"), default to P3
not P1. On-property scope alone does NOT elevate a line role to P1.

PRIORITY RULES (for a property in timeline {tl}):

- P1 = Call this person FIRST. They own the uniform buying decision today.
  * Any on-property GM, Hotel Manager, or Director of Operations
  * Any incoming GM announced for this property (HOT/WARM phase)
  * Regional VP/SVP whose patch EXPLICITLY covers this property's region
    (e.g. "VP Commercial Services LATAM" for a Caribbean property — P1)
  * Cluster General Manager covering this property
  * Director of Procurement / Director of Purchasing for this operator
  * For independent brands: founders, principals, COOs directly involved

- P2 = Strong backup / warm secondary contact.
  * On-property F&B Director, Director of Housekeeping, HR Director
  * Regional president of the brand's sub-organization (e.g. President LAC
    for Caribbean — their patch covers this property, call-worthy)
  * SVP Owner Relations / Growth — procurement-adjacent at regional level
  * Cluster GM at a sibling property in the same cluster

- P3 = Useful for research or escalation, not primary outreach.
  * Senior corporate roles (SVP Finance, SVP HR) at the regional level
  * Dept heads at a clearly different property in the same chain
  * Corporate procurement at HQ (not regional) level

- P4 = Do not call. Wrong person, irrelevant, or historical.
  * Former employee (left 2023 or earlier)
  * Front desk / concierge / sales / marketing / PR staff
  * Construction/development contractors
  * People at verified-historical companies (previous owners/operators)
  * Group-level executives whose region is GLOBAL or covers too broad a
    geography to be directly reachable (e.g. "Group President Americas"
    covering all of North + South America for a single Caribbean property)
  * Brand executives whose sub-brand clearly doesn't include this property
    (e.g. "SVP Field Ops LUXURY" for an all-inclusive property — P4 because
    Luxury ≠ Inclusive Collection)

For HOT/WARM pre-opening properties, an "incoming GM" who was announced but
hasn't started yet is STILL P1 — that announcement IS the buying moment.
Don't downgrade because they're "not on site yet."

For each candidate, also correct their scope if wrong:
- hotel_specific  = currently at THIS property
- chain_area      = at a sibling/cluster property OR regional role covering this property
- chain_corporate = at corporate HQ, not property-facing

Respond with ONLY a JSON array, one entry per candidate (by idx):
[
  {{"idx": 0, "priority": "P1", "reasoning": "...", "scope_correction": "hotel_specific"}},
  {{"idx": 1, "priority": "P4", "reasoning": "Former GM who left 2020.", "scope_correction": "chain_area"}},
  ...
]

CANDIDATES:
{json.dumps(contacts_payload, indent=2)}
"""

    resp = await ce._call_gemini(prompt)
    if not resp:
        logger.warning(
            "[ITER 6/REASONING] Gemini returned nothing — priorities unchanged"
        )
        return 0

    # Parse the response
    verdicts = None
    if isinstance(resp, list):
        verdicts = resp
    elif isinstance(resp, dict):
        # Gemini sometimes wraps in {"contacts": [...]} or {"verdicts": [...]}
        for k in ("verdicts", "contacts", "results", "items"):
            if k in resp and isinstance(resp[k], list):
                verdicts = resp[k]
                break
    elif isinstance(resp, str):
        try:
            parsed = json.loads(resp)
            if isinstance(parsed, list):
                verdicts = parsed
        except json.JSONDecodeError:
            pass

    if not verdicts:
        logger.warning(
            f"[ITER 6/REASONING] Could not parse Gemini response: {str(resp)[:200]!r}"
        )
        state.iterations_done = 6
        return 0

    # Apply verdicts to contacts
    applied = 0
    for v in verdicts:
        try:
            idx = int(v.get("idx", -1))
        except (TypeError, ValueError):
            continue
        if idx < 0 or idx >= len(state.discovered_names):
            continue

        contact = state.discovered_names[idx]
        priority = (v.get("priority") or "").upper().strip()
        reasoning = (v.get("reasoning") or "").strip()
        scope_corr = (v.get("scope_correction") or "").strip().lower()

        if priority in ("P1", "P2", "P3", "P4"):
            contact["_final_priority"] = priority
        if reasoning:
            contact["_final_reasoning"] = reasoning
            # Rich evidence: reasoning replaces the placeholder source_detail
            # IF the prior source_detail was weak ("LinkedIn mentions...").
            prior = contact.get("source_detail") or ""
            if not prior or "mentions" in prior.lower() or len(prior) < 30:
                contact["source_detail"] = reasoning
            else:
                # Append reasoning to preserve both prior evidence and new insight
                contact["source_detail"] = f"{prior} · {reasoning}"
        if scope_corr in ("hotel_specific", "chain_area", "chain_corporate"):
            contact["scope"] = scope_corr

        applied += 1

    logger.info(
        f"[ITER 6/REASONING] Applied {applied}/{len(state.discovered_names)} verdicts. "
        f"P1={sum(1 for c in state.discovered_names if c.get('_final_priority') == 'P1')}, "
        f"P2={sum(1 for c in state.discovered_names if c.get('_final_priority') == 'P2')}, "
        f"P3={sum(1 for c in state.discovered_names if c.get('_final_priority') == 'P3')}, "
        f"P4={sum(1 for c in state.discovered_names if c.get('_final_priority') == 'P4')}"
    )

    state.iterations_done = 6
    return applied


async def run_iterative_research(state: ResearchState) -> ResearchState:
    """
    Run the 4-iteration research loop. Stops early if:
    - We have 8+ contacts and basic LinkedIn coverage, OR
    - Last iteration produced no new facts (converged), OR
    - Hit max iterations.

    PHASE B: Before running any iteration, classify the project type and
    route accordingly. If the lead is rejected (residences_only), skip
    all iterations and return immediately with rejection flags set.
    """
    logger.info(
        f"[ITER] Starting iterative research for: {state.hotel_name} "
        f"(brand={state.brand}, country={state.country})"
    )

    # ══════════════════════════════════════════════════════════════
    # PHASE B — Project-type classification (runs BEFORE any iteration)
    # ══════════════════════════════════════════════════════════════
    # Uses what we already know (hotel_name, brand, timeline_label) to
    # decide:
    #   - Should we research at all? (residences_only → reject)
    #   - Should we hunt the property GM, or go straight to corporate?
    #   - What's the "story" we'll tell sales about this lead?
    try:
        from app.config.project_type_intelligence import classify_project_type

        # We don't have a description here — we only have what's on the lead.
        # The classifier will use hotel_name + management_company and produce
        # a sensible result. Better signals come in later when we have
        # scraped text — but Iter 1 runs after this, so we classify now and
        # optionally re-classify mid-flow if we want (not doing that yet).
        classification = classify_project_type(
            hotel_name=state.hotel_name or "",
            description=state.description or "",  # ← PHASE B fix: use DB description
            project_type=state.project_type or "",
            source_text=state.description
            or "",  # same — source_text AND description match
            timeline_label=state.timeline_label or "",
            management_company=state.management_company or state.brand or "",
        )

        # Persist on state for downstream access (Iter 6, sales narrative)
        state.project_type = classification.project_type
        state.project_confidence = classification.confidence
        state.project_signals = classification.signals
        state.phase_reason = classification.phase_reason
        state.should_reject = classification.should_reject
        state.rejection_reason = classification.rejection_reason

        logger.info(
            f"[PHASE B] Project type classified: {classification.project_type} "
            f"(confidence={classification.confidence}, signals={classification.signals}) "
            f"→ starting_phase={classification.starting_phase}, "
            f"should_reject={classification.should_reject}"
        )

        # ── Early reject for residences_only ──
        # Zero uniform opportunity. Don't waste Gemini/Serper cycles.
        if classification.should_reject:
            logger.warning(
                f"[PHASE B REJECT] Skipping all iterations for {state.hotel_name} — "
                f"reason={classification.rejection_reason}. "
                f"The lead SHOULD be marked status='rejected' by the caller."
            )
            state.iterations_done = 0
            return state

    except Exception as exc:
        # Defensive: if classification blows up, log and continue with the
        # existing pipeline. Never block research on a classifier bug.
        logger.warning(
            f"[PHASE B] Project-type classification failed ({exc}); "
            f"falling through to standard pipeline."
        )

    # ── Iteration 1: discovery ──
    new_facts = await iteration_1_discovery(state)
    logger.info(
        f"[ITER 1/DISCOVERY] +{new_facts} facts. "
        f"Owner={state.owner_company!r}, OperatorParent={state.operator_parent!r}, "
        f"Stage={state.project_stage!r}, Names={len(state.discovered_names)}"
    )

    # ── Iteration 2: GM hunt ──
    if _should_continue(state):
        new_facts = await iteration_2_gm_hunt(state)
        logger.info(
            f"[ITER 2/GM_HUNT] +{new_facts} facts. "
            f"NamedGM={state.has_named_gm}, Names={len(state.discovered_names)}"
        )

    # ── Iteration 2.5: on-property dept heads (Director of Sales, Rooms, F&B, HR) ──
    # Recovers the Phase 3 contacts the old v4 pipeline used to find:
    # Hotel Manager, Director of Sales/Events, Housekeeping, F&B Director, HR.
    # Only runs for HOT/URGENT/EXPIRED leads (skipped for WARM/COOL where
    # these dept heads aren't hired yet).
    if _should_continue(state):
        new_facts = await iteration_2_5_property_staff(state)
        logger.info(
            f"[ITER 2.5/STAFF] +{new_facts} facts. "
            f"Names={len(state.discovered_names)}"
        )

    # ── Iteration 3: corporate / owner hunt ──
    if _should_continue(state):
        new_facts = await iteration_3_corporate_hunt(state)
        logger.info(
            f"[ITER 3/CORPORATE] +{new_facts} facts. Names={len(state.discovered_names)}"
        )

    # ── Iteration 4: linkedin lookup (always run if we have any names) ──
    if state.discovered_names:
        await iteration_4_linkedin_lookup(state)
        with_linkedin = sum(1 for n in state.discovered_names if n.get("linkedin"))
        logger.info(
            f"[ITER 4/LINKEDIN] {with_linkedin}/{len(state.discovered_names)} have LinkedIn URLs"
        )

    # ── Iteration 5: verify current role (prevents stale hotel_specific tags) ──
    if state.discovered_names:
        await iteration_5_verify_current_role(state)
        downgraded = sum(
            1
            for n in state.discovered_names
            if n.get("_verification_result")
            in ("former_employee", "no_recent_mentions", "inconclusive")
            or (n.get("_verification_result") and n.get("scope") != "hotel_specific")
        )
        logger.info(
            f"[ITER 5/VERIFY] verified {len(state.discovered_names)} contacts, "
            f"{downgraded} downgraded from hotel_specific"
        )

    # ── Iteration 5.5: regional fit verification for ambiguous-region titles ──
    if state.discovered_names:
        await iteration_5_5_regional_fit(state)

    # ── Iteration 6: REASONING PASS (Shift D) ──
    # The final, most important step. Gemini reasons about who is ACTUALLY
    # running operations for this specific property, in this specific phase,
    # RIGHT NOW — and assigns final priorities (P1/P2/P3/P4) with reasoning.
    if state.discovered_names:
        await iteration_6_reasoning_pass(state)

    return state


def _should_continue(state: ResearchState) -> bool:
    """
    Always continue through all iterations. The abundance principle:
    more info is better. Every iteration adds value. The strategist
    (Iter 6) handles prioritization — not early stopping.
    """
    # Only hard-stop safety: prevent infinite loops
    if state.iterations_done >= 10:
        return False
    return True


# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════


def _shorten_hotel_name(name: str) -> str:
    """
    Strip common hotel suffixes to create a shorter search-friendly name.
    'Dreams Rose Hall Resort & Spa' → 'Dreams Rose Hall'
    'The Ritz-Carlton, Turks & Caicos' → 'Ritz-Carlton, Turks & Caicos'
    """
    s = (name or "").strip()
    # Strip leading "The "
    s = re.sub(r"^The\s+", "", s, flags=re.IGNORECASE)
    # Strip trailing hotel suffixes (order matters — longest first)
    suffixes = [
        r"\s+Resort\s*&\s*Spa$",
        r"\s+Resort\s+and\s+Spa$",
        r"\s+Hotel\s*&\s*Spa$",
        r"\s+Hotel\s+and\s+Spa$",
        r"\s+Beach\s+Resort\s*&\s*Spa$",
        r"\s+Beach\s+Resort$",
        r"\s+Resort\s+Hotel$",
        r"\s+Resort$",
        r"\s+Hotel\s*&\s*Residences$",
        r"\s+Hotel\s*&\s*Casino$",
        r"\s+Hotel\s*&\s*Suites$",
        r"\s+Hotel\s*&\s*Tower$",
        r"\s+Spa\s*&\s*Resort$",
        r"\s+Hotel$",
        r"\s+Suites$",
    ]
    for pat in suffixes:
        s = re.sub(pat, "", s, flags=re.IGNORECASE)
    return s.strip() or name.strip()


async def _extract_contacts_from_snippets(
    snippets: list[dict],
    hotel_name: str,
    location: str,
    ce_module,
) -> list[dict]:
    """
    SNIPPET-FIRST EXTRACTION: Feed Serper result titles + snippets directly
    to Gemini to extract names and titles WITHOUT scraping any URLs.

    This catches contacts from LinkedIn profiles, RocketReach pages,
    TripAdvisor reviews, and any other source where Google's snippet
    already contains the person's name and title — even when scraping
    the actual URL would fail (403, empty content, blocked).

    Returns a list of contact dicts: [{name, title, organization, source, ...}]
    """
    if not snippets:
        return []

    # Build a text block of all snippets for Gemini
    snippet_lines = []
    for i, item in enumerate(snippets):
        title = (item.get("title") or "").strip()
        snippet = (item.get("snippet") or "").strip()
        url = (item.get("url") or "").strip()
        if not snippet and not title:
            continue
        snippet_lines.append(
            f"Result {i+1} [{url[:80]}]:\n  Title: {title}\n  Snippet: {snippet}"
        )

    if not snippet_lines:
        return []

    snippet_block = "\n\n".join(snippet_lines[:20])  # cap at 20 results

    prompt = f"""Extract contact names from these Google search result snippets about the hotel "{hotel_name}" in {location}.

For each person mentioned who works at or is associated with this hotel (or its parent brand/operator), return their name, title, and organization.

IMPORTANT:
- Read the snippet text carefully. "Carl Ainscough - Hotel Manager" means Carl Ainscough IS the Hotel Manager.
- "Mr. X the General Manager at the hotel" means X IS the General Manager.
- LinkedIn snippets often show: "Name - Title · Company · Location" — extract all parts.
- RocketReach snippets say "Name is currently a Title at Company" — extract directly.
- If a person clearly works at a DIFFERENT hotel (not {hotel_name}), mark scope as "wrong_hotel".
- Only extract real person names. Skip company names, locations, generic text.

TITLE ACCURACY — CRITICAL:
- Each person's title must come from text DIRECTLY adjacent to their name.
- Do NOT assign a title to Person A that actually belongs to Person B in the same snippet.
- Example: "Omar Dueñas García ... Regional Commercial Director Juan Carlos Mendez" —
  Omar's title is NOT "Regional Commercial Director" — that belongs to Juan Carlos.
- If a person's title is unclear or not directly stated, leave it EMPTY rather than guessing.
- LinkedIn profile format: "Name - Title at Company" → the title is between the dash and "at".
- RocketReach format: "Name is currently a Title at Company" → title is after "currently a".

JOB POSTING DETECTION — CRITICAL:
- If someone is POSTING a job opening (e.g. "General Manager | {hotel_name} | Job Opportunity Link"),
  they are a RECRUITER, not the actual role holder. Do NOT extract them as the GM.
- Signals: "Job Opportunity", "hiring", "we're looking for", "apply now", "open position",
  "lnkd.in/e" (shortened job links), "Base Compensation", "Property opening in"
- Recruiters often have titles like "Principal", "Talent Acquisition", "HCA", "Headhunter"
- If the snippet is a job posting, SKIP the poster entirely.

SEARCH RESULT SNIPPETS:
{snippet_block}

Respond ONLY with JSON:
{{"contacts": [
  {{"name": "...", "title": "...", "organization": "...", "scope": "hotel_specific" | "chain_area" | "wrong_hotel", "confidence": "high" | "medium" | "low"}}
]}}
If no contacts found, return {{"contacts": []}}
"""

    try:
        resp = await ce_module._call_gemini(prompt)
    except Exception as ex:
        logger.debug(f"Snippet extraction Gemini call failed: {ex}")
        return []

    if not resp:
        return []

    contacts = []
    if isinstance(resp, dict):
        contacts = resp.get("contacts", [])
    elif isinstance(resp, list):
        contacts = resp

    extracted = []
    for c in contacts:
        if c.get("scope") in ("wrong_hotel", "irrelevant"):
            continue
        name = (c.get("name") or "").strip()
        if name and len(name) >= 3 and len(name.split()) >= 2:
            extracted.append(c)

    if extracted:
        logger.info(
            f"[SNIPPET] Extracted {len(extracted)} contacts from snippets: "
            f"{', '.join(c.get('name', '?') for c in extracted)}"
        )

    return extracted


async def _run_queries_and_extract(
    state: ResearchState,
    queries: list[str],
    ce_module,
    scrape_limit: int = 5,
) -> None:
    """
    Run a batch of queries, extract contacts from snippets FIRST,
    then scrape top results for additional detail.
    """
    import asyncio
    import os

    all_results = []
    for q in queries:
        if q in state.queries_run:
            continue
        state.queries_run.append(q)
        logger.info(f"[ITER {state.iterations_done + 1}] Query: {q}")
        try:
            results = await ce_module._search_web(q, max_results=5)
        except Exception as ex:
            logger.warning(f"Search failed for {q!r}: {ex}")
            continue
        for r in results:
            url = r.get("url", "")
            if url and url not in [u.get("url") for u in all_results]:
                all_results.append(r)
        # Throttle between queries
        delay = 0.5 if os.getenv("SERPER_API_KEY") else 1.5
        await asyncio.sleep(delay)

    if not all_results:
        return

    # ── SNIPPET-FIRST EXTRACTION ──
    # Feed ALL snippets to Gemini BEFORE scraping. This catches contacts
    # from LinkedIn, RocketReach, TripAdvisor, and any blocked site
    # where the snippet already contains name + title.
    location = ", ".join(filter(None, [state.city, state.state, state.country]))
    snippet_contacts = await _extract_contacts_from_snippets(
        all_results, state.hotel_name, location, ce_module
    )
    for c in snippet_contacts:
        name = (c.get("name") or "").strip()
        if not name or len(name) < 3 or len(name.split()) < 2:
            continue
        if any(
            n.get("name", "").lower().strip() == name.lower()
            for n in state.discovered_names
        ):
            continue
        # Find the source URL — match snippet back to result.
        # SKIP junk URLs (Facebook, Instagram — can't be opened, useless as evidence).
        # PREFER quality sources (RocketReach, LinkedIn, HospitalityNet, news sites).
        _JUNK_DOMAINS = {
            "facebook.com",
            "instagram.com",
            "tiktok.com",
            "pinterest.com",
            "twitter.com",
            "x.com",
        }
        _PREFERRED_DOMAINS = {
            "rocketreach.co",
            "linkedin.com",
            "hospitalitynet.org",
            "hotel-online.com",
            "hotelexecutive.com",
            "hoteldive.com",
            "travelpulse.com",
            "travelweekly.com",
            "hospitalitymagazine.com",
            "mauinow.com",
            "jamaica-gleaner.com",
            "jhta.org",
        }

        source_url = ""
        fallback_url = ""
        name_lower = name.lower()
        for r in all_results:
            blob = ((r.get("snippet") or "") + " " + (r.get("title") or "")).lower()
            if name_lower.replace(" ", "").replace(".", "") not in blob.replace(
                " ", ""
            ).replace(".", ""):
                continue
            url = r.get("url", "")
            url_lower = url.lower()
            # Skip junk domains entirely
            if any(d in url_lower for d in _JUNK_DOMAINS):
                continue
            # Preferred source? Use immediately
            if any(d in url_lower for d in _PREFERRED_DOMAINS):
                source_url = url
                break
            # Otherwise save as fallback (first non-junk match)
            if not fallback_url:
                fallback_url = url
        source_url = source_url or fallback_url
        entry = {
            "name": name,
            "title": (c.get("title") or "").strip(),
            "organization": (c.get("organization") or "").strip(),
            "scope": c.get("scope") or "unknown",
            "confidence": c.get("confidence") or "medium",
            "source": source_url,
            "source_type": "snippet",
            "linkedin": c.get("linkedin"),
            "_iteration_found": state.iterations_done + 1,
        }
        state.discovered_names.append(entry)

    # ── CAPTURE LinkedIn URLs from search results ──
    # When a linkedin.com/in/ URL appears in results, the scraper will skip
    # it ("non-article site"). But the URL itself is gold — save it on any
    # matching contact. Example: search for "Dreams Rose Hall general manager"
    # returns https://jm.linkedin.com/in/carl-ainscough-cja1961 — that URL
    # should be saved on Carl Ainscough's contact record.
    for r in all_results:
        url = r.get("url") or ""
        if "linkedin.com/in/" not in url:
            continue
        # Try to match this LinkedIn URL to a discovered contact
        snippet = ((r.get("snippet") or "") + " " + (r.get("title") or "")).lower()
        for contact in state.discovered_names:
            if contact.get("linkedin"):
                continue  # already has a LinkedIn URL
            cname = (contact.get("name") or "").lower().strip()
            if not cname or len(cname) < 3:
                continue
            # Match by name in snippet text OR name in URL slug
            url_slug = url.lower().split("linkedin.com/in/")[-1].split("?")[0]
            name_parts = cname.split()
            # Check if name appears in snippet
            name_in_snippet = cname.replace(" ", "") in snippet.replace(" ", "")
            # Check if name parts appear in URL slug (carl-ainscough in slug)
            name_in_slug = all(
                part.replace(".", "") in url_slug
                for part in name_parts
                if len(part) > 2
            )
            if name_in_snippet or name_in_slug:
                contact["linkedin"] = url
                logger.info(
                    f"[LINKEDIN] Captured URL for {contact['name']} from search results: {url}"
                )
                break  # one URL per contact

    # ── THEN scrape URLs for additional contacts + detail ──
    # Sort by hospitality news priority (hospitalitynet, etc come first)
    try:
        from app.config.enrichment_config import HOSPITALITY_NEWS_DOMAINS

        def _prio(item):
            u = (item.get("url") or "").lower()
            for i, d in enumerate(HOSPITALITY_NEWS_DOMAINS):
                if d in u:
                    return i
            return 100

        all_results.sort(key=_prio)
    except Exception:
        pass

    for item in all_results[:scrape_limit]:
        url = item.get("url", "")
        if url in state.urls_scraped:
            continue
        state.urls_scraped.append(url)

        try:
            text = await ce_module._scrape_url(url)
        except Exception as ex:
            logger.debug(f"Scrape failed for {url}: {ex}")
            continue
        if not text or len(text) < 100:
            continue

        try:
            extracted = await ce_module._extract_contacts_with_gemini(
                text, state.hotel_name, location
            )
        except Exception as ex:
            logger.debug(f"Gemini extraction failed for {url}: {ex}")
            continue
        if not extracted:
            continue

        for c in extracted.get("contacts", []):
            if c.get("scope") in ("wrong_hotel", "irrelevant"):
                continue
            name = (c.get("name") or "").strip()
            if not name or len(name) < 3 or len(name.split()) < 2:
                continue
            # Skip if already discovered (from snippets or earlier)
            if any(
                n.get("name", "").lower().strip() == name.lower()
                for n in state.discovered_names
            ):
                continue
            entry = {
                "name": name,
                "title": (c.get("title") or "").strip(),
                "organization": (c.get("organization") or "").strip(),
                "scope": c.get("scope") or "unknown",
                "confidence": c.get("confidence") or "medium",
                "source": url,
                "source_type": "trade_press",
                "linkedin": c.get("linkedin"),
                "_iteration_found": state.iterations_done + 1,
            }
            state.discovered_names.append(entry)


def _fact_count(state: ResearchState) -> int:
    """Count of distinct facts in state (used for new-info detection)."""
    n = 0
    if state.owner_company:
        n += 1
    if state.operator_parent:
        n += 1
    if state.project_stage:
        n += 1
    n += len(state.discovered_names)
    n += len(state.cluster_siblings)
    return n


def _guess_owner_from_state(state: ResearchState) -> Optional[str]:
    """
    Look at the organizations attached to discovered contacts to guess
    the owner company. The owner often appears repeatedly in extracted
    organization fields.
    """
    if not state.discovered_names:
        return None
    org_counts: dict[str, int] = {}
    for n in state.discovered_names:
        org = (n.get("organization") or "").strip()
        if not org or len(org) < 3:
            continue
        # Skip the brand itself and the operator parent
        if state.brand and org.lower() == state.brand.lower():
            continue
        if state.operator_parent and org.lower() in state.operator_parent.lower():
            continue
        org_counts[org] = org_counts.get(org, 0) + 1
    if not org_counts:
        return None
    # Return the most-repeated org with more than 1 mention
    best = sorted(org_counts.items(), key=lambda kv: -kv[1])[0]
    if best[1] >= 2:
        return best[0]
    return None


def _guess_stage_from_state(state: ResearchState) -> str:
    """
    Infer project stage from scraped URL evidence.

    PHASE B: Now uses the full Phase A classifier (classify_project_type)
    on the combined URL/query text. The classifier's output types map to
    this function's historical outputs:
        new_opening      → "greenfield"   (maintains legacy naming)
        reopening        → "reopening"
        conversion       → "conversion"
        renovation       → "renovation"
        rebrand          → "conversion"   (rebrand = brand conversion)
        residences_only  → "greenfield"   (won't get here — rejected earlier)
        ownership_change → "conversion"
        unknown          → "greenfield"   (conservative default)

    NOTE: Returns the legacy string names ("greenfield", "reopening",
    "conversion", "renovation") because `state.project_stage` is used
    elsewhere with those exact strings. The canonical Phase A type is
    stored separately on `state.project_type`.
    """
    try:
        from app.config.project_type_intelligence import classify_project_type

        # Combine URL evidence + hotel metadata + description into a single text blob.
        # Description is the richest input; URLs give additional signals from scraped articles.
        url_text = " ".join(state.urls_scraped)
        combined = (
            (state.description or "")
            + " "
            + url_text
            + " "
            + " ".join(
                filter(
                    None,
                    [
                        state.hotel_name or "",
                        state.brand or "",
                        state.management_company or "",
                    ],
                )
            )
        )

        r = classify_project_type(
            hotel_name=state.hotel_name or "",
            description=combined,
            source_text=combined,
            timeline_label=state.timeline_label or "",
            management_company=state.management_company or state.brand or "",
        )

        # Update the richer fields too — re-classification after Iter 1 gives
        # us a better picture than the initial top-of-run call.
        state.project_type = r.project_type
        state.project_confidence = r.confidence
        state.project_signals = r.signals
        state.phase_reason = r.phase_reason

        logger.info(
            f"[STAGE/phase_a] Re-classified after Iter 1: "
            f"project_type={r.project_type}, confidence={r.confidence}, "
            f"signals={r.signals[:3]}"
        )

        # Map to legacy project_stage strings
        mapping = {
            "reopening": "reopening",
            "conversion": "conversion",
            "renovation": "renovation",
            "rebrand": "conversion",
            "ownership_change": "conversion",
            "new_opening": "greenfield",
            "residences_only": "greenfield",  # shouldn't reach here
            "unknown": "greenfield",
        }
        return mapping.get(r.project_type, "greenfield")

    except Exception as exc:
        # Defensive fallback to the old keyword heuristic
        logger.debug(f"Phase A classifier failed in _guess_stage; fallback. {exc}")
        text_blob = " ".join(state.urls_scraped).lower()
        if any(
            k in text_blob
            for k in ("reopen", "post-hurricane", "renovation", "rebuild")
        ):
            return "reopening"
        if any(k in text_blob for k in ("rebrand", "conversion", "joins")):
            return "conversion"
        if any(k in text_blob for k in ("renovation", "renovate")):
            return "renovation"
        return "greenfield"


def _looks_like_gm(title: str) -> bool:
    """Did we find a named property GM?"""
    if not title:
        return False
    t = title.lower()
    return any(
        k in t
        for k in (
            "general manager",
            "hotel manager",
            "managing director",
            "cluster gm",
            "cluster general manager",
            "task force",
            "pre-opening manager",
        )
    )
