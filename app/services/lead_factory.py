"""
SMART LEAD HUNTER - Lead Factory
=================================
Single entry point for ALL lead creation, regardless of source.
Ensures every lead gets: normalization, scoring, dedup check, enrichment.

Used by:
- POST /leads (manual API)
- orchestrator.save_leads_to_database (pipeline)
- scraping_tasks._save_lead_impl (Celery)

DEDUP STRATEGY (in save_lead_to_db):
  1. Exact match on `hotel_name_normalized` (fast path).
  2. Fuzzy match. Three steps:
     a) Build candidate pool by location.
     b) Normalize both names (strip multi-word brand suffixes, punctuation,
        generic mid-words like "the"/"hotel"/"resort").
     c) Strip shared location words from both names so that "Miami Beach"
        appearing in both "Grand Hyatt Miami Beach" and "Hilton Miami Beach"
        doesn't falsely signal a duplicate.
     d) Compare cores: identical → match; containment with shared first word
        → match; ≥2 shared words AND ≥60% overlap of shorter set → match.

  Candidate pool rules:
    - Always include leads where city OR state matches.
    - If `state` is blank in the new lead, OR the country is in
      SMALL_COUNTRIES (Caribbean / micro-states where city ≈ state ≈ country),
      ALSO include leads from the same country. Catches the Royalton-Barbados
      case where the new extraction had city='St. James' but the existing
      record had city='Barbados'.

  Why this catches both real-world bugs:
    - Royalton case: country fallback puts Barbados record into pool; after
      stripping suffix and location words, both cores are 'royalton vessence'
      → identical → match.
    - Ritz Savannah case: city filter puts both in pool; after dropping "the"/
      "hotel" generic tokens AND stripping "savannah" location word, both
      cores are 'ritz carlton' → identical → match.
"""

import logging
import re
from typing import Dict, Optional, Tuple

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.potential_lead import PotentialLead

from app.services.utils import (
    normalize_hotel_name,
    normalize_state,
    local_now,
    get_timeline_label,
)
from app.services.scorer import calculate_lead_score
from app.config.intelligence_config import SCORE_HOT_THRESHOLD, SCORE_WARM_THRESHOLD

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# DEDUP NORMALIZATION
# ─────────────────────────────────────────────────────────────────────────────

# Multi-word brand/collection suffixes to strip BEFORE punctuation removal.
# Comma is REQUIRED (not optional) so the regex doesn't eat the brand from
# names like "Four Seasons Resort Maui" or "Viceroy Snowmass" — those are
# the actual hotel name, not a third-party brand suffix.
_BRAND_SUFFIXES = re.compile(
    r",\s*(?:An?\s+)?(?:Autograph|Curio|Luxury|Tribute|Tapestry|Unbound)\s+Collection.*$"
    r"|,\s*(?:A\s+)?(?:Viceroy|Auberge|Ritz-Carlton|Four Seasons|Six Senses)\s+(?:Resort|Collection|Hotel|Estate).*$"
    r"|\s*[-\u2013\u2014]\s*(?:Adults?\s+Only|All[- ]Inclusive).*$"
    r"|,\s*by\s+(?:Hilton|Hyatt|Marriott|IHG).*$",
    re.IGNORECASE,
)

# Generic words that don't help distinguish hotels — dropped from the word
# set used for fuzzy matching. "The Ritz-Carlton Hotel Savannah" and
# "Ritz-Carlton Savannah" must reduce to the same set after dropping these.
_GENERIC_TOKENS = frozenset(
    {
        "the",
        "a",
        "an",
        "by",
        "of",
        "and",
        "hotel",
        "hotels",
        "resort",
        "resorts",
        "inn",
        "lodge",
        "suites",
        "suite",
        "spa",
        "club",
        "collection",
        "residences",
        "residence",
        "house",
        "tower",
        "towers",
    }
)

# Countries where city ≈ state ≈ country (or location data is sparse) so the
# city/state filter often fails to put duplicate records into the same pool.
# When the new lead's country is in this set, we ALSO match candidates by
# country alone.
_SMALL_COUNTRIES = frozenset(
    {
        "anguilla",
        "antigua",
        "antigua and barbuda",
        "aruba",
        "bahamas",
        "barbados",
        "belize",
        "bermuda",
        "british virgin islands",
        "cayman islands",
        "curacao",
        "curaçao",
        "dominica",
        "dominican republic",
        "grenada",
        "guadeloupe",
        "haiti",
        "jamaica",
        "martinique",
        "montserrat",
        "puerto rico",
        "saba",
        "saint barthelemy",
        "saint barthélemy",
        "saint kitts and nevis",
        "saint lucia",
        "st lucia",
        "st. lucia",
        "saint martin",
        "saint vincent and the grenadines",
        "sint maarten",
        "trinidad and tobago",
        "turks and caicos",
        "turks and caicos islands",
        "u.s. virgin islands",
        "us virgin islands",
        "usvi",
        "andorra",
        "liechtenstein",
        "luxembourg",
        "monaco",
        "san marino",
        "vatican city",
    }
)


def _normalize_for_dedup(name: str) -> str:
    """Aggressively normalize a hotel name for fuzzy comparison.

    Pipeline:
      1. Strip multi-word brand/collection suffixes (", An Autograph
         Collection ..." etc.) — comma-prefixed only so we don't eat
         the brand from names where the brand IS the hotel name.
      2. Lowercase, strip ALL non-alphanumeric chars to whitespace.
      3. Drop generic tokens like "the", "hotel", "resort".
      4. Collapse whitespace.

    Examples:
      "The Ritz-Carlton Hotel Savannah, A Member Of Marriott"
                                              →  "ritz carlton savannah member marriott"
      "Ritz-Carlton Savannah"                 →  "ritz carlton savannah"
      "Royalton Vessence Barbados, An Autograph Collection All-Inclusive Resort"
                                              →  "royalton vessence barbados"
      "Four Seasons Resort Maui"              →  "four seasons maui"
    """
    if not name:
        return ""
    cleaned = _BRAND_SUFFIXES.sub("", name)
    cleaned = re.sub(r"[^a-z0-9\s]", " ", cleaned.lower())
    tokens = [t for t in cleaned.split() if t and t not in _GENERIC_TOKENS]
    return " ".join(tokens)


def _strip_location_words(core: str, *location_strings: Optional[str]) -> str:
    """Remove location words (≥3 chars) from a normalized core name.

    Used during fuzzy dedup so that location terms appearing in both names
    (e.g. "Miami Beach" in "Grand Hyatt Miami Beach" AND in "Hilton Miami
    Beach") don't inflate the word-overlap score and cause false matches
    between different brands at the same location.

    Pass locations from BOTH leads so we strip whichever variants apply.
    """
    location_words: set[str] = set()
    for loc in location_strings:
        if not loc:
            continue
        for w in re.sub(r"[^a-z0-9\s]", " ", loc.lower()).split():
            if len(w) >= 3:
                location_words.add(w)
    if not location_words:
        return core
    return " ".join(w for w in core.split() if w not in location_words)


def _names_match(core_a: str, core_b: str) -> bool:
    """Decide whether two normalized + location-stripped names refer to the
    same hotel.

    Logic:
      - Identical strings → match.
      - One contained in the other AND shared first word → match.
        (Prevents "hyatt" inside "grand hyatt" from matching.)
      - ≥2 shared words AND those make up ≥60% of the shorter word set → match.
    """
    if not core_a or not core_b:
        return False
    if core_a == core_b:
        return True

    words_a = core_a.split()
    words_b = core_b.split()
    if not words_a or not words_b:
        return False

    short, long = (
        (words_a, words_b) if len(words_a) <= len(words_b) else (words_b, words_a)
    )

    if " ".join(short) in " ".join(long) and short[0] == long[0]:
        return True

    set_a = set(words_a)
    set_b = set(words_b)
    common = set_a & set_b
    if len(common) < 2:
        return False

    overlap_ratio = len(common) / len(set(short))
    return overlap_ratio >= 0.6


def _build_location_filters(lead_dict: Dict) -> list:
    """Return SQLAlchemy filter clauses for the fuzzy candidate pool.

    Always includes city/state matches when those fields are present.
    Adds country match when state is missing OR country is in SMALL_COUNTRIES
    (catches the Caribbean/micro-state case where city ≈ state ≈ country).
    """
    city = (lead_dict.get("city") or "").strip().lower()
    state = (lead_dict.get("state") or "").strip().lower()
    country = (lead_dict.get("country") or "").strip().lower()

    filters = []
    if city:
        filters.append(PotentialLead.city.ilike(f"%{city}%"))
    if state:
        filters.append(PotentialLead.state.ilike(f"%{state}%"))
        # Also catch when state was accidentally stored in city field
        filters.append(PotentialLead.city.ilike(f"%{state}%"))
    if country and (not state or country in _SMALL_COUNTRIES):
        filters.append(PotentialLead.country.ilike(f"%{country}%"))
    return filters


# ─────────────────────────────────────────────────────────────────────────────
# JUNK / VALIDATION HELPERS
# ─────────────────────────────────────────────────────────────────────────────


def extract_year(date_str: Optional[str]) -> Optional[int]:
    """Extract year from opening date string like 'Q3 2027' or '2026'."""
    if not date_str:
        return None
    match = re.search(r"20\d{2}", str(date_str))
    return int(match.group()) if match else None


# Patterns that indicate article titles / market summaries, not real hotels
_JUNK_PATTERNS = [
    re.compile(r"^\d+ new hotels? (in|for|forecasted|opening)", re.IGNORECASE),
    re.compile(r"hotels? forecasted for", re.IGNORECASE),
    re.compile(r"hotels? opening in 20\d{2}", re.IGNORECASE),
    re.compile(r"hotels? in \d{4}", re.IGNORECASE),
    re.compile(r"hotel construction", re.IGNORECASE),
    re.compile(r"hotel pipeline", re.IGNORECASE),
    re.compile(r"hotel forecast", re.IGNORECASE),
    re.compile(r"new openings for 20\d{2}", re.IGNORECASE),
    # Unnamed/generic projects
    re.compile(r"^new \d+-key ", re.IGNORECASE),
    re.compile(r"^multiple\s+(new\s+)?hotels?", re.IGNORECASE),
    re.compile(r"^several\s+(new\s+)?hotels?", re.IGNORECASE),
    re.compile(r"^various\s+(new\s+)?hotels?", re.IGNORECASE),
    re.compile(r"^unnamed\b", re.IGNORECASE),
    re.compile(r"\(unnamed\)", re.IGNORECASE),
    re.compile(r"^untitled hotel", re.IGNORECASE),
    re.compile(r"resort \(unnamed\)", re.IGNORECASE),
    re.compile(r"^proposed\s", re.IGNORECASE),
    # Non-hotel venues
    re.compile(r"\bcamps?\b", re.IGNORECASE),
    re.compile(r"\bglamping\b", re.IGNORECASE),
    re.compile(r"\bcampground\b", re.IGNORECASE),
    re.compile(r"\btreehouse\b", re.IGNORECASE),
    re.compile(r"\btiny\s+house\b", re.IGNORECASE),
]


def prepare_lead(
    lead_dict: Dict,
) -> Tuple[Optional[PotentialLead], Optional[str], Dict]:
    """Normalize, score, and build a PotentialLead from any source."""
    hotel_name = (lead_dict.get("hotel_name") or "").strip()
    if not hotel_name:
        return None, "No hotel name", {}

    for pattern in _JUNK_PATTERNS:
        if pattern.search(hotel_name):
            return None, f"Article title, not a hotel: {hotel_name}", {}

    opening_date = (lead_dict.get("opening_date") or "").strip()
    vague_dates = [
        "coming soon",
        "tbd",
        "tba",
        "unknown",
        "announced",
        "not announced",
        "n/a",
    ]
    if opening_date.lower() in vague_dates:
        lead_dict["opening_date"] = None

    # ── HARD LOCATION GATE ────────────────────────────────────────────────
    # Refuse leads where BOTH state AND country are empty AND the city
    # isn't in our known US/Caribbean market list.
    #
    # Rationale: city alone is insufficient signal in general — "Jinan"
    # could be the Chinese city, an OCR misread, or junk. Without state
    # or country the scorer falls through to "Assume US" (+10) and
    # international leads slip in.
    #
    # Exception: if the city IS in our recognized US or Caribbean
    # keyword list (Chicago, Miami, Gustavia, Mustique, Parrot Cay, etc.),
    # trust the extraction — the city name itself carries the geographic
    # signal.
    #
    # Cases handled:
    #   state="FL", country=""        → scorer recognizes US via state
    #   state="", country="Bahamas"   → scorer recognizes Caribbean
    #   state="Shandong", country=""  → scorer Step 6 rejects as international
    #   city="Chicago", rest empty    → passes (city is in OTHER_US_KEYWORDS)
    #   city="Gustavia", rest empty   → passes (city is in CARIBBEAN_KEYWORDS)
    #   city="Jinan", rest empty      → REJECTED (city not in any list)
    #   city="Kyoto", rest empty      → already rejected by scorer's
    #                                   INTERNATIONAL_SKIP city check
    state_present = bool((lead_dict.get("state") or "").strip())
    country_present = bool((lead_dict.get("country") or "").strip())
    if not state_present and not country_present:
        from app.services.scorer import is_known_us_or_caribbean_city

        city = (lead_dict.get("city") or "").strip()
        if not is_known_us_or_caribbean_city(city):
            return (
                None,
                f"Insufficient location — city '{city}' not in US/Caribbean list: {hotel_name}",
                {},
            )

    normalized = normalize_hotel_name(hotel_name)

    score_result = calculate_lead_score(
        hotel_name=hotel_name,
        city=lead_dict.get("city"),
        state=normalize_state(lead_dict.get("state") or ""),
        country=lead_dict.get("country", "USA"),
        opening_date=lead_dict.get("opening_date"),
        room_count=lead_dict.get("room_count"),
        contact_name=lead_dict.get("contact_name"),
        contact_email=lead_dict.get("contact_email"),
        contact_phone=lead_dict.get("contact_phone"),
        brand=lead_dict.get("brand"),
    )

    if not score_result.get("should_save", True):
        return None, score_result.get("skip_reason", "Filtered"), score_result

    pipeline_score = lead_dict.get("qualification_score") or lead_dict.get("lead_score")
    final_score = pipeline_score if pipeline_score else score_result["total_score"]

    room_count = None
    try:
        room_count = int(float(lead_dict.get("room_count", 0) or 0))
        if room_count == 0:
            room_count = None
    except (ValueError, TypeError):
        pass

    lead = PotentialLead(
        hotel_name=hotel_name,
        hotel_name_normalized=normalized,
        brand=lead_dict.get("brand") or None,
        brand_tier=score_result.get("brand_tier"),
        hotel_type=lead_dict.get("property_type") or lead_dict.get("hotel_type"),
        hotel_website=lead_dict.get("hotel_website"),
        city=lead_dict.get("city"),
        state=lead_dict.get("state"),
        country=lead_dict.get("country", "USA"),
        location_type=score_result.get("location_type"),
        opening_date=lead_dict.get("opening_date"),
        opening_year=score_result.get("opening_year")
        or extract_year(lead_dict.get("opening_date")),
        timeline_label=get_timeline_label(lead_dict.get("opening_date") or ""),
        room_count=room_count,
        contact_name=lead_dict.get("contact_name"),
        contact_title=lead_dict.get("contact_title"),
        contact_email=lead_dict.get("contact_email"),
        contact_phone=lead_dict.get("contact_phone"),
        description=lead_dict.get("key_insights") or lead_dict.get("description"),
        key_insights=lead_dict.get("key_insights"),
        management_company=lead_dict.get("management_company"),
        developer=lead_dict.get("developer"),
        owner=lead_dict.get("owner"),
        source_url=lead_dict.get("source_url"),
        source_site=lead_dict.get("source_name")
        or lead_dict.get("source_site")
        or "manual",
        lead_score=final_score,
        score_breakdown=score_result.get("breakdown", {}),
        status="expired"
        if get_timeline_label(lead_dict.get("opening_date") or "") == "EXPIRED"
        else "new",
        raw_data=lead_dict.get("raw_data"),
        scraped_at=local_now(),
        created_at=local_now(),
        updated_at=local_now(),
    )

    return lead, None, score_result


def enrich_existing_lead(existing: PotentialLead, lead_dict: Dict) -> bool:
    """Enrich an existing lead with new/better data from a duplicate extraction."""
    enriched = False

    enrichment_fields = {
        "brand": lead_dict.get("brand"),
        "city": lead_dict.get("city"),
        "state": lead_dict.get("state"),
        "country": lead_dict.get("country"),
        "opening_date": lead_dict.get("opening_date"),
        "room_count": lead_dict.get("room_count"),
        "contact_name": lead_dict.get("contact_name"),
        "contact_title": lead_dict.get("contact_title"),
        "contact_email": lead_dict.get("contact_email"),
        "contact_phone": lead_dict.get("contact_phone"),
        "description": lead_dict.get("key_insights") or lead_dict.get("description"),
        "hotel_type": lead_dict.get("property_type") or lead_dict.get("hotel_type"),
    }

    for field, new_val in enrichment_fields.items():
        if not new_val:
            continue
        old_val = getattr(existing, field, None)
        if not old_val:
            setattr(existing, field, new_val)
            enriched = True
        elif field == "description" and len(str(new_val)) > len(str(old_val)):
            setattr(existing, field, new_val)
            enriched = True
        elif field == "opening_date" and len(str(new_val)) > len(str(old_val)):
            setattr(existing, field, new_val)
            existing.timeline_label = get_timeline_label(str(new_val))
            enriched = True
        elif field == "room_count" and not old_val and new_val:
            setattr(existing, field, new_val)
            enriched = True
        elif (
            field == "room_count"
            and old_val
            and new_val
            and int(new_val) > 0
            and int(old_val) == 0
        ):
            setattr(existing, field, new_val)
            enriched = True

    new_source_url = lead_dict.get("source_url")
    if new_source_url:
        existing_urls = existing.source_urls or []
        if new_source_url not in existing_urls:
            existing.source_urls = existing_urls + [new_source_url]
            enriched = True

        extractions = dict(existing.source_extractions or {})
        if new_source_url not in extractions:
            extractions[new_source_url] = {
                k: v
                for k, v in {
                    "hotel_name": lead_dict.get("hotel_name"),
                    "brand": lead_dict.get("brand"),
                    "city": lead_dict.get("city"),
                    "state": lead_dict.get("state"),
                    "country": lead_dict.get("country"),
                    "opening_date": lead_dict.get("opening_date"),
                    "room_count": lead_dict.get("room_count"),
                    "contact_name": lead_dict.get("contact_name"),
                    "contact_email": lead_dict.get("contact_email"),
                    "contact_phone": lead_dict.get("contact_phone"),
                    "key_insights": lead_dict.get("key_insights"),
                    "source_name": lead_dict.get("source_name")
                    or lead_dict.get("source_site"),
                }.items()
                if v
            }
            existing.source_extractions = extractions
            enriched = True

    if enriched:
        if existing.opening_date:
            existing.timeline_label = get_timeline_label(existing.opening_date)
        existing.updated_at = local_now()

    return enriched


async def save_lead_to_db(
    lead_dict: Dict,
    session: AsyncSession,
    commit: bool = True,
) -> Dict:
    """Full pipeline: normalize → dedup → enrich OR score → save."""
    hotel_name = (lead_dict.get("hotel_name") or "").strip()
    if not hotel_name:
        return {"status": "skipped", "id": None, "reason": "No hotel name"}

    normalized = normalize_hotel_name(hotel_name)

    # ── EXACT MATCH on potential_leads ────────────────────────────────
    result = await session.execute(
        select(PotentialLead).where(PotentialLead.hotel_name_normalized == normalized)
    )
    existing = result.scalars().first()

    if existing:
        enriched = enrich_existing_lead(existing, lead_dict)
        if enriched:
            logger.info(f"   🔄 Enriched (exact): {hotel_name}")
        if commit:
            await session.commit()
            try:
                from app.services.revenue_updater import update_lead_revenue

                await update_lead_revenue(existing.id)
            except Exception:
                pass
        return {
            "status": "enriched" if enriched else "duplicate",
            "id": existing.id,
            "reason": "Already exists (exact match)",
        }

    # ── EXACT MATCH on existing_hotels ────────────────────────────────
    # If this hotel already lives as an existing_hotel (graduated lead,
    # SAP import, manual add), we should NOT create a new potential_lead
    # for it — that creates the prospects/clients duplication this
    # whole flow is designed to prevent.
    from app.models.existing_hotel import ExistingHotel

    eh_result = await session.execute(
        select(ExistingHotel).where(ExistingHotel.hotel_name_normalized == normalized)
    )
    eh_existing = eh_result.scalars().first()
    if eh_existing:
        logger.info(
            f"   ⏭ Skipped (already in existing_hotels): {hotel_name} → EH#{eh_existing.id}"
        )
        return {
            "status": "duplicate",
            "id": None,
            "reason": f"Already exists in existing_hotels (EH#{eh_existing.id})",
        }

    # ── FUZZY MATCH ───────────────────────────────────────────────────
    new_core = _normalize_for_dedup(hotel_name)
    if new_core and len(new_core) > 3:
        from sqlalchemy import or_ as sql_or

        fuzzy_query = select(PotentialLead).where(
            PotentialLead.status.notin_(["expired", "rejected"])
        )
        location_filters = _build_location_filters(lead_dict)
        if location_filters:
            fuzzy_query = fuzzy_query.where(sql_or(*location_filters))

        candidates = (await session.execute(fuzzy_query)).scalars().all()

        for candidate in candidates:
            cand_core = _normalize_for_dedup(candidate.hotel_name or "")
            if not cand_core or len(cand_core) <= 3:
                continue

            # Strip location words from BOTH cores using locations from
            # both leads so that shared geo terms (e.g. "Miami Beach" in
            # both names) don't inflate the match.
            new_stripped = _strip_location_words(
                new_core,
                lead_dict.get("city"),
                lead_dict.get("state"),
                lead_dict.get("country"),
                candidate.city,
                candidate.state,
                candidate.country,
            )
            cand_stripped = _strip_location_words(
                cand_core,
                lead_dict.get("city"),
                lead_dict.get("state"),
                lead_dict.get("country"),
                candidate.city,
                candidate.state,
                candidate.country,
            )

            if _names_match(new_stripped, cand_stripped):
                enriched = enrich_existing_lead(candidate, lead_dict)
                if enriched:
                    logger.info(
                        f"   🔄 Fuzzy match: '{hotel_name}' → '{candidate.hotel_name}' "
                        f"(cores: '{new_stripped}' ~ '{cand_stripped}')"
                    )
                else:
                    logger.info(
                        f"   = Fuzzy duplicate: '{hotel_name}' → '{candidate.hotel_name}'"
                    )
                if commit:
                    await session.commit()
                return {
                    "status": "enriched" if enriched else "duplicate",
                    "id": candidate.id,
                    "reason": f"Fuzzy match: {candidate.hotel_name}",
                }

    # ── PREPARE NEW LEAD ──────────────────────────────────────────────
    lead, skip_reason, score_result = prepare_lead(lead_dict)

    if lead is None:
        logger.info(f"   ⏭️ Skipped: {hotel_name} - {skip_reason}")
        return {"status": "skipped", "id": None, "reason": skip_reason}

    # ── DIRECT-TO-EXISTING ROUTE ──────────────────────────────────────
    # If this lead's opening_date is already < 3 months away (or in the
    # past), it belongs in existing_hotels — not potential_leads.
    # Skip the potential_leads save and write directly to existing_hotels.
    # The transfer_lead service handles the full pipeline (scoring under
    # Option B, revenue calc, dedup against existing rows).
    from app.services.utils import get_timeline_label

    timeline = get_timeline_label(lead.opening_date or "")
    if timeline == "EXPIRED":
        try:
            from app.services.lead_transfer import (
                _build_existing_hotel_from_lead,
                _find_existing_hotel_match,
                _enrich_existing_from_lead,
            )
            from app.services.existing_hotel_scorer import apply_score_to_hotel
            from app.services.revenue_updater import update_hotel_revenue

            # Dedup check against existing_hotels — merge if match
            eh_match = await _find_existing_hotel_match(lead, session)
            if eh_match:
                _enrich_existing_from_lead(eh_match, lead)
                apply_score_to_hotel(eh_match)
                if commit:
                    await session.commit()
                    try:
                        await update_hotel_revenue(eh_match.id)
                    except Exception:
                        pass
                logger.info(
                    f"   ⇄ Direct-to-existing (merge): '{hotel_name}' → EH#{eh_match.id} "
                    f"(opening {lead.opening_date} is < 3 months — graduated immediately)"
                )
                return {
                    "status": "merged_to_existing",
                    "id": eh_match.id,
                    "reason": f"Opening < 3mo, merged into EH#{eh_match.id}",
                }

            # No match — create new existing_hotel directly
            new_eh = _build_existing_hotel_from_lead(lead)
            apply_score_to_hotel(new_eh)
            session.add(new_eh)
            if commit:
                await session.commit()
                await session.refresh(new_eh)
                try:
                    await update_hotel_revenue(new_eh.id)
                except Exception:
                    pass
            logger.info(
                f"   ✓ Direct-to-existing (new): '{hotel_name}' → EH#{new_eh.id} "
                f"(opening {lead.opening_date} is < 3 months — graduated immediately)"
            )
            return {
                "status": "saved_to_existing",
                "id": new_eh.id,
                "reason": f"Opening < 3mo, created EH#{new_eh.id}",
            }
        except Exception as e:
            # Fall through to normal potential_leads save if direct-to-existing fails
            logger.warning(
                f"Direct-to-existing failed for {hotel_name}: {e}. "
                f"Falling back to potential_leads."
            )

    # ── SAVE to potential_leads ───────────────────────────────────────
    session.add(lead)
    if commit:
        await session.commit()
        await session.refresh(lead)

    quality = (
        "🔴 HOT"
        if lead.lead_score >= SCORE_HOT_THRESHOLD
        else "🟠 WARM"
        if lead.lead_score >= SCORE_WARM_THRESHOLD
        else "🔵 COOL"
    )
    logger.info(f"   {quality} [{lead.lead_score}] {hotel_name}")

    # Auto-calculate revenue potential
    try:
        from app.services.revenue_updater import update_lead_revenue

        await update_lead_revenue(lead.id)
    except Exception as e:
        logger.warning(f"Revenue calc failed for {hotel_name}: {e}")

    # Auto geo-enrich: website discovery + geocoding
    # NOTE: PotentialLead is already imported at the top of this file.
    # Re-importing it here would shadow the module-level binding and
    # cause UnboundLocalError at the dedup check above.
    try:
        from app.services.lead_geo_enrichment import enrich_lead_geo
        from sqlalchemy import update as sql_update

        geo = await enrich_lead_geo(
            hotel_name=lead.hotel_name,
            city=lead.city,
            state=lead.state,
            country=lead.country,
            brand=lead.brand,
            existing_website=lead.hotel_website,
            address=getattr(lead, "address", None),
            zip_code=getattr(lead, "zip_code", None),
        )
        if geo.get("latitude") or geo.get("hotel_website"):
            await session.execute(
                sql_update(PotentialLead)
                .where(PotentialLead.id == lead.id)
                .values(
                    latitude=geo.get("latitude"),
                    longitude=geo.get("longitude"),
                    hotel_website=geo.get("hotel_website") or lead.hotel_website,
                    website_verified=geo.get("website_verified"),
                )
            )
            await session.commit()
            logger.info(
                f"   🌐 Geo enriched: {hotel_name} → "
                f"({geo.get('latitude'):.4f}, {geo.get('longitude'):.4f}) "
                f"website={geo.get('hotel_website', 'not found')}"
                if geo.get("latitude")
                else f"   🌐 Website found: {hotel_name} → {geo.get('hotel_website')}"
            )
    except Exception as e:
        logger.warning(f"Geo enrichment failed for {hotel_name}: {e}")

    return {"status": "saved", "id": lead.id, "reason": None}


async def save_leads_batch(
    lead_dicts: list,
    session: AsyncSession,
) -> Dict:
    """Save a batch of leads through the full pipeline."""
    saved = 0
    duplicates = 0
    enriched = 0
    skipped = 0
    errors = 0

    for lead_dict in lead_dicts:
        try:
            async with session.begin_nested():
                result = await save_lead_to_db(lead_dict, session, commit=False)

            status = result["status"]
            if status == "saved":
                saved += 1
            elif status == "duplicate":
                duplicates += 1
            elif status == "enriched":
                enriched += 1
                duplicates += 1  # Count enriched as duplicate for backward compat
            elif status == "skipped":
                skipped += 1

        except Exception as e:
            logger.error(f"   ❌ Error: {lead_dict.get('hotel_name', 'unknown')}: {e}")
            errors += 1

    await session.commit()

    logger.info(
        f"\n✅ SAVED: {saved} | Duplicates: {duplicates} | Skipped: {skipped} | Errors: {errors}"
    )
    return {
        "saved": saved,
        "duplicates": duplicates,
        "enriched": enriched,
        "skipped": skipped,
        "errors": errors,
    }
