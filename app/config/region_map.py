"""
Country → Region Mapping for Contact Enrichment
================================================
Corporate hotel executives describe their patch on LinkedIn by REGIONAL
bucket — "Latin America & Caribbean", "North America" — not by individual
country. When we search for them, adding the right regional term narrows
results to the people who actually cover that geography.

JA Uniforms serves USA + Caribbean only. This file maps just those
countries. Add more entries here when/if the sales territory expands.
"""

COUNTRY_TO_REGIONS: dict[str, list[str]] = {
    # ── USA + territories ──
    "United States": ["North America", "Americas", "USA"],
    "USA": ["North America", "Americas", "USA"],
    "US": ["North America", "Americas", "USA"],
    # ── Caribbean (English-speaking + island nations served) ──
    "Jamaica": ["Caribbean", "Latin America", "Americas"],
    "Bahamas": ["Caribbean", "Latin America", "Americas"],
    "Barbados": ["Caribbean", "Latin America", "Americas"],
    "Dominican Republic": ["Caribbean", "Latin America", "Americas"],
    "Cuba": ["Caribbean", "Latin America", "Americas"],
    "Puerto Rico": ["Caribbean", "Latin America", "Americas"],
    "Aruba": ["Caribbean", "Latin America", "Americas"],
    "Curacao": ["Caribbean", "Latin America", "Americas"],
    "Curaçao": ["Caribbean", "Latin America", "Americas"],
    "St. Lucia": ["Caribbean", "Latin America", "Americas"],
    "Saint Lucia": ["Caribbean", "Latin America", "Americas"],
    "St. Maarten": ["Caribbean", "Latin America", "Americas"],
    "Sint Maarten": ["Caribbean", "Latin America", "Americas"],
    "Trinidad and Tobago": ["Caribbean", "Latin America", "Americas"],
    "Antigua and Barbuda": ["Caribbean", "Latin America", "Americas"],
    "Grenada": ["Caribbean", "Latin America", "Americas"],
    "Cayman Islands": ["Caribbean", "Latin America", "Americas"],
    "Turks and Caicos": ["Caribbean", "Latin America", "Americas"],
    "British Virgin Islands": ["Caribbean", "Latin America", "Americas"],
    "U.S. Virgin Islands": ["Caribbean", "Latin America", "Americas"],
    "Bermuda": ["Caribbean", "Atlantic", "Americas"],
    "Haiti": ["Caribbean", "Latin America", "Americas"],
}

# Common country-name aliases. Normalize before lookup.
_COUNTRY_ALIASES = {
    "united states of america": "United States",
    "u.s.a.": "USA",
    "u.s.": "US",
    "dr": "Dominican Republic",
}


def regional_terms_for_country(country: str | None) -> list[str]:
    """
    Map a country name to the ordered list of regional terms used by
    corporate hotel executives on LinkedIn.

    Returns empty list for unknown countries (caller should handle gracefully).
    """
    if not country:
        return []

    normalized = country.strip()
    alias = _COUNTRY_ALIASES.get(normalized.lower())
    if alias:
        normalized = alias

    return COUNTRY_TO_REGIONS.get(normalized, [])


def primary_region(country: str | None) -> str | None:
    """Return just the most-specific regional term, or None if unknown."""
    terms = regional_terms_for_country(country)
    return terms[0] if terms else None
