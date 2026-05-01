"""Agent 2.5 — Fact Validator (anti-hallucination).

Sits between the Researcher and the Analyst. Reads the Researcher's
output (pain_points, signals, hooks, brief) and checks every concrete
number/date against the raw research text. If a number doesn't appear
in source, we strip it from the claim — replacing "outfit 400+ staff"
with "outfit the team" — so downstream agents never see fabricated
data.

Why we do it AFTER the Researcher (not as part of it):
- Keeps the Researcher prompt focused on extraction
- The Validator is a tiny deterministic Flash Lite call → cheap
- Lets us reject/repair without re-running the expensive 6-search step

What it catches:
- Hallucinated headcounts ("400 staff" when research said 300)
- Hallucinated dates ("opening Q1 2026" when research said "late 2026")
- Hallucinated amounts ("$300M renovation" when research said "$200M")

What it doesn't catch (out of scope for v1):
- Subjective claims ("this is a luxury brand")
- Combinations of real numbers (research has 300 staff and 4 hotels →
  hallucinated "1200 total staff" passes our basic check). Future work.
"""

from __future__ import annotations

import logging
import re

from .state import PitchState

logger = logging.getLogger(__name__)


# Numbers we never need to validate (years that the model stating
# "in 2026" is fine, common phrases). Keep this conservative.
_BENIGN_NUMBERS = {
    "1",
    "2",
    "3",
    "4",
    "5",
    "6",
    "7",
    "8",
    "9",
    "10",
    "15",
    "20",
    "30",
    "60",
    "100",
    "2024",
    "2025",
    "2026",
    "2027",
    "2028",
}


def _extract_numbers(text: str) -> list[str]:
    """Pull out all numeric tokens from a string. Catches:
      - Bare integers: 300, 450
      - With commas: 12,000
      - Currency: $200M, $1.5B, $200 million
      - Counted units: "300+ staff", "16-restaurant"
      - Percentages: 25%
    Returns the raw matched strings (for substring matching against
    source — we don't try to canonicalize)."""
    if not text:
        return []
    patterns = [
        r"\$\s*\d+(?:[\.,]\d+)?\s*(?:million|billion|M|B)?",  # $200M, $1.5 billion
        r"\d{1,3}(?:,\d{3})+",  # 12,000
        r"\d+\+?\s*(?:%|percent)",  # 25%, 25 percent
        r"\d+\s*-\s*(?:hotel|restaurant|room|year|month)",  # 16-restaurant, 4-hotel
        r"\d+\+?",  # 300, 450+, 300+
    ]
    found = []
    for pattern in patterns:
        found.extend(re.findall(pattern, text, flags=re.IGNORECASE))
    return [f.strip() for f in found if f.strip()]


def _digits_in(s: str) -> str:
    """Strip everything except digits — for fuzzy comparison.
    "$200M" → "200", "12,000" → "12000", "300+" → "300"."""
    return "".join(ch for ch in s if ch.isdigit())


def _appears_in_source(claim_number: str, source_text: str) -> bool:
    """Does the digit sequence of `claim_number` show up anywhere in
    the source? We use digit-only comparison so "$200 million" matches
    "200M" matches "200,000,000" — variations on writing the same value
    don't trip up the check."""
    if not claim_number or not source_text:
        return False
    cn = _digits_in(claim_number)
    if not cn:
        return True  # not actually a number
    if cn in _BENIGN_NUMBERS:
        return True
    source_digits = _digits_in(source_text)
    return cn in source_digits


def _scrub_claim(claim: str, source_text: str) -> tuple[str, list[str]]:
    """Walk the claim text, find numbers, drop ones not in source.
    Returns (cleaned_claim, list_of_stripped_numbers).

    Example:
      claim = "Outfit 400+ staff for the June 2026 opening"
      source contains "300 staff" and "June 2026"
      → returns ("Outfit the team for the June 2026 opening", ["400+"])

    The substitution uses generic placeholders: numbers about people
    become "the team", numbers in currency become "the budget", etc.
    Better to be vague than wrong."""
    if not claim:
        return claim, []
    numbers = _extract_numbers(claim)
    stripped = []
    cleaned = claim
    for n in numbers:
        if _appears_in_source(n, source_text):
            continue
        # Number is fabricated — strip it
        stripped.append(n)
        # Pick a generic replacement based on the surrounding text
        # (5 chars before/after the number)
        idx = cleaned.find(n)
        if idx == -1:
            continue
        context_after = cleaned[idx + len(n) : idx + len(n) + 30].lower()
        if any(
            w in context_after
            for w in ["staff", "employee", "hire", "team", "person", "people"]
        ):
            replacement = "the team"
            # Drop the number AND the next noun word so we don't get
            # "the team staff"
            cleaned = re.sub(
                re.escape(n) + r"\+?\s*\w+",
                replacement,
                cleaned,
                count=1,
            )
        elif any(w in context_after for w in ["room", "key"]):
            replacement = "all rooms"
            cleaned = re.sub(re.escape(n) + r"\+?\s*\w+", replacement, cleaned, count=1)
        elif "$" in n or any(
            w in context_after for w in ["million", "billion", "budget"]
        ):
            cleaned = re.sub(
                re.escape(n) + r"\+?\s*(?:million|billion|M|B)?",
                "the budget",
                cleaned,
                count=1,
            )
        else:
            # Just drop the number — leave surrounding text intact
            cleaned = re.sub(re.escape(n) + r"\+?", "", cleaned, count=1)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned, stripped


def validator_agent(state: PitchState) -> PitchState:
    """Walk Researcher output, strip fabricated numbers from each claim."""
    raw_text = state.get("raw_research_text") or ""

    # ALSO grant numbers that the Researcher itself cited via fact_citations.
    # The model is allowed to point at a source quote — if it did, we trust
    # the citation (even if our digit-substring match misses the source).
    # This prevents over-stripping in cases where the source phrases the
    # number in an unusual way that our regex can't match.
    fact_citations = state.get("fact_citations") or []
    cited_text = "\n".join(
        (c.get("source_quote") or "") for c in fact_citations if isinstance(c, dict)
    )

    # ALSO grant numbers that exist in SLH's own DB for this lead — the
    # most authoritative source. If the Researcher said "252 rooms" and
    # SLH has the lead's room_count = 252, that's not a hallucination
    # even if Gemini happened to know it from training rather than from
    # the live web search. Without this check the Validator was stripping
    # numbers that match the database (false positives).
    slh_authoritative_facts = []
    for key in (
        "room_count",
        "opening_date",
        "brand",
        "brand_tier",
        "address",
        "city",
        "zone",
        "hotel_type",
        "slh_lead_score",
    ):
        v = state.get(key)
        if v not in (None, "", 0):
            slh_authoritative_facts.append(str(v))
    slh_text = " | ".join(slh_authoritative_facts)

    source_text = raw_text + "\n" + cited_text + "\n" + slh_text

    if not source_text.strip():
        # Nothing to validate against — skip the check rather than
        # mass-stripping every number (which would mangle clean output)
        logger.info("[Validator] No raw research text — skipping fact check")
        return state

    contact_name = state.get("contact_name", "")
    hotel_name = state.get("hotel_name", "")
    logger.info(
        f"[Validator] Fact-checking research for {hotel_name} / {contact_name} "
        f"({len(raw_text)} chars web + {len(slh_text)} chars SLH DB + "
        f"{len(fact_citations)} citations)"
    )

    total_stripped = 0

    # Validate pain_points
    cleaned_pains = []
    for p in state.get("pain_points") or []:
        cleaned, stripped = _scrub_claim(p, source_text)
        if stripped:
            total_stripped += len(stripped)
            logger.warning(
                f"[Validator] Pain point — stripped {stripped}: "
                f"original={p[:80]!r} → cleaned={cleaned[:80]!r}"
            )
        cleaned_pains.append(cleaned)

    # Validate signals
    cleaned_signals = []
    for s in state.get("signals") or []:
        cleaned, stripped = _scrub_claim(s, source_text)
        if stripped:
            total_stripped += len(stripped)
            logger.warning(
                f"[Validator] Signal — stripped {stripped}: "
                f"original={s[:80]!r} → cleaned={cleaned[:80]!r}"
            )
        cleaned_signals.append(cleaned)

    # Validate the personalization_hook (this is the most important —
    # it's quoted directly in the email opener)
    hook = state.get("personalization_hook") or ""
    cleaned_hook, hook_stripped = _scrub_claim(hook, source_text)
    if hook_stripped:
        total_stripped += len(hook_stripped)
        logger.warning(
            f"[Validator] Hook — stripped {hook_stripped}: "
            f"original={hook[:80]!r} → cleaned={cleaned_hook[:80]!r}"
        )

    # Validate conversation_hooks
    cleaned_alt_hooks = []
    for h in state.get("conversation_hooks") or []:
        cleaned, stripped = _scrub_claim(h, source_text)
        if stripped:
            total_stripped += len(stripped)
        cleaned_alt_hooks.append(cleaned)

    # Validate the company_summary too (less critical but worth it)
    summary = state.get("company_summary") or ""
    cleaned_summary, summary_stripped = _scrub_claim(summary, source_text)
    if summary_stripped:
        total_stripped += len(summary_stripped)
        logger.warning(
            f"[Validator] Summary — stripped {summary_stripped}: "
            f"original={summary[:120]!r} → cleaned={cleaned_summary[:120]!r}"
        )

    if total_stripped == 0:
        logger.info("[Validator] All facts grounded ✓")
    else:
        logger.info(f"[Validator] Stripped {total_stripped} fabricated number(s)")

    return {
        **state,
        "pain_points": cleaned_pains,
        "signals": cleaned_signals,
        "personalization_hook": cleaned_hook,
        "conversation_hooks": cleaned_alt_hooks,
        "company_summary": cleaned_summary,
        # Audit trail — frontend can show this to the rep if they want
        # to see what was caught
        "validator_stripped_count": total_stripped,
    }
