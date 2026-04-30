"""Agent 2 — Analyst (v2).

What changed from v1:
  - Anchors fit_score to SLH's existing lead_score (which is account-fit
    based on brand_tier + zone + room_count + hotel_type) instead of
    asking Gemini to re-derive a 100-pt score from scratch
  - Adjusts that anchor by Researcher findings (positive: hiring spike,
    renovation completion, etc.; negative: bankruptcy, budget cuts)
  - JSON mode — no more brittle "FIT_SCORE: 87" string parsing
  - Returns 5 value props in 3 categories (emotional, operational,
    tactical) — Writer picks the best 1-2 to lead with
  - Returns fit_breakdown so the rep can see WHY this scored 87 vs 65
"""

from __future__ import annotations

import logging

from .state import PitchState
from .config import get_llm
from ._helpers import (
    JA_BACKGROUND,
    fmt_known_context,
    fmt_list,
    invoke_json,
)

logger = logging.getLogger(__name__)


_DEFAULT_ANALYSIS = {
    "fit_score": 50,
    "fit_breakdown": {
        "base_account_score": 50,
        "research_adjustment": 0,
        "rationale": "",
    },
    "primary_angle": "",
    "value_props": {
        "emotional": [],
        "operational": [],
        "tactical": [],
    },
}


def _flatten_value_props(props_dict: dict) -> list[str]:
    """Convert the 3-category value_props dict into a flat ordered list,
    interleaving emotional/operational/tactical so the Writer sees variety
    even if it only takes the first 3."""
    if not isinstance(props_dict, dict):
        return list(props_dict) if isinstance(props_dict, list) else []

    emo = props_dict.get("emotional") or []
    ops = props_dict.get("operational") or []
    tac = props_dict.get("tactical") or []

    flat: list[str] = []
    for trio in zip(emo, ops, tac):
        for v in trio:
            if v:
                flat.append(v)
    # Append leftovers
    for category in (emo, ops, tac):
        for v in category[len(flat) // 3 :]:
            if v and v not in flat:
                flat.append(v)
    return flat[:6]  # cap at 6


def analyst_agent(state: PitchState) -> PitchState:
    hotel_name = state.get("hotel_name", "")
    contact_name = state.get("contact_name", "")

    logger.info(f"[Analyst] Scoring fit for {hotel_name} / {contact_name}")

    pain_str = fmt_list(state.get("pain_points"), "Not identified")
    signal_str = fmt_list(state.get("signals"), "Not identified")
    hiring_str = fmt_list(state.get("hiring_signals"), "No hiring signals found")
    awards_str = fmt_list(state.get("awards"), "No awards found")
    news_str = fmt_list(state.get("recent_news"), "No notable recent news")

    base_score = state.get("slh_lead_score") or 50
    is_client = bool(state.get("is_client"))

    # Fast-path: existing client → high fit by default, focus on retention/expansion
    cold_or_warm = (
        "WARM (existing JA Uniforms client)" if is_client else "COLD (prospect)"
    )

    prompt = f"""You are a senior B2B sales analyst for J.A. Uniforms.

{JA_BACKGROUND}

═══ JOB ═══
Score this lead's OUTREACH FIT (0-100) and produce 5 specific value
propositions. The score should reflect how likely a thoughtful outreach
email gets a meaningful response — NOT just account size or revenue.

═══ ALREADY-SCORED ACCOUNT FIT ═══
SLH's account-fit score for this hotel: {base_score}/100
(based on brand tier, zone, room count, hotel type — already weighed)

You will ANCHOR your fit_score to {base_score} and ADJUST up/down by
research findings:
  +5 to +15:  active hiring spike, renovation nearing completion,
              recent award (uniform-relevant), pre-opening within 3-12mo,
              new GM/EAM hire
  -5 to -15:  bankruptcy/distress signals, recently sold to budget chain,
              no detectable pre-opening activity, contact has clearly
              left the role

═══ HOTEL CONTEXT ═══
Hotel: {hotel_name}
{fmt_known_context(dict(state))}

Status: {cold_or_warm}

═══ RESEARCH FINDINGS ═══
Hotel summary: {state.get('company_summary', '(none)')}
Outreach angle from research: {state.get('outreach_angle', '(none)')}
Personalization hook: {state.get('personalization_hook', '(none)')}

Pain Points:
{pain_str}

Buying Signals:
{signal_str}

Hiring Signals:
{hiring_str}

Awards / Recent News:
{awards_str}
{news_str}

═══ CONTACT ═══
{contact_name} — {state.get('contact_title', '')}
{state.get('contact_summary', '(none)')}

═══ J.A. UNIFORMS — WHAT YOU CAN OFFER ═══
- Custom uniforms tailored to brand standards
- Bulk ordering with volume discounts
- Fast turnaround on pre-opening orders (industry-leading)
- Inventory management portal
- Individual size profiles per employee
- Dedicated account manager

═══ RESPONSE FORMAT (JSON, no markdown) ═══
{{
  "fit_score": <integer 0-100>,
  "fit_breakdown": {{
    "base_account_score": {base_score},
    "research_adjustment": <integer, can be negative>,
    "rationale": "1-2 sentences explaining what drove the adjustment"
  }},
  "primary_angle": "the single best value-prop angle to lead the email with",
  "value_props": {{
    "emotional": [
      "value prop tied to brand pride / status / guest experience perception",
      "..."
    ],
    "operational": [
      "value prop tied to time/cost/effort saved (specific number if possible)",
      "..."
    ],
    "tactical": [
      "value prop tied to a specific JA feature solving a specific found pain",
      "..."
    ]
  }}
}}

Hard rules:
- fit_score MUST equal base_account_score + research_adjustment, no surprises.
- value_props arrays should each have 1-2 props (so 3-6 total across all categories).
- Every value prop must be SPECIFIC to this hotel's situation. No "we offer
  premium quality" — instead "Custom uniform program tailored to {state.get('brand') or 'your'} brand
  standards across {state.get('room_count') or 'your'}-room property."
- If is_client=true, value props should focus on EXPANSION / additional
  departments / consolidating reorders, not first-time acquisition.
"""

    analysis = invoke_json(get_llm(), prompt, _DEFAULT_ANALYSIS)

    # Validate / clamp fit_score
    try:
        score = int(analysis.get("fit_score") or base_score)
    except (TypeError, ValueError):
        score = base_score
    score = max(0, min(100, score))

    # Flatten the 3-category props to a list for downstream agents
    props_dict = analysis.get("value_props") or {}
    flat_props = _flatten_value_props(props_dict)

    logger.info(
        f"[Analyst] fit={score} (base={base_score} + adj="
        f"{analysis.get('fit_breakdown', {}).get('research_adjustment', 0)})"
    )

    return {
        **state,
        "fit_score": score,
        "fit_breakdown": analysis.get("fit_breakdown") or {},
        "primary_angle": analysis.get("primary_angle") or "",
        "value_props": flat_props,
    }
