"""Shared helpers used by all 5 outreach agents.

Most important things:
  - normalize_llm_content: handles both string and list-of-blocks
    responses from langchain-google-vertexai 3.x
  - parse_json_loose: lenient JSON parsing that survives markdown
    fences, preamble text, and trailing commentary
  - JA_BACKGROUND: the canonical company description every prompt
    references — single source of truth for what JA Uniforms is
  - SENDER_DEFAULT: fallback if no logged-in user name was provided
  - tone_for_timeline: maps URGENT/HOT/WARM/COOL → email tone guidance
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Optional

from langchain_core.messages import HumanMessage

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Canonical company background — referenced by every prompt so we have ONE
# place to update positioning if it changes.
# ─────────────────────────────────────────────────────────────────────────────
JA_BACKGROUND = """J.A. Uniforms is a Miami-based premium uniform supplier for
luxury and upscale hotels. Specializes in custom uniforms tailored to brand
standards, bulk ordering with volume discounts, fast turnaround on pre-opening
orders, individual size profiles per employee, inventory management portal,
and dedicated account manager service. Strong existing footprint in Caribbean
+ South Florida resort properties."""


SENDER_DEFAULT = "the J.A. Uniforms team"


def sender_signature(first_name: Optional[str]) -> str:
    """Build the email signature line. Uses the logged-in user's first
    name when available; otherwise a generic fallback. Company is always
    'J.A. Uniforms'."""
    name = (first_name or "").strip()
    if name:
        return f"{name}\nJ.A. Uniforms"
    return SENDER_DEFAULT


def writer_sender_id(first_name: Optional[str]) -> str:
    """First-person identifier for the email body (used when prompt says
    'You are X writing this email')."""
    name = (first_name or "").strip()
    return name if name else "a sales rep at J.A. Uniforms"


# ─────────────────────────────────────────────────────────────────────────────
# Tone mapping — drives the Writer's voice
# ─────────────────────────────────────────────────────────────────────────────


def tone_for_timeline(timeline_label: Optional[str], is_client: bool = False) -> str:
    """Return short tone descriptor the Writer should use.

    is_client overrides timeline because warm-relationship rules > urgency.
    """
    if is_client:
        return "warm-existing-client"  # already a JA customer; not cold
    label = (timeline_label or "").upper()
    if label == "URGENT":
        # 3-6 months out — uniforms typically ordered in this window
        return "urgent-but-not-pushy"
    if label == "HOT":
        # 6-12 months out, sweet spot
        return "confident-helpful"
    if label == "WARM":
        # 12-18 months out
        return "early-rapport"
    if label == "COOL":
        # 18+ months out
        return "patient-curiosity"
    return "neutral-professional"


# ─────────────────────────────────────────────────────────────────────────────
# LLM response normalization
# ─────────────────────────────────────────────────────────────────────────────


def normalize_llm_content(response_content: Any) -> str:
    """ChatVertexAI in langchain >= 1.x can return either:
      - a plain string
      - a list of dicts {"type": "text", "text": "..."}
      - a list of strings
    Normalize to a single string. Returns empty string if extraction fails."""
    if isinstance(response_content, str):
        return response_content
    if isinstance(response_content, list):
        parts = []
        for block in response_content:
            if isinstance(block, dict):
                t = block.get("text") or block.get("content") or ""
                if t:
                    parts.append(str(t))
            elif isinstance(block, str):
                parts.append(block)
        return "\n".join(parts)
    return str(response_content) if response_content is not None else ""


# ─────────────────────────────────────────────────────────────────────────────
# Lenient JSON parsing
# ─────────────────────────────────────────────────────────────────────────────


def parse_json_loose(raw: str, default: dict) -> dict:
    """Extract a JSON object from a Gemini response, tolerant of:
      - markdown ```json ... ``` fences
      - preamble or trailing commentary
      - trailing commas (some Gemini outputs include them)
      - TRUNCATED responses (max_output_tokens cutoff) — attempts to
        balance braces to recover whatever fields completed
      - missing keys (filled in from `default`)
    Returns `default` (with whatever keys it has) on any failure."""
    if not raw or not raw.strip():
        logger.warning("Outreach: LLM returned empty content")
        return dict(default)

    # Strip ``` and ```json fences anywhere in the text
    clean = re.sub(r"```(?:json|JSON)?", "", raw).strip()

    # Find the start of the JSON object
    start = clean.find("{")
    if start == -1:
        logger.warning(
            f"Outreach: no JSON object in LLM response. "
            f"Preview: {raw[:300].replace(chr(10), ' ')!r}"
        )
        return dict(default)

    # Try to find the matching closing brace, OR repair if truncated
    candidate = clean[start:]
    end = candidate.rfind("}")

    if end == -1:
        # No closing brace at all — Gemini was truncated. Try to repair
        # by closing the open structure with the right number of }/].
        repaired = _attempt_json_repair(candidate)
        if repaired:
            try:
                parsed = json.loads(repaired)
                logger.info(
                    "Outreach: recovered truncated JSON via brace-balance repair"
                )
                if not isinstance(parsed, dict):
                    return dict(default)
                out = dict(default)
                out.update({k: v for k, v in parsed.items() if v is not None})
                return out
            except json.JSONDecodeError:
                pass
        logger.warning(
            f"Outreach: JSON has no closing brace and couldn't be repaired. "
            f"Preview: {raw[:300].replace(chr(10), ' ')!r}"
        )
        return dict(default)

    candidate = candidate[: end + 1]

    # Try direct parse first
    try:
        parsed = json.loads(candidate)
    except json.JSONDecodeError:
        # Try removing trailing commas before } or ]
        repaired = re.sub(r",(\s*[}\]])", r"\1", candidate)
        try:
            parsed = json.loads(repaired)
        except json.JSONDecodeError as e:
            # Last-resort: try the brace-balance repair on the original
            repaired2 = _attempt_json_repair(clean[start:])
            if repaired2:
                try:
                    parsed = json.loads(repaired2)
                    logger.info("Outreach: recovered JSON via brace-balance repair")
                except json.JSONDecodeError:
                    logger.warning(
                        f"Outreach: JSON parse failed: {e}. "
                        f"Preview: {candidate[:300].replace(chr(10), ' ')!r}"
                    )
                    return dict(default)
            else:
                logger.warning(
                    f"Outreach: JSON parse failed: {e}. "
                    f"Preview: {candidate[:300].replace(chr(10), ' ')!r}"
                )
                return dict(default)

    if not isinstance(parsed, dict):
        return dict(default)

    # Merge with defaults to ensure all expected keys exist
    out = dict(default)
    out.update({k: v for k, v in parsed.items() if v is not None})
    return out


def _attempt_json_repair(s: str) -> str | None:
    """Last-resort brace balancer for truncated JSON. Walks the string,
    tracks the open-brace/bracket stack respecting string boundaries,
    and appends the right closing chars to make it parseable.

    Cuts the string at the last ',' or ']' or quote-ended position
    before appending closers, so we don't end up with `"foo": "barba`
    (truncated mid-string)."""
    if not s:
        return None
    stack = []
    in_string = False
    escape = False
    last_safe = 0  # last position where we could safely truncate

    for i, ch in enumerate(s):
        if escape:
            escape = False
            continue
        if ch == "\\":
            escape = True
            continue
        if ch == '"':
            in_string = not in_string
            if not in_string:
                last_safe = i + 1
            continue
        if in_string:
            continue
        if ch in "{[":
            stack.append(ch)
        elif ch in "}]":
            if stack:
                stack.pop()
            last_safe = i + 1
        elif ch == ",":
            last_safe = i  # truncate BEFORE the comma

    if not stack:
        return None  # already balanced — caller would've parsed it

    # If we ended inside a string, the truncated content is corrupt.
    # Cut back to the last safe position.
    truncated = s[:last_safe].rstrip().rstrip(",")
    # Append the right closers in reverse stack order
    closers = "".join("}" if op == "{" else "]" for op in reversed(stack))
    return truncated + closers


# ─────────────────────────────────────────────────────────────────────────────
# JSON-mode LLM invocation
# ─────────────────────────────────────────────────────────────────────────────


def invoke_json(llm, prompt: str, default: dict) -> dict:
    """Invoke the LLM with response_mime_type=application/json bound,
    parse the result tolerantly, return parsed dict (or `default`).

    JSON mode forces Gemini to return valid JSON — no markdown wrapping,
    no preamble. Eliminates the `outreach_angle: ""` empty-default bug
    that plagued v1 of this pipeline."""
    try:
        bound = llm.bind(response_mime_type="application/json")
    except Exception:
        # Fall back to plain invoke — parser will still try its best
        bound = llm

    response = bound.invoke([HumanMessage(content=prompt)])
    raw = normalize_llm_content(response.content)
    return parse_json_loose(raw, default)


def invoke_text(llm, prompt: str) -> str:
    """Invoke the LLM, return the response as a plain string. Used by
    the LinkedIn message generator and other free-form outputs."""
    response = llm.invoke([HumanMessage(content=prompt)])
    return normalize_llm_content(response.content).strip()


# ─────────────────────────────────────────────────────────────────────────────
# Format helpers for prompts
# ─────────────────────────────────────────────────────────────────────────────


def fmt_list(items: Optional[list], fallback: str = "Not identified") -> str:
    """Render a list as bullet lines, or a fallback if empty."""
    if not items:
        return fallback
    return "\n".join(f"- {x}" for x in items if x)


def fmt_known_context(state: dict) -> str:
    """Render the SLH-known context block that goes into every agent's
    prompt. Skips fields that are missing/empty so the prompt isn't
    cluttered with 'unknown / unknown / unknown' lines."""
    rows = []
    if state.get("brand"):
        rows.append(f"Brand: {state['brand']}")
    tier = state.get("brand_tier")
    if tier:
        tier_label = {
            "tier1_ultra_luxury": "Ultra Luxury (top tier)",
            "tier2_luxury": "Luxury",
            "tier3_upper_upscale": "Upper Upscale",
            "tier4_upscale": "Upscale",
        }.get(tier, tier)
        rows.append(f"Brand Tier: {tier_label}")
    if state.get("project_type"):
        pt_label = {
            "new_opening": "New Build (pre-opening)",
            "renovation": "Renovation in progress",
            "rebrand": "Brand conversion (new flag)",
            "reopening": "Reopening (was closed)",
            "conversion": "Independent → branded conversion",
            "ownership_change": "Recent ownership change",
        }.get(state["project_type"], state["project_type"])
        rows.append(f"Project Type: {pt_label}")
    if state.get("timeline_label"):
        tl_label = {
            "URGENT": "URGENT — opens in 3-6 months (tight uniform window)",
            "HOT": "HOT — opens in 6-12 months (sweet spot for outreach)",
            "WARM": "WARM — opens in 12-18 months (planning phase)",
            "COOL": "COOL — opens in 18+ months (long-lead)",
            "EXPIRED": "Already operating",
            "TBD": "Timing unclear",
        }.get(state["timeline_label"], state["timeline_label"])
        rows.append(f"Timeline: {tl_label}")
    if state.get("opening_date"):
        rows.append(f"Opening Date: {state['opening_date']}")
    if state.get("room_count"):
        rows.append(f"Rooms: {state['room_count']}")
    if state.get("hotel_type"):
        rows.append(f"Type: {state['hotel_type']}")
    if state.get("zone"):
        rows.append(f"Region: {state['zone']}")
    if state.get("is_client"):
        rows.append("⚠️  EXISTING J.A. UNIFORMS CLIENT — this is NOT a cold opener")
    if state.get("contact_priority"):
        rows.append(f"Contact Priority Tier: {state['contact_priority']} (P1=top)")
    if state.get("slh_lead_score"):
        rows.append(f"SLH Account Score: {state['slh_lead_score']}/100 (account fit)")
    if not rows:
        return "(No additional context available)"
    return "\n".join(rows)
