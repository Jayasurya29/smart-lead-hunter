"""Agent 3 — Writer (v2).

What changed from v1:
  - Email + LinkedIn message generated in ONE prompt for tonal
    consistency (v1 was 2 separate calls that sometimes drifted)
  - Tone is selected from timeline_label + is_client (urgent vs warm
    vs cool — the same template would feel WRONG sent to a contact
    whose hotel opens in 2027 vs one opening in 4 months)
  - Reads `previous_feedback` from Critic on retry — Writer actually
    knows what to fix on the second attempt instead of regenerating
    blindly with the same prompt
  - Sender first name comes from state['sender_first_name'] (logged-in
    user) — no hardcoded "Jay"
  - Existing client (is_client=true) → completely different opener
    structure (skips cold-email "I noticed..." pattern)
  - JSON mode — single subject + body + linkedin in one structured
    response, no fragile "SUBJECT:" / "BODY:" string parsing
"""

from __future__ import annotations

import logging

from .state import PitchState
from .config import get_writer_llm
from ._helpers import (
    JA_BACKGROUND,
    fmt_list,
    fmt_known_context,
    invoke_json,
    writer_sender_id,
    tone_for_timeline,
)

logger = logging.getLogger(__name__)


# Tone guidance map — full instructions per tone variant
_TONE_GUIDANCE = {
    "urgent-but-not-pushy": (
        "URGENT: opening is 3-6 months away. Reference the timing pressure "
        "lightly (uniforms typically need a 3-4 month production lead time) "
        "without being aggressive. CTA should be specific — '15 min next "
        "Tuesday or Thursday' beats 'are you available'."
    ),
    "confident-helpful": (
        "HOT: opening 6-12 months out — sweet spot. Lead with the "
        "personalization hook, then a clear value prop, then a soft CTA. "
        "Don't oversell — sound like a peer, not a vendor."
    ),
    "early-rapport": (
        "WARM: opening 12-18 months out. The goal is RAPPORT not closing. "
        "Acknowledge it's early. CTA = 'love to stay in touch' or 'happy "
        "to share what's worked for similar properties when you're ready'."
    ),
    "patient-curiosity": (
        "COOL: 18+ months out. Even softer. CTA = 'no rush, but happy to "
        "share notes' — DO NOT pitch a meeting yet."
    ),
    "warm-existing-client": (
        "EXISTING CLIENT: skip the cold-email 'I noticed' opener entirely. "
        "Tone is collegial, like a quick email between colleagues. Reference "
        "their continued partnership. CTA is about a SPECIFIC expansion "
        "(new property, new department, reorder) tied to recent news, "
        "not a discovery call."
    ),
    "neutral-professional": (
        "Standard professional tone. Lead with the personalization hook, "
        "one clear value prop, soft CTA."
    ),
}


_DEFAULT_OUTPUT = {
    "email_subject": "",
    "email_body": "",
    "linkedin_message": "",
    "tone": "",
}


# Closings the Writer might generate despite our prompt instruction not to
_KNOWN_CLOSINGS = {
    "best",
    "best,",
    "best regards",
    "best regards,",
    "regards",
    "regards,",
    "cheers",
    "cheers,",
    "thanks",
    "thanks,",
    "thank you",
    "thank you,",
    "sincerely",
    "sincerely,",
    "warm regards",
    "warm regards,",
    "kind regards",
    "kind regards,",
}


def _strip_signature(body: str) -> str:
    """Trim closing + name + company lines from the bottom of the email,
    if Gemini included them despite the prompt instruction not to.
    Outlook auto-appends the rep's real signature on send, so leaving
    our generated one would create a duplicate.

    We walk lines bottom-up looking for a known closing word OR a
    standalone "J.A. Uniforms" / company line. When found, we cut
    everything from that line onward."""
    if not body:
        return ""
    body = body.rstrip()
    lines = body.split("\n")

    # Pass 1: closing word found → cut from there
    for i in range(len(lines) - 1, -1, -1):
        normalized = lines[i].strip().lower()
        if normalized in _KNOWN_CLOSINGS:
            return "\n".join(lines[:i]).rstrip()

    # Pass 2: trailing "J.A. Uniforms" line (no closing above it)
    # Walk back from the end, drop trailing lines that are just the
    # company name. Stops when we hit a normal content line.
    last_kept = len(lines)
    for i in range(len(lines) - 1, -1, -1):
        normalized = lines[i].strip().lower().replace(".", "")
        if not normalized:
            last_kept = i
            continue
        if normalized in {"ja uniforms", "j a uniforms", "jauniforms"}:
            last_kept = i
            continue
        break
    if last_kept < len(lines):
        return "\n".join(lines[:last_kept]).rstrip()

    return body


def writer_agent(state: PitchState) -> PitchState:
    contact_name = state.get("contact_name", "")
    contact_title = state.get("contact_title", "")
    hotel_name = state.get("hotel_name", "")
    fit_score = state.get("fit_score") or 0

    logger.info(
        f"[Writer] Drafting outreach for {contact_name} at {hotel_name} (fit={fit_score})"
    )

    # If fit is too low, skip — don't waste tokens on a doomed outreach
    if fit_score < 30:
        logger.info(f"[Writer] Skipping (fit_score {fit_score} < 30 threshold)")
        return {
            **state,
            "email_subject": None,
            "email_body": None,
            "linkedin_message": None,
            "tone": "skipped-low-fit",
            "rewrite_count": (state.get("rewrite_count") or 0) + 1,
        }

    is_client = bool(state.get("is_client"))
    tone_key = tone_for_timeline(state.get("timeline_label"), is_client=is_client)
    tone_instruction = _TONE_GUIDANCE.get(
        tone_key, _TONE_GUIDANCE["neutral-professional"]
    )

    sender_first = state.get("sender_first_name") or ""
    sender_id = writer_sender_id(sender_first)
    first_name = (contact_name.split()[0] if contact_name else "").strip()

    # Critic-feedback-aware rewrite
    previous_feedback = state.get("previous_feedback") or ""
    rewrite_count = state.get("rewrite_count") or 0
    feedback_block = ""
    if previous_feedback and rewrite_count > 0:
        feedback_block = f"""
═══ PREVIOUS DRAFT FEEDBACK (you must address this) ═══
{previous_feedback}

Rewrite the email AND LinkedIn message addressing the specific issues above.
Do not just tweak — make the change the feedback requested.
"""

    pain_str = fmt_list(state.get("pain_points"), "(no specific pain points found)")
    value_str = fmt_list(state.get("value_props"), "(no value props from analyst)")
    news_first = (state.get("recent_news") or [None])[0]
    hooks = state.get("conversation_hooks") or []
    hooks_str = fmt_list(hooks[:3], "(none)")

    prompt = f"""You are {sender_id} at J.A. Uniforms, writing a personalized
outreach to a hotel decision-maker.

{JA_BACKGROUND}

═══ TONE FOR THIS EMAIL ═══
{tone_instruction}

═══ CONTACT + HOTEL ═══
{contact_name} — {contact_title} at {hotel_name}
{fmt_known_context(dict(state))}

About them: {state.get('contact_summary') or '(no specific contact intel)'}

═══ RESEARCH BRIEF ═══
Hotel summary: {state.get('company_summary') or '(none)'}
Outreach angle: {state.get('outreach_angle') or '(none)'}
Personalization hook (PRIMARY — try this first): {state.get('personalization_hook') or '(none)'}
Recent news lead: {news_first or '(none)'}

Alternative conversation hooks (use one if primary feels off):
{hooks_str}

Pain points to consider:
{pain_str}

Value props from Analyst (use 1, max 2 — don't list them all):
{value_str}
{feedback_block}

═══ EMAIL RULES ═══
1. Address {first_name or contact_name} by FIRST NAME only
2. 70-110 words MAX in the body — short is better than long
3. Reference ONE specific fact from the personalization_hook or
   conversation_hooks — proves you did real homework
4. ONE clear CTA — no double asks
5. NO buzzwords (no "synergy", "leverage", "revolutionary", "robust",
   "best-in-class", "industry-leading")
6. NO em-dashes — use commas or periods instead
7. Sound like a real person who did 5 minutes of research, NOT a template
8. End with the CTA. DO NOT include a closing ("Best,", "Regards,",
   "Thanks,") OR a name signature OR a company name. Outlook auto-appends
   the rep's full signature (logo, contact info, certifications) when
   the email opens — adding our own would create a duplicate signature.
   The body stops at the question mark of the CTA.
9. ANTI-HALLUCINATION: do NOT include any number (headcount, room count,
   budget, date, percentage) that doesn't appear in the research brief
   above. If the brief says "300 staff", you can write "300 staff". If
   it doesn't mention a number, write "the team" or "your staff" — never
   invent. The recipient can fact-check the email; a wrong number kills
   credibility instantly.

═══ LINKEDIN RULES ═══
1. Address as "{first_name or contact_name}" only
2. Maximum 280 characters (LinkedIn note limit, with margin)
3. Reference ONE specific thing about the hotel or their role
4. NO formal language ("pleased", "esteemed", "appreciate")
5. NO sales pitch — just a natural connection request
6. NO sign-off — LinkedIn DMs don't have one
7. Different from the email opener — show variety

═══ RESPONSE FORMAT (JSON, no markdown) ═══
{{
  "email_subject": "the subject line — max 60 chars, conversational, NOT clickbait",
  "email_body": "the email body following ALL rules above. Ends at the CTA. NO closing/signature.",
  "linkedin_message": "the LinkedIn note following ALL LinkedIn rules above",
  "tone": "{tone_key}"
}}
"""

    output = invoke_json(get_writer_llm(), prompt, _DEFAULT_OUTPUT)

    # Defensive cleanup: if Gemini ignored the no-signature rule and
    # included a closing/signature anyway, strip it. Outlook adds the
    # real one on send.
    body = _strip_signature(output.get("email_body") or "")

    logger.info(
        f"[Writer] Done — subject: {(output.get('email_subject') or '')[:60]!r}, "
        f"tone: {tone_key}"
    )

    return {
        **state,
        "email_subject": output.get("email_subject") or "",
        "email_body": body,
        "linkedin_message": (output.get("linkedin_message") or "").strip(),
        "tone": output.get("tone") or tone_key,
        "rewrite_count": rewrite_count + 1,
    }
