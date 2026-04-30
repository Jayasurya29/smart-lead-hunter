"""Agent 4 — Critic (v2).

What changed from v1:
  - 6 criteria each scored 1-5 (structured rubric) instead of yes/no
  - Each criterion has SPECIFIC fix guidance returned to Writer
  - Approval threshold: avg >= 3.8 AND no individual score < 3
  - Writes a structured `previous_feedback` field that Writer reads
    on retry — Writer knows EXACTLY what to fix vs vague "needs work"
  - Critiques BOTH email and LinkedIn (v1 only blocked email)
  - JSON mode — no fragile text parsing
"""

from __future__ import annotations

import logging

from .state import PitchState
from .config import get_llm_lite
from ._helpers import invoke_json

logger = logging.getLogger(__name__)


_RUBRIC_CRITERIA = [
    "addresses_by_first_name",  # uses first name only, not "Mr. X"
    "human_not_template",  # sounds like a real person
    "specific_personalization",  # references a specific fact (not generic)
    "right_length",  # 70-110 words for body
    "single_clear_cta",  # exactly ONE ask
    "no_buzzwords",  # no "synergy", "leverage", etc.
]

_DEFAULT_CRITIQUE = {
    "scores": {c: 3 for c in _RUBRIC_CRITERIA},
    "approved": False,
    "summary_feedback": "",
    "fix_instructions": "",
    "linkedin_quality": "ok",
}


def critic_agent(state: PitchState) -> PitchState:
    contact_name = state.get("contact_name", "")
    subject = state.get("email_subject", "") or ""
    body = state.get("email_body", "") or ""
    linkedin = state.get("linkedin_message", "") or ""
    rewrite_count = state.get("rewrite_count") or 0

    logger.info(
        f"[Critic] Reviewing draft for {contact_name} (rewrite #{rewrite_count})"
    )

    # Skip review if Writer skipped (low fit_score)
    if not body:
        logger.info("[Critic] No email to critique (Writer skipped)")
        return {
            **state,
            "quality_approved": False,
            "quality_feedback": "Skipped: no email body",
            "critic_rubric": {},
        }

    first_name = (contact_name.split()[0] if contact_name else "").strip()

    prompt = f"""You are a senior B2B sales coach reviewing a cold outreach
email + LinkedIn message draft. Be strict but fair — the goal is a
response, not a perfect form letter.

═══ EMAIL ═══
Subject: {subject}

Body:
{body}

═══ LINKEDIN MESSAGE ═══
{linkedin}

═══ RUBRIC ═══
Score each criterion 1-5 (5 = excellent, 3 = acceptable, 1 = broken).

1. addresses_by_first_name: Email uses "{first_name or contact_name}" by
   first name only? (5 = yes, properly. 1 = uses formal/last name only)
2. human_not_template: Sounds like a real human wrote it for THIS person?
   (5 = clearly. 1 = obvious template)
3. specific_personalization: References a SPECIFIC fact about the hotel
   or contact, not generic? (5 = clearly verifiable specific fact.
   1 = "I noticed your hotel offers great service")
4. right_length: Body is 70-110 words? (5 = within range. 3 = within
   30% of range. 1 = way off)
5. single_clear_cta: Exactly ONE ask, clearly worded? (5 = yes.
   3 = one but vague. 1 = multiple asks or no CTA)
6. no_buzzwords: Free of buzzwords (synergy, leverage, revolutionary,
   robust, best-in-class)? (5 = clean. 1 = full of them)

For EACH criterion that scored < 4, give a SPECIFIC fix instruction.
Don't say "improve personalization" — say "swap the generic 'great
service' line in the second paragraph for a reference to their recent
[X] award/news/expansion".

═══ APPROVAL ═══
APPROVED = avg score >= 3.8 AND no individual score < 3

═══ RESPONSE FORMAT (JSON, no markdown) ═══
{{
  "scores": {{
    "addresses_by_first_name": <1-5>,
    "human_not_template": <1-5>,
    "specific_personalization": <1-5>,
    "right_length": <1-5>,
    "single_clear_cta": <1-5>,
    "no_buzzwords": <1-5>
  }},
  "approved": <true|false>,
  "summary_feedback": "1-sentence overall verdict — what's the BIGGEST issue if any",
  "fix_instructions": "specific, actionable rewrite guidance — what exactly to change. If approved, leave empty.",
  "linkedin_quality": "good | ok | weak — independent assessment of the LinkedIn note"
}}
"""

    critique = invoke_json(get_llm_lite(), prompt, _DEFAULT_CRITIQUE)

    # Validate scores — defensive coercion
    scores = critique.get("scores") or {}
    safe_scores = {}
    for crit in _RUBRIC_CRITERIA:
        try:
            v = int(scores.get(crit, 3))
        except (TypeError, ValueError):
            v = 3
        safe_scores[crit] = max(1, min(5, v))

    # Compute approval ourselves so the LLM can't lie about it
    avg = sum(safe_scores.values()) / len(safe_scores)
    min_score = min(safe_scores.values())
    approved = avg >= 3.8 and min_score >= 3

    # Build feedback string the Writer will read on retry
    fix_instructions = critique.get("fix_instructions") or ""
    summary = critique.get("summary_feedback") or ""

    if approved:
        logger.info(
            f"[Critic] APPROVED (avg={avg:.1f}, min={min_score}). LinkedIn: "
            f"{critique.get('linkedin_quality')}"
        )
        feedback_for_writer = ""
    else:
        # Identify which criteria failed for targeted feedback
        weak_criteria = [
            f"  - {c.replace('_', ' ')}: scored {s}/5"
            for c, s in safe_scores.items()
            if s < 4
        ]
        feedback_for_writer = (
            f"Previous draft failed quality review (avg {avg:.1f}/5).\n"
            f"Weak areas:\n" + "\n".join(weak_criteria) + "\n\n"
            f"Specific fixes needed:\n{fix_instructions}"
        )
        logger.info(
            f"[Critic] REJECTED (avg={avg:.1f}, min={min_score}). "
            f"Weak: {[c for c, s in safe_scores.items() if s < 4]}"
        )

    return {
        **state,
        "quality_approved": approved,
        "quality_feedback": summary,
        "critic_rubric": {
            "scores": safe_scores,
            "average": round(avg, 2),
            "min": min_score,
            "linkedin_quality": critique.get("linkedin_quality") or "unknown",
        },
        "previous_feedback": feedback_for_writer,
    }
