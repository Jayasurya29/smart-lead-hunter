"""Shared LangGraph state for the outreach pipeline (v2).

Extended over the original PitchIQ schema to include:
  - already-known SLH context (brand_tier, project_type, timeline_label,
    is_client, room_count, etc.) — agents don't re-derive these
  - sender_first_name (current logged-in user, never hardcoded)
  - structured hotel_intel + contact_intel (richer researcher payload)
  - critic_rubric (per-criterion 1-5 with targeted feedback)
  - conversation_hooks (3 alternates, not just one)
  - rich follow_up_sequence (full touch objects with subject + body)
"""

from typing import TypedDict, Optional


class PitchState(TypedDict, total=False):
    # ── Input ─────────────────────────────────────────────────────────
    contact_name: str
    contact_title: str
    hotel_name: str
    hotel_location: Optional[str]
    linkedin_url: Optional[str]
    email: Optional[str]

    # ── Already-known SLH context (NOT re-derived by agents) ─────────
    sender_first_name: Optional[str]
    brand: Optional[str]
    brand_tier: Optional[str]
    project_type: Optional[str]
    timeline_label: Optional[str]  # URGENT/HOT/WARM/COOL/TBD
    opening_date: Optional[str]
    room_count: Optional[int]
    hotel_type: Optional[str]
    zone: Optional[str]
    is_client: Optional[bool]
    contact_priority: Optional[str]  # P1/P2/P3/P4
    contact_scope: Optional[str]
    slh_lead_score: Optional[int]  # SLH's own 100-pt score

    # ── Researcher output ─────────────────────────────────────────────
    company_summary: Optional[str]
    contact_summary: Optional[str]
    hotel_intel: Optional[dict]
    contact_intel: Optional[dict]
    recent_news: Optional[list[str]]
    pain_points: Optional[list[str]]
    signals: Optional[list[str]]
    hiring_signals: Optional[list[str]]
    expansion_signals: Optional[list[str]]
    awards: Optional[list[str]]
    outreach_angle: Optional[str]
    personalization_hook: Optional[str]
    conversation_hooks: Optional[list[str]]
    hotel_tier_inferred: Optional[str]

    # ── Analyst output ────────────────────────────────────────────────
    fit_score: Optional[int]
    fit_breakdown: Optional[dict]
    value_props: Optional[list[str]]
    primary_angle: Optional[str]

    # ── Writer output ─────────────────────────────────────────────────
    email_subject: Optional[str]
    email_body: Optional[str]
    linkedin_message: Optional[str]
    tone: Optional[str]

    # ── Critic output ─────────────────────────────────────────────────
    quality_approved: Optional[bool]
    quality_feedback: Optional[str]
    critic_rubric: Optional[dict]
    rewrite_count: Optional[int]
    previous_feedback: Optional[str]

    # ── Scheduler output ──────────────────────────────────────────────
    send_time: Optional[str]
    follow_up_sequence: Optional[list]
