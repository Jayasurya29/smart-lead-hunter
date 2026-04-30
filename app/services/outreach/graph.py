"""LangGraph orchestrator for the v2 outreach pipeline.

Researcher → Analyst → Writer → Critic → (rewrite up to 2x) → Scheduler.

The conditional-edge from Critic kicks back to Writer if the rubric
average is below 3.8 OR any individual score is below 3. Cap at 2
rewrites to avoid infinite loops.
"""

from __future__ import annotations

from langgraph.graph import StateGraph, END

from .state import PitchState
from .researcher import researcher_agent
from .analyst import analyst_agent
from .writer import writer_agent
from .critic import critic_agent
from .scheduler import scheduler_agent


def should_rewrite(state: PitchState) -> str:
    if state.get("quality_approved"):
        return "scheduler"
    if (state.get("rewrite_count") or 0) >= 3:
        # Allow up to 2 rewrites (initial draft + 2 retries = 3 total)
        return "scheduler"
    return "writer"


def build_graph():
    graph = StateGraph(PitchState)

    graph.add_node("researcher", researcher_agent)
    graph.add_node("analyst", analyst_agent)
    graph.add_node("writer", writer_agent)
    graph.add_node("critic", critic_agent)
    graph.add_node("scheduler", scheduler_agent)

    graph.set_entry_point("researcher")
    graph.add_edge("researcher", "analyst")
    graph.add_edge("analyst", "writer")
    graph.add_edge("writer", "critic")
    graph.add_conditional_edges(
        "critic",
        should_rewrite,
        {"writer": "writer", "scheduler": "scheduler"},
    )
    graph.add_edge("scheduler", END)

    return graph.compile()


def build_initial_state(
    *,
    # Required
    contact_name: str,
    contact_title: str,
    hotel_name: str,
    # Contact fields
    hotel_location: str = "",
    linkedin_url: str = "",
    email: str = "",
    # NEW v2: sender + SLH context (NOT re-derived by agents)
    sender_first_name: str = "",
    brand: str = "",
    brand_tier: str = "",
    project_type: str = "",
    timeline_label: str = "",
    opening_date: str = "",
    room_count: int | None = None,
    hotel_type: str = "",
    zone: str = "",
    is_client: bool = False,
    contact_priority: str = "",
    contact_scope: str = "",
    slh_lead_score: int | None = None,
) -> dict:
    """Build the initial state dict for the pipeline. All agent-output
    fields start as None — they get filled in as each agent runs."""
    return {
        # Input
        "contact_name": contact_name,
        "contact_title": contact_title or "",
        "hotel_name": hotel_name,
        "hotel_location": hotel_location or None,
        "linkedin_url": linkedin_url or None,
        "email": email or None,
        # SLH context
        "sender_first_name": sender_first_name or None,
        "brand": brand or None,
        "brand_tier": brand_tier or None,
        "project_type": project_type or None,
        "timeline_label": timeline_label or None,
        "opening_date": opening_date or None,
        "room_count": room_count,
        "hotel_type": hotel_type or None,
        "zone": zone or None,
        "is_client": is_client,
        "contact_priority": contact_priority or None,
        "contact_scope": contact_scope or None,
        "slh_lead_score": slh_lead_score,
        # Researcher
        "company_summary": None,
        "contact_summary": None,
        "hotel_intel": None,
        "contact_intel": None,
        "recent_news": None,
        "pain_points": None,
        "signals": None,
        "hiring_signals": None,
        "expansion_signals": None,
        "awards": None,
        "outreach_angle": None,
        "personalization_hook": None,
        "conversation_hooks": None,
        "hotel_tier_inferred": None,
        # Analyst
        "fit_score": None,
        "fit_breakdown": None,
        "value_props": None,
        "primary_angle": None,
        # Writer
        "email_subject": None,
        "email_body": None,
        "linkedin_message": None,
        "tone": None,
        # Critic
        "quality_approved": None,
        "quality_feedback": None,
        "critic_rubric": None,
        "rewrite_count": None,
        "previous_feedback": None,
        # Scheduler
        "send_time": None,
        "follow_up_sequence": None,
    }
