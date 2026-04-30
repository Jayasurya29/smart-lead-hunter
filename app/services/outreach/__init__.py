"""
Outreach pipeline — ported from PitchIQ (https://github.com/Jayasurya29/pitchiq).

Five LangGraph agents that take a contact + hotel and produce:
  - personalization research brief (Researcher)
  - 100-pt fit score (Analyst)
  - email + LinkedIn message (Writer)
  - quality check with up to 2 rewrites (Critic)
  - send time + 3-touch follow-up sequence (Scheduler)

Phase 1 (current): generates outputs only. No email sending — sales rep
copies the email body into Gmail/Outlook and sends from there. The
"Mark as Sent" button in the UI updates the DB but does not call any
email API. See the Resend + email_sender code in the PitchIQ repo if
we add real send later.
"""
