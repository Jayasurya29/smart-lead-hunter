"""
Smart Lead Hunter — Sales Notifications
========================================

Pre-opening digest: emails sales when leads cross critical procurement
windows. Created 2026-05-06 (HV-2).

Why this exists
---------------
The 6-month uniform procurement window IS the product. Every HOT lead
represents a uniform decision happening in the next quarter, and
contracts that miss that window get bought from the incumbent the
property already used at sister hotels. A daily digest emailed at 7AM
saying "3 HOT leads crossed the 9-month mark today: …" is the
difference between "we knew about them in time" and "we didn't get
the RFP."

The digest task is hooked into Celery beat in app/tasks/celery_app.py
and runs each morning. It also stores `notified_at` in `notes` so the
same crossing isn't reported twice.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Iterable, Optional

logger = logging.getLogger(__name__)


# ── Configuration ──────────────────────────────────────────────────────
# Single env var DIGEST_RECIPIENTS lets ops set who gets the email
# without code changes. Comma-separated emails. If unset OR SMTP isn't
# configured, the digest task logs to stdout and short-circuits.
def _get_digest_recipients() -> list[str]:
    raw = os.getenv("DIGEST_RECIPIENTS", "")
    return [e.strip() for e in raw.split(",") if e.strip()]


def _smtp_configured() -> bool:
    # Same env vars as app/utils/email.py. Kept duplicated so the
    # notifications module doesn't have to import private helpers
    # from utils/email.py (avoids circular deps in tests).
    return bool(
        os.getenv("SMTP_HOST")
        and os.getenv("SMTP_USER")
        and os.getenv("SMTP_PASSWORD")
    )


# ── Digest content building ────────────────────────────────────────────
def build_digest_html(crossings: list[dict], generated_at: datetime) -> str:
    """Render the digest as a single HTML string ready for email."""
    if not crossings:
        return _empty_digest_html(generated_at)

    rows_html = []
    for c in crossings:
        score_color = (
            "#16a34a"
            if (c.get("lead_score") or 0) >= 70
            else "#ca8a04"
            if (c.get("lead_score") or 0) >= 50
            else "#64748b"
        )
        location_parts = [c.get("city") or "", c.get("state") or "", c.get("country") or ""]
        location = ", ".join(p for p in location_parts if p)
        rows_html.append(
            f"""
            <tr>
              <td style="padding: 12px; border-bottom: 1px solid #e2e8f0; font-weight: 600; color: #0f172a;">
                {c.get('hotel_name', '<unknown>')}
              </td>
              <td style="padding: 12px; border-bottom: 1px solid #e2e8f0; color: #475569;">
                {location}
              </td>
              <td style="padding: 12px; border-bottom: 1px solid #e2e8f0; color: #475569;">
                {c.get('opening_date', '')} <span style="color: #94a3b8;">({c.get('months_out', '?')} mo)</span>
              </td>
              <td style="padding: 12px; border-bottom: 1px solid #e2e8f0; color: #475569;">
                {c.get('brand_tier', '').replace('_', ' ').title()}
              </td>
              <td style="padding: 12px; border-bottom: 1px solid #e2e8f0;">
                <span style="background: {score_color}; color: white; border-radius: 4px; padding: 2px 8px; font-weight: 600;">
                  {c.get('lead_score', 0)}
                </span>
              </td>
            </tr>
            """
        )

    return f"""
    <div style="font-family: 'Segoe UI', Arial, sans-serif; max-width: 760px; margin: 0 auto; padding: 24px;">
      <div style="margin-bottom: 16px;">
        <h2 style="color: #0a1628; margin: 0;">Pre-Opening Digest</h2>
        <p style="color: #64748b; font-size: 13px; margin: 4px 0 0;">
          {generated_at.strftime('%A, %B %d, %Y')} · J.A. Uniforms · Smart Lead Hunter
        </p>
      </div>
      <div style="background: #f8fafc; border-radius: 8px; padding: 16px; margin-bottom: 16px; border-left: 4px solid #2563eb;">
        <strong style="color: #0a1628;">{len(crossings)}</strong>
        <span style="color: #475569;">lead(s) just crossed into the 6&ndash;12 month procurement window. These are HOT — uniform decisions are being made now.</span>
      </div>
      <table style="width: 100%; border-collapse: collapse; background: white; border: 1px solid #e2e8f0; border-radius: 8px; overflow: hidden;">
        <thead>
          <tr style="background: #f1f5f9;">
            <th style="padding: 12px; text-align: left; color: #334155; font-size: 12px; text-transform: uppercase;">Hotel</th>
            <th style="padding: 12px; text-align: left; color: #334155; font-size: 12px; text-transform: uppercase;">Location</th>
            <th style="padding: 12px; text-align: left; color: #334155; font-size: 12px; text-transform: uppercase;">Opens</th>
            <th style="padding: 12px; text-align: left; color: #334155; font-size: 12px; text-transform: uppercase;">Tier</th>
            <th style="padding: 12px; text-align: left; color: #334155; font-size: 12px; text-transform: uppercase;">Score</th>
          </tr>
        </thead>
        <tbody>
          {''.join(rows_html)}
        </tbody>
      </table>
      <p style="color: #94a3b8; font-size: 12px; margin-top: 16px;">
        Reply directly to this email if a lead looks wrong — Sales Intel will fix the data.
      </p>
    </div>
    """


def _empty_digest_html(generated_at: datetime) -> str:
    return f"""
    <div style="font-family: 'Segoe UI', Arial, sans-serif; max-width: 760px; margin: 0 auto; padding: 24px;">
      <h2 style="color: #0a1628; margin: 0;">Pre-Opening Digest</h2>
      <p style="color: #64748b; font-size: 13px;">
        {generated_at.strftime('%A, %B %d, %Y')} · No new lead crossings into the 6-12 month window today.
      </p>
    </div>
    """


# ── Sending ────────────────────────────────────────────────────────────
async def send_digest_email(
    recipients: Iterable[str],
    crossings: list[dict],
    *,
    generated_at: Optional[datetime] = None,
) -> bool:
    """Send the pre-opening digest to the given recipients.

    Returns True on success (or skipped-because-not-configured), False
    on send failure. Never raises — digest failures must not crash the
    Celery task.
    """
    generated_at = generated_at or datetime.now()
    recipients = [r for r in recipients if r]
    if not recipients:
        logger.info(
            "Pre-opening digest: no DIGEST_RECIPIENTS configured — printing %d crossing(s) to stdout",
            len(crossings),
        )
        for c in crossings:
            logger.info(
                "  %s — %s — opens %s (%s months) — score %s",
                c.get("hotel_name"),
                c.get("city"),
                c.get("opening_date"),
                c.get("months_out"),
                c.get("lead_score"),
            )
        return True

    if not _smtp_configured():
        logger.warning(
            "Pre-opening digest: SMTP not configured — set SMTP_HOST/USER/PASSWORD; "
            "digest with %d crossing(s) NOT sent to %s",
            len(crossings),
            ", ".join(recipients),
        )
        return True  # skipped-as-configured is not a failure

    html = build_digest_html(crossings, generated_at)
    subject = (
        f"SLH Pre-Opening Digest · {len(crossings)} new lead(s) in 6-12mo window"
        if crossings
        else "SLH Pre-Opening Digest · no new crossings"
    )

    try:
        import asyncio
        import smtplib
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText

        smtp_host = os.getenv("SMTP_HOST", "")
        smtp_port = int(os.getenv("SMTP_PORT", "587"))
        smtp_user = os.getenv("SMTP_USER", "")
        smtp_password = os.getenv("SMTP_PASSWORD", "")

        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = f"Smart Lead Hunter <{smtp_user}>"
        msg["To"] = ", ".join(recipients)
        msg.attach(MIMEText(html, "html"))

        def _send_sync():
            with smtplib.SMTP(smtp_host, smtp_port, timeout=15) as server:
                server.starttls()
                server.login(smtp_user, smtp_password)
                server.sendmail(smtp_user, list(recipients), msg.as_string())

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _send_sync)
        logger.info(
            "Pre-opening digest sent: %d crossing(s) → %s",
            len(crossings),
            ", ".join(recipients),
        )
        return True
    except Exception as e:
        logger.error("Pre-opening digest send failed: %s", e)
        return False
