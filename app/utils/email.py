"""
Smart Lead Hunter — Email Utility
===================================
Sends OTP verification emails via SMTP.
Falls back to console logging if SMTP is not configured.

Required .env vars:
    SMTP_HOST=smtp.gmail.com
    SMTP_PORT=587
    SMTP_USER=yourapp@gmail.com
    SMTP_PASSWORD=your_app_password
"""

import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)

SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
FROM_NAME = "Smart Lead Hunter"


def _smtp_configured() -> bool:
    return bool(SMTP_HOST and SMTP_USER and SMTP_PASSWORD)


def _build_otp_html(first_name: str, otp: str) -> str:
    return f"""
    <div style="font-family: 'Segoe UI', Arial, sans-serif; max-width: 480px; margin: 0 auto; padding: 32px;">
        <div style="text-align: center; margin-bottom: 24px;">
            <h2 style="color: #0a1628; margin: 0;">Smart Lead Hunter</h2>
            <p style="color: #64748b; font-size: 13px; margin-top: 4px;">J.A. Uniforms · Hotel Intelligence</p>
        </div>
        <div style="background: #fafafa; border: 1px solid #e2e8f0; border-radius: 12px; padding: 32px; text-align: center;">
            <p style="color: #334155; font-size: 15px; margin: 0 0 8px;">Hi {first_name},</p>
            <p style="color: #64748b; font-size: 14px; margin: 0 0 24px;">Your verification code is:</p>
            <div style="font-size: 36px; font-weight: bold; letter-spacing: 8px; color: #0a1628; font-family: monospace; margin: 16px 0;">
                {otp}
            </div>
            <p style="color: #94a3b8; font-size: 12px; margin-top: 24px;">
                This code expires in 10 minutes.<br>
                If you didn't request this, ignore this email.
            </p>
        </div>
    </div>
    """


async def send_verification_email(to_email: str, first_name: str, otp: str) -> bool:
    """Send OTP verification email. Returns True if sent successfully.

    If SMTP is not configured, prints OTP to console for development.
    """
    if not _smtp_configured():
        logger.warning(
            f"SMTP not configured — OTP for {to_email}: {otp} "
            f"(set SMTP_HOST/SMTP_USER/SMTP_PASSWORD in .env)"
        )
        print(f"\n{'='*50}")
        print(f"  OTP for {to_email}: {otp}")
        print(f"{'='*50}\n")
        return True  # Return True so registration flow continues

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"Your verification code: {otp}"
        msg["From"] = f"{FROM_NAME} <{SMTP_USER}>"
        msg["To"] = to_email

        html_body = _build_otp_html(first_name, otp)
        msg.attach(MIMEText(html_body, "html"))

        # Run SMTP in thread to avoid blocking the event loop
        import asyncio

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _send_smtp, msg, to_email)

        logger.info(f"Verification email sent to {to_email}")
        return True

    except Exception as e:
        logger.error(f"Failed to send email to {to_email}: {e}")
        return False


def _send_smtp(msg: MIMEMultipart, to_email: str):
    """Synchronous SMTP send — called in executor."""
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=10) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.sendmail(SMTP_USER, to_email, msg.as_string())
