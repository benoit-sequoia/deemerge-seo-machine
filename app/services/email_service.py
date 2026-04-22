from __future__ import annotations
import smtplib
from email.message import EmailMessage
from ..config import Settings


class EmailService:
    def __init__(self, settings: Settings):
        self.settings = settings

    def send(self, subject: str, body: str) -> dict:
        if not all([self.settings.smtp_host, self.settings.smtp_user, self.settings.smtp_pass, self.settings.alert_email_to]):
            return {"ok": False, "skipped": True, "reason": "missing smtp config"}
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = self.settings.smtp_user
        msg["To"] = self.settings.alert_email_to
        msg.set_content(body)
        with smtplib.SMTP(self.settings.smtp_host, self.settings.smtp_port, timeout=30) as server:
            server.starttls()
            server.login(self.settings.smtp_user, self.settings.smtp_pass)
            server.send_message(msg)
        return {"ok": True}
