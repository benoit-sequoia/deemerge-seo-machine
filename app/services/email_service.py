from __future__ import annotations

import smtplib
from email.message import EmailMessage

from app.config import Settings


class EmailService:
    def __init__(self, settings: Settings):
        self.settings = settings

    def send(self, subject: str, body: str) -> None:
        if not all([self.settings.smtp_host, self.settings.smtp_port, self.settings.smtp_user, self.settings.smtp_pass, self.settings.alert_email_to]):
            return
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = self.settings.smtp_user
        msg["To"] = self.settings.alert_email_to
        msg.set_content(body)

        with smtplib.SMTP(self.settings.smtp_host, self.settings.smtp_port, timeout=20) as server:
            server.starttls()
            server.login(self.settings.smtp_user, self.settings.smtp_pass)
            server.send_message(msg)
