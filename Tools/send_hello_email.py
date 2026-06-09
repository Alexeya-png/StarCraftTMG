"""Send a plain-text or HTML email through an SMTP server.

Usage from PowerShell:
    $env:TMG_SMTP_PASSWORD = "your-email-password"
    python Tools/send_hello_email.py --dry-run
    python Tools/send_hello_email.py `
        --to recipient@example.com `
        --subject "Bringing TMG Stats to the official level" `
        --body-file Tools/archon_studio_email.txt `
        --html-file Tools/archon_studio_email.html

You can also put TMG_SMTP_PASSWORD in .env or Tools/.env.
"""

from __future__ import annotations

import argparse
import os
import smtplib
import ssl
import sys
from email.utils import formataddr, formatdate, make_msgid
from email.message import EmailMessage
from pathlib import Path


DEFAULT_SMTP_HOST = "tmg-stats.org"
DEFAULT_SMTP_PORT = 465
DEFAULT_USERNAME = "customersupport@tmg-stats.org"
DEFAULT_FROM = "customersupport@tmg-stats.org"
DEFAULT_FROM_NAME = "TMG Stats"
DEFAULT_TO = "feen2939@gmail.com"
DEFAULT_SUBJECT = "hello world"
DEFAULT_BODY = "hello world"
PASSWORD_ENV = "TMG_SMTP_PASSWORD"


def load_env_files() -> None:
    """Load ignored .env files when python-dotenv is installed."""
    try:
        from dotenv import load_dotenv
    except ImportError:
        return

    repo_root = Path(__file__).resolve().parents[1]
    load_dotenv(repo_root / ".env")
    load_dotenv(Path(__file__).resolve().parent / ".env", override=True)


def read_message_file(path: str | None) -> str | None:
    if not path:
        return None
    return Path(path).read_text(encoding="utf-8")


def build_message(
    sender: str,
    sender_name: str,
    recipient: str,
    subject: str,
    body: str,
    html_body: str | None = None,
) -> EmailMessage:
    message = EmailMessage()
    message["From"] = formataddr((sender_name, sender))
    message["To"] = recipient
    message["Reply-To"] = sender
    message["Subject"] = subject
    message["Date"] = formatdate(localtime=True)
    message["Message-ID"] = make_msgid(domain=sender.rpartition("@")[2])
    message.set_content(body)
    if html_body:
        message.add_alternative(html_body, subtype="html")
    return message


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Send a plain-text or HTML email using SMTP."
    )
    parser.add_argument("--host", default=os.getenv("TMG_SMTP_HOST", DEFAULT_SMTP_HOST))
    parser.add_argument("--port", type=int, default=int(os.getenv("TMG_SMTP_PORT", DEFAULT_SMTP_PORT)))
    parser.add_argument("--username", default=os.getenv("TMG_SMTP_USERNAME", DEFAULT_USERNAME))
    parser.add_argument("--sender", default=os.getenv("TMG_SMTP_FROM", DEFAULT_FROM))
    parser.add_argument("--sender-name", default=os.getenv("TMG_SMTP_FROM_NAME", DEFAULT_FROM_NAME))
    parser.add_argument("--to", default=os.getenv("TMG_SMTP_TO", DEFAULT_TO))
    parser.add_argument("--subject", default=os.getenv("TMG_SMTP_SUBJECT", DEFAULT_SUBJECT))
    parser.add_argument("--body", default=os.getenv("TMG_SMTP_BODY", DEFAULT_BODY))
    parser.add_argument(
        "--body-file",
        help="Read the plain-text body from a UTF-8 file instead of --body.",
    )
    parser.add_argument(
        "--html-file",
        help="Use a UTF-8 HTML file as the rendered email body, not as an attachment.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the email that would be sent without connecting to SMTP.",
    )
    return parser.parse_args()


def main() -> int:
    load_env_files()
    args = parse_args()
    password = os.getenv(PASSWORD_ENV)

    body = read_message_file(args.body_file) or args.body
    html_body = read_message_file(args.html_file)
    message = build_message(
        args.sender,
        args.sender_name,
        args.to,
        args.subject,
        body,
        html_body,
    )

    if args.dry_run:
        print(message)
        return 0

    if not password:
        print(
            f"Missing SMTP password. Set {PASSWORD_ENV} in PowerShell, .env, or Tools/.env.",
            file=sys.stderr,
        )
        return 2

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(args.host, args.port, context=context, timeout=30) as smtp:
        smtp.login(args.username, password)
        smtp.send_message(message)

    print(f"Sent test email to {args.to} from {args.sender}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
