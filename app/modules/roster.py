from __future__ import annotations

import os
from urllib.parse import quote


def get_beta_roster_pdf_url_template() -> str:
    return (os.getenv('BETA_ROSTER_PDF_URL_TEMPLATE') or '').strip()


def _clean_roster_id(value: str | None) -> str:
    raw_value = str(value or '').strip()
    safe_chars = []
    for char in raw_value:
        if char.isalnum() or char in ('-', '_'):
            safe_chars.append(char)
    return ''.join(safe_chars)[:80]


def _build_beta_roster_pdf_url(roster_id: str) -> str | None:
    clean_roster_id = _clean_roster_id(roster_id)
    if not clean_roster_id:
        return None
    template = get_beta_roster_pdf_url_template()
    if not template:
        return None
    return template.format(roster_id=quote(clean_roster_id, safe=''))

