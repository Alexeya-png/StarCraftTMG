from __future__ import annotations

import os

from flask import request

from .auth import is_admin


def get_site_url() -> str:
    raw_value = (os.getenv('SITE_URL') or 'https://tmg-stats.org').strip() or 'https://tmg-stats.org'
    return raw_value.rstrip('/')


def build_absolute_url(path: str) -> str:
    clean_path = '/' + str(path or '').lstrip('/')
    return f"{get_site_url()}{clean_path}"


def canonical_url(path: str | None = None) -> str:
    target_path = path if path is not None else request.path
    return build_absolute_url(target_path)


def default_meta_description() -> str:
    return 'TMG Stats is the ELO rating site for the StarCraft TMG community with player profiles, match reports and rankings.'


def base_context(
        page_title: str,
        active_page: str,
        *,
        meta_description: str | None = None,
        canonical_path: str | None = None,
        meta_robots: str = 'index,follow',
        og_type: str = 'website',
) -> dict:
    return {
        'page_title': page_title,
        'active_page': active_page,
        'is_admin': is_admin(),
        'meta_description': (meta_description or default_meta_description()).strip(),
        'canonical_url': canonical_url(canonical_path),
        'meta_robots': meta_robots,
        'og_type': og_type,
        'site_url': get_site_url(),
        'google_site_verification': (os.getenv('GOOGLE_SITE_VERIFICATION') or '').strip(),
    }
