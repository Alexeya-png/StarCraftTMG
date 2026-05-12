from __future__ import annotations

from datetime import datetime, timezone
from html import escape

from flask import Response

from app.database import fetch_leaderboard
from .context import build_absolute_url

def _xml_response(xml_parts: list[str]) -> Response:
    return Response('\n'.join(xml_parts), mimetype='application/xml')

def _today_lastmod() -> str:
    return datetime.now(timezone.utc).date().isoformat()

def _render_sitemap_urlset(entries: list[tuple[str, str | None]]) -> Response:
    unique_entries: list[tuple[str, str | None]] = []
    seen_urls: set[str] = set()
    for url, lastmod in entries:
        if url in seen_urls:
            continue
        seen_urls.add(url)
        unique_entries.append((url, lastmod))

    xml_parts = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ]
    for url, lastmod in unique_entries:
        xml_parts.append('  <url>')
        xml_parts.append(f'    <loc>{escape(url)}</loc>')
        if lastmod:
            xml_parts.append(f'    <lastmod>{escape(lastmod)}</lastmod>')
        xml_parts.append('  </url>')
    xml_parts.append('</urlset>')
    return _xml_response(xml_parts)

def _render_sitemap_index(entries: list[tuple[str, str | None]]) -> Response:
    xml_parts = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ]
    for url, lastmod in entries:
        xml_parts.append('  <sitemap>')
        xml_parts.append(f'    <loc>{escape(url)}</loc>')
        if lastmod:
            xml_parts.append(f'    <lastmod>{escape(lastmod)}</lastmod>')
        xml_parts.append('  </sitemap>')
    xml_parts.append('</sitemapindex>')
    return _xml_response(xml_parts)

def _build_pages_sitemap_entries() -> list[tuple[str, str | None]]:
    lastmod = _today_lastmod()
    return [
        (build_absolute_url('/'), lastmod),
        (build_absolute_url('/leaderboard'), lastmod),
        (build_absolute_url('/reports'), lastmod),
        (build_absolute_url('/submit'), lastmod),
        (build_absolute_url('/leagues'), lastmod),
    ]

def _build_player_sitemap_entries() -> list[tuple[str, str | None]]:
    try:
        players = fetch_leaderboard(include_active=True, include_inactive=True)
    except Exception:
        return []

    entries: list[tuple[str, str | None]] = []
    for player in players:
        player_id = int(player.get('id') or 0)
        matches_count = int(player.get('matches_count') or 0)
        if player_id <= 0 or matches_count <= 0:
            continue
        entries.append((build_absolute_url(f'/players/{player_id}'), None))
    return entries
