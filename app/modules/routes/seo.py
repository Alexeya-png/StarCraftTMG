from __future__ import annotations

from flask import Blueprint, current_app, make_response, send_from_directory

from app.modules.context import build_absolute_url
from app.modules.seo import (
    _build_pages_sitemap_entries,
    _build_player_sitemap_entries,
    _render_sitemap_index,
    _render_sitemap_urlset,
    _today_lastmod,
)

bp = Blueprint('seo', __name__)

@bp.route('/robots.txt')
def robots_txt():
    sitemap_url = build_absolute_url('/sitemap.xml')
    lines = [
        'User-agent: *',
        'Allow: /',
        'Disallow: /admin',
        f'Sitemap: {sitemap_url}',
    ]
    response = make_response('\n'.join(lines) + '\n')
    response.mimetype = 'text/plain'
    return response

@bp.route('/sitemap.xml')
def sitemap_xml():
    lastmod = _today_lastmod()
    entries = [
        (build_absolute_url('/sitemap-pages.xml'), lastmod),
        (build_absolute_url('/sitemap-players.xml'), lastmod),
    ]
    return _render_sitemap_index(entries)

@bp.route('/service-worker.js')
def service_worker():
    response = make_response(
        send_from_directory(current_app.static_folder, 'service-worker.js', mimetype='application/javascript')
    )
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Service-Worker-Allowed'] = '/'
    return response

@bp.route('/sitemap-pages.xml')
def sitemap_pages_xml():
    return _render_sitemap_urlset(_build_pages_sitemap_entries())

@bp.route('/sitemap-players.xml')
def sitemap_players_xml():
    return _render_sitemap_urlset(_build_player_sitemap_entries())

@bp.route('/google35d0caf8d54d36f2.html')
def google_site_verification():
    return send_from_directory(current_app.static_folder, 'google35d0caf8d54d36f2.html', mimetype='text/html')
