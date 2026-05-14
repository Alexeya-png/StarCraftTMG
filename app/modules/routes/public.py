from __future__ import annotations

from io import BytesIO

from flask import Blueprint, Response, abort, jsonify, make_response, redirect, render_template, request, send_file

from app.database import (
    fetch_current_league,
    fetch_league_results_overview,
    HEALTH_CHECK_DATABASE,
    fetch_player_profile,
    ping_database,
)
from app.modules.context import base_context
from app.modules.forms import _render_submit_page
from app.modules.payloads import _parse_leaderboard_filters
from app.modules.roster import _build_beta_roster_pdf_url, _clean_roster_id, _download_roster_pdf_via_browser
from app.modules.visual_backgrounds import resolve_player_visual_background

bp = Blueprint('public', __name__)

@bp.route('/')
def home():
    context = base_context(
        'TMG Stats – StarCraft TMG Community Ratings',
        'home',
        meta_description='TMG Stats is the ELO rating site for the StarCraft TMG community with player profiles, match reports and rankings.',
        canonical_path='/',
    )
    return render_template('home.html', **context)

@bp.route('/health')
def health():
    if request.args.get('db') != '1' and not HEALTH_CHECK_DATABASE:
        return jsonify({'status': 'ok', 'database_error': None})
    ok, error = ping_database()
    return jsonify({'status': 'ok' if ok else 'error', 'database_error': error})

@bp.route('/leaderboard')
def leaderboard():
    search, show_active, show_inactive, show_active_ranked, active_ranked_query_present = _parse_leaderboard_filters()

    is_filtered_page = bool(str(search).strip()) or not (show_active and not show_inactive) or show_active_ranked
    context = base_context(
        'Global Rating – TMG Stats',
        'leaderboard',
        meta_description='Global StarCraft TMG ELO leaderboard with player ratings, win rates and active community rankings.',
        canonical_path='/leaderboard',
        meta_robots='noindex,follow' if is_filtered_page else 'index,follow',
    )
    context.update(
        {
            'search': search,
            'show_active': show_active,
            'show_inactive': show_inactive,
            'show_active_ranked': show_active_ranked,
            'active_ranked_query_present': active_ranked_query_present,
        }
    )
    return render_template('leaderboard.html', **context)

@bp.route('/roster-pdf/<string:roster_id>')
def roster_pdf(roster_id: str):
    clean_roster_id = _clean_roster_id(roster_id)
    if not clean_roster_id:
        abort(404)

    direct_pdf_url = _build_beta_roster_pdf_url(clean_roster_id)
    if direct_pdf_url:
        return redirect(direct_pdf_url, code=302)

    try:
        pdf_bytes, filename = _download_roster_pdf_via_browser(clean_roster_id)
    except Exception as exc:
        return Response(str(exc), status=502, mimetype='text/plain; charset=utf-8')

    return send_file(
        BytesIO(pdf_bytes),
        mimetype='application/pdf',
        as_attachment=False,
        download_name=filename or f'roster-{clean_roster_id}.pdf',
        max_age=0,
    )

@bp.route('/reports')
@bp.route('/players')
def game_reports():
    if request.path == '/players':
        return redirect('/reports', code=301)

    current_page = max(1, request.args.get('page', 1, type=int) or 1)
    per_page = max(1, min(request.args.get('per_page', 25, type=int) or 25, 100))
    search = request.args.get('search', '')
    show_ranked_only = request.args.get('ranked_only') == '1'
    ranked_only_query_present = 'ranked_only' in request.args

    is_filtered_page = bool(str(search).strip()) or current_page > 1 or per_page != 25 or show_ranked_only
    context = base_context(
        'Game Reports – TMG Stats',
        'reports',
        meta_description='Recent StarCraft TMG game reports with results, races, missions and rating changes.',
        canonical_path='/reports',
        meta_robots='noindex,follow' if is_filtered_page else 'index,follow',
    )
    try:
        current_league = fetch_current_league(required=False)
    except Exception:
        current_league = None

    context.update(
        {
            'search': search,
            'per_page': per_page,
            'current_page': current_page,
            'show_ranked_only': show_ranked_only,
            'ranked_only_query_present': ranked_only_query_present,
            'current_league': current_league,
        }
    )
    return render_template('game_reports.html', **context)

@bp.route('/players/<int:player_id>')
def player_profile(player_id: int):
    db_error = None
    profile = None

    try:
        profile = fetch_player_profile(player_id)
    except Exception as exc:
        db_error = str(exc)

    if db_error:
        context = base_context(
            'Player Profile – TMG Stats',
            'players',
            meta_description='StarCraft TMG player profile with rating history and recent match results.',
            canonical_path=f'/players/{player_id}',
        )
        context.update({'player': None, 'recent_matches': [], 'rating_chart': None, 'priority_matchup_report': None,
                        'head_to_head_report': None, 'db_error': db_error})
        return make_response(render_template('player_profile.html', **context), 500)

    if not profile:
        context = base_context(
            'Player Not Found – TMG Stats',
            'players',
            meta_description='StarCraft TMG player profile page.',
            canonical_path=f'/players/{player_id}',
            meta_robots='noindex,follow',
        )
        context.update({'player': None, 'recent_matches': [], 'rating_chart': None, 'priority_matchup_report': None,
                        'head_to_head_report': None, 'db_error': None})
        return make_response(render_template('player_profile.html', **context), 404)

    player_name = profile['player']['name']
    priority_race = str(profile['player'].get('priority_race') or '').strip()
    meta_description = f"{player_name} player profile on TMG Stats with current rating, recent matches and rating history."
    if priority_race:
        meta_description = f"{player_name} {priority_race} player profile on TMG Stats with current rating, recent matches and rating history."

    context = base_context(
        f"{player_name} – Player Profile – TMG Stats",
        'players',
        meta_description=meta_description,
        canonical_path=f'/players/{player_id}',
        og_type='profile',
    )
    context.update(
        {
            'player': profile['player'],
            'recent_matches': profile['recent_matches'],
            'rating_chart': profile.get('rating_chart'),
            'priority_matchup_report': profile.get('priority_matchup_report'),
            'head_to_head_report': profile.get('head_to_head_report'),
            'profile_background': resolve_player_visual_background(profile['player']),
            'db_error': None,
        }
    )
    return render_template('player_profile.html', **context)

@bp.route('/leagues')
def league_results_page():
    context = base_context(
        'League Results – TMG Stats',
        'leagues',
        meta_description='Current and previous TMG Stats league results with champions, most active players and race leaders.',
        canonical_path='/leagues',
    )
    return render_template('league_results.html', **context)

@bp.route('/roster-pdf-test', methods=['GET'])
def roster_pdf_test_page():
    context = base_context(
        'Roster PDF Test – TMG Stats',
        'reports',
        meta_description='Client-side test page for generating roster PDFs from StarCraft TMG beta shared_rosters.',
        canonical_path='/roster-pdf-test',
        meta_robots='noindex,nofollow',
    )
    return render_template('roster_pdf_test.html', **context)
