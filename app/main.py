import base64
import hashlib
import hmac
import json
import os
from datetime import datetime, timedelta, timezone
from html import escape
from pathlib import Path

from dotenv import load_dotenv
from flask import Flask, Response, jsonify, make_response, redirect, render_template, request, send_from_directory

from app.database import (
    MatchSubmissionRateLimitError,
    delete_match_admin,
    fetch_game_reports_page,
    fetch_leaderboard,
    fetch_match_admin,
    fetch_mission_suggestions,
    fetch_player_admin,
    fetch_player_name_suggestions,
    fetch_player_profile,
    ping_database,
    submit_match_result,
    submit_tts_match_result,
    update_match_admin,
    update_player_admin,
)

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent

load_dotenv(PROJECT_ROOT / '.env')

app = Flask(
    __name__,
    template_folder=str(BASE_DIR / 'templates'),
    static_folder=str(BASE_DIR / 'static'),
    static_url_path='/static',
)
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 31536000


def _build_asset_version() -> str:
    explicit_version = os.getenv('APP_VERSION', '').strip()
    if explicit_version:
        return explicit_version

    asset_files = [
        BASE_DIR / 'static' / 'styles.css',
        BASE_DIR / 'static' / 'favicon.png',
        BASE_DIR / 'static' / 'Race' / 'logo.png',
    ]
    version_parts: list[str] = []
    for path in asset_files:
        try:
            stat = path.stat()
            version_parts.append(f"{path.name}:{int(stat.st_mtime)}:{stat.st_size}")
        except OSError:
            continue

    if not version_parts:
        return '1'

    digest = hashlib.sha1('|'.join(version_parts).encode('utf-8')).hexdigest()
    return digest[:12]


ASSET_VERSION = _build_asset_version()


@app.context_processor
def inject_asset_version() -> dict:
    return {'asset_version': ASSET_VERSION}


RACE_OPTIONS = [
    {'label': 'Терран', 'slug': 'terran'},
    {'label': 'Протосс', 'slug': 'protoss'},
    {'label': 'Зерг', 'slug': 'zerg'},
]
RACE_LABELS = [item['label'] for item in RACE_OPTIONS]

GAME_TYPE_OPTIONS = ['1к', '2к', 'Grand Offensive']
DEFAULT_MISSION_OPTIONS = [
    'Divide and Conquer',
    'Frontlines',
    'Gather the Resources',
    'Hold Position',
    'Supply Drop',
    'Frontline',
    'Other / Custom',
]

ADMIN_COOKIE_NAME = 'starcraft_admin_session'
ADMIN_SESSION_HOURS = 12
SUBMIT_NAME_SUGGESTION_LIMIT = int(os.getenv('SUBMIT_NAME_SUGGESTION_LIMIT', '200') or '200')


def _get_site_url() -> str:
    raw_value = (os.getenv('SITE_URL') or 'https://tmg-stats.org').strip() or 'https://tmg-stats.org'
    return raw_value.rstrip('/')


def _get_tts_submit_token() -> str:
    return (os.getenv('TTS_SUBMIT_TOKEN') or '').strip()


def _coerce_tts_game_type(value: str | None) -> str:
    clean_value = str(value or '').strip()
    mapping = {
        '1k': '1к',
        '2k': '2к',
        '1к': '1к',
        '2к': '2к',
        'Grand Offensive': 'Grand Offensive',
    }
    return mapping.get(clean_value, clean_value)


def _parse_tts_request_payload() -> dict:
    payload = request.get_json(silent=True)
    if isinstance(payload, dict):
        return {str(key): value for key, value in payload.items()}
    return {key: value for key, value in request.form.items()}


def _build_absolute_url(path: str) -> str:
    clean_path = '/' + str(path or '').lstrip('/')
    return f"{_get_site_url()}{clean_path}"


def _canonical_url(path: str | None = None) -> str:
    target_path = path if path is not None else request.path
    return _build_absolute_url(target_path)


def _default_meta_description() -> str:
    return 'StarCraft ELO ratings, player profiles and match reports for the StarCraft TMG community.'


@app.after_request
def apply_fast_page_headers(response):
    if request.path.startswith('/static'):
        response.headers['Cache-Control'] = 'public, max-age=31536000, immutable'
        response.headers.pop('Pragma', None)
        response.headers.pop('Expires', None)
        return response

    response.headers['Cache-Control'] = 'no-cache, max-age=0, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    response.headers['Vary'] = 'Cookie'

    if request.path.startswith('/admin'):
        response.headers['X-Robots-Tag'] = 'noindex, nofollow, noarchive'
    elif request.path == '/health':
        response.headers['X-Robots-Tag'] = 'noindex, nofollow'

    return response


def _get_admin_login() -> str:
    return (os.getenv('ADMIN_LOGIN') or os.getenv('ADMIN_USERNAME') or 'admin').strip() or 'admin'



def _get_admin_password() -> str:
    return (os.getenv('ADMIN_PASSWORD') or 'admin').strip() or 'admin'



def _get_admin_secret() -> str:
    secret = (
        os.getenv('ADMIN_SECRET')
        or os.getenv('SECRET_KEY')
        or os.getenv('password')
        or 'starcraft-local-admin-secret'
    )
    return secret.strip() or 'starcraft-local-admin-secret'



def _b64encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode('utf-8').rstrip('=')



def _b64decode(value: str) -> bytes:
    padding = '=' * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)



def _build_admin_cookie(login: str) -> str:
    expires_at = int((datetime.now(timezone.utc) + timedelta(hours=ADMIN_SESSION_HOURS)).timestamp())
    payload = {'login': login, 'exp': expires_at}
    payload_bytes = json.dumps(payload, separators=(',', ':'), ensure_ascii=False).encode('utf-8')
    payload_part = _b64encode(payload_bytes)
    signature = hmac.new(_get_admin_secret().encode('utf-8'), payload_part.encode('utf-8'), hashlib.sha256).hexdigest()
    return f'{payload_part}.{signature}'



def _read_admin_cookie(token: str | None) -> dict | None:
    if not token or '.' not in token:
        return None

    payload_part, signature = token.rsplit('.', 1)
    expected_signature = hmac.new(
        _get_admin_secret().encode('utf-8'),
        payload_part.encode('utf-8'),
        hashlib.sha256,
    ).hexdigest()

    if not hmac.compare_digest(signature, expected_signature):
        return None

    try:
        payload = json.loads(_b64decode(payload_part).decode('utf-8'))
    except Exception:
        return None

    exp = int(payload.get('exp') or 0)
    if exp <= int(datetime.now(timezone.utc).timestamp()):
        return None

    return payload



def _is_admin() -> bool:
    payload = _read_admin_cookie(request.cookies.get(ADMIN_COOKIE_NAME))
    if not payload:
        return False
    return payload.get('login') == _get_admin_login()



def _base_context(
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
        'is_admin': _is_admin(),
        'meta_description': (meta_description or _default_meta_description()).strip(),
        'canonical_url': _canonical_url(canonical_path),
        'meta_robots': meta_robots,
        'og_type': og_type,
        'site_url': _get_site_url(),
        'google_site_verification': (os.getenv('GOOGLE_SITE_VERIFICATION') or '').strip(),
    }



def _redirect_to_admin_login():
    return redirect('/admin', code=303)



def _merge_mission_options() -> list[str]:
    try:
        db_values = fetch_mission_suggestions(limit=100)
    except Exception:
        db_values = []

    merged: list[str] = []
    for value in DEFAULT_MISSION_OPTIONS + db_values:
        clean_value = str(value).strip()
        if clean_value and clean_value not in merged:
            merged.append(clean_value)
    return merged



def _build_submit_form_state(raw_values: dict | None = None) -> dict:
    source = raw_values or {}
    return {
        'winner_name': str(source.get('winner_name', '')).strip(),
        'opponent_name': str(source.get('opponent_name', '')).strip(),
        'winner_race': str(source.get('winner_race', 'Терран')).strip() or 'Терран',
        'opponent_race': str(source.get('opponent_race', 'Протосс')).strip() or 'Протосс',
        'result_type': str(source.get('result_type', 'win')).strip() or 'win',
        'is_ranked': str(source.get('is_ranked', 'yes')).strip() or 'yes',
        'game_type': str(source.get('game_type', '1к')).strip() or '1к',
        'mission_name': str(source.get('mission_name', '')).strip(),
        'player1_score': str(source.get('player1_score', '')).strip(),
        'player2_score': str(source.get('player2_score', '')).strip(),
        'comment': str(source.get('comment', '')).strip(),
    }



def _render_submit_page(
    *,
    form_state: dict | None = None,
    error_message: str | None = None,
    success_data: dict | None = None,
    status_code: int = 200,
):
    name_suggestions: list[str] = []
    try:
        name_suggestions = fetch_player_name_suggestions(limit=SUBMIT_NAME_SUGGESTION_LIMIT)
    except Exception:
        name_suggestions = []

    context = _base_context(
        'Submit Match – StarCraft ELO',
        'submit',
        meta_description='Submit a StarCraft TMG match result to the StarCraft ELO community rating site.',
        canonical_path='/submit',
    )
    context.update(
        {
            'race_options': RACE_OPTIONS,
            'game_type_options': GAME_TYPE_OPTIONS,
            'mission_options': _merge_mission_options(),
            'name_suggestions': name_suggestions,
            'form_state': _build_submit_form_state(form_state),
            'error_message': error_message,
            'success_data': success_data,
        }
    )
    return make_response(render_template('submit_match.html', **context), status_code)



def _build_admin_player_form_state(player: dict | None = None, source: dict | None = None) -> dict:
    player = player or {}
    source = source or {}
    return {
        'name': str(source.get('name', player.get('name', ''))).strip(),
        'country_code': str(source.get('country_code', player.get('country_code', ''))).strip(),
        'country_name': str(source.get('country_name', player.get('country_name', ''))).strip(),
        'discord_url': str(source.get('discord_url', player.get('discord_url', ''))).strip(),
        'priority_race': str(source.get('priority_race', player.get('priority_race', ''))).strip(),
        'current_elo': str(source.get('current_elo', player.get('current_elo_input', 1000))).strip(),
        'is_active': str(source.get('is_active', 'on' if player.get('is_active', True) else '')).strip(),
    }



def _build_admin_match_form_state(match: dict | None = None, source: dict | None = None) -> dict:
    match = match or {}
    source = source or {}
    return {
        'player1_name': str(source.get('player1_name', match.get('player1_name', ''))).strip(),
        'player2_name': str(source.get('player2_name', match.get('player2_name', ''))).strip(),
        'winner_side': str(source.get('winner_side', match.get('winner_side', 'player1'))).strip() or 'player1',
        'player1_race': str(source.get('player1_race', match.get('player1_race', 'Терран'))).strip() or 'Терран',
        'player2_race': str(source.get('player2_race', match.get('player2_race', 'Протосс'))).strip() or 'Протосс',
        'is_ranked': str(source.get('is_ranked', 'yes' if match.get('is_ranked', True) else 'no')).strip() or 'yes',
        'game_type': str(source.get('game_type', match.get('game_type', '1к'))).strip() or '1к',
        'mission_name': str(source.get('mission_name', match.get('mission_name', ''))).strip(),
        'player1_score': str(source.get('player1_score', match.get('player1_score', ''))).strip(),
        'player2_score': str(source.get('player2_score', match.get('player2_score', ''))).strip(),
        'comment': str(source.get('comment', match.get('comment', ''))).strip(),
        'played_at': str(source.get('played_at', match.get('played_at_input', ''))).strip(),
    }



def _parse_admin_datetime(value: str) -> datetime:
    clean_value = str(value or '').strip()
    if not clean_value:
        raise ValueError('Enter played at date and time.')

    for fmt in ('%Y-%m-%dT%H:%M', '%Y-%m-%d %H:%M', '%Y-%m-%dT%H:%M:%S'):
        try:
            return datetime.strptime(clean_value, fmt)
        except ValueError:
            continue

    raise ValueError('Use date format YYYY-MM-DDTHH:MM.')



def _pagination_numbers(current_page: int, total_pages: int) -> list[int | str]:
    if total_pages <= 7:
        return list(range(1, total_pages + 1))
    if current_page <= 4:
        return [1, 2, 3, 4, '...', total_pages]
    if current_page >= total_pages - 3:
        return [1, '...', total_pages - 3, total_pages - 2, total_pages - 1, total_pages]
    return [1, '...', current_page - 1, current_page, current_page + 1, '...', total_pages]


@app.route('/robots.txt')
def robots_txt():
    sitemap_url = _build_absolute_url('/sitemap.xml')
    lines = [
        'User-agent: *',
        'Allow: /',
        'Disallow: /admin',
        f'Sitemap: {sitemap_url}',
    ]
    response = make_response('\n'.join(lines) + '\n')
    response.mimetype = 'text/plain'
    return response


@app.route('/sitemap.xml')
def sitemap_xml():
    entries: list[tuple[str, str | None]] = [
        (_build_absolute_url('/'), None),
        (_build_absolute_url('/leaderboard'), None),
        (_build_absolute_url('/reports'), None),
        (_build_absolute_url('/submit'), None),
    ]

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

    return Response('\n'.join(xml_parts), mimetype='application/xml')

@app.route('/')
def home():
    context = _base_context(
        'StarCraft ELO – StarCraft TMG Community Ratings',
        'home',
        meta_description='StarCraft ELO ratings, match reports and player profiles for the StarCraft TMG community.',
        canonical_path='/',
    )
    return render_template('home.html', **context)


@app.route('/health')
def health():
    ok, error = ping_database()
    return jsonify({'status': 'ok' if ok else 'error', 'database_error': error})


@app.route('/leaderboard')
def leaderboard():
    db_error = None
    players = []
    search = request.args.get('search', '')

    selected_statuses = {
        value.strip().lower()
        for value in request.args.getlist('status')
        if value and value.strip()
    }
    if not selected_statuses:
        selected_statuses = {'active'}

    show_active = 'active' in selected_statuses
    show_inactive = 'inactive' in selected_statuses

    try:
        players = fetch_leaderboard(
            search=search,
            include_active=show_active,
            include_inactive=show_inactive,
        )
    except Exception as exc:
        db_error = str(exc)

    is_filtered_page = bool(str(search).strip()) or not (show_active and not show_inactive)
    context = _base_context(
        'Global Rating – StarCraft ELO',
        'leaderboard',
        meta_description='Global StarCraft ELO leaderboard with player ratings, win rates and active community rankings.',
        canonical_path='/leaderboard',
        meta_robots='noindex,follow' if is_filtered_page else 'index,follow',
    )
    context.update(
        {
            'players': players,
            'search': search,
            'show_active': show_active,
            'show_inactive': show_inactive,
            'db_error': db_error,
        }
    )
    return render_template('leaderboard.html', **context)


@app.route('/reports')
@app.route('/players')
def game_reports():
    if request.path == '/players':
        return redirect('/reports', code=301)

    db_error = None
    matches = []
    total_matches = 0
    current_page = max(1, request.args.get('page', 1, type=int) or 1)
    per_page = max(1, min(request.args.get('per_page', 25, type=int) or 25, 100))
    search = request.args.get('search', '')
    total_pages = 1
    pagination_numbers: list[int | str] = []

    try:
        page_data = fetch_game_reports_page(search=search, page=current_page, per_page=per_page)
        matches = page_data['items']
        total_matches = page_data['total_count']
        current_page = page_data['page']
        total_pages = page_data['total_pages']
        pagination_numbers = _pagination_numbers(current_page, total_pages)
    except Exception as exc:
        db_error = str(exc)

    is_filtered_page = bool(str(search).strip()) or current_page > 1 or per_page != 25
    context = _base_context(
        'Game Reports – StarCraft ELO',
        'reports',
        meta_description='Recent StarCraft TMG game reports with results, races, missions and rating changes.',
        canonical_path='/reports',
        meta_robots='noindex,follow' if is_filtered_page else 'index,follow',
    )
    context.update(
        {
            'matches': matches,
            'search': search,
            'per_page': per_page,
            'db_error': db_error,
            'total_matches': total_matches,
            'current_page': current_page,
            'total_pages': total_pages,
            'pagination_numbers': pagination_numbers,
        }
    )
    return render_template('game_reports.html', **context)


@app.route('/players/<int:player_id>')
def player_profile(player_id: int):
    db_error = None
    profile = None

    try:
        profile = fetch_player_profile(player_id)
    except Exception as exc:
        db_error = str(exc)

    if db_error:
        context = _base_context(
            'Player Profile – StarCraft ELO',
            'players',
            meta_description='StarCraft ELO player profile with rating history and recent match results.',
            canonical_path=f'/players/{player_id}',
        )
        context.update({'player': None, 'recent_matches': [], 'rating_chart': None, 'priority_matchup_report': None, 'db_error': db_error})
        return make_response(render_template('player_profile.html', **context), 500)

    if not profile:
        context = _base_context(
            'Player Not Found – StarCraft ELO',
            'players',
            meta_description='StarCraft ELO player profile page.',
            canonical_path=f'/players/{player_id}',
            meta_robots='noindex,follow',
        )
        context.update({'player': None, 'recent_matches': [], 'rating_chart': None, 'priority_matchup_report': None, 'db_error': None})
        return make_response(render_template('player_profile.html', **context), 404)

    player_name = profile['player']['name']
    priority_race = str(profile['player'].get('priority_race') or '').strip()
    meta_description = f"{player_name} player profile on StarCraft ELO with current rating, recent matches and rating history."
    if priority_race:
        meta_description = f"{player_name} {priority_race} player profile on StarCraft ELO with current rating, recent matches and rating history."

    context = _base_context(
        f"{player_name} – Player Profile – StarCraft ELO",
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
            'db_error': None,
        }
    )
    return render_template('player_profile.html', **context)


@app.route('/submit', methods=['GET'])
def submit_result():
    return _render_submit_page()


@app.route('/submit', methods=['POST'])
def submit_result_post():
    form_state = {key: value for key, value in request.form.items()}

    try:
        success_data = submit_match_result(
            winner_name=form_state.get('winner_name', ''),
            opponent_name=form_state.get('opponent_name', ''),
            winner_race=form_state.get('winner_race', ''),
            opponent_race=form_state.get('opponent_race', ''),
            result_type=form_state.get('result_type', 'win'),
            is_ranked=form_state.get('is_ranked', 'yes'),
            game_type=form_state.get('game_type', ''),
            mission_name=form_state.get('mission_name', ''),
            player1_score=form_state.get('player1_score', ''),
            player2_score=form_state.get('player2_score', ''),
            comment=form_state.get('comment', ''),
        )
    except Exception as exc:
        return _render_submit_page(form_state=form_state, error_message=str(exc), status_code=400)

    return _render_submit_page(form_state=None, success_data=success_data, status_code=200)


@app.route('/api/tts/submit-match', methods=['POST'])
def submit_tts_match():
    payload = _parse_tts_request_payload()

    expected_token = _get_tts_submit_token()
    provided_token = str(payload.get('api_token', '') or '').strip()
    if expected_token and provided_token != expected_token:
        return jsonify({'ok': False, 'message': 'Invalid TTS submit token.'}), 403

    first_player_name = str(
        payload.get('first_player_name')
        or payload.get('winner_name')
        or ''
    ).strip()

    second_player_name = str(
        payload.get('second_player_name')
        or payload.get('opponent_name')
        or ''
    ).strip()

    first_player_race = str(
        payload.get('first_player_race')
        or payload.get('winner_race')
        or ''
    ).strip()

    second_player_race = str(
        payload.get('second_player_race')
        or payload.get('opponent_race')
        or ''
    ).strip()

    try:
        success_data = submit_tts_match_result(
            winner_name=first_player_name,
            opponent_name=second_player_name,
            winner_race=first_player_race,
            opponent_race=second_player_race,
            result_type=str(payload.get('result_type', 'win') or 'win'),
            is_ranked=str(payload.get('is_ranked', 'yes') or 'yes'),
            game_type=_coerce_tts_game_type(payload.get('game_type', '')),
            mission_name=str(payload.get('mission_name', '') or ''),
            player1_score=payload.get('player1_score', ''),
            player2_score=payload.get('player2_score', ''),
            comment='',
        )
    except MatchSubmissionRateLimitError as exc:
        return jsonify({'ok': False, 'message': str(exc)}), 429
    except Exception as exc:
        return jsonify({'ok': False, 'message': str(exc)}), 400

    return jsonify({'ok': True, 'message': 'Match submitted successfully.', 'data': success_data}), 200
    
@app.route('/admin', methods=['GET'])
def admin():
    context = _base_context('Admin Panel', 'admin', meta_description='Admin area.', canonical_path='/admin', meta_robots='noindex,nofollow')
    context.update({'login_error': None, 'admin_login_default': _get_admin_login()})
    template_name = 'admin_dashboard.html' if context['is_admin'] else 'admin_login.html'
    return render_template(template_name, **context)


@app.route('/admin/login', methods=['POST'])
def admin_login():
    login = str(request.form.get('login', '')).strip()
    password = str(request.form.get('password', '')).strip()

    if login != _get_admin_login() or password != _get_admin_password():
        context = _base_context('Admin Panel', 'admin', meta_description='Admin area.', canonical_path='/admin', meta_robots='noindex,nofollow')
        context.update({'login_error': 'Wrong login or password.', 'admin_login_default': login})
        return make_response(render_template('admin_login.html', **context), 401)

    response = redirect('/admin', code=303)
    response.set_cookie(
        ADMIN_COOKIE_NAME,
        _build_admin_cookie(login),
        max_age=ADMIN_SESSION_HOURS * 60 * 60,
        httponly=True,
        samesite='Lax',
        secure=False,
        path='/',
    )
    return response


@app.route('/admin/logout', methods=['POST'])
def admin_logout():
    response = redirect('/admin', code=303)
    response.delete_cookie(ADMIN_COOKIE_NAME, path='/')
    return response


@app.route('/admin/players/<int:player_id>', methods=['GET'])
def admin_edit_player(player_id: int):
    if not _is_admin():
        return _redirect_to_admin_login()

    player = fetch_player_admin(player_id)
    if not player:
        context = _base_context('Player Not Found', 'admin', meta_description='Admin area.', canonical_path=f'/admin/players/{player_id}', meta_robots='noindex,nofollow')
        context.update(
            {
                'player': None,
                'form_state': _build_admin_player_form_state(),
                'error_message': 'Player not found.',
                'success_message': None,
                'race_options': RACE_LABELS,
            }
        )
        return make_response(render_template('admin_edit_player.html', **context), 404)

    context = _base_context(f"Edit Player – {player['name']}", 'admin', meta_description='Admin area.', canonical_path=f'/admin/players/{player_id}', meta_robots='noindex,nofollow')
    context.update(
        {
            'player': player,
            'form_state': _build_admin_player_form_state(player),
            'error_message': None,
            'success_message': 'Player saved.' if request.args.get('saved') == '1' else None,
            'race_options': RACE_LABELS,
        }
    )
    return render_template('admin_edit_player.html', **context)


@app.route('/admin/players/<int:player_id>', methods=['POST'])
def admin_edit_player_post(player_id: int):
    if not _is_admin():
        return _redirect_to_admin_login()

    form_state = {key: value for key, value in request.form.items()}
    try:
        player = update_player_admin(
            player_id=player_id,
            name=form_state.get('name', ''),
            country_code=form_state.get('country_code', ''),
            country_name=form_state.get('country_name', ''),
            discord_url=form_state.get('discord_url', ''),
            priority_race=form_state.get('priority_race', ''),
            current_elo=form_state.get('current_elo', ''),
            is_active=form_state.get('is_active') == 'on',
        )
    except Exception as exc:
        existing_player = fetch_player_admin(player_id)
        context = _base_context('Edit Player', 'admin', meta_description='Admin area.', canonical_path=f'/admin/players/{player_id}', meta_robots='noindex,nofollow')
        context.update(
            {
                'player': existing_player,
                'form_state': _build_admin_player_form_state(existing_player, form_state),
                'error_message': str(exc),
                'success_message': None,
                'race_options': RACE_LABELS,
            }
        )
        return make_response(render_template('admin_edit_player.html', **context), 400)

    return redirect(f'/admin/players/{player["id"]}?saved=1', code=303)


@app.route('/admin/matches/<int:match_id>', methods=['GET'])
def admin_edit_match(match_id: int):
    if not _is_admin():
        return _redirect_to_admin_login()

    match = fetch_match_admin(match_id)
    if not match:
        context = _base_context('Match Not Found', 'admin', meta_description='Admin area.', canonical_path=f'/admin/matches/{match_id}', meta_robots='noindex,nofollow')
        context.update(
            {
                'match': None,
                'form_state': _build_admin_match_form_state(),
                'error_message': 'Match not found.',
                'success_message': None,
                'race_options': RACE_LABELS,
                'game_type_options': GAME_TYPE_OPTIONS,
                'mission_options': _merge_mission_options(),
            }
        )
        return make_response(render_template('admin_edit_match.html', **context), 404)

    context = _base_context(f'Edit Match – #{match_id}', 'admin', meta_description='Admin area.', canonical_path=f'/admin/matches/{match_id}', meta_robots='noindex,nofollow')
    context.update(
        {
            'match': match,
            'form_state': _build_admin_match_form_state(match),
            'error_message': None,
            'success_message': 'Match saved and ELO recalculated.' if request.args.get('saved') == '1' else None,
            'race_options': RACE_LABELS,
            'game_type_options': GAME_TYPE_OPTIONS,
            'mission_options': _merge_mission_options(),
        }
    )
    return render_template('admin_edit_match.html', **context)


@app.route('/admin/matches/<int:match_id>', methods=['POST'])
def admin_edit_match_post(match_id: int):
    if not _is_admin():
        return _redirect_to_admin_login()

    form_state = {key: value for key, value in request.form.items()}
    action = str(form_state.get('action', 'save')).strip() or 'save'

    if action == 'delete':
        try:
            delete_match_admin(match_id)
        except Exception as exc:
            existing_match = fetch_match_admin(match_id)
            context = _base_context('Edit Match', 'admin', meta_description='Admin area.', canonical_path=f'/admin/matches/{match_id}', meta_robots='noindex,nofollow')
            context.update(
                {
                    'match': existing_match,
                    'form_state': _build_admin_match_form_state(existing_match, form_state),
                    'error_message': str(exc),
                    'success_message': None,
                    'race_options': RACE_LABELS,
                    'game_type_options': GAME_TYPE_OPTIONS,
                    'mission_options': _merge_mission_options(),
                }
            )
            return make_response(render_template('admin_edit_match.html', **context), 400)
        return redirect('/reports', code=303)

    try:
        played_at = _parse_admin_datetime(form_state.get('played_at', ''))
        match = update_match_admin(
            match_id=match_id,
            player1_name=form_state.get('player1_name', ''),
            player2_name=form_state.get('player2_name', ''),
            winner_side=form_state.get('winner_side', ''),
            player1_race=form_state.get('player1_race', ''),
            player2_race=form_state.get('player2_race', ''),
            is_ranked=form_state.get('is_ranked', 'yes'),
            game_type=form_state.get('game_type', ''),
            mission_name=form_state.get('mission_name', ''),
            player1_score=form_state.get('player1_score', ''),
            player2_score=form_state.get('player2_score', ''),
            comment=form_state.get('comment', ''),
            played_at=played_at,
        )
    except Exception as exc:
        existing_match = fetch_match_admin(match_id)
        context = _base_context('Edit Match', 'admin', meta_description='Admin area.', canonical_path=f'/admin/matches/{match_id}', meta_robots='noindex,nofollow')
        context.update(
            {
                'match': existing_match,
                'form_state': _build_admin_match_form_state(existing_match, form_state),
                'error_message': str(exc),
                'success_message': None,
                'race_options': RACE_LABELS,
                'game_type_options': GAME_TYPE_OPTIONS,
                'mission_options': _merge_mission_options(),
            }
        )
        return make_response(render_template('admin_edit_match.html', **context), 400)

    return redirect(f'/admin/matches/{match["id"]}?saved=1', code=303)

@app.route('/google35d0caf8d54d36f2.html')
def google_site_verification():
    return send_from_directory(app.static_folder, 'google35d0caf8d54d36f2.html', mimetype='text/html')

application = app


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8000, debug=True)
