import base64
import hashlib
import hmac
import json
import os
import threading
from datetime import datetime, timedelta, timezone
from html import escape
from io import BytesIO
from pathlib import Path
from urllib.parse import quote

from dotenv import load_dotenv
from flask import Flask, Response, abort, jsonify, make_response, redirect, render_template, request, send_file, \
    send_from_directory

from app.database import (
    MatchSubmissionRateLimitError,
    delete_admin_feedback_message,
    delete_match_admin,
    decorate_players_with_current_league_awards,
    fetch_admin_feedback_messages,
    fetch_current_league,
    fetch_game_reports_page,
    fetch_league_results_overview,
    fetch_leaderboard,
    fetch_match_admin,
    fetch_mission_suggestions,
    fetch_player_admin,
    fetch_player_name_suggestions,
    fetch_player_profile,
    ping_database,
    refresh_application_cache,
    submit_admin_feedback_message,
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
ADMIN_MATCH_RACE_OPTIONS = ['Terran', 'Protoss', 'Zerg']

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
FEEDBACK_MESSAGE_MAX_LENGTH = 300

CACHE_WARMUP_ON_STARTUP = (os.getenv('APP_WARMUP_ON_STARTUP') or '1').strip().lower() not in {'0', 'false', 'no', 'off'}
CACHE_REFRESH_BACKGROUND = (os.getenv('APP_CACHE_REFRESH_BACKGROUND') or '1').strip().lower() not in {'0', 'false', 'no', 'off'}


def _get_supabase_webhook_secret() -> str:
    return (os.getenv('SUPABASE_WEBHOOK_SECRET') or '').strip()


def _is_valid_supabase_webhook_request() -> bool:
    secret = _get_supabase_webhook_secret()
    if not secret:
        return False

    header_secret = (request.headers.get('X-Webhook-Secret') or '').strip()
    auth_header = (request.headers.get('Authorization') or '').strip()

    expected_bearer = f'Bearer {secret}'
    return hmac.compare_digest(header_secret, secret) or hmac.compare_digest(auth_header, expected_bearer)


def _run_cache_refresh(reason: str = 'manual') -> dict:
    started_at = datetime.now(timezone.utc).isoformat()
    result = refresh_application_cache(force_refresh=True)
    result['reason'] = reason
    result['started_at'] = started_at
    result['finished_at'] = datetime.now(timezone.utc).isoformat()
    return result


def _run_cache_refresh_background(reason: str) -> None:
    def worker() -> None:
        try:
            _run_cache_refresh(reason)
        except Exception:
            app.logger.exception('Cache refresh failed: %s', reason)

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()


def _warmup_cache_on_startup() -> None:
    if not CACHE_WARMUP_ON_STARTUP:
        return
    _run_cache_refresh_background('startup')


def _get_site_url() -> str:
    raw_value = (os.getenv('SITE_URL') or 'https://tmg-stats.org').strip() or 'https://tmg-stats.org'
    return raw_value.rstrip('/')


def _get_tts_submit_token() -> str:
    return (os.getenv('TTS_SUBMIT_TOKEN') or '').strip()


def _get_beta_roster_site_url() -> str:
    return (os.getenv(
        'BETA_ROSTER_SITE_URL') or 'https://starcrafttmgbeta.web.app/').strip() or 'https://starcrafttmgbeta.web.app/'


def _get_beta_roster_pdf_url_template() -> str:
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
    template = _get_beta_roster_pdf_url_template()
    if not template:
        return None
    return template.format(roster_id=quote(clean_roster_id, safe=''))


def _download_roster_pdf_via_browser(roster_id: str) -> tuple[bytes, str]:
    clean_roster_id = _clean_roster_id(roster_id)
    if not clean_roster_id:
        raise ValueError('Roster ID is empty.')

    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        raise RuntimeError(
            'Playwright is not installed. Run: pip install playwright and then playwright install chromium'
        ) from exc

    beta_url = _get_beta_roster_site_url()
    load_selectors = [
        'button:has(i.fa-solid.fa-cloud-arrow-down)',
        'button:has(i.fa-cloud-arrow-down)',
        'button:has(.fa-cloud-arrow-down)',
    ]
    pdf_selectors = [
        'button.ab-add-btn:has-text("PDF")',
        'button:has-text("PDF")',
        '[role="button"]:has-text("PDF")',
    ]

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-dev-shm-usage'],
        )
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        try:
            page.goto(beta_url, wait_until='domcontentloaded', timeout=45000)
            seed_input = page.locator('#ab-seed-input')
            seed_input.wait_for(state='visible', timeout=20000)
            seed_input.fill(clean_roster_id)

            load_clicked = False
            for selector in load_selectors:
                locator = page.locator(selector)
                if locator.count() > 0:
                    locator.first.click(timeout=10000)
                    load_clicked = True
                    break
            if not load_clicked:
                try:
                    seed_input.press('Enter')
                except Exception:
                    pass

            page.wait_for_timeout(1200)

            pdf_button = None
            for selector in pdf_selectors:
                locator = page.locator(selector)
                if locator.count() > 0:
                    pdf_button = locator.first
                    break
            if pdf_button is None:
                raise RuntimeError('Could not find the PDF button on the beta roster site.')

            try:
                pdf_button.wait_for(state='visible', timeout=20000)
            except PlaywrightTimeoutError as exc:
                raise RuntimeError('The PDF button did not become available in time.') from exc

            with page.expect_download(timeout=30000) as download_info:
                pdf_button.click(timeout=10000)
            download = download_info.value
            pdf_bytes = download.path().read_bytes()
            filename = download.suggested_filename or f'roster-{clean_roster_id}.pdf'
            return pdf_bytes, filename
        finally:
            context.close()
            browser.close()


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
    return 'TMG Stats is the ELO rating site for the StarCraft TMG community with player profiles, match reports and rankings.'


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
        'player1_roster_id': str(source.get('player1_roster_id', '')).strip(),
        'player2_roster_id': str(source.get('player2_roster_id', '')).strip(),
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
        'Submit Match – TMG Stats',
        'submit',
        meta_description='Submit a StarCraft TMG match result to the TMG Stats community rating site.',
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


def _normalize_admin_race_option(value: str | None, default: str = 'Terran') -> str:
    clean_value = str(value or '').strip()
    mapping = {
        'Терран': 'Terran',
        'Протосс': 'Protoss',
        'Зерг': 'Zerg',
        'Terran': 'Terran',
        'Protoss': 'Protoss',
        'Zerg': 'Zerg',
    }
    normalized = mapping.get(clean_value, clean_value)
    if normalized in ADMIN_MATCH_RACE_OPTIONS:
        return normalized
    return default


def _build_feedback_form_state(raw_values: dict | None = None) -> dict:
    source = raw_values or {}
    return {
        'player_name': str(source.get('player_name', '')).strip(),
        'message_text': str(source.get('message_text', '')).strip(),
    }


def _client_ip_address() -> str:
    forwarded_for = str(request.headers.get('X-Forwarded-For', '')).strip()
    if forwarded_for:
        return forwarded_for.split(',', 1)[0].strip()
    return str(request.remote_addr or '').strip()


def _render_feedback_page(
        *,
        form_state: dict | None = None,
        error_message: str | None = None,
        success_message: str | None = None,
        status_code: int = 200,
):
    name_suggestions: list[str] = []
    feedback_messages: list[dict] = []
    feedback_load_error: str | None = None

    try:
        name_suggestions = fetch_player_name_suggestions(limit=SUBMIT_NAME_SUGGESTION_LIMIT)
    except Exception:
        name_suggestions = []

    if _is_admin():
        try:
            feedback_messages = fetch_admin_feedback_messages(limit=200)
        except Exception as exc:
            feedback_load_error = str(exc)

    context = _base_context(
        'Contact Admin – TMG Stats',
        'feedback',
        meta_description='Leave a short message for the TMG Stats admin team.',
        canonical_path='/feedback',
        meta_robots='noindex,follow',
    )
    context.update(
        {
            'form_state': _build_feedback_form_state(form_state),
            'error_message': error_message,
            'success_message': success_message,
            'feedback_messages': feedback_messages,
            'feedback_load_error': feedback_load_error,
            'name_suggestions': name_suggestions,
            'feedback_message_max_length': FEEDBACK_MESSAGE_MAX_LENGTH,
        }
    )
    return make_response(render_template('feedback.html', **context), status_code)


def _build_admin_match_form_state(match: dict | None = None, source: dict | None = None) -> dict:
    match = match or {}
    source = source or {}
    return {
        'player1_name': str(source.get('player1_name', match.get('player1_name', ''))).strip(),
        'player2_name': str(source.get('player2_name', match.get('player2_name', ''))).strip(),
        'winner_side': str(source.get('winner_side', match.get('winner_side', 'player1'))).strip() or 'player1',
        'player1_race': _normalize_admin_race_option(source.get('player1_race', match.get('player1_race', 'Terran')),
                                                     'Terran'),
        'player2_race': _normalize_admin_race_option(source.get('player2_race', match.get('player2_race', 'Protoss')),
                                                     'Protoss'),
        'is_ranked': str(source.get('is_ranked', 'yes' if match.get('is_ranked', True) else 'no')).strip() or 'yes',
        'game_type': str(source.get('game_type', match.get('game_type', '1к'))).strip() or '1к',
        'mission_name': str(source.get('mission_name', match.get('mission_name', ''))).strip(),
        'player1_score': str(source.get('player1_score', match.get('player1_score', ''))).strip(),
        'player2_score': str(source.get('player2_score', match.get('player2_score', ''))).strip(),
        'player1_roster_id': str(source.get('player1_roster_id', match.get('player1_roster_id', ''))).strip(),
        'player2_roster_id': str(source.get('player2_roster_id', match.get('player2_roster_id', ''))).strip(),
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


def _serialize_leaderboard_players(players: list[dict]) -> list[dict]:
    serialized: list[dict] = []

    for player in players:
        serialized.append(
            {
                'id': int(player['id']),
                'name': str(player.get('name') or ''),
                'rank_position': int(player.get('rank_position') or 0),
                'current_elo': int(player.get('current_elo') or 0),
                'current_elo_display': str(player.get('current_elo_display') or player.get('current_elo') or ''),
                'matches_count': int(player.get('matches_count') or 0),
                'win_rate': player.get('win_rate') or 0,
                'win_rate_display': str(player.get('win_rate_display') or ''),
                'wins': int(player.get('wins') or 0),
                'losses': int(player.get('losses') or 0),
                'profile_url': str(player.get('profile_url') or f"/players/{player['id']}"),
                'flag_url': str(player.get('flag_url') or ''),
                'priority_race': str(player.get('priority_race') or ''),
                'priority_race_slug': str(player.get('priority_race') or '').strip().lower(),
                'is_current_league_best_overall': bool(player.get('is_current_league_best_overall')),
                'is_current_league_most_active': bool(player.get('is_current_league_most_active')),
                'award_name_class': str(player.get('award_name_class') or ''),
            }
        )

    return serialized


def _parse_leaderboard_filters() -> tuple[str, bool, bool, bool, bool]:
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

    active_ranked_raw = request.args.get('active_ranked')
    show_active_ranked = str(active_ranked_raw or '').strip().lower() in {'1', 'true', 'yes', 'on'}
    active_ranked_query_present = active_ranked_raw is not None

    return search, show_active, show_inactive, show_active_ranked, active_ranked_query_present


def _leaderboard_payload(
        *,
        search: str,
        show_active: bool,
        show_inactive: bool,
        show_active_ranked: bool,
) -> tuple[dict, str | None]:
    try:
        players = fetch_leaderboard(
            search=search,
            include_active=show_active,
            include_inactive=show_inactive,
            active_ranked_only=show_active_ranked,
        )
        players = decorate_players_with_current_league_awards(players)
        return {
            'ok': True,
            'players': _serialize_leaderboard_players(players),
            'players_count': len(players),
            'show_active': show_active,
            'show_inactive': show_inactive,
            'show_active_ranked': show_active_ranked,
            'search': search,
        }, None
    except Exception as exc:
        return {
            'ok': False,
            'players': [],
            'players_count': 0,
            'show_active': show_active,
            'show_inactive': show_inactive,
            'show_active_ranked': show_active_ranked,
            'search': search,
            'error': str(exc),
        }, str(exc)


def _serialize_game_reports(matches: list[dict]) -> list[dict]:
    serialized: list[dict] = []

    for match in matches:
        serialized.append(
            {
                'id': int(match.get('id') or 0),
                'played_at_label': str(match.get('played_at_label') or ''),
                'winner_name': str(match.get('winner_name') or ''),
                'loser_name': str(match.get('loser_name') or ''),
                'winner_race': str(match.get('winner_race') or ''),
                'loser_race': str(match.get('loser_race') or ''),
                'winner_profile_url': str(match.get('winner_profile_url') or ''),
                'loser_profile_url': str(match.get('loser_profile_url') or ''),
                'ranked_label': str(match.get('ranked_label') or ''),
                'game_type_display': str(match.get('game_type_display') or ''),
                'score_display': str(match.get('score_display') or ''),
                'mission_name_display': str(match.get('mission_name_display') or ''),
                'comment_display': str(match.get('comment_display') or ''),
                'player1_roster_id': str(match.get('player1_roster_id') or ''),
                'player2_roster_id': str(match.get('player2_roster_id') or ''),
                'winner_roster_id': str(match.get('winner_roster_id') or ''),
                'loser_roster_id': str(match.get('loser_roster_id') or ''),
                'is_ranked': bool(match.get('is_ranked')),
                'is_tie': bool(match.get('is_tie')),
                'league_id': int(match.get('league_id') or 0),
                'league_name': str(match.get('league_name') or ''),
            }
        )

    return serialized


def _reports_payload(
        *,
        search: str,
        current_page: int,
        per_page: int,
        show_ranked_only: bool,
) -> tuple[dict, str | None]:
    try:
        page_data = fetch_game_reports_page(
            search=search,
            page=current_page,
            per_page=per_page,
            ranked_only=show_ranked_only,
        )
        matches = _serialize_game_reports(page_data['items'])
        total_matches = int(page_data['total_count'])
        resolved_page = int(page_data['page'])
        total_pages = int(page_data['total_pages'])
        return {
            'ok': True,
            'matches': matches,
            'search': search,
            'per_page': int(page_data['per_page']),
            'total_matches': total_matches,
            'current_page': resolved_page,
            'total_pages': total_pages,
            'pagination_numbers': _pagination_numbers(resolved_page, total_pages),
            'show_ranked_only': show_ranked_only,
            'current_league': page_data.get('current_league'),
        }, None
    except Exception as exc:
        return {
            'ok': False,
            'matches': [],
            'search': search,
            'per_page': per_page,
            'total_matches': 0,
            'current_page': current_page,
            'total_pages': 1,
            'pagination_numbers': [],
            'show_ranked_only': show_ranked_only,
            'current_league': None,
            'error': str(exc),
        }, str(exc)


def _pagination_numbers(current_page: int, total_pages: int) -> list[int | str]:
    if total_pages <= 7:
        return list(range(1, total_pages + 1))
    if current_page <= 4:
        return [1, 2, 3, 4, '...', total_pages]
    if current_page >= total_pages - 3:
        return [1, '...', total_pages - 3, total_pages - 2, total_pages - 1, total_pages]
    return [1, '...', current_page - 1, current_page, current_page + 1, '...', total_pages]


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
        (_build_absolute_url('/'), lastmod),
        (_build_absolute_url('/leaderboard'), lastmod),
        (_build_absolute_url('/reports'), lastmod),
        (_build_absolute_url('/submit'), lastmod),
        (_build_absolute_url('/leagues'), lastmod),
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
        entries.append((_build_absolute_url(f'/players/{player_id}'), None))
    return entries


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
    lastmod = _today_lastmod()
    entries = [
        (_build_absolute_url('/sitemap-pages.xml'), lastmod),
        (_build_absolute_url('/sitemap-players.xml'), lastmod),
    ]
    return _render_sitemap_index(entries)


@app.route('/sitemap-pages.xml')
def sitemap_pages_xml():
    return _render_sitemap_urlset(_build_pages_sitemap_entries())


@app.route('/sitemap-players.xml')
def sitemap_players_xml():
    return _render_sitemap_urlset(_build_player_sitemap_entries())


@app.route('/')
def home():
    context = _base_context(
        'TMG Stats – StarCraft TMG Community Ratings',
        'home',
        meta_description='TMG Stats is the ELO rating site for the StarCraft TMG community with player profiles, match reports and rankings.',
        canonical_path='/',
    )
    return render_template('home.html', **context)


@app.route('/health')
def health():
    ok, error = ping_database()
    return jsonify({'status': 'ok' if ok else 'error', 'database_error': error})


@app.route('/admin/cache/refresh', methods=['POST'])
def admin_cache_refresh():
    if not _is_admin():
        return jsonify({'ok': False, 'error': 'Unauthorized'}), 401

    try:
        result = _run_cache_refresh('admin')
    except Exception as exc:
        return jsonify({'ok': False, 'error': str(exc)}), 500

    return jsonify({'ok': True, 'cache': result})


@app.route('/api/supabase/cache-webhook', methods=['POST'])
def supabase_cache_webhook():
    if not _is_valid_supabase_webhook_request():
        return jsonify({'ok': False, 'error': 'Unauthorized'}), 401

    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        payload = {}

    event_type = str(payload.get('type') or payload.get('event') or payload.get('record') or 'db_change')
    table_name = str(payload.get('table') or payload.get('table_name') or '')

    reason_parts = ['supabase_webhook']
    if table_name:
        reason_parts.append(table_name)
    if event_type:
        reason_parts.append(event_type)

    reason = ':'.join(reason_parts)

    if CACHE_REFRESH_BACKGROUND:
        _run_cache_refresh_background(reason)
        return jsonify({'ok': True, 'refresh': 'scheduled', 'reason': reason})

    try:
        result = _run_cache_refresh(reason)
    except Exception as exc:
        return jsonify({'ok': False, 'error': str(exc)}), 500

    return jsonify({'ok': True, 'refresh': 'completed', 'cache': result})


@app.route('/leaderboard')
def leaderboard():
    search, show_active, show_inactive, show_active_ranked, active_ranked_query_present = _parse_leaderboard_filters()

    is_filtered_page = bool(str(search).strip()) or not (show_active and not show_inactive) or show_active_ranked
    context = _base_context(
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


@app.route('/api/leaderboard', methods=['GET'])
def leaderboard_api():
    search, show_active, show_inactive, show_active_ranked, _ = _parse_leaderboard_filters()
    payload, db_error = _leaderboard_payload(
        search=search,
        show_active=show_active,
        show_inactive=show_inactive,
        show_active_ranked=show_active_ranked,
    )
    status_code = 200 if payload.get('ok') else 500
    if db_error:
        payload['error'] = db_error
    return jsonify(payload), status_code


@app.route('/api/reports', methods=['GET'])
def reports_api():
    current_page = max(1, request.args.get('page', 1, type=int) or 1)
    per_page = max(1, min(request.args.get('per_page', 25, type=int) or 25, 100))
    search = request.args.get('search', '')
    show_ranked_only = request.args.get('ranked_only') == '1'
    payload, db_error = _reports_payload(
        search=search,
        current_page=current_page,
        per_page=per_page,
        show_ranked_only=show_ranked_only,
    )
    status_code = 200 if payload.get('ok') else 500
    if db_error:
        payload['error'] = db_error
    return jsonify(payload), status_code


@app.route('/roster-pdf/<string:roster_id>')
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


@app.route('/reports')
@app.route('/players')
def game_reports():
    if request.path == '/players':
        return redirect('/reports', code=301)

    current_page = max(1, request.args.get('page', 1, type=int) or 1)
    per_page = max(1, min(request.args.get('per_page', 25, type=int) or 25, 100))
    search = request.args.get('search', '')
    show_ranked_only = request.args.get('ranked_only') == '1'
    ranked_only_query_present = 'ranked_only' in request.args

    is_filtered_page = bool(str(search).strip()) or current_page > 1 or per_page != 25 or show_ranked_only
    context = _base_context(
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
            'Player Profile – TMG Stats',
            'players',
            meta_description='StarCraft TMG player profile with rating history and recent match results.',
            canonical_path=f'/players/{player_id}',
        )
        context.update({'player': None, 'recent_matches': [], 'rating_chart': None, 'priority_matchup_report': None,
                        'db_error': db_error})
        return make_response(render_template('player_profile.html', **context), 500)

    if not profile:
        context = _base_context(
            'Player Not Found – TMG Stats',
            'players',
            meta_description='StarCraft TMG player profile page.',
            canonical_path=f'/players/{player_id}',
            meta_robots='noindex,follow',
        )
        context.update({'player': None, 'recent_matches': [], 'rating_chart': None, 'priority_matchup_report': None,
                        'db_error': None})
        return make_response(render_template('player_profile.html', **context), 404)

    player_name = profile['player']['name']
    priority_race = str(profile['player'].get('priority_race') or '').strip()
    meta_description = f"{player_name} player profile on TMG Stats with current rating, recent matches and rating history."
    if priority_race:
        meta_description = f"{player_name} {priority_race} player profile on TMG Stats with current rating, recent matches and rating history."

    context = _base_context(
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
            'db_error': None,
        }
    )
    return render_template('player_profile.html', **context)


@app.route('/api/leagues', methods=['GET'])
def league_results_api():
    try:
        league_sections = fetch_league_results_overview()
        return jsonify({'ok': True, 'league_sections': league_sections})
    except Exception as exc:
        return jsonify({'ok': False, 'league_sections': [], 'error': str(exc)}), 500


@app.route('/leagues')
def league_results_page():
    context = _base_context(
        'League Results – TMG Stats',
        'leagues',
        meta_description='Current and previous TMG Stats league results with champions, most active players and race leaders.',
        canonical_path='/leagues',
    )
    return render_template('league_results.html', **context)


@app.route('/roster-pdf-test', methods=['GET'])
def roster_pdf_test_page():
    context = _base_context(
        'Roster PDF Test – TMG Stats',
        'reports',
        meta_description='Client-side test page for generating roster PDFs from StarCraft TMG beta shared_rosters.',
        canonical_path='/roster-pdf-test',
        meta_robots='noindex,nofollow',
    )
    return render_template('roster_pdf_test.html', **context)


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
            player1_roster_id=form_state.get('player1_roster_id', ''),
            player2_roster_id=form_state.get('player2_roster_id', ''),
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

    first_player_roster_id = str(
        payload.get('first_player_roster_id')
        or payload.get('winner_roster_id')
        or payload.get('player1_roster_id')
        or ''
    ).strip()

    second_player_roster_id = str(
        payload.get('second_player_roster_id')
        or payload.get('opponent_roster_id')
        or payload.get('player2_roster_id')
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
            player1_roster_id=first_player_roster_id,
            player2_roster_id=second_player_roster_id,
            comment='',
        )
    except MatchSubmissionRateLimitError as exc:
        return jsonify({'ok': False, 'message': str(exc)}), 429
    except Exception as exc:
        return jsonify({'ok': False, 'message': str(exc)}), 400

    return jsonify({'ok': True, 'message': 'Match submitted successfully.', 'data': success_data}), 200


@app.route('/feedback', methods=['GET'])
def feedback_page():
    success_message = None
    if request.args.get('sent') == '1':
        success_message = 'Your message has been sent to the admin.'
    elif request.args.get('deleted') == '1':
        success_message = 'Message deleted.'
    return _render_feedback_page(success_message=success_message)


@app.route('/feedback', methods=['POST'])
def feedback_page_post():
    form_state = {key: value for key, value in request.form.items()}
    action = str(form_state.get('action', 'send_message')).strip() or 'send_message'

    if action == 'delete_message':
        if not _is_admin():
            return _render_feedback_page(error_message='Only admin can delete messages.', status_code=403)

        try:
            delete_admin_feedback_message(form_state.get('message_id', ''))
        except Exception as exc:
            return _render_feedback_page(error_message=str(exc), status_code=400)

        return redirect('/feedback?deleted=1', code=303)

    try:
        submit_admin_feedback_message(
            player_name=form_state.get('player_name', ''),
            message_text=form_state.get('message_text', ''),
            ip_address=_client_ip_address(),
        )
    except Exception as exc:
        return _render_feedback_page(form_state=form_state, error_message=str(exc), status_code=400)

    return redirect('/feedback?sent=1', code=303)


@app.route('/admin', methods=['GET'])
def admin():
    context = _base_context('Admin Panel', 'admin', meta_description='Admin area.', canonical_path='/admin',
                            meta_robots='noindex,nofollow')
    context.update({'login_error': None, 'admin_login_default': _get_admin_login()})
    template_name = 'admin_dashboard.html' if context['is_admin'] else 'admin_login.html'
    return render_template(template_name, **context)


@app.route('/admin/login', methods=['POST'])
def admin_login():
    login = str(request.form.get('login', '')).strip()
    password = str(request.form.get('password', '')).strip()

    if login != _get_admin_login() or password != _get_admin_password():
        context = _base_context('Admin Panel', 'admin', meta_description='Admin area.', canonical_path='/admin',
                                meta_robots='noindex,nofollow')
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
        context = _base_context('Player Not Found', 'admin', meta_description='Admin area.',
                                canonical_path=f'/admin/players/{player_id}', meta_robots='noindex,nofollow')
        context.update(
            {
                'player': None,
                'form_state': _build_admin_player_form_state(),
                'error_message': 'Player not found.',
                'success_message': None,
                'race_options': ADMIN_MATCH_RACE_OPTIONS,
            }
        )
        return make_response(render_template('admin_edit_player.html', **context), 404)

    context = _base_context(f"Edit Player – {player['name']}", 'admin', meta_description='Admin area.',
                            canonical_path=f'/admin/players/{player_id}', meta_robots='noindex,nofollow')
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
        context = _base_context('Edit Player', 'admin', meta_description='Admin area.',
                                canonical_path=f'/admin/players/{player_id}', meta_robots='noindex,nofollow')
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
        context = _base_context('Match Not Found', 'admin', meta_description='Admin area.',
                                canonical_path=f'/admin/matches/{match_id}', meta_robots='noindex,nofollow')
        context.update(
            {
                'match': None,
                'form_state': _build_admin_match_form_state(),
                'error_message': 'Match not found.',
                'success_message': None,
                'race_options': ADMIN_MATCH_RACE_OPTIONS,
                'game_type_options': GAME_TYPE_OPTIONS,
                'mission_options': _merge_mission_options(),
            }
        )
        return make_response(render_template('admin_edit_match.html', **context), 404)

    context = _base_context(f'Edit Match – #{match_id}', 'admin', meta_description='Admin area.',
                            canonical_path=f'/admin/matches/{match_id}', meta_robots='noindex,nofollow')
    context.update(
        {
            'match': match,
            'form_state': _build_admin_match_form_state(match),
            'error_message': None,
            'success_message': 'Match saved and ELO recalculated.' if request.args.get('saved') == '1' else None,
            'race_options': ADMIN_MATCH_RACE_OPTIONS,
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
            context = _base_context('Edit Match', 'admin', meta_description='Admin area.',
                                    canonical_path=f'/admin/matches/{match_id}', meta_robots='noindex,nofollow')
            context.update(
                {
                    'match': existing_match,
                    'form_state': _build_admin_match_form_state(existing_match, form_state),
                    'error_message': str(exc),
                    'success_message': None,
                    'race_options': ADMIN_MATCH_RACE_OPTIONS,
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
            player1_roster_id=form_state.get('player1_roster_id', ''),
            player2_roster_id=form_state.get('player2_roster_id', ''),
            comment=form_state.get('comment', ''),
            played_at=played_at,
        )
    except Exception as exc:
        existing_match = fetch_match_admin(match_id)
        context = _base_context('Edit Match', 'admin', meta_description='Admin area.',
                                canonical_path=f'/admin/matches/{match_id}', meta_robots='noindex,nofollow')
        context.update(
            {
                'match': existing_match,
                'form_state': _build_admin_match_form_state(existing_match, form_state),
                'error_message': str(exc),
                'success_message': None,
                'race_options': ADMIN_MATCH_RACE_OPTIONS,
                'game_type_options': GAME_TYPE_OPTIONS,
                'mission_options': _merge_mission_options(),
            }
        )
        return make_response(render_template('admin_edit_match.html', **context), 400)

    return redirect(f'/admin/matches/{match["id"]}?saved=1', code=303)


@app.route('/google35d0caf8d54d36f2.html')
def google_site_verification():
    return send_from_directory(app.static_folder, 'google35d0caf8d54d36f2.html', mimetype='text/html')

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8000, debug=True)
