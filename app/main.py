import base64
import hashlib
import hmac
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qs

from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.database import (
    delete_match_admin,
    fetch_game_reports,
    fetch_game_reports_page,
    fetch_leaderboard,
    fetch_match_admin,
    fetch_mission_suggestions,
    fetch_player_admin,
    fetch_player_name_suggestions,
    fetch_player_profile,
    ping_database,
    submit_match_result,
    update_match_admin,
    update_player_admin,
)

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent

load_dotenv(PROJECT_ROOT / '.env')

app = FastAPI(title='StarCraft ELO', version='0.1.0')

app.mount('/static', StaticFiles(directory=BASE_DIR / 'static'), name='static')
templates = Jinja2Templates(directory=str(BASE_DIR / 'templates'))

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


def _apply_fast_page_headers(response):
    response.headers['Cache-Control'] = 'private, max-age=5'
    response.headers['Vary'] = 'Cookie'
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


def _is_admin(request: Request) -> bool:
    payload = _read_admin_cookie(request.cookies.get(ADMIN_COOKIE_NAME))
    if not payload:
        return False
    return payload.get('login') == _get_admin_login()


def _base_context(request: Request, page_title: str, active_page: str) -> dict:
    return {
        'request': request,
        'page_title': page_title,
        'active_page': active_page,
        'is_admin': _is_admin(request),
    }


def _redirect_to_admin_login() -> RedirectResponse:
    return RedirectResponse('/admin', status_code=303)


def _merge_mission_options() -> list[str]:
    try:
        db_values = fetch_mission_suggestions(limit=100)
    except Exception:
        db_values = []

    merged = []
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
        'is_ranked': str(source.get('is_ranked', 'yes')).strip() or 'yes',
        'game_type': str(source.get('game_type', '1к')).strip() or '1к',
        'mission_name': str(source.get('mission_name', '')).strip(),
        'comment': str(source.get('comment', '')).strip(),
    }


def _parse_form(request_body: bytes) -> dict[str, str]:
    raw_body = request_body.decode('utf-8')
    parsed_body = parse_qs(raw_body, keep_blank_values=True)
    return {key: values[-1] if values else '' for key, values in parsed_body.items()}


def _render_submit_page(
    request: Request,
    *,
    form_state: dict | None = None,
    error_message: str | None = None,
    success_data: dict | None = None,
    status_code: int = 200,
):
    name_suggestions = []
    try:
        name_suggestions = fetch_player_name_suggestions(limit=SUBMIT_NAME_SUGGESTION_LIMIT)
    except Exception:
        name_suggestions = []

    context = _base_context(request, 'Submit Match', 'submit')
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

    return _apply_fast_page_headers(templates.TemplateResponse('submit_match.html', context, status_code=status_code))


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


@app.get('/', response_class=HTMLResponse)
async def home(request: Request):
    context = _base_context(request, 'StarCraft ELO', 'home')
    return _apply_fast_page_headers(templates.TemplateResponse('home.html', context))


@app.get('/health')
async def health():
    ok, error = ping_database()
    return {'status': 'ok' if ok else 'error', 'database_error': error}


@app.get('/leaderboard', response_class=HTMLResponse)
async def leaderboard(request: Request, search: str = ''):
    db_error = None
    players = []

    selected_statuses = {
        value.strip().lower()
        for value in request.query_params.getlist('status')
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

    context = _base_context(request, 'Global Rating', 'leaderboard')
    context.update(
        {
            'players': players,
            'search': search,
            'show_active': show_active,
            'show_inactive': show_inactive,
            'db_error': db_error,
        }
    )
    return _apply_fast_page_headers(templates.TemplateResponse('leaderboard.html', context))


@app.get('/reports', response_class=HTMLResponse)
@app.get('/players', response_class=HTMLResponse)
async def game_reports(request: Request, search: str = '', page: int = 1, per_page: int = 100):
    db_error = None
    matches = []
    total_matches = 0
    current_page = 1
    total_pages = 1
    pagination_numbers = []

    try:
        page_data = fetch_game_reports_page(search=search, page=page, per_page=per_page)
        matches = page_data['items']
        total_matches = page_data['total_count']
        current_page = page_data['page']
        total_pages = page_data['total_pages']

        if total_pages <= 7:
            pagination_numbers = list(range(1, total_pages + 1))
        else:
            if current_page <= 4:
                pagination_numbers = [1, 2, 3, 4, '...', total_pages]
            elif current_page >= total_pages - 3:
                pagination_numbers = [1, '...', total_pages - 3, total_pages - 2, total_pages - 1, total_pages]
            else:
                pagination_numbers = [1, '...', current_page - 1, current_page, current_page + 1, '...', total_pages]
    except Exception as exc:
        db_error = str(exc)

    context = _base_context(request, 'Game Reports', 'reports')
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
    return _apply_fast_page_headers(templates.TemplateResponse('game_reports.html', context))


@app.get('/players/{player_id}', response_class=HTMLResponse)
async def player_profile(request: Request, player_id: int):
    db_error = None
    profile = None

    try:
        profile = fetch_player_profile(player_id)
    except Exception as exc:
        db_error = str(exc)

    if db_error:
        context = _base_context(request, 'Player Profile', 'players')
        context.update({'player': None, 'recent_matches': [], 'rating_chart': None, 'db_error': db_error})
        return _apply_fast_page_headers(templates.TemplateResponse('player_profile.html', context, status_code=500))

    if not profile:
        context = _base_context(request, 'Player Not Found', 'players')
        context.update({'player': None, 'recent_matches': [], 'rating_chart': None, 'db_error': None})
        return _apply_fast_page_headers(templates.TemplateResponse('player_profile.html', context, status_code=404))

    context = _base_context(request, f"{profile['player']['name']} – Player Profile", 'players')
    context.update(
        {
            'player': profile['player'],
            'recent_matches': profile['recent_matches'],
            'rating_chart': profile.get('rating_chart'),
            'db_error': None,
        }
    )
    return _apply_fast_page_headers(templates.TemplateResponse('player_profile.html', context))


@app.get('/submit', response_class=HTMLResponse)
async def submit_result(request: Request):
    return _apply_fast_page_headers(_render_submit_page(request))


@app.post('/submit', response_class=HTMLResponse)
async def submit_result_post(request: Request):
    form_state = _parse_form(await request.body())

    try:
        success_data = submit_match_result(
            winner_name=form_state.get('winner_name', ''),
            opponent_name=form_state.get('opponent_name', ''),
            winner_race=form_state.get('winner_race', ''),
            opponent_race=form_state.get('opponent_race', ''),
            is_ranked=form_state.get('is_ranked', 'yes'),
            game_type=form_state.get('game_type', ''),
            mission_name=form_state.get('mission_name', ''),
            comment=form_state.get('comment', ''),
        )
    except Exception as exc:
        return _render_submit_page(request, form_state=form_state, error_message=str(exc), status_code=400)

    return _apply_fast_page_headers(_render_submit_page(request, form_state=None, success_data=success_data, status_code=200))


@app.get('/admin', response_class=HTMLResponse)
async def admin(request: Request):
    context = _base_context(request, 'Admin Panel', 'admin')
    context.update(
        {
            'login_error': None,
            'admin_login_default': _get_admin_login(),
        }
    )
    template_name = 'admin_dashboard.html' if context['is_admin'] else 'admin_login.html'
    return _apply_fast_page_headers(templates.TemplateResponse(template_name, context))


@app.post('/admin/login')
async def admin_login(request: Request):
    form_state = _parse_form(await request.body())
    login = str(form_state.get('login', '')).strip()
    password = str(form_state.get('password', '')).strip()

    if login != _get_admin_login() or password != _get_admin_password():
        context = _base_context(request, 'Admin Panel', 'admin')
        context.update(
            {
                'login_error': 'Wrong login or password.',
                'admin_login_default': login,
            }
        )
        return _apply_fast_page_headers(templates.TemplateResponse('admin_login.html', context, status_code=401))

    response = RedirectResponse('/admin', status_code=303)
    response.set_cookie(
        ADMIN_COOKIE_NAME,
        _build_admin_cookie(login),
        max_age=ADMIN_SESSION_HOURS * 60 * 60,
        httponly=True,
        samesite='lax',
        secure=False,
        path='/',
    )
    return response


@app.post('/admin/logout')
async def admin_logout(request: Request):
    response = RedirectResponse('/admin', status_code=303)
    response.delete_cookie(ADMIN_COOKIE_NAME, path='/')
    return response


@app.get('/admin/players/{player_id}', response_class=HTMLResponse)
async def admin_edit_player(request: Request, player_id: int):
    if not _is_admin(request):
        return _redirect_to_admin_login()

    player = fetch_player_admin(player_id)
    if not player:
        context = _base_context(request, 'Player Not Found', 'admin')
        context.update({'player': None, 'form_state': _build_admin_player_form_state(), 'error_message': 'Player not found.', 'success_message': None, 'race_options': RACE_LABELS})
        return _apply_fast_page_headers(templates.TemplateResponse('admin_edit_player.html', context, status_code=404))

    context = _base_context(request, f"Edit Player – {player['name']}", 'admin')
    context.update(
        {
            'player': player,
            'form_state': _build_admin_player_form_state(player),
            'error_message': None,
            'success_message': 'Player saved.' if request.query_params.get('saved') == '1' else None,
            'race_options': RACE_LABELS,
        }
    )
    return _apply_fast_page_headers(templates.TemplateResponse('admin_edit_player.html', context))


@app.post('/admin/players/{player_id}', response_class=HTMLResponse)
async def admin_edit_player_post(request: Request, player_id: int):
    if not _is_admin(request):
        return _redirect_to_admin_login()

    form_state = _parse_form(await request.body())
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
        context = _base_context(request, 'Edit Player', 'admin')
        context.update(
            {
                'player': existing_player,
                'form_state': _build_admin_player_form_state(existing_player, form_state),
                'error_message': str(exc),
                'success_message': None,
                'race_options': RACE_LABELS,
            }
        )
        return _apply_fast_page_headers(templates.TemplateResponse('admin_edit_player.html', context, status_code=400))

    return RedirectResponse(f'/admin/players/{player["id"]}?saved=1', status_code=303)


@app.get('/admin/matches/{match_id}', response_class=HTMLResponse)
async def admin_edit_match(request: Request, match_id: int):
    if not _is_admin(request):
        return _redirect_to_admin_login()

    match = fetch_match_admin(match_id)
    if not match:
        context = _base_context(request, 'Match Not Found', 'admin')
        context.update({'match': None, 'form_state': _build_admin_match_form_state(), 'error_message': 'Match not found.', 'success_message': None, 'race_options': RACE_LABELS, 'game_type_options': GAME_TYPE_OPTIONS, 'mission_options': _merge_mission_options()})
        return _apply_fast_page_headers(templates.TemplateResponse('admin_edit_match.html', context, status_code=404))

    context = _base_context(request, f'Edit Match – #{match_id}', 'admin')
    context.update(
        {
            'match': match,
            'form_state': _build_admin_match_form_state(match),
            'error_message': None,
            'success_message': 'Match saved and ELO recalculated.' if request.query_params.get('saved') == '1' else None,
            'race_options': RACE_LABELS,
            'game_type_options': GAME_TYPE_OPTIONS,
            'mission_options': _merge_mission_options(),
        }
    )
    return _apply_fast_page_headers(templates.TemplateResponse('admin_edit_match.html', context))


@app.post('/admin/matches/{match_id}', response_class=HTMLResponse)
async def admin_edit_match_post(request: Request, match_id: int):
    if not _is_admin(request):
        return _redirect_to_admin_login()

    form_state = _parse_form(await request.body())
    action = str(form_state.get('action', 'save')).strip() or 'save'

    if action == 'delete':
        try:
            delete_match_admin(match_id)
        except Exception as exc:
            existing_match = fetch_match_admin(match_id)
            context = _base_context(request, 'Edit Match', 'admin')
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
            return _apply_fast_page_headers(templates.TemplateResponse('admin_edit_match.html', context, status_code=400))
        return RedirectResponse('/reports', status_code=303)

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
            comment=form_state.get('comment', ''),
            played_at=played_at,
        )
    except Exception as exc:
        existing_match = fetch_match_admin(match_id)
        context = _base_context(request, 'Edit Match', 'admin')
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
        return _apply_fast_page_headers(templates.TemplateResponse('admin_edit_match.html', context, status_code=400))

    return RedirectResponse(f'/admin/matches/{match["id"]}?saved=1', status_code=303)