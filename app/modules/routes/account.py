from __future__ import annotations

import json
import os
import secrets
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request

from flask import Blueprint, make_response, redirect, render_template, request

from app.database import (
    PROFILE_COLOR_DEFAULT,
    PROFILE_COLOR_OPTIONS,
    fetch_flag_options,
    fetch_player_name_suggestions,
    fetch_user_account,
    get_or_create_user_account_from_google,
    link_user_account_to_player,
    normalize_profile_color_value,
    update_user_discord_profile,
    update_user_profile_settings,
)
from app.modules.auth import build_user_cookie, current_user_session
from app.modules.config import (
    ADMIN_MATCH_RACE_OPTIONS,
    DISCORD_OAUTH_STATE_COOKIE_NAME,
    GOOGLE_OAUTH_STATE_COOKIE_NAME,
    USER_COOKIE_NAME,
    USER_SESSION_DAYS,
)
from app.modules.context import base_context

bp = Blueprint('account', __name__)
OUTBOUND_USER_AGENT = 'TMGStats/1.0 (+https://tmg-stats.org)'


def _google_client_id() -> str:
    return (os.getenv('GOOGLE_CLIENT_ID') or '').strip()


def _google_client_secret() -> str:
    return (os.getenv('GOOGLE_CLIENT_SECRET') or '').strip()


def _google_redirect_uri() -> str:
    configured = (os.getenv('GOOGLE_REDIRECT_URI') or '').strip()
    if configured:
        return configured
    return request.host_url.rstrip('/') + '/auth/google/callback'


def _google_oauth_enabled() -> bool:
    return bool(_google_client_id() and _google_client_secret())


def _discord_client_id() -> str:
    return (os.getenv('DISCORD_CLIENT_ID') or '').strip()


def _discord_client_secret() -> str:
    return (os.getenv('DISCORD_CLIENT_SECRET') or '').strip()


def _discord_redirect_uri() -> str:
    configured = (os.getenv('DISCORD_REDIRECT_URI') or '').strip()
    if configured:
        return configured.rstrip('/')

    site_url = (os.getenv('SITE_URL') or '').strip()
    if site_url:
        return site_url.rstrip('/') + '/auth/discord/callback'

    return request.host_url.rstrip('/') + '/auth/discord/callback'


def _discord_oauth_enabled() -> bool:
    return bool(_discord_client_id() and _discord_client_secret())


def _secure_cookie_enabled() -> bool:
    configured = str(os.getenv('SESSION_COOKIE_SECURE') or '').strip().lower()
    if configured:
        return configured not in {'0', 'false', 'no', 'off'}
    hostname = str(request.host or '').split(':', 1)[0].strip().lower()
    return hostname not in {'localhost', '127.0.0.1', '::1'}


def _oauth_error(message: str):
    return redirect('/login?error=' + urllib_parse.quote(message), code=303)


def _exchange_google_code(code: str) -> dict:
    payload = urllib_parse.urlencode(
        {
            'code': code,
            'client_id': _google_client_id(),
            'client_secret': _google_client_secret(),
            'redirect_uri': _google_redirect_uri(),
            'grant_type': 'authorization_code',
        }
    ).encode('utf-8')
    req = urllib_request.Request(
        'https://oauth2.googleapis.com/token',
        data=payload,
        headers={
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': OUTBOUND_USER_AGENT,
        },
        method='POST',
    )
    try:
        with urllib_request.urlopen(req, timeout=10) as response:
            return json.loads(response.read().decode('utf-8'))
    except urllib_error.HTTPError as exc:
        detail = exc.read().decode('utf-8', errors='ignore')
        raise RuntimeError(detail or 'Google token exchange failed.') from None


def _fetch_google_profile(access_token: str) -> dict:
    req = urllib_request.Request(
        'https://openidconnect.googleapis.com/v1/userinfo',
        headers={
            'Authorization': f'Bearer {access_token}',
            'User-Agent': OUTBOUND_USER_AGENT,
        },
        method='GET',
    )
    try:
        with urllib_request.urlopen(req, timeout=10) as response:
            return json.loads(response.read().decode('utf-8'))
    except urllib_error.HTTPError as exc:
        detail = exc.read().decode('utf-8', errors='ignore')
        raise RuntimeError(detail or 'Could not fetch Google profile.') from None


def _exchange_discord_code(code: str) -> dict:
    payload = urllib_parse.urlencode(
        {
            'client_id': _discord_client_id(),
            'client_secret': _discord_client_secret(),
            'grant_type': 'authorization_code',
            'code': code,
            'redirect_uri': _discord_redirect_uri(),
        }
    ).encode('utf-8')
    req = urllib_request.Request(
        'https://discord.com/api/v10/oauth2/token',
        data=payload,
        headers={
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json',
            'User-Agent': OUTBOUND_USER_AGENT,
        },
        method='POST',
    )
    try:
        with urllib_request.urlopen(req, timeout=10) as response:
            return json.loads(response.read().decode('utf-8'))
    except urllib_error.HTTPError as exc:
        detail = exc.read().decode('utf-8', errors='ignore')
        raise RuntimeError(detail or 'Discord token exchange failed.') from None


def _fetch_discord_profile(access_token: str) -> dict:
    req = urllib_request.Request(
        'https://discord.com/api/v10/users/@me',
        headers={
            'Authorization': f'Bearer {access_token}',
            'Accept': 'application/json',
            'User-Agent': OUTBOUND_USER_AGENT,
        },
        method='GET',
    )
    try:
        with urllib_request.urlopen(req, timeout=10) as response:
            return json.loads(response.read().decode('utf-8'))
    except urllib_error.HTTPError as exc:
        detail = exc.read().decode('utf-8', errors='ignore')
        raise RuntimeError(detail or 'Could not fetch Discord profile.') from None


def _build_account_form_state(account: dict | None = None, source: dict | None = None) -> dict:
    account = account or {}
    player = account.get('player') or {}
    source = source or {}
    aliases = account.get('aliases') or []
    alias_text = '\n'.join(str(alias.get('alias_name') or '') for alias in aliases if alias.get('alias_name'))
    raw_name_color = source.get('name_color') if 'name_color' in source else player.get('name_color', PROFILE_COLOR_DEFAULT)
    try:
        name_color = normalize_profile_color_value(raw_name_color)
    except ValueError:
        name_color = PROFILE_COLOR_DEFAULT
    return {
        'player_name': str(source.get('player_name', player.get('name', ''))).strip(),
        'country_code': str(source.get('country_code', player.get('country_code', ''))).strip(),
        'discord_url': str(source.get('discord_url', player.get('discord_url', ''))).strip(),
        'priority_race': str(source.get('priority_race', player.get('priority_race', ''))).strip(),
        'name_color': name_color,
        'ladder_show_flag': str(source.get('ladder_show_flag', 'on' if player.get('ladder_show_flag', True) else '')).strip(),
        'ladder_show_aliases': str(source.get('ladder_show_aliases', 'on' if player.get('ladder_show_aliases', False) else '')).strip(),
        'ladder_show_badges': str(source.get('ladder_show_badges', 'on' if player.get('ladder_show_badges', True) else '')).strip(),
        'offrace_terran_enabled': str(source.get('offrace_terran_enabled', 'on' if player.get('offrace_terran_enabled', False) else '')).strip(),
        'offrace_protoss_enabled': str(source.get('offrace_protoss_enabled', 'on' if player.get('offrace_protoss_enabled', False) else '')).strip(),
        'offrace_zerg_enabled': str(source.get('offrace_zerg_enabled', 'on' if player.get('offrace_zerg_enabled', False) else '')).strip(),
        'ladder_rating_race': str(source.get('ladder_rating_race', player.get('ladder_rating_race', ''))).strip(),
        'aliases': str(source.get('aliases', alias_text)).strip(),
    }


def _render_account_page(
    *,
    account: dict,
    form_state: dict | None = None,
    error_message: str | None = None,
    success_message: str | None = None,
    status_code: int = 200,
):
    if account.get('player'):
        name_suggestions = []
    else:
        try:
            name_suggestions = fetch_player_name_suggestions(limit=300)
        except Exception:
            name_suggestions = []
    try:
        flag_options = fetch_flag_options()
    except Exception:
        flag_options = []

    context = base_context(
        'Account - TMG Stats',
        'account',
        meta_description='Manage your TMG Stats player profile.',
        canonical_path='/account',
        meta_robots='noindex,nofollow',
    )
    context.update(
        {
            'account': account,
            'player': account.get('player'),
            'form_state': form_state or _build_account_form_state(account),
            'error_message': error_message,
            'success_message': success_message,
            'race_options': ADMIN_MATCH_RACE_OPTIONS,
            'profile_color_options': PROFILE_COLOR_OPTIONS,
            'flag_options': flag_options,
            'name_suggestions': name_suggestions,
            'google_oauth_enabled': _google_oauth_enabled(),
            'google_redirect_uri': _google_redirect_uri(),
            'discord_oauth_enabled': _discord_oauth_enabled(),
        }
    )
    return make_response(render_template('account.html', **context), status_code)


@bp.route('/login', methods=['GET'])
def login_page():
    if current_user_session():
        return redirect('/account', code=303)

    context = base_context(
        'Login - TMG Stats',
        'account',
        meta_description='Sign in to TMG Stats with Google.',
        canonical_path='/login',
        meta_robots='noindex,nofollow',
    )
    context.update(
        {
            'google_oauth_enabled': _google_oauth_enabled(),
            'google_redirect_uri': _google_redirect_uri(),
            'login_error': request.args.get('error', ''),
        }
    )
    return render_template('login.html', **context)


@bp.route('/auth/google/start', methods=['GET'])
def google_auth_start():
    if not _google_oauth_enabled():
        return _oauth_error('Google login is not configured yet.')

    state = secrets.token_urlsafe(32)
    params = urllib_parse.urlencode(
        {
            'client_id': _google_client_id(),
            'redirect_uri': _google_redirect_uri(),
            'response_type': 'code',
            'scope': 'openid email profile',
            'state': state,
            'prompt': 'select_account',
        }
    )
    response = redirect(f'https://accounts.google.com/o/oauth2/v2/auth?{params}', code=302)
    response.set_cookie(
        GOOGLE_OAUTH_STATE_COOKIE_NAME,
        state,
        max_age=10 * 60,
        httponly=True,
        samesite='Lax',
        secure=_secure_cookie_enabled(),
        path='/',
    )
    return response


@bp.route('/auth/google/callback', methods=['GET'])
def google_auth_callback():
    if not _google_oauth_enabled():
        return _oauth_error('Google login is not configured yet.')

    expected_state = request.cookies.get(GOOGLE_OAUTH_STATE_COOKIE_NAME)
    provided_state = request.args.get('state', '')
    if not expected_state or not provided_state or not secrets.compare_digest(expected_state, provided_state):
        return _oauth_error('Google login state expired. Try again.')

    code = request.args.get('code', '')
    if not code:
        return _oauth_error(request.args.get('error_description') or request.args.get('error') or 'Google did not return an auth code.')

    try:
        token_payload = _exchange_google_code(code)
        access_token = str(token_payload.get('access_token') or '')
        if not access_token:
            raise RuntimeError('Google did not return an access token.')
        google_profile = _fetch_google_profile(access_token)
        account = get_or_create_user_account_from_google(google_profile)
    except Exception as exc:
        return _oauth_error(str(exc))

    response = redirect('/account', code=303)
    response.set_cookie(
        USER_COOKIE_NAME,
        build_user_cookie(account),
        max_age=USER_SESSION_DAYS * 24 * 60 * 60,
        httponly=True,
        samesite='Lax',
        secure=_secure_cookie_enabled(),
        path='/',
    )
    response.delete_cookie(GOOGLE_OAUTH_STATE_COOKIE_NAME, path='/')
    return response


def _discord_oauth_error(message: str):
    response = redirect('/account?error=' + urllib_parse.quote(message), code=303)
    response.delete_cookie(DISCORD_OAUTH_STATE_COOKIE_NAME, path='/')
    return response


@bp.route('/auth/discord/start', methods=['GET'])
def discord_auth_start():
    session = current_user_session()
    if not session:
        return redirect('/login', code=303)
    if not _discord_oauth_enabled():
        return _discord_oauth_error('Discord connection is not configured yet.')

    try:
        account = fetch_user_account(int(session['account_id']), include_player_extras=False)
    except Exception as exc:
        return _discord_oauth_error(str(exc))
    if not account or not account.get('player_id'):
        return _discord_oauth_error('Link your account to a player first.')

    state = secrets.token_urlsafe(32)
    params = urllib_parse.urlencode(
        {
            'client_id': _discord_client_id(),
            'redirect_uri': _discord_redirect_uri(),
            'response_type': 'code',
            'scope': 'identify',
            'state': state,
            'prompt': 'consent',
        }
    )
    response = redirect(f'https://discord.com/oauth2/authorize?{params}', code=302)
    response.set_cookie(
        DISCORD_OAUTH_STATE_COOKIE_NAME,
        state,
        max_age=10 * 60,
        httponly=True,
        samesite='Lax',
        secure=_secure_cookie_enabled(),
        path='/',
    )
    return response


@bp.route('/auth/discord/callback', methods=['GET'])
def discord_auth_callback():
    session = current_user_session()
    if not session:
        return redirect('/login', code=303)
    if not _discord_oauth_enabled():
        return _discord_oauth_error('Discord connection is not configured yet.')

    expected_state = request.cookies.get(DISCORD_OAUTH_STATE_COOKIE_NAME)
    provided_state = request.args.get('state', '')
    if not expected_state or not provided_state or not secrets.compare_digest(expected_state, provided_state):
        return _discord_oauth_error('Discord connection state expired. Try again.')

    code = request.args.get('code', '')
    if not code:
        return _discord_oauth_error(
            request.args.get('error_description') or request.args.get('error') or 'Discord did not return an auth code.'
        )

    try:
        token_payload = _exchange_discord_code(code)
        access_token = str(token_payload.get('access_token') or '')
        if not access_token:
            raise RuntimeError('Discord did not return an access token.')
        discord_profile = _fetch_discord_profile(access_token)
        update_user_discord_profile(
            account_id=int(session['account_id']),
            discord_user_id=str(discord_profile.get('id') or ''),
        )
    except Exception as exc:
        return _discord_oauth_error(str(exc))

    response = redirect('/account?discord=connected', code=303)
    response.delete_cookie(DISCORD_OAUTH_STATE_COOKIE_NAME, path='/')
    return response


@bp.route('/logout', methods=['POST'])
def user_logout():
    response = redirect('/', code=303)
    response.delete_cookie(USER_COOKIE_NAME, path='/')
    return response


@bp.route('/account', methods=['GET'])
def account_page():
    session = current_user_session()
    if not session:
        return redirect('/login', code=303)

    try:
        account = fetch_user_account(int(session['account_id']), include_player_extras=False)
    except Exception as exc:
        response = redirect('/login?error=' + urllib_parse.quote(str(exc)), code=303)
        response.delete_cookie(USER_COOKIE_NAME, path='/')
        return response
    if not account:
        response = redirect('/login?error=Account%20not%20found.', code=303)
        response.delete_cookie(USER_COOKIE_NAME, path='/')
        return response

    success_message = None
    if request.args.get('saved') == '1':
        success_message = 'Profile saved.'
    elif request.args.get('discord') == 'connected':
        success_message = 'Discord connected.'
    return _render_account_page(
        account=account,
        error_message=request.args.get('error') or None,
        success_message=success_message,
    )


@bp.route('/account', methods=['POST'])
def account_page_post():
    session = current_user_session()
    if not session:
        return redirect('/login', code=303)

    try:
        account = fetch_user_account(int(session['account_id']), include_player_extras=False)
    except Exception as exc:
        response = redirect('/login?error=' + urllib_parse.quote(str(exc)), code=303)
        response.delete_cookie(USER_COOKIE_NAME, path='/')
        return response
    if not account:
        response = redirect('/login?error=Account%20not%20found.', code=303)
        response.delete_cookie(USER_COOKIE_NAME, path='/')
        return response

    form_state = {key: value for key, value in request.form.items()}
    action = str(form_state.get('action', 'save_profile')).strip() or 'save_profile'

    try:
        if action == 'link_player':
            account = link_user_account_to_player(
                account_id=int(session['account_id']),
                player_name=form_state.get('player_name', ''),
            )
        else:
            account = update_user_profile_settings(
                account_id=int(session['account_id']),
                country_code=form_state.get('country_code', ''),
                discord_url=form_state.get(
                    'discord_url',
                    str((account.get('player') or {}).get('discord_url') or ''),
                ),
                priority_race=form_state.get('priority_race', ''),
                name_color=form_state.get('name_color', ''),
                ladder_show_flag=form_state.get('ladder_show_flag'),
                ladder_show_aliases=form_state.get('ladder_show_aliases'),
                ladder_show_badges=form_state.get('ladder_show_badges'),
                offrace_terran_enabled=form_state.get('offrace_terran_enabled'),
                offrace_protoss_enabled=form_state.get('offrace_protoss_enabled'),
                offrace_zerg_enabled=form_state.get('offrace_zerg_enabled'),
                ladder_rating_race=form_state.get('ladder_rating_race', ''),
                aliases=form_state.get('aliases', ''),
            )
    except Exception as exc:
        return _render_account_page(
            account=account,
            form_state=_build_account_form_state(account, form_state),
            error_message=str(exc),
            status_code=400,
        )

    response = redirect('/account?saved=1', code=303)
    response.set_cookie(
        USER_COOKIE_NAME,
        build_user_cookie(account),
        max_age=USER_SESSION_DAYS * 24 * 60 * 60,
        httponly=True,
        samesite='Lax',
        secure=_secure_cookie_enabled(),
        path='/',
    )
    return response
