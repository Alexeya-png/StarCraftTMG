from __future__ import annotations

from flask import Blueprint, make_response, redirect, render_template, request

from app.database import (
    delete_match_admin,
    fetch_match_admin,
    fetch_player_admin,
    update_match_admin,
    update_player_admin,
)
from app.modules.auth import (
    build_admin_cookie,
    get_admin_login,
    get_admin_password,
    is_admin,
    redirect_to_admin_login,
)
from app.modules.config import ADMIN_COOKIE_NAME, ADMIN_MATCH_RACE_OPTIONS, ADMIN_SESSION_HOURS, GAME_TYPE_OPTIONS, RACE_LABELS
from app.modules.context import base_context
from app.modules.forms import (
    _build_admin_match_form_state,
    _build_admin_player_form_state,
    _merge_mission_options,
    _parse_admin_datetime,
)

bp = Blueprint('admin', __name__)

@bp.route('/admin', methods=['GET'])
def admin():
    context = base_context('Admin Panel', 'admin', meta_description='Admin area.', canonical_path='/admin',
                            meta_robots='noindex,nofollow')
    context.update({'login_error': None, 'admin_login_default': get_admin_login()})
    template_name = 'admin_dashboard.html' if context['is_admin'] else 'admin_login.html'
    return render_template(template_name, **context)

@bp.route('/admin/login', methods=['POST'])
def admin_login():
    login = str(request.form.get('login', '')).strip()
    password = str(request.form.get('password', '')).strip()

    if login != get_admin_login() or password != get_admin_password():
        context = base_context('Admin Panel', 'admin', meta_description='Admin area.', canonical_path='/admin',
                                meta_robots='noindex,nofollow')
        context.update({'login_error': 'Wrong login or password.', 'admin_login_default': login})
        return make_response(render_template('admin_login.html', **context), 401)

    response = redirect('/admin', code=303)
    response.set_cookie(
        ADMIN_COOKIE_NAME,
        build_admin_cookie(login),
        max_age=ADMIN_SESSION_HOURS * 60 * 60,
        httponly=True,
        samesite='Lax',
        secure=False,
        path='/',
    )
    return response

@bp.route('/admin/logout', methods=['POST'])
def admin_logout():
    response = redirect('/admin', code=303)
    response.delete_cookie(ADMIN_COOKIE_NAME, path='/')
    return response

@bp.route('/admin/players/<int:player_id>', methods=['GET'])
def admin_edit_player(player_id: int):
    if not is_admin():
        return redirect_to_admin_login()

    player = fetch_player_admin(player_id)
    if not player:
        context = base_context('Player Not Found', 'admin', meta_description='Admin area.',
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

    context = base_context(f"Edit Player – {player['name']}", 'admin', meta_description='Admin area.',
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

@bp.route('/admin/players/<int:player_id>', methods=['POST'])
def admin_edit_player_post(player_id: int):
    if not is_admin():
        return redirect_to_admin_login()

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
        context = base_context('Edit Player', 'admin', meta_description='Admin area.',
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

@bp.route('/admin/matches/<int:match_id>', methods=['GET'])
def admin_edit_match(match_id: int):
    if not is_admin():
        return redirect_to_admin_login()

    match = fetch_match_admin(match_id)
    if not match:
        context = base_context('Match Not Found', 'admin', meta_description='Admin area.',
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

    context = base_context(f'Edit Match – #{match_id}', 'admin', meta_description='Admin area.',
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

@bp.route('/admin/matches/<int:match_id>', methods=['POST'])
def admin_edit_match_post(match_id: int):
    if not is_admin():
        return redirect_to_admin_login()

    form_state = {key: value for key, value in request.form.items()}
    action = str(form_state.get('action', 'save')).strip() or 'save'

    if action == 'delete':
        try:
            delete_match_admin(match_id)
        except Exception as exc:
            existing_match = fetch_match_admin(match_id)
            context = base_context('Edit Match', 'admin', meta_description='Admin area.',
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
        context = base_context('Edit Match', 'admin', meta_description='Admin area.',
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
