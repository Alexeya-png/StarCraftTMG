from __future__ import annotations

from datetime import datetime

from flask import make_response, render_template, request

from app.database import (
    fetch_admin_feedback_messages,
    fetch_mission_suggestions,
    fetch_player_name_suggestions,
)
from .auth import is_admin
from .config import (
    ADMIN_MATCH_RACE_OPTIONS,
    DEFAULT_MISSION_OPTIONS,
    FEEDBACK_MESSAGE_MAX_LENGTH,
    GAME_TYPE_OPTIONS,
    RACE_OPTIONS,
    SUBMIT_NAME_SUGGESTION_LIMIT,
)
from .context import base_context

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
        'winner_race': str(source.get('winner_race', 'Terran')).strip() or 'Terran',
        'opponent_race': str(source.get('opponent_race', 'Protoss')).strip() or 'Protoss',
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

    context = base_context(
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
        '??????': 'Terran',
        '???????': 'Protoss',
        '????': 'Zerg',
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

    if is_admin():
        try:
            feedback_messages = fetch_admin_feedback_messages(limit=200)
        except Exception as exc:
            feedback_load_error = str(exc)

    context = base_context(
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
