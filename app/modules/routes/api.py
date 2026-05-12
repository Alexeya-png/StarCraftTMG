from __future__ import annotations

from flask import Blueprint, jsonify, request

from app.database import MatchSubmissionRateLimitError, fetch_league_results_overview, submit_tts_match_result
from app.modules.auth import is_admin
from app.modules.cache import is_valid_supabase_webhook_request, run_cache_refresh, run_cache_refresh_background
from app.modules.config import CACHE_REFRESH_BACKGROUND
from app.modules.payloads import _leaderboard_payload, _parse_leaderboard_filters, _reports_payload
from app.modules.tts import _coerce_tts_game_type, _parse_tts_request_payload, get_tts_submit_token

bp = Blueprint('api', __name__)

@bp.route('/admin/cache/refresh', methods=['POST'])
def admin_cache_refresh():
    if not is_admin():
        return jsonify({'ok': False, 'error': 'Unauthorized'}), 401

    try:
        result = run_cache_refresh('admin')
    except Exception as exc:
        return jsonify({'ok': False, 'error': str(exc)}), 500

    return jsonify({'ok': True, 'cache': result})

@bp.route('/api/supabase/cache-webhook', methods=['POST'])
def supabase_cache_webhook():
    if not is_valid_supabase_webhook_request():
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
        run_cache_refresh_background(reason)
        return jsonify({'ok': True, 'refresh': 'scheduled', 'reason': reason})

    try:
        result = run_cache_refresh(reason)
    except Exception as exc:
        return jsonify({'ok': False, 'error': str(exc)}), 500

    return jsonify({'ok': True, 'refresh': 'completed', 'cache': result})

@bp.route('/api/leaderboard', methods=['GET'])
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

@bp.route('/api/reports', methods=['GET'])
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

@bp.route('/api/leagues', methods=['GET'])
def league_results_api():
    try:
        league_sections = fetch_league_results_overview()
        return jsonify({'ok': True, 'league_sections': league_sections})
    except Exception as exc:
        return jsonify({'ok': False, 'league_sections': [], 'error': str(exc)}), 500

@bp.route('/api/tts/submit-match', methods=['POST'])
def submit_tts_match():
    payload = _parse_tts_request_payload()

    expected_token = get_tts_submit_token()
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
