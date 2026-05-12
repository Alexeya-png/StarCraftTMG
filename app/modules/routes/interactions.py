from __future__ import annotations

from flask import Blueprint, redirect, request

from app.database import delete_admin_feedback_message, submit_admin_feedback_message, submit_match_result
from app.modules.auth import is_admin
from app.modules.forms import _client_ip_address, _render_feedback_page, _render_submit_page

bp = Blueprint('interactions', __name__)

@bp.route('/submit', methods=['GET'])
def submit_result():
    return _render_submit_page()

@bp.route('/submit', methods=['POST'])
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

@bp.route('/feedback', methods=['GET'])
def feedback_page():
    success_message = None
    if request.args.get('sent') == '1':
        success_message = 'Your message has been sent to the admin.'
    elif request.args.get('deleted') == '1':
        success_message = 'Message deleted.'
    return _render_feedback_page(success_message=success_message)

@bp.route('/feedback', methods=['POST'])
def feedback_page_post():
    form_state = {key: value for key, value in request.form.items()}
    action = str(form_state.get('action', 'send_message')).strip() or 'send_message'

    if action == 'delete_message':
        if not is_admin():
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
