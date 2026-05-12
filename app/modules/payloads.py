from __future__ import annotations

from flask import request

from app.database import fetch_game_reports_page, fetch_leaderboard

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
