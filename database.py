from __future__ import annotations

import math
from collections import defaultdict
import os
import re
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Generator

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
FLAGS_DIR = BASE_DIR / 'static' / 'Flags'
ALLOWED_FLAG_EXTENSIONS = {'.png', '.jpg', '.jpeg', '.svg', '.webp'}


class DatabaseConfigError(RuntimeError):
    pass


REQUIRED_ENV_VARS = ('user', 'password', 'host', 'port', 'dbname')

RACE_LABELS = {
    'Терран': 'Terran',
    'Протосс': 'Protoss',
    'Зерг': 'Zerg',
    'Terran': 'Terran',
    'Protoss': 'Protoss',
    'Zerg': 'Zerg',
}

COUNTRY_ALIASES = {
    'ua': 'ua',
    'ukraine': 'ua',
    'украина': 'ua',
    'ukraina': 'ua',
    'ukrayina': 'ua',
    'pl': 'pl',
    'poland': 'pl',
    'polska': 'pl',
    'польша': 'pl',
    'polen': 'pl',
    'us': 'us',
    'usa': 'us',
    'unitedstates': 'us',
    'states': 'us',
    'америка': 'us',
    'сша': 'us',
    'gb': 'gb',
    'uk': 'gb',
    'unitedkingdom': 'gb',
    'britain': 'gb',
    'england': 'gb',
    'великабритания': 'gb',
    'de': 'de',
    'germany': 'de',
    'deutschland': 'de',
    'германия': 'de',
    'fr': 'fr',
    'france': 'fr',
    'франция': 'fr',
    'es': 'es',
    'spain': 'es',
    'espana': 'es',
    'испания': 'es',
    'it': 'it',
    'italy': 'it',
    'italia': 'it',
    'италия': 'it',
    'se': 'se',
    'sweden': 'se',
    'sverige': 'se',
    'швеция': 'se',
    'ca': 'ca',
    'canada': 'ca',
    'канада': 'ca',
    'br': 'br',
    'brazil': 'br',
    'brasil': 'br',
    'бразилия': 'br',
    'kr': 'kr',
    'korea': 'kr',
    'southkorea': 'kr',
    'koreasouth': 'kr',
    'корея': 'kr',
    'южнаякорея': 'kr',
    'cn': 'cn',
    'china': 'cn',
    'китай': 'cn',
    'jp': 'jp',
    'japan': 'jp',
    'япония': 'jp',
    'ru': 'ru',
    'russia': 'ru',
    'россия': 'ru',
}


COUNTRY_NAME_BY_CODE = {
    'ua': 'Ukraine',
    'pl': 'Poland',
    'us': 'United States',
    'gb': 'United Kingdom',
    'de': 'Germany',
    'fr': 'France',
    'es': 'Spain',
    'it': 'Italy',
    'se': 'Sweden',
    'ca': 'Canada',
    'br': 'Brazil',
    'kr': 'South Korea',
    'cn': 'China',
    'jp': 'Japan',
    'ru': 'Russia',
}

_PLAYER_COLUMNS_CACHE: set[str] | None = None


def _resolve_country_code(country_code: str | None, country_name: str | None = None) -> str:
    clean_code = _slugify(country_code)
    if clean_code:
        return COUNTRY_ALIASES.get(clean_code, clean_code)

    clean_name = _slugify(country_name)
    if clean_name:
        return COUNTRY_ALIASES.get(clean_name, clean_name)

    return ''


def _resolve_country_name(country_code: str | None, country_name: str | None = None) -> str:
    clean_name = _normalize_text(country_name)
    if clean_name:
        return clean_name

    code = _resolve_country_code(country_code, country_name)
    if not code:
        return ''

    return COUNTRY_NAME_BY_CODE.get(code, code.upper())


def _get_db_settings() -> dict[str, str]:
    settings = {
        'user': os.getenv('user', '').strip(),
        'password': os.getenv('password', '').strip(),
        'host': os.getenv('host', '').strip(),
        'port': os.getenv('port', '').strip(),
        'dbname': os.getenv('dbname', '').strip(),
        'sslmode': os.getenv('sslmode', 'require').strip() or 'require',
    }

    missing = [key for key in REQUIRED_ENV_VARS if not settings[key]]
    if missing:
        joined = ', '.join(missing)
        raise DatabaseConfigError(f'Missing database environment variables: {joined}')

    return settings


@contextmanager
def get_db_connection() -> Generator[psycopg2.extensions.connection, None, None]:
    settings = _get_db_settings()
    connection = psycopg2.connect(
        user=settings['user'],
        password=settings['password'],
        host=settings['host'],
        port=settings['port'],
        dbname=settings['dbname'],
        sslmode=settings['sslmode'],
        cursor_factory=RealDictCursor,
    )
    try:
        yield connection
    finally:
        connection.close()


def ensure_database_schema() -> None:
    return None


def ping_database() -> tuple[bool, str | None]:
    try:
        ensure_database_schema()
        with get_db_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute('SELECT 1 AS ok;')
                cursor.fetchone()
        return True, None
    except Exception as exc:
        return False, str(exc)


def _normalize_text(value: str | None) -> str:
    if value is None:
        return ''
    return str(value).strip()


def _normalize_race_label(value: str | None) -> str:
    clean_value = _normalize_text(value)
    if not clean_value:
        return ''
    return RACE_LABELS.get(clean_value, clean_value)


def _normalize_elo_value(value) -> str:
    if value is None:
        return '—'

    try:
        return str(int(value))
    except (TypeError, ValueError):
        return str(value)


def _normalize_discord_url(value: str | None) -> str:
    clean_value = _normalize_text(value)
    if not clean_value:
        return ''

    if re.match(r'^[a-z][a-z0-9+.-]*://', clean_value, re.IGNORECASE):
        return clean_value

    return f'https://{clean_value}'


def _slugify(value: str | None) -> str:
    clean_value = _normalize_text(value).lower()
    if not clean_value:
        return ''
    return re.sub(r'[^a-zа-яё0-9]+', '', clean_value)


def _build_flag_index() -> dict[str, str]:
    if not FLAGS_DIR.exists():
        return {}

    index: dict[str, str] = {}
    for path in FLAGS_DIR.rglob('*'):
        if not path.is_file() or path.suffix.lower() not in ALLOWED_FLAG_EXTENSIONS:
            continue

        relative_path = path.relative_to(FLAGS_DIR).as_posix()
        web_path = f'/static/Flags/{relative_path}'

        candidates = {
            _slugify(path.stem),
            _slugify(relative_path),
            _slugify(path.name),
        }

        for candidate in list(candidates):
            alias = COUNTRY_ALIASES.get(candidate)
            if alias:
                candidates.add(alias)

        for candidate in candidates:
            if candidate and candidate not in index:
                index[candidate] = web_path

    return index


FLAG_INDEX = _build_flag_index()


def _resolve_flag_url(country_code: str | None, country_name: str | None) -> str:
    candidates = []

    code_key = COUNTRY_ALIASES.get(_slugify(country_code), _slugify(country_code))
    name_key = COUNTRY_ALIASES.get(_slugify(country_name), _slugify(country_name))

    if code_key:
        candidates.append(code_key)
    if name_key and name_key not in candidates:
        candidates.append(name_key)

    for candidate in candidates:
        flag_url = FLAG_INDEX.get(candidate)
        if flag_url:
            return flag_url

    return ''


def _get_player_column_names(connection: psycopg2.extensions.connection) -> set[str]:
    global _PLAYER_COLUMNS_CACHE

    if _PLAYER_COLUMNS_CACHE is not None:
        return _PLAYER_COLUMNS_CACHE

    with connection.cursor() as cursor:
        cursor.execute(
            '''
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'players';
            '''
        )
        rows = cursor.fetchall()

    _PLAYER_COLUMNS_CACHE = {row['column_name'] for row in rows}
    return _PLAYER_COLUMNS_CACHE


def _humanize_last_played(value) -> str:
    if not value:
        return '—'

    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return value
    else:
        parsed = value

    now = datetime.now(parsed.tzinfo) if getattr(parsed, 'tzinfo', None) else datetime.now()
    delta = now - parsed
    total_seconds = int(delta.total_seconds())

    if total_seconds < 0:
        return parsed.strftime('%Y-%m-%d')
    if total_seconds < 3600:
        minutes = max(1, total_seconds // 60)
        return f'{minutes} min'
    if total_seconds < 86400:
        hours = total_seconds // 3600
        return '1 hour' if hours == 1 else f'{hours} hours'
    if total_seconds < 2592000:
        days = total_seconds // 86400
        return '1 day' if days == 1 else f'{days} days'

    return parsed.strftime('%Y-%m-%d')


def _format_match_datetime(value) -> str:
    if not value:
        return '—'

    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return value
    else:
        parsed = value

    return parsed.strftime('%Y-%m-%d %H:%M')


def _format_match_date(value) -> str:
    if not value:
        return '—'

    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return value
    else:
        parsed = value

    return parsed.strftime('%Y-%m-%d')


def _format_percent(value) -> str:
    if value is None:
        return '0%'

    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return str(value)

    if numeric.is_integer():
        return f'{int(numeric)}%'
    return f'{numeric:.1f}%'


def _format_delta(value) -> str:
    if value is None:
        return '—'

    try:
        numeric = int(value)
    except (TypeError, ValueError):
        return str(value)

    if numeric > 0:
        return f'+{numeric}'
    return str(numeric)


def _get_player_select_expressions(connection: psycopg2.extensions.connection) -> dict[str, str]:
    player_columns = _get_player_column_names(connection)

    if 'country_code' in player_columns:
        country_code_expr = "NULLIF(TRIM(p.country_code), '')"
    else:
        country_code_expr = 'NULL::text'

    country_name_expr = 'NULL::text'

    if 'priority_race' in player_columns:
        priority_race_expr = "NULLIF(TRIM(p.priority_race), '')"
    else:
        priority_race_expr = 'NULL::text'

    if 'discord_url' in player_columns:
        discord_url_expr = "NULLIF(TRIM(p.discord_url), '')"
    else:
        discord_url_expr = 'NULL::text'

    return {
        'country_code_expr': country_code_expr,
        'country_name_expr': country_name_expr,
        'priority_race_expr': priority_race_expr,
        'discord_url_expr': discord_url_expr,
    }


def _prepare_player_row(row: dict) -> dict:
    player = dict(row)
    player['country_code'] = _resolve_country_code(player.get('country_code'), player.get('country_name'))
    player['country_name'] = _resolve_country_name(player.get('country_code'), player.get('country_name'))
    player['priority_race'] = _normalize_race_label(player.get('priority_race'))
    player['discord_url'] = _normalize_discord_url(player.get('discord_url'))
    player['flag_url'] = _resolve_flag_url(player.get('country_code'), player.get('country_name'))
    player['last_played_label'] = _humanize_last_played(player.get('last_match_at'))
    player['current_elo_display'] = _normalize_elo_value(player.get('current_elo'))
    player['win_rate_display'] = _format_percent(player.get('win_rate'))
    player['profile_url'] = f"/players/{player['id']}"
    return player


def _build_rating_chart(current_elo, rating_rows: list[dict]) -> dict:
    series: list[dict] = []

    if rating_rows:
        first_old_elo = rating_rows[0].get('old_elo')
        first_played_at = rating_rows[0].get('played_at')
        if first_old_elo is not None:
            try:
                series.append(
                    {
                        'label': 'Start',
                        'full_label': 'Starting rating',
                        'elo': int(first_old_elo),
                        'played_at': first_played_at,
                        'played_at_label': _format_match_datetime(first_played_at),
                    }
                )
            except (TypeError, ValueError):
                pass

        for row in rating_rows:
            try:
                old_elo_value = int(row.get('old_elo')) if row.get('old_elo') is not None else None
                elo_value = int(row.get('new_elo'))
                elo_delta_value = int(row.get('elo_delta')) if row.get('elo_delta') is not None else None
            except (TypeError, ValueError):
                continue

            played_at_value = row.get('played_at')
            series.append(
                {
                    'label': _format_match_date(played_at_value),
                    'full_label': _format_match_datetime(played_at_value),
                    'played_at': played_at_value,
                    'played_at_label': _format_match_datetime(played_at_value),
                    'elo': elo_value,
                    'old_elo': old_elo_value,
                    'elo_delta': elo_delta_value,
                }
            )

    if not series:
        fallback_elo = 1000
        try:
            fallback_elo = int(current_elo)
        except (TypeError, ValueError):
            pass

        series = [
            {
                'label': 'Current',
                'full_label': 'Current rating',
                'played_at': None,
                'played_at_label': 'Current rating',
                'elo': fallback_elo,
                'old_elo': fallback_elo,
                'elo_delta': 0,
            }
        ]

    raw_values = [point['elo'] for point in series]
    raw_low_value = min(raw_values)
    raw_high_value = max(raw_values)

    if len(series) <= 2:
        ema_values = raw_values[:]
    else:
        smoothing_span = max(8, min(22, math.ceil(len(series) / 24)))
        alpha = 2 / (smoothing_span + 1)
        ema_values = []
        previous_ema = float(raw_values[0])
        for value in raw_values:
            previous_ema = (alpha * float(value)) + ((1 - alpha) * previous_ema)
            ema_values.append(round(previous_ema, 2))

    combined_values = raw_values + ema_values
    low_value = min(combined_values)
    high_value = max(combined_values)
    spread = high_value - low_value
    padding = max(16, math.ceil(spread * 0.12)) if spread else 24
    axis_min = max(0, math.floor(low_value - padding))
    axis_max = math.ceil(high_value + padding)

    if axis_max <= axis_min:
        axis_max = axis_min + 1

    width = 980
    height = 328
    plot_left = 58
    plot_right = 18
    plot_top = 18
    plot_bottom = 58
    plot_width = width - plot_left - plot_right
    plot_height = height - plot_top - plot_bottom

    def point_x(index: int) -> float:
        if len(series) == 1:
            return plot_left + plot_width / 2
        return plot_left + (plot_width * index) / (len(series) - 1)

    def point_y(value: float) -> float:
        return plot_top + ((axis_max - value) / (axis_max - axis_min)) * plot_height

    raw_pairs = []
    ema_pairs = []
    hover_points = []

    for index, point in enumerate(series):
        x_value = point_x(index)
        raw_y = point_y(point['elo'])
        ema_y = point_y(ema_values[index])

        raw_pairs.append(f'{x_value:.2f},{raw_y:.2f}')
        ema_pairs.append(f'{x_value:.2f},{ema_y:.2f}')

        hover_points.append(
            {
                'x': round(x_value, 2),
                'raw_y': round(raw_y, 2),
                'ema_y': round(ema_y, 2),
                'elo': point['elo'],
                'smoothed_elo': round(ema_values[index], 2),
                'label': point.get('full_label') or point['label'],
                'played_at_label': point.get('played_at_label') or point.get('full_label') or point['label'],
                'date_label': _format_match_date(point.get('played_at')) if point.get('played_at') else point.get('label') or '',
                'elo_delta_display': _format_delta(point.get('elo_delta')),
                'old_elo_display': _normalize_elo_value(point.get('old_elo')),
                'new_elo_display': _normalize_elo_value(point.get('elo')),
            }
        )

    def build_area(points: list[str], first_x: float, last_x: float) -> str:
        return f"{first_x:.2f},{plot_top + plot_height:.2f} {' '.join(points)} {last_x:.2f},{plot_top + plot_height:.2f}"

    if hover_points:
        raw_area_points = build_area(raw_pairs, hover_points[0]['x'], hover_points[-1]['x'])
        ema_area_points = build_area(ema_pairs, hover_points[0]['x'], hover_points[-1]['x'])

        for index, point in enumerate(hover_points):
            if len(hover_points) == 1:
                left_x = plot_left
                right_x = plot_left + plot_width
            else:
                prev_x = hover_points[index - 1]['x'] if index > 0 else point['x']
                next_x = hover_points[index + 1]['x'] if index < len(hover_points) - 1 else point['x']
                left_x = plot_left if index == 0 else (prev_x + point['x']) / 2
                right_x = plot_left + plot_width if index == len(hover_points) - 1 else (point['x'] + next_x) / 2

            point['band_x'] = round(left_x, 2)
            point['band_width'] = round(max(8, right_x - left_x), 2)
    else:
        raw_area_points = ''
        ema_area_points = ''

    tick_count = 5
    y_ticks = []
    for index in range(tick_count):
        ratio = index / (tick_count - 1)
        y_value = plot_top + ratio * plot_height
        elo_value = axis_max - ratio * (axis_max - axis_min)
        y_ticks.append({'y': round(y_value, 2), 'label': str(int(round(elo_value)))})

    x_ticks = []
    if hover_points:
        max_x_ticks = min(7, len(hover_points))
        tick_indices = sorted(
            {
                round(step * (len(hover_points) - 1) / max(1, max_x_ticks - 1))
                for step in range(max_x_ticks)
            }
        )
        for index in tick_indices:
            point = hover_points[index]
            x_ticks.append(
                {
                    'x': point['x'],
                    'label': point.get('date_label') or point.get('played_at_label') or '—',
                }
            )

    start_label = series[0].get('full_label') or series[0]['label']
    end_label = series[-1].get('full_label') or series[-1]['label']

    return {
        'width': width,
        'height': height,
        'plot_left': plot_left,
        'plot_right': plot_right,
        'plot_top': plot_top,
        'plot_bottom': plot_bottom,
        'plot_width': plot_width,
        'plot_height': plot_height,
        'raw_points_attr': ' '.join(raw_pairs),
        'raw_area_points': raw_area_points,
        'smoothed_points_attr': ' '.join(ema_pairs),
        'smoothed_area_points': ema_area_points,
        'hover_points': hover_points,
        'y_ticks': y_ticks,
        'x_ticks': x_ticks,
        'lowest_elo': raw_low_value,
        'highest_elo': raw_high_value,
        'current_elo': raw_values[-1],
        'matches_tracked': max(0, len(series) - 1),
        'start_label': start_label,
        'end_label': end_label,
        'smoothing_label': 'EMA',
    }


def fetch_leaderboard(
    search: str = '',
    *,
    include_active: bool = True,
    include_inactive: bool = False,
) -> list[dict]:
    normalized_search = search.strip()
    like_pattern = f'%{normalized_search}%'

    if not include_active and not include_inactive:
        include_active = True

    if include_active and include_inactive:
        status_clause = 'TRUE'
    elif include_active:
        status_clause = 'p.is_active = TRUE'
    else:
        status_clause = 'p.is_active = FALSE'

    with get_db_connection() as connection:
        expr = _get_player_select_expressions(connection)

        query = f'''
        WITH ranked AS (
            SELECT
                p.id,
                p.name,
                p.current_elo AS current_elo,
                p.last_match_at,
                {expr['country_code_expr']} AS country_code,
                {expr['country_name_expr']} AS country_name,
                {expr['priority_race_expr']} AS priority_race,
                p.matches_count,
                p.wins,
                p.losses,
                ROW_NUMBER() OVER (
                    ORDER BY p.current_elo DESC, p.wins DESC, p.matches_count DESC, p.name ASC
                ) AS rank_position,
                CASE
                    WHEN p.matches_count > 0 THEN ROUND((p.wins::numeric / p.matches_count::numeric) * 100, 1)
                    ELSE 0
                END AS win_rate
            FROM players p
            WHERE ({status_clause})
              AND (%s = '' OR p.name ILIKE %s)
        )
        SELECT
            id,
            name,
            current_elo,
            last_match_at,
            country_code,
            country_name,
            priority_race,
            matches_count,
            wins,
            losses,
            rank_position,
            win_rate
        FROM ranked
        ORDER BY rank_position ASC, name ASC;
        '''

        with connection.cursor() as cursor:
            cursor.execute(query, (normalized_search, like_pattern))
            rows = cursor.fetchall()

    return [_prepare_player_row(row) for row in rows]



def _prepare_game_report_row(row: dict) -> dict:
    match = dict(row)
    match['played_at_label'] = _format_match_date(match.get('played_at'))
    match['winner_race'] = _normalize_race_label(match.get('winner_race'))
    match['loser_race'] = _normalize_race_label(match.get('loser_race'))
    match['winner_profile_url'] = f"/players/{match['winner_id']}"
    match['loser_profile_url'] = f"/players/{match['loser_id']}"
    match['winner_elo_delta_display'] = _format_delta(match.get('winner_elo_delta'))
    match['loser_elo_delta_display'] = _format_delta(match.get('loser_elo_delta'))
    match['winner_rating_display'] = f"{_normalize_elo_value(match.get('winner_old_elo'))} → {_normalize_elo_value(match.get('winner_new_elo'))}" if match.get('winner_old_elo') is not None and match.get('winner_new_elo') is not None else '—'
    match['loser_rating_display'] = f"{_normalize_elo_value(match.get('loser_old_elo'))} → {_normalize_elo_value(match.get('loser_new_elo'))}" if match.get('loser_old_elo') is not None and match.get('loser_new_elo') is not None else '—'
    match['ranked_label'] = 'Ranked' if match.get('is_ranked') else 'Unranked'
    match['comment_display'] = _normalize_text(match.get('comment')) or '—'
    match['game_type_display'] = _normalize_text(match.get('game_type')) or '—'
    match['mission_name_display'] = _normalize_text(match.get('mission_name')) or '—'
    return match



def fetch_game_reports(search: str = '', limit: int = 100) -> list[dict]:
    safe_limit = max(1, min(limit, 500))
    normalized_search = search.strip()
    like_pattern = f'%{normalized_search}%'

    query = '''
    SELECT
        m.id,
        m.played_at,
        winner.id AS winner_id,
        winner.name AS winner_name,
        loser.id AS loser_id,
        loser.name AS loser_name,
        CASE
            WHEN m.winner_player_id = m.player1_id THEN m.player1_race
            ELSE m.player2_race
        END AS winner_race,
        CASE
            WHEN m.winner_player_id = m.player1_id THEN m.player2_race
            ELSE m.player1_race
        END AS loser_race,
        m.is_ranked,
        m.game_type,
        m.mission_name,
        m.comment,
        winner_history.old_elo AS winner_old_elo,
        winner_history.new_elo AS winner_new_elo,
        winner_history.elo_delta AS winner_elo_delta,
        loser_history.old_elo AS loser_old_elo,
        loser_history.new_elo AS loser_new_elo,
        loser_history.elo_delta AS loser_elo_delta
    FROM matches m
    JOIN players winner ON winner.id = m.winner_player_id
    JOIN players loser
      ON loser.id = CASE
            WHEN m.winner_player_id = m.player1_id THEN m.player2_id
            ELSE m.player1_id
        END
    LEFT JOIN rating_history winner_history
      ON winner_history.match_id = m.id
     AND winner_history.player_id = winner.id
    LEFT JOIN rating_history loser_history
      ON loser_history.match_id = m.id
     AND loser_history.player_id = loser.id
    WHERE (
            %s = ''
            OR winner.name ILIKE %s
            OR loser.name ILIKE %s
            OR COALESCE(m.game_type, '') ILIKE %s
            OR COALESCE(m.mission_name, '') ILIKE %s
            OR COALESCE(m.comment, '') ILIKE %s
          )
    ORDER BY m.played_at DESC, m.id DESC
    LIMIT %s;
    '''

    with get_db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                query,
                (
                    normalized_search,
                    like_pattern,
                    like_pattern,
                    like_pattern,
                    like_pattern,
                    like_pattern,
                    safe_limit,
                ),
            )
            rows = cursor.fetchall()

    return [_prepare_game_report_row(row) for row in rows]


def fetch_player_profile(player_id: int, recent_matches_limit: int = 20) -> dict | None:
    safe_recent_matches_limit = max(1, min(recent_matches_limit, 50))

    with get_db_connection() as connection:
        expr = _get_player_select_expressions(connection)

        profile_query = f'''
        WITH ranked AS (
            SELECT
                p.id,
                p.name,
                p.current_elo,
                p.last_match_at,
                {expr['country_code_expr']} AS country_code,
                {expr['country_name_expr']} AS country_name,
                {expr['priority_race_expr']} AS priority_race,
                {expr['discord_url_expr']} AS discord_url,
                p.matches_count,
                p.wins,
                p.losses,
                p.created_at,
                ROW_NUMBER() OVER (
                    ORDER BY p.current_elo DESC, p.wins DESC, p.matches_count DESC, p.name ASC
                ) AS rank_position,
                CASE
                    WHEN p.matches_count > 0 THEN ROUND((p.wins::numeric / p.matches_count::numeric) * 100, 1)
                    ELSE 0
                END AS win_rate
            FROM players p
            WHERE p.is_active = TRUE
        )
        SELECT
            id,
            name,
            current_elo,
            last_match_at,
            country_code,
            country_name,
            priority_race,
            discord_url,
            matches_count,
            wins,
            losses,
            created_at,
            rank_position,
            win_rate
        FROM ranked
        WHERE id = %s;
        '''

        recent_matches_query = '''
        SELECT
            m.id,
            m.played_at,
            CASE
                WHEN m.player1_id = %(player_id)s THEN opponent_two.id
                ELSE opponent_one.id
            END AS opponent_id,
            CASE
                WHEN m.player1_id = %(player_id)s THEN opponent_two.name
                ELSE opponent_one.name
            END AS opponent_name,
            CASE
                WHEN m.winner_player_id = %(player_id)s THEN 'Win'
                ELSE 'Loss'
            END AS result_label,
            CASE
                WHEN m.winner_player_id = %(player_id)s THEN TRUE
                ELSE FALSE
            END AS is_win,
            CASE
                WHEN m.player1_id = %(player_id)s THEN m.player1_race
                ELSE m.player2_race
            END AS player_race,
            CASE
                WHEN m.player1_id = %(player_id)s THEN m.player2_race
                ELSE m.player1_race
            END AS opponent_race,
            rh.old_elo,
            rh.new_elo,
            rh.elo_delta
        FROM matches m
        JOIN players opponent_one ON opponent_one.id = m.player1_id
        JOIN players opponent_two ON opponent_two.id = m.player2_id
        LEFT JOIN rating_history rh
            ON rh.match_id = m.id
           AND rh.player_id = %(player_id)s
        WHERE m.player1_id = %(player_id)s OR m.player2_id = %(player_id)s
        ORDER BY m.played_at DESC, m.id DESC
        LIMIT %(recent_limit)s;
        '''

        rating_chart_query = '''
        SELECT
            m.played_at,
            rh.old_elo,
            rh.new_elo,
            rh.elo_delta
        FROM rating_history rh
        JOIN matches m ON m.id = rh.match_id
        WHERE rh.player_id = %s
        ORDER BY m.played_at ASC, rh.id ASC;
        '''

        with connection.cursor() as cursor:
            cursor.execute(profile_query, (player_id,))
            player_row = cursor.fetchone()

            if not player_row:
                return None

            cursor.execute(
                recent_matches_query,
                {
                    'player_id': player_id,
                    'recent_limit': safe_recent_matches_limit,
                },
            )
            match_rows = cursor.fetchall()

            cursor.execute(rating_chart_query, (player_id,))
            rating_rows = cursor.fetchall()

    player = _prepare_player_row(player_row)
    player['member_since_label'] = _format_match_datetime(player.get('created_at'))

    recent_matches: list[dict] = []
    for row in match_rows:
        match = dict(row)
        match['played_at_label'] = _format_match_date(match.get('played_at'))
        match['player_race'] = _normalize_race_label(match.get('player_race'))
        match['opponent_race'] = _normalize_race_label(match.get('opponent_race'))
        match['old_elo_display'] = _normalize_elo_value(match.get('old_elo'))
        match['new_elo_display'] = _normalize_elo_value(match.get('new_elo'))
        match['elo_delta_display'] = _format_delta(match.get('elo_delta'))
        match['opponent_profile_url'] = f"/players/{match['opponent_id']}"
        recent_matches.append(match)

    rating_chart = _build_rating_chart(player.get('current_elo'), [dict(row) for row in rating_rows])

    return {
        'player': player,
        'recent_matches': recent_matches,
        'rating_chart': rating_chart,
    }


RACE_OPTIONS = ('Терран', 'Протосс', 'Зерг')
GAME_TYPE_OPTIONS = ('1к', '2к', 'Grand Offensive')
DEFAULT_K_FACTOR = 32


def _normalize_player_name(value: str | None) -> str:
    clean_value = _normalize_text(value)
    if not clean_value:
        return ''
    return re.sub(r'\s+', ' ', clean_value).strip()


def _normalize_player_key(value: str | None) -> str:
    clean_value = _normalize_player_name(value)
    if not clean_value:
        return ''
    return clean_value.casefold()


def _coerce_ranked_value(value) -> bool:
    if isinstance(value, bool):
        return value
    normalized = _normalize_text(value).lower()
    return normalized in {'1', 'true', 'yes', 'y', 'ranked', 'on'}


def _calculate_expected_score(player_elo: int, opponent_elo: int) -> float:
    return 1 / (1 + 10 ** ((opponent_elo - player_elo) / 400))


def _calculate_elo_result(winner_elo: int, loser_elo: int, k_factor: int = DEFAULT_K_FACTOR) -> dict:
    expected_winner = _calculate_expected_score(winner_elo, loser_elo)
    expected_loser = _calculate_expected_score(loser_elo, winner_elo)

    winner_delta = int(round(k_factor * (1 - expected_winner)))
    loser_delta = -winner_delta

    winner_new = max(0, winner_elo + winner_delta)
    loser_new = max(0, loser_elo + loser_delta)

    return {
        'winner_old_elo': winner_elo,
        'winner_new_elo': winner_new,
        'winner_delta': winner_delta,
        'winner_expected_score': expected_winner,
        'loser_old_elo': loser_elo,
        'loser_new_elo': loser_new,
        'loser_delta': loser_delta,
        'loser_expected_score': expected_loser,
        'k_factor': k_factor,
    }


def _get_or_create_player(cursor, player_name: str) -> dict:
    normalized_name = _normalize_player_name(player_name)
    normalized_key = _normalize_player_key(player_name)

    cursor.execute(
        '''
        SELECT id, name, current_elo, is_active
        FROM players
        WHERE name_normalized = %s
        LIMIT 1;
        ''',
        (normalized_key,),
    )
    row = cursor.fetchone()

    if row:
        player = dict(row)
        if player['name'] != normalized_name or not player.get('is_active', True):
            cursor.execute(
                '''
                UPDATE players
                SET name = %s,
                    is_active = TRUE,
                    updated_at = NOW()
                WHERE id = %s
                RETURNING id, name, current_elo, is_active;
                ''',
                (normalized_name, player['id']),
            )
            player = dict(cursor.fetchone())
        player['created'] = False
        return player

    cursor.execute(
        '''
        INSERT INTO players (
            name,
            name_normalized,
            current_elo,
            matches_count,
            wins,
            losses,
            is_active,
            created_at,
            updated_at
        )
        VALUES (%s, %s, 1000, 0, 0, 0, TRUE, NOW(), NOW())
        RETURNING id, name, current_elo, is_active;
        ''',
        (normalized_name, normalized_key),
    )
    player = dict(cursor.fetchone())
    player['created'] = True
    return player


def _refresh_priority_race(cursor, player_id: int) -> None:
    cursor.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'players'
          AND column_name = 'priority_race'
        LIMIT 1;
        """
    )
    if not cursor.fetchone():
        return

    cursor.execute(
        '''
        WITH race_counts AS (
            SELECT race, COUNT(*)::int AS race_count
            FROM (
                SELECT m.player1_race AS race
                FROM matches m
                WHERE m.player1_id = %s
                  AND m.player1_race IS NOT NULL

                UNION ALL

                SELECT m.player2_race AS race
                FROM matches m
                WHERE m.player2_id = %s
                  AND m.player2_race IS NOT NULL
            ) races
            GROUP BY race
            ORDER BY race_count DESC, race ASC
            LIMIT 1
        )
        UPDATE players p
        SET priority_race = (SELECT race FROM race_counts),
            updated_at = NOW()
        WHERE p.id = %s;
        ''',
        (player_id, player_id, player_id),
    )


def fetch_player_name_suggestions(limit: int = 500) -> list[str]:
    safe_limit = max(1, min(limit, 2000))

    with get_db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                '''
                SELECT name
                FROM players
                WHERE is_active = TRUE
                ORDER BY current_elo DESC, name ASC
                LIMIT %s;
                ''',
                (safe_limit,),
            )
            rows = cursor.fetchall()

    return [row['name'] for row in rows if _normalize_text(row.get('name'))]


def fetch_mission_suggestions(limit: int = 50) -> list[str]:
    safe_limit = max(1, min(limit, 200))
    ensure_database_schema()

    with get_db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                '''
                SELECT DISTINCT mission_name
                FROM matches
                WHERE mission_name IS NOT NULL
                  AND NULLIF(TRIM(mission_name), '') IS NOT NULL
                ORDER BY mission_name ASC
                LIMIT %s;
                ''',
                (safe_limit,),
            )
            rows = cursor.fetchall()

    return [row['mission_name'] for row in rows if _normalize_text(row.get('mission_name'))]


def submit_match_result(
    *,
    winner_name: str,
    opponent_name: str,
    winner_race: str,
    opponent_race: str,
    is_ranked,
    game_type: str,
    mission_name: str,
    comment: str = '',
) -> dict:
    ensure_database_schema()

    clean_winner_name = _normalize_player_name(winner_name)
    clean_opponent_name = _normalize_player_name(opponent_name)
    clean_winner_race = _normalize_text(winner_race)
    clean_opponent_race = _normalize_text(opponent_race)
    clean_game_type = _normalize_text(game_type)
    clean_mission_name = _normalize_player_name(mission_name)
    clean_comment = _normalize_text(comment)
    ranked_match = _coerce_ranked_value(is_ranked)

    if not clean_winner_name:
        raise ValueError('Enter the winner name.')
    if not clean_opponent_name:
        raise ValueError('Enter the opponent name.')
    if _normalize_player_key(clean_winner_name) == _normalize_player_key(clean_opponent_name):
        raise ValueError('Winner and opponent must be different players.')
    if clean_winner_race not in RACE_OPTIONS:
        raise ValueError('Choose the winner race.')
    if clean_opponent_race not in RACE_OPTIONS:
        raise ValueError('Choose the opponent race.')
    if clean_game_type not in GAME_TYPE_OPTIONS:
        raise ValueError('Choose the game type.')
    if not clean_mission_name:
        raise ValueError('Choose the mission.')
    if len(clean_comment) > 4000:
        raise ValueError('Comment is too long.')

    played_at = datetime.now()

    with get_db_connection() as connection:
        try:
            with connection.cursor() as cursor:
                winner_player = _get_or_create_player(cursor, clean_winner_name)
                opponent_player = _get_or_create_player(cursor, clean_opponent_name)

                cursor.execute(
                    '''
                    INSERT INTO matches (
                        player1_id,
                        player2_id,
                        winner_player_id,
                        played_at,
                        created_at,
                        comment,
                        player1_race,
                        player2_race,
                        is_ranked,
                        game_type,
                        mission_name
                    )
                    VALUES (%s, %s, %s, %s, NOW(), %s, %s, %s, %s, %s, %s)
                    RETURNING id, played_at;
                    ''',
                    (
                        winner_player['id'],
                        opponent_player['id'],
                        winner_player['id'],
                        played_at,
                        clean_comment or None,
                        clean_winner_race,
                        clean_opponent_race,
                        ranked_match,
                        clean_game_type,
                        clean_mission_name,
                    ),
                )
                match_row = dict(cursor.fetchone())

                winner_old_elo = int(winner_player.get('current_elo') or 1000)
                opponent_old_elo = int(opponent_player.get('current_elo') or 1000)

                if ranked_match:
                    elo_result = _calculate_elo_result(winner_old_elo, opponent_old_elo)
                    winner_new_elo = elo_result['winner_new_elo']
                    opponent_new_elo = elo_result['loser_new_elo']
                else:
                    elo_result = {
                        'winner_old_elo': winner_old_elo,
                        'winner_new_elo': winner_old_elo,
                        'winner_delta': 0,
                        'winner_expected_score': 0,
                        'loser_old_elo': opponent_old_elo,
                        'loser_new_elo': opponent_old_elo,
                        'loser_delta': 0,
                        'loser_expected_score': 0,
                        'k_factor': 0,
                    }
                    winner_new_elo = winner_old_elo
                    opponent_new_elo = opponent_old_elo

                cursor.execute(
                    '''
                    UPDATE players
                    SET current_elo = %s,
                        matches_count = matches_count + 1,
                        wins = wins + 1,
                        last_match_at = %s,
                        updated_at = NOW()
                    WHERE id = %s;
                    ''',
                    (winner_new_elo, played_at, winner_player['id']),
                )
                cursor.execute(
                    '''
                    UPDATE players
                    SET current_elo = %s,
                        matches_count = matches_count + 1,
                        losses = losses + 1,
                        last_match_at = %s,
                        updated_at = NOW()
                    WHERE id = %s;
                    ''',
                    (opponent_new_elo, played_at, opponent_player['id']),
                )

                if ranked_match:
                    cursor.execute(
                        '''
                        INSERT INTO rating_history (
                            match_id,
                            player_id,
                            old_elo,
                            new_elo,
                            elo_delta,
                            expected_score,
                            actual_score,
                            k_factor,
                            created_at
                        )
                        VALUES
                            (%s, %s, %s, %s, %s, %s, 1, %s, NOW()),
                            (%s, %s, %s, %s, %s, %s, 0, %s, NOW());
                        ''',
                        (
                            match_row['id'],
                            winner_player['id'],
                            elo_result['winner_old_elo'],
                            elo_result['winner_new_elo'],
                            elo_result['winner_delta'],
                            elo_result['winner_expected_score'],
                            elo_result['k_factor'],
                            match_row['id'],
                            opponent_player['id'],
                            elo_result['loser_old_elo'],
                            elo_result['loser_new_elo'],
                            elo_result['loser_delta'],
                            elo_result['loser_expected_score'],
                            elo_result['k_factor'],
                        ),
                    )

                _refresh_priority_race(cursor, winner_player['id'])
                _refresh_priority_race(cursor, opponent_player['id'])

            connection.commit()
        except Exception:
            connection.rollback()
            raise

    return {
        'match_id': match_row['id'],
        'played_at_label': _format_match_datetime(match_row.get('played_at') or played_at),
        'winner_player_id': winner_player['id'],
        'winner_name': winner_player['name'],
        'winner_created': winner_player.get('created', False),
        'winner_race': _normalize_race_label(clean_winner_race),
        'winner_profile_url': f"/players/{winner_player['id']}",
        'opponent_player_id': opponent_player['id'],
        'opponent_name': opponent_player['name'],
        'opponent_created': opponent_player.get('created', False),
        'opponent_race': _normalize_race_label(clean_opponent_race),
        'opponent_profile_url': f"/players/{opponent_player['id']}",
        'is_ranked': ranked_match,
        'game_type': clean_game_type,
        'mission_name': clean_mission_name,
        'comment': clean_comment,
        'winner_old_elo_display': _normalize_elo_value(elo_result['winner_old_elo']),
        'winner_new_elo_display': _normalize_elo_value(elo_result['winner_new_elo']),
        'winner_delta_display': _format_delta(elo_result['winner_delta']),
        'opponent_old_elo_display': _normalize_elo_value(elo_result['loser_old_elo']),
        'opponent_new_elo_display': _normalize_elo_value(elo_result['loser_new_elo']),
        'opponent_delta_display': _format_delta(elo_result['loser_delta']),
    }



def fetch_player_admin(player_id: int) -> dict | None:
    with get_db_connection() as connection:
        expr = _get_player_select_expressions(connection)
        query = f'''
        SELECT
            p.id,
            p.name,
            p.current_elo,
            p.last_match_at,
            {expr['country_code_expr']} AS country_code,
            {expr['country_name_expr']} AS country_name,
            {expr['priority_race_expr']} AS priority_race,
            {expr['discord_url_expr']} AS discord_url,
            p.matches_count,
            p.wins,
            p.losses,
            p.created_at,
            p.is_active
        FROM players p
        WHERE p.id = %s
        LIMIT 1;
        '''
        with connection.cursor() as cursor:
            cursor.execute(query, (player_id,))
            row = cursor.fetchone()

    if not row:
        return None

    player = _prepare_player_row(row)
    player['created_at_label'] = _format_match_datetime(player.get('created_at'))
    player['current_elo_input'] = int(row.get('current_elo') or 1000)
    player['is_active'] = bool(row.get('is_active', True))
    return player



def update_player_admin(
    *,
    player_id: int,
    name: str,
    country_code: str,
    country_name: str,
    discord_url: str,
    priority_race: str,
    current_elo,
    is_active,
) -> dict:
    clean_name = _normalize_player_name(name)
    clean_key = _normalize_player_key(name)
    clean_country_name = _normalize_text(country_name)
    clean_country_code = _resolve_country_code(country_code, clean_country_name)
    clean_discord_url = _normalize_text(discord_url)
    clean_priority_race = _normalize_text(priority_race)

    if not clean_name:
        raise ValueError('Enter the player name.')
    if clean_priority_race and clean_priority_race not in RACE_OPTIONS:
        raise ValueError('Choose a valid priority race.')

    try:
        clean_current_elo = max(0, int(current_elo))
    except (TypeError, ValueError):
        raise ValueError('ELO must be a whole number.')

    active_value = bool(is_active)

    with get_db_connection() as connection:
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    '''
                    SELECT id
                    FROM players
                    WHERE name_normalized = %s
                      AND id <> %s
                    LIMIT 1;
                    ''',
                    (clean_key, player_id),
                )
                existing = cursor.fetchone()
                if existing:
                    raise ValueError('Another player already has this name.')

                player_columns = _get_player_column_names(connection)
                assignments = [
                    'name = %s',
                    'name_normalized = %s',
                    'current_elo = %s',
                ]
                params = [
                    clean_name,
                    clean_key,
                    clean_current_elo,
                ]

                if 'country_code' in player_columns:
                    assignments.append("country_code = NULLIF(%s, '')")
                    params.append(clean_country_code)

                if 'discord_url' in player_columns:
                    assignments.append("discord_url = NULLIF(%s, '')")
                    params.append(clean_discord_url)

                if 'priority_race' in player_columns:
                    assignments.append("priority_race = NULLIF(%s, '')")
                    params.append(clean_priority_race)

                if 'is_active' in player_columns:
                    assignments.append('is_active = %s')
                    params.append(active_value)

                assignments.append('updated_at = NOW()')
                params.append(player_id)

                query = f'''
                    UPDATE players
                    SET {", ".join(assignments)}
                    WHERE id = %s
                    RETURNING id;
                '''
                cursor.execute(query, tuple(params))
                row = cursor.fetchone()
                if not row:
                    raise ValueError('Player not found.')
            connection.commit()
        except Exception:
            connection.rollback()
            raise

    updated = fetch_player_admin(player_id)
    if not updated:
        raise ValueError('Player not found after update.')
    return updated



def fetch_match_admin(match_id: int) -> dict | None:
    with get_db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                '''
                SELECT
                    m.id,
                    m.played_at,
                    m.player1_id,
                    p1.name AS player1_name,
                    m.player2_id,
                    p2.name AS player2_name,
                    m.winner_player_id,
                    CASE
                        WHEN m.winner_player_id = m.player1_id THEN 'player1'
                        ELSE 'player2'
                    END AS winner_side,
                    m.player1_race,
                    m.player2_race,
                    m.is_ranked,
                    m.game_type,
                    m.mission_name,
                    m.comment
                FROM matches m
                JOIN players p1 ON p1.id = m.player1_id
                JOIN players p2 ON p2.id = m.player2_id
                WHERE m.id = %s
                LIMIT 1;
                ''',
                (match_id,),
            )
            row = cursor.fetchone()

    if not row:
        return None

    match = dict(row)
    match['player1_name'] = _normalize_player_name(match.get('player1_name'))
    match['player2_name'] = _normalize_player_name(match.get('player2_name'))
    match['player1_race'] = _normalize_race_label(match.get('player1_race'))
    match['player2_race'] = _normalize_race_label(match.get('player2_race'))
    match['game_type'] = _normalize_text(match.get('game_type'))
    match['mission_name'] = _normalize_text(match.get('mission_name'))
    match['comment'] = _normalize_text(match.get('comment'))
    match['played_at_label'] = _format_match_datetime(match.get('played_at'))
    played_at = match.get('played_at')
    if played_at:
        match['played_at_input'] = played_at.strftime('%Y-%m-%dT%H:%M')
    else:
        match['played_at_input'] = ''
    return match



def _rebuild_ratings_and_player_stats(cursor) -> None:
    cursor.execute('DELETE FROM rating_history;')
    cursor.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'players';
        """
    )
    player_columns = {row['column_name'] for row in cursor.fetchall()}

    reset_assignments = [
        'current_elo = 1000',
        'matches_count = 0',
        'wins = 0',
        'losses = 0',
        'last_match_at = NULL',
        'updated_at = NOW()',
    ]
    if 'priority_race' in player_columns:
        reset_assignments.append('priority_race = NULL')

    cursor.execute(
        f'''
        UPDATE players
        SET {", ".join(reset_assignments)};
        '''
    )
    cursor.execute(
        '''
        SELECT id, player1_id, player2_id, winner_player_id, played_at, is_ranked
        FROM matches
        ORDER BY played_at ASC, id ASC;
        '''
    )
    matches = cursor.fetchall()

    player_state: dict[int, dict] = defaultdict(lambda: {
        'elo': 1000,
        'matches_count': 0,
        'wins': 0,
        'losses': 0,
        'last_match_at': None,
    })
    rating_rows = []
    touched_players: set[int] = set()

    for row in matches:
        match = dict(row)
        match_id = int(match['id'])
        player1_id = int(match['player1_id'])
        player2_id = int(match['player2_id'])
        winner_id = int(match['winner_player_id'])
        played_at = match.get('played_at')
        ranked_match = bool(match.get('is_ranked'))

        if winner_id not in {player1_id, player2_id}:
            raise ValueError(f'Match {match_id} has invalid winner_player_id.')

        loser_id = player2_id if winner_id == player1_id else player1_id
        winner_state = player_state[winner_id]
        loser_state = player_state[loser_id]

        winner_old_elo = int(winner_state['elo'])
        loser_old_elo = int(loser_state['elo'])

        if ranked_match:
            elo_result = _calculate_elo_result(winner_old_elo, loser_old_elo)
            winner_new_elo = elo_result['winner_new_elo']
            loser_new_elo = elo_result['loser_new_elo']
            rating_rows.append(
                (
                    match_id,
                    winner_id,
                    elo_result['winner_old_elo'],
                    elo_result['winner_new_elo'],
                    elo_result['winner_delta'],
                    elo_result['winner_expected_score'],
                    1,
                    elo_result['k_factor'],
                )
            )
            rating_rows.append(
                (
                    match_id,
                    loser_id,
                    elo_result['loser_old_elo'],
                    elo_result['loser_new_elo'],
                    elo_result['loser_delta'],
                    elo_result['loser_expected_score'],
                    0,
                    elo_result['k_factor'],
                )
            )
        else:
            winner_new_elo = winner_old_elo
            loser_new_elo = loser_old_elo

        winner_state['elo'] = winner_new_elo
        winner_state['matches_count'] += 1
        winner_state['wins'] += 1
        winner_state['last_match_at'] = played_at

        loser_state['elo'] = loser_new_elo
        loser_state['matches_count'] += 1
        loser_state['losses'] += 1
        loser_state['last_match_at'] = played_at

        touched_players.add(winner_id)
        touched_players.add(loser_id)

    if rating_rows:
        cursor.executemany(
            '''
            INSERT INTO rating_history (
                match_id,
                player_id,
                old_elo,
                new_elo,
                elo_delta,
                expected_score,
                actual_score,
                k_factor,
                created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW());
            ''',
            rating_rows,
        )

    if player_state:
        updates = []
        for player_id, state in player_state.items():
            updates.append(
                (
                    int(state['elo']),
                    int(state['matches_count']),
                    int(state['wins']),
                    int(state['losses']),
                    state['last_match_at'],
                    player_id,
                )
            )

        cursor.executemany(
            '''
            UPDATE players
            SET current_elo = %s,
                matches_count = %s,
                wins = %s,
                losses = %s,
                last_match_at = %s,
                updated_at = NOW()
            WHERE id = %s;
            ''',
            updates,
        )

    for player_id in touched_players:
        _refresh_priority_race(cursor, player_id)



def update_match_admin(
    *,
    match_id: int,
    player1_name: str,
    player2_name: str,
    winner_side: str,
    player1_race: str,
    player2_race: str,
    is_ranked,
    game_type: str,
    mission_name: str,
    comment: str,
    played_at: datetime,
) -> dict:
    clean_player1_name = _normalize_player_name(player1_name)
    clean_player2_name = _normalize_player_name(player2_name)
    clean_player1_race = _normalize_text(player1_race)
    clean_player2_race = _normalize_text(player2_race)
    ranked_match = _coerce_ranked_value(is_ranked)
    clean_game_type = _normalize_text(game_type)
    clean_mission_name = _normalize_player_name(mission_name)
    clean_comment = _normalize_text(comment)
    clean_winner_side = _normalize_text(winner_side)

    if not clean_player1_name:
        raise ValueError('Enter player 1 name.')
    if not clean_player2_name:
        raise ValueError('Enter player 2 name.')
    if _normalize_player_key(clean_player1_name) == _normalize_player_key(clean_player2_name):
        raise ValueError('Players must be different.')
    if clean_player1_race not in RACE_OPTIONS:
        raise ValueError('Choose player 1 race.')
    if clean_player2_race not in RACE_OPTIONS:
        raise ValueError('Choose player 2 race.')
    if clean_game_type not in GAME_TYPE_OPTIONS:
        raise ValueError('Choose the game type.')
    if not clean_mission_name:
        raise ValueError('Choose the mission.')
    if clean_winner_side not in {'player1', 'player2'}:
        raise ValueError('Choose the winner side.')
    if not isinstance(played_at, datetime):
        raise ValueError('Enter a valid played at date and time.')
    if len(clean_comment) > 4000:
        raise ValueError('Comment is too long.')

    with get_db_connection() as connection:
        try:
            with connection.cursor() as cursor:
                cursor.execute('SELECT id FROM matches WHERE id = %s LIMIT 1;', (match_id,))
                existing = cursor.fetchone()
                if not existing:
                    raise ValueError('Match not found.')

                player1 = _get_or_create_player(cursor, clean_player1_name)
                player2 = _get_or_create_player(cursor, clean_player2_name)
                winner_player_id = player1['id'] if clean_winner_side == 'player1' else player2['id']

                cursor.execute(
                    '''
                    UPDATE matches
                    SET player1_id = %s,
                        player2_id = %s,
                        winner_player_id = %s,
                        played_at = %s,
                        comment = %s,
                        player1_race = %s,
                        player2_race = %s,
                        is_ranked = %s,
                        game_type = %s,
                        mission_name = %s
                    WHERE id = %s;
                    ''',
                    (
                        player1['id'],
                        player2['id'],
                        winner_player_id,
                        played_at,
                        clean_comment or None,
                        clean_player1_race,
                        clean_player2_race,
                        ranked_match,
                        clean_game_type,
                        clean_mission_name,
                        match_id,
                    ),
                )
                _rebuild_ratings_and_player_stats(cursor)
            connection.commit()
        except Exception:
            connection.rollback()
            raise

    updated = fetch_match_admin(match_id)
    if not updated:
        raise ValueError('Match not found after update.')
    return updated



def delete_match_admin(match_id: int) -> None:
    connection = None
    try:
        with get_db_connection() as db_connection:
            connection = db_connection
            with connection.cursor() as cursor:
                cursor.execute('SELECT id FROM matches WHERE id = %s LIMIT 1;', (match_id,))
                existing = cursor.fetchone()
                if not existing:
                    raise ValueError('Match not found.')

                cursor.execute('DELETE FROM rating_history WHERE match_id = %s;', (match_id,))
                cursor.execute('DELETE FROM matches WHERE id = %s;', (match_id,))
                _rebuild_ratings_and_player_stats(cursor)

            connection.commit()
    except Exception:
        if connection is not None:
            try:
                connection.rollback()
            except Exception:
                pass
        raise
