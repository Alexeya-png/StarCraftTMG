
from __future__ import annotations

import json
import math
import os
import re
import threading
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib import error as urllib_error
from urllib import request as urllib_request
from urllib.parse import urlencode

from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
FLAGS_DIR = BASE_DIR / 'static' / 'Flags'
ALLOWED_FLAG_EXTENSIONS = {'.png', '.jpg', '.jpeg', '.svg', '.webp'}


class DatabaseConfigError(RuntimeError):
    pass


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

RACE_OPTIONS = ('Терран', 'Протосс', 'Зерг')
GAME_TYPE_OPTIONS = ('1к', '2к', 'Grand Offensive')
DEFAULT_K_FACTOR = 32
TOP_PLAYER_K_FACTOR = 10
HIGH_RATING_K_FACTOR = 16
BASE_RATING_K_FACTOR = 32
NEW_PLAYER_K_FACTOR = 40
NEW_PLAYER_MATCHES_LIMIT = 5
CALIBRATION_MATCHES_REQUIRED = 3
DATA_CACHE_TTL_SECONDS = max(0, int(os.getenv('APP_DATA_CACHE_TTL_SECONDS', '300') or '300'))

_DATA_CACHE_LOCK = threading.RLock()
_DATA_CACHE: dict[str, Any] = {
    'players': None,
    'matches': None,
    'rating_history': None,
    'loaded_at': 0.0,
    'version': 0,
}



def _normalize_text(value: str | None) -> str:
    if value is None:
        return ''
    return str(value).strip()


def _slugify(value: str | None) -> str:
    clean_value = _normalize_text(value).lower()
    if not clean_value:
        return ''
    return re.sub(r'[^a-zа-яё0-9]+', '', clean_value)


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


def _normalize_race_label(value: str | None) -> str:
    clean_value = _normalize_text(value)
    if not clean_value:
        return ''
    return RACE_LABELS.get(clean_value, clean_value)


def _normalize_discord_url(value: str | None) -> str:
    clean_value = _normalize_text(value)
    if not clean_value:
        return ''

    if re.match(r'^[a-z][a-z0-9+.-]*://', clean_value, re.IGNORECASE):
        return clean_value

    return f'https://{clean_value}'


def _normalize_elo_value(value) -> str:
    if value is None:
        return '—'
    try:
        return str(int(value))
    except (TypeError, ValueError):
        return str(value)


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


def _parse_datetime(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    except ValueError:
        return None


def _format_match_datetime(value) -> str:
    parsed = _parse_datetime(value)
    if not parsed:
        return '—'
    return parsed.strftime('%Y-%m-%d %H:%M')


def _format_match_date(value) -> str:
    parsed = _parse_datetime(value)
    if not parsed:
        return '—'
    return parsed.strftime('%Y-%m-%d')


def _humanize_last_played(value) -> str:
    parsed = _parse_datetime(value)
    if not parsed:
        return '—'

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


def _determine_k_factor(current_elo: int, matches_played_before_match: int) -> int:
    if int(matches_played_before_match or 0) < NEW_PLAYER_MATCHES_LIMIT:
        return NEW_PLAYER_K_FACTOR
    if int(current_elo or 0) >= 2400:
        return TOP_PLAYER_K_FACTOR
    if int(current_elo or 0) >= 2000:
        return HIGH_RATING_K_FACTOR
    return BASE_RATING_K_FACTOR


def _calculate_elo_result(
    winner_elo: int,
    loser_elo: int,
    winner_matches_played_before_match: int = 0,
    loser_matches_played_before_match: int = 0,
) -> dict:
    expected_winner = _calculate_expected_score(winner_elo, loser_elo)
    expected_loser = _calculate_expected_score(loser_elo, winner_elo)

    winner_k_factor = _determine_k_factor(winner_elo, winner_matches_played_before_match)
    loser_k_factor = _determine_k_factor(loser_elo, loser_matches_played_before_match)

    winner_delta = int(round(winner_k_factor * (1 - expected_winner)))
    loser_delta = int(round(loser_k_factor * (0 - expected_loser)))

    winner_new = max(0, winner_elo + winner_delta)
    loser_new = max(0, loser_elo + loser_delta)

    return {
        'winner_old_elo': winner_elo,
        'winner_new_elo': winner_new,
        'winner_delta': winner_delta,
        'winner_expected_score': expected_winner,
        'winner_k_factor': winner_k_factor,
        'loser_old_elo': loser_elo,
        'loser_new_elo': loser_new,
        'loser_delta': loser_delta,
        'loser_expected_score': expected_loser,
        'loser_k_factor': loser_k_factor,
        'k_factor': max(winner_k_factor, loser_k_factor),
    }


def _get_supabase_settings() -> dict[str, str]:
    explicit_url = _normalize_text(os.getenv('SUPABASE_URL'))
    project_ref = ''

    if explicit_url:
        url = explicit_url.rstrip('/')
    else:
        user_value = _normalize_text(os.getenv('user'))
        match = re.match(r'^postgres\.([a-z0-9]+)$', user_value)
        if match:
            project_ref = match.group(1)

        if not project_ref:
            project_ref = _normalize_text(os.getenv('SUPABASE_PROJECT_REF'))

        if not project_ref:
            raise DatabaseConfigError(
                'Set SUPABASE_URL or SUPABASE_PROJECT_REF in .env. '
                'For Supabase HTTP mode you also need SUPABASE_SERVICE_ROLE_KEY.'
            )
        url = f'https://{project_ref}.supabase.co'

    key = (
        _normalize_text(os.getenv('SUPABASE_SERVICE_ROLE_KEY'))
        or _normalize_text(os.getenv('SUPABASE_KEY'))
        or _normalize_text(os.getenv('SUPABASE_ANON_KEY'))
    )
    if not key:
        raise DatabaseConfigError(
            'Set SUPABASE_SERVICE_ROLE_KEY or SUPABASE_KEY in .env for HTTP access.'
        )

    return {
        'url': url,
        'key': key,
        'schema': _normalize_text(os.getenv('SUPABASE_SCHEMA')) or 'public',
    }


def _encode_filter_value(value) -> str:
    if value is None:
        return 'null'
    if isinstance(value, bool):
        return 'true' if value else 'false'
    return str(value)


def _supabase_request(
    method: str,
    path: str,
    *,
    query: dict[str, Any] | None = None,
    payload: Any | None = None,
    prefer: str | None = None,
    return_headers: bool = False,
):
    settings = _get_supabase_settings()
    query = query or {}

    parts = []
    for key, value in query.items():
        if value is None:
            continue
        if isinstance(value, list):
            for item in value:
                parts.append((key, item))
        else:
            parts.append((key, value))

    query_string = urlencode(parts, doseq=True, safe='(),.*:+')
    url = f"{settings['url']}{path}"
    if query_string:
        url = f'{url}?{query_string}'

    headers = {
        'apikey': settings['key'],
        'Authorization': f"Bearer {settings['key']}",
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Accept-Profile': settings['schema'],
        'Content-Profile': settings['schema'],
    }
    if prefer:
        headers['Prefer'] = prefer

    data = None
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode('utf-8')

    req = urllib_request.Request(url, data=data, headers=headers, method=method.upper())
    try:
        with urllib_request.urlopen(req, timeout=30) as response:
            raw_body = response.read()
            text_body = raw_body.decode('utf-8') if raw_body else ''
            if text_body:
                try:
                    body = json.loads(text_body)
                except json.JSONDecodeError:
                    body = text_body
            else:
                body = None
            if return_headers:
                return body, dict(response.headers)
            return body
    except urllib_error.HTTPError as exc:
        error_text = exc.read().decode('utf-8', errors='ignore')
        try:
            payload = json.loads(error_text)
            message = payload.get('message') or payload.get('hint') or payload.get('details') or error_text
        except json.JSONDecodeError:
            message = error_text or str(exc)
        raise RuntimeError(f'Supabase HTTP {exc.code}: {message}') from None


def _rest_select(
    table: str,
    *,
    select: str = '*',
    filters: list[tuple[str, str, Any]] | None = None,
    order: str | None = None,
    limit: int | None = None,
    offset: int | None = None,
    single: bool = False,
    count: bool = False,
):
    query: dict[str, Any] = {'select': select}
    if order:
        query['order'] = order
    if limit is not None:
        query['limit'] = int(limit)
    if offset is not None:
        query['offset'] = int(offset)

    for field, op, value in filters or []:
        if op == 'in':
            encoded = ','.join(_encode_filter_value(item) for item in value)
            query[field] = f'in.({encoded})'
        elif op == 'is':
            query[field] = f'is.{_encode_filter_value(value)}'
        else:
            query[field] = f'{op}.{_encode_filter_value(value)}'

    prefer = 'count=exact' if count else None
    body, headers = _supabase_request(
        'GET',
        f'/rest/v1/{table}',
        query=query,
        prefer=prefer,
        return_headers=True,
    )

    if single:
        if isinstance(body, list):
            return body[0] if body else None
        return body

    if count:
        content_range = headers.get('Content-Range', '')
        total = 0
        if '/' in content_range:
            try:
                total = int(content_range.rsplit('/', 1)[1])
            except ValueError:
                total = 0
        return body or [], total

    return body or []


def _rest_fetch_all(
    table: str,
    *,
    select: str = '*',
    filters: list[tuple[str, str, Any]] | None = None,
    order: str | None = None,
    page_size: int = 1000,
) -> list[dict]:
    items: list[dict] = []
    offset = 0

    while True:
        batch = _rest_select(
            table,
            select=select,
            filters=filters,
            order=order,
            limit=page_size,
            offset=offset,
        )
        if not batch:
            break
        items.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size

    return items


def _rest_insert(table: str, payload: Any, *, upsert: bool = False):
    prefer = 'return=representation'
    if upsert:
        prefer = 'resolution=merge-duplicates,return=representation'
    return _supabase_request('POST', f'/rest/v1/{table}', payload=payload, prefer=prefer)


def _rest_update(table: str, payload: dict, *, filters: list[tuple[str, str, Any]]):
    query: dict[str, Any] = {}
    for field, op, value in filters:
        if op == 'in':
            encoded = ','.join(_encode_filter_value(item) for item in value)
            query[field] = f'in.({encoded})'
        elif op == 'is':
            query[field] = f'is.{_encode_filter_value(value)}'
        else:
            query[field] = f'{op}.{_encode_filter_value(value)}'
    return _supabase_request(
        'PATCH',
        f'/rest/v1/{table}',
        query=query,
        payload=payload,
        prefer='return=representation',
    )


def _rest_delete(table: str, *, filters: list[tuple[str, str, Any]] | None = None):
    query: dict[str, Any] = {}
    for field, op, value in filters or []:
        if op == 'in':
            encoded = ','.join(_encode_filter_value(item) for item in value)
            query[field] = f'in.({encoded})'
        elif op == 'is':
            query[field] = f'is.{_encode_filter_value(value)}'
        else:
            query[field] = f'{op}.{_encode_filter_value(value)}'
    return _supabase_request(
        'DELETE',
        f'/rest/v1/{table}',
        query=query,
        prefer='return=representation',
    )


def _cache_is_fresh() -> bool:
    loaded_at = float(_DATA_CACHE.get('loaded_at') or 0.0)
    if loaded_at <= 0:
        return False
    if DATA_CACHE_TTL_SECONDS <= 0:
        return False
    return (time.time() - loaded_at) < DATA_CACHE_TTL_SECONDS


def invalidate_application_cache() -> None:
    with _DATA_CACHE_LOCK:
        _DATA_CACHE['players'] = None
        _DATA_CACHE['matches'] = None
        _DATA_CACHE['rating_history'] = None
        _DATA_CACHE['loaded_at'] = 0.0
        _DATA_CACHE['version'] = int(_DATA_CACHE.get('version') or 0) + 1


def warmup_application_cache(*, force_refresh: bool = False) -> dict[str, Any]:
    with _DATA_CACHE_LOCK:
        if not force_refresh and _cache_is_fresh() and all(_DATA_CACHE.get(key) is not None for key in ('players', 'matches', 'rating_history')):
            return {
                'players': [dict(row) for row in _DATA_CACHE['players']],
                'matches': [dict(row) for row in _DATA_CACHE['matches']],
                'rating_history': [dict(row) for row in _DATA_CACHE['rating_history']],
                'version': int(_DATA_CACHE.get('version') or 0),
            }

    players = _rest_fetch_all('players', order='id.asc')
    matches = _rest_fetch_all('matches', order='played_at.asc,id.asc')
    rating_history = _rest_fetch_all('rating_history', order='id.asc')
    loaded_at = time.time()

    with _DATA_CACHE_LOCK:
        _DATA_CACHE['players'] = [dict(row) for row in players]
        _DATA_CACHE['matches'] = [dict(row) for row in matches]
        _DATA_CACHE['rating_history'] = [dict(row) for row in rating_history]
        _DATA_CACHE['loaded_at'] = loaded_at
        version = int(_DATA_CACHE.get('version') or 0)
        return {
            'players': [dict(row) for row in _DATA_CACHE['players']],
            'matches': [dict(row) for row in _DATA_CACHE['matches']],
            'rating_history': [dict(row) for row in _DATA_CACHE['rating_history']],
            'version': version,
        }


def _cache_snapshot(*, force_refresh: bool = False) -> dict[str, Any]:
    return warmup_application_cache(force_refresh=force_refresh)


def ping_database() -> tuple[bool, str | None]:
    try:
        _rest_select('players', select='id', limit=1)
        return True, None
    except Exception as exc:
        return False, str(exc)


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


def _prepare_game_report_row(row: dict) -> dict:
    match = dict(row)
    match['played_at_label'] = _format_match_date(match.get('played_at'))
    match['winner_race'] = _normalize_race_label(match.get('winner_race'))
    match['loser_race'] = _normalize_race_label(match.get('loser_race'))
    match['winner_profile_url'] = f"/players/{match['winner_id']}"
    match['loser_profile_url'] = f"/players/{match['loser_id']}"
    match['winner_elo_delta_display'] = _format_delta(match.get('winner_elo_delta'))
    match['loser_elo_delta_display'] = _format_delta(match.get('loser_elo_delta'))
    match['winner_rating_display'] = (
        f"{_normalize_elo_value(match.get('winner_old_elo'))} → {_normalize_elo_value(match.get('winner_new_elo'))}"
        if match.get('winner_old_elo') is not None and match.get('winner_new_elo') is not None
        else '—'
    )
    match['loser_rating_display'] = (
        f"{_normalize_elo_value(match.get('loser_old_elo'))} → {_normalize_elo_value(match.get('loser_new_elo'))}"
        if match.get('loser_old_elo') is not None and match.get('loser_new_elo') is not None
        else '—'
    )
    match['ranked_label'] = 'Ranked' if match.get('is_ranked') else 'Unranked'
    match['comment_display'] = _normalize_text(match.get('comment')) or '—'
    match['game_type_display'] = _normalize_text(match.get('game_type')) or '—'
    match['mission_name_display'] = _normalize_text(match.get('mission_name')) or '—'
    return match


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


def _fetch_all_players_raw(*, force_refresh: bool = False) -> list[dict]:
    snapshot = _cache_snapshot(force_refresh=force_refresh)
    return [dict(row) for row in snapshot['players']]


def _fetch_all_matches_raw(*, force_refresh: bool = False) -> list[dict]:
    snapshot = _cache_snapshot(force_refresh=force_refresh)
    return [dict(row) for row in snapshot['matches']]


def _fetch_all_rating_history_raw(*, force_refresh: bool = False) -> list[dict]:
    snapshot = _cache_snapshot(force_refresh=force_refresh)
    return [dict(row) for row in snapshot['rating_history']]


def fetch_player_name_suggestions(limit: int = 500) -> list[str]:
    safe_limit = max(1, min(limit, 2000))
    players = _fetch_all_players_raw()
    players.sort(key=lambda row: (-int(row.get('current_elo') or 0), _normalize_player_name(row.get('name'))))
    return [_normalize_player_name(row.get('name')) for row in players[:safe_limit] if _normalize_player_name(row.get('name'))]


def fetch_mission_suggestions(limit: int = 50) -> list[str]:
    safe_limit = max(1, min(limit, 200))
    missions = []
    seen = set()
    for row in _fetch_all_matches_raw():
        mission = _normalize_player_name(row.get('mission_name'))
        if mission and mission not in seen:
            seen.add(mission)
            missions.append(mission)
    missions.sort()
    return missions[:safe_limit]


def fetch_leaderboard(
    search: str = '',
    *,
    include_active: bool = True,
    include_inactive: bool = False,
) -> list[dict]:
    normalized_search = search.strip().casefold()

    if not include_active and not include_inactive:
        include_active = True

    rows = []
    for row in _fetch_all_players_raw():
        is_active = bool(row.get('is_active', True))
        if include_active and include_inactive:
            allowed = True
        elif include_active:
            allowed = is_active
        else:
            allowed = not is_active
        if not allowed:
            continue

        player_name = _normalize_player_name(row.get('name'))
        if normalized_search and normalized_search not in player_name.casefold():
            continue

        matches_count = int(row.get('matches_count') or 0)
        wins = int(row.get('wins') or 0)
        win_rate = round((wins / matches_count) * 100, 1) if matches_count > 0 else 0
        prepared = dict(row)
        prepared['matches_count'] = matches_count
        prepared['wins'] = wins
        prepared['losses'] = int(row.get('losses') or 0)
        prepared['win_rate'] = win_rate
        rows.append(prepared)

    rows.sort(
        key=lambda row: (
            -int(row.get('current_elo') or 0),
            -int(row.get('wins') or 0),
            -int(row.get('matches_count') or 0),
            _normalize_player_name(row.get('name')).casefold(),
        )
    )

    prepared_rows = []
    for index, row in enumerate(rows, start=1):
        row = dict(row)
        row['rank_position'] = index
        prepared_rows.append(_prepare_player_row(row))
    return prepared_rows


def fetch_game_reports_page(search: str = '', page: int = 1, per_page: int = 25) -> dict:
    safe_per_page = max(1, min(int(per_page or 25), 100))
    safe_page = max(1, int(page or 1))
    normalized_search = search.strip().casefold()

    players_by_id = {int(row['id']): row for row in _fetch_all_players_raw()}
    history_rows = _fetch_all_rating_history_raw()
    history_by_pair = {(int(row['match_id']), int(row['player_id'])): row for row in history_rows}

    items = []
    for row in _fetch_all_matches_raw():
        match = dict(row)
        player1_id = int(match['player1_id'])
        player2_id = int(match['player2_id'])
        winner_id = int(match['winner_player_id'])
        loser_id = player2_id if winner_id == player1_id else player1_id

        winner = players_by_id.get(winner_id)
        loser = players_by_id.get(loser_id)
        if not winner or not loser:
            continue

        winner_name = _normalize_player_name(winner.get('name'))
        loser_name = _normalize_player_name(loser.get('name'))
        searchable = ' '.join(
            [
                winner_name,
                loser_name,
                _normalize_text(match.get('game_type')),
                _normalize_text(match.get('mission_name')),
                _normalize_text(match.get('comment')),
            ]
        ).casefold()

        if normalized_search and normalized_search not in searchable:
            continue

        winner_history = history_by_pair.get((int(match['id']), winner_id), {})
        loser_history = history_by_pair.get((int(match['id']), loser_id), {})

        item = {
            'id': int(match['id']),
            'played_at': match.get('played_at'),
            'winner_id': winner_id,
            'winner_name': winner_name,
            'loser_id': loser_id,
            'loser_name': loser_name,
            'winner_race': match.get('player1_race') if winner_id == player1_id else match.get('player2_race'),
            'loser_race': match.get('player2_race') if winner_id == player1_id else match.get('player1_race'),
            'is_ranked': bool(match.get('is_ranked')),
            'game_type': match.get('game_type'),
            'mission_name': match.get('mission_name'),
            'comment': match.get('comment'),
            'winner_old_elo': winner_history.get('old_elo'),
            'winner_new_elo': winner_history.get('new_elo'),
            'winner_elo_delta': winner_history.get('elo_delta'),
            'loser_old_elo': loser_history.get('old_elo'),
            'loser_new_elo': loser_history.get('new_elo'),
            'loser_elo_delta': loser_history.get('elo_delta'),
        }
        items.append(item)

    items.sort(key=lambda row: int(row['id']), reverse=True)

    total_count = len(items)
    total_pages = max(1, math.ceil(total_count / safe_per_page)) if total_count else 1
    safe_page = min(safe_page, total_pages)
    start = (safe_page - 1) * safe_per_page
    end = start + safe_per_page
    page_items = [_prepare_game_report_row(row) for row in items[start:end]]

    return {
        'items': page_items,
        'total_count': total_count,
        'page': safe_page,
        'per_page': safe_per_page,
        'total_pages': total_pages,
    }


def fetch_game_reports(search: str = '', limit: int = 100) -> list[dict]:
    page_data = fetch_game_reports_page(search=search, page=1, per_page=limit)
    return page_data['items']


def fetch_player_profile(player_id: int, recent_matches_limit: int = 20) -> dict | None:
    safe_recent_matches_limit = max(1, min(recent_matches_limit, 50))
    players_by_id = {int(row['id']): row for row in _fetch_all_players_raw()}
    player_row = players_by_id.get(int(player_id))

    if not player_row or not bool(player_row.get('is_active', True)):
        return None

    leaderboard_rows = fetch_leaderboard(search='', include_active=True, include_inactive=False)
    leaderboard_by_id = {int(row['id']): row for row in leaderboard_rows}
    player = leaderboard_by_id.get(int(player_id))
    if not player:
        return None

    history_rows = _fetch_all_rating_history_raw()
    history_by_pair = {(int(row['match_id']), int(row['player_id'])): row for row in history_rows}
    matches = _fetch_all_matches_raw()

    recent_matches = []
    rating_chart_rows = []

    for match in matches:
        match_id = int(match['id'])
        player1_id = int(match['player1_id'])
        player2_id = int(match['player2_id'])
        if int(player_id) not in {player1_id, player2_id}:
            continue

        played_at = match.get('played_at')
        history_row = history_by_pair.get((match_id, int(player_id)))
        if history_row:
            rating_chart_rows.append(
                {
                    'played_at': played_at,
                    'old_elo': history_row.get('old_elo'),
                    'new_elo': history_row.get('new_elo'),
                    'elo_delta': history_row.get('elo_delta'),
                }
            )

        opponent_id = player2_id if player1_id == int(player_id) else player1_id
        opponent = players_by_id.get(opponent_id)
        if not opponent:
            continue

        is_win = int(match['winner_player_id']) == int(player_id)
        recent_matches.append(
            {
                'id': match_id,
                'played_at': played_at,
                'opponent_id': opponent_id,
                'opponent_name': _normalize_player_name(opponent.get('name')),
                'result_label': 'Win' if is_win else 'Loss',
                'is_win': is_win,
                'player_race': match.get('player1_race') if player1_id == int(player_id) else match.get('player2_race'),
                'opponent_race': match.get('player2_race') if player1_id == int(player_id) else match.get('player1_race'),
                'old_elo': history_row.get('old_elo') if history_row else None,
                'new_elo': history_row.get('new_elo') if history_row else None,
                'elo_delta': history_row.get('elo_delta') if history_row else None,
                'opponent_profile_url': f"/players/{opponent_id}",
            }
        )

    recent_matches.sort(key=lambda row: (_parse_datetime(row.get('played_at')) or datetime.min, int(row['id'])), reverse=True)
    recent_matches = recent_matches[:safe_recent_matches_limit]
    for row in recent_matches:
        row['played_at_label'] = _format_match_date(row.get('played_at'))
        row['player_race'] = _normalize_race_label(row.get('player_race'))
        row['opponent_race'] = _normalize_race_label(row.get('opponent_race'))
        row['old_elo_display'] = _normalize_elo_value(row.get('old_elo'))
        row['new_elo_display'] = _normalize_elo_value(row.get('new_elo'))
        row['elo_delta_display'] = _format_delta(row.get('elo_delta'))

    rating_chart_rows.sort(key=lambda row: _parse_datetime(row.get('played_at')) or datetime.min)
    player['member_since_label'] = _format_match_datetime(player_row.get('created_at'))
    rating_chart = _build_rating_chart(player.get('current_elo'), rating_chart_rows)

    return {
        'player': player,
        'recent_matches': recent_matches,
        'rating_chart': rating_chart,
    }


def _rest_get_player_by_name_key(name_key: str) -> dict | None:
    return _rest_select('players', filters=[('name_normalized', 'eq', name_key)], single=True)


def _rest_get_player_by_id(player_id: int) -> dict | None:
    return _rest_select('players', filters=[('id', 'eq', player_id)], single=True)


def _get_or_create_player(player_name: str) -> dict:
    normalized_name = _normalize_player_name(player_name)
    normalized_key = _normalize_player_key(player_name)

    existing = _rest_get_player_by_name_key(normalized_key)
    if existing:
        updates = {}
        if _normalize_player_name(existing.get('name')) != normalized_name:
            updates['name'] = normalized_name
        if updates:
            updates['updated_at'] = datetime.utcnow().isoformat()
            rows = _rest_update('players', updates, filters=[('id', 'eq', existing['id'])])
            existing = rows[0] if rows else existing
        existing = dict(existing)
        existing['created'] = False
        return existing

    rows = _rest_insert(
        'players',
        {
            'name': normalized_name,
            'name_normalized': normalized_key,
            'is_active': False,
        },
    )
    created = rows[0] if isinstance(rows, list) else rows
    created = dict(created)
    created['created'] = True
    return created


def _refresh_priority_race(player_id: int, *, force_refresh: bool = False) -> None:
    match_rows = _fetch_all_matches_raw(force_refresh=force_refresh)
    counts: dict[str, int] = defaultdict(int)

    for row in match_rows:
        if int(row['player1_id']) == int(player_id) and _normalize_text(row.get('player1_race')):
            counts[_normalize_text(row.get('player1_race'))] += 1
        if int(row['player2_id']) == int(player_id) and _normalize_text(row.get('player2_race')):
            counts[_normalize_text(row.get('player2_race'))] += 1

    selected = ''
    if counts:
        selected = sorted(counts.items(), key=lambda item: (-item[1], item[0]))[0][0]

    _rest_update(
        'players',
        {
            'priority_race': selected or None,
            'updated_at': datetime.utcnow().isoformat(),
        },
        filters=[('id', 'eq', player_id)],
    )


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

    played_at = datetime.now().isoformat()

    winner_player = _get_or_create_player(clean_winner_name)
    opponent_player = _get_or_create_player(clean_opponent_name)

    winner_old_elo = int(winner_player.get('current_elo') or 1000)
    opponent_old_elo = int(opponent_player.get('current_elo') or 1000)

    winner_matches_before_match = int(winner_player.get('matches_count') or 0)
    opponent_matches_before_match = int(opponent_player.get('matches_count') or 0)

    if ranked_match:
        elo_result = _calculate_elo_result(
            winner_old_elo,
            opponent_old_elo,
            winner_matches_before_match,
            opponent_matches_before_match,
        )
        winner_new_elo = elo_result['winner_new_elo']
        opponent_new_elo = elo_result['loser_new_elo']
    else:
        elo_result = {
            'winner_old_elo': winner_old_elo,
            'winner_new_elo': winner_old_elo,
            'winner_delta': 0,
            'winner_expected_score': 0,
            'winner_k_factor': 0,
            'loser_old_elo': opponent_old_elo,
            'loser_new_elo': opponent_old_elo,
            'loser_delta': 0,
            'loser_expected_score': 0,
            'loser_k_factor': 0,
            'k_factor': 0,
        }
        winner_new_elo = winner_old_elo
        opponent_new_elo = opponent_old_elo

    match_rows = _rest_insert(
        'matches',
        {
            'player1_id': winner_player['id'],
            'player2_id': opponent_player['id'],
            'winner_player_id': winner_player['id'],
            'played_at': played_at,
            'comment': clean_comment or None,
            'player1_race': clean_winner_race,
            'player2_race': clean_opponent_race,
            'is_ranked': ranked_match,
            'game_type': clean_game_type,
            'mission_name': clean_mission_name,
        },
    )
    match_row = match_rows[0] if isinstance(match_rows, list) else match_rows

    winner_matches_after_match = winner_matches_before_match + 1
    opponent_matches_after_match = opponent_matches_before_match + 1

    _rest_update(
        'players',
        {
            'current_elo': winner_new_elo,
            'matches_count': winner_matches_after_match,
            'wins': int(winner_player.get('wins') or 0) + 1,
            'last_match_at': played_at,
            'is_active': winner_matches_after_match >= CALIBRATION_MATCHES_REQUIRED,
            'updated_at': datetime.utcnow().isoformat(),
        },
        filters=[('id', 'eq', winner_player['id'])],
    )
    _rest_update(
        'players',
        {
            'current_elo': opponent_new_elo,
            'matches_count': opponent_matches_after_match,
            'losses': int(opponent_player.get('losses') or 0) + 1,
            'last_match_at': played_at,
            'is_active': opponent_matches_after_match >= CALIBRATION_MATCHES_REQUIRED,
            'updated_at': datetime.utcnow().isoformat(),
        },
        filters=[('id', 'eq', opponent_player['id'])],
    )

    if ranked_match:
        _rest_insert(
            'rating_history',
            [
                {
                    'match_id': match_row['id'],
                    'player_id': winner_player['id'],
                    'old_elo': elo_result['winner_old_elo'],
                    'new_elo': elo_result['winner_new_elo'],
                    'elo_delta': elo_result['winner_delta'],
                    'expected_score': elo_result['winner_expected_score'],
                    'actual_score': 1,
                    'k_factor': elo_result['winner_k_factor'],
                },
                {
                    'match_id': match_row['id'],
                    'player_id': opponent_player['id'],
                    'old_elo': elo_result['loser_old_elo'],
                    'new_elo': elo_result['loser_new_elo'],
                    'elo_delta': elo_result['loser_delta'],
                    'expected_score': elo_result['loser_expected_score'],
                    'actual_score': 0,
                    'k_factor': elo_result['loser_k_factor'],
                },
            ],
        )

    _refresh_priority_race(winner_player['id'], force_refresh=True)
    _refresh_priority_race(opponent_player['id'])
    invalidate_application_cache()

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
    row = _rest_get_player_by_id(player_id)
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

    existing = _rest_get_player_by_name_key(clean_key)
    if existing and int(existing['id']) != int(player_id):
        raise ValueError('Another player already has this name.')

    rows = _rest_update(
        'players',
        {
            'name': clean_name,
            'name_normalized': clean_key,
            'current_elo': clean_current_elo,
            'country_code': clean_country_code or None,
            'discord_url': clean_discord_url or None,
            'priority_race': clean_priority_race or None,
            'is_active': active_value,
            'updated_at': datetime.utcnow().isoformat(),
        },
        filters=[('id', 'eq', player_id)],
    )
    if not rows:
        raise ValueError('Player not found.')

    invalidate_application_cache()
    updated = fetch_player_admin(player_id)
    if not updated:
        raise ValueError('Player not found after update.')
    return updated


def fetch_match_admin(match_id: int) -> dict | None:
    row = _rest_select('matches', filters=[('id', 'eq', match_id)], single=True)
    if not row:
        return None

    players_by_id = {int(player['id']): player for player in _fetch_all_players_raw()}
    player1 = players_by_id.get(int(row['player1_id']))
    player2 = players_by_id.get(int(row['player2_id']))
    if not player1 or not player2:
        return None

    match = dict(row)
    match['player1_name'] = _normalize_player_name(player1.get('name'))
    match['player2_name'] = _normalize_player_name(player2.get('name'))
    match['winner_side'] = 'player1' if int(row['winner_player_id']) == int(row['player1_id']) else 'player2'
    match['player1_race'] = _normalize_race_label(match.get('player1_race'))
    match['player2_race'] = _normalize_race_label(match.get('player2_race'))
    match['game_type'] = _normalize_text(match.get('game_type'))
    match['mission_name'] = _normalize_text(match.get('mission_name'))
    match['comment'] = _normalize_text(match.get('comment'))
    match['played_at_label'] = _format_match_datetime(match.get('played_at'))
    parsed = _parse_datetime(match.get('played_at'))
    match['played_at_input'] = parsed.strftime('%Y-%m-%dT%H:%M') if parsed else ''
    return match


def _rebuild_ratings_and_player_stats() -> None:
    players = _fetch_all_players_raw(force_refresh=True)
    matches = _fetch_all_matches_raw(force_refresh=True)

    _rest_delete('rating_history', filters=[('id', 'gt', 0)])

    player_state: dict[int, dict] = {
        int(player['id']): {
            'elo': 1000,
            'matches_count': 0,
            'wins': 0,
            'losses': 0,
            'last_match_at': None,
        }
        for player in players
    }

    rating_rows = []
    touched_players: set[int] = set()

    matches.sort(key=lambda row: (_parse_datetime(row.get('played_at')) or datetime.min, int(row['id'])))

    for match in matches:
        match_id = int(match['id'])
        player1_id = int(match['player1_id'])
        player2_id = int(match['player2_id'])
        winner_id = int(match['winner_player_id'])
        played_at = match.get('played_at')
        ranked_match = bool(match.get('is_ranked'))

        if winner_id not in {player1_id, player2_id}:
            raise ValueError(f'Match {match_id} has invalid winner_player_id.')

        loser_id = player2_id if winner_id == player1_id else player1_id
        winner_state = player_state.setdefault(winner_id, {'elo': 1000, 'matches_count': 0, 'wins': 0, 'losses': 0, 'last_match_at': None})
        loser_state = player_state.setdefault(loser_id, {'elo': 1000, 'matches_count': 0, 'wins': 0, 'losses': 0, 'last_match_at': None})

        winner_old_elo = int(winner_state['elo'])
        loser_old_elo = int(loser_state['elo'])

        winner_matches_before_match = int(winner_state['matches_count'])
        loser_matches_before_match = int(loser_state['matches_count'])

        if ranked_match:
            elo_result = _calculate_elo_result(
                winner_old_elo,
                loser_old_elo,
                winner_matches_before_match,
                loser_matches_before_match,
            )
            winner_new_elo = elo_result['winner_new_elo']
            loser_new_elo = elo_result['loser_new_elo']
            rating_rows.append(
                {
                    'match_id': match_id,
                    'player_id': winner_id,
                    'old_elo': elo_result['winner_old_elo'],
                    'new_elo': elo_result['winner_new_elo'],
                    'elo_delta': elo_result['winner_delta'],
                    'expected_score': elo_result['winner_expected_score'],
                    'actual_score': 1,
                    'k_factor': elo_result['winner_k_factor'],
                }
            )
            rating_rows.append(
                {
                    'match_id': match_id,
                    'player_id': loser_id,
                    'old_elo': elo_result['loser_old_elo'],
                    'new_elo': elo_result['loser_new_elo'],
                    'elo_delta': elo_result['loser_delta'],
                    'expected_score': elo_result['loser_expected_score'],
                    'actual_score': 0,
                    'k_factor': elo_result['loser_k_factor'],
                }
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
        _rest_insert('rating_history', rating_rows)

    for player in players:
        player_id = int(player['id'])
        state = player_state.get(player_id, {'elo': 1000, 'matches_count': 0, 'wins': 0, 'losses': 0, 'last_match_at': None})
        _rest_update(
            'players',
            {
                'current_elo': int(state['elo']),
                'matches_count': int(state['matches_count']),
                'wins': int(state['wins']),
                'losses': int(state['losses']),
                'last_match_at': state['last_match_at'],
                'is_active': int(state['matches_count']) >= CALIBRATION_MATCHES_REQUIRED,
                'priority_race': None,
                'updated_at': datetime.utcnow().isoformat(),
            },
            filters=[('id', 'eq', player_id)],
        )

    for index, player_id in enumerate(sorted(touched_players)):
        _refresh_priority_race(player_id, force_refresh=(index == 0))

    invalidate_application_cache()


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

    existing = _rest_select('matches', filters=[('id', 'eq', match_id)], single=True)
    if not existing:
        raise ValueError('Match not found.')

    player1 = _get_or_create_player(clean_player1_name)
    player2 = _get_or_create_player(clean_player2_name)
    winner_player_id = player1['id'] if clean_winner_side == 'player1' else player2['id']

    rows = _rest_update(
        'matches',
        {
            'player1_id': player1['id'],
            'player2_id': player2['id'],
            'winner_player_id': winner_player_id,
            'played_at': played_at.isoformat(),
            'comment': clean_comment or None,
            'player1_race': clean_player1_race,
            'player2_race': clean_player2_race,
            'is_ranked': ranked_match,
            'game_type': clean_game_type,
            'mission_name': clean_mission_name,
        },
        filters=[('id', 'eq', match_id)],
    )
    if not rows:
        raise ValueError('Match not found.')

    invalidate_application_cache()
    _rebuild_ratings_and_player_stats()

    updated = fetch_match_admin(match_id)
    if not updated:
        raise ValueError('Match not found after update.')
    return updated


def delete_match_admin(match_id: int) -> None:
    existing = _rest_select('matches', filters=[('id', 'eq', match_id)], single=True)
    if not existing:
        raise ValueError('Match not found.')

    _rest_delete('rating_history', filters=[('match_id', 'eq', match_id)])
    _rest_delete('matches', filters=[('id', 'eq', match_id)])
    invalidate_application_cache()
    _rebuild_ratings_and_player_stats()
