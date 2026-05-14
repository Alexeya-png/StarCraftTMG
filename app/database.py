
from __future__ import annotations

import copy
import json
import logging
import math
import os
import re
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib import error as urllib_error
from urllib import request as urllib_request
from urllib.parse import urlencode

from dotenv import load_dotenv

try:
    from .players import resolve_player_canonical_name
except ImportError:
    from players import resolve_player_canonical_name

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
FLAGS_DIR = BASE_DIR / 'static' / 'Flags'
ALLOWED_FLAG_EXTENSIONS = {'.png', '.jpg', '.jpeg', '.svg', '.webp'}


class DatabaseConfigError(RuntimeError):
    pass


class MatchSubmissionRateLimitError(ValueError):
    pass


RACE_LABELS = {
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

RACE_DB_LABELS = {
    'Терран': 'Терран',
    'Протосс': 'Протосс',
    'Зерг': 'Зерг',
    'Terran': 'Терран',
    'Protoss': 'Протосс',
    'Zerg': 'Зерг',
    '??????': 'Терран',
    '???????': 'Протосс',
    '????': 'Зерг',
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
GAME_TYPE_DB_LABELS = {
    '1к': '1к',
    '2к': '2к',
    '1k': '1к',
    '2k': '2к',
    '1K': '1к',
    '2K': '2к',
    '1?': '1к',
    '2?': '2к',
    'Grand Offensive': 'Grand Offensive',
    'grand offensive': 'Grand Offensive',
}
DEFAULT_K_FACTOR = 32
BASE_RATING_K_FACTOR = 32
ESTABLISHED_PLAYER_K_FACTOR = 24
STABLE_PLAYER_K_FACTOR = 16
ESTABLISHED_PLAYER_RANKED_MATCHES_THRESHOLD = 15
STABLE_PLAYER_RANKED_MATCHES_THRESHOLD = 40
RANKED_1K_ELO_MULTIPLIER = 0.35
WINNER_SEED_ELO_BONUS_MULTIPLIER = 1.0
ACTIVE_PLAYER_DAYS_WINDOW = 365
DATA_CACHE_TTL_SECONDS = max(0, int(os.getenv('APP_DATA_CACHE_TTL_SECONDS', '300') or '300'))
LEAGUE_SUMMARY_CACHE_TTL_SECONDS = max(0, int(os.getenv('APP_LEAGUE_SUMMARY_CACHE_TTL_SECONDS', '300') or '300'))
PAGE_CACHE_TTL_SECONDS = max(0, int(os.getenv('APP_PAGE_CACHE_TTL_SECONDS', '900') or '900'))
DISK_CACHE_MAX_AGE_SECONDS = max(0, int(os.getenv('APP_DISK_CACHE_MAX_AGE_SECONDS', '604800') or '604800'))
DISK_CACHE_PATH = Path(os.getenv('APP_DISK_CACHE_PATH') or (BASE_DIR / '.cache' / 'application_data.json'))
SUPABASE_HTTP_TIMEOUT_SECONDS = max(1, int(os.getenv('SUPABASE_HTTP_TIMEOUT_SECONDS', '8') or '8'))
USE_DISK_CACHE_ON_MISS = (os.getenv('APP_USE_DISK_CACHE_ON_MISS') or '0').strip().lower() not in {'0', 'false', 'no', 'off'}
BLOCKING_CACHE_LOAD_ON_MISS = (os.getenv('APP_BLOCKING_CACHE_LOAD_ON_MISS') or '1').strip().lower() not in {'0', 'false', 'no', 'off'}
ALLOW_EMPTY_CACHE_ON_MISS = (os.getenv('APP_ALLOW_EMPTY_CACHE_ON_MISS') or '0').strip().lower() not in {'0', 'false', 'no', 'off'}
HEALTH_CHECK_DATABASE = (os.getenv('APP_HEALTH_CHECK_DB') or '0').strip().lower() not in {'0', 'false', 'no', 'off'}
TTS_PLAYER_SUBMIT_COOLDOWN_SECONDS = max(0, int(os.getenv('TTS_PLAYER_SUBMIT_COOLDOWN_SECONDS', '3600') or '3600'))
FEEDBACK_MESSAGE_MAX_LENGTH = 300
FEEDBACK_PLAYER_NAME_MAX_LENGTH = 80
FEEDBACK_TABLE_NAME = 'admin_feedback_messages'
LEAGUE_TABLE_NAME = 'leagues'
LEAGUE_BADGES_TABLE_NAME = 'player_league_badges'
CURRENT_LEAGUE_SETTING_KEY = 'current_league_id'
CURRENT_LEAGUE_SETTING_FALLBACK_KEYS = ('current_league_id', 'current_league')

LEAGUE_BADGE_KIND_CHAMPION = 'champion'
LEAGUE_BADGE_KIND_CONTENDER = 'contender'
LEAGUE_BADGE_KINDS = (LEAGUE_BADGE_KIND_CHAMPION, LEAGUE_BADGE_KIND_CONTENDER)
LEAGUE_BADGE_RACES = ('Terran', 'Protoss', 'Zerg')
LEAGUE_BADGE_RACE_SLUGS = {
    'Terran': 'terran',
    'Protoss': 'protoss',
    'Zerg': 'zerg',
}
LEAGUE_BADGE_KIND_TITLES = {
    LEAGUE_BADGE_KIND_CHAMPION: 'Champion',
    LEAGUE_BADGE_KIND_CONTENDER: 'Contender',
}
LEAGUE_BADGE_KIND_ICONS = {
    LEAGUE_BADGE_KIND_CHAMPION: '🏆',
    LEAGUE_BADGE_KIND_CONTENDER: '⚔',
}
LEAGUE_BADGE_RACE_ICONS = {
    'Terran': '⚙',
    'Protoss': 'ψ',
    'Zerg': '☣',
}
LEAGUE_BADGE_DEFINITIONS = {
    f'{kind}_{race_slug}': {
        'title': LEAGUE_BADGE_KIND_TITLES[kind],
        'icon': LEAGUE_BADGE_KIND_ICONS[kind],
        'race_icon': LEAGUE_BADGE_RACE_ICONS[race],
        'variant': f'{kind}-{race_slug}',
        'kind': kind,
        'race': race,
        'race_slug': race_slug,
        'image_url': f'/static/badges/{kind}-{race_slug}.png',
        'description_template': f'{LEAGUE_BADGE_KIND_TITLES[kind]} of {{league_name}} for {race}.',
    }
    for kind in LEAGUE_BADGE_KINDS
    for race, race_slug in LEAGUE_BADGE_RACE_SLUGS.items()
}
LEAGUE_HERO_RACES = LEAGUE_BADGE_RACES
LEAGUE_HEAD_TO_HEAD_POINT_LEAD_LIMIT = 2
LEAGUE_POINTS_MIN_OPPONENT_MATCHES = 4
LEAGUE_POINTS_WIN = 3.0
LEAGUE_POINTS_DRAW = 1.0
LEAGUE_POINTS_LOSS = -1.0
LEAGUE_POINTS_FAVORITE_WIN = 1.0
LEAGUE_POINTS_FAVORITE_DRAW = 0.0
LEAGUE_POINTS_FAVORITE_LOSS = -2.0
LEAGUE_POINTS_RULES_TEXT = (
    'Win = 3 points. Draw = 1 point. Loss = -1 point. '
    'Points count only if the opponent has played more than 3 ranked matches in this league. '
    'If a player leads the same opponent by 2 or more wins, that player is the head-to-head favorite. '
    'A favorite gets +1 point for a win, 0 points for a draw, and -2 points for a loss. '
    'The trailing player keeps the normal +3 points for a win, +1 point for a draw, and -1 point for a loss.'
)

_DATA_CACHE_LOCK = threading.RLock()
_SUBMIT_MATCH_LOCK = threading.RLock()
_DATA_CACHE_REFRESH_LOCK = threading.Lock()
_DATA_CACHE_REFRESH_IN_PROGRESS = False
_DATA_CACHE: dict[str, Any] = {
    'players': None,
    'matches': None,
    'rating_history': None,
    'all_leagues': None,
    'current_league': None,
    'player_league_badges': None,
    'loaded_at': 0.0,
    'version': 0,
}

_LEAGUE_SUMMARY_CACHE_LOCK = threading.RLock()
_LEAGUE_SUMMARY_CACHE: dict[int, dict[str, Any]] = {}

_LEAGUE_CONFIG_CACHE_LOCK = threading.RLock()
_LEAGUE_CONFIG_CACHE: dict[str, Any] = {
    'all_leagues': None,
    'current_league': None,
    'loaded_at': 0.0,
}

_PAGE_CACHE_LOCK = threading.RLock()
_PAGE_CACHE: dict[str, dict[str, Any]] = {}


MATCH_META_PREFIX = '[[match_meta:'
MATCH_META_PATTERN = re.compile(r'^\[\[match_meta:(\{.*?\})\]\]\s*', re.DOTALL)


def _get_data_cache_version() -> int:
    with _DATA_CACHE_LOCK:
        return int(_DATA_CACHE.get('version') or 0)



def _make_page_cache_key(prefix: str, *parts: Any, **kwargs: Any) -> str:
    payload = {
        'data_version': _get_data_cache_version(),
        'parts': parts,
        'kwargs': kwargs,
    }
    try:
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str, separators=(',', ':'))
    except TypeError:
        encoded = repr(payload)
    return f'{prefix}:{encoded}'


def _get_page_cache(key: str):
    if PAGE_CACHE_TTL_SECONDS <= 0:
        return None

    now = time.time()
    with _PAGE_CACHE_LOCK:
        cached = _PAGE_CACHE.get(key)
        if not cached:
            return None
        loaded_at = float(cached.get('loaded_at') or 0.0)
        if loaded_at <= 0 or (now - loaded_at) >= PAGE_CACHE_TTL_SECONDS:
            _PAGE_CACHE.pop(key, None)
            return None
        return copy.deepcopy(cached.get('value'))


def _set_page_cache(key: str, value):
    if PAGE_CACHE_TTL_SECONDS <= 0:
        return value
    with _PAGE_CACHE_LOCK:
        _PAGE_CACHE[key] = {
            'loaded_at': time.time(),
            'value': copy.deepcopy(value),
        }
    return value


def invalidate_page_cache(prefix: str | None = None) -> None:
    with _PAGE_CACHE_LOCK:
        if not prefix:
            _PAGE_CACHE.clear()
            return
        clean_prefix = str(prefix)
        for key in list(_PAGE_CACHE.keys()):
            if key.startswith(clean_prefix):
                _PAGE_CACHE.pop(key, None)



def _normalize_text(value: str | None) -> str:
    if value is None:
        return ''
    return str(value).strip()


def _parse_match_meta(comment: str | None) -> tuple[dict[str, Any], str]:
    clean_comment = _normalize_text(comment)
    if not clean_comment:
        return {}, ''

    match = MATCH_META_PATTERN.match(clean_comment)
    if not match:
        return {}, clean_comment

    try:
        metadata = json.loads(match.group(1))
    except json.JSONDecodeError:
        return {}, clean_comment

    if not isinstance(metadata, dict):
        return {}, clean_comment

    visible_comment = clean_comment[match.end():].strip()
    return metadata, visible_comment



def _coerce_match_score_value(value, *, allow_blank: bool = False, field_label: str = 'Score') -> int | None:
    clean_value = _normalize_text(value)
    if not clean_value:
        if allow_blank:
            return None
        raise ValueError(f'{field_label} is required.')

    try:
        numeric_value = int(clean_value)
    except (TypeError, ValueError):
        raise ValueError(f'{field_label} must be a whole number.')

    if numeric_value < 0 or numeric_value > 100:
        raise ValueError(f'{field_label} must be between 0 and 100.')

    return numeric_value



def _extract_match_score_details(row: dict | None) -> dict[str, Any]:
    source = dict(row or {})
    metadata, visible_comment = _parse_match_meta(source.get('comment'))

    player1_score_source = source.get('player1_score', metadata.get('player1_score'))
    player2_score_source = source.get('player2_score', metadata.get('player2_score'))
    player1_roster_source = source.get('player1_roster_id', metadata.get('player1_roster_id'))
    player2_roster_source = source.get('player2_roster_id', metadata.get('player2_roster_id'))

    player1_score = _coerce_match_score_value(player1_score_source, allow_blank=True, field_label='Player 1 score')
    player2_score = _coerce_match_score_value(player2_score_source, allow_blank=True, field_label='Player 2 score')
    player1_roster_id = _normalize_roster_id(player1_roster_source, field_label='Player 1 roster ID') if _normalize_text(player1_roster_source) else ''
    player2_roster_id = _normalize_roster_id(player2_roster_source, field_label='Player 2 roster ID') if _normalize_text(player2_roster_source) else ''

    return {
        'visible_comment': visible_comment,
        'player1_score': player1_score,
        'player2_score': player2_score,
        'player1_roster_id': player1_roster_id,
        'player2_roster_id': player2_roster_id,
        'has_score': player1_score is not None and player2_score is not None,
    }



def _build_match_comment_payload(
    comment: str | None,
    player1_score,
    player2_score,
    player1_roster_id: str | None = None,
    player2_roster_id: str | None = None,
) -> str | None:
    clean_comment = _normalize_text(comment)
    metadata = {
        'player1_score': _coerce_match_score_value(player1_score, field_label='Player 1 score'),
        'player2_score': _coerce_match_score_value(player2_score, field_label='Player 2 score'),
    }

    if _normalize_text(player1_roster_id):
        metadata['player1_roster_id'] = _normalize_roster_id(player1_roster_id, field_label='Player 1 roster ID')
    if _normalize_text(player2_roster_id):
        metadata['player2_roster_id'] = _normalize_roster_id(player2_roster_id, field_label='Player 2 roster ID')

    metadata_blob = json.dumps(metadata, ensure_ascii=False, separators=(',', ':'))
    payload = f'{MATCH_META_PREFIX}{metadata_blob}]]'
    if clean_comment:
        payload = f'{payload} {clean_comment}'
    return payload


def _format_match_score(player1_score, player2_score) -> str:
    left_score = _coerce_match_score_value(player1_score, allow_blank=True, field_label='Player 1 score')
    right_score = _coerce_match_score_value(player2_score, allow_blank=True, field_label='Player 2 score')
    if left_score is None or right_score is None:
        return ''
    return f'{left_score}:{right_score}'

def _normalize_roster_id(value: str | None, *, field_label: str = 'Roster ID') -> str:
    clean_value = _normalize_text(value).upper()
    if not clean_value:
        return ''

    normalized = ''.join(char for char in clean_value if char.isalnum() or char in {'-', '_'})
    if not normalized:
        raise ValueError(f'{field_label} is invalid.')
    if len(normalized) > 80:
        raise ValueError(f'{field_label} must be at most 80 characters.')
    return normalized



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


def _normalize_race_db_label(value: str | None) -> str:
    clean_value = _normalize_text(value)
    if not clean_value:
        return ''
    return RACE_DB_LABELS.get(clean_value, clean_value)


def _normalize_game_type_label(value: str | None) -> str:
    clean_value = _normalize_text(value)
    if not clean_value:
        return ''
    return GAME_TYPE_DB_LABELS.get(clean_value, GAME_TYPE_DB_LABELS.get(clean_value.lower(), clean_value))


def _normalize_discord_url(value: str | None) -> str:
    clean_value = _normalize_text(value)
    if not clean_value:
        return ''

    if re.match(r'^[a-z][a-z0-9+.-]*://', clean_value, re.IGNORECASE):
        return clean_value

    return f'https://{clean_value}'


def _normalize_elo_value(value) -> str:
    if value is None:
        return ''
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
        return ''
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


def _is_player_active_by_last_match(last_match_at) -> bool:
    parsed = _parse_datetime(last_match_at)
    if not parsed:
        return False

    now = datetime.now(parsed.tzinfo) if getattr(parsed, 'tzinfo', None) else datetime.now()
    if parsed > now:
        return True

    return (now - parsed) <= timedelta(days=ACTIVE_PLAYER_DAYS_WINDOW)


def _format_match_datetime(value) -> str:
    parsed = _parse_datetime(value)
    if not parsed:
        return ''
    return parsed.strftime('%Y-%m-%d %H:%M')


def _format_match_date(value) -> str:
    parsed = _parse_datetime(value)
    if not parsed:
        return ''
    return parsed.strftime('%Y-%m-%d')


def _humanize_last_played(value) -> str:
    parsed = _parse_datetime(value)
    if not parsed:
        return ''

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


def _normalize_search_term(value: str | None) -> str:
    clean_value = _normalize_text(value)
    if not clean_value:
        return ''
    return re.sub(r'[\s,()]+', ' ', clean_value).strip()


def _coerce_ranked_value(value) -> bool:
    if isinstance(value, bool):
        return value
    normalized = _normalize_text(value).lower()
    return normalized in {'1', 'true', 'yes', 'y', 'ranked', 'on'}


def _normalize_match_result_type(value: str | None) -> str:
    normalized = _normalize_text(value).lower()
    return 'draw' if normalized in {'draw', 'tie'} else 'win'


def _is_match_draw(match: dict) -> bool:
    return _normalize_match_result_type(match.get('result_type')) == 'draw'


def _get_match_display_result(match: dict) -> str:
    return 'TIE' if _is_match_draw(match) else 'WIN'


def _calculate_expected_score(player_elo: int, opponent_elo: int) -> float:
    return 1 / (1 + 10 ** ((opponent_elo - player_elo) / 400))


def _determine_k_factor(current_elo: int, ranked_matches_played_before_match: int) -> int:
    ranked_matches_count = int(ranked_matches_played_before_match or 0)
    if ranked_matches_count >= STABLE_PLAYER_RANKED_MATCHES_THRESHOLD:
        return STABLE_PLAYER_K_FACTOR
    if ranked_matches_count >= ESTABLISHED_PLAYER_RANKED_MATCHES_THRESHOLD:
        return ESTABLISHED_PLAYER_K_FACTOR
    return BASE_RATING_K_FACTOR


def _count_player_ranked_matches_before_submit(player_id: int) -> int:
    _, total = _rest_select(
        'rating_history',
        select='id',
        filters=[('player_id', 'eq', int(player_id))],
        limit=1,
        count=True,
    )
    return int(total or 0)


def _resolve_ranked_elo_multiplier(game_type: str | None) -> float:
    clean_game_type = _normalize_game_type_label(game_type)
    if clean_game_type == '1к':
        return RANKED_1K_ELO_MULTIPLIER
    return 1.0


def _has_player1_seed_bonus(match: dict | None = None, *, player1_roster_id: str | None = None) -> bool:
    if player1_roster_id is not None:
        return bool(_normalize_text(player1_roster_id))

    score_details = _extract_match_score_details(match)
    return bool(score_details.get('player1_roster_id'))


def _apply_positive_winner_seed_bonus(delta: int | None) -> int:
    return int(delta or 0)


def _apply_winner_seed_bonus_to_win_elo_result(
    elo_result: dict,
    *,
    winner_old_elo: int,
    loser_old_elo: int,
    apply_bonus: bool,
) -> dict:
    adjusted_result = dict(elo_result)
    adjusted_result['winner_seed_bonus_applied'] = False
    adjusted_result['winner_seed_bonus_multiplier'] = 1.0
    return adjusted_result


def _calculate_elo_delta_with_multiplier(base_k_factor: int, actual_score: float, expected_score: float, multiplier: float) -> int:
    return int(round(base_k_factor * (actual_score - expected_score) * multiplier))


def _calculate_elo_result_for_actual_scores(
    player1_elo: int,
    player2_elo: int,
    player1_actual_score: float,
    player2_actual_score: float,
    player1_matches_played_before_match: int = 0,
    player2_matches_played_before_match: int = 0,
    elo_multiplier: float = 1.0,
) -> dict:
    expected_player1 = _calculate_expected_score(player1_elo, player2_elo)
    expected_player2 = _calculate_expected_score(player2_elo, player1_elo)

    player1_base_k_factor = _determine_k_factor(player1_elo, player1_matches_played_before_match)
    player2_base_k_factor = _determine_k_factor(player2_elo, player2_matches_played_before_match)

    player1_delta = _calculate_elo_delta_with_multiplier(
        player1_base_k_factor,
        player1_actual_score,
        expected_player1,
        elo_multiplier,
    )
    player2_delta = _calculate_elo_delta_with_multiplier(
        player2_base_k_factor,
        player2_actual_score,
        expected_player2,
        elo_multiplier,
    )

    player1_new = max(0, player1_elo + player1_delta)
    player2_new = max(0, player2_elo + player2_delta)

    return {
        'player1_old_elo': player1_elo,
        'player1_new_elo': player1_new,
        'player1_delta': player1_delta,
        'player1_expected_score': expected_player1,
        'player1_actual_score': player1_actual_score,
        'player1_k_factor': player1_base_k_factor,
        'player1_k_factor_effective': player1_base_k_factor * elo_multiplier,
        'player2_old_elo': player2_elo,
        'player2_new_elo': player2_new,
        'player2_delta': player2_delta,
        'player2_expected_score': expected_player2,
        'player2_actual_score': player2_actual_score,
        'player2_k_factor': player2_base_k_factor,
        'player2_k_factor_effective': player2_base_k_factor * elo_multiplier,
        'k_factor': max(player1_base_k_factor, player2_base_k_factor),
        'elo_multiplier': elo_multiplier,
    }


def _calculate_elo_result(
    winner_elo: int,
    loser_elo: int,
    winner_matches_played_before_match: int = 0,
    loser_matches_played_before_match: int = 0,
    elo_multiplier: float = 1.0,
) -> dict:
    base_result = _calculate_elo_result_for_actual_scores(
        winner_elo,
        loser_elo,
        1.0,
        0.0,
        winner_matches_played_before_match,
        loser_matches_played_before_match,
        elo_multiplier,
    )

    return {
        'winner_old_elo': base_result['player1_old_elo'],
        'winner_new_elo': base_result['player1_new_elo'],
        'winner_delta': base_result['player1_delta'],
        'winner_expected_score': base_result['player1_expected_score'],
        'winner_actual_score': base_result['player1_actual_score'],
        'winner_k_factor': base_result['player1_k_factor'],
        'loser_old_elo': base_result['player2_old_elo'],
        'loser_new_elo': base_result['player2_new_elo'],
        'loser_delta': base_result['player2_delta'],
        'loser_expected_score': base_result['player2_expected_score'],
        'loser_actual_score': base_result['player2_actual_score'],
        'loser_k_factor': base_result['player2_k_factor'],
        'k_factor': base_result['k_factor'],
    }


def _calculate_draw_elo_result(
    player1_elo: int,
    player2_elo: int,
    player1_matches_played_before_match: int = 0,
    player2_matches_played_before_match: int = 0,
    elo_multiplier: float = 1.0,
) -> dict:
    base_result = _calculate_elo_result_for_actual_scores(
        player1_elo,
        player2_elo,
        0.5,
        0.5,
        player1_matches_played_before_match,
        player2_matches_played_before_match,
        elo_multiplier,
    )

    return {
        'player1_old_elo': base_result['player1_old_elo'],
        'player1_new_elo': base_result['player1_new_elo'],
        'player1_delta': base_result['player1_delta'],
        'player1_expected_score': base_result['player1_expected_score'],
        'player1_actual_score': base_result['player1_actual_score'],
        'player1_k_factor': base_result['player1_k_factor'],
        'player2_old_elo': base_result['player2_old_elo'],
        'player2_new_elo': base_result['player2_new_elo'],
        'player2_delta': base_result['player2_delta'],
        'player2_expected_score': base_result['player2_expected_score'],
        'player2_actual_score': base_result['player2_actual_score'],
        'player2_k_factor': base_result['player2_k_factor'],
        'k_factor': base_result['k_factor'],
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
        with urllib_request.urlopen(req, timeout=SUPABASE_HTTP_TIMEOUT_SECONDS) as response:
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


def _rest_select_raw(
    table: str,
    *,
    query: dict[str, Any] | None = None,
    single: bool = False,
    count: bool = False,
):
    prefer = 'count=exact' if count else None
    body, headers = _supabase_request(
        'GET',
        f'/rest/v1/{table}',
        query=query or {},
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



def _coerce_positive_int(value) -> int | None:
    clean_value = _normalize_text(value)
    if not clean_value:
        return None
    try:
        numeric_value = int(clean_value)
    except (TypeError, ValueError):
        return None
    return numeric_value if numeric_value > 0 else None


def _prepare_league_row(row: dict | None) -> dict | None:
    if not row:
        return None

    league = dict(row)
    league_id = _coerce_positive_int(league.get('id'))
    if not league_id:
        return None

    league['id'] = league_id
    league['name'] = _normalize_text(league.get('name')) or f'League {league_id}'
    league['slug'] = _normalize_text(league.get('slug'))
    league['starts_at'] = _normalize_text(league.get('starts_at') or league.get('start_date'))
    league['ends_at'] = _normalize_text(league.get('ends_at') or league.get('end_date'))

    date_parts = []
    if league['starts_at']:
        date_parts.append(league['starts_at'])
    if league['ends_at']:
        date_parts.append(league['ends_at'])
    league['date_range_label'] = ' – '.join(date_parts)
    return league


def _fetch_league_by_setting_value(setting_value: str | None) -> dict | None:
    clean_value = _normalize_text(setting_value)
    if not clean_value:
        return None

    league_id = _coerce_positive_int(clean_value)
    if league_id:
        row = _rest_select(LEAGUE_TABLE_NAME, filters=[('id', 'eq', league_id)], single=True)
        return _prepare_league_row(row)

    row = _rest_select(LEAGUE_TABLE_NAME, filters=[('slug', 'eq', clean_value)], single=True)
    prepared = _prepare_league_row(row)
    if prepared:
        return prepared

    row = _rest_select(LEAGUE_TABLE_NAME, filters=[('name', 'eq', clean_value)], single=True)
    return _prepare_league_row(row)


def _fetch_current_league_uncached(*, required: bool = False) -> dict | None:
    setting_rows = _rest_select(
        'system_settings',
        select='setting_key,setting_value',
        filters=[('setting_key', 'in', list(CURRENT_LEAGUE_SETTING_FALLBACK_KEYS))],
    )
    settings_by_key = {
        _normalize_text(row.get('setting_key')): _normalize_text(row.get('setting_value'))
        for row in setting_rows
    }

    for key in CURRENT_LEAGUE_SETTING_FALLBACK_KEYS:
        league = _fetch_league_by_setting_value(settings_by_key.get(key))
        if league:
            return league

    if required:
        raise ValueError(
            'Current league is not configured. Add a league to Supabase and set '
            'system_settings.current_league_id to that league id.'
        )
    return None


def _fetch_all_leagues_uncached() -> list[dict]:
    rows = _rest_fetch_all(LEAGUE_TABLE_NAME, select='id,name,slug,starts_at,ends_at')
    leagues: list[dict] = []
    for row in rows:
        prepared = _prepare_league_row(row)
        if prepared:
            leagues.append(prepared)

    def sort_key(league: dict) -> tuple:
        parsed_start = _parse_datetime(league.get('starts_at'))
        parsed_end = _parse_datetime(league.get('ends_at'))
        return (
            parsed_start or datetime.min,
            parsed_end or datetime.min,
            int(league.get('id') or 0),
        )

    leagues.sort(key=sort_key, reverse=True)
    return leagues



def _league_config_cache_is_fresh() -> bool:
    loaded_at = float(_LEAGUE_CONFIG_CACHE.get('loaded_at') or 0.0)
    if loaded_at <= 0:
        return False
    if LEAGUE_SUMMARY_CACHE_TTL_SECONDS <= 0:
        return False
    return (time.time() - loaded_at) < LEAGUE_SUMMARY_CACHE_TTL_SECONDS


def _load_league_config_cache(*, force_refresh: bool = False) -> dict[str, Any]:
    with _LEAGUE_CONFIG_CACHE_LOCK:
        if (
            not force_refresh
            and _league_config_cache_is_fresh()
            and _LEAGUE_CONFIG_CACHE.get('all_leagues') is not None
        ):
            return {
                'all_leagues': copy.deepcopy(_LEAGUE_CONFIG_CACHE.get('all_leagues') or []),
                'current_league': copy.deepcopy(_LEAGUE_CONFIG_CACHE.get('current_league')),
            }

    if not force_refresh:
        with _DATA_CACHE_LOCK:
            data_all_leagues = copy.deepcopy(_DATA_CACHE.get('all_leagues') or [])
            data_current_league = copy.deepcopy(_DATA_CACHE.get('current_league'))
            data_loaded_at = float(_DATA_CACHE.get('loaded_at') or 0.0)

        if data_loaded_at > 0 and (data_all_leagues or data_current_league):
            with _LEAGUE_CONFIG_CACHE_LOCK:
                _LEAGUE_CONFIG_CACHE['all_leagues'] = copy.deepcopy(data_all_leagues)
                _LEAGUE_CONFIG_CACHE['current_league'] = copy.deepcopy(data_current_league)
                _LEAGUE_CONFIG_CACHE['loaded_at'] = data_loaded_at
            return {
                'all_leagues': data_all_leagues,
                'current_league': data_current_league,
            }

        disk_snapshot = _read_disk_cache_snapshot()
        if disk_snapshot and (disk_snapshot.get('all_leagues') or disk_snapshot.get('current_league')):
            snapshot = _apply_cache_snapshot(disk_snapshot)
            return {
                'all_leagues': copy.deepcopy(snapshot.get('all_leagues') or []),
                'current_league': copy.deepcopy(snapshot.get('current_league')),
            }

    all_leagues = _fetch_all_leagues_uncached()
    current_league = _fetch_current_league_uncached(required=False)

    with _LEAGUE_CONFIG_CACHE_LOCK:
        _LEAGUE_CONFIG_CACHE['all_leagues'] = copy.deepcopy(all_leagues)
        _LEAGUE_CONFIG_CACHE['current_league'] = copy.deepcopy(current_league)
        _LEAGUE_CONFIG_CACHE['loaded_at'] = time.time()
        return {
            'all_leagues': copy.deepcopy(all_leagues),
            'current_league': copy.deepcopy(current_league),
        }


def fetch_current_league(*, required: bool = False, force_refresh: bool = False) -> dict | None:
    config = _load_league_config_cache(force_refresh=force_refresh)
    current_league = config.get('current_league')
    if current_league:
        return current_league
    if required:
        raise ValueError(
            'Current league is not configured. Add a league to Supabase and set '
            'system_settings.current_league_id to that league id.'
        )
    return None


def fetch_all_leagues(*, force_refresh: bool = False) -> list[dict]:
    config = _load_league_config_cache(force_refresh=force_refresh)
    return list(config.get('all_leagues') or [])


def _fetch_leagues_by_ids(league_ids: list[int]) -> dict[int, dict]:
    clean_ids = sorted({int(league_id) for league_id in league_ids if _coerce_positive_int(league_id)})
    if not clean_ids:
        return {}

    cached_leagues = {
        int(league['id']): dict(league)
        for league in fetch_all_leagues()
        if int(league.get('id') or 0) in clean_ids
    }
    if len(cached_leagues) == len(clean_ids):
        return cached_leagues

    rows = _rest_select(
        LEAGUE_TABLE_NAME,
        select='id,name,slug,starts_at,ends_at',
        filters=[('id', 'in', clean_ids)],
    )
    leagues_by_id: dict[int, dict] = {}
    for row in rows:
        prepared = _prepare_league_row(row)
        if prepared:
            leagues_by_id[int(prepared['id'])] = prepared
    return leagues_by_id


def _select_featured_leagues() -> list[dict]:
    leagues = fetch_all_leagues()
    if not leagues:
        return []

    current_league = fetch_current_league(required=False)
    if not current_league:
        return leagues[:2]

    ordered: list[dict] = []
    current_id = int(current_league['id'])
    for league in leagues:
        if int(league['id']) == current_id:
            ordered.append(league)
            break
    for league in leagues:
        if int(league['id']) != current_id:
            ordered.append(league)
        if len(ordered) >= 2:
            break
    return ordered[:2]


def _player_league_sort_key(row: dict) -> tuple:
    return (
        -float(row.get('points') or 0),
        -float(row.get('win_rate_numeric') or 0.0),
        -int(row.get('wins') or 0),
        -int(row.get('matches_count') or 0),
        -int(row.get('current_elo') or 0),
        _normalize_player_name(row.get('name')).casefold(),
    )


def _player_activity_sort_key(row: dict) -> tuple:
    return (
        -int(row.get('matches_count') or 0),
        -int(row.get('wins') or 0),
        -float(row.get('points') or 0),
        -float(row.get('win_rate_numeric') or 0.0),
        -int(row.get('current_elo') or 0),
        _normalize_player_name(row.get('name')).casefold(),
    )


def _format_league_points(value) -> str:
    try:
        numeric_value = float(value or 0)
    except (TypeError, ValueError):
        return '0'

    if numeric_value.is_integer():
        return str(int(numeric_value))
    return f'{numeric_value:.2f}'.rstrip('0').rstrip('.')


def _format_league_points_delta(value) -> str:
    if value is None:
        return '-'
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return '-'

    prefix = '+' if numeric_value > 0 else ''
    return f'{prefix}{_format_league_points(numeric_value)}'


def _normalize_league_badge_kind(value) -> str:
    kind = _normalize_text(value).strip().lower()
    return kind if kind in LEAGUE_BADGE_KINDS else ''


def _normalize_league_badge_race(value) -> str:
    race = _normalize_race_label(value)
    return race if race in LEAGUE_BADGE_RACE_SLUGS else ''


def _build_league_badge_code(*, kind: str, race: str) -> str:
    clean_kind = _normalize_league_badge_kind(kind)
    clean_race = _normalize_league_badge_race(race)
    if not clean_kind or not clean_race:
        return ''
    return f'{clean_kind}_{LEAGUE_BADGE_RACE_SLUGS[clean_race]}'


def _league_badge_codes_for_kind(kind: str) -> list[str]:
    clean_kind = _normalize_league_badge_kind(kind)
    if not clean_kind:
        return []
    return [
        _build_league_badge_code(kind=clean_kind, race=race)
        for race in LEAGUE_BADGE_RACES
    ]


def _resolve_league_badge_kind_for_league(league_id: int | None) -> str:
    clean_league_id = _coerce_positive_int(league_id)
    if not clean_league_id:
        return LEAGUE_BADGE_KIND_CHAMPION

    try:
        current_league = fetch_current_league(required=False)
    except Exception:
        current_league = None

    current_league_id = _coerce_positive_int((current_league or {}).get('id'))
    if current_league_id and current_league_id == clean_league_id:
        return LEAGUE_BADGE_KIND_CONTENDER
    return LEAGUE_BADGE_KIND_CHAMPION


def _build_league_race_badge(*, league: dict | None, race: str, kind: str | None = None) -> dict | None:
    prepared_league = _prepare_league_row(league) if league else None
    league_id = _coerce_positive_int((prepared_league or {}).get('id'))
    badge_kind = _normalize_league_badge_kind(kind) or _resolve_league_badge_kind_for_league(league_id)
    badge_code = _build_league_badge_code(kind=badge_kind, race=race)
    if not badge_code:
        return None
    return _build_league_badge_display(badge_code=badge_code, league=prepared_league, row={'league_id': league_id})


def _opponent_has_enough_league_matches(player_match_counts: dict[int, int], opponent_id: int) -> bool:
    return int(player_match_counts.get(int(opponent_id), 0) or 0) >= LEAGUE_POINTS_MIN_OPPONENT_MATCHES


def _build_league_points_delta_lookup(match_rows: list[dict] | None = None) -> dict[tuple[int, int], float]:
    source_rows = match_rows if match_rows is not None else _fetch_all_matches_raw()
    rows_by_league: dict[int, list[dict]] = defaultdict(list)
    match_counts_by_league: dict[int, dict[int, int]] = defaultdict(lambda: defaultdict(int))

    for row in source_rows:
        if not bool(row.get('is_ranked')):
            continue

        league_id = _coerce_positive_int(row.get('league_id'))
        match_id = _coerce_positive_int(row.get('id'))
        player1_id = _coerce_positive_int(row.get('player1_id'))
        player2_id = _coerce_positive_int(row.get('player2_id'))
        if not league_id or not match_id or not player1_id or not player2_id:
            continue

        rows_by_league[int(league_id)].append(dict(row))
        match_counts_by_league[int(league_id)][int(player1_id)] += 1
        match_counts_by_league[int(league_id)][int(player2_id)] += 1

    lookup: dict[tuple[int, int], float] = {}

    for league_id, league_rows in rows_by_league.items():
        league_rows.sort(
            key=lambda row: (_parse_datetime(row.get('played_at')) or datetime.min, int(row.get('id') or 0))
        )
        league_match_counts = match_counts_by_league[int(league_id)]
        head_to_head: dict[tuple[int, int], dict[int, int]] = {}

        for match in league_rows:
            match_id = int(match.get('id') or 0)
            player1_id = int(match.get('player1_id') or 0)
            player2_id = int(match.get('player2_id') or 0)
            if match_id <= 0 or player1_id <= 0 or player2_id <= 0:
                continue

            pair_key = tuple(sorted((player1_id, player2_id)))
            pair_wins = head_to_head.setdefault(pair_key, defaultdict(int))
            player1_points = 0.0
            player2_points = 0.0

            player1_lead_before = int(pair_wins[player1_id] or 0) - int(pair_wins[player2_id] or 0)
            player2_lead_before = int(pair_wins[player2_id] or 0) - int(pair_wins[player1_id] or 0)
            player1_is_favorite = player1_lead_before >= LEAGUE_HEAD_TO_HEAD_POINT_LEAD_LIMIT
            player2_is_favorite = player2_lead_before >= LEAGUE_HEAD_TO_HEAD_POINT_LEAD_LIMIT

            if _is_match_draw(match):
                if _opponent_has_enough_league_matches(league_match_counts, player2_id):
                    player1_points = LEAGUE_POINTS_FAVORITE_DRAW if player1_is_favorite else LEAGUE_POINTS_DRAW
                if _opponent_has_enough_league_matches(league_match_counts, player1_id):
                    player2_points = LEAGUE_POINTS_FAVORITE_DRAW if player2_is_favorite else LEAGUE_POINTS_DRAW
            else:
                winner_id = int(match.get('winner_player_id') or 0)
                loser_id = player2_id if winner_id == player1_id else player1_id if winner_id == player2_id else 0

                if winner_id in {player1_id, player2_id} and loser_id in {player1_id, player2_id}:
                    winner_lead_before = int(pair_wins[winner_id] or 0) - int(pair_wins[loser_id] or 0)
                    loser_lead_before = int(pair_wins[loser_id] or 0) - int(pair_wins[winner_id] or 0)
                    winner_points = (
                        LEAGUE_POINTS_FAVORITE_WIN
                        if winner_lead_before >= LEAGUE_HEAD_TO_HEAD_POINT_LEAD_LIMIT
                        else LEAGUE_POINTS_WIN
                    )
                    loser_points = (
                        LEAGUE_POINTS_FAVORITE_LOSS
                        if loser_lead_before >= LEAGUE_HEAD_TO_HEAD_POINT_LEAD_LIMIT
                        else LEAGUE_POINTS_LOSS
                    )

                    if winner_id == player1_id:
                        if _opponent_has_enough_league_matches(league_match_counts, player2_id):
                            player1_points = winner_points
                        if _opponent_has_enough_league_matches(league_match_counts, player1_id):
                            player2_points = loser_points
                    elif winner_id == player2_id:
                        if _opponent_has_enough_league_matches(league_match_counts, player1_id):
                            player2_points = winner_points
                        if _opponent_has_enough_league_matches(league_match_counts, player2_id):
                            player1_points = loser_points

                    pair_wins[winner_id] += 1

            lookup[(match_id, player1_id)] = player1_points
            lookup[(match_id, player2_id)] = player2_points

    return lookup


def _prepare_league_player_card(row: dict | None) -> dict | None:
    if not row:
        return None

    prepared = dict(row)
    prepared['profile_url'] = f"/players/{prepared['id']}"
    prepared['flag_url'] = _resolve_flag_url(prepared.get('country_code'), prepared.get('country_name'))
    prepared['country_code'] = _resolve_country_code(prepared.get('country_code'), prepared.get('country_name'))
    prepared['priority_race'] = _normalize_race_label(prepared.get('priority_race'))
    prepared['priority_race_slug'] = prepared['priority_race'].strip().lower()
    prepared['current_elo_display'] = _normalize_elo_value(prepared.get('current_elo'))
    prepared['points_display'] = _format_league_points(prepared.get('points'))
    prepared['win_rate_display'] = _format_percent(prepared.get('win_rate_numeric'))
    prepared['record_display'] = f"{int(prepared.get('wins') or 0)}-{int(prepared.get('losses') or 0)}"
    if int(prepared.get('draws') or 0) > 0:
        prepared['record_display'] += f"-{int(prepared.get('draws') or 0)}"
    prepared['matches_label'] = f"{int(prepared.get('matches_count') or 0)} match"
    if int(prepared.get('matches_count') or 0) != 1:
        prepared['matches_label'] += 'es'
    return prepared


def _build_league_results_summary(league: dict) -> dict:
    league_id = int(league['id'])
    match_rows = [
        dict(row)
        for row in _fetch_all_matches_raw()
        if _coerce_positive_int(row.get('league_id')) == league_id and bool(row.get('is_ranked'))
    ]
    match_rows.sort(key=lambda row: (_parse_datetime(row.get('played_at')) or datetime.min, int(row.get('id') or 0)))

    player_ids: set[int] = set()
    player_rows: dict[int, dict] = {}
    stats_by_player: dict[int, dict] = {}

    league_match_counts_by_player: dict[int, int] = defaultdict(int)

    for match in match_rows:
        player1_id = int(match.get('player1_id') or 0)
        player2_id = int(match.get('player2_id') or 0)
        if player1_id > 0:
            player_ids.add(player1_id)
            league_match_counts_by_player[player1_id] += 1
        if player2_id > 0:
            player_ids.add(player2_id)
            league_match_counts_by_player[player2_id] += 1

    if player_ids:
        player_rows = _fetch_players_by_ids_cached(sorted(player_ids))

    def ensure_player(player_id: int) -> dict:
        if player_id not in stats_by_player:
            base_player = dict(player_rows.get(player_id) or {'id': player_id, 'name': f'Player {player_id}'})
            stats_by_player[player_id] = {
                'id': player_id,
                'name': _normalize_player_name(base_player.get('name')) or f'Player {player_id}',
                'current_elo': int(base_player.get('current_elo') or 0),
                'priority_race': _normalize_race_label(base_player.get('priority_race')),
                'country_code': _resolve_country_code(base_player.get('country_code'), base_player.get('country_name')),
                'country_name': _resolve_country_name(base_player.get('country_code'), base_player.get('country_name')),
                'matches_count': 0,
                'wins': 0,
                'losses': 0,
                'draws': 0,
                'points': 0.0,
            }
        return stats_by_player[player_id]

    head_to_head: dict[tuple[int, int], dict[int, int]] = {}

    for match in match_rows:
        player1_id = int(match.get('player1_id') or 0)
        player2_id = int(match.get('player2_id') or 0)
        if player1_id <= 0 or player2_id <= 0:
            continue

        pair_key = tuple(sorted((player1_id, player2_id)))
        pair_wins = head_to_head.setdefault(pair_key, defaultdict(int))

        player1 = ensure_player(player1_id)
        player2 = ensure_player(player2_id)
        player1['matches_count'] += 1
        player2['matches_count'] += 1

        player1_lead_before = int(pair_wins[player1_id] or 0) - int(pair_wins[player2_id] or 0)
        player2_lead_before = int(pair_wins[player2_id] or 0) - int(pair_wins[player1_id] or 0)
        player1_is_favorite = player1_lead_before >= LEAGUE_HEAD_TO_HEAD_POINT_LEAD_LIMIT
        player2_is_favorite = player2_lead_before >= LEAGUE_HEAD_TO_HEAD_POINT_LEAD_LIMIT

        if _is_match_draw(match):
            player1['draws'] += 1
            player2['draws'] += 1
            player1_points = LEAGUE_POINTS_FAVORITE_DRAW if player1_is_favorite else LEAGUE_POINTS_DRAW
            player2_points = LEAGUE_POINTS_FAVORITE_DRAW if player2_is_favorite else LEAGUE_POINTS_DRAW
            if _opponent_has_enough_league_matches(league_match_counts_by_player, player2_id):
                player1['points'] += player1_points
            if _opponent_has_enough_league_matches(league_match_counts_by_player, player1_id):
                player2['points'] += player2_points
            continue

        winner_id = int(match.get('winner_player_id') or 0)
        loser_id = player2_id if winner_id == player1_id else player1_id if winner_id == player2_id else 0

        if winner_id == player1_id:
            player1['wins'] += 1
            player2['losses'] += 1
        elif winner_id == player2_id:
            player2['wins'] += 1
            player1['losses'] += 1

        if winner_id in {player1_id, player2_id} and loser_id in {player1_id, player2_id}:
            winner_lead_before = int(pair_wins[winner_id] or 0) - int(pair_wins[loser_id] or 0)
            loser_lead_before = int(pair_wins[loser_id] or 0) - int(pair_wins[winner_id] or 0)
            winner_points = LEAGUE_POINTS_FAVORITE_WIN if winner_lead_before >= LEAGUE_HEAD_TO_HEAD_POINT_LEAD_LIMIT else LEAGUE_POINTS_WIN
            loser_points = LEAGUE_POINTS_FAVORITE_LOSS if loser_lead_before >= LEAGUE_HEAD_TO_HEAD_POINT_LEAD_LIMIT else LEAGUE_POINTS_LOSS

            if winner_id == player1_id:
                if _opponent_has_enough_league_matches(league_match_counts_by_player, player2_id):
                    player1['points'] += winner_points
                if _opponent_has_enough_league_matches(league_match_counts_by_player, player1_id):
                    player2['points'] += loser_points
            elif winner_id == player2_id:
                if _opponent_has_enough_league_matches(league_match_counts_by_player, player1_id):
                    player2['points'] += winner_points
                if _opponent_has_enough_league_matches(league_match_counts_by_player, player2_id):
                    player1['points'] += loser_points

            pair_wins[winner_id] += 1

    prepared_players: list[dict] = []
    for stats in stats_by_player.values():
        stats['priority_race'] = _normalize_race_label(stats.get('priority_race'))
        matches_count = int(stats.get('matches_count') or 0)
        wins = int(stats.get('wins') or 0)
        draws = int(stats.get('draws') or 0)
        stats['win_rate_numeric'] = round((((wins + (draws * 0.5)) / matches_count) * 100), 1) if matches_count > 0 else 0.0
        prepared_players.append(_prepare_league_player_card(stats))

    standings = sorted(prepared_players, key=_player_league_sort_key)
    for position, player in enumerate(standings, start=1):
        player['place'] = position

    most_active_player = sorted(prepared_players, key=_player_activity_sort_key)[0] if prepared_players else None
    best_player = standings[0] if standings else None

    badge_kind = _resolve_league_badge_kind_for_league(league_id)
    is_current_league = badge_kind == LEAGUE_BADGE_KIND_CONTENDER

    race_leaders: list[dict] = []
    for race_name in LEAGUE_HERO_RACES:
        candidates = [row for row in prepared_players if _normalize_race_label(row.get('priority_race')) == race_name]
        leader = sorted(candidates, key=_player_league_sort_key)[0] if candidates else None
        race_leaders.append({
            'race': race_name,
            'player': leader,
            'badge': _build_league_race_badge(league=league, race=race_name, kind=badge_kind) if leader else None,
        })

    return {
        'league': league,
        'matches_count': len(match_rows),
        'players_count': len(prepared_players),
        'best_player': best_player,
        'most_active_player': most_active_player,
        'race_leaders': race_leaders,
        'standings': standings[:10],
        'points_rules': LEAGUE_POINTS_RULES_TEXT,
        'badge_kind': badge_kind,
        'badge_kind_title': LEAGUE_BADGE_KIND_TITLES.get(badge_kind, ''),
        'is_current_league': is_current_league,
    }



def _get_league_results_summary_cached(league: dict, *, force_refresh: bool = False) -> dict:
    league_id = int(league['id'])
    now = time.time()

    if not force_refresh and LEAGUE_SUMMARY_CACHE_TTL_SECONDS > 0:
        with _LEAGUE_SUMMARY_CACHE_LOCK:
            cached = _LEAGUE_SUMMARY_CACHE.get(league_id)
            if cached and (now - float(cached.get('loaded_at') or 0.0)) < LEAGUE_SUMMARY_CACHE_TTL_SECONDS:
                return copy.deepcopy(cached['summary'])

    summary = _build_league_results_summary(league)

    if LEAGUE_SUMMARY_CACHE_TTL_SECONDS > 0:
        with _LEAGUE_SUMMARY_CACHE_LOCK:
            _LEAGUE_SUMMARY_CACHE[league_id] = {
                'loaded_at': now,
                'summary': copy.deepcopy(summary),
            }

    return summary


def _fetch_league_results_overview_uncached() -> list[dict]:
    return [_get_league_results_summary_cached(league) for league in _select_featured_leagues()]


def fetch_league_results_overview() -> list[dict]:
    key = _make_page_cache_key('leagues_overview')
    cached = _get_page_cache(key)
    if cached is not None:
        return cached
    result = _fetch_league_results_overview_uncached()
    return _set_page_cache(key, result)


def _build_award_name_class(*, is_best_overall: bool = False, is_most_active: bool = False) -> str:
    if is_best_overall and is_most_active:
        return 'player-name-award-combo'
    if is_best_overall:
        return 'player-name-award-overall'
    if is_most_active:
        return 'player-name-award-active'
    return ''


def fetch_current_league_awards() -> dict[str, Any]:
    current_league = fetch_current_league(required=False)
    if not current_league:
        return {
            'league': None,
            'best_overall_player_id': None,
            'most_active_player_id': None,
            'award_player_ids': set(),
        }

    summary = _get_league_results_summary_cached(current_league)
    best_player = summary.get('best_player') or {}
    most_active_player = summary.get('most_active_player') or {}
    best_player_id = _coerce_positive_int(best_player.get('id'))
    active_player_id = _coerce_positive_int(most_active_player.get('id'))
    award_player_ids = {player_id for player_id in (best_player_id, active_player_id) if player_id}

    return {
        'league': current_league,
        'best_overall_player_id': best_player_id,
        'most_active_player_id': active_player_id,
        'award_player_ids': award_player_ids,
    }


def decorate_players_with_current_league_awards(players: list[dict]) -> list[dict]:
    try:
        awards = fetch_current_league_awards()
    except Exception:
        awards = {
            'best_overall_player_id': None,
            'most_active_player_id': None,
        }

    best_player_id = _coerce_positive_int(awards.get('best_overall_player_id'))
    active_player_id = _coerce_positive_int(awards.get('most_active_player_id'))

    decorated: list[dict] = []
    for player in players:
        prepared = dict(player)
        player_id = _coerce_positive_int(prepared.get('id'))
        is_best_overall = bool(player_id and best_player_id and player_id == best_player_id)
        is_most_active = bool(player_id and active_player_id and player_id == active_player_id)
        prepared['is_current_league_best_overall'] = is_best_overall
        prepared['is_current_league_most_active'] = is_most_active
        prepared['award_name_class'] = _build_award_name_class(
            is_best_overall=is_best_overall,
            is_most_active=is_most_active,
        )
        decorated.append(prepared)
    return decorated


def decorate_player_with_current_league_awards(player: dict) -> dict:
    decorated = decorate_players_with_current_league_awards([player])
    return decorated[0] if decorated else dict(player)


def _is_missing_league_badges_table_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return LEAGUE_BADGES_TABLE_NAME in message and (
        'does not exist' in message
        or 'schema cache' in message
        or 'could not find the table' in message
    )


def _is_unique_league_badge_conflict_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return (
        LEAGUE_BADGES_TABLE_NAME in message
        and ('duplicate key' in message or 'unique constraint' in message)
    )


def _normalize_league_badge_code(value) -> str:
    code = _normalize_text(value).strip().lower().replace('-', '_')
    return code if code in LEAGUE_BADGE_DEFINITIONS else ''


def _build_league_badge_display(*, badge_code: str, league: dict | None, row: dict | None = None) -> dict | None:
    clean_badge_code = _normalize_league_badge_code(badge_code)
    if not clean_badge_code:
        return None

    prepared_league = _prepare_league_row(league) if league else None
    league_id = _coerce_positive_int((prepared_league or {}).get('id') or (row or {}).get('league_id'))
    if not league_id:
        return None

    league_name = _normalize_text((prepared_league or {}).get('name')) or f'Season {league_id}'
    definition = LEAGUE_BADGE_DEFINITIONS[clean_badge_code]
    title = definition['title']
    race = definition['race']
    race_slug = definition['race_slug']
    kind = definition['kind']
    icon = definition['icon']
    race_icon = definition['race_icon']

    return {
        'code': clean_badge_code,
        'label': f'{title} {league_name} · {race}',
        'short_label': f'{icon} {title} {league_name}',
        'description': definition['description_template'].format(league_name=league_name),
        'league_id': league_id,
        'league_name': league_name,
        'variant': definition['variant'],
        'kind': kind,
        'kind_title': title,
        'race': race,
        'race_slug': race_slug,
        'race_icon': race_icon,
        'image_url': definition['image_url'],
        'awarded_at': _normalize_text((row or {}).get('awarded_at')),
        'awarded_match_id': _coerce_positive_int((row or {}).get('awarded_match_id')),
    }


def _delete_league_badge_rows(rows: list[dict]) -> None:
    for row in rows or []:
        row_id = _coerce_positive_int(row.get('id'))
        if not row_id:
            continue
        _rest_delete(LEAGUE_BADGES_TABLE_NAME, filters=[('id', 'eq', row_id)])



def _ensure_player_league_badge(
    *,
    player_id: int,
    league: dict,
    badge_code: str,
    awarded_match_id: int | None = None,
) -> dict | None:
    clean_player_id = _coerce_positive_int(player_id)
    clean_badge_code = _normalize_league_badge_code(badge_code)
    prepared_league = _prepare_league_row(league)
    league_id = _coerce_positive_int((prepared_league or {}).get('id'))
    if not clean_player_id or not clean_badge_code or not prepared_league or not league_id:
        return None

    existing_for_code = _rest_select(
        LEAGUE_BADGES_TABLE_NAME,
        select='id,player_id,league_id,badge_code,awarded_at,awarded_match_id',
        filters=[
            ('league_id', 'eq', league_id),
            ('badge_code', 'eq', clean_badge_code),
        ],
        order='awarded_at.desc,id.desc',
    )

    matching_existing = None
    rows_to_delete: list[dict] = []
    for row in existing_for_code or []:
        if _coerce_positive_int(row.get('player_id')) == clean_player_id and matching_existing is None:
            matching_existing = row
        else:
            rows_to_delete.append(row)

    if rows_to_delete:
        _delete_league_badge_rows(rows_to_delete)

    existing_for_player = _rest_select(
        LEAGUE_BADGES_TABLE_NAME,
        select='id,player_id,league_id,badge_code,awarded_at,awarded_match_id',
        filters=[('player_id', 'eq', clean_player_id)],
    )
    player_rows_to_delete = [
        row for row in existing_for_player or []
        if not (
            _coerce_positive_int(row.get('league_id')) == league_id
            and _normalize_league_badge_code(row.get('badge_code')) == clean_badge_code
        )
    ]
    if player_rows_to_delete:
        _delete_league_badge_rows(player_rows_to_delete)

    if matching_existing:
        badge = _build_league_badge_display(badge_code=clean_badge_code, league=prepared_league, row=matching_existing)
        if badge:
            badge['created'] = False
        return badge

    payload = {
        'player_id': clean_player_id,
        'league_id': league_id,
        'badge_code': clean_badge_code,
    }
    clean_match_id = _coerce_positive_int(awarded_match_id)
    if clean_match_id:
        payload['awarded_match_id'] = clean_match_id

    try:
        inserted_rows = _rest_insert(LEAGUE_BADGES_TABLE_NAME, payload)
        inserted = inserted_rows[0] if isinstance(inserted_rows, list) and inserted_rows else inserted_rows
    except Exception as exc:
        if not _is_unique_league_badge_conflict_error(exc):
            raise
        _rest_update(
            LEAGUE_BADGES_TABLE_NAME,
            {'player_id': clean_player_id, 'awarded_match_id': clean_match_id},
            filters=[
                ('league_id', 'eq', league_id),
                ('badge_code', 'eq', clean_badge_code),
            ],
        )
        inserted = _rest_select(
            LEAGUE_BADGES_TABLE_NAME,
            select='id,player_id,league_id,badge_code,awarded_at,awarded_match_id',
            filters=[
                ('league_id', 'eq', league_id),
                ('badge_code', 'eq', clean_badge_code),
            ],
            single=True,
        )
        badge = _build_league_badge_display(badge_code=clean_badge_code, league=prepared_league, row=inserted)
        if badge:
            badge['created'] = False
        return badge

    badge = _build_league_badge_display(badge_code=clean_badge_code, league=prepared_league, row=inserted or payload)
    if badge:
        badge['created'] = True
    return badge


def _sync_league_badge_targets(*, league: dict, badge_kind: str, targets_by_badge_code: dict[str, int], awarded_match_id: int | None = None) -> list[dict]:
    prepared_league = _prepare_league_row(league)
    league_id = _coerce_positive_int((prepared_league or {}).get('id'))
    clean_badge_kind = _normalize_league_badge_kind(badge_kind)
    if not prepared_league or not league_id or not clean_badge_kind:
        return []

    badge_codes = list(LEAGUE_BADGE_DEFINITIONS.keys())
    existing_rows = _rest_select(
        LEAGUE_BADGES_TABLE_NAME,
        select='id,player_id,league_id,badge_code,awarded_at,awarded_match_id',
        filters=[
            ('league_id', 'eq', league_id),
            ('badge_code', 'in', badge_codes),
        ],
    )

    rows_to_delete: list[dict] = []
    for row in existing_rows or []:
        code = _normalize_league_badge_code(row.get('badge_code'))
        desired_player_id = _coerce_positive_int(targets_by_badge_code.get(code))
        if not desired_player_id or _coerce_positive_int(row.get('player_id')) != desired_player_id:
            rows_to_delete.append(row)

    if rows_to_delete:
        _delete_league_badge_rows(rows_to_delete)

    awarded_badges: list[dict] = []
    used_player_ids: set[int] = set()
    for race_name in LEAGUE_BADGE_RACES:
        badge_code = _build_league_badge_code(kind=clean_badge_kind, race=race_name)
        player_id = _coerce_positive_int(targets_by_badge_code.get(badge_code))
        if not badge_code or not player_id or player_id in used_player_ids:
            continue
        used_player_ids.add(player_id)
        badge = _ensure_player_league_badge(
            player_id=player_id,
            league=prepared_league,
            badge_code=badge_code,
            awarded_match_id=awarded_match_id,
        )
        if badge:
            awarded_badges.append(badge)

    return awarded_badges



def _sync_league_badges_for_league(league: dict | int, *, awarded_match_id: int | None = None) -> list[dict]:
    if isinstance(league, dict):
        prepared_league = _prepare_league_row(league)
    else:
        prepared_league = _prepare_league_row(
            _rest_select(LEAGUE_TABLE_NAME, filters=[('id', 'eq', int(league))], single=True)
        )
    if not prepared_league:
        return []

    summary = _get_league_results_summary_cached(prepared_league, force_refresh=True)
    badge_kind = _normalize_league_badge_kind(summary.get('badge_kind')) or _resolve_league_badge_kind_for_league(prepared_league.get('id'))
    targets_by_badge_code: dict[str, int] = {}

    for entry in summary.get('race_leaders') or []:
        race_name = _normalize_league_badge_race((entry or {}).get('race'))
        player_id = _coerce_positive_int(((entry or {}).get('player') or {}).get('id'))
        badge_code = _build_league_badge_code(kind=badge_kind, race=race_name)
        if badge_code and player_id and player_id not in targets_by_badge_code.values():
            targets_by_badge_code[badge_code] = player_id

    return _sync_league_badge_targets(
        league=prepared_league,
        badge_kind=badge_kind,
        targets_by_badge_code=targets_by_badge_code,
        awarded_match_id=awarded_match_id,
    )

def _sync_league_badges_after_ranked_match(*, league_id: int | None, match_id: int | None = None) -> list[dict]:
    clean_league_id = _coerce_positive_int(league_id)
    if not clean_league_id:
        return []
    try:
        return _sync_league_badges_for_league(clean_league_id, awarded_match_id=match_id)
    except Exception as exc:
        if _is_missing_league_badges_table_error(exc):
            return []
        raise


def _sync_all_current_league_badges() -> list[dict]:
    awarded_badges: list[dict] = []
    try:
        leagues = fetch_all_leagues(force_refresh=True)
        current_league = fetch_current_league(required=False, force_refresh=True)
        current_league_id = _coerce_positive_int((current_league or {}).get('id'))
        ordered_leagues = [league for league in leagues if _coerce_positive_int(league.get('id')) != current_league_id]
        ordered_leagues.extend([league for league in leagues if _coerce_positive_int(league.get('id')) == current_league_id])
        for league in ordered_leagues:
            awarded_badges.extend(_sync_league_badges_for_league(league))
    except Exception as exc:
        if _is_missing_league_badges_table_error(exc):
            return awarded_badges
        raise
    return awarded_badges


def fetch_player_badges(player_id: int) -> list[dict]:
    target_player_id = _coerce_positive_int(player_id)
    if not target_player_id:
        return []

    with _DATA_CACHE_LOCK:
        cached_badges = _DATA_CACHE.get('player_league_badges')

    if cached_badges is not None:
        rows = [
            dict(row)
            for row in cached_badges
            if _coerce_positive_int(row.get('player_id')) == target_player_id
        ]
        rows.sort(
            key=lambda row: (
                -int(row.get('league_id') or 0),
                _normalize_text(row.get('badge_code')),
            )
        )
    else:
        try:
            rows = _rest_select(
                LEAGUE_BADGES_TABLE_NAME,
                select='id,player_id,league_id,badge_code,awarded_at,awarded_match_id',
                filters=[('player_id', 'eq', target_player_id)],
                order='league_id.desc,badge_code.asc',
            )
        except Exception as exc:
            if _is_missing_league_badges_table_error(exc):
                return []
            raise

    rows = [row for row in rows if _normalize_league_badge_code(row.get('badge_code'))]
    league_ids = [_coerce_positive_int(row.get('league_id')) for row in rows]
    leagues_by_id = _fetch_leagues_by_ids([league_id for league_id in league_ids if league_id])

    badges: list[dict] = []
    for row in rows:
        league_id = _coerce_positive_int(row.get('league_id'))
        badge = _build_league_badge_display(
            badge_code=row.get('badge_code'),
            league=leagues_by_id.get(int(league_id)) if league_id else None,
            row=row,
        )
        if badge:
            badges.append(badge)

    current_league_id = None
    try:
        current_league_id = _coerce_positive_int((fetch_current_league(required=False) or {}).get('id'))
    except Exception:
        current_league_id = None

    badges.sort(
        key=lambda badge: (
            0 if current_league_id and int(badge.get('league_id') or 0) == current_league_id else 1,
            0 if badge.get('kind') == LEAGUE_BADGE_KIND_CONTENDER else 1,
            -int(badge.get('league_id') or 0),
            str(badge.get('code') or ''),
            str(badge.get('awarded_at') or ''),
        )
    )
    return badges[:1]

def _resolve_match_league_id(*, ranked_match: bool, existing_league_id=None) -> int | None:
    if not ranked_match:
        return None

    existing_id = _coerce_positive_int(existing_league_id)
    if existing_id:
        return existing_id

    current_league = fetch_current_league(required=True)
    return int(current_league['id']) if current_league else None


def _cache_is_fresh() -> bool:
    loaded_at = float(_DATA_CACHE.get('loaded_at') or 0.0)
    if loaded_at <= 0:
        return False
    if DATA_CACHE_TTL_SECONDS <= 0:
        return False
    return (time.time() - loaded_at) < DATA_CACHE_TTL_SECONDS


def _cache_has_core_data() -> bool:
    return all(_DATA_CACHE.get(key) is not None for key in ('players', 'matches', 'rating_history'))


def _cache_snapshot_from_memory() -> dict[str, Any]:
    return {
        'players': [dict(row) for row in _DATA_CACHE.get('players') or []],
        'matches': [dict(row) for row in _DATA_CACHE.get('matches') or []],
        'rating_history': [dict(row) for row in _DATA_CACHE.get('rating_history') or []],
        'all_leagues': copy.deepcopy(_DATA_CACHE.get('all_leagues') or []),
        'current_league': copy.deepcopy(_DATA_CACHE.get('current_league')),
        'player_league_badges': [dict(row) for row in _DATA_CACHE.get('player_league_badges') or []],
        'loaded_at': float(_DATA_CACHE.get('loaded_at') or 0.0),
        'version': int(_DATA_CACHE.get('version') or 0),
    }


def _empty_cache_snapshot() -> dict[str, Any]:
    return {
        'players': [],
        'matches': [],
        'rating_history': [],
        'all_leagues': [],
        'current_league': None,
        'player_league_badges': [],
        'loaded_at': 0.0,
        'version': int(_DATA_CACHE.get('version') or 0),
    }


def _apply_cache_snapshot(snapshot: dict[str, Any], *, increment_version: bool = False) -> dict[str, Any]:
    loaded_at = float(snapshot.get('loaded_at') or time.time())
    players = [dict(row) for row in snapshot.get('players') or []]
    matches = [dict(row) for row in snapshot.get('matches') or []]
    rating_history = [dict(row) for row in snapshot.get('rating_history') or []]
    all_leagues = copy.deepcopy(snapshot.get('all_leagues') or [])
    current_league = copy.deepcopy(snapshot.get('current_league'))
    player_league_badges = [dict(row) for row in snapshot.get('player_league_badges') or []]

    with _DATA_CACHE_LOCK:
        _DATA_CACHE['players'] = players
        _DATA_CACHE['matches'] = matches
        _DATA_CACHE['rating_history'] = rating_history
        _DATA_CACHE['all_leagues'] = all_leagues
        _DATA_CACHE['current_league'] = current_league
        _DATA_CACHE['player_league_badges'] = player_league_badges
        _DATA_CACHE['loaded_at'] = loaded_at
        if increment_version:
            _DATA_CACHE['version'] = int(_DATA_CACHE.get('version') or 0) + 1
        elif snapshot.get('version') is not None:
            _DATA_CACHE['version'] = int(snapshot.get('version') or 0)
        result = _cache_snapshot_from_memory()

    if all_leagues or current_league:
        with _LEAGUE_CONFIG_CACHE_LOCK:
            _LEAGUE_CONFIG_CACHE['all_leagues'] = copy.deepcopy(all_leagues)
            _LEAGUE_CONFIG_CACHE['current_league'] = copy.deepcopy(current_league)
            _LEAGUE_CONFIG_CACHE['loaded_at'] = loaded_at

    return result


def _read_disk_cache_snapshot() -> dict[str, Any] | None:
    if not USE_DISK_CACHE_ON_MISS:
        return None
    if DISK_CACHE_MAX_AGE_SECONDS <= 0:
        return None
    try:
        payload = json.loads(DISK_CACHE_PATH.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError, TypeError):
        return None
    if not isinstance(payload, dict):
        return None
    loaded_at = float(payload.get('loaded_at') or 0.0)
    if loaded_at <= 0 or (time.time() - loaded_at) > DISK_CACHE_MAX_AGE_SECONDS:
        return None
    if not all(isinstance(payload.get(key), list) for key in ('players', 'matches', 'rating_history')):
        return None
    return payload


def _write_disk_cache_snapshot(snapshot: dict[str, Any]) -> None:
    try:
        DISK_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        temp_path = DISK_CACHE_PATH.with_suffix(f'{DISK_CACHE_PATH.suffix}.tmp')
        temp_path.write_text(json.dumps(snapshot, ensure_ascii=False, separators=(',', ':')), encoding='utf-8')
        temp_path.replace(DISK_CACHE_PATH)
    except OSError:
        return


def _refresh_application_cache_background() -> None:
    global _DATA_CACHE_REFRESH_IN_PROGRESS
    with _DATA_CACHE_REFRESH_LOCK:
        if _DATA_CACHE_REFRESH_IN_PROGRESS:
            return
        _DATA_CACHE_REFRESH_IN_PROGRESS = True

    def worker() -> None:
        global _DATA_CACHE_REFRESH_IN_PROGRESS
        try:
            refresh_application_cache(force_refresh=True)
        except Exception:
            logging.getLogger(__name__).exception('Background application cache refresh failed')
        finally:
            with _DATA_CACHE_REFRESH_LOCK:
                _DATA_CACHE_REFRESH_IN_PROGRESS = False

    threading.Thread(target=worker, daemon=True).start()


def invalidate_application_cache() -> None:
    with _DATA_CACHE_LOCK:
        _DATA_CACHE['players'] = None
        _DATA_CACHE['matches'] = None
        _DATA_CACHE['rating_history'] = None
        _DATA_CACHE['all_leagues'] = None
        _DATA_CACHE['current_league'] = None
        _DATA_CACHE['player_league_badges'] = None
        _DATA_CACHE['loaded_at'] = 0.0
        _DATA_CACHE['version'] = int(_DATA_CACHE.get('version') or 0) + 1

    with _LEAGUE_SUMMARY_CACHE_LOCK:
        _LEAGUE_SUMMARY_CACHE.clear()

    with _LEAGUE_CONFIG_CACHE_LOCK:
        _LEAGUE_CONFIG_CACHE['all_leagues'] = None
        _LEAGUE_CONFIG_CACHE['current_league'] = None
        _LEAGUE_CONFIG_CACHE['loaded_at'] = 0.0

    invalidate_page_cache()


def warmup_application_cache(*, force_refresh: bool = False) -> dict[str, Any]:
    with _DATA_CACHE_LOCK:
        if not force_refresh and _cache_is_fresh() and _cache_has_core_data():
            return _cache_snapshot_from_memory()
        if not force_refresh and _cache_has_core_data():
            snapshot = _cache_snapshot_from_memory()
            _refresh_application_cache_background()
            return snapshot

    if not force_refresh:
        disk_snapshot = _read_disk_cache_snapshot()
        if disk_snapshot:
            snapshot = _apply_cache_snapshot(disk_snapshot)
            _refresh_application_cache_background()
            return snapshot
        if not BLOCKING_CACHE_LOAD_ON_MISS and ALLOW_EMPTY_CACHE_ON_MISS:
            _refresh_application_cache_background()
            return _empty_cache_snapshot()

    players = _rest_fetch_all('players', order='id.asc')
    matches = _rest_fetch_all('matches', order='played_at.asc,id.asc')
    rating_history = _rest_fetch_all('rating_history', order='id.asc')
    all_leagues = _fetch_all_leagues_uncached()
    current_league = _fetch_current_league_uncached(required=False)
    try:
        player_league_badges = _rest_fetch_all(
            LEAGUE_BADGES_TABLE_NAME,
            select='id,player_id,league_id,badge_code,awarded_at,awarded_match_id',
            order='league_id.desc,badge_code.asc',
        )
    except Exception as exc:
        if _is_missing_league_badges_table_error(exc):
            player_league_badges = []
        else:
            raise

    loaded_at = time.time()
    snapshot = {
        'players': players,
        'matches': matches,
        'rating_history': rating_history,
        'all_leagues': all_leagues,
        'current_league': current_league,
        'player_league_badges': player_league_badges,
        'loaded_at': loaded_at,
        'version': int(_DATA_CACHE.get('version') or 0),
    }

    result = _apply_cache_snapshot(snapshot, increment_version=force_refresh)
    _write_disk_cache_snapshot(result)
    return result


def _cache_snapshot(*, force_refresh: bool = False) -> dict[str, Any]:
    return warmup_application_cache(force_refresh=force_refresh)



def refresh_application_cache(*, force_refresh: bool = True) -> dict[str, Any]:
    if force_refresh:
        invalidate_application_cache()

    snapshot = warmup_application_cache(force_refresh=force_refresh)
    leagues = fetch_all_leagues(force_refresh=force_refresh)
    current_league = fetch_current_league(required=False, force_refresh=force_refresh)

    league_sections = fetch_league_results_overview()
    leaderboard_active = fetch_leaderboard(include_active=True, include_inactive=False, active_ranked_only=False)
    reports_page_1 = fetch_game_reports_page(page=1, per_page=25, ranked_only=False)

    with _PAGE_CACHE_LOCK:
        page_cache_entries = len(_PAGE_CACHE)

    return {
        'players_count': len(snapshot.get('players') or []),
        'matches_count': len(snapshot.get('matches') or []),
        'rating_history_count': len(snapshot.get('rating_history') or []),
        'leagues_count': len(leagues),
        'current_league_id': int(current_league['id']) if current_league else None,
        'league_sections_count': len(league_sections),
        'leaderboard_active_count': len(leaderboard_active),
        'reports_page_1_count': int(reports_page_1.get('total_count') or 0),
        'page_cache_entries': page_cache_entries,
        'cache_version': int(snapshot.get('version') or 0),
    }

def ping_database() -> tuple[bool, str | None]:
    if not HEALTH_CHECK_DATABASE:
        return True, None
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
    player['is_active'] = _is_player_active_by_last_match(player.get('last_match_at'))
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
    match['player1_roster_id'] = _normalize_roster_id(match.get('player1_roster_id'), field_label='Player 1 roster ID') if _normalize_text(match.get('player1_roster_id')) else ''
    match['player2_roster_id'] = _normalize_roster_id(match.get('player2_roster_id'), field_label='Player 2 roster ID') if _normalize_text(match.get('player2_roster_id')) else ''
    match['winner_roster_id'] = _normalize_roster_id(match.get('winner_roster_id'), field_label='Winner roster ID') if _normalize_text(match.get('winner_roster_id')) else ''
    match['loser_roster_id'] = _normalize_roster_id(match.get('loser_roster_id'), field_label='Loser roster ID') if _normalize_text(match.get('loser_roster_id')) else ''
    match['winner_profile_url'] = f"/players/{match['winner_id']}"
    match['loser_profile_url'] = f"/players/{match['loser_id']}"
    match['winner_elo_delta_display'] = _format_delta(match.get('winner_elo_delta'))
    match['loser_elo_delta_display'] = _format_delta(match.get('loser_elo_delta'))
    match['winner_rating_display'] = (
        f"{_normalize_elo_value(match.get('winner_old_elo'))} → {_normalize_elo_value(match.get('winner_new_elo'))}"
        if match.get('winner_old_elo') is not None and match.get('winner_new_elo') is not None
        else ''
    )
    match['loser_rating_display'] = (
        f"{_normalize_elo_value(match.get('loser_old_elo'))} → {_normalize_elo_value(match.get('loser_new_elo'))}"
        if match.get('loser_old_elo') is not None and match.get('loser_new_elo') is not None
        else ''
    )
    match['ranked_label'] = 'Ranked' if match.get('is_ranked') else 'Unranked'
    match['comment_display'] = _normalize_text(match.get('comment'))
    match['game_type_display'] = _normalize_text(match.get('game_type'))
    match['mission_name_display'] = _normalize_text(match.get('mission_name'))
    match['winner_score'] = _coerce_match_score_value(match.get('winner_score'), allow_blank=True, field_label='Winner score')
    match['loser_score'] = _coerce_match_score_value(match.get('loser_score'), allow_blank=True, field_label='Loser score')
    match['score_display'] = _format_match_score(match.get('winner_score'), match.get('loser_score'))
    match['result_type'] = _normalize_match_result_type(match.get('result_type'))
    match['is_tie'] = match['result_type'] == 'draw'
    match['result_label'] = 'TIE' if match['is_tie'] else 'WIN'
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
                    'label': point.get('date_label') or point.get('played_at_label') or '',
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


def _fetch_ranked_player_ids(*, force_refresh: bool = False) -> set[int]:
    matches = _fetch_all_matches_raw(force_refresh=force_refresh)
    ranked_player_ids: set[int] = set()

    for match in matches:
        if not bool(match.get('is_ranked')):
            continue

        try:
            ranked_player_ids.add(int(match['player1_id']))
            ranked_player_ids.add(int(match['player2_id']))
        except (KeyError, TypeError, ValueError):
            continue

    return ranked_player_ids


def _fetch_all_rating_history_raw(*, force_refresh: bool = False) -> list[dict]:
    snapshot = _cache_snapshot(force_refresh=force_refresh)
    return [dict(row) for row in snapshot['rating_history']]


def fetch_player_name_suggestions(limit: int = 500) -> list[str]:
    safe_limit = max(1, min(limit, 2000))
    rows = _fetch_all_players_raw()
    rows.sort(
        key=lambda row: (
            -int(row.get('current_elo') or 0),
            _normalize_player_name(row.get('name')).casefold(),
        )
    )

    suggestions: list[str] = []
    seen: set[str] = set()
    for row in rows:
        player_name = _normalize_player_name(row.get('name'))
        player_key = player_name.casefold()
        if not player_name or player_key in seen:
            continue
        seen.add(player_key)
        suggestions.append(player_name)
        if len(suggestions) >= safe_limit:
            break
    return suggestions


def fetch_mission_suggestions(limit: int = 50) -> list[str]:
    safe_limit = max(1, min(limit, 200))
    missions = []
    seen = set()
    rows = _fetch_all_matches_raw()
    rows.sort(
        key=lambda row: (
            _parse_datetime(row.get('played_at')) or datetime.min,
            int(row.get('id') or 0),
        ),
        reverse=True,
    )
    for row in rows:
        mission = _normalize_player_name(row.get('mission_name'))
        if mission and mission not in seen:
            seen.add(mission)
            missions.append(mission)
        if len(missions) >= safe_limit:
            break
    missions.sort()
    return missions[:safe_limit]


def _fetch_leaderboard_uncached(
    search: str = '',
    *,
    include_active: bool = True,
    include_inactive: bool = False,
    active_ranked_only: bool = False,
) -> list[dict]:
    normalized_search = _normalize_search_term(search).casefold()

    if not include_active and not include_inactive:
        include_active = True

    rows = _fetch_all_players_raw()
    ranked_player_ids = _fetch_ranked_player_ids() if active_ranked_only else set()

    filtered_rows: list[dict] = []
    for row in rows:
        prepared = dict(row)
        prepared['matches_count'] = int(row.get('matches_count') or 0)
        prepared['wins'] = int(row.get('wins') or 0)
        prepared['losses'] = int(row.get('losses') or 0)
        prepared['draws'] = int(row.get('draws') or 0)
        prepared['current_elo'] = int(row.get('current_elo') or 0)
        prepared['is_active'] = _is_player_active_by_last_match(row.get('last_match_at'))

        if normalized_search and normalized_search not in _normalize_player_name(prepared.get('name')).casefold():
            continue
        if include_active and not include_inactive and not prepared['is_active']:
            continue
        if include_inactive and not include_active and prepared['is_active']:
            continue
        if active_ranked_only and int(prepared['id']) not in ranked_player_ids:
            continue

        filtered_rows.append(prepared)

    filtered_rows.sort(
        key=lambda row: (
            -int(row.get('current_elo') or 0),
            -int(row.get('wins') or 0),
            -int(row.get('matches_count') or 0),
            _normalize_player_name(row.get('name')).casefold(),
        )
    )

    prepared_rows = []
    for rank_position, prepared in enumerate(filtered_rows, start=1):
        prepared['win_rate'] = round((prepared['wins'] / prepared['matches_count']) * 100, 1) if prepared['matches_count'] > 0 else 0
        prepared['rank_position'] = rank_position
        prepared_rows.append(_prepare_player_row(prepared))
    return prepared_rows


def fetch_leaderboard(
    search: str = '',
    *,
    include_active: bool = True,
    include_inactive: bool = False,
    active_ranked_only: bool = False,
) -> list[dict]:
    key = _make_page_cache_key(
        'leaderboard',
        search,
        include_active=include_active,
        include_inactive=include_inactive,
        active_ranked_only=active_ranked_only,
    )
    cached = _get_page_cache(key)
    if cached is not None:
        return cached
    result = _fetch_leaderboard_uncached(
        search=search,
        include_active=include_active,
        include_inactive=include_inactive,
        active_ranked_only=active_ranked_only,
    )
    return _set_page_cache(key, result)


def _fetch_player_match_rows(player_id: int, *, limit: int | None = None, order_desc: bool = True) -> list[dict]:
    query: dict[str, Any] = {
        'select': 'id,played_at,player1_id,player2_id,winner_player_id,player1_race,player2_race,is_ranked,game_type,mission_name,comment,result_type',
        'or': f'(player1_id.eq.{int(player_id)},player2_id.eq.{int(player_id)})',
        'order': 'played_at.desc,id.desc' if order_desc else 'played_at.asc,id.asc',
    }
    if limit is not None:
        query['limit'] = int(limit)
    return _rest_select_raw('matches', query=query)


def _fetch_players_by_ids(player_ids: list[int]) -> dict[int, dict]:
    unique_ids = sorted({int(player_id) for player_id in player_ids if player_id is not None})
    if not unique_ids:
        return {}
    cached = _fetch_players_by_ids_cached(unique_ids)
    if cached:
        return cached
    rows = _rest_select('players', select='id,name', filters=[('id', 'in', unique_ids)])
    return {int(row['id']): dict(row) for row in rows}


def _fetch_history_for_matches(match_ids: list[int], player_ids: list[int]) -> dict[tuple[int, int], dict]:
    unique_match_ids = sorted({int(match_id) for match_id in match_ids if match_id is not None})
    unique_player_ids = sorted({int(player_id) for player_id in player_ids if player_id is not None})
    if not unique_match_ids or not unique_player_ids:
        return {}

    match_id_set = set(unique_match_ids)
    player_id_set = set(unique_player_ids)
    rows = [
        row for row in _fetch_all_rating_history_raw()
        if int(row.get('match_id') or 0) in match_id_set
        and int(row.get('player_id') or 0) in player_id_set
    ]
    return {(int(row['match_id']), int(row['player_id'])): dict(row) for row in rows}


def _build_match_search_or_clause(search: str) -> str:
    normalized_search = _normalize_search_term(search)
    if not normalized_search:
        return ''

    like_value = f'*{normalized_search}*'
    or_parts = [
        f'comment.ilike.{like_value}',
        f'game_type.ilike.{like_value}',
        f'mission_name.ilike.{like_value}',
    ]

    matching_players = _rest_select(
        'players',
        select='id',
        filters=[('name', 'ilike', like_value)],
        limit=200,
    )
    matching_player_ids = [str(int(row['id'])) for row in matching_players]
    if matching_player_ids:
        joined_ids = ','.join(matching_player_ids)
        or_parts.append(f'player1_id.in.({joined_ids})')
        or_parts.append(f'player2_id.in.({joined_ids})')

    return '(' + ','.join(or_parts) + ')'


def _fetch_game_reports_page_uncached(search: str = '', page: int = 1, per_page: int = 25, *, ranked_only: bool = False) -> dict:
    safe_per_page = max(1, min(int(per_page or 25), 100))
    requested_page = max(1, int(page or 1))

    current_league = fetch_current_league(required=False)
    current_league_id = _coerce_positive_int((current_league or {}).get('id'))

    all_matches = _fetch_all_matches_raw()
    all_player_ids: set[int] = set()

    for match in all_matches:
        try:
            all_player_ids.add(int(match.get('player1_id') or 0))
            all_player_ids.add(int(match.get('player2_id') or 0))
        except (TypeError, ValueError):
            continue

    players_by_id = _fetch_players_by_ids_cached(list(all_player_ids))

    normalized_search = _normalize_search_term(search).casefold()

    def match_is_in_reports_scope(match: dict) -> bool:
        match_league_id = _coerce_positive_int(match.get('league_id'))
        match_is_ranked = bool(match.get('is_ranked'))

        if ranked_only:
            return bool(
                current_league_id
                and match_is_ranked
                and match_league_id == current_league_id
            )

        is_current_league_match = bool(
            current_league_id
            and match_league_id == current_league_id
        )

        is_friendly_null_match = bool(
            match_league_id is None
            and match_is_ranked is False
        )

        return is_current_league_match or is_friendly_null_match

    def match_passes_search(match: dict) -> bool:
        if not normalized_search:
            return True

        player1_id = _coerce_positive_int(match.get('player1_id'))
        player2_id = _coerce_positive_int(match.get('player2_id'))

        player1_name = _normalize_player_name((players_by_id.get(player1_id) or {}).get('name')).casefold()
        player2_name = _normalize_player_name((players_by_id.get(player2_id) or {}).get('name')).casefold()

        searchable_parts = [
            _normalize_text(match.get('comment')).casefold(),
            _normalize_text(match.get('game_type')).casefold(),
            _normalize_text(match.get('mission_name')).casefold(),
            player1_name,
            player2_name,
        ]

        return any(normalized_search in part for part in searchable_parts if part)

    filtered_matches = [
        dict(match)
        for match in all_matches
        if match_is_in_reports_scope(match) and match_passes_search(match)
    ]

    filtered_matches.sort(
        key=lambda row: (
            int(row.get('id') or 0),
            _parse_datetime(row.get('played_at')) or datetime.min,
        ),
        reverse=True,
    )

    total_count = len(filtered_matches)
    total_pages = max(1, math.ceil(total_count / safe_per_page)) if total_count else 1
    safe_page = min(requested_page, total_pages)
    offset = (safe_page - 1) * safe_per_page
    match_rows = filtered_matches[offset:offset + safe_per_page]

    page_match_ids = [int(row['id']) for row in match_rows]
    page_player_ids = []
    for row in match_rows:
        page_player_ids.append(int(row['player1_id']))
        page_player_ids.append(int(row['player2_id']))

    page_players_by_id = _fetch_players_by_ids(page_player_ids)
    history_by_pair = _fetch_history_for_matches(page_match_ids, page_player_ids)

    items = []
    for row in match_rows:
        match = dict(row)
        player1_id = int(match['player1_id'])
        player2_id = int(match['player2_id'])
        result_type = _normalize_match_result_type(match.get('result_type'))
        is_tie = result_type == 'draw'

        player1 = page_players_by_id.get(player1_id) or players_by_id.get(player1_id)
        player2 = page_players_by_id.get(player2_id) or players_by_id.get(player2_id)
        if not player1 or not player2:
            continue

        player1_name = _normalize_player_name(player1.get('name'))
        player2_name = _normalize_player_name(player2.get('name'))

        if is_tie:
            winner_id = player1_id
            loser_id = player2_id
            winner_name = player1_name
            loser_name = player2_name
            winner_race = match.get('player1_race')
            loser_race = match.get('player2_race')
        else:
            winner_id = _coerce_positive_int(match.get('winner_player_id'))
            if winner_id not in {player1_id, player2_id}:
                continue

            loser_id = player2_id if winner_id == player1_id else player1_id
            winner_name = player1_name if winner_id == player1_id else player2_name
            loser_name = player2_name if winner_id == player1_id else player1_name
            winner_race = match.get('player1_race') if winner_id == player1_id else match.get('player2_race')
            loser_race = match.get('player2_race') if winner_id == player1_id else match.get('player1_race')

        winner_history = history_by_pair.get((int(match['id']), winner_id), {})
        loser_history = history_by_pair.get((int(match['id']), loser_id), {})

        score_details = _extract_match_score_details(match)
        player1_score = score_details['player1_score']
        player2_score = score_details['player2_score']

        if is_tie or winner_id == player1_id:
            winner_score = player1_score
            loser_score = player2_score
            winner_roster_id = score_details['player1_roster_id']
            loser_roster_id = score_details['player2_roster_id']
        else:
            winner_score = player2_score
            loser_score = player1_score
            winner_roster_id = score_details['player2_roster_id']
            loser_roster_id = score_details['player1_roster_id']

        match_league_id = _coerce_positive_int(match.get('league_id'))
        is_friendly_null_match = match_league_id is None and bool(match.get('is_ranked')) is False

        item = {
            'id': int(match['id']),
            'played_at': match.get('played_at'),
            'winner_id': winner_id,
            'winner_name': winner_name,
            'loser_id': loser_id,
            'loser_name': loser_name,
            'winner_race': winner_race,
            'loser_race': loser_race,
            'winner_score': winner_score,
            'loser_score': loser_score,
            'is_ranked': bool(match.get('is_ranked')),
            'game_type': match.get('game_type'),
            'mission_name': match.get('mission_name'),
            'comment': score_details['visible_comment'],
            'player1_roster_id': score_details['player1_roster_id'],
            'player2_roster_id': score_details['player2_roster_id'],
            'winner_roster_id': winner_roster_id,
            'loser_roster_id': loser_roster_id,
            'winner_old_elo': winner_history.get('old_elo'),
            'winner_new_elo': winner_history.get('new_elo'),
            'winner_elo_delta': winner_history.get('elo_delta'),
            'loser_old_elo': loser_history.get('old_elo'),
            'loser_new_elo': loser_history.get('new_elo'),
            'loser_elo_delta': loser_history.get('elo_delta'),
            'result_type': result_type,
            'is_tie': is_tie,
            'league_id': match_league_id,
            'league_name': 'Friendly' if is_friendly_null_match else ((current_league or {}).get('name') or ''),
        }

        items.append(_prepare_game_report_row(item))

    return {
        'items': items,
        'total_count': total_count,
        'page': safe_page,
        'per_page': safe_per_page,
        'total_pages': total_pages,
        'current_league': current_league,
    }

def fetch_game_reports_page(search: str = '', page: int = 1, per_page: int = 25, *, ranked_only: bool = False) -> dict:
    key = _make_page_cache_key(
        'game_reports_page',
        search,
        page=int(page or 1),
        per_page=int(per_page or 25),
        ranked_only=bool(ranked_only),
    )
    cached = _get_page_cache(key)
    if cached is not None:
        return cached
    result = _fetch_game_reports_page_uncached(
        search=search,
        page=page,
        per_page=per_page,
        ranked_only=ranked_only,
    )
    return _set_page_cache(key, result)


def fetch_game_reports(search: str = '', limit: int = 100, *, ranked_only: bool = False) -> list[dict]:
    page_data = fetch_game_reports_page(search=search, page=1, per_page=limit, ranked_only=ranked_only)
    return page_data['items']




def _race_matchup_report_from_matches(player_id: int, priority_race: str | None, matches: list[dict]) -> dict:
    normalized_priority_race = _normalize_race_label(priority_race)
    race_code_by_name = {
        'Terran': 'T',
        'Zerg': 'Z',
        'Protoss': 'P',
    }
    matchup_order = (
        ('Terran', 'T', 'terran'),
        ('Zerg', 'Z', 'zerg'),
        ('Protoss', 'P', 'protoss'),
    )

    cards_map: dict[str, dict[str, Any]] = {
        opponent_name: {
            'opponent_race': opponent_name,
            'opponent_slug': opponent_slug,
            'matchup_code': f"{race_code_by_name.get(normalized_priority_race, 'Pr')}v{opponent_code}",
            'wins': 0,
            'losses': 0,
            'total_games': 0,
            'win_rate': 0.0,
            'loss_rate': 0.0,
        }
        for opponent_name, opponent_code, opponent_slug in matchup_order
    }

    if normalized_priority_race not in race_code_by_name:
        return {
            'priority_race': normalized_priority_race,
            'priority_race_code': 'Pr',
            'cards': list(cards_map.values()),
        }

    player_id = int(player_id)

    for match in matches:
        player1_id = int(match['player1_id'])
        player2_id = int(match['player2_id'])
        if player_id not in {player1_id, player2_id}:
            continue

        player_race = _normalize_race_label(
            match.get('player1_race') if player1_id == player_id else match.get('player2_race')
        )
        opponent_race = _normalize_race_label(
            match.get('player2_race') if player1_id == player_id else match.get('player1_race')
        )

        if player_race != normalized_priority_race or opponent_race not in cards_map:
            continue

        if _is_match_draw(match):
            continue

        card = cards_map[opponent_race]
        card['total_games'] += 1
        if int(match['winner_player_id']) == player_id:
            card['wins'] += 1
        else:
            card['losses'] += 1

    cards = []
    for opponent_name, opponent_code, opponent_slug in matchup_order:
        card = cards_map[opponent_name]
        total_games = int(card['total_games'])
        wins = int(card['wins'])
        losses = int(card['losses'])
        card['matchup_code'] = f"{race_code_by_name[normalized_priority_race]}v{opponent_code}"
        card['opponent_slug'] = opponent_slug
        card['win_rate'] = round((wins / total_games) * 100, 1) if total_games > 0 else 0.0
        card['loss_rate'] = round((losses / total_games) * 100, 1) if total_games > 0 else 0.0
        cards.append(card)

    return {
        'priority_race': normalized_priority_race,
        'priority_race_code': race_code_by_name[normalized_priority_race],
        'cards': cards,
    }


def _format_head_to_head_record(wins: int, losses: int, draws: int = 0) -> str:
    record = f'{int(wins or 0)}-{int(losses or 0)}'
    if int(draws or 0) > 0:
        record += f'-{int(draws or 0)}'
    return record


def _datetime_sort_tuple(value) -> tuple[int, int, int]:
    parsed = _parse_datetime(value)
    if not parsed:
        return (0, 0, 0)
    if getattr(parsed, 'tzinfo', None):
        parsed = parsed.replace(tzinfo=None)
    seconds = (parsed.hour * 3600) + (parsed.minute * 60) + parsed.second
    return (parsed.toordinal(), seconds, parsed.microsecond)


def _build_head_to_head_points_preview(*, is_player_favorite: bool, opponent_has_enough_matches: bool) -> dict:
    if not opponent_has_enough_matches:
        return {
            'win': '0',
            'draw': '0',
            'loss': '0',
        }

    return {
        'win': _format_league_points(LEAGUE_POINTS_FAVORITE_WIN if is_player_favorite else LEAGUE_POINTS_WIN),
        'draw': _format_league_points(LEAGUE_POINTS_FAVORITE_DRAW if is_player_favorite else LEAGUE_POINTS_DRAW),
        'loss': _format_league_points(LEAGUE_POINTS_FAVORITE_LOSS if is_player_favorite else LEAGUE_POINTS_LOSS),
    }


def _build_player_head_to_head_report(
    player_id: int,
    player: dict,
    matches: list[dict],
    opponents_by_id: dict[int, dict],
    history_by_match_id: dict[int, dict],
) -> dict:
    player_id = int(player_id)
    player_name = _normalize_player_name(player.get('name')) or f'Player {player_id}'

    try:
        current_league = fetch_current_league(required=False)
    except Exception:
        current_league = None

    current_league_id = _coerce_positive_int((current_league or {}).get('id'))
    current_league_name = _normalize_text((current_league or {}).get('name')) if current_league else ''

    current_league_match_counts_by_player: dict[int, int] = defaultdict(int)
    if current_league_id:
        for row in _fetch_all_matches_raw():
            if not bool(row.get('is_ranked')):
                continue
            if _coerce_positive_int(row.get('league_id')) != current_league_id:
                continue
            player1_id = _coerce_positive_int(row.get('player1_id'))
            player2_id = _coerce_positive_int(row.get('player2_id'))
            if player1_id:
                current_league_match_counts_by_player[int(player1_id)] += 1
            if player2_id:
                current_league_match_counts_by_player[int(player2_id)] += 1

    league_points_delta_lookup = _build_league_points_delta_lookup()
    current_league_player_points = 0.0
    current_league_player_matches = 0

    league_ids = [
        league_id
        for league_id in (_coerce_positive_int(match.get('league_id')) for match in matches)
        if league_id
    ]
    if current_league_id:
        league_ids.append(current_league_id)
    leagues_by_id = _fetch_leagues_by_ids(league_ids)

    opponents: dict[int, dict[str, Any]] = {}

    for match in matches:
        player1_id = _coerce_positive_int(match.get('player1_id'))
        player2_id = _coerce_positive_int(match.get('player2_id'))
        if not player1_id or not player2_id or player_id not in {player1_id, player2_id}:
            continue

        opponent_id = player2_id if player1_id == player_id else player1_id
        opponent = opponents_by_id.get(int(opponent_id)) or {'id': opponent_id, 'name': f'Player {opponent_id}'}
        opponent_name = _normalize_player_name(opponent.get('name')) or f'Player {opponent_id}'

        stats = opponents.setdefault(
            int(opponent_id),
            {
                'opponent_id': int(opponent_id),
                'opponent_name': opponent_name,
                'opponent_profile_url': f'/players/{int(opponent_id)}',
                'opponent_priority_race': _normalize_race_label(opponent.get('priority_race')),
                'opponent_flag_url': _resolve_flag_url(opponent.get('country_code'), opponent.get('country_name')),
                'opponent_current_elo_display': _normalize_elo_value(opponent.get('current_elo')),
                'matches_count': 0,
                'wins': 0,
                'losses': 0,
                'draws': 0,
                'latest_played_at': '',
                'current_league': {
                    'league_id': int(current_league_id) if current_league_id else None,
                    'league_name': current_league_name,
                    'matches_count': 0,
                    'wins': 0,
                    'losses': 0,
                    'draws': 0,
                },
                'matches': [],
            },
        )

        is_player1 = player1_id == player_id
        is_tie = _is_match_draw(match)
        is_win = False
        is_loss = False
        result_label = 'Tie'
        if not is_tie:
            is_win = _coerce_positive_int(match.get('winner_player_id')) == player_id
            is_loss = not is_win
            result_label = 'Win' if is_win else 'Loss'

        stats['matches_count'] += 1
        if is_tie:
            stats['draws'] += 1
        elif is_win:
            stats['wins'] += 1
        elif is_loss:
            stats['losses'] += 1

        played_at = _normalize_text(match.get('played_at'))
        if not stats['latest_played_at'] or _datetime_sort_tuple(played_at) > _datetime_sort_tuple(stats['latest_played_at']):
            stats['latest_played_at'] = played_at

        score_details = _extract_match_score_details(match)
        player_score = score_details['player1_score'] if is_player1 else score_details['player2_score']
        opponent_score = score_details['player2_score'] if is_player1 else score_details['player1_score']
        league_id = _coerce_positive_int(match.get('league_id'))
        league = leagues_by_id.get(int(league_id)) if league_id else None
        league_name = _normalize_text((league or {}).get('name'))
        if not league_name:
            league_name = 'Friendly' if not bool(match.get('is_ranked')) else 'Ranked'

        match_id = int(match.get('id') or 0)
        history_row = history_by_match_id.get(match_id, {})
        league_points_delta = league_points_delta_lookup.get((match_id, player_id))
        if current_league_id and league_id == current_league_id and bool(match.get('is_ranked')):
            current_league_player_matches += 1
            current_league_player_points += float(league_points_delta or 0.0)
        if league_points_delta is None:
            league_points_delta_class = ''
        elif league_points_delta > 0:
            league_points_delta_class = 'delta-win'
        elif league_points_delta < 0:
            league_points_delta_class = 'delta-loss'
        else:
            league_points_delta_class = 'delta-tie'

        match_row = {
            'id': match_id,
            'played_at': played_at,
            'played_at_label': _format_match_date(match.get('played_at')),
            'result_label': result_label,
            'is_win': is_win,
            'is_loss': is_loss,
            'is_tie': is_tie,
            'player_race': _normalize_race_label(match.get('player1_race') if is_player1 else match.get('player2_race')),
            'opponent_race': _normalize_race_label(match.get('player2_race') if is_player1 else match.get('player1_race')),
            'score_display': _format_match_score(player_score, opponent_score),
            'elo_delta_display': _format_delta(history_row.get('elo_delta')),
            'is_ranked': bool(match.get('is_ranked')),
            'ranked_label': 'Ranked' if bool(match.get('is_ranked')) else 'Unranked',
            'league_id': int(league_id) if league_id else None,
            'league_name': league_name,
            'league_points_delta': league_points_delta,
            'league_points_delta_display': _format_league_points_delta(league_points_delta),
            'league_points_delta_class': league_points_delta_class,
            'is_current_league': bool(current_league_id and league_id == current_league_id and bool(match.get('is_ranked'))),
        }
        stats['matches'].append(match_row)

        if match_row['is_current_league']:
            league_stats = stats['current_league']
            league_stats['matches_count'] += 1
            if is_tie:
                league_stats['draws'] += 1
            elif is_win:
                league_stats['wins'] += 1
            elif is_loss:
                league_stats['losses'] += 1

    prepared_opponents: list[dict] = []
    for stats in opponents.values():
        wins = int(stats.get('wins') or 0)
        losses = int(stats.get('losses') or 0)
        draws = int(stats.get('draws') or 0)
        matches_count = int(stats.get('matches_count') or 0)
        stats['score_display'] = _format_head_to_head_record(wins, losses, draws)
        stats['win_rate_numeric'] = round((wins / matches_count) * 100, 1) if matches_count else 0.0
        stats['win_rate_display'] = _format_percent(stats['win_rate_numeric'])
        stats['matches_label'] = f'{matches_count} match' + ('' if matches_count == 1 else 'es')

        league_stats = stats['current_league']
        league_wins = int(league_stats.get('wins') or 0)
        league_losses = int(league_stats.get('losses') or 0)
        league_draws = int(league_stats.get('draws') or 0)
        league_matches = int(league_stats.get('matches_count') or 0)
        player_lead = league_wins - league_losses
        is_player_favorite = current_league_id is not None and player_lead >= LEAGUE_HEAD_TO_HEAD_POINT_LEAD_LIMIT
        is_opponent_favorite = current_league_id is not None and -player_lead >= LEAGUE_HEAD_TO_HEAD_POINT_LEAD_LIMIT

        if not current_league_id:
            favorite_status = 'no-league'
            favorite_label = 'No current league'
            favorite_detail = 'Current league is not configured.'
        elif league_matches <= 0:
            favorite_status = 'none'
            favorite_label = 'No favorite yet'
            favorite_detail = f'No ranked matches in {current_league_name or "the current league"} yet.'
        elif is_player_favorite:
            favorite_status = 'player'
            favorite_label = f'{player_name} is favorite'
            favorite_detail = f'{player_name} leads {league_wins}-{league_losses} in {current_league_name or "the current league"}.'
        elif is_opponent_favorite:
            favorite_status = 'opponent'
            favorite_label = f'{stats["opponent_name"]} is favorite'
            favorite_detail = f'{stats["opponent_name"]} leads {league_losses}-{league_wins} in {current_league_name or "the current league"}.'
        else:
            favorite_status = 'none'
            favorite_label = 'No favorite yet'
            favorite_detail = f'Favorite starts at a {LEAGUE_HEAD_TO_HEAD_POINT_LEAD_LIMIT}+ win lead.'

        opponent_league_matches = int(current_league_match_counts_by_player.get(int(stats['opponent_id']), 0) or 0)
        opponent_has_enough_matches = bool(
            current_league_id
            and _opponent_has_enough_league_matches(current_league_match_counts_by_player, int(stats['opponent_id']))
        )
        points_preview = _build_head_to_head_points_preview(
            is_player_favorite=is_player_favorite,
            opponent_has_enough_matches=opponent_has_enough_matches,
        )

        league_stats['score_display'] = _format_head_to_head_record(league_wins, league_losses, league_draws)
        league_stats['win_rate_numeric'] = round((league_wins / league_matches) * 100, 1) if league_matches else 0.0
        league_stats['win_rate_display'] = _format_percent(league_stats['win_rate_numeric'])
        league_stats['player_lead'] = player_lead
        league_stats['favorite_status'] = favorite_status
        league_stats['favorite_label'] = favorite_label
        league_stats['favorite_detail'] = favorite_detail
        league_stats['is_player_favorite'] = is_player_favorite
        league_stats['is_opponent_favorite'] = is_opponent_favorite
        league_stats['opponent_league_matches'] = opponent_league_matches
        league_stats['opponent_points_eligible'] = opponent_has_enough_matches
        league_stats['points_preview'] = points_preview
        if not current_league_id:
            league_stats['points_eligibility_label'] = 'No current league'
        elif opponent_has_enough_matches:
            league_stats['points_eligibility_label'] = 'Points active'
        else:
            league_stats['points_eligibility_label'] = (
                f'Opponent has {opponent_league_matches}/{LEAGUE_POINTS_MIN_OPPONENT_MATCHES} league matches'
            )

        stats['matches'].sort(
            key=lambda row: (_datetime_sort_tuple(row.get('played_at')), int(row.get('id') or 0)),
            reverse=True,
        )
        prepared_opponents.append(stats)

    prepared_opponents.sort(
        key=lambda row: (
            -int((row.get('current_league') or {}).get('matches_count') or 0),
            -int(row.get('matches_count') or 0),
            tuple(-part for part in _datetime_sort_tuple(row.get('latest_played_at'))),
            _normalize_player_name(row.get('opponent_name')).casefold(),
        ),
    )

    return {
        'player_id': player_id,
        'player_name': player_name,
        'current_league': current_league,
        'current_league_player': {
            'league_id': int(current_league_id) if current_league_id else None,
            'league_name': current_league_name,
            'points': current_league_player_points,
            'points_display': _format_league_points(current_league_player_points),
            'matches_count': current_league_player_matches,
            'matches_label': f'{current_league_player_matches} match' + ('' if current_league_player_matches == 1 else 'es'),
        },
        'points_rules': LEAGUE_POINTS_RULES_TEXT,
        'opponents': prepared_opponents,
    }



def _get_cached_player_by_id(player_id: int) -> dict | None:
    target_id = int(player_id)
    for row in _fetch_all_players_raw():
        if int(row.get('id') or 0) == target_id:
            return dict(row)
    return None


def _fetch_player_match_rows_cached(player_id: int, *, order_desc: bool = True) -> list[dict]:
    target_id = int(player_id)
    rows = []
    for row in _fetch_all_matches_raw():
        try:
            player1_id = int(row.get('player1_id') or 0)
            player2_id = int(row.get('player2_id') or 0)
        except (TypeError, ValueError):
            continue
        if target_id in {player1_id, player2_id}:
            rows.append(dict(row))

    rows.sort(
        key=lambda row: (_parse_datetime(row.get('played_at')) or datetime.min, int(row.get('id') or 0)),
        reverse=order_desc,
    )
    return rows


def _fetch_players_by_ids_cached(player_ids: list[int]) -> dict[int, dict]:
    target_ids = {int(player_id) for player_id in player_ids if player_id is not None}
    if not target_ids:
        return {}
    return {
        int(row['id']): dict(row)
        for row in _fetch_all_players_raw()
        if int(row.get('id') or 0) in target_ids
    }


def _fetch_rating_history_for_player_cached(player_id: int) -> list[dict]:
    target_id = int(player_id)
    rows = [dict(row) for row in _fetch_all_rating_history_raw() if int(row.get('player_id') or 0) == target_id]
    rows.sort(key=lambda row: int(row.get('id') or 0))
    return rows


def _summarize_player_record_from_matches(player_id: int, matches: list[dict]) -> dict[str, Any]:
    target_id = int(player_id)
    stats: dict[str, Any] = {
        'matches_count': 0,
        'wins': 0,
        'losses': 0,
        'draws': 0,
        'last_match_at': None,
        'priority_race': None,
    }
    race_counts: dict[str, int] = defaultdict(int)

    for match in matches:
        player1_id = _coerce_positive_int(match.get('player1_id'))
        player2_id = _coerce_positive_int(match.get('player2_id'))
        if not player1_id or not player2_id or target_id not in {player1_id, player2_id}:
            continue

        is_player1 = player1_id == target_id
        stats['matches_count'] += 1

        if _is_match_draw(match):
            stats['draws'] += 1
        else:
            winner_id = _coerce_positive_int(match.get('winner_player_id'))
            if winner_id == target_id:
                stats['wins'] += 1
            elif winner_id in {player1_id, player2_id}:
                stats['losses'] += 1

        played_at = match.get('played_at')
        if played_at and (
            not stats['last_match_at']
            or _datetime_sort_tuple(played_at) > _datetime_sort_tuple(stats['last_match_at'])
        ):
            stats['last_match_at'] = played_at

        race = _normalize_race_label(match.get('player1_race') if is_player1 else match.get('player2_race'))
        if race:
            race_counts[race] += 1

    if race_counts:
        stats['priority_race'] = sorted(race_counts.items(), key=lambda item: (-item[1], item[0]))[0][0]

    return stats


def _fetch_player_profile_uncached(player_id: int, recent_matches_limit: int = 20) -> dict | None:
    safe_recent_matches_limit = max(1, min(recent_matches_limit, 50))
    player_row = _get_cached_player_by_id(int(player_id))
    if not player_row:
        return None

    matches = _fetch_player_match_rows_cached(int(player_id), order_desc=True)
    match_summary = _summarize_player_record_from_matches(int(player_id), matches)

    player_base = dict(player_row)
    player_base['matches_count'] = int(match_summary.get('matches_count') or 0)
    player_base['wins'] = int(match_summary.get('wins') or 0)
    player_base['losses'] = int(match_summary.get('losses') or 0)
    player_base['draws'] = int(match_summary.get('draws') or 0)
    if match_summary.get('last_match_at'):
        player_base['last_match_at'] = match_summary['last_match_at']
    if match_summary.get('priority_race'):
        player_base['priority_race'] = match_summary['priority_race']
    player_base['win_rate'] = round((player_base['wins'] / player_base['matches_count']) * 100, 1) if player_base['matches_count'] > 0 else 0
    player = _prepare_player_row(player_base)
    player['rank_position'] = _compute_player_rank_position(int(player_id))
    player = decorate_player_with_current_league_awards(player)
    player['badges'] = fetch_player_badges(int(player_id))

    match_ids = [int(match['id']) for match in matches]
    opponent_ids = [
        int(match['player2_id']) if int(match['player1_id']) == int(player_id) else int(match['player1_id'])
        for match in matches
    ]
    opponents_by_id = _fetch_players_by_ids_cached(opponent_ids)

    history_rows = _fetch_rating_history_for_player_cached(int(player_id))
    history_by_match_id = {int(row['match_id']): dict(row) for row in history_rows}

    recent_matches = []
    for match in matches[:safe_recent_matches_limit]:
        match_id = int(match['id'])
        player1_id = int(match['player1_id'])
        player2_id = int(match['player2_id'])
        opponent_id = player2_id if player1_id == int(player_id) else player1_id
        opponent = opponents_by_id.get(opponent_id)
        if not opponent:
            continue

        history_row = history_by_match_id.get(match_id)
        is_tie = _is_match_draw(match)
        is_win = False
        is_loss = False
        result_label = 'TIE'
        if not is_tie:
            is_win = int(match['winner_player_id']) == int(player_id)
            is_loss = not is_win
            result_label = 'Win' if is_win else 'Loss'

        score_details = _extract_match_score_details(match)
        player_score = score_details['player1_score'] if player1_id == int(player_id) else score_details['player2_score']
        opponent_score = score_details['player2_score'] if player1_id == int(player_id) else score_details['player1_score']

        prepared = {
            'id': match_id,
            'played_at': match.get('played_at'),
            'opponent_id': opponent_id,
            'opponent_name': _normalize_player_name(opponent.get('name')),
            'result_label': result_label,
            'is_win': is_win,
            'is_loss': is_loss,
            'is_tie': is_tie,
            'player_race': match.get('player1_race') if player1_id == int(player_id) else match.get('player2_race'),
            'opponent_race': match.get('player2_race') if player1_id == int(player_id) else match.get('player1_race'),
            'player_score': player_score,
            'opponent_score': opponent_score,
            'score_display': _format_match_score(player_score, opponent_score),
            'old_elo': history_row.get('old_elo') if history_row else None,
            'new_elo': history_row.get('new_elo') if history_row else None,
            'elo_delta': history_row.get('elo_delta') if history_row else None,
            'opponent_profile_url': f"/players/{opponent_id}",
        }
        prepared['played_at_label'] = _format_match_date(prepared.get('played_at'))
        prepared['player_race'] = _normalize_race_label(prepared.get('player_race'))
        prepared['opponent_race'] = _normalize_race_label(prepared.get('opponent_race'))
        prepared['old_elo_display'] = _normalize_elo_value(prepared.get('old_elo'))
        prepared['new_elo_display'] = _normalize_elo_value(prepared.get('new_elo'))
        prepared['elo_delta_display'] = _format_delta(prepared.get('elo_delta'))
        recent_matches.append(prepared)

    matches_by_id = {int(match['id']): match for match in matches}
    rating_chart_rows = []
    for history_row in history_rows:
        match = matches_by_id.get(int(history_row['match_id']))
        if not match:
            continue
        rating_chart_rows.append(
            {
                'played_at': match.get('played_at'),
                'old_elo': history_row.get('old_elo'),
                'new_elo': history_row.get('new_elo'),
                'elo_delta': history_row.get('elo_delta'),
            }
        )

    rating_chart_rows.sort(key=lambda row: _parse_datetime(row.get('played_at')) or datetime.min)
    player['member_since_label'] = _format_match_datetime(player_row.get('created_at'))
    rating_chart = _build_rating_chart(player.get('current_elo'), rating_chart_rows)
    priority_matchup_report = _race_matchup_report_from_matches(player_id, player.get('priority_race'), matches)
    head_to_head_report = _build_player_head_to_head_report(
        int(player_id),
        player,
        matches,
        opponents_by_id,
        history_by_match_id,
    )

    return {
        'player': player,
        'recent_matches': recent_matches,
        'rating_chart': rating_chart,
        'priority_matchup_report': priority_matchup_report,
        'head_to_head_report': head_to_head_report,
    }


def fetch_player_profile(player_id: int, recent_matches_limit: int = 20) -> dict | None:
    key = _make_page_cache_key(
        'player_profile',
        int(player_id),
        recent_matches_limit=int(recent_matches_limit or 20),
    )
    cached = _get_page_cache(key)
    if cached is not None:
        return cached
    result = _fetch_player_profile_uncached(player_id, recent_matches_limit=recent_matches_limit)
    return _set_page_cache(key, result)


def _rest_get_player_by_name_key(name_key: str) -> dict | None:
    return _rest_select('players', filters=[('name_normalized', 'eq', name_key)], single=True)


def _rest_get_player_by_id(player_id: int) -> dict | None:
    return _rest_select('players', filters=[('id', 'eq', player_id)], single=True)


def _compute_player_rank_position(player_id: int) -> int | str:
    rows = _fetch_all_players_raw()
    rows.sort(
        key=lambda row: (
            -int(row.get('current_elo') or 0),
            -int(row.get('wins') or 0),
            -int(row.get('matches_count') or 0),
            _normalize_player_name(row.get('name')).casefold(),
        )
    )

    for index, row in enumerate(rows, start=1):
        if int(row['id']) == int(player_id):
            return index

    return ''


def _get_or_create_player(player_name: str) -> dict:
    resolved_player_name = resolve_player_canonical_name(player_name)
    normalized_name = _normalize_player_name(resolved_player_name)
    normalized_key = _normalize_player_key(resolved_player_name)

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

    try:
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
    except Exception:
        existing = _rest_get_player_by_name_key(normalized_key)
        if existing:
            existing = dict(existing)
            existing['created'] = False
            return existing
        raise


def _find_recent_duplicate_match(
    *,
    player1_id: int,
    player2_id: int,
    player1_race: str,
    player2_race: str,
    is_ranked: bool,
    game_type: str,
    mission_name: str,
    comment: str,
    result_type: str,
    league_id: int | None,
    submitted_at: datetime,
    window_seconds: int = 5,
) -> dict | None:
    rows = _rest_select(
        'matches',
        select='id,played_at,player1_id,player2_id,winner_player_id,player1_race,player2_race,is_ranked,game_type,mission_name,comment,result_type,league_id',
        filters=[('player1_id', 'eq', int(player1_id)), ('player2_id', 'eq', int(player2_id))],
        order='played_at.desc,id.desc',
        limit=20,
    )

    for row in rows:
        played_at = _parse_datetime(row.get('played_at'))
        if not played_at:
            continue

        if getattr(played_at, 'tzinfo', None):
            played_at_naive = played_at.replace(tzinfo=None)
        else:
            played_at_naive = played_at

        delta_seconds = abs((submitted_at - played_at_naive).total_seconds())
        if delta_seconds > window_seconds:
            continue
        if _normalize_text(row.get('player1_race')) != _normalize_text(player1_race):
            continue
        if _normalize_text(row.get('player2_race')) != _normalize_text(player2_race):
            continue
        if bool(row.get('is_ranked')) != bool(is_ranked):
            continue
        if _normalize_text(row.get('game_type')) != _normalize_text(game_type):
            continue
        if _normalize_player_name(row.get('mission_name')) != _normalize_player_name(mission_name):
            continue
        if _normalize_text(row.get('comment')) != _normalize_text(comment):
            continue
        if _normalize_match_result_type(row.get('result_type')) != _normalize_match_result_type(result_type):
            continue
        if _coerce_positive_int(row.get('league_id')) != _coerce_positive_int(league_id):
            continue
        return dict(row)

    return None


def _build_submit_result_from_existing_match(
    match_row: dict,
    *,
    player1: dict,
    player2: dict,
) -> dict:
    match_id = int(match_row['id'])
    ranked_match = bool(match_row.get('is_ranked'))
    clean_result_type = _normalize_match_result_type(match_row.get('result_type'))
    history_rows = _fetch_all_rating_history_raw(force_refresh=True)
    history_by_pair = {(int(row['match_id']), int(row['player_id'])): row for row in history_rows}
    score_details = _extract_match_score_details(match_row)

    player1_history = history_by_pair.get((match_id, int(player1['id'])), {})
    player2_history = history_by_pair.get((match_id, int(player2['id'])), {})

    return {
        'match_id': match_id,
        'played_at_label': _format_match_datetime(match_row.get('played_at')),
        'result_type': clean_result_type,
        'is_tie': clean_result_type == 'draw',
        'winner_player_id': None if clean_result_type == 'draw' else player1['id'],
        'winner_name': player1['name'],
        'winner_created': bool(player1.get('created', False)),
        'winner_race': _normalize_race_label(match_row.get('player1_race')),
        'winner_profile_url': f"/players/{player1['id']}",
        'opponent_player_id': player2['id'],
        'opponent_name': player2['name'],
        'opponent_created': bool(player2.get('created', False)),
        'opponent_race': _normalize_race_label(match_row.get('player2_race')),
        'opponent_profile_url': f"/players/{player2['id']}",
        'is_ranked': ranked_match,
        'game_type': _normalize_text(match_row.get('game_type')),
        'mission_name': _normalize_player_name(match_row.get('mission_name')),
        'comment': score_details['visible_comment'],
        'player1_score': score_details['player1_score'],
        'player2_score': score_details['player2_score'],
        'score_display': _format_match_score(score_details['player1_score'], score_details['player2_score']),
        'player1_roster_id': score_details['player1_roster_id'],
        'player2_roster_id': score_details['player2_roster_id'],
        'winner_old_elo_display': _normalize_elo_value(player1_history.get('old_elo')),
        'winner_new_elo_display': _normalize_elo_value(player1_history.get('new_elo')),
        'winner_delta_display': _format_delta(player1_history.get('elo_delta')),
        'opponent_old_elo_display': _normalize_elo_value(player2_history.get('old_elo')),
        'opponent_new_elo_display': _normalize_elo_value(player2_history.get('new_elo')),
        'opponent_delta_display': _format_delta(player2_history.get('elo_delta')),
        'league_id': _coerce_positive_int(match_row.get('league_id')),
    }


def _format_seconds_as_wait_label(total_seconds: int) -> str:
    seconds = max(1, int(total_seconds))
    minutes, remaining_seconds = divmod(seconds, 60)
    hours, remaining_minutes = divmod(minutes, 60)

    parts: list[str] = []
    if hours > 0:
        parts.append(f"{hours}h")
    if remaining_minutes > 0:
        parts.append(f"{remaining_minutes}m")
    if not parts:
        parts.append(f"{remaining_seconds}s")
    return ' '.join(parts)


def _find_recent_match_for_any_player(
    *,
    player_ids: list[int],
    submitted_at: datetime,
    window_seconds: int,
) -> dict | None:
    unique_ids = sorted({int(value) for value in player_ids if value is not None})
    if not unique_ids or window_seconds <= 0:
        return None

    candidate_rows: dict[int, dict[str, Any]] = {}
    for field_name in ('player1_id', 'player2_id'):
        rows = _rest_select(
            'matches',
            select='id,played_at,player1_id,player2_id,winner_player_id,player1_race,player2_race,is_ranked,game_type,mission_name,comment,result_type',
            filters=[(field_name, 'in', unique_ids)],
            order='played_at.desc,id.desc',
            limit=50,
        )
        for row in rows:
            try:
                candidate_rows[int(row['id'])] = dict(row)
            except (KeyError, TypeError, ValueError):
                continue

    nearest_row = None
    nearest_delta = None
    for row in candidate_rows.values():
        played_at = _parse_datetime(row.get('played_at'))
        if not played_at:
            continue

        if getattr(played_at, 'tzinfo', None):
            played_at_naive = played_at.replace(tzinfo=None)
        else:
            played_at_naive = played_at

        delta_seconds = (submitted_at - played_at_naive).total_seconds()
        if delta_seconds < 0 or delta_seconds > window_seconds:
            continue

        if nearest_delta is None or delta_seconds < nearest_delta:
            nearest_delta = delta_seconds
            nearest_row = row

    return nearest_row


def _enforce_tts_player_submit_cooldown(
    *,
    player1: dict,
    player2: dict,
    submitted_at: datetime,
) -> None:
    if TTS_PLAYER_SUBMIT_COOLDOWN_SECONDS <= 0:
        return

    recent_match = _find_recent_match_for_any_player(
        player_ids=[int(player1['id']), int(player2['id'])],
        submitted_at=submitted_at,
        window_seconds=TTS_PLAYER_SUBMIT_COOLDOWN_SECONDS,
    )
    if not recent_match:
        return

    played_at = _parse_datetime(recent_match.get('played_at'))
    if not played_at:
        return

    if getattr(played_at, 'tzinfo', None):
        played_at_naive = played_at.replace(tzinfo=None)
    else:
        played_at_naive = played_at

    remaining_seconds = TTS_PLAYER_SUBMIT_COOLDOWN_SECONDS - int((submitted_at - played_at_naive).total_seconds())
    if remaining_seconds <= 0:
        return

    blocked_names: list[str] = []
    recent_player_ids = {int(recent_match.get('player1_id') or 0), int(recent_match.get('player2_id') or 0)}
    if int(player1['id']) in recent_player_ids:
        blocked_names.append(str(player1.get('name') or 'Player 1'))
    if int(player2['id']) in recent_player_ids and int(player2['id']) != int(player1['id']):
        blocked_names.append(str(player2.get('name') or 'Player 2'))

    if not blocked_names:
        blocked_names = [str(player1.get('name') or 'Player 1'), str(player2.get('name') or 'Player 2')]

    wait_label = _format_seconds_as_wait_label(remaining_seconds)
    raise MatchSubmissionRateLimitError(
        f"Submission cooldown is active for: {', '.join(blocked_names)}. Try again in {wait_label}."
    )


def submit_tts_match_result(
    *,
    winner_name: str,
    opponent_name: str,
    winner_race: str,
    opponent_race: str,
    result_type: str,
    is_ranked,
    game_type: str,
    mission_name: str,
    player1_score,
    player2_score,
    player1_roster_id: str = '',
    player2_roster_id: str = '',
    comment: str = '',
) -> dict:
    with _SUBMIT_MATCH_LOCK:
        clean_player1_name = _normalize_player_name(winner_name)
        clean_player2_name = _normalize_player_name(opponent_name)

        if not clean_player1_name:
            raise ValueError('Enter the first player name.')
        if not clean_player2_name:
            raise ValueError('Enter the second player name.')
        if _normalize_player_key(clean_player1_name) == _normalize_player_key(clean_player2_name):
            raise ValueError('Players must be different.')

        submitted_at = datetime.now()
        player1 = _get_or_create_player(clean_player1_name)
        player2 = _get_or_create_player(clean_player2_name)
        _enforce_tts_player_submit_cooldown(
            player1=player1,
            player2=player2,
            submitted_at=submitted_at,
        )

        return submit_match_result(
            winner_name=clean_player1_name,
            opponent_name=clean_player2_name,
            winner_race=winner_race,
            opponent_race=opponent_race,
            result_type=result_type,
            is_ranked=is_ranked,
            game_type=game_type,
            mission_name=mission_name,
            player1_score=player1_score,
            player2_score=player2_score,
            player1_roster_id=player1_roster_id,
            player2_roster_id=player2_roster_id,
            comment=comment,
        )


def submit_match_result(
    *,
    winner_name: str,
    opponent_name: str,
    winner_race: str,
    opponent_race: str,
    result_type: str,
    is_ranked,
    game_type: str,
    mission_name: str,
    player1_score,
    player2_score,
    player1_roster_id: str = '',
    player2_roster_id: str = '',
    comment: str,
) -> dict:
    with _SUBMIT_MATCH_LOCK:
        clean_player1_name = _normalize_player_name(winner_name)
        clean_player2_name = _normalize_player_name(opponent_name)
        clean_player1_race = _normalize_race_db_label(winner_race)
        clean_player2_race = _normalize_race_db_label(opponent_race)
        clean_game_type = _normalize_game_type_label(game_type)
        clean_mission_name = _normalize_player_name(mission_name)
        clean_comment = _normalize_text(comment)
        clean_player1_score = _coerce_match_score_value(player1_score, field_label='Player 1 score')
        clean_player2_score = _coerce_match_score_value(player2_score, field_label='Player 2 score')
        clean_player1_roster_id = _normalize_roster_id(player1_roster_id, field_label='Player 1 roster ID')
        clean_player2_roster_id = _normalize_roster_id(player2_roster_id, field_label='Player 2 roster ID')
        comment_payload = _build_match_comment_payload(
            clean_comment,
            clean_player1_score,
            clean_player2_score,
            clean_player1_roster_id,
            clean_player2_roster_id,
        )
        clean_result_type = _normalize_match_result_type(result_type)
        ranked_match = _coerce_ranked_value(is_ranked)

        if not clean_player1_name:
            raise ValueError('Enter the first player name.')
        if not clean_player2_name:
            raise ValueError('Enter the second player name.')
        if _normalize_player_key(clean_player1_name) == _normalize_player_key(clean_player2_name):
            raise ValueError('Players must be different.')
        if clean_player1_race not in RACE_OPTIONS:
            raise ValueError('Choose the first player race.')
        if clean_player2_race not in RACE_OPTIONS:
            raise ValueError('Choose the second player race.')
        if clean_game_type not in GAME_TYPE_OPTIONS:
            raise ValueError('Choose the game type.')
        if not clean_mission_name:
            raise ValueError('Choose the mission.')
        if len(comment_payload or '') > 4000:
            raise ValueError('Comment is too long.')

        match_league_id = _resolve_match_league_id(ranked_match=ranked_match)

        submitted_at = datetime.now()
        played_at = submitted_at.isoformat()

        player1 = _get_or_create_player(clean_player1_name)
        player2 = _get_or_create_player(clean_player2_name)

        duplicate_match = _find_recent_duplicate_match(
            player1_id=int(player1['id']),
            player2_id=int(player2['id']),
            player1_race=clean_player1_race,
            player2_race=clean_player2_race,
            is_ranked=ranked_match,
            game_type=clean_game_type,
            mission_name=clean_mission_name,
            comment=comment_payload,
            result_type=clean_result_type,
            league_id=match_league_id,
            submitted_at=submitted_at,
        )
        if duplicate_match:
            duplicate_result = _build_submit_result_from_existing_match(
                duplicate_match,
                player1=player1,
                player2=player2,
            )
            if ranked_match and match_league_id:
                duplicate_result['awarded_badges'] = _sync_league_badges_after_ranked_match(
                    league_id=match_league_id,
                    match_id=duplicate_match.get('id'),
                )
            return duplicate_result

        player1_old_elo = int(player1.get('current_elo') or 1000)
        player2_old_elo = int(player2.get('current_elo') or 1000)

        player1_matches_before_match = int(player1.get('matches_count') or 0)
        player2_matches_before_match = int(player2.get('matches_count') or 0)
        player1_ranked_matches_before_match = _count_player_ranked_matches_before_submit(int(player1['id']))
        player2_ranked_matches_before_match = _count_player_ranked_matches_before_submit(int(player2['id']))
        elo_multiplier = _resolve_ranked_elo_multiplier(clean_game_type)

        if ranked_match:
            if clean_result_type == 'draw':
                elo_result = _calculate_draw_elo_result(
                    player1_old_elo,
                    player2_old_elo,
                    player1_ranked_matches_before_match,
                    player2_ranked_matches_before_match,
                    elo_multiplier,
                )
            else:
                win_elo_result = _calculate_elo_result(
                    player1_old_elo,
                    player2_old_elo,
                    player1_ranked_matches_before_match,
                    player2_ranked_matches_before_match,
                    elo_multiplier,
                )
                win_elo_result = _apply_winner_seed_bonus_to_win_elo_result(
                    win_elo_result,
                    winner_old_elo=player1_old_elo,
                    loser_old_elo=player2_old_elo,
                    apply_bonus=False,
                )
                elo_result = {
                    'player1_old_elo': win_elo_result['winner_old_elo'],
                    'player1_new_elo': win_elo_result['winner_new_elo'],
                    'player1_delta': win_elo_result['winner_delta'],
                    'player1_expected_score': win_elo_result['winner_expected_score'],
                    'player1_actual_score': 1.0,
                    'player1_k_factor': win_elo_result['winner_k_factor'],
                    'player2_old_elo': win_elo_result['loser_old_elo'],
                    'player2_new_elo': win_elo_result['loser_new_elo'],
                    'player2_delta': win_elo_result['loser_delta'],
                    'player2_expected_score': win_elo_result['loser_expected_score'],
                    'player2_actual_score': 0.0,
                    'player2_k_factor': win_elo_result['loser_k_factor'],
                    'k_factor': win_elo_result['k_factor'],
                    'winner_seed_bonus_applied': win_elo_result['winner_seed_bonus_applied'],
                    'winner_seed_bonus_multiplier': win_elo_result['winner_seed_bonus_multiplier'],
                }
            player1_new_elo = elo_result['player1_new_elo']
            player2_new_elo = elo_result['player2_new_elo']
        else:
            elo_result = {
                'player1_old_elo': player1_old_elo,
                'player1_new_elo': player1_old_elo,
                'player1_delta': 0,
                'player1_expected_score': 0,
                'player1_actual_score': 0.5 if clean_result_type == 'draw' else 1.0,
                'player1_k_factor': 0,
                'player2_old_elo': player2_old_elo,
                'player2_new_elo': player2_old_elo,
                'player2_delta': 0,
                'player2_expected_score': 0,
                'player2_actual_score': 0.5 if clean_result_type == 'draw' else 0.0,
                'player2_k_factor': 0,
                'k_factor': 0,
                'winner_seed_bonus_applied': False,
                'winner_seed_bonus_multiplier': 1.0,
            }
            player1_new_elo = player1_old_elo
            player2_new_elo = player2_old_elo

        match_payload = {
            'player1_id': player1['id'],
            'player2_id': player2['id'],
            'played_at': played_at,
            'comment': comment_payload,
            'player1_race': clean_player1_race,
            'player2_race': clean_player2_race,
            'is_ranked': ranked_match,
            'game_type': clean_game_type,
            'mission_name': clean_mission_name,
            'player1_roster_id': clean_player1_roster_id or None,
            'player2_roster_id': clean_player2_roster_id or None,
            'result_type': clean_result_type,
            'winner_player_id': None if clean_result_type == 'draw' else player1['id'],
            'league_id': match_league_id,
        }

        match_rows = _rest_insert('matches', match_payload)
        match_row = match_rows[0] if isinstance(match_rows, list) else match_rows

        player1_matches_after_match = player1_matches_before_match + 1
        player2_matches_after_match = player2_matches_before_match + 1

        player1_updates = {
            'current_elo': player1_new_elo,
            'matches_count': player1_matches_after_match,
            'last_match_at': played_at,
            'is_active': _is_player_active_by_last_match(played_at),
            'updated_at': datetime.utcnow().isoformat(),
        }
        player2_updates = {
            'current_elo': player2_new_elo,
            'matches_count': player2_matches_after_match,
            'last_match_at': played_at,
            'is_active': _is_player_active_by_last_match(played_at),
            'updated_at': datetime.utcnow().isoformat(),
        }

        if clean_result_type == 'draw':
            player1_updates['draws'] = int(player1.get('draws') or 0) + 1
            player2_updates['draws'] = int(player2.get('draws') or 0) + 1
        else:
            player1_updates['wins'] = int(player1.get('wins') or 0) + 1
            player2_updates['losses'] = int(player2.get('losses') or 0) + 1

        _rest_update('players', player1_updates, filters=[('id', 'eq', player1['id'])])
        _rest_update('players', player2_updates, filters=[('id', 'eq', player2['id'])])

        if ranked_match:
            _rest_insert(
                'rating_history',
                [
                    {
                        'match_id': match_row['id'],
                        'player_id': player1['id'],
                        'old_elo': elo_result['player1_old_elo'],
                        'new_elo': elo_result['player1_new_elo'],
                        'elo_delta': elo_result['player1_delta'],
                        'expected_score': elo_result['player1_expected_score'],
                        'actual_score': elo_result['player1_actual_score'],
                        'k_factor': elo_result['player1_k_factor'],
                    },
                    {
                        'match_id': match_row['id'],
                        'player_id': player2['id'],
                        'old_elo': elo_result['player2_old_elo'],
                        'new_elo': elo_result['player2_new_elo'],
                        'elo_delta': elo_result['player2_delta'],
                        'expected_score': elo_result['player2_expected_score'],
                        'actual_score': elo_result['player2_actual_score'],
                        'k_factor': elo_result['player2_k_factor'],
                    },
                ],
            )

        invalidate_application_cache()
        awarded_badges = []
        if ranked_match and match_league_id:
            awarded_badges = _sync_league_badges_after_ranked_match(
                league_id=match_league_id,
                match_id=match_row.get('id'),
            )

        return {
            'match_id': match_row['id'],
            'played_at_label': _format_match_datetime(match_row.get('played_at') or played_at),
            'result_type': clean_result_type,
            'is_tie': clean_result_type == 'draw',
            'winner_player_id': None if clean_result_type == 'draw' else player1['id'],
            'winner_name': player1['name'],
            'winner_created': player1.get('created', False),
            'winner_race': _normalize_race_label(clean_player1_race),
            'winner_profile_url': f"/players/{player1['id']}",
            'opponent_player_id': player2['id'],
            'opponent_name': player2['name'],
            'opponent_created': player2.get('created', False),
            'opponent_race': _normalize_race_label(clean_player2_race),
            'opponent_profile_url': f"/players/{player2['id']}",
            'is_ranked': ranked_match,
            'game_type': clean_game_type,
            'mission_name': clean_mission_name,
            'comment': clean_comment,
            'player1_score': clean_player1_score,
            'player2_score': clean_player2_score,
            'score_display': _format_match_score(clean_player1_score, clean_player2_score),
            'player1_roster_id': clean_player1_roster_id,
            'player2_roster_id': clean_player2_roster_id,
            'winner_old_elo_display': _normalize_elo_value(elo_result['player1_old_elo']),
            'winner_new_elo_display': _normalize_elo_value(elo_result['player1_new_elo']),
            'winner_delta_display': _format_delta(elo_result['player1_delta']),
            'opponent_old_elo_display': _normalize_elo_value(elo_result['player2_old_elo']),
            'opponent_new_elo_display': _normalize_elo_value(elo_result['player2_new_elo']),
            'opponent_delta_display': _format_delta(elo_result['player2_delta']),
            'league_id': match_league_id,
            'awarded_badges': awarded_badges,
        }



def _feedback_storage_error(exc: Exception) -> Exception:
    message = str(exc)
    lowered = message.lower()
    if FEEDBACK_TABLE_NAME in lowered and (
        'does not exist' in lowered
        or 'schema cache' in lowered
        or 'could not find the table' in lowered
    ):
        return RuntimeError(
            'Feedback storage is not ready yet. Create the public.admin_feedback_messages table first.'
        )
    return exc


def _prepare_feedback_message_row(row: dict) -> dict:
    item = dict(row)
    item['player_name'] = _normalize_player_name(item.get('player_name'))
    item['message_text'] = _normalize_text(item.get('message_text'))
    item['created_at_label'] = _format_match_datetime(item.get('created_at'))
    return item


def fetch_admin_feedback_messages(limit: int = 200) -> list[dict]:
    clean_limit = max(1, min(int(limit or 200), 500))
    try:
        rows = _rest_select(
            FEEDBACK_TABLE_NAME,
            order='created_at.desc,id.desc',
            limit=clean_limit,
        )
    except Exception as exc:
        raise _feedback_storage_error(exc) from None
    return [_prepare_feedback_message_row(row) for row in rows]




def delete_admin_feedback_message(message_id: int) -> None:
    try:
        clean_message_id = int(message_id)
    except (TypeError, ValueError):
        raise ValueError('Invalid message id.')

    existing = _rest_select(
        FEEDBACK_TABLE_NAME,
        select='id',
        filters=[('id', 'eq', clean_message_id)],
        single=True,
    )
    if not existing:
        raise ValueError('Message not found.')

    try:
        _rest_delete(
            FEEDBACK_TABLE_NAME,
            filters=[('id', 'eq', clean_message_id)],
        )
    except Exception as exc:
        raise _feedback_storage_error(exc) from None

def submit_admin_feedback_message(
    *,
    player_name: str,
    message_text: str,
    ip_address: str | None = None,
) -> dict:
    clean_player_name = _normalize_player_name(player_name)
    clean_player_name_key = _normalize_player_key(clean_player_name)
    clean_message_text = _normalize_text(message_text)
    clean_ip = _normalize_text(ip_address)

    if not clean_player_name:
        raise ValueError('Enter your player name.')
    if len(clean_player_name) > FEEDBACK_PLAYER_NAME_MAX_LENGTH:
        raise ValueError(f'Player name must be at most {FEEDBACK_PLAYER_NAME_MAX_LENGTH} characters long.')
    if len(clean_message_text) < 3:
        raise ValueError('Message must contain at least 3 characters.')
    if len(clean_message_text) > FEEDBACK_MESSAGE_MAX_LENGTH:
        raise ValueError(f'Message must be at most {FEEDBACK_MESSAGE_MAX_LENGTH} characters long.')

    payload = {
        'player_name': clean_player_name,
        'player_name_normalized': clean_player_name_key,
        'message_text': clean_message_text,
        'created_at': datetime.utcnow().isoformat(),
        'ip_address': clean_ip or None,
    }

    try:
        rows = _rest_insert(FEEDBACK_TABLE_NAME, payload)
    except Exception as exc:
        raise _feedback_storage_error(exc) from None

    if isinstance(rows, list) and rows:
        return _prepare_feedback_message_row(rows[0])
    return _prepare_feedback_message_row(payload)

def fetch_player_admin(player_id: int) -> dict | None:
    row = _rest_get_player_by_id(player_id)
    if not row:
        return None

    player = _prepare_player_row(row)
    player['created_at_label'] = _format_match_datetime(player.get('created_at'))
    player['current_elo_input'] = int(row.get('current_elo') or 1000)
    player['is_active'] = _is_player_active_by_last_match(row.get('last_match_at'))
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

    if not clean_name:
        raise ValueError('Enter the player name.')

    try:
        clean_current_elo = max(0, int(current_elo))
    except (TypeError, ValueError):
        raise ValueError('ELO must be a whole number.')

    existing = _rest_get_player_by_name_key(clean_key)
    if existing and int(existing['id']) != int(player_id):
        raise ValueError('Another player already has this name.')

    current_player = _rest_get_player_by_id(player_id)
    if not current_player:
        raise ValueError('Player not found.')

    rows = _rest_update(
        'players',
        {
            'name': clean_name,
            'name_normalized': clean_key,
            'current_elo': clean_current_elo,
            'country_code': clean_country_code or None,
            'discord_url': clean_discord_url or None,
            'is_active': _is_player_active_by_last_match(current_player.get('last_match_at')),
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

    players_by_id = _fetch_players_by_ids([int(row['player1_id']), int(row['player2_id'])])
    player1 = players_by_id.get(int(row['player1_id']))
    player2 = players_by_id.get(int(row['player2_id']))
    if not player1 or not player2:
        return None

    match = dict(row)
    match['player1_name'] = _normalize_player_name(player1.get('name'))
    match['player2_name'] = _normalize_player_name(player2.get('name'))
    match['result_type'] = _normalize_match_result_type(match.get('result_type'))
    if match['result_type'] == 'draw':
        match['winner_side'] = 'tie'
    else:
        match['winner_side'] = 'player1' if int(row['winner_player_id']) == int(row['player1_id']) else 'player2'
    match['player1_race'] = _normalize_race_label(match.get('player1_race'))
    match['player2_race'] = _normalize_race_label(match.get('player2_race'))
    match['game_type'] = _normalize_text(match.get('game_type'))
    match['mission_name'] = _normalize_text(match.get('mission_name'))
    score_details = _extract_match_score_details(match)
    match['player1_score'] = score_details['player1_score']
    match['player2_score'] = score_details['player2_score']
    match['score_display'] = _format_match_score(score_details['player1_score'], score_details['player2_score'])
    match['player1_roster_id'] = score_details['player1_roster_id']
    match['player2_roster_id'] = score_details['player2_roster_id']
    match['comment'] = score_details['visible_comment']
    match['played_at_label'] = _format_match_datetime(match.get('played_at'))
    parsed = _parse_datetime(match.get('played_at'))
    match['played_at_input'] = parsed.strftime('%Y-%m-%dT%H:%M') if parsed else ''
    match['league_id'] = _coerce_positive_int(match.get('league_id'))
    match['league_name'] = ''
    if match['league_id']:
        league = _prepare_league_row(_rest_select(LEAGUE_TABLE_NAME, filters=[('id', 'eq', match['league_id'])], single=True))
        match['league_name'] = league.get('name') if league else ''
    return match


def _rebuild_ratings_and_player_stats() -> None:
    players = _fetch_all_players_raw(force_refresh=True)
    matches = _fetch_all_matches_raw(force_refresh=True)

    _rest_delete('rating_history', filters=[('id', 'gt', 0)])

    player_state: dict[int, dict] = {
        int(player['id']): {
            'elo': 1000,
            'matches_count': 0,
            'ranked_matches_count': 0,
            'wins': 0,
            'losses': 0,
            'draws': 0,
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
        played_at = match.get('played_at')
        ranked_match = bool(match.get('is_ranked'))
        result_type = _normalize_match_result_type(match.get('result_type'))

        player1_state = player_state.setdefault(player1_id, {'elo': 1000, 'matches_count': 0, 'ranked_matches_count': 0, 'wins': 0, 'losses': 0, 'draws': 0, 'last_match_at': None})
        player2_state = player_state.setdefault(player2_id, {'elo': 1000, 'matches_count': 0, 'ranked_matches_count': 0, 'wins': 0, 'losses': 0, 'draws': 0, 'last_match_at': None})

        player1_old_elo = int(player1_state['elo'])
        player2_old_elo = int(player2_state['elo'])

        player1_ranked_matches_before_match = int(player1_state.get('ranked_matches_count') or 0)
        player2_ranked_matches_before_match = int(player2_state.get('ranked_matches_count') or 0)

        elo_multiplier = _resolve_ranked_elo_multiplier(match.get('game_type'))

        if result_type == 'draw':
            if ranked_match:
                elo_result = _calculate_draw_elo_result(
                    player1_old_elo,
                    player2_old_elo,
                    player1_ranked_matches_before_match,
                    player2_ranked_matches_before_match,
                    elo_multiplier,
                )
                player1_new_elo = elo_result['player1_new_elo']
                player2_new_elo = elo_result['player2_new_elo']
                rating_rows.append(
                    {
                        'match_id': match_id,
                        'player_id': player1_id,
                        'old_elo': elo_result['player1_old_elo'],
                        'new_elo': elo_result['player1_new_elo'],
                        'elo_delta': elo_result['player1_delta'],
                        'expected_score': elo_result['player1_expected_score'],
                        'actual_score': elo_result['player1_actual_score'],
                        'k_factor': elo_result['player1_k_factor'],
                    }
                )
                rating_rows.append(
                    {
                        'match_id': match_id,
                        'player_id': player2_id,
                        'old_elo': elo_result['player2_old_elo'],
                        'new_elo': elo_result['player2_new_elo'],
                        'elo_delta': elo_result['player2_delta'],
                        'expected_score': elo_result['player2_expected_score'],
                        'actual_score': elo_result['player2_actual_score'],
                        'k_factor': elo_result['player2_k_factor'],
                    }
                )
            else:
                player1_new_elo = player1_old_elo
                player2_new_elo = player2_old_elo

            player1_state['elo'] = player1_new_elo
            player1_state['matches_count'] += 1
            if ranked_match:
                player1_state['ranked_matches_count'] = int(player1_state.get('ranked_matches_count') or 0) + 1
            player1_state['draws'] += 1
            player1_state['last_match_at'] = played_at

            player2_state['elo'] = player2_new_elo
            player2_state['matches_count'] += 1
            if ranked_match:
                player2_state['ranked_matches_count'] = int(player2_state.get('ranked_matches_count') or 0) + 1
            player2_state['draws'] += 1
            player2_state['last_match_at'] = played_at

            touched_players.add(player1_id)
            touched_players.add(player2_id)
            continue

        winner_id = int(match['winner_player_id'])
        if winner_id not in {player1_id, player2_id}:
            raise ValueError(f'Match {match_id} has invalid winner_player_id.')

        loser_id = player2_id if winner_id == player1_id else player1_id
        winner_state = player1_state if winner_id == player1_id else player2_state
        loser_state = player2_state if loser_id == player2_id else player1_state

        winner_old_elo = int(winner_state['elo'])
        loser_old_elo = int(loser_state['elo'])

        winner_ranked_matches_before_match = int(winner_state.get('ranked_matches_count') or 0)
        loser_ranked_matches_before_match = int(loser_state.get('ranked_matches_count') or 0)

        if ranked_match:
            elo_result = _calculate_elo_result(
                winner_old_elo,
                loser_old_elo,
                winner_ranked_matches_before_match,
                loser_ranked_matches_before_match,
                elo_multiplier,
            )
            elo_result = _apply_winner_seed_bonus_to_win_elo_result(
                elo_result,
                winner_old_elo=winner_old_elo,
                loser_old_elo=loser_old_elo,
                apply_bonus=False,
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
        if ranked_match:
            winner_state['ranked_matches_count'] = int(winner_state.get('ranked_matches_count') or 0) + 1
        winner_state['wins'] += 1
        winner_state['last_match_at'] = played_at

        loser_state['elo'] = loser_new_elo
        loser_state['matches_count'] += 1
        if ranked_match:
            loser_state['ranked_matches_count'] = int(loser_state.get('ranked_matches_count') or 0) + 1
        loser_state['losses'] += 1
        loser_state['last_match_at'] = played_at

        touched_players.add(winner_id)
        touched_players.add(loser_id)

    if rating_rows:
        _rest_insert('rating_history', rating_rows)

    for player in players:
        player_id = int(player['id'])
        state = player_state.get(player_id, {'elo': 1000, 'matches_count': 0, 'ranked_matches_count': 0, 'wins': 0, 'losses': 0, 'draws': 0, 'last_match_at': None})
        _rest_update(
            'players',
            {
                'current_elo': int(state['elo']),
                'matches_count': int(state['matches_count']),
                'wins': int(state['wins']),
                'losses': int(state['losses']),
                'draws': int(state['draws']),
                'last_match_at': state['last_match_at'],
                'is_active': _is_player_active_by_last_match(state['last_match_at']),
                'updated_at': datetime.utcnow().isoformat(),
            },
            filters=[('id', 'eq', player_id)],
        )

    invalidate_application_cache()
    _sync_all_current_league_badges()
    invalidate_page_cache()


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
    player1_score,
    player2_score,
    player1_roster_id: str = '',
    player2_roster_id: str = '',
    comment: str,
    played_at: datetime,
) -> dict:
    clean_player1_name = _normalize_player_name(player1_name)
    clean_player2_name = _normalize_player_name(player2_name)
    clean_player1_race = _normalize_race_db_label(player1_race)
    clean_player2_race = _normalize_race_db_label(player2_race)
    ranked_match = _coerce_ranked_value(is_ranked)
    clean_game_type = _normalize_game_type_label(game_type)
    clean_mission_name = _normalize_player_name(mission_name)
    clean_comment = _normalize_text(comment)
    clean_player1_score = _coerce_match_score_value(player1_score, field_label='Player 1 score')
    clean_player2_score = _coerce_match_score_value(player2_score, field_label='Player 2 score')
    clean_player1_roster_id = _normalize_roster_id(player1_roster_id, field_label='Player 1 roster ID')
    clean_player2_roster_id = _normalize_roster_id(player2_roster_id, field_label='Player 2 roster ID')
    comment_payload = _build_match_comment_payload(
        clean_comment,
        clean_player1_score,
        clean_player2_score,
        clean_player1_roster_id,
        clean_player2_roster_id,
    )
    clean_winner_side = _normalize_text(winner_side)
    clean_result_type = 'draw' if clean_winner_side == 'tie' else 'win'

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
    if clean_winner_side not in {'player1', 'player2', 'tie'}:
        raise ValueError('Choose the match result.')
    if not isinstance(played_at, datetime):
        raise ValueError('Enter a valid played at date and time.')
    if len(comment_payload or '') > 4000:
        raise ValueError('Comment is too long.')

    existing = _rest_select('matches', filters=[('id', 'eq', match_id)], single=True)
    if not existing:
        raise ValueError('Match not found.')

    match_league_id = _resolve_match_league_id(
        ranked_match=ranked_match,
        existing_league_id=existing.get('league_id'),
    )

    player1 = _get_or_create_player(clean_player1_name)
    player2 = _get_or_create_player(clean_player2_name)
    winner_player_id = None if clean_result_type == 'draw' else (player1['id'] if clean_winner_side == 'player1' else player2['id'])

    rows = _rest_update(
        'matches',
        {
            'player1_id': player1['id'],
            'player2_id': player2['id'],
            'winner_player_id': winner_player_id,
            'result_type': clean_result_type,
            'played_at': played_at.isoformat(),
            'comment': comment_payload,
            'player1_race': clean_player1_race,
            'player2_race': clean_player2_race,
            'is_ranked': ranked_match,
            'game_type': clean_game_type,
            'mission_name': clean_mission_name,
            'player1_roster_id': clean_player1_roster_id or None,
            'player2_roster_id': clean_player2_roster_id or None,
            'league_id': match_league_id,
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
