from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

from app.modules.config import BASE_DIR


BACKGROUND_CONFIG_PATH = BASE_DIR / 'static' / 'art' / 'player-backgrounds.json'


def _clean_slug(value: Any) -> str:
    return str(value or '').strip().lower()


RACE_SLUG_ALIASES = {
    'terran': 'terran',
    'protoss': 'protoss',
    'zerg': 'zerg',
    '\u0442\u0435\u0440\u0440\u0430\u043d': 'terran',
    '\u043f\u0440\u043e\u0442\u043e\u0441\u0441': 'protoss',
    '\u0437\u0435\u0440\u0433': 'zerg',
}


def _clean_race_slug(value: Any) -> str:
    slug = _clean_slug(value)
    return RACE_SLUG_ALIASES.get(slug, slug)


@lru_cache(maxsize=1)
def _load_background_config() -> dict[str, Any]:
    try:
        raw_data = json.loads(BACKGROUND_CONFIG_PATH.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError):
        return {'backgrounds': {}, 'players': {}, 'race_defaults': {}, 'race_variants': {}}

    if not isinstance(raw_data, dict):
        return {'backgrounds': {}, 'players': {}, 'race_defaults': {}, 'race_variants': {}}

    backgrounds = raw_data.get('backgrounds')
    players = raw_data.get('players')
    race_defaults = raw_data.get('race_defaults')
    race_variants = raw_data.get('race_variants')
    return {
        'backgrounds': backgrounds if isinstance(backgrounds, dict) else {},
        'players': players if isinstance(players, dict) else {},
        'race_defaults': race_defaults if isinstance(race_defaults, dict) else {},
        'race_variants': race_variants if isinstance(race_variants, dict) else {},
    }


def _normalise_background(background_id: Any) -> dict[str, str] | None:
    if background_id in (None, ''):
        return None

    config = _load_background_config()
    key = str(background_id)
    raw_background = config['backgrounds'].get(key)
    if not isinstance(raw_background, dict):
        return None

    url = str(raw_background.get('url') or '').strip()
    if not url.startswith('/static/'):
        return None

    return {
        'id': key,
        'name': str(raw_background.get('name') or '').strip(),
        'race': _clean_slug(raw_background.get('race')),
        'url': url,
        'position': str(raw_background.get('position') or 'center center').strip(),
    }


def resolve_race_visual_background_id(
    race_slug: Any,
    *,
    player_id: Any = None,
    current_background_id: Any = None,
) -> str | None:
    clean_race_slug = _clean_race_slug(race_slug)
    if not clean_race_slug:
        return None

    current_background = _normalise_background(current_background_id)
    if current_background and current_background.get('race') == clean_race_slug:
        return current_background['id']

    config = _load_background_config()
    race_variants = config['race_variants'].get(clean_race_slug)
    if isinstance(race_variants, list) and race_variants:
        try:
            variant_index = int(player_id) % len(race_variants)
        except (TypeError, ValueError):
            variant_index = 0
        background = _normalise_background(race_variants[variant_index])
        if background and background.get('race') == clean_race_slug:
            return background['id']

    background = _normalise_background(config['race_defaults'].get(clean_race_slug))
    if background and background.get('race') == clean_race_slug:
        return background['id']

    return None


def resolve_player_visual_background(player: dict[str, Any] | None) -> dict[str, str] | None:
    if not player:
        return None

    config = _load_background_config()
    player_id = str(player.get('id') or '').strip()
    background_id = player.get('background_id') or player.get('profile_background_id')
    race_slug = _clean_race_slug(player.get('priority_race_slug') or player.get('priority_race'))

    if background_id in (None, '') and player_id:
        background_id = config['players'].get(player_id)

    background = _normalise_background(background_id)
    if background and (not race_slug or background.get('race') == race_slug):
        return background

    background_id = resolve_race_visual_background_id(race_slug, player_id=player_id)
    return _normalise_background(background_id)
