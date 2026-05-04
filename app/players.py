from __future__ import annotations

import json
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parent
PLAYERS_JSON_PATH = BASE_DIR / 'players.json'


def _normalize_player_name(value: str | None) -> str:
    if value is None:
        return ''
    return ' '.join(str(value).split()).strip()


def _normalize_player_key(value: str | None) -> str:
    clean_value = _normalize_player_name(value)
    if not clean_value:
        return ''
    return clean_value.casefold()


def _load_players_alias_file() -> Any:
    if not PLAYERS_JSON_PATH.exists():
        return []

    try:
        return json.loads(PLAYERS_JSON_PATH.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError):
        return []


def _iter_alias_groups(raw_data: Any) -> list[list[str]]:
    groups: list[list[str]] = []

    if isinstance(raw_data, dict):
        for canonical_name, aliases in raw_data.items():
            names = [_normalize_player_name(canonical_name)]
            if isinstance(aliases, list):
                names.extend(_normalize_player_name(alias) for alias in aliases)
            clean_names = [name for name in names if name]
            if clean_names:
                groups.append(clean_names)
        return groups

    if not isinstance(raw_data, list):
        return groups

    for item in raw_data:
        if isinstance(item, dict):
            canonical_name = _normalize_player_name(item.get('name') or item.get('player') or item.get('canonical'))
            aliases = item.get('aliases') or item.get('names') or []
            names = [canonical_name]
            if isinstance(aliases, list):
                names.extend(_normalize_player_name(alias) for alias in aliases)
            clean_names = [name for name in names if name]
            if clean_names:
                groups.append(clean_names)
            continue

        if isinstance(item, list):
            clean_names = [_normalize_player_name(name) for name in item]
            clean_names = [name for name in clean_names if name]
            if clean_names:
                groups.append(clean_names)

    return groups


def _build_alias_map() -> dict[str, str]:
    raw_data = _load_players_alias_file()
    alias_groups = _iter_alias_groups(raw_data)
    alias_map: dict[str, str] = {}

    for group in alias_groups:
        canonical_name = group[0]
        for name in group:
            normalized_key = _normalize_player_key(name)
            if normalized_key and normalized_key not in alias_map:
                alias_map[normalized_key] = canonical_name

    return alias_map


def resolve_player_canonical_name(player_name: str | None) -> str:
    clean_name = _normalize_player_name(player_name)
    if not clean_name:
        return ''

    alias_map = _build_alias_map()
    return alias_map.get(_normalize_player_key(clean_name), clean_name)
