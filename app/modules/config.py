from __future__ import annotations

import hashlib
import os
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parents[1]
PROJECT_ROOT = BASE_DIR.parent

load_dotenv(PROJECT_ROOT / '.env')

RACE_OPTIONS = [
    {'label': 'Терран', 'slug': 'terran'},
    {'label': 'Протосс', 'slug': 'protoss'},
    {'label': 'Зерг', 'slug': 'zerg'},
]
RACE_LABELS = [item['label'] for item in RACE_OPTIONS]
ADMIN_MATCH_RACE_OPTIONS = ['Terran', 'Protoss', 'Zerg']

GAME_TYPE_OPTIONS = ['1к', '2к', 'Grand Offensive']
DEFAULT_MISSION_OPTIONS = [
    'Divide and Conquer',
    'Frontlines',
    'Gather the Resources',
    'Hold Position',
    'Supply Drop',
    'Frontline',
    'Other / Custom',
]

ADMIN_COOKIE_NAME = 'starcraft_admin_session'
ADMIN_SESSION_HOURS = 12
SUBMIT_NAME_SUGGESTION_LIMIT = int(os.getenv('SUBMIT_NAME_SUGGESTION_LIMIT', '200') or '200')
FEEDBACK_MESSAGE_MAX_LENGTH = 300

CACHE_WARMUP_ON_STARTUP = (os.getenv('APP_WARMUP_ON_STARTUP') or '0').strip().lower() not in {'0', 'false', 'no', 'off'}
CACHE_REFRESH_BACKGROUND = (os.getenv('APP_CACHE_REFRESH_BACKGROUND') or '0').strip().lower() not in {'0', 'false', 'no', 'off'}
APP_DISPLAY_VERSION = (os.getenv('APP_DISPLAY_VERSION') or 'v2').strip() or 'v2'


def _build_asset_version() -> str:
    explicit_version = os.getenv('APP_VERSION', '').strip()
    if explicit_version:
        return explicit_version

    asset_files = [
        BASE_DIR / 'static' / 'styles.css',
        BASE_DIR / 'static' / 'favicon.png',
        BASE_DIR / 'static' / 'Race' / 'logo.png',
    ]
    version_parts: list[str] = []
    for path in asset_files:
        try:
            stat = path.stat()
            version_parts.append(f"{path.name}:{int(stat.st_mtime)}:{stat.st_size}")
        except OSError:
            continue

    if not version_parts:
        return '1'

    digest = hashlib.sha1('|'.join(version_parts).encode('utf-8')).hexdigest()
    return digest[:12]


ASSET_VERSION = _build_asset_version()
