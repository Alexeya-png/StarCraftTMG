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
USER_COOKIE_NAME = 'starcraft_user_session'
GOOGLE_OAUTH_STATE_COOKIE_NAME = 'starcraft_google_oauth_state'
ADMIN_SESSION_HOURS = 12
USER_SESSION_DAYS = int(os.getenv('USER_SESSION_DAYS', '30') or '30')
SUBMIT_NAME_SUGGESTION_LIMIT = int(os.getenv('SUBMIT_NAME_SUGGESTION_LIMIT', '200') or '200')
FEEDBACK_MESSAGE_MAX_LENGTH = 300

CACHE_WARMUP_ON_STARTUP = (os.getenv('APP_WARMUP_ON_STARTUP') or '0').strip().lower() not in {'0', 'false', 'no', 'off'}
CACHE_REFRESH_BACKGROUND = (os.getenv('APP_CACHE_REFRESH_BACKGROUND') or '1').strip().lower() not in {'0', 'false', 'no', 'off'}
APP_DISPLAY_VERSION = (os.getenv('APP_DISPLAY_VERSION') or 'v2').strip() or 'v2'
SUPPORTER_WEBHOOK_BUILD = 'supporter-webhook-2026-06-10-6'
LIVE_API_RATE_LIMIT_PER_MINUTE = max(0, int(os.getenv('APP_LIVE_API_RATE_LIMIT_PER_MINUTE', '600') or '600'))
MATCH_SUBMIT_RATE_LIMIT_PER_HOUR = max(0, int(os.getenv('APP_MATCH_SUBMIT_RATE_LIMIT_PER_HOUR', '20') or '20'))
TTS_SUBMIT_RATE_LIMIT_PER_HOUR = max(0, int(os.getenv('APP_TTS_SUBMIT_RATE_LIMIT_PER_HOUR', '120') or '120'))
FEEDBACK_RATE_LIMIT_PER_HOUR = max(0, int(os.getenv('APP_FEEDBACK_RATE_LIMIT_PER_HOUR', '10') or '10'))
SUPPORT_CHECKOUT_RATE_LIMIT_PER_MINUTE = max(
    0,
    int(os.getenv('APP_SUPPORT_CHECKOUT_RATE_LIMIT_PER_MINUTE', '20') or '20'),
)
WEBHOOK_RATE_LIMIT_PER_MINUTE = max(0, int(os.getenv('APP_WEBHOOK_RATE_LIMIT_PER_MINUTE', '120') or '120'))
AUTH_RATE_LIMIT_PER_TEN_MINUTES = max(0, int(os.getenv('APP_AUTH_RATE_LIMIT_PER_TEN_MINUTES', '30') or '30'))
PROTECTED_WRITE_RATE_LIMIT_PER_FIVE_MINUTES = max(
    0,
    int(os.getenv('APP_PROTECTED_WRITE_RATE_LIMIT_PER_FIVE_MINUTES', '60') or '60'),
)


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
