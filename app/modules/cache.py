from __future__ import annotations

import hmac
import os
import threading
from datetime import datetime, timezone

from flask import current_app, request

from app.database import refresh_application_cache
from .config import CACHE_WARMUP_ON_STARTUP


def get_supabase_webhook_secret() -> str:
    return (os.getenv('SUPABASE_WEBHOOK_SECRET') or '').strip()


def is_valid_supabase_webhook_request() -> bool:
    secret = get_supabase_webhook_secret()
    if not secret:
        return False

    header_secret = (request.headers.get('X-Webhook-Secret') or '').strip()
    auth_header = (request.headers.get('Authorization') or '').strip()

    expected_bearer = f'Bearer {secret}'
    return hmac.compare_digest(header_secret, secret) or hmac.compare_digest(auth_header, expected_bearer)


def run_cache_refresh(reason: str = 'manual') -> dict:
    started_at = datetime.now(timezone.utc).isoformat()
    result = refresh_application_cache(force_refresh=True)
    result['reason'] = reason
    result['started_at'] = started_at
    result['finished_at'] = datetime.now(timezone.utc).isoformat()
    return result


def run_cache_refresh_background(reason: str) -> None:
    app = current_app._get_current_object()

    def worker() -> None:
        with app.app_context():
            try:
                run_cache_refresh(reason)
            except Exception:
                app.logger.exception('Cache refresh failed: %s', reason)

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()


def warmup_cache_on_startup() -> None:
    if not CACHE_WARMUP_ON_STARTUP:
        return
    run_cache_refresh_background('startup')
