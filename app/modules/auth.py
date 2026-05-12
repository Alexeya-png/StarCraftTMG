from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
from datetime import datetime, timedelta, timezone

from flask import redirect, request

from .config import ADMIN_COOKIE_NAME, ADMIN_SESSION_HOURS


def get_admin_login() -> str:
    return (os.getenv('ADMIN_LOGIN') or os.getenv('ADMIN_USERNAME') or 'admin').strip() or 'admin'


def get_admin_password() -> str:
    return (os.getenv('ADMIN_PASSWORD') or 'admin').strip() or 'admin'


def get_admin_secret() -> str:
    secret = (
        os.getenv('ADMIN_SECRET')
        or os.getenv('SECRET_KEY')
        or os.getenv('password')
        or 'starcraft-local-admin-secret'
    )
    return secret.strip() or 'starcraft-local-admin-secret'


def _b64encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode('utf-8').rstrip('=')


def _b64decode(value: str) -> bytes:
    padding = '=' * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)


def build_admin_cookie(login: str) -> str:
    expires_at = int((datetime.now(timezone.utc) + timedelta(hours=ADMIN_SESSION_HOURS)).timestamp())
    payload = {'login': login, 'exp': expires_at}
    payload_bytes = json.dumps(payload, separators=(',', ':'), ensure_ascii=False).encode('utf-8')
    payload_part = _b64encode(payload_bytes)
    signature = hmac.new(get_admin_secret().encode('utf-8'), payload_part.encode('utf-8'), hashlib.sha256).hexdigest()
    return f'{payload_part}.{signature}'


def read_admin_cookie(token: str | None) -> dict | None:
    if not token or '.' not in token:
        return None

    payload_part, signature = token.rsplit('.', 1)
    expected_signature = hmac.new(
        get_admin_secret().encode('utf-8'),
        payload_part.encode('utf-8'),
        hashlib.sha256,
    ).hexdigest()

    if not hmac.compare_digest(signature, expected_signature):
        return None

    try:
        payload = json.loads(_b64decode(payload_part).decode('utf-8'))
    except Exception:
        return None

    exp = int(payload.get('exp') or 0)
    if exp <= int(datetime.now(timezone.utc).timestamp()):
        return None

    return payload


def is_admin() -> bool:
    payload = read_admin_cookie(request.cookies.get(ADMIN_COOKIE_NAME))
    if not payload:
        return False
    return payload.get('login') == get_admin_login()


def redirect_to_admin_login():
    return redirect('/admin', code=303)
