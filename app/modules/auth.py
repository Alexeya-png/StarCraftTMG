from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
from datetime import datetime, timedelta, timezone

from flask import redirect, request

from .config import ADMIN_COOKIE_NAME, ADMIN_SESSION_HOURS, USER_COOKIE_NAME, USER_SESSION_DAYS


def get_admin_login() -> str:
    return (os.getenv('ADMIN_LOGIN') or os.getenv('ADMIN_USERNAME') or 'admin').strip() or 'admin'


def get_admin_password() -> str:
    return (os.getenv('ADMIN_PASSWORD') or 'admin').strip() or 'admin'


def get_admin_email() -> str:
    return (os.getenv('ADMIN_GOOGLE_EMAIL') or 'aradionov246@gmail.com').strip().lower() or 'aradionov246@gmail.com'


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


def _sign_payload(payload: dict, *, purpose: str) -> str:
    payload_bytes = json.dumps(payload, separators=(',', ':'), ensure_ascii=False).encode('utf-8')
    payload_part = _b64encode(payload_bytes)
    signing_key = f'{purpose}:{get_admin_secret()}'.encode('utf-8')
    signature = hmac.new(signing_key, payload_part.encode('utf-8'), hashlib.sha256).hexdigest()
    return f'{payload_part}.{signature}'


def _read_signed_payload(token: str | None, *, purpose: str) -> dict | None:
    if not token or '.' not in token:
        return None

    payload_part, signature = token.rsplit('.', 1)
    signing_key = f'{purpose}:{get_admin_secret()}'.encode('utf-8')
    expected_signature = hmac.new(signing_key, payload_part.encode('utf-8'), hashlib.sha256).hexdigest()

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


def build_user_cookie(account: dict) -> str:
    expires_at = int((datetime.now(timezone.utc) + timedelta(days=USER_SESSION_DAYS)).timestamp())
    payload = {
        'account_id': int(account.get('id') or 0),
        'email': str(account.get('email') or ''),
        'display_name': str(account.get('display_name') or account.get('email') or ''),
        'player_id': int(account.get('player_id') or 0),
        'exp': expires_at,
    }
    return _sign_payload(payload, purpose='user-session')


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


def read_user_cookie(token: str | None = None) -> dict | None:
    return _read_signed_payload(token if token is not None else request.cookies.get(USER_COOKIE_NAME), purpose='user-session')


def is_admin() -> bool:
    user = current_user_session()
    if not user:
        return False
    return str(user.get('email') or '').strip().lower() == get_admin_email()


def current_user_session() -> dict | None:
    payload = read_user_cookie()
    if not payload:
        return None
    if int(payload.get('account_id') or 0) <= 0:
        return None
    return payload


def require_user_session():
    if current_user_session():
        return None
    return redirect('/login', code=303)


def redirect_to_admin_login():
    if current_user_session():
        return redirect('/account', code=303)
    return redirect('/login', code=303)
