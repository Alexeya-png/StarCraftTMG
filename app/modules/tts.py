from __future__ import annotations

import os

from flask import request


def get_tts_submit_token() -> str:
    return (os.getenv('TTS_SUBMIT_TOKEN') or '').strip()

def _coerce_tts_game_type(value: str | None) -> str:
    clean_value = str(value or '').strip()
    mapping = {
        '1k': '1к',
        '2k': '2к',
        '1к': '1к',
        '2к': '2к',
        'Grand Offensive': 'Grand Offensive',
    }
    return mapping.get(clean_value, clean_value)

def _parse_tts_request_payload() -> dict:
    payload = request.get_json(silent=True)
    if isinstance(payload, dict):
        return {str(key): value for key, value in payload.items()}
    return {key: value for key, value in request.form.items()}
