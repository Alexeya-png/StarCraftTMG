from __future__ import annotations

import time
from functools import wraps
from typing import Any, Callable

from flask import request, Response, jsonify

# Simple in-memory rate limiter
_rate_limit_store: dict[str, list[float]] = {}
_lock_times: dict[str, float] = {}


def consume_rate_limit(
    key: str,
    limit: int,
    window_seconds: int,
) -> tuple[bool, int]:
    """
    Check if a request should be rate-limited.

    Args:
        key: Unique identifier for the rate limit rule
        limit: Maximum requests allowed
        window_seconds: Time window in seconds

    Returns:
        (allowed: bool, retry_after: int in seconds)
    """
    now = time.time()
    cutoff = now - window_seconds

    # Initialize if not present
    if key not in _rate_limit_store:
        _rate_limit_store[key] = []

    # Remove old timestamps outside the window
    _rate_limit_store[key] = [ts for ts in _rate_limit_store[key] if ts > cutoff]

    # Check if limit exceeded
    if len(_rate_limit_store[key]) >= limit:
        # Calculate retry_after as the time until the oldest request expires
        oldest_ts = _rate_limit_store[key][0]
        retry_after = int((oldest_ts + window_seconds - now) + 1)
        return False, max(1, retry_after)

    # Record this request
    _rate_limit_store[key].append(now)
    return True, 0


def rate_limit_response(retry_after: int) -> Response:
    """Generate a 429 Too Many Requests response."""
    response = jsonify({'error': 'Rate limit exceeded'})
    response.status_code = 429
    response.headers['Retry-After'] = str(retry_after)
    return response

