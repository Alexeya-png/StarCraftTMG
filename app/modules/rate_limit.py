from __future__ import annotations

import math
import threading
import time
from collections import deque

from flask import Response, jsonify, request

_lock = threading.Lock()
_buckets: dict[tuple[str, str], deque[float]] = {}
_MAX_BUCKETS = 10000


def client_ip_address() -> str:
    """Return the client's IP address, honoring X-Forwarded-For for proxied requests."""
    forwarded_for = request.headers.get('X-Forwarded-For', '')
    if forwarded_for:
        # The left-most entry is the originating client; later entries are proxies.
        first_hop = forwarded_for.split(',')[0].strip()
        if first_hop:
            return first_hop
    return request.remote_addr or 'unknown'


def _client_key() -> str:
    return request.remote_addr or 'unknown'


def _prune(now: float) -> None:
    # Drop empty or fully expired buckets to keep memory bounded.
    stale = [key for key, hits in _buckets.items() if not hits or hits[-1] <= now - 86400]
    for key in stale:
        _buckets.pop(key, None)


def consume_rate_limit(bucket: str, limit: int, window_seconds: int) -> tuple[bool, int]:
    """Record a hit for the current client in `bucket`.

    Returns (allowed, retry_after_seconds). A non-positive limit disables the rule.
    Uses an in-process sliding window, so limits apply per worker process.
    """
    if not limit or limit <= 0:
        return True, 0

    now = time.monotonic()
    key = (bucket, _client_key())
    cutoff = now - window_seconds

    with _lock:
        hits = _buckets.get(key)
        if hits is None:
            if len(_buckets) >= _MAX_BUCKETS:
                _prune(now)
            hits = _buckets[key] = deque()

        while hits and hits[0] <= cutoff:
            hits.popleft()

        if len(hits) >= limit:
            retry_after = max(1, int(math.ceil(hits[0] + window_seconds - now)))
            return False, retry_after

        hits.append(now)
        return True, 0


def rate_limit_response(retry_after: int):
    retry_after = max(1, int(retry_after or 1))
    message = 'Too many requests. Please try again later.'

    wants_json = request.path.startswith('/api/') or request.is_json
    if wants_json:
        response = jsonify({'ok': False, 'message': message, 'error': message, 'retry_after': retry_after})
    else:
        response = Response(message, mimetype='text/plain')

    response.status_code = 429
    response.headers['Retry-After'] = str(retry_after)
    return response
