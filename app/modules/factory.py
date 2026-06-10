from __future__ import annotations

from flask import Flask, request

from app.database import sync_application_cache_from_shared_snapshot
from .cache import warmup_cache_on_startup
from .config import (
    APP_DISPLAY_VERSION,
    ASSET_VERSION,
    AUTH_RATE_LIMIT_PER_TEN_MINUTES,
    BASE_DIR,
    FEEDBACK_RATE_LIMIT_PER_HOUR,
    LIVE_API_RATE_LIMIT_PER_MINUTE,
    MATCH_SUBMIT_RATE_LIMIT_PER_HOUR,
    PROTECTED_WRITE_RATE_LIMIT_PER_FIVE_MINUTES,
    SUPPORT_CHECKOUT_RATE_LIMIT_PER_MINUTE,
    TTS_SUBMIT_RATE_LIMIT_PER_HOUR,
    WEBHOOK_RATE_LIMIT_PER_MINUTE,
)
from .rate_limit import consume_rate_limit, rate_limit_response
from .routes import account, admin, api, interactions, payments, public, seo


def create_app() -> Flask:
    app = Flask(
        __name__,
        template_folder=str(BASE_DIR / 'templates'),
        static_folder=str(BASE_DIR / 'static'),
        static_url_path='/static',
    )
    app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 31536000
    app.config['MAX_CONTENT_LENGTH'] = 64 * 1024

    @app.before_request
    def sync_shared_application_cache() -> None:
        if request.path.startswith('/static'):
            return
        try:
            sync_application_cache_from_shared_snapshot()
        except Exception:
            app.logger.exception('Shared application cache sync failed')

    @app.before_request
    def apply_request_rate_limits():
        path = request.path
        method = request.method.upper()
        rule = None

        if method == 'GET' and path in {'/api/leaderboard', '/api/reports', '/api/leagues'}:
            rule = ('live_api', LIVE_API_RATE_LIMIT_PER_MINUTE, 60)
        elif method == 'POST' and path == '/submit':
            rule = ('submit_match', MATCH_SUBMIT_RATE_LIMIT_PER_HOUR, 3600)
        elif method == 'POST' and path == '/api/tts/submit-match':
            rule = ('tts_submit', TTS_SUBMIT_RATE_LIMIT_PER_HOUR, 3600)
        elif method == 'POST' and path == '/feedback':
            rule = ('feedback', FEEDBACK_RATE_LIMIT_PER_HOUR, 3600)
        elif method == 'POST' and path == '/support/checkout':
            rule = ('support_checkout', SUPPORT_CHECKOUT_RATE_LIMIT_PER_MINUTE, 60)
        elif method == 'POST' and path == '/api/supabase/cache-webhook':
            rule = ('cache_webhook', WEBHOOK_RATE_LIMIT_PER_MINUTE, 60)
        elif method == 'POST' and path == '/api/stripe/webhook':
            rule = ('stripe_webhook', WEBHOOK_RATE_LIMIT_PER_MINUTE, 60)
        elif path.startswith('/auth/google/'):
            rule = ('google_auth', AUTH_RATE_LIMIT_PER_TEN_MINUTES, 600)
        elif method == 'POST' and (path == '/account' or path.startswith('/admin')):
            rule = ('protected_write', PROTECTED_WRITE_RATE_LIMIT_PER_FIVE_MINUTES, 300)

        if not rule:
            return None

        allowed, retry_after = consume_rate_limit(
            rule[0],
            limit=rule[1],
            window_seconds=rule[2],
        )
        if allowed:
            return None
        return rate_limit_response(retry_after)

    @app.context_processor
    def inject_asset_version() -> dict:
        return {'app_display_version': APP_DISPLAY_VERSION, 'asset_version': ASSET_VERSION}

    @app.after_request
    def apply_fast_page_headers(response):
        if request.path.startswith('/static'):
            response.headers['Cache-Control'] = 'public, max-age=31536000, immutable'
            response.headers.pop('Pragma', None)
            response.headers.pop('Expires', None)
            return response

        if request.path.startswith('/api') or request.path.startswith('/admin'):
            response.headers['Cache-Control'] = 'no-store, max-age=0, must-revalidate'
        else:
            response.headers['Cache-Control'] = 'no-cache, max-age=0, must-revalidate'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
        response.headers['Vary'] = 'Cookie'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['X-Frame-Options'] = 'DENY'
        response.headers['Referrer-Policy'] = 'strict-origin-when-cross-origin'
        response.headers['Permissions-Policy'] = 'camera=(), microphone=(), geolocation=()'

        if request.path.startswith('/admin'):
            response.headers['X-Robots-Tag'] = 'noindex, nofollow, noarchive'
        elif request.path == '/health':
            response.headers['X-Robots-Tag'] = 'noindex, nofollow'

        return response

    app.register_blueprint(seo.bp)
    app.register_blueprint(public.bp)
    app.register_blueprint(account.bp)
    app.register_blueprint(api.bp)
    app.register_blueprint(interactions.bp)
    app.register_blueprint(payments.bp)
    app.register_blueprint(admin.bp)

    with app.app_context():
        warmup_cache_on_startup()

    return app
