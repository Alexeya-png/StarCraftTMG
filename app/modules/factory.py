from __future__ import annotations

from flask import Flask, request

from .cache import warmup_cache_on_startup
from .config import ASSET_VERSION, BASE_DIR
from .routes import admin, api, interactions, public, seo


def create_app() -> Flask:
    app = Flask(
        __name__,
        template_folder=str(BASE_DIR / 'templates'),
        static_folder=str(BASE_DIR / 'static'),
        static_url_path='/static',
    )
    app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 31536000

    @app.context_processor
    def inject_asset_version() -> dict:
        return {'asset_version': ASSET_VERSION}

    @app.after_request
    def apply_fast_page_headers(response):
        if request.path.startswith('/static'):
            response.headers['Cache-Control'] = 'public, max-age=31536000, immutable'
            response.headers.pop('Pragma', None)
            response.headers.pop('Expires', None)
            return response

        response.headers['Cache-Control'] = 'no-cache, max-age=0, must-revalidate'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
        response.headers['Vary'] = 'Cookie'

        if request.path.startswith('/admin'):
            response.headers['X-Robots-Tag'] = 'noindex, nofollow, noarchive'
        elif request.path == '/health':
            response.headers['X-Robots-Tag'] = 'noindex, nofollow'

        return response

    app.register_blueprint(seo.bp)
    app.register_blueprint(public.bp)
    app.register_blueprint(api.bp)
    app.register_blueprint(interactions.bp)
    app.register_blueprint(admin.bp)

    with app.app_context():
        warmup_cache_on_startup()

    return app
