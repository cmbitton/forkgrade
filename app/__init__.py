import logging
import time

from flask import Flask, g, has_request_context, render_template, request
from flask_limiter import Limiter
from sqlalchemy import event
from sqlalchemy.engine import Engine
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    datefmt='%H:%M:%S',
)
_perf_log = logging.getLogger('forkgrade.perf')


# -- SQLAlchemy query timing (engine-level, fires for every cursor execute) --
@event.listens_for(Engine, 'before_cursor_execute')
def _before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    conn.info['_query_t'] = time.monotonic()


@event.listens_for(Engine, 'after_cursor_execute')
def _after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    elapsed = time.monotonic() - conn.info.pop('_query_t', time.monotonic())
    if has_request_context():
        g._db_queries = getattr(g, '_db_queries', 0) + 1
        g._db_time = getattr(g, '_db_time', 0.0) + elapsed


def create_app():
    app = Flask(__name__)
    app.config.from_object('app.config.Config')

    # Initialize extensions
    from app.db import db, cache
    db.init_app(app)
    cache.init_app(app)

    def _real_ip():
        return request.headers.get('X-Forwarded-For', request.remote_addr).split(',')[0].strip()

    # Rate limiting — 60 req/min per IP, in-memory (single machine)
    limiter = Limiter(
        key_func=_real_ip,
        storage_uri='memory://',
        default_limits=['60 per minute'],
    )
    limiter.init_app(app)

    _GOOD_BOTS = ('Googlebot', 'bingbot', 'Slurp', 'DuckDuckBot')

    @limiter.request_filter
    def _exempt_rate_limit():
        if request.path.startswith('/static/'):
            return True
        ua = request.headers.get('User-Agent', '')
        return any(bot in ua for bot in _GOOD_BOTS)

    # -- Per-request performance logging --
    @app.before_request
    def _req_start():
        g._req_start = time.monotonic()
        g._db_queries = 0
        g._db_time = 0.0

    @app.after_request
    def _req_end(response):
        start = getattr(g, '_req_start', None)
        if start is None:
            return response
        elapsed_ms = (time.monotonic() - start) * 1000
        db_ms = getattr(g, '_db_time', 0.0) * 1000
        n_queries = getattr(g, '_db_queries', 0)
        level = logging.WARNING if elapsed_ms > 500 else logging.INFO
        _perf_log.log(
            level,
            '%s %s %d | %.0fms total | %d quer%s %.0fms db',
            request.method,
            request.path,
            response.status_code,
            elapsed_ms,
            n_queries,
            'ies' if n_queries != 1 else 'y',
            db_ms,
        )
        return response

    # Register custom Jinja2 filters
    from app.utils import get_region_display
    import sys as _sys, os as _os
    _sys.path.insert(0, _os.path.join(_os.path.dirname(__file__), '..', 'scripts'))
    from fda_codes import code_short_title

    @app.template_filter('region_display')
    def region_display_filter(region):
        return get_region_display(region)

    @app.template_filter('fda_title')
    def fda_title_filter(code):
        return code_short_title(code)

    @app.template_filter('format_date')
    def format_date_filter(value):
        if value is None:
            return 'N/A'
        try:
            return value.strftime('%b %-d, %Y')
        except Exception:
            return str(value)

    # Register blueprints
    from app.routes.home import home_bp
    from app.routes.region import region_bp
    from app.routes.sitemap import sitemap_bp

    app.register_blueprint(home_bp)
    app.register_blueprint(region_bp)
    app.register_blueprint(sitemap_bp)

    # Error handlers
    @app.errorhandler(429)
    def rate_limited(e):
        return render_template(
            '429.html',
            title='Too Many Requests | ' + app.config['SITE_NAME'],
            description='You have sent too many requests. Please wait a moment and try again.',
            canonical_url=app.config['BASE_URL'] + '/',
        ), 429

    @app.errorhandler(404)
    def not_found(e):
        return render_template(
            '404.html',
            title='Page Not Found | ' + app.config['SITE_NAME'],
            description='The page you are looking for could not be found.',
            canonical_url=app.config['BASE_URL'] + '/'
        ), 404

    @app.errorhandler(410)
    def gone(e):
        return render_template(
            '410.html',
            title='Page Removed | ' + app.config['SITE_NAME'],
            description='This page has been permanently removed.',
            canonical_url=app.config['BASE_URL'] + '/'
        ), 410

    @app.errorhandler(500)
    def server_error(e):
        return render_template(
            '500.html',
            title='Server Error | ' + app.config['SITE_NAME'],
            description='An internal server error occurred.',
            canonical_url=app.config['BASE_URL'] + '/'
        ), 500

    return app
