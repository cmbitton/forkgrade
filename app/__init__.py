import logging
import time

from flask import Flask, g, has_request_context, render_template, request
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

    # -- Per-request performance logging --
    @app.before_request
    def _req_start():
        g._req_start = time.monotonic()
        g._db_queries = 0
        g._db_time = 0.0

    @app.after_request
    def _req_end(response):
        elapsed_ms = (time.monotonic() - g._req_start) * 1000
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

    @app.template_filter('region_display')
    def region_display_filter(region):
        return get_region_display(region)

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
    @app.errorhandler(404)
    def not_found(e):
        return render_template(
            '404.html',
            title='Page Not Found | ' + app.config['SITE_NAME'],
            description='The page you are looking for could not be found.',
            canonical_url=app.config['BASE_URL'] + '/'
        ), 404

    @app.errorhandler(500)
    def server_error(e):
        return render_template(
            '500.html',
            title='Server Error | ' + app.config['SITE_NAME'],
            description='An internal server error occurred.',
            canonical_url=app.config['BASE_URL'] + '/'
        ), 500

    return app
