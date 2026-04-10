import os


class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production')
    _db_url = os.environ.get('DATABASE_URL', 'sqlite:///health_inspections.db')
    # Fly.io sets DATABASE_URL with postgres:// prefix; SQLAlchemy requires postgresql://
    if _db_url.startswith('postgres://'):
        _db_url = _db_url.replace('postgres://', 'postgresql://', 1)
    SQLALCHEMY_DATABASE_URI = _db_url
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ENGINE_OPTIONS = {
        'pool_pre_ping': True,   # validate connection before use; handles stale connections
        'pool_size': 5,          # per worker; 2 workers × 5 = 10 persistent connections
        'max_overflow': 10,      # per worker; 2 workers × 15 max = 30 total (fits Postgres limits)
        'pool_timeout': 10,      # fail fast rather than stall a request for 30s
        'pool_recycle': 1800,    # recycle after 30 min, not 5 — pre_ping handles stale connections
        'connect_args': {'connect_timeout': 5},
    }
    CACHE_TYPE = 'SimpleCache'
    CACHE_DEFAULT_TIMEOUT = 3600
    SITE_NAME = os.environ.get('SITE_NAME', 'ForkGrade')
    BASE_URL = os.environ.get('BASE_URL', 'https://forkgrade.com')
