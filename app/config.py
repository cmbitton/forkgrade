import os


class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production')
    SQLALCHEMY_DATABASE_URI = os.environ.get(
        'DATABASE_URL',
        'sqlite:///health_inspections.db'
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    CACHE_TYPE = 'SimpleCache'
    CACHE_DEFAULT_TIMEOUT = 3600
    SITE_NAME = os.environ.get('SITE_NAME', 'ForkGrade')
    BASE_URL = os.environ.get('BASE_URL', 'https://forkgrade.com')
