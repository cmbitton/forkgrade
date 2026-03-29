import json
import urllib.request
from datetime import date, timedelta

from flask import Blueprint, render_template, request, current_app
from sqlalchemy import func, and_
from app.db import db, cache
from app.models.restaurant import Restaurant
from app.models.inspection import Inspection

home_bp = Blueprint('home', __name__)

_NON_RESTAURANT_TYPES = {'School / Childcare', 'Healthcare Facility', 'Grocery / Market', 'Catering'}

SUPPORTED_REGIONS = [
    {'slug': 'nyc',          'display': 'NYC'},
    {'slug': 'rhode-island', 'display': 'Rhode Island'},
]
_SUPPORTED_SLUGS  = {r['slug'] for r in SUPPORTED_REGIONS}
_DEFAULT_REGION   = 'nyc'

# ip-api.com regionName → our region slug
_GEO_REGION_MAP = {
    'New York':     'nyc',
    'Rhode Island': 'rhode-island',
}


def _client_ip():
    xff = request.headers.get('X-Forwarded-For', '')
    return xff.split(',')[0].strip() if xff else (request.remote_addr or '')


def _geolocate(ip):
    """Return a supported region slug for the given IP. Cached 1 h per IP.
    Falls back to _DEFAULT_REGION on any error or local address."""
    if not ip or ip.startswith(('127.', '10.', '192.168.', '::1')):
        return _DEFAULT_REGION
    cache_key = f'geo_{ip}'
    hit = cache.get(cache_key)
    if hit is not None:
        return hit
    try:
        url = f'http://ip-api.com/json/{ip}?fields=regionName'
        with urllib.request.urlopen(url, timeout=2) as resp:
            data = json.loads(resp.read())
        result = _GEO_REGION_MAP.get(data.get('regionName', ''), _DEFAULT_REGION)
    except Exception:
        result = _DEFAULT_REGION
    cache.set(cache_key, result, timeout=3600)
    return result


def _recent_inspections(limit=10, restaurants_only=False):
    q = db.session.query(Inspection, Restaurant).join(Restaurant)
    if restaurants_only:
        q = q.filter(
            Restaurant.cuisine_type.isnot(None),
            ~Restaurant.cuisine_type.in_(_NON_RESTAURANT_TYPES),
        )
    return q.order_by(Inspection.inspection_date.desc()).limit(limit).all()


def _worst_in_region(region, limit=10):
    """Lowest-scoring restaurants whose most recent inspection was in the past 30 days."""
    cache_key = f'worst_region_{region}'
    hit = cache.get(cache_key)
    if hit is not None:
        return hit

    cutoff = date.today() - timedelta(days=30)
    latest_sq = (
        db.session.query(
            Inspection.restaurant_id,
            func.max(Inspection.inspection_date).label('max_date'),
        )
        .group_by(Inspection.restaurant_id)
        .subquery()
    )
    rows = (
        db.session.query(Inspection, Restaurant)
        .join(Restaurant)
        .join(latest_sq, and_(
            latest_sq.c.restaurant_id == Inspection.restaurant_id,
            latest_sq.c.max_date == Inspection.inspection_date,
        ))
        .filter(
            Restaurant.region == region,
            Inspection.inspection_date >= cutoff,
            Inspection.score.isnot(None),
        )
        .order_by(Inspection.score.asc())
        .limit(limit)
        .all()
    )
    cache.set(cache_key, rows, timeout=300)
    return rows


@home_bp.route('/api/worst/<region>')
def api_worst(region):
    """Returns rendered rows for the worst-in-region list (used by the pill AJAX)."""
    if region not in _SUPPORTED_SLUGS:
        return '', 404
    rows = _worst_in_region(region)
    return render_template('components/_worst_rows.html', rows=rows)


@home_bp.route('/')
def index():
    q    = request.args.get('q', '').strip()
    feed = request.args.get('feed', 'restaurants')

    # Resolve which region's worst list to show:
    # 1. Explicit ?worst= param  2. Geolocation  3. Default
    worst_param = request.args.get('worst', '').strip().lower()
    if worst_param in _SUPPORTED_SLUGS:
        worst_region = worst_param
    else:
        worst_region = _geolocate(_client_ip())

    if q:
        search_results = Restaurant.query.filter(
            Restaurant.name.ilike(f'%{q}%'),
            Restaurant.inspections.any(),
        ).order_by(Restaurant.name).limit(20).all()

        return render_template(
            'home.html',
            title=f'Search results for "{q}" | {current_app.config["SITE_NAME"]}',
            description='Search restaurant health inspection scores and violation history across the US.',
            canonical_url=current_app.config['BASE_URL'] + '/',
            search_query=q,
            search_results=search_results,
            regions=[],
            recent_inspections=[],
            worst_scores=[],
            worst_region=worst_region,
            supported_regions=SUPPORTED_REGIONS,
            total_restaurants=0,
            total_inspections=0,
            feed=feed,
        )

    # Cache the region list and recently-inspected (not worst — that's per-region cached separately)
    cache_key = f'home_page_data_{feed}'
    cached = cache.get(cache_key)
    if cached:
        regions, recent_inspections, total_restaurants, total_inspections = cached
    else:
        region_counts = (
            db.session.query(Restaurant.region, func.count(Restaurant.id))
            .group_by(Restaurant.region)
            .order_by(Restaurant.region)
            .all()
        )
        regions = [{'region': r, 'count': c} for r, c in region_counts]

        recent_inspections = _recent_inspections(
            limit=10,
            restaurants_only=(feed != 'all'),
        )

        total_restaurants = (
            db.session.query(func.count(func.distinct(Restaurant.id)))
            .join(Inspection, Restaurant.id == Inspection.restaurant_id)
            .scalar()
        )
        total_inspections = db.session.query(func.count(Inspection.id)).scalar()

        cache.set(cache_key, (
            regions, recent_inspections, total_restaurants, total_inspections
        ), timeout=300)

    worst_scores = _worst_in_region(worst_region)

    return render_template(
        'home.html',
        title=f'Restaurant Health Inspection Scores | {current_app.config["SITE_NAME"]}',
        description='Search restaurant health inspection scores and violation history across the US.',
        canonical_url=current_app.config['BASE_URL'] + '/',
        search_query=q,
        search_results=None,
        regions=regions,
        recent_inspections=recent_inspections,
        worst_scores=worst_scores,
        worst_region=worst_region,
        supported_regions=SUPPORTED_REGIONS,
        total_restaurants=total_restaurants,
        total_inspections=total_inspections,
        feed=feed,
    )
