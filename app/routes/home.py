from datetime import date, timedelta

from flask import Blueprint, render_template, request, current_app
from sqlalchemy import func
from app.db import db, cache
from app.models.restaurant import Restaurant
from app.models.inspection import Inspection
from app.utils import search_restaurants

home_bp = Blueprint('home', __name__)

_NON_RESTAURANT_TYPES = {'School / Childcare', 'Healthcare Facility', 'Grocery / Market', 'Catering'}


def _recent_inspections(limit=10, restaurants_only=False):
    q = db.session.query(Inspection, Restaurant).join(Restaurant)
    if restaurants_only:
        q = q.filter(
            Restaurant.cuisine_type.isnot(None),
            ~Restaurant.cuisine_type.in_(_NON_RESTAURANT_TYPES),
        )
    return q.order_by(Inspection.inspection_date.desc()).limit(limit).all()


def _lowest_scores(limit=10):
    """Bottom scores across all regions whose most recent inspection was in the past 30 days."""
    cache_key = 'lowest_scores_month'
    hit = cache.get(cache_key)
    if hit is not None:
        return hit

    cutoff = date.today() - timedelta(days=30)
    rows = (
        db.session.query(Inspection, Restaurant)
        .join(Restaurant, Restaurant.id == Inspection.restaurant_id)
        .filter(
            Inspection.inspection_date == Restaurant.latest_inspection_date,
            Inspection.inspection_date >= cutoff,
            Inspection.score.isnot(None),
        )
        .order_by(Inspection.score.asc())
        .limit(limit)
        .all()
    )
    cache.set(cache_key, rows, timeout=300)
    return rows


@home_bp.route('/about', strict_slashes=False)
def about():
    return render_template(
        'about.html',
        title=f'About | {current_app.config["SITE_NAME"]}',
        description='ForkGrade is a free public tool that aggregates food establishment health inspection data from government sources.',
        canonical_url=current_app.config['BASE_URL'] + '/about',
    )


@home_bp.route('/methodology', strict_slashes=False)
def methodology():
    return render_template(
        'methodology.html',
        title=f'Methodology | {current_app.config["SITE_NAME"]}',
        description='How ForkGrade calculates health inspection scores from violation data.',
        canonical_url=current_app.config['BASE_URL'] + '/methodology',
    )


@home_bp.route('/')
def index():
    q    = request.args.get('q', '').strip()
    feed = request.args.get('feed', 'restaurants')

    if q:
        sort     = request.args.get('sort', 'date')
        page     = max(1, request.args.get('page', 1, type=int))
        per_page = 25
        search_results, search_total = search_restaurants(q, sort=sort, page=page, per_page=per_page)
        total_pages = max(1, (search_total + per_page - 1) // per_page)

        return render_template(
            'home.html',
            title=f'Search results for "{q}" | {current_app.config["SITE_NAME"]}',
            description='Search health inspection scores and violation history across the US.',
            canonical_url=current_app.config['BASE_URL'] + '/',
            search_query=q,
            search_results=search_results,
            search_total=search_total,
            search_sort=sort,
            search_page=page,
            search_total_pages=total_pages,
            search_base_path='/',
            regions=[],
            recent_inspections=[],
            lowest_scores=[],
            total_restaurants=0,
            total_inspections=0,
            feed=feed,
        )

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

    lowest_scores = _lowest_scores()

    return render_template(
        'home.html',
        title=f'Restaurant Health Inspection Scores | {current_app.config["SITE_NAME"]}',
        description='Search health inspection scores and violation history across the US.',
        canonical_url=current_app.config['BASE_URL'] + '/',
        search_query=q,
        search_results=None,
        regions=regions,
        recent_inspections=recent_inspections,
        lowest_scores=lowest_scores,
        total_restaurants=total_restaurants,
        total_inspections=total_inspections,
        feed=feed,
    )
