from datetime import date
from flask import Blueprint, render_template, request, current_app
from sqlalchemy import func, and_
from app.db import db, cache
from app.models.restaurant import Restaurant
from app.models.inspection import Inspection

home_bp = Blueprint('home', __name__)

# cuisine_type values that indicate non-restaurant facilities
_NON_RESTAURANT_TYPES = {'School / Childcare', 'Healthcare Facility', 'Grocery / Market', 'Catering'}


def _recent_inspections(limit=10, restaurants_only=False):
    q = db.session.query(Inspection, Restaurant).join(Restaurant)
    if restaurants_only:
        q = q.filter(
            Restaurant.cuisine_type.isnot(None),
            ~Restaurant.cuisine_type.in_(_NON_RESTAURANT_TYPES),
        )
    return q.order_by(Inspection.inspection_date.desc()).limit(limit).all()


@home_bp.route('/')
def index():
    q = request.args.get('q', '').strip()
    feed = request.args.get('feed', 'restaurants')

    search_results = None
    if q:
        search_results = Restaurant.query.filter(
            Restaurant.name.ilike(f'%{q}%')
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
            low_scores_this_month=[],
            total_restaurants=0,
            total_inspections=0,
            feed=feed,
        )

    cache_key = f'home_page_data_{feed}'
    cached = cache.get(cache_key)
    if cached:
        regions, recent_inspections, low_scores_this_month, total_restaurants, total_inspections = cached
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

        today = date.today()
        month_start = today.replace(day=1)

        # Subquery: most recent inspection date per restaurant
        latest_sq = (
            db.session.query(
                Inspection.restaurant_id,
                func.max(Inspection.inspection_date).label('max_date'),
            )
            .group_by(Inspection.restaurant_id)
            .subquery()
        )
        # Only show restaurants whose MOST RECENT inspection was low-scoring
        # and happened this month — filters out ones that were re-inspected and passed.
        low_scores_this_month = (
            db.session.query(Inspection, Restaurant)
            .join(Restaurant)
            .join(latest_sq, and_(
                latest_sq.c.restaurant_id == Inspection.restaurant_id,
                latest_sq.c.max_date == Inspection.inspection_date,
            ))
            .filter(
                Inspection.inspection_date >= month_start,
                Inspection.score.isnot(None),
            )
            .order_by(Inspection.score.asc())
            .limit(10)
            .all()
        )

        total_restaurants = db.session.query(func.count(Restaurant.id)).scalar()
        total_inspections = db.session.query(func.count(Inspection.id)).scalar()

        cache.set(cache_key, (
            regions, recent_inspections, low_scores_this_month,
            total_restaurants, total_inspections
        ), timeout=300)

    return render_template(
        'home.html',
        title=f'Restaurant Health Inspection Scores | {current_app.config["SITE_NAME"]}',
        description='Search restaurant health inspection scores and violation history across the US.',
        canonical_url=current_app.config['BASE_URL'] + '/',
        search_query=q,
        search_results=search_results,
        regions=regions,
        recent_inspections=recent_inspections,
        low_scores_this_month=low_scores_this_month,
        total_restaurants=total_restaurants,
        total_inspections=total_inspections,
        feed=feed,
    )
