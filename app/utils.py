"""Shared utility helpers."""

from app.db import db
from app.models.restaurant import Restaurant
from app.models.inspection import Inspection

REGION_DISPLAY_NAMES = {
    'nyc': 'NYC',
}


def get_region_display(region: str) -> str:
    """Return a human-readable display name for a region slug."""
    return REGION_DISPLAY_NAMES.get(region, region.replace('-', ' ').title())


def search_restaurants(q, region=None, sort='date', page=1, per_page=25):
    """Return (rows, total) for a name search.

    rows  — list of (Restaurant, Inspection|None) tuples
    total — total matching count (for pagination)

    region: if given, scopes to that region only.
    sort:   'date' (newest first), 'score' (best→worst), 'name' (A–Z)
    """
    query = (
        db.session.query(Restaurant, Inspection)
        .outerjoin(Inspection, db.and_(
            Inspection.restaurant_id == Restaurant.id,
            Inspection.inspection_date == Restaurant.latest_inspection_date,
        ))
        .filter(Restaurant.name.ilike(f'%{q}%'))
    )

    if region:
        query = query.filter(Restaurant.region == region)

    if sort == 'score':
        query = query.order_by(
            db.case((Inspection.score.is_(None), 1), else_=0),
            Inspection.score.desc(),
        )
    elif sort == 'name':
        query = query.order_by(Restaurant.name.asc())
    else:  # date (default)
        query = query.order_by(
            db.case((Inspection.inspection_date.is_(None), 1), else_=0),
            Inspection.inspection_date.desc(),
        )

    total = query.count()
    rows = query.offset((page - 1) * per_page).limit(per_page).all()
    return rows, total
