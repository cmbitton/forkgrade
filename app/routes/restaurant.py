import math
import re
from flask import render_template, current_app, abort
from sqlalchemy.orm import selectinload
from app.db import db, cache
from app.models.restaurant import Restaurant
from app.models.inspection import Inspection
from app.utils import get_region_display


def _cuisine_slug(label: str) -> str:
    s = label.lower()
    s = re.sub(r"[/&'\u2019,]+", '-', s)
    s = re.sub(r'\s+', '-', s)
    s = re.sub(r'[^a-z0-9-]', '', s)
    return re.sub(r'-+', '-', s).strip('-')


def get_nearby_restaurants(restaurant, limit=3):
    """Return up to `limit` nearby locations, sorted by distance."""
    if restaurant.latitude is not None and restaurant.longitude is not None:
        # Use a bounding box (~5km) to avoid loading the entire table
        radius = 0.05  # ~5.5km at RI latitudes
        for attempt in range(3):
            r = radius * (2 ** attempt)
            candidates = (
                Restaurant.query
                .filter(
                    Restaurant.region == restaurant.region,
                    Restaurant.id != restaurant.id,
                    Restaurant.latitude.isnot(None),
                    Restaurant.longitude.isnot(None),
                    Restaurant.latitude.between(
                        restaurant.latitude - r, restaurant.latitude + r),
                    Restaurant.longitude.between(
                        restaurant.longitude - r, restaurant.longitude + r),
                    Restaurant.inspections.any(),
                )
                .limit(50)
                .all()
            )
            if len(candidates) >= limit:
                break

        if candidates:
            def dist(r):
                dlat = r.latitude - restaurant.latitude
                dlng = r.longitude - restaurant.longitude
                return math.sqrt(dlat * dlat + dlng * dlng)
            candidates.sort(key=dist)
            return candidates[:limit]

    # Fallback: same city
    return (
        Restaurant.query
        .filter(
            Restaurant.region == restaurant.region,
            Restaurant.city == restaurant.city,
            Restaurant.id != restaurant.id,
            Restaurant.inspections.any(),
        )
        .limit(limit)
        .all()
    )


def render_restaurant(restaurant):
    """Render the restaurant detail page."""
    cache_key = f'restaurant_{restaurant.id}'
    cached = cache.get(cache_key)
    if cached:
        return cached

    inspections = (
        Inspection.query
        .options(selectinload(Inspection.violations))
        .filter_by(restaurant_id=restaurant.id)
        .filter(Inspection.not_future())
        .order_by(Inspection.inspection_date.desc())
        .all()
    )

    if not inspections:
        abort(404)

    latest_inspection = inspections[0]
    latest_violations = latest_inspection.violations

    # Determine what NYC grade to surface.
    # - A/B/C/Z/N/P from latest inspection → show as-is
    # - No grade on a cycle inspection → restaurant failed initial and is
    #   awaiting re-inspection; NYC requires them to post "Grade Pending"
    # - No grade on a compliance/admin visit → not a grading event, show nothing
    _itype = (latest_inspection.inspection_type or '').lower()
    if latest_inspection.grade in ('A', 'B', 'C', 'Z', 'N', 'P'):
        current_grade = latest_inspection.grade
    elif not latest_inspection.grade and 'cycle inspection' in _itype:
        current_grade = 'Z'  # render as "Grade Pending"
    else:
        current_grade = None

    total_inspections = len(inspections)

    # Violation counts from latest inspection only
    total_critical = 0
    total_major = 0
    total_minor = 0
    for v in latest_violations:
        if v.severity == 'critical':
            total_critical += 1
        elif v.severity == 'major':
            total_major += 1
        else:
            total_minor += 1

    nearby = get_nearby_restaurants(restaurant)

    # Build JSON-LD
    local_biz = {
        "@type": "Restaurant",
        "name": restaurant.name,
        "address": {
            "@type": "PostalAddress",
            "streetAddress": restaurant.address or '',
            "addressLocality": restaurant.city or '',
            "addressRegion": restaurant.state or '',
            "postalCode": restaurant.zip or ''
        }
    }
    if latest_inspection and latest_inspection.score is not None:
        local_biz["aggregateRating"] = {
            "@type": "AggregateRating",
            "ratingValue": str(latest_inspection.score),
            "bestRating": "100",
            "worstRating": "0",
            "ratingCount": str(total_inspections)
        }

    json_ld = {
        "@context": "https://schema.org",
        "@graph": [
            local_biz,
            {
                "@type": "BreadcrumbList",
                "itemListElement": [
                    {
                        "@type": "ListItem",
                        "position": 1,
                        "name": "Home",
                        "item": current_app.config['BASE_URL'] + '/'
                    },
                    {
                        "@type": "ListItem",
                        "position": 2,
                        "name": get_region_display(restaurant.region),
                        "item": current_app.config['BASE_URL'] + f'/{restaurant.region}/'
                    },
                    {
                        "@type": "ListItem",
                        "position": 3,
                        "name": restaurant.city or restaurant.region,
                        "item": current_app.config['BASE_URL'] + f'/{restaurant.region}/{restaurant.city_slug}/'
                    },
                    {
                        "@type": "ListItem",
                        "position": 4,
                        "name": restaurant.name,
                        "item": current_app.config['BASE_URL'] + f'/{restaurant.region}/{restaurant.slug}/'
                    }
                ]
            }
        ]
    }

    if restaurant.latitude is not None and restaurant.longitude is not None:
        json_ld['@graph'][0]['geo'] = {
            "@type": "GeoCoordinates",
            "latitude": restaurant.latitude,
            "longitude": restaurant.longitude
        }

    site_name = current_app.config['SITE_NAME']
    base_url = current_app.config['BASE_URL']

    _tier_labels = {'low': 'Low Risk', 'medium': 'Medium Risk', 'high': 'High Risk'}
    score = latest_inspection.score if latest_inspection else None
    tier_label = _tier_labels.get(restaurant.score_tier, '')

    if score is not None and tier_label:
        description = (
            f"{restaurant.display_name} health inspection score: {score} out of 100 ({tier_label}). "
            f"{restaurant.city}, {restaurant.state}. See full violation history, inspection dates, and detailed findings."
        )
    else:
        description = (
            f"{restaurant.display_name} in {restaurant.city}, {restaurant.state}. "
            f"View full health inspection history, violation records, and detailed findings."
        )
    if len(description) > 160:
        description = description[:157] + '...'

    if score is not None:
        og_title = f"{restaurant.display_name} — Health Inspection Score: {score}/100"
    else:
        og_title = f"{restaurant.display_name} — Health Inspection Score & History"
    if tier_label:
        og_description = (
            f"{restaurant.display_name} in {restaurant.city}, {restaurant.state}. "
            f"Rated {tier_label}. See full inspection history and violations."
        )
    else:
        og_description = description

    canonical_url = f"{base_url}/{restaurant.region}/{restaurant.slug}/"

    breadcrumbs = [
        {'name': 'Home', 'url': '/'},
        {'name': get_region_display(restaurant.region), 'url': f'/{restaurant.region}/'},
        {'name': restaurant.city or restaurant.region, 'url': f'/{restaurant.region}/{restaurant.city_slug}/'},
        {'name': restaurant.display_name}
    ]

    cuisine_slug = _cuisine_slug(restaurant.cuisine_type) if restaurant.cuisine_type else None

    response = render_template(
        'restaurant.html',
        title=f'{restaurant.display_name} Health Inspection Score & History — {restaurant.city}, {restaurant.state} | {site_name}',
        description=description,
        og_title=og_title,
        og_description=og_description,
        canonical_url=canonical_url,
        restaurant=restaurant,
        inspections=inspections,
        latest_inspection=latest_inspection,
        latest_violations=latest_violations,
        current_grade=current_grade,
        total_inspections=total_inspections,
        total_critical=total_critical,
        total_major=total_major,
        total_minor=total_minor,
        nearby=nearby,
        json_ld=json_ld,
        breadcrumbs=breadcrumbs,
        cuisine_slug=cuisine_slug,
    )
    cache.set(cache_key, response, timeout=300)
    return response
