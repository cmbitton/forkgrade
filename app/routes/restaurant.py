import math
import re
from flask import render_template, current_app, abort
from sqlalchemy.orm import selectinload
from app.db import db
from app.models.restaurant import Restaurant
from app.models.inspection import Inspection


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
            Restaurant.id != restaurant.id
        )
        .limit(limit)
        .all()
    )


def render_restaurant(restaurant):
    """Render the restaurant detail page."""
    inspections = (
        Inspection.query
        .options(selectinload(Inspection.violations))
        .filter_by(restaurant_id=restaurant.id)
        .order_by(Inspection.inspection_date.desc())
        .all()
    )

    latest_inspection = inspections[0] if inspections else None
    latest_violations = latest_inspection.violations if latest_inspection else []

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
    json_ld = {
        "@context": "https://schema.org",
        "@graph": [
            {
                "@type": "LocalBusiness",
                "name": restaurant.name,
                "address": {
                    "@type": "PostalAddress",
                    "streetAddress": restaurant.address or '',
                    "addressLocality": restaurant.city or '',
                    "addressRegion": restaurant.state or '',
                    "postalCode": restaurant.zip or ''
                }
            },
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
                        "name": restaurant.region.replace('-', ' ').title(),
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

    if latest_inspection:
        last_date_str = latest_inspection.inspection_date.strftime('%b %-d, %Y')
    else:
        last_date_str = 'N/A'

    score_str = f" Current score: {latest_inspection.score}." if latest_inspection and latest_inspection.score is not None else ''
    description = (
        f"View the full health inspection history for {restaurant.display_name} "
        f"in {restaurant.city}, {restaurant.state}. Last inspected {last_date_str}.{score_str} "
        f"{total_inspections} inspection{'s' if total_inspections != 1 else ''} on record."
    )

    canonical_url = f"{base_url}/{restaurant.region}/{restaurant.slug}/"

    breadcrumbs = [
        {'name': 'Home', 'url': '/'},
        {'name': restaurant.region.replace('-', ' ').title(), 'url': f'/{restaurant.region}/'},
        {'name': restaurant.city or restaurant.region, 'url': f'/{restaurant.region}/{restaurant.city_slug}/'},
        {'name': restaurant.display_name}
    ]

    cuisine_slug = _cuisine_slug(restaurant.cuisine_type) if restaurant.cuisine_type else None

    return render_template(
        'restaurant.html',
        title=f'{restaurant.display_name} Health Inspection Score & History — {restaurant.city}, {restaurant.state} | {site_name}',
        description=description,
        canonical_url=canonical_url,
        restaurant=restaurant,
        inspections=inspections,
        latest_inspection=latest_inspection,
        latest_violations=latest_violations,
        total_inspections=total_inspections,
        total_critical=total_critical,
        total_major=total_major,
        total_minor=total_minor,
        nearby=nearby,
        json_ld=json_ld,
        breadcrumbs=breadcrumbs,
        cuisine_slug=cuisine_slug,
    )
