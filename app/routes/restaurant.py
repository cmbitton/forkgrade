import math
import re
from flask import render_template, current_app, abort
from sqlalchemy.orm import selectinload
from app.db import db, cache
from app.models.restaurant import Restaurant
from app.models.inspection import Inspection
from app.utils import get_region_display, get_region_aliases, cuisine_slug as _cuisine_slug
from app.helpers.summary import build_summary
from app.helpers.inspection_collapse import collapse_inspections


def get_nearby_restaurants(restaurant, limit=3):
    """Return up to `limit` nearby (Restaurant, score, score_tier) tuples, sorted by distance.

    Strategy:
      1. If the target has real lat/lng (NYC, Chicago, RI, Boston), do a
         cos(lat)-corrected bounding box around it within the same region.
         Exclude (0,0) sentinel coords — NYC has ~366 such rows that would
         otherwise cluster as "0m away".
      2. Otherwise (FL/TX/GA/AZ/PA — ~70% of dataset has no lat/lng), fall back
         to same ZIP first, then same city to fill any remaining slots. Ordered
         by latest_inspection_date so results are deterministic and surface
         recently-inspected places, not whatever Postgres scans first.
    """
    from app.models.inspection import Inspection

    def _with_scores(restaurants):
        """Attach latest score to a list of Restaurant objects in a single query."""
        if not restaurants:
            return []
        rid_map = {r.id: r for r in restaurants}
        rows = (
            db.session.query(Inspection.restaurant_id, Inspection.score, Inspection.risk_score)
            .filter(
                Inspection.restaurant_id.in_(list(rid_map.keys())),
                Inspection.inspection_date == db.session.query(Restaurant.latest_inspection_date)
                    .filter(Restaurant.id == Inspection.restaurant_id)
                    .correlate(Inspection)
                    .scalar_subquery(),
            )
            .all()
        )
        score_map = {}
        for rid, score, risk_score in rows:
            if score is not None:
                tier = 'low' if score >= 75 else ('medium' if score >= 55 else 'high')
            else:
                tier = None
            score_map[rid] = (score, tier)
        # Preserve incoming order (which is sorted by distance for the geo path).
        return [(r, *score_map.get(r.id, (None, None))) for r in restaurants]

    # ── Phase 1: geographic search ───────────────────────────────────────────
    has_real_coords = (
        restaurant.latitude is not None
        and restaurant.longitude is not None
        and not (restaurant.latitude == 0 and restaurant.longitude == 0)
    )
    if has_real_coords:
        # cos(latitude) corrects for longitude shrinking toward the poles —
        # at NYC (40.7°N), 0.01° longitude is only ~0.84km vs ~1.11km for
        # latitude. Without this, the box is rectangular not square, and the
        # Euclidean sort under-counts E-W distance, misordering candidates.
        cos_lat = max(0.1, math.cos(math.radians(restaurant.latitude)))

        # Cos-corrected squared distance, computed on the SQL side. We sort
        # on this with ORDER BY so Postgres returns the actual closest rows,
        # not whatever order the bounding-box scan happened to produce. NYC
        # in particular has dense neighborhoods with thousands of restaurants
        # in a 5km box — without SQL ORDER BY, a Python-side `limit(N)` was
        # picking 500 arbitrary candidates and missing the real closest.
        # No sqrt needed — squared distance is monotonic for ordering.
        lat_diff = Restaurant.latitude - restaurant.latitude
        lng_diff = (Restaurant.longitude - restaurant.longitude) * cos_lat
        dist_sq_expr = lat_diff * lat_diff + lng_diff * lng_diff

        # Start at a 5km half-side; grow to 10 then 20 if we don't have enough.
        candidates = []
        for attempt in range(3):
            half_km = 5.0 * (2 ** attempt)
            dlat = half_km / 111.0
            dlng = half_km / (111.0 * cos_lat)
            candidates = (
                Restaurant.query
                .filter(
                    Restaurant.region == restaurant.region,
                    Restaurant.id != restaurant.id,
                    Restaurant.latitude.isnot(None),
                    Restaurant.longitude.isnot(None),
                    # Exclude (0,0) sentinel — ~366 such rows in NYC alone
                    # would otherwise all show as "0m away" from each other.
                    db.or_(Restaurant.latitude != 0, Restaurant.longitude != 0),
                    Restaurant.latitude.between(
                        restaurant.latitude - dlat, restaurant.latitude + dlat),
                    Restaurant.longitude.between(
                        restaurant.longitude - dlng, restaurant.longitude + dlng),
                    Restaurant.latest_inspection_date.isnot(None),
                )
                .order_by(dist_sq_expr)
                .limit(50)
                .all()
            )
            if len(candidates) >= limit:
                break

        if candidates:
            # SQL has already returned them in distance order — just slice.
            return _with_scores(candidates[:limit])

    # ── Phase 2: ZIP-then-city fallback ──────────────────────────────────────
    base_filters = [
        Restaurant.region == restaurant.region,
        Restaurant.id != restaurant.id,
        Restaurant.latest_inspection_date.isnot(None),
    ]
    fallback = []
    # Compare on the first 5 chars of ZIP on BOTH sides — Florida sometimes
    # stores the +4 jammed in (e.g. "331767930") while neighbors store plain
    # 5-char zips ("33176"). A literal == match would never find them.
    target_zip5 = (restaurant.zip or '').strip()[:5] if restaurant.zip else ''
    if target_zip5:
        fallback = (
            Restaurant.query
            .filter(
                *base_filters,
                db.func.substr(Restaurant.zip, 1, 5) == target_zip5,
            )
            .order_by(Restaurant.latest_inspection_date.desc())
            .limit(limit)
            .all()
        )
    if len(fallback) < limit and restaurant.city:
        existing_ids = {r.id for r in fallback}
        city_q = Restaurant.query.filter(*base_filters, Restaurant.city == restaurant.city)
        if existing_ids:
            city_q = city_q.filter(Restaurant.id.notin_(existing_ids))
        more = (
            city_q
            .order_by(Restaurant.latest_inspection_date.desc())
            .limit(limit - len(fallback))
            .all()
        )
        fallback.extend(more)
    return _with_scores(fallback)


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

    # Boston's source data pairs every Fail with a closeout Pass/CP ~7–14
    # days later that re-stamps the same violations. Collapsing pairs here
    # means the timeline, stat cards, and summary all see the same logical
    # visits. Other regions are no-ops through this helper.
    inspections = collapse_inspections(inspections)

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

    # Ultra-thin pages (1 inspection, 0 violations) render <300 words and dilute
    # site-wide quality. Noindex them so Google drops them from the index on
    # recrawl — they remain crawlable and accessible, just not ranked.
    total_violations_latest = total_critical + total_major + total_minor
    noindex = total_inspections <= 1 and total_violations_latest == 0

    nearby = get_nearby_restaurants(restaurant)

    summary_data = build_summary(restaurant.id)

    # Build JSON-LD
    local_biz = {
        "@type": "Restaurant",
        "name": restaurant.display_name,
        "areaServed": {
            "@type": "AdministrativeArea",
            "name": get_region_display(restaurant.region),
            **({"alternateName": get_region_aliases(restaurant.region)}
               if get_region_aliases(restaurant.region) else {}),
        },
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
                        "name": restaurant.display_name,
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

    if summary_data and summary_data.get('faq'):
        json_ld['@graph'].append({
            "@type": "FAQPage",
            "mainEntity": [
                {
                    "@type": "Question",
                    "name": qa['question'],
                    "acceptedAnswer": {
                        "@type": "Answer",
                        "text": qa['answer']
                    }
                }
                for qa in summary_data['faq']
            ]
        })

    site_name = current_app.config['SITE_NAME']
    base_url = current_app.config['BASE_URL']

    _tier_labels = {'low': 'Low Risk', 'medium': 'Medium Risk', 'high': 'High Risk'}
    score = latest_inspection.score if latest_inspection else None
    score_tier = latest_inspection.score_tier if latest_inspection else None
    tier_label = _tier_labels.get(score_tier, '')
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

    # Compute display tier from score (avoids lazy-loading inspections relationship)
    if score is not None and score >= 75:
        score_display_tier = 'low'
    elif score is not None and score >= 55:
        score_display_tier = 'medium'
    elif score is not None:
        score_display_tier = 'high'
    else:
        score_display_tier = None

    # Violation-legend scheme: picks which set of severity labels the template
    # should display. The Texas region bundles two health departments —
    # Houston HHD uses "Substantial/Serious/General", San Antonio Metro uses
    # FDA standard "Priority/Priority Foundation/Core" — so we can't branch
    # on `restaurant.region` alone.
    #
    # FRAGILE — TECHNICAL DEBT: we discriminate the two Texas sources by
    # source_id format (Houston = UUID-hex with dashes, SA = numeric). This
    # holds today but silently mis-labels any future Texas data source whose
    # IDs happen to contain dashes. The proper fix is a `data_source` column
    # on restaurants (or a small lookup keyed by source_id prefix), populated
    # by each importer. Revisit before adding a third Texas source.
    if restaurant.region == 'nyc':
        violation_scheme = 'nyc'
    elif restaurant.region == 'texas' and restaurant.source_id and '-' in restaurant.source_id:
        violation_scheme = 'houston'
    else:
        violation_scheme = 'fda'

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
        latest_score=score,
        score_tier=score_tier,
        score_display_tier=score_display_tier,
        violation_scheme=violation_scheme,
        summary_data=summary_data,
        noindex=noindex,
    )
    cache.set(cache_key, response, timeout=300)
    return response
