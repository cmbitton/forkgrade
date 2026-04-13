import logging
import re
import time
from datetime import date, timedelta

from flask import Blueprint, render_template, request, abort, current_app, redirect
from sqlalchemy import func
from sqlalchemy.orm import selectinload

_log = logging.getLogger('forkgrade.perf')

from app.db import db, cache
from app.models.restaurant import Restaurant
from app.models.inspection import Inspection
from app.routes.restaurant import render_restaurant
from app.utils import get_region_display, get_region_aliases, region_location, search_restaurants

region_bp = Blueprint('region', __name__)


# ── Legacy region redirects ───────────────────────────────────────────────────
# Houston and San Antonio were previously standalone regions. They were merged
# into 'texas'. Any bookmarked or indexed URL under the old slugs 301s to the
# matching path under /texas/. Static routes take precedence over the generic
# /<region>/ routes, so these match first. These slugs must remain reserved —
# we can never introduce a new region called 'houston' or 'san-antonio' again.

def _legacy_texas_redirect(subpath: str = ''):
    target = f'/texas/{subpath}' if subpath else '/texas/'
    if request.query_string:
        target = f'{target}?{request.query_string.decode()}'
    return redirect(target, code=301)


@region_bp.route('/houston/', defaults={'subpath': ''})
@region_bp.route('/houston/<path:subpath>')
def legacy_houston(subpath):
    return _legacy_texas_redirect(subpath)


@region_bp.route('/san-antonio/', defaults={'subpath': ''})
@region_bp.route('/san-antonio/<path:subpath>')
def legacy_san_antonio(subpath):
    return _legacy_texas_redirect(subpath)


_NON_RESTAURANT_TYPES = frozenset([
    'School / Childcare', 'Healthcare Facility', 'Grocery / Market', 'Catering'
])

_SORT_DEFAULTS = {'date': 'desc', 'score': 'desc', 'name': 'asc'}


def _scored_restaurants(region, order='asc', limit=5, days=None):
    """(Restaurant, Inspection) tuples sorted by risk_score, restaurants only."""
    col = Inspection.risk_score.asc() if order == 'asc' else Inspection.risk_score.desc()
    filters = [
        Restaurant.region == region,
        Inspection.risk_score.isnot(None),
        Inspection.not_future(),
    ]
    if days is not None:
        cutoff = date.today() - timedelta(days=days)
        filters.append(Inspection.inspection_date >= cutoff)
    return (
        db.session.query(Restaurant, Inspection)
        .join(Inspection, db.and_(
            Inspection.restaurant_id == Restaurant.id,
            Inspection.inspection_date == Restaurant.latest_inspection_date,
        ))
        .filter(*filters)
        .order_by(col)
        .limit(limit)
        .all()
    )


def _city_slug(city: str) -> str:
    c = city.lower().replace("'", '')
    c = re.sub(r'\s+', '-', c)
    return re.sub(r'[^a-z0-9-]', '', c)


def _cuisine_slug(label: str) -> str:
    s = label.lower()
    s = re.sub(r"[/&'\u2019,]+", '-', s)
    s = re.sub(r'\s+', '-', s)
    s = re.sub(r'[^a-z0-9-]', '', s)
    return re.sub(r'-+', '-', s).strip('-')


def _home_state(region: str) -> str | None:
    """Return the most common restaurant.state for this region (its native state).

    Used to exclude out-of-state vendors from the city/neighborhood pages.
    Cached 1 h — the home state for a region never changes in practice.
    """
    cache_key = f'home_state_{region}'
    hit = cache.get(cache_key)
    if hit is not None:
        return hit
    row = (
        db.session.query(Restaurant.state, func.count(Restaurant.id))
        .filter(Restaurant.region == region, Restaurant.state.isnot(None))
        .group_by(Restaurant.state)
        .order_by(func.count(Restaurant.id).desc())
        .first()
    )
    result = row[0] if row else None
    cache.set(cache_key, result, timeout=3600)
    return result


def _city_list(region: str, home_state: str | None) -> list:
    """Distinct city names for a region, filtered to home state. Cached 1 h."""
    cache_key = f'city_list_{region}'
    hit = cache.get(cache_key)
    if hit is not None:
        return hit
    q = db.session.query(Restaurant.city).filter(Restaurant.region == region)
    if home_state:
        q = q.filter(Restaurant.state == home_state)
    result = q.distinct().all()
    cache.set(cache_key, result, timeout=3600)
    return result


def _cuisine_min_count(size):
    """Minimum cuisine type count threshold based on location count."""
    if size >= 10000:
        return 150
    if size >= 3000:
        return 50
    return 20


def _get_cuisine_types(region):
    """Return list of {slug, label, count} dicts for cuisine types in region.

    Minimum count threshold scales with region size via _cuisine_min_count(). Cached 1 h.
    """
    cache_key = f'cuisine_types_{region}'
    hit = cache.get(cache_key)
    if hit is not None:
        return hit
    region_size = db.session.query(func.count(Restaurant.id)).filter(
        Restaurant.region == region
    ).scalar() or 0
    min_count = _cuisine_min_count(region_size)
    rows = (
        db.session.query(Restaurant.cuisine_type, func.count(Restaurant.id))
        .filter(
            Restaurant.region == region,
            Restaurant.cuisine_type.isnot(None),
            Restaurant.latest_inspection_date.isnot(None),
        )
        .group_by(Restaurant.cuisine_type)
        .having(func.count(Restaurant.id) >= min_count)
        .all()
    )
    result = [
        {'slug': _cuisine_slug(label), 'label': label, 'count': cnt}
        for label, cnt in rows if label
    ]
    cache.set(cache_key, result, timeout=3600)
    return result


def _cuisine_rows(region, cuisine_type, city_name=None, sort='date', sort_dir=None, page=1, per_page=25):
    """(Restaurant, Inspection|None) for a cuisine type, with sort and pagination.

    Returns (rows, total_count).
    sort: 'date', 'score', 'name'
    sort_dir: 'asc' or 'desc' (defaults per _SORT_DEFAULTS)
    Cached 5 min per unique (region, cuisine, city, sort, sort_dir, page) combination.
    """
    if sort_dir is None:
        sort_dir = _SORT_DEFAULTS.get(sort, 'desc')
    cache_key = f'cuisine_rows_{region}_{cuisine_type}_{city_name or ""}_{sort}_{sort_dir}_{page}'
    hit = cache.get(cache_key)
    _log.info('_cuisine_rows cache %s | key=%r', 'HIT' if hit is not None else 'MISS', cache_key)
    if hit is not None:
        return hit

    t0 = time.monotonic()

    # Phase 1: count + sort/paginate on restaurants table only (indexed, fast)
    base_filters = [
        Restaurant.region == region,
        Restaurant.cuisine_type == cuisine_type,
    ]
    if city_name:
        base_filters.append(Restaurant.city == city_name)
    total = Restaurant.query.filter(*base_filters).count()

    rq = Restaurant.query.filter(*base_filters)
    if sort == 'score':
        score_sub = (
            db.session.query(Inspection.risk_score)
            .filter(
                Inspection.restaurant_id == Restaurant.id,
                Inspection.inspection_date == Restaurant.latest_inspection_date,
            )
            .correlate(Restaurant)
            .limit(1)
            .scalar_subquery()
        )
        # risk_score: lower = better. desc (default) = best first = ASC
        score_col = score_sub.asc() if sort_dir == 'desc' else score_sub.desc()
        rq = rq.order_by(
            db.case((score_sub.is_(None), 1), else_=0),
            score_col,
        )
    elif sort == 'name':
        rq = rq.order_by(Restaurant.name.asc() if sort_dir == 'asc' else Restaurant.name.desc())
    else:  # date
        date_col = Restaurant.latest_inspection_date.desc() if sort_dir == 'desc' else Restaurant.latest_inspection_date.asc()
        rq = rq.order_by(
            db.case((Restaurant.latest_inspection_date.is_(None), 1), else_=0),
            date_col,
        )

    page_ids = [r.id for r in rq.with_entities(Restaurant.id)
                .offset((page - 1) * per_page).limit(per_page).all()]

    # Phase 2: join only the paginated slice with inspections
    if page_ids:
        id_order = db.case(
            {rid: i for i, rid in enumerate(page_ids)},
            value=Restaurant.id,
        )
        rows = (
            db.session.query(Restaurant, Inspection)
            .outerjoin(
                Inspection,
                db.and_(
                    Inspection.restaurant_id == Restaurant.id,
                    Inspection.inspection_date == Restaurant.latest_inspection_date,
                    Inspection.not_future(),
                )
            )
            .filter(Restaurant.id.in_(page_ids))
            .order_by(id_order)
            .all()
        )
    else:
        rows = []
    result = (rows, total)
    stored = cache.set(cache_key, result, timeout=300)
    _log.info('_cuisine_rows queried in %.0fms, total=%d, cache.set=%s',
              (time.monotonic() - t0) * 1000, total, stored)
    return result


def render_cuisine(region, cuisine_slug_str, cuisine_label, rows,
                   city_name=None, city_slug_str=None,
                   total=0, page=1, per_page=25, sort='date', sort_dir=None):
    site_name      = current_app.config['SITE_NAME']
    base_url       = current_app.config['BASE_URL']
    region_display = get_region_display(region)

    if city_name:
        title         = f'{cuisine_label} Health Inspections in {city_name} | {site_name}'
        description   = (f'Health inspection scores for {total} {cuisine_label} locations '
                         f'in {city_name}, {region_display}.')
        canonical_url = f'{base_url}/{region}/{city_slug_str}/{cuisine_slug_str}/'
        heading       = f'{cuisine_label} in {city_name}'
        breadcrumbs   = [
            {'name': 'Home',          'url': '/'},
            {'name': region_display,  'url': f'/{region}/'},
            {'name': city_name,       'url': f'/{region}/{city_slug_str}/'},
            {'name': cuisine_label},
        ]
        base_path     = f'/{region}/{city_slug_str}/{cuisine_slug_str}/'
    else:
        title         = f'{cuisine_label} Health Inspections in {region_display} | {site_name}'
        description   = (f'Health inspection scores for {total} {cuisine_label} locations '
                         f'in {region_display}.')
        canonical_url = f'{base_url}/{region}/{cuisine_slug_str}/'
        heading       = f'{cuisine_label} in {region_display}'
        breadcrumbs   = [
            {'name': 'Home',         'url': '/'},
            {'name': region_display, 'url': f'/{region}/'},
            {'name': cuisine_label},
        ]
        base_path     = f'/{region}/{cuisine_slug_str}/'

    if sort_dir is None:
        sort_dir = _SORT_DEFAULTS.get(sort, 'desc')
    total_pages = max(1, (total + per_page - 1) // per_page)

    return render_template(
        'neighborhood.html',
        title         = title,
        description   = description,
        canonical_url = canonical_url,
        region        = region,
        region_display= region_display,
        city_name     = heading,
        city_slug     = cuisine_slug_str,
        rows          = rows,
        breadcrumbs   = breadcrumbs,
        is_cuisine    = True,
        sort          = sort,
        sort_dir      = sort_dir,
        page          = page,
        per_page      = per_page,
        total         = total,
        total_pages   = total_pages,
        base_path     = base_path,
    )


def render_neighborhood(region, city_slug_str, city_name, restaurants_with_scores,
                        sort='date', sort_dir=None, page=1, per_page=25, total=None):
    site_name      = current_app.config['SITE_NAME']
    base_url       = current_app.config['BASE_URL']
    region_display = get_region_display(region)

    # Cuisine types present in this city for sub-navigation — cached 1 h
    ct_cache_key = f'city_cuisine_types_{region}_{city_slug_str}'
    city_cuisine_types = cache.get(ct_cache_key)
    if city_cuisine_types is None:
        city_size = db.session.query(func.count(Restaurant.id)).filter(
            Restaurant.region == region, Restaurant.city == city_name
        ).scalar() or 0
        min_count = _cuisine_min_count(city_size)
        city_cuisine_rows = (
            db.session.query(Restaurant.cuisine_type, func.count(Restaurant.id))
            .join(Inspection, Restaurant.id == Inspection.restaurant_id)
            .filter(Restaurant.region == region, Restaurant.city == city_name,
                    Restaurant.cuisine_type.isnot(None))
            .group_by(Restaurant.cuisine_type)
            .having(func.count(Restaurant.id) >= min_count)
            .all()
        )
        city_cuisine_types = [
            {'slug': _cuisine_slug(label), 'label': label, 'count': cnt}
            for label, cnt in city_cuisine_rows if label
        ]
        cache.set(ct_cache_key, city_cuisine_types, timeout=3600)

    breadcrumbs = [
        {'name': 'Home',          'url': '/'},
        {'name': region_display,  'url': f'/{region}/'},
        {'name': city_name},
    ]

    if sort_dir is None:
        sort_dir = _SORT_DEFAULTS.get(sort, 'desc')
    if total is None:
        total = len(restaurants_with_scores)
    total_pages = max(1, (total + per_page - 1) // per_page)
    from app.utils import get_region_state_abbr as _state_abbr
    _loc = f'{city_name}, {_state_abbr(region)}' if _state_abbr(region) else f'{city_name}, {region_display}'
    return render_template(
        'neighborhood.html',
        title         = f'Restaurant Health Inspections in {city_name}, {region_display} — Scores & Rankings | {site_name}',
        description   = f'Restaurant health inspection scores in {_loc}. Search {total}+ facilities with violation history, scores, and risk tiers.',
        canonical_url = f'{base_url}/{region}/{city_slug_str}/',
        region        = region,
        region_display= region_display,
        city_name     = city_name,
        city_slug     = city_slug_str,
        rows          = restaurants_with_scores,
        breadcrumbs   = breadcrumbs,
        city_cuisine_types = city_cuisine_types,
        is_cuisine    = False,
        sort          = sort,
        sort_dir      = sort_dir,
        page          = page,
        per_page      = per_page,
        total         = total,
        total_pages   = total_pages,
        base_path     = f'/{region}/{city_slug_str}/',
    )


@region_bp.route('/<region>/')
def region_index(region):
    q = request.args.get('q', '').strip()

    site_name      = current_app.config['SITE_NAME']
    base_url       = current_app.config['BASE_URL']
    region_display = get_region_display(region)

    cache_key = f'region_index_{region}'

    aliases = get_region_aliases(region)
    if q:
        # Use cached count if warm; otherwise a fast index-only count
        cached_for_count = cache.get(cache_key)
        count = cached_for_count[0] if cached_for_count else (
            Restaurant.query.filter_by(region=region).count()
        )
        if count == 0:
            abort(404)
        sort     = request.args.get('sort', 'date')
        sort_dir = request.args.get('dir', _SORT_DEFAULTS.get(sort, 'desc'))
        page     = max(1, request.args.get('page', 1, type=int))
        per_page = 25
        search_results, search_total = search_restaurants(q, region=region, sort=sort, sort_dir=sort_dir, page=page, per_page=per_page)
        total_pages = max(1, (search_total + per_page - 1) // per_page)
        return render_template(
            'region.html',
            title          = f'Health Inspections in {region_display} | {site_name}',
            description    = f'Search health inspection scores in {region_location(region)}.',
            canonical_url  = f'{base_url}/{region}/',
            region         = region,
            region_display = region_display,
            search_query        = q,
            search_results      = search_results,
            search_total        = search_total,
            search_sort         = sort,
            search_sort_dir     = sort_dir,
            search_page         = page,
            search_total_pages  = total_pages,
            search_base_path    = f'/{region}/',
            total_restaurants   = count,
            total_inspections   = 0,
            neighborhoods       = [],
            top_cities          = [],
            recent_inspections  = [],
            top_restaurants     = [],
            bottom_restaurants  = [],
            cuisine_types       = [],
            aliases             = aliases,
        )
    cached = cache.get(cache_key)
    if cached:
        (count, total_inspections, neighborhoods, top_cities,
         recent_inspections, bottom_restaurants, cuisine_types) = cached
    else:
        count = (
            Restaurant.query
            .filter_by(region=region)
            .filter(Restaurant.latest_inspection_date.isnot(None))
            .count()
        )
        if count == 0:
            abort(404)

        insp_cache_key = f'total_inspections_{region}'
        total_inspections = cache.get(insp_cache_key)
        if total_inspections is None:
            total_inspections = (
                db.session.query(func.count(Inspection.id))
                .filter(Inspection.region == region)
                .scalar()
            )
            cache.set(insp_cache_key, total_inspections, timeout=3600)

        home_state = _home_state(region)
        city_q = (
            db.session.query(Restaurant.city, func.count(Restaurant.id))
            .filter(
                Restaurant.region == region,
                Restaurant.city.isnot(None),
                Restaurant.city != '',
                Restaurant.city != '0',
            )
        )
        if home_state:
            city_q = city_q.filter(Restaurant.state == home_state)
        city_count = func.count(Restaurant.id)
        city_rows = (
            city_q.group_by(Restaurant.city)
                  .having(city_count >= 3)
                  .order_by(Restaurant.city)
                  .all()
        )
        neighborhoods = [
            {'city': city, 'count': cnt, 'city_slug': _city_slug(city or '')}
            for city, cnt in city_rows
        ]
        top_cities = sorted(neighborhoods, key=lambda n: n['count'], reverse=True)[:8]

        recent_inspections = (
            db.session.query(Inspection, Restaurant)
            .join(Restaurant)
            .filter(
                Restaurant.region == region,
                Inspection.inspection_date == Restaurant.latest_inspection_date,
                Inspection.not_future(),
            )
            .order_by(Restaurant.latest_inspection_date.desc())
            .limit(20)
            .all()
        )

        bottom_restaurants = _scored_restaurants(region, order='desc', limit=5, days=30)
        cuisine_types = _get_cuisine_types(region)

        cache.set(cache_key, (
            count, total_inspections, neighborhoods, top_cities,
            recent_inspections, bottom_restaurants, cuisine_types,
        ), timeout=300)

    return render_template(
        'region.html',
        title          = f'Restaurant Health Inspections in {region_display} — Scores & Violations | {site_name}',
        description    = f'Search {count:,}+ restaurant health inspection scores in {region_location(region)}. Violations, risk tiers, and inspection history for every food establishment.',
        canonical_url  = f'{base_url}/{region}/',
        region         = region,
        region_display = region_display,
        search_query   = '',
        search_results = None,
        total_restaurants  = count,
        total_inspections  = total_inspections,
        neighborhoods      = neighborhoods,
        top_cities         = top_cities,
        recent_inspections = recent_inspections,
        top_restaurants    = [],
        bottom_restaurants = bottom_restaurants,
        cuisine_types      = cuisine_types,
        aliases            = aliases,
    )


@region_bp.route('/<region>/insights/')
def region_insights(region):
    from app.models.region_stats import RegionStats

    site_name      = current_app.config['SITE_NAME']
    base_url       = current_app.config['BASE_URL']
    region_display = get_region_display(region)

    stats = db.session.get(RegionStats, region)
    if not stats:
        abort(404)
    data = stats.data

    sc = data['severity_counts']
    most_common_sev = max(sc, key=sc.get) if any(sc.values()) else 'minor'

    return render_template(
        'insights.html',
        title          = f'Health Inspection Insights: {region_display} | {site_name}',
        description    = (f'Explore health inspection trends, risk patterns, and violation data '
                          f'across {data["total_locations"]:,}+ food establishments in '
                          f'{region_location(region)}.'),
        canonical_url  = f'{base_url}/{region}/insights/',
        region         = region,
        region_display = region_display,
        breadcrumbs    = [
            {'name': 'Home',         'url': '/'},
            {'name': region_display, 'url': f'/{region}/'},
            {'name': 'Insights'},
        ],
        data            = data,
        most_common_sev = most_common_sev,
    )


@region_bp.route('/<region>/<path_slug>/')
def region_sub(region, path_slug):
    # 1. Try restaurant slug
    restaurant = Restaurant.query.filter_by(region=region, slug=path_slug).first()
    if restaurant:
        return render_restaurant(restaurant)

    # 2. Try city slug — only match cities in the region's home state
    home_state = _home_state(region)
    cities = _city_list(region, home_state)
    city_name = next(
        (c[0] for c in cities if c[0] and _city_slug(c[0]) == path_slug),
        None
    )
    if city_name:
        sort = request.args.get('sort', 'date')
        sort_dir = request.args.get('dir', _SORT_DEFAULTS.get(sort, 'desc'))
        page = max(1, int(request.args.get('page', 1) or 1))
        per_page = 25

        city_cache_key = f'city_page_{region}_{path_slug}_{sort}_{sort_dir}_{page}'
        cached = cache.get(city_cache_key)
        if cached:
            rows, total = cached
        else:
            # Phase 1: count + sort/paginate on restaurants table only (indexed, fast)
            base_filter = [
                Restaurant.region == region,
                Restaurant.city == city_name,
                Restaurant.latest_inspection_date.isnot(None),
            ]
            total = Restaurant.query.filter(*base_filter).count()

            rq = Restaurant.query.filter(*base_filter)
            if sort == 'score':
                # Need inspection data for score sort — use subquery
                score_sub = (
                    db.session.query(Inspection.risk_score)
                    .filter(
                        Inspection.restaurant_id == Restaurant.id,
                        Inspection.inspection_date == Restaurant.latest_inspection_date,
                    )
                    .correlate(Restaurant)
                    .limit(1)
                    .scalar_subquery()
                )
                # risk_score: lower = better. desc (default) = best first = ASC
                score_col = score_sub.asc() if sort_dir == 'desc' else score_sub.desc()
                rq = rq.order_by(
                    db.case((score_sub.is_(None), 1), else_=0),
                    score_col,
                )
            elif sort == 'name':
                rq = rq.order_by(Restaurant.name.asc() if sort_dir == 'asc' else Restaurant.name.desc())
            else:  # date — sort on denormalized column, fully indexed
                rq = rq.order_by(
                    Restaurant.latest_inspection_date.desc() if sort_dir == 'desc'
                    else Restaurant.latest_inspection_date.asc()
                )

            page_ids = [r.id for r in rq.with_entities(Restaurant.id)
                        .offset((page - 1) * per_page).limit(per_page).all()]

            # Phase 2: join only the 25 paginated restaurants with their inspections
            if page_ids:
                id_order = db.case(
                    {rid: i for i, rid in enumerate(page_ids)},
                    value=Restaurant.id,
                )
                rows = (
                    db.session.query(Restaurant, Inspection)
                    .outerjoin(
                        Inspection,
                        db.and_(
                            Inspection.restaurant_id == Restaurant.id,
                            Inspection.inspection_date == Restaurant.latest_inspection_date,
                            Inspection.not_future(),
                        )
                    )
                    .filter(Restaurant.id.in_(page_ids))
                    .order_by(id_order)
                    .all()
                )
            else:
                rows = []
            cache.set(city_cache_key, (rows, total), timeout=300)

        return render_neighborhood(region, path_slug, city_name, rows,
                                   sort=sort, sort_dir=sort_dir, page=page, per_page=per_page, total=total)

    # 3. Try cuisine/category slug
    cuisine_types = _get_cuisine_types(region)
    cuisine_map = {ct['slug']: ct['label'] for ct in cuisine_types}
    cuisine_label = cuisine_map.get(path_slug)
    if cuisine_label:
        sort = request.args.get('sort', 'date')
        sort_dir = request.args.get('dir', _SORT_DEFAULTS.get(sort, 'desc'))
        page = max(1, int(request.args.get('page', 1) or 1))
        rows, total = _cuisine_rows(region, cuisine_label, sort=sort, sort_dir=sort_dir, page=page)
        return render_cuisine(region, path_slug, cuisine_label, rows,
                              total=total, page=page, sort=sort, sort_dir=sort_dir)

    abort(404)


@region_bp.route('/<region>/<city_slug_str>/<cuisine_slug_str>/')
def region_city_cuisine(region, city_slug_str, cuisine_slug_str):
    home_state = _home_state(region)
    cities = _city_list(region, home_state)
    city_name = next(
        (c[0] for c in cities if c[0] and _city_slug(c[0]) == city_slug_str),
        None
    )
    if not city_name:
        abort(404)

    cuisine_types = _get_cuisine_types(region)
    cuisine_map = {ct['slug']: ct['label'] for ct in cuisine_types}
    cuisine_label = cuisine_map.get(cuisine_slug_str)
    if not cuisine_label:
        abort(404)

    sort = request.args.get('sort', 'date')
    sort_dir = request.args.get('dir', _SORT_DEFAULTS.get(sort, 'desc'))
    page = max(1, int(request.args.get('page', 1) or 1))
    rows, total = _cuisine_rows(region, cuisine_label, city_name=city_name, sort=sort, sort_dir=sort_dir, page=page)
    if not rows and page == 1:
        abort(404)

    return render_cuisine(region, cuisine_slug_str, cuisine_label, rows,
                          city_name=city_name, city_slug_str=city_slug_str,
                          total=total, page=page, sort=sort, sort_dir=sort_dir)
