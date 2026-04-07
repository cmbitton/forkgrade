import re

from flask import Blueprint, Response, current_app
from sqlalchemy import func
from app.db import db, cache
from app.models.restaurant import Restaurant
from app.models.inspection import Inspection


def _cuisine_slug(label: str) -> str:
    s = label.lower()
    s = re.sub(r"[/&'\u2019,]+", '-', s)
    s = re.sub(r'\s+', '-', s)
    s = re.sub(r'[^a-z0-9-]', '', s)
    return re.sub(r'-+', '-', s).strip('-')


def _city_slug(city: str) -> str:
    c = city.lower().replace("'", '')
    c = re.sub(r'\s+', '-', c)
    return re.sub(r'[^a-z0-9-]', '', c)


sitemap_bp = Blueprint('sitemap', __name__)


def _xml_response(content):
    return Response(content, mimetype='application/xml')


@sitemap_bp.route('/sitemap.xml')
@cache.cached(timeout=3600)
def sitemap_index():
    base_url = current_app.config['BASE_URL']
    total = Restaurant.query.count()

    if total > 1000:
        # Sitemap index pointing to per-region sitemaps
        regions = (
            db.session.query(Restaurant.region)
            .group_by(Restaurant.region)
            .all()
        )
        lines = ['<?xml version="1.0" encoding="UTF-8"?>']
        lines.append('<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">')
        for (region,) in regions:
            lines.append(f'  <sitemap>')
            lines.append(f'    <loc>{base_url}/sitemap-{region}.xml</loc>')
            lines.append(f'  </sitemap>')
        lines.append('</sitemapindex>')
        return _xml_response('\n'.join(lines))
    else:
        # Single sitemap with all pages
        restaurants = Restaurant.query.all()
        lines = ['<?xml version="1.0" encoding="UTF-8"?>']
        lines.append('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">')

        # Home
        lines.append(f'  <url><loc>{base_url}/</loc><changefreq>daily</changefreq><priority>1.0</priority></url>')

        # Region index pages
        regions = set(r.region for r in restaurants)
        for region in sorted(regions):
            lines.append(f'  <url><loc>{base_url}/{region}/</loc><changefreq>daily</changefreq><priority>0.8</priority></url>')

        seen_neighborhoods = set()
        seen_cuisines = set()
        for r in restaurants:
            # Neighborhood pages
            key = (r.region, r.city_slug)
            if key not in seen_neighborhoods:
                seen_neighborhoods.add(key)
                lines.append(
                    f'  <url><loc>{base_url}/{r.region}/{r.city_slug}/</loc>'
                    f'<changefreq>weekly</changefreq><priority>0.7</priority></url>'
                )
            # Region-level cuisine pages
            if r.cuisine_type:
                ckey = (r.region, r.cuisine_type)
                if ckey not in seen_cuisines:
                    seen_cuisines.add(ckey)
                    cslug = _cuisine_slug(r.cuisine_type)
                    lines.append(
                        f'  <url><loc>{base_url}/{r.region}/{cslug}/</loc>'
                        f'<changefreq>weekly</changefreq><priority>0.7</priority></url>'
                    )

        # City+cuisine pages
        city_cuisine_pairs = (
            db.session.query(Restaurant.region, Restaurant.city, Restaurant.cuisine_type)
            .filter(Restaurant.cuisine_type.isnot(None))
            .distinct()
            .all()
        )
        for region, city, cuisine in city_cuisine_pairs:
            if not city or not cuisine:
                continue
            cs = _city_slug(city)
            cslug = _cuisine_slug(cuisine)
            lines.append(
                f'  <url><loc>{base_url}/{region}/{cs}/{cslug}/</loc>'
                f'<changefreq>weekly</changefreq><priority>0.6</priority></url>'
            )

        # Restaurant pages
        for r in restaurants:
            lines.append(
                f'  <url><loc>{base_url}/{r.region}/{r.slug}/</loc>'
                f'<changefreq>weekly</changefreq><priority>0.6</priority></url>'
            )

        lines.append('</urlset>')
        return _xml_response('\n'.join(lines))


_SITEMAP_PAGE_SIZE = 30000  # stay well under Google's 50k URL limit (page 1 also has static pages)


def _build_region_sitemap_lines(base_url, region, slugs_with_dates,
                                city_cuisine_pairs, cities, cuisines,
                                include_static=True):
    """Build URL lines for one region sitemap page."""
    lines = []
    if include_static:
        lines.append(f'  <url><loc>{base_url}/{region}/</loc><changefreq>daily</changefreq><priority>0.8</priority></url>')
        for cs in cities:
            lines.append(
                f'  <url><loc>{base_url}/{region}/{cs}/</loc>'
                f'<changefreq>weekly</changefreq><priority>0.7</priority></url>'
            )
        for cslug in cuisines:
            lines.append(
                f'  <url><loc>{base_url}/{region}/{cslug}/</loc>'
                f'<changefreq>weekly</changefreq><priority>0.7</priority></url>'
            )
        for cs, cslug in city_cuisine_pairs:
            lines.append(
                f'  <url><loc>{base_url}/{region}/{cs}/{cslug}/</loc>'
                f'<changefreq>weekly</changefreq><priority>0.6</priority></url>'
            )
    for slug, lm in slugs_with_dates:
        lm_tag = f'<lastmod>{lm}</lastmod>' if lm else ''
        lines.append(
            f'  <url><loc>{base_url}/{region}/{slug}/</loc>'
            f'{lm_tag}<changefreq>weekly</changefreq><priority>0.6</priority></url>'
        )
    return lines


@sitemap_bp.route('/sitemap-<region>.xml')
@cache.cached(timeout=3600)
def sitemap_region(region):
    base_url = current_app.config['BASE_URL']

    # Use latest_inspection_date IS NOT NULL — avoids slow correlated subquery
    rows = (
        db.session.query(Restaurant.id, Restaurant.slug,
                         Restaurant.city, Restaurant.cuisine_type,
                         Restaurant.latest_inspection_date)
        .filter_by(region=region)
        .filter(Restaurant.latest_inspection_date.isnot(None))
        .order_by(Restaurant.id)
        .all()
    )

    if not rows:
        return _xml_response('<?xml version="1.0" encoding="UTF-8"?>'
                             '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"></urlset>')

    slugs_with_dates = [(r.slug, r.latest_inspection_date.isoformat() if r.latest_inspection_date else None)
                        for r in rows]

    seen_neighborhoods, seen_cuisines = set(), set()
    cities, cuisines = [], []
    for r in rows:
        if r.city:
            cs = _city_slug(r.city)
            if cs not in seen_neighborhoods:
                seen_neighborhoods.add(cs)
                cities.append(cs)
        if r.cuisine_type and r.cuisine_type not in seen_cuisines:
            seen_cuisines.add(r.cuisine_type)
            cuisines.append(_cuisine_slug(r.cuisine_type))

    city_cuisine_pairs = [
        (_city_slug(city), _cuisine_slug(cuisine))
        for city, cuisine in (
            db.session.query(Restaurant.city, Restaurant.cuisine_type)
            .filter_by(region=region)
            .filter(Restaurant.cuisine_type.isnot(None),
                    Restaurant.city.isnot(None))
            .distinct()
            .all()
        )
        if city and cuisine
    ]

    static_count = 1 + len(cities) + len(cuisines) + len(city_cuisine_pairs)
    total = static_count + len(slugs_with_dates)

    if total <= _SITEMAP_PAGE_SIZE:
        # Single file
        lines = ['<?xml version="1.0" encoding="UTF-8"?>',
                 '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']
        lines += _build_region_sitemap_lines(base_url, region, slugs_with_dates,
                                             city_cuisine_pairs, cities, cuisines,
                                             include_static=True)
        lines.append('</urlset>')
        return _xml_response('\n'.join(lines))
    else:
        # Too large — return a sitemap index pointing to numbered pages
        n_pages = (len(slugs_with_dates) + _SITEMAP_PAGE_SIZE - 1) // _SITEMAP_PAGE_SIZE
        lines = ['<?xml version="1.0" encoding="UTF-8"?>',
                 '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']
        for page in range(1, n_pages + 1):
            lines.append(f'  <sitemap><loc>{base_url}/sitemap-{region}-{page}.xml</loc></sitemap>')
        lines.append('</sitemapindex>')
        return _xml_response('\n'.join(lines))


@sitemap_bp.route('/sitemap-<region>-<int:page>.xml')
@cache.cached(timeout=3600)
def sitemap_region_page(region, page):
    base_url = current_app.config['BASE_URL']

    rows = (
        db.session.query(Restaurant.slug, Restaurant.latest_inspection_date)
        .filter_by(region=region)
        .filter(Restaurant.latest_inspection_date.isnot(None))
        .order_by(Restaurant.id)
        .offset((page - 1) * _SITEMAP_PAGE_SIZE)
        .limit(_SITEMAP_PAGE_SIZE)
        .all()
    )
    if not rows:
        from flask import abort
        abort(404)

    slugs_with_dates = [(r.slug, r.latest_inspection_date.isoformat() if r.latest_inspection_date else None)
                        for r in rows]

    # Static pages (cities, cuisines) only on page 1
    cities, cuisines, city_cuisine_pairs = [], [], []
    if page == 1:
        seen_n, seen_c = set(), set()
        all_rows = (
            db.session.query(Restaurant.city, Restaurant.cuisine_type)
            .filter_by(region=region)
            .filter(Restaurant.latest_inspection_date.isnot(None))
            .all()
        )
        for r in all_rows:
            if r.city:
                cs = _city_slug(r.city)
                if cs not in seen_n:
                    seen_n.add(cs)
                    cities.append(cs)
            if r.cuisine_type and r.cuisine_type not in seen_c:
                seen_c.add(r.cuisine_type)
                cuisines.append(_cuisine_slug(r.cuisine_type))
        city_cuisine_pairs = [
            (_city_slug(city), _cuisine_slug(cuisine))
            for city, cuisine in (
                db.session.query(Restaurant.city, Restaurant.cuisine_type)
                .filter_by(region=region)
                .filter(Restaurant.cuisine_type.isnot(None),
                        Restaurant.city.isnot(None))
                .distinct().all()
            )
            if city and cuisine
        ]

    lines = ['<?xml version="1.0" encoding="UTF-8"?>',
             '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']
    if page == 1:
        lines.append(f'  <url><loc>{base_url}/{region}/</loc><changefreq>daily</changefreq><priority>0.8</priority></url>')
    lines += _build_region_sitemap_lines(base_url, region, slugs_with_dates,
                                         city_cuisine_pairs, cities, cuisines,
                                         include_static=(page == 1))
    lines.append('</urlset>')
    return _xml_response('\n'.join(lines))


@sitemap_bp.route('/robots.txt')
def robots_txt():
    base_url = current_app.config['BASE_URL']
    content = f"""User-agent: *
Allow: /

# Disallow paginated and sorted variants — canonical is page 1 with default sort
Disallow: /*?page=
Disallow: /*?sort=
Disallow: /*?feed=

Sitemap: {base_url}/sitemap.xml
"""
    return Response(content, mimetype='text/plain')
