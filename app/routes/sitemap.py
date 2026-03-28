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


@sitemap_bp.route('/sitemap-<region>.xml')
@cache.cached(timeout=3600)
def sitemap_region(region):
    base_url = current_app.config['BASE_URL']
    restaurants = Restaurant.query.filter_by(region=region).all()

    if not restaurants:
        return _xml_response('<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"></urlset>')

    # Build lastmod lookup: restaurant_id → latest inspection date
    lastmod_rows = (
        db.session.query(Inspection.restaurant_id, func.max(Inspection.inspection_date))
        .filter(Inspection.restaurant_id.in_([r.id for r in restaurants]))
        .group_by(Inspection.restaurant_id)
        .all()
    )
    lastmod = {rid: d.isoformat() for rid, d in lastmod_rows if d}

    lines = ['<?xml version="1.0" encoding="UTF-8"?>']
    lines.append('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">')

    # Region index
    lines.append(f'  <url><loc>{base_url}/{region}/</loc><changefreq>daily</changefreq><priority>0.8</priority></url>')

    seen_neighborhoods = set()
    seen_cuisines = set()

    for r in restaurants:
        cs = r.city_slug
        if cs not in seen_neighborhoods:
            seen_neighborhoods.add(cs)
            lines.append(
                f'  <url><loc>{base_url}/{region}/{cs}/</loc>'
                f'<changefreq>weekly</changefreq><priority>0.7</priority></url>'
            )
        if r.cuisine_type and r.cuisine_type not in seen_cuisines:
            seen_cuisines.add(r.cuisine_type)
            cslug = _cuisine_slug(r.cuisine_type)
            lines.append(
                f'  <url><loc>{base_url}/{region}/{cslug}/</loc>'
                f'<changefreq>weekly</changefreq><priority>0.7</priority></url>'
            )

    # City+cuisine pages
    city_cuisine_pairs = (
        db.session.query(Restaurant.city, Restaurant.cuisine_type)
        .filter_by(region=region)
        .filter(Restaurant.cuisine_type.isnot(None))
        .distinct()
        .all()
    )
    for city, cuisine in city_cuisine_pairs:
        if not city or not cuisine:
            continue
        cs = _city_slug(city)
        cslug = _cuisine_slug(cuisine)
        lines.append(
            f'  <url><loc>{base_url}/{region}/{cs}/{cslug}/</loc>'
            f'<changefreq>weekly</changefreq><priority>0.6</priority></url>'
        )

    # Restaurant pages with lastmod
    for r in restaurants:
        lm = lastmod.get(r.id)
        lm_tag = f'<lastmod>{lm}</lastmod>' if lm else ''
        lines.append(
            f'  <url><loc>{base_url}/{region}/{r.slug}/</loc>'
            f'{lm_tag}<changefreq>weekly</changefreq><priority>0.6</priority></url>'
        )

    lines.append('</urlset>')
    return _xml_response('\n'.join(lines))


@sitemap_bp.route('/robots.txt')
def robots_txt():
    base_url = current_app.config['BASE_URL']
    content = f"""User-agent: *
Allow: /

Sitemap: {base_url}/sitemap.xml
"""
    return Response(content, mimetype='text/plain')
