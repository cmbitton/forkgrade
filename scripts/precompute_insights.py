"""Precompute insights data for one or all regions and store in region_stats table.

Usage:
    python scripts/precompute_insights.py              # all regions
    python scripts/precompute_insights.py florida      # single region
"""
import os
import re
import sys
import time
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from scripts.fda_codes import CODE_DESCRIPTION as _FDA_DESC
except Exception:
    _FDA_DESC = {}

from app import create_app
from app.db import db
from app.models.restaurant import Restaurant
from app.models.inspection import Inspection
from app.models.violation import Violation
from app.models.region_stats import RegionStats
from app.utils import REGION_INFO
from app.zip_neighborhoods import get_neighborhood_name
from sqlalchemy import func

app = create_app()


def _city_slug(city: str) -> str:
    c = city.lower().replace("'", '')
    c = re.sub(r'\s+', '-', c)
    return re.sub(r'[^a-z0-9-]', '', c)


def compute_region(region: str) -> dict | None:
    """Run all insights queries for a region. Returns the data dict or None if no data."""
    _t = time.monotonic

    t0 = _t()
    total_locations = (
        db.session.query(func.count(Restaurant.id))
        .filter(Restaurant.region == region,
                Restaurant.latest_inspection_date.isnot(None))
        .scalar() or 0
    )
    if total_locations == 0:
        print(f'  [{region}] no data — skipping')
        return None

    total_inspections = (
        db.session.query(func.count(Inspection.id))
        .join(Restaurant, Inspection.restaurant_id == Restaurant.id)
        .filter(Restaurant.region == region, Inspection.not_future())
        .scalar() or 0
    )
    print(f'  [{region}] counts: {(_t()-t0)*1000:.0f}ms')

    # Violations: aggregate on short columns, then fetch descriptions for top 10
    t0 = _t()
    code_counts = (
        db.session.query(
            Violation.violation_code,
            Violation.severity,
            func.count(Violation.id).label('cnt'),
        )
        .join(Inspection, Violation.inspection_id == Inspection.id)
        .filter(Inspection.region == region)
        .group_by(Violation.violation_code, Violation.severity)
        .all()
    )
    total_violations = sum(int(r.cnt) for r in code_counts)
    _sev_counts: dict = {}
    for r in code_counts:
        sev = r.severity or 'minor'
        _sev_counts[sev] = _sev_counts.get(sev, 0) + int(r.cnt)
    severity_counts = {
        'critical': _sev_counts.get('critical', 0),
        'major':    _sev_counts.get('major', 0),
        'minor':    _sev_counts.get('minor', 0),
    }
    print(f'  [{region}] violation_counts: {(_t()-t0)*1000:.0f}ms')

    # ── Merge counts across severities per code ─────────────────────────────
    t0 = _t()
    _code_totals: dict[str, int] = {}
    _code_sev: dict[str, str] = {}
    _code_sev_cnt: dict[str, int] = {}
    for r in code_counts:
        code = r.violation_code or ''
        if not code:
            continue
        cnt = int(r.cnt)
        _code_totals[code] = _code_totals.get(code, 0) + cnt
        if cnt > _code_sev_cnt.get(code, 0):
            _code_sev[code] = r.severity or 'minor'
            _code_sev_cnt[code] = cnt

    _ranked = sorted(_code_totals.items(), key=lambda x: x[1], reverse=True)[:30]
    _code_list = [code for code, _ in _ranked]

    # Fetch descriptions — filtered by region, no empty strings
    desc_rows = (
        db.session.query(Violation.violation_code, Violation.description)
        .join(Inspection, Violation.inspection_id == Inspection.id)
        .filter(
            Inspection.region == region,
            Violation.violation_code.in_(_code_list),
            Violation.description.isnot(None),
            Violation.description != '',
        )
        .distinct(Violation.violation_code)
        .all()
    )
    desc_map = {r.violation_code: r.description for r in desc_rows}

    _sev_prefix = re.compile(r'^(High Priority|Intermediate|Basic)\s*[-:]\s*', re.IGNORECASE)
    _instance_flags = re.compile(r'\s*\*\*(Corrected On-Site|Repeat Violation|Warning)\*\*', re.IGNORECASE)

    def _clean_desc(raw: str) -> str:
        """Strip severity prefix and inspection-instance notes, keep category text only."""
        s = _sev_prefix.sub('', raw).strip()
        period_idx = s.find('. ')
        if period_idx != -1:
            s = s[:period_idx + 1]
        s = _instance_flags.sub('', s).strip().rstrip('.')
        return s or raw

    def _fda_lookup(code: str) -> str:
        """Try exact match, then strip subsection parentheses."""
        if code in _FDA_DESC:
            return _FDA_DESC[code]
        base = re.sub(r'\([^)]+\)$', '', code).strip()
        if base != code and base in _FDA_DESC:
            return _FDA_DESC[base]
        return ''

    seen_descs: set = set()
    top_violations = []
    for code, total_cnt in _ranked:
        raw_desc = desc_map.get(code, '')
        desc = _clean_desc(raw_desc) if raw_desc else ''
        # If description is missing or still looks like a raw code, try FDA lookup
        if not desc or len(desc) < 5 or desc == code:
            desc = _fda_lookup(code)
        elif len(desc) < 40:
            fda = _fda_lookup(code)
            if fda:
                desc = fda
        if not desc or desc == code:
            continue  # skip entries we can't describe meaningfully
        if desc in seen_descs:
            continue
        seen_descs.add(desc)
        top_violations.append({
            'description': desc,
            'severity':    _code_sev.get(code, 'minor'),
            'count':       total_cnt,
            'pct':         round(total_cnt / total_inspections * 100, 1)
                           if total_inspections > 0 else 0.0,
        })
        if len(top_violations) == 10:
            break
    print(f'  [{region}] violation_descriptions: {(_t()-t0)*1000:.0f}ms')

    t0 = _t()
    score_row = (
        db.session.query(
            func.avg(Inspection.score),
            func.min(Inspection.inspection_date),
            func.max(Inspection.inspection_date),
        )
        .join(Restaurant, Inspection.restaurant_id == Restaurant.id)
        .filter(Restaurant.region == region,
                Inspection.score.isnot(None),
                Inspection.not_future())
        .first()
    )
    avg_score  = round(float(score_row[0]), 1) if score_row and score_row[0] else None
    date_first = score_row[1].isoformat() if score_row and score_row[1] else None
    date_last  = score_row[2].isoformat() if score_row and score_row[2] else None

    median_score = (
        db.session.query(
            func.percentile_cont(0.5).within_group(Inspection.score.asc())
        )
        .select_from(Inspection)
        .join(Restaurant, Inspection.restaurant_id == Restaurant.id)
        .filter(Restaurant.region == region,
                Inspection.score.isnot(None),
                Inspection.not_future())
        .scalar()
    )
    median_score = round(float(median_score), 1) if median_score is not None else None
    print(f'  [{region}] score_stats: {(_t()-t0)*1000:.0f}ms')

    t0 = _t()
    tier_expr = db.case(
        (Inspection.score >= 75, 'low'),
        (Inspection.score >= 55, 'medium'),
        else_='high',
    )
    tier_rows = (
        db.session.query(tier_expr.label('tier'),
                         func.count(Restaurant.id).label('cnt'))
        .select_from(Restaurant)
        .join(Inspection, db.and_(
            Inspection.restaurant_id == Restaurant.id,
            Inspection.inspection_date == Restaurant.latest_inspection_date,
        ))
        .filter(Restaurant.region == region,
                Restaurant.latest_inspection_date.isnot(None),
                Inspection.score.isnot(None))
        .group_by(tier_expr)
        .all()
    )
    tier_counts = {'low': 0, 'medium': 0, 'high': 0}
    for tier, cnt in tier_rows:
        if tier in tier_counts:
            tier_counts[tier] = int(cnt)
    tier_total = sum(tier_counts.values())
    tier_pcts = {
        k: round(v / tier_total * 100, 1) if tier_total > 0 else 0.0
        for k, v in tier_counts.items()
    }

    _BUCKETS = [(0, '0–24'), (25, '25–54'), (55, '55–64'), (65, '65–74'), (75, '75–84'), (85, '85–100')]
    bucket_expr = db.case(
        (Inspection.score < 25, 0),
        (Inspection.score < 55, 25),
        (Inspection.score < 65, 55),
        (Inspection.score < 75, 65),
        (Inspection.score < 85, 75),
        else_=85,
    )
    hist_rows = (
        db.session.query(bucket_expr.label('bucket'),
                         func.count(Inspection.id).label('cnt'))
        .join(Restaurant, Inspection.restaurant_id == Restaurant.id)
        .filter(Restaurant.region == region,
                Inspection.score.isnot(None),
                Inspection.score >= 0,
                Inspection.not_future())
        .group_by(bucket_expr)
        .order_by(bucket_expr)
        .all()
    )
    hist_map = {int(b): int(c) for b, c in hist_rows if b is not None}
    score_histogram = [
        {'label': label, 'start': start, 'count': hist_map.get(start, 0)}
        for start, label in _BUCKETS
    ]
    print(f'  [{region}] tiers+histogram: {(_t()-t0)*1000:.0f}ms')

    t0 = _t()
    cutoff_date = date.today() - timedelta(days=730)
    month_expr  = func.to_char(Inspection.inspection_date, 'YYYY-MM')
    trend_rows  = (
        db.session.query(
            month_expr.label('month'),
            func.count(Inspection.id).label('cnt'),
            func.avg(Inspection.score).label('avg_score'),
        )
        .filter(Inspection.inspection_date >= cutoff_date,
                Inspection.not_future())
        .join(Restaurant, Inspection.restaurant_id == Restaurant.id)
        .filter(Restaurant.region == region)
        .group_by(month_expr)
        .order_by(month_expr)
        .all()
    )
    monthly_trends = [
        {
            'month':     row.month,
            'count':     int(row.cnt),
            'avg_score': round(float(row.avg_score), 1) if row.avg_score else None,
        }
        for row in trend_rows
    ]
    print(f'  [{region}] monthly_trends: {(_t()-t0)*1000:.0f}ms')

    t0 = _t()
    city_rows = (
        db.session.query(
            Restaurant.city,
            func.count(Restaurant.id).label('total'),
            func.sum(db.case((Inspection.score < 55, 1), else_=0)).label('high_risk'),
            func.avg(Inspection.score).label('avg_score'),
        )
        .join(Inspection, db.and_(
            Inspection.restaurant_id == Restaurant.id,
            Inspection.inspection_date == Restaurant.latest_inspection_date,
        ))
        .filter(Restaurant.region == region,
                Restaurant.latest_inspection_date.isnot(None),
                Restaurant.city.isnot(None),
                Restaurant.city != '',
                Restaurant.city != '0',
                Inspection.score.isnot(None))
        .group_by(Restaurant.city)
        .having(func.count(Restaurant.id) >= 10)
        .all()
    )
    city_data = [
        {
            'city':      row.city,
            'city_slug': _city_slug(row.city),
            'total':     int(row.total),
            'high_risk': int(row.high_risk or 0),
            'pct_high':  round(int(row.high_risk or 0) / int(row.total) * 100, 1),
            'avg_score': round(float(row.avg_score), 1) if row.avg_score else None,
        }
        for row in city_rows
    ]

    neighborhood_by_zip = False
    if len(city_data) < 5:
        zip_rows = (
            db.session.query(
                Restaurant.zip,
                func.count(Restaurant.id).label('total'),
                func.sum(db.case((Inspection.score < 55, 1), else_=0)).label('high_risk'),
                func.avg(Inspection.score).label('avg_score'),
            )
            .join(Inspection, db.and_(
                Inspection.restaurant_id == Restaurant.id,
                Inspection.inspection_date == Restaurant.latest_inspection_date,
            ))
            .filter(Restaurant.region == region,
                    Restaurant.latest_inspection_date.isnot(None),
                    Restaurant.zip.isnot(None),
                    Restaurant.zip != '',
                    Inspection.score.isnot(None))
            .group_by(Restaurant.zip)
            .having(func.count(Restaurant.id) >= 10)
            .all()
        )
        if len(zip_rows) >= 5:
            city_data = [
                {
                    'city':      get_neighborhood_name(region, row.zip),
                    'zip':       row.zip,
                    'city_slug': '',
                    'total':     int(row.total),
                    'high_risk': int(row.high_risk or 0),
                    'pct_high':  round(int(row.high_risk or 0) / int(row.total) * 100, 1),
                    'avg_score': round(float(row.avg_score), 1) if row.avg_score else None,
                }
                for row in zip_rows
            ]
            neighborhood_by_zip = True
        else:
            city_data = []

    worst_cities = sorted(city_data, key=lambda x: x['pct_high'], reverse=True)[:10]
    best_cities  = sorted(
        [c for c in city_data if c['avg_score'] is not None],
        key=lambda x: x['avg_score'], reverse=True
    )[:10]
    print(f'  [{region}] neighborhood: {(_t()-t0)*1000:.0f}ms')

    t0 = _t()
    cuisine_rows = (
        db.session.query(
            Restaurant.cuisine_type,
            func.count(Restaurant.id).label('total'),
            func.avg(Inspection.score).label('avg_score'),
            func.sum(db.case((Inspection.score < 55, 1), else_=0)).label('high_risk'),
        )
        .join(Inspection, db.and_(
            Inspection.restaurant_id == Restaurant.id,
            Inspection.inspection_date == Restaurant.latest_inspection_date,
        ))
        .filter(Restaurant.region == region,
                Restaurant.cuisine_type.isnot(None),
                Inspection.score.isnot(None))
        .group_by(Restaurant.cuisine_type)
        .having(func.count(Restaurant.id) >= 50)
        .order_by(func.avg(Inspection.score).asc())
        .limit(10)
        .all()
    )
    cuisine_risk = [
        {
            'cuisine':   row.cuisine_type,
            'total':     int(row.total),
            'avg_score': round(float(row.avg_score), 1) if row.avg_score else None,
            'pct_high':  round(int(row.high_risk or 0) / int(row.total) * 100, 1),
        }
        for row in cuisine_rows
    ]
    print(f'  [{region}] cuisine: {(_t()-t0)*1000:.0f}ms')

    return {
        'total_locations':    total_locations,
        'total_inspections':  total_inspections,
        'total_violations':   total_violations,
        'avg_score':          avg_score,
        'median_score':       median_score,
        'date_first':         date_first,
        'date_last':          date_last,
        'tier_counts':        tier_counts,
        'tier_pcts':          tier_pcts,
        'score_histogram':    score_histogram,
        'top_violations':     top_violations,
        'severity_counts':    severity_counts,
        'monthly_trends':     monthly_trends,
        'worst_cities':       worst_cities,
        'best_cities':        best_cities,
        'neighborhood_by_zip': neighborhood_by_zip,
        'cuisine_risk':       cuisine_risk,
    }


def upsert(region: str, data: dict) -> None:
    stats = db.session.get(RegionStats, region)
    if stats:
        stats.data = data
        stats.updated_at = db.func.now()
    else:
        db.session.add(RegionStats(region=region, data=data))
    db.session.commit()
    print(f'  [{region}] saved to region_stats')


if __name__ == '__main__':
    regions = sys.argv[1:] or list(REGION_INFO.keys())

    with app.app_context():
        for region in regions:
            wall = time.monotonic()
            print(f'Computing insights for {region}...')
            data = compute_region(region)
            if data:
                upsert(region, data)
            print(f'  [{region}] total: {(time.monotonic()-wall)*1000:.0f}ms\n')
