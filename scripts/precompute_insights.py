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

try:
    from scripts.import_florida import _FL_ITEM_DESC
except Exception:
    _FL_ITEM_DESC = {}

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

    # Code-based overrides — checked first, by violation_code
    _CODE_OVERRIDES = {
        # Houston ordinance codes
        "20-20(c)":             "Required permit or inspection report information not displayed",
        "20-21.23(a)":          "Walls, ceilings, doors, or windows not maintained in good repair",
        "20-21.10(a)":          "Equipment or utensils not made of safe, corrosion-resistant materials",
        "20-21.11(a)(07)":      "Non-food contact surfaces not cleaned frequently enough",
        "20-21.23(g)":          "Light fixtures, vent covers, or wall-mounted equipment not kept clean",
        "20-21.22(a)":          "Floors not clean in food preparation, storage, or warewashing areas",
        "20-21.21(b)":          "Outside openings not protected against entry of insects or pests",
        "20-21.20(a)(03)":      "Waste containers or dumpsters not properly maintained or covered",
        "20-21.03(b)(10)b":     "Ready-to-eat food not properly date-marked for time/temperature safety",
        "20-21.10(m)":          "Plumbing fixtures not properly installed or maintained",
        "20-21.12(g)":          "Sanitizer test kit not available or chemical sanitizer at wrong concentration",
        "21-244(a)":            "Required no-smoking signs not posted",
        "20-21.11(a)(08)c[04]": "Dry food contact surfaces not cleaned at required frequency",
    }

    # Rephrase standard/requirement text as violation descriptions
    _DESC_OVERRIDES = {
        "Non-food contact surfaces clean":
            "Non-food contact surfaces not clean",
        "Food & non-food contact surfaces cleanable, properly designed, constructed, & used":
            "Food or non-food contact surfaces not properly designed, constructed, or maintained",
        "Physical facilities installed, maintained, & clean":
            "Physical facilities not properly installed, maintained, or clean",
        "Toilet facilities: properly constructed, supplied, & cleaned":
            "Toilet facilities not properly constructed, supplied, or cleaned",
        "Contamination prevented during food preparation, storage & display":
            "Food not protected from contamination during preparation, storage, or display",
        "Insects, rodents & animals not present; no unauthorized persons":
            "Facility not protected against entry of insects, rodents, or animals",
        "Adequate ventilation & lighting; designated areas used":
            "Inadequate ventilation or lighting; designated areas not properly used",
        "Proper date marking & disposition":
            "Ready-to-eat food not properly date-marked or disposed",
        "Adequate handwashing facilities supplied & accessible":
            "Handwashing facilities not adequately supplied or accessible",
        "Wiping cloths: properly used & stored":
            "Wiping cloths not properly used or stored in sanitizer",
        "Certification by accredited program, compliance with Code, or correct responses":
            "No certified food protection manager or food safety certification",
        "Food separated and protected":
            "Food not properly separated or protected from contamination",
        "Food-contact surfaces: cleaned and sanitized":
            "Food-contact surfaces not properly cleaned and sanitized",
        "Food-contact surfaces; cleaned & sanitized":
            "Food-contact surfaces not properly cleaned and sanitized",
        "Proper cold holding temperatures":
            "Cold food not held at 41°F or below",
        "Proper cooling methods used; adequate equipment for temperature control":
            "Improper cooling methods or inadequate temperature control equipment",
        "In-use utensils: properly stored":
            "In-use utensils not properly stored between uses",
        "Certified food protection manager":
            "No certified food protection manager on duty",
        # Houston — terse section titles and verbose regulatory text
        "Hand-washing sink supplies":
            "No soap, paper towels, or hand dryer at handwashing sink",
        "Plumbing fixture installation":
            "Plumbing fixtures not properly installed or maintained",
        "Hand Drying Provision":
            "No paper towels or hand dryer at handwashing sink",
    }

    _sev_prefix = re.compile(r'^(High Priority|Intermediate|Basic)\s*[-:]\s*', re.IGNORECASE)
    _instance_flags = re.compile(r'\s*\*\*(Corrected On-Site|Repeat Violation|Warning)\*\*', re.IGNORECASE)
    _leading_code_re = re.compile(r'^[\d]+-[\d.]+(?:\([^)]*\))?\s+')

    def _clean_desc(raw: str) -> tuple[str, str]:
        """Clean description, return (desc, embedded_fda_code).
        - Strips severity prefix, instance flags, and leading code refs
        - If first sentence is a terse title (<25 chars), uses the rest
        """
        embedded = ''
        s = _sev_prefix.sub('', raw).strip()
        s = _instance_flags.sub('', s).strip()
        # Strip leading FDA code ref (e.g. "8-304.11(k) the permit holder...")
        m = _leading_code_re.match(s)
        if m:
            embedded = m.group().strip().rstrip('.-– ')
            s = s[m.end():].strip()
        # Handle "Title. details..." — if title is terse, skip to details
        period_idx = s.find('. ')
        if period_idx != -1:
            first = s[:period_idx].strip()
            rest = s[period_idx + 2:].strip()
            if len(first) < 25 and rest:
                s = rest  # skip terse section title, use details
            else:
                s = first  # keep first sentence
        s = s.strip().rstrip('.')
        if s:
            s = s[0].upper() + s[1:]
        return s or raw, embedded

    def _fda_lookup(code: str) -> str:
        """Try exact, then strip all subsection parentheses progressively."""
        if not code:
            return ''
        if code in _FDA_DESC:
            return _FDA_DESC[code]
        # Strip trailing subsections one at a time: 4-601.11(A)(2) → 4-601.11(A) → 4-601.11
        cur = code
        while True:
            stripped = re.sub(r'\([^)]*\)$', '', cur).strip()
            if stripped == cur:
                break
            cur = stripped
            if cur in _FDA_DESC:
                return _FDA_DESC[cur]
        return ''

    def _fl_lookup(code: str) -> str:
        """Look up FL-XX item codes."""
        if not code.startswith('FL-'):
            return ''
        try:
            return _FL_ITEM_DESC.get(int(code[3:]), '')
        except (ValueError, TypeError):
            return ''

    _STOP = frozenset('a an the of or and not no is are in at to for from by on with other'.split())

    def _desc_words(desc: str) -> frozenset:
        return frozenset(
            w for w in re.sub(r'[^a-z\s]', '', desc.lower()).split()
            if w not in _STOP and len(w) > 2
        )

    def _similar_to_any(desc: str, seen: list) -> bool:
        words = _desc_words(desc)
        if len(words) < 2:
            return False
        for sw in seen:
            if not sw:
                continue
            overlap = len(words & sw)
            if overlap >= 2 and overlap / min(len(words), len(sw)) >= 0.5:
                return True
        return False

    seen_word_sets: list = []
    top_violations = []
    for code, total_cnt in _ranked:
        # 1. Code-based override (highest priority)
        if code in _CODE_OVERRIDES:
            desc = _CODE_OVERRIDES[code]
        else:
            raw_desc = desc_map.get(code, '')
            desc, embedded = _clean_desc(raw_desc) if raw_desc else ('', '')
            # 2. Description text overrides (Philly/RI requirement→violation rephrasing)
            desc = _DESC_OVERRIDES.get(desc, desc)
            # 3. FL item lookup or FDA code lookup
            better = _fl_lookup(code) or _fda_lookup(code) or _fda_lookup(embedded)
            if better and (not desc or len(desc) < 50 or len(desc) > 100 or desc == code):
                desc = better
            # 4. Last resort: cap long descriptions at word boundary
            if len(desc) > 100:
                idx = desc.rfind(' ', 0, 100)
                if idx > 40:
                    desc = desc[:idx].rstrip(' ,;/')
        if not desc or len(desc) < 5 or desc == code:
            continue
        if _similar_to_any(desc, seen_word_sets):
            continue
        seen_word_sets.append(_desc_words(desc))
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
                         func.count(Restaurant.id).label('cnt'))
        .select_from(Restaurant)
        .join(Inspection, db.and_(
            Inspection.restaurant_id == Restaurant.id,
            Inspection.inspection_date == Restaurant.latest_inspection_date,
        ))
        .filter(Restaurant.region == region,
                Restaurant.latest_inspection_date.isnot(None),
                Inspection.score.isnot(None),
                Inspection.score >= 0)
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
    # Full per-city dict keyed by slug. summary.py looks this up so every
    # facility gets a real city comparison instead of falling back to the
    # region-wide average just because their city wasn't in the top/bottom 10.
    city_stats = {
        c['city_slug']: {
            'avg_score': c['avg_score'],
            'pct_high':  c['pct_high'],
            'total':     c['total'],
        }
        for c in city_data if c.get('city_slug')
    }
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
        'city_stats':         city_stats,
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
