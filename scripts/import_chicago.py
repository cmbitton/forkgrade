#!/usr/bin/env python3
"""
Chicago Department of Public Health restaurant inspection importer.

Data source: Chicago Open Data SODA2 API
  https://data.cityofchicago.org/resource/4ijn-s7e5.json

Each row in the source is ONE INSPECTION with violations packed into a single
pipe-delimited string field.  This script parses those into individual Violation
records.

Modes:
  --full    Paginate through all ~200k restaurant inspections (first-time setup)
  --daily   Fetch only recent inspections (default, last 7 days)
  --days=N  Override the lookback window

Usage:
    nat-health/bin/python3 scripts/import_chicago.py           # daily refresh
    nat-health/bin/python3 scripts/import_chicago.py --full    # full import

Scoring:
  Severity is extracted from embedded labels in violation comments:
    "(PRIORITY ...)"            → critical (weight 3)
    "(PRIORITY FOUNDATION ...)" → major    (weight 2)
    No label                    → minor    (weight 1)
  risk_score = sum of weights
  score (0-100) = round(100 * exp(-risk_score * 0.05))
"""

import json
import math
import re
import sys
import time
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

SODA_URL = "https://data.cityofchicago.org/resource/4ijn-s7e5.json"
REGION = "chicago"
STATE = "IL"

# Only import restaurant-type facilities
FACILITY_TYPES = {
    "Restaurant",
    "Bakery",
    "Catering",
    "Golden Diner",
    "Liquor",
    "TAVERN",
    "Mobile Food Preparer",
    "Mobile Food Dispenser",
}

# ── Inspection type normalisation ────────────────────────────────────────────

_TYPE_MAP = {
    "canvass":        "Routine",
    "canvas":         "Routine",
    "license":        "License",
    "complaint":      "Complaint",
    "fire":           "Complaint",
    "consultation":   "Consultation",
}


def _norm_inspection_type(raw: str) -> str:
    low = raw.strip().lower()
    if "re-inspect" in low or "reinspect" in low:
        return "Reinspection"
    for key, val in _TYPE_MAP.items():
        if key in low:
            return val
    return raw.strip().title() or "Routine"


# ── Violation parsing ────────────────────────────────────────────────────────

_PRIORITY_FOUNDATION_RE = re.compile(r'\bPRIORITY\s+FOUNDATION\b', re.IGNORECASE)
_PRIORITY_RE = re.compile(r'\bPRIORITY\b', re.IGNORECASE)
_VIOL_NUM_RE = re.compile(r'^(\d+)\.\s*')


def parse_violations(raw: str | None) -> list[dict]:
    """Parse Chicago's pipe-delimited violation string into structured dicts."""
    if not raw:
        return []
    violations = []
    for chunk in raw.split('|'):
        chunk = chunk.strip()
        if not chunk:
            continue

        # Extract violation number
        m = _VIOL_NUM_RE.match(chunk)
        code = m.group(1) if m else None
        text = chunk[m.end():] if m else chunk

        # Split description from comments
        parts = text.split(' - Comments:', 1)
        description = parts[0].strip()
        comments = parts[1].strip() if len(parts) > 1 else ''

        # Determine severity from embedded labels in the full text
        if _PRIORITY_FOUNDATION_RE.search(chunk):
            severity = 'major'
        elif _PRIORITY_RE.search(chunk):
            severity = 'critical'
        else:
            severity = 'minor'

        violations.append({
            'code': code,
            'description': description.capitalize() if description else None,
            'comments': comments,
            'severity': severity,
        })
    return violations


# ── Scoring (same formula as all other regions) ─────────────────────────────

_SEVERITY_WEIGHT = {'critical': 3.0, 'major': 2.0, 'minor': 1.0}


def compute_score(violations: list[dict]) -> tuple[float, int]:
    risk = sum(_SEVERITY_WEIGHT.get(v['severity'], 1.0) for v in violations)
    score = round(100 * math.exp(-risk * 0.05))
    return risk, score


# ── Result mapping ───────────────────────────────────────────────────────────

_RESULT_MAP = {
    'Pass':               'Pass',
    'Pass w/ Conditions':  'Conditional Pass',
    'Fail':               'Fail',
    'No Entry':           None,
    'Not Ready':          None,
    'Out of Business':    None,
    'Business Not Located': None,
}


def map_result(raw: str) -> str | None:
    return _RESULT_MAP.get(raw.strip(), raw.strip() or None)


# ── Slug helpers ─────────────────────────────────────────────────────────────

def make_slug(name: str) -> str:
    s = re.sub(r"['\u2019]", '', name.lower().strip())
    s = re.sub(r'[\s_/&]+', '-', s)
    s = re.sub(r'[^a-z0-9-]', '', s)
    s = re.sub(r'-+', '-', s).strip('-')
    return f"{s}-chicago"


def unique_slug(base: str, seen: set) -> str:
    slug, n = base, 2
    while slug in seen:
        slug = f"{base}-{n}"
        n += 1
    seen.add(slug)
    return slug


# ── Date helpers ─────────────────────────────────────────────────────────────

def parse_date(s: str) -> date | None:
    if not s:
        return None
    for fmt in ('%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S', '%m/%d/%Y'):
        try:
            return datetime.strptime(s.strip(), fmt).date()
        except ValueError:
            pass
    return None


def _float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# ── SODA API fetch ───────────────────────────────────────────────────────────

def fetch_inspections(since: date | None = None, limit: int = 1000):
    """Paginate through SODA2 API. If since is None, fetch everything."""
    all_rows = []
    offset = 0

    where_parts = ["facility_type = 'Restaurant'"]
    if since:
        since_str = since.strftime('%Y-%m-%dT00:00:00.000')
        where_parts.append(f"inspection_date >= '{since_str}'")
    where = ' AND '.join(where_parts)

    label = f"since {since}" if since else "(full)"
    print(f"Fetching Chicago inspections {label}...")

    while True:
        params = urllib.parse.urlencode({
            '$limit':  str(limit),
            '$offset': str(offset),
            '$where':  where,
            '$order':  'inspection_date DESC',
        })
        url = f"{SODA_URL}?{params}"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})

        for attempt in range(3):
            try:
                with urllib.request.urlopen(req, timeout=30) as r:
                    batch = json.loads(r.read())
                break
            except Exception as e:
                if attempt < 2:
                    wait = 2 ** (attempt + 1)
                    print(f"  Retry {attempt+1} after error: {e} (waiting {wait}s)")
                    time.sleep(wait)
                else:
                    raise

        if not batch:
            break
        all_rows.extend(batch)
        print(f"  Fetched {len(all_rows):,} rows...", end='\r')
        if len(batch) < limit:
            break
        offset += limit
        time.sleep(0.2)

    print(f"\n  {len(all_rows):,} rows total.")
    return all_rows


# ── Database write ───────────────────────────────────────────────────────────

def write_to_db(rows, app, db, Restaurant, Inspection, Violation):
    with app.app_context():
        # Load existing Chicago restaurants keyed by source_id (license_)
        existing = {
            r.source_id: r
            for r in Restaurant.query.filter_by(region=REGION)
                                     .filter(Restaurant.source_id.isnot(None))
                                     .all()
        }
        seen_slugs = {r.slug for r in existing.values()}
        other_slugs = {
            r.slug for r in Restaurant.query.filter(
                Restaurant.region != REGION
            ).with_entities(Restaurant.slug).all()
        }
        seen_slugs |= other_slugs

        # Pre-load existing Chicago inspection source_ids for fast dedup
        existing_insp_ids = set(
            sid for (sid,) in
            db.session.query(Inspection.source_id)
                      .filter(Inspection.region == REGION,
                              Inspection.source_id.isnot(None))
                      .all()
        )

        new_r = new_i = skipped = 0
        total = len(rows)
        # Pending new restaurants that need a flush to get their IDs
        pending_restaurants = []

        for idx, row in enumerate(rows):
            license_num = (row.get('license_') or '').strip()
            name = (row.get('dba_name') or row.get('aka_name') or '').strip()
            if not name or not license_num:
                skipped += 1
                continue

            result = map_result(row.get('results', ''))
            if result is None:
                skipped += 1
                continue

            insp_date = parse_date(row.get('inspection_date', ''))
            if not insp_date or insp_date.year < 1990:
                skipped += 1
                continue
            # Reject future dates (allow 1-day buffer)
            if insp_date > date.today() + timedelta(days=1):
                skipped += 1
                continue

            # Get or create restaurant
            if license_num in existing:
                restaurant = existing[license_num]
            else:
                slug = unique_slug(make_slug(name), seen_slugs)
                restaurant = Restaurant(
                    source_id=license_num,
                    name=name,
                    slug=slug,
                    address=(row.get('address') or '').strip().title(),
                    city='Chicago',
                    state=STATE,
                    zip=(row.get('zip') or '').strip(),
                    latitude=_float(row.get('latitude')),
                    longitude=_float(row.get('longitude')),
                    region=REGION,
                )
                db.session.add(restaurant)
                existing[license_num] = restaurant
                pending_restaurants.append(restaurant)
                new_r += 1

            # Duplicate guard by inspection source_id
            insp_source = (row.get('inspection_id') or '').strip()
            if insp_source and insp_source in existing_insp_ids:
                continue

            # Flush pending restaurants to get their IDs
            if pending_restaurants:
                db.session.flush()
                pending_restaurants.clear()

            # Parse violations and compute score
            violations = parse_violations(row.get('violations'))
            risk, score = compute_score(violations)

            insp = Inspection(
                restaurant_id=restaurant.id,
                inspection_date=insp_date,
                source_id=insp_source or None,
                score=score,
                risk_score=risk,
                result=result,
                inspection_type=_norm_inspection_type(
                    row.get('inspection_type', '')
                ),
                region=REGION,
            )
            db.session.add(insp)
            db.session.flush()
            new_i += 1
            if insp_source:
                existing_insp_ids.add(insp_source)

            for v in violations:
                db.session.add(Violation(
                    inspection_id=insp.id,
                    violation_code=v['code'],
                    description=v['description'],
                    inspector_notes=v['comments'] or None,
                    severity=v['severity'],
                    corrected_on_site=False,
                ))

            # Update latest date
            prev = restaurant.latest_inspection_date
            if prev is None or insp_date > prev:
                restaurant.latest_inspection_date = insp_date
                restaurant.ai_summary = None

            # Commit every 1000 inspections
            if new_i % 1000 == 0 and new_i > 0:
                db.session.commit()
                print(f"  {idx+1:,}/{total:,} rows | {new_r:,} restaurants, {new_i:,} inspections", flush=True)

        db.session.commit()
        print(f"  {total:,}/{total:,} rows | {new_r:,} restaurants, {new_i:,} inspections", flush=True)
        return new_r, new_i, skipped


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    full_mode = '--full' in sys.argv

    days = 7
    for arg in sys.argv[1:]:
        if arg.startswith('--days='):
            days = int(arg.split('=', 1)[1])

    from app import create_app
    from app.db import db
    from app.models.restaurant import Restaurant
    from app.models.inspection import Inspection
    from app.models.violation import Violation

    if full_mode:
        rows = fetch_inspections(since=None)
    else:
        since = date.today() - timedelta(days=days)
        rows = fetch_inspections(since=since)

    if not rows:
        print("No rows to process.")
        return

    app = create_app()
    print(f"Processing {len(rows):,} inspection rows...")
    new_r, new_i, skipped = write_to_db(
        rows, app, db, Restaurant, Inspection, Violation
    )

    print(f"\nDone.")
    print(f"  {new_r:,} new restaurants")
    print(f"  {new_i:,} new inspections")
    print(f"  {skipped:,} rows skipped")


if __name__ == "__main__":
    main()
