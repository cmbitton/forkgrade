#!/usr/bin/env python3
"""
Boston Inspectional Services restaurant inspection importer.

Data source: Boston Open Data CKAN API
  https://data.boston.gov/api/3/action/datastore_search?resource_id=4582bec6-2b4f-4f9e-bc55-cbaa73117f4c

Each row in the source is ONE VIOLATION.  This script groups by
(licenseno + inspection date) to build one Inspection record per visit,
with Violation rows attached.

Modes:
  --full    Paginate through all rows (first-time setup)
  --daily   Fetch only recent inspections (default, last 7 days)
  --days=N  Override the lookback window

Usage:
    nat-health/bin/python3 scripts/import_boston.py           # daily refresh
    nat-health/bin/python3 scripts/import_boston.py --full    # full import

Scoring:
  viol_level *** → critical (weight 3)
  viol_level **  → major    (weight 2)
  viol_level *   → minor    (weight 1)
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

RESOURCE_ID = "4582bec6-2b4f-4f9e-bc55-cbaa73117f4c"
SEARCH_URL = "https://data.boston.gov/api/3/action/datastore_search"
SQL_URL = "https://data.boston.gov/api/3/action/datastore_search_sql"
REGION = "boston"
STATE = "MA"

# ── Severity mapping ────────────────────────────────────────────────────────

SEVERITY_MAP = {
    '***': 'critical',
    '**':  'major',
    '*':   'minor',
}

# ── Result mapping ──────────────────────────────────────────────────────────

RESULT_MAP = {
    'HE_Pass':    'Pass',
    'HE_Fail':    'Fail',
    'HE_FailExt': 'Conditional Pass',
    'HE_Filed':   'Conditional Pass',
    'HE_TSOP':    'Conditional Pass',
    'HE_FAILNOR': 'Fail',
    'Pass':       'Pass',
    'Fail':       'Fail',
    'Failed':     'Fail',
    'PassViol':   'Conditional Pass',
    'NoViol':     'Pass',
}
# These are skipped: HE_Hearing, HE_NotReq, HE_VolClos, HE_OutBus,
#                     HE_Closure, HE_Misc, HE_Hold, DATAERR, Closed

# License categories to import
INCLUDE_CATS = {'FS', 'FT', 'MFW', 'RF'}


# ── Date / location helpers ─────────────────────────────────────────────────

_LOC_RE = re.compile(r'\(\s*(-?[\d.]+)\s*,\s*(-?[\d.]+)\s*\)')


def parse_date(s: str) -> date | None:
    if not s:
        return None
    for fmt in ('%Y-%m-%d %H:%M:%S+00', '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d'):
        try:
            return datetime.strptime(s.strip()[:19], fmt[:fmt.index('+')]
                                     if '+' in fmt else fmt).date()
        except ValueError:
            pass
    # Fallback: just grab the date prefix
    try:
        return datetime.strptime(s.strip()[:10], '%Y-%m-%d').date()
    except ValueError:
        return None


def parse_location(s: str | None) -> tuple[float | None, float | None]:
    if not s:
        return None, None
    m = _LOC_RE.search(s)
    if m:
        try:
            return float(m.group(1)), float(m.group(2))
        except ValueError:
            pass
    return None, None


# ── Scoring (same formula as all regions) ────────────────────────────────────

_SEV_WEIGHT = {'critical': 3.0, 'major': 2.0, 'minor': 1.0}


def compute_score(violations: list[dict]) -> tuple[float, int]:
    risk = sum(_SEV_WEIGHT.get(v['severity'], 1.0) for v in violations)
    score = round(100 * math.exp(-risk * 0.05))
    return risk, score


# ── Slug helpers ─────────────────────────────────────────────────────────────

def make_slug(name: str, city: str) -> str:
    s = re.sub(r"['\u2019]", '', name.lower().strip())
    s = re.sub(r'[\s_/&]+', '-', s)
    s = re.sub(r'[^a-z0-9-]', '', s)
    s = re.sub(r'-+', '-', s).strip('-')
    c = re.sub(r'[^a-z0-9-]', '', city.lower().replace(' ', '-'))
    return f"{s}-{c}" if c else f"{s}-boston"


def unique_slug(base: str, seen: set) -> str:
    slug, n = base, 2
    while slug in seen:
        slug = f"{base}-{n}"
        n += 1
    seen.add(slug)
    return slug


# ── Group rows ───────────────────────────────────────────────────────────────

def group_rows(rows):
    """
    Group violation rows into restaurants and inspections.
    Returns (restaurants, inspections) dicts.
    """
    restaurants = {}   # licenseno → {name, address, city, zip, lat, lng}
    inspections = {}   # (licenseno, date) → {result, violations: []}

    for row in rows:
        licenseno = (row.get('licenseno') or '').strip()
        if not licenseno:
            continue

        cat = (row.get('licensecat') or '').strip()
        if cat and cat not in INCLUDE_CATS:
            continue

        # Restaurant record
        if licenseno not in restaurants:
            name = (row.get('businessname') or row.get('dbaname') or '').strip()
            if not name:
                continue
            lat, lng = parse_location(row.get('location'))
            city = (row.get('city') or '').strip().title()
            restaurants[licenseno] = {
                'name':    name,
                'address': (row.get('address') or '').strip().title(),
                'city':    city or 'Boston',
                'zip':     (row.get('zip') or '').strip(),
                'lat':     lat,
                'lng':     lng,
            }

        # Inspection record
        raw_date = row.get('resultdttm', '')
        insp_date = parse_date(raw_date)
        if not insp_date or insp_date.year < 2000:
            continue

        raw_result = (row.get('result') or '').strip()
        result = RESULT_MAP.get(raw_result)
        if result is None:
            continue  # skip non-inspection results

        key = (licenseno, insp_date)
        if key not in inspections:
            inspections[key] = {
                'date':       insp_date,
                'result':     result,
                'violations': [],
            }

        # Violation row
        viol_level = (row.get('viol_level') or '').strip()
        severity = SEVERITY_MAP.get(viol_level)
        if not severity:
            continue  # no violation on this row (null or '-')

        vcode = (row.get('violation') or '').strip()
        vdesc = (row.get('violdesc') or '').strip()
        comments = (row.get('comments') or '').strip()

        if vcode or vdesc:
            inspections[key]['violations'].append({
                'code':     vcode or None,
                'desc':     vdesc.capitalize() if vdesc else None,
                'comments': comments or None,
                'severity': severity,
            })

    return restaurants, inspections


# ── CKAN API fetch ───────────────────────────────────────────────────────────

def fetch_full(page_size: int = 10000):
    """Paginate through all rows via datastore_search."""
    all_rows = []
    offset = 0
    total = None
    print("Fetching Boston inspections (full)...")

    while True:
        params = urllib.parse.urlencode({
            'resource_id': RESOURCE_ID,
            'limit': str(page_size),
            'offset': str(offset),
        })
        url = f"{SEARCH_URL}?{params}"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})

        for attempt in range(3):
            try:
                with urllib.request.urlopen(req, timeout=60) as r:
                    data = json.loads(r.read())
                break
            except Exception as e:
                if attempt < 2:
                    wait = 2 ** (attempt + 1)
                    print(f"  Retry {attempt+1}: {e} (waiting {wait}s)")
                    time.sleep(wait)
                else:
                    raise

        result = data.get('result', {})
        records = result.get('records', [])
        if total is None:
            total = result.get('total', '?')
        all_rows.extend(records)
        print(f"  {len(all_rows):,}/{total:,} rows...", end='\r', flush=True)

        if len(records) < page_size:
            break
        offset += page_size
        time.sleep(0.1)

    print(f"\n  {len(all_rows):,} rows total.")
    return all_rows


def fetch_recent(since: date, page_size: int = 10000):
    """Fetch recent rows via datastore_search_sql."""
    since_str = since.strftime('%Y-%m-%d')
    all_rows = []
    offset = 0
    print(f"Fetching Boston inspections since {since}...")

    while True:
        sql = (
            f'SELECT * FROM "{RESOURCE_ID}" '
            f'WHERE "resultdttm" >= \'{since_str}\' '
            f'ORDER BY "resultdttm" DESC '
            f'LIMIT {page_size} OFFSET {offset}'
        )
        params = urllib.parse.urlencode({'sql': sql})
        url = f"{SQL_URL}?{params}"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})

        for attempt in range(3):
            try:
                with urllib.request.urlopen(req, timeout=60) as r:
                    data = json.loads(r.read())
                break
            except Exception as e:
                if attempt < 2:
                    wait = 2 ** (attempt + 1)
                    print(f"  Retry {attempt+1}: {e} (waiting {wait}s)")
                    time.sleep(wait)
                else:
                    raise

        records = data.get('result', {}).get('records', [])
        all_rows.extend(records)
        print(f"  {len(all_rows):,} rows...", end='\r', flush=True)

        if len(records) < page_size:
            break
        offset += page_size
        time.sleep(0.1)

    print(f"\n  {len(all_rows):,} rows total.")
    return all_rows


# ── Database write ───────────────────────────────────────────────────────────

def write_to_db(restaurants, inspections, app, db, Restaurant, Inspection, Violation):
    with app.app_context():
        # Load existing Boston restaurants keyed by source_id (licenseno)
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

        # Pre-load existing inspection keys for fast dedup
        existing_insp_keys = set(
            (str(rid), d)
            for rid, d in
            db.session.query(
                Inspection.restaurant_id, Inspection.inspection_date
            ).filter(Inspection.region == REGION).all()
        )

        new_r = new_i = 0
        total_insp = len(inspections)
        processed = 0
        pending_restaurants = []

        for (licenseno, insp_date), idata in inspections.items():
            processed += 1

            rdata = restaurants.get(licenseno)
            if not rdata:
                continue

            # Get or create restaurant
            if licenseno in existing:
                restaurant = existing[licenseno]
            else:
                slug = unique_slug(
                    make_slug(rdata['name'], rdata['city']), seen_slugs
                )
                restaurant = Restaurant(
                    source_id=licenseno,
                    name=rdata['name'],
                    slug=slug,
                    address=rdata['address'],
                    city=rdata['city'],
                    state=STATE,
                    zip=rdata['zip'],
                    latitude=rdata['lat'],
                    longitude=rdata['lng'],
                    region=REGION,
                )
                db.session.add(restaurant)
                existing[licenseno] = restaurant
                pending_restaurants.append(restaurant)
                new_r += 1

            # Flush pending restaurants to get IDs
            if pending_restaurants:
                db.session.flush()
                pending_restaurants.clear()

            # Duplicate guard
            if (str(restaurant.id), insp_date) in existing_insp_keys:
                continue

            violations = idata['violations']
            risk, score = compute_score(violations)

            insp = Inspection(
                restaurant_id=restaurant.id,
                inspection_date=insp_date,
                score=score,
                risk_score=risk,
                result=idata['result'],
                inspection_type='Routine',
                region=REGION,
            )
            db.session.add(insp)
            db.session.flush()
            new_i += 1
            existing_insp_keys.add((str(restaurant.id), insp_date))

            for v in violations:
                db.session.add(Violation(
                    inspection_id=insp.id,
                    violation_code=v['code'],
                    description=v['desc'],
                    inspector_notes=v['comments'],
                    severity=v['severity'],
                    corrected_on_site=False,
                ))

            # Update latest date
            prev = restaurant.latest_inspection_date
            if prev is None or insp_date > prev:
                restaurant.latest_inspection_date = insp_date
                restaurant.ai_summary = None

            # Commit + progress every 1000 inspections
            if new_i % 1000 == 0 and new_i > 0:
                db.session.commit()
                print(f"  {processed:,}/{total_insp:,} | "
                      f"{new_r:,} restaurants, {new_i:,} inspections",
                      flush=True)

        db.session.commit()
        print(f"  {processed:,}/{total_insp:,} | "
              f"{new_r:,} restaurants, {new_i:,} inspections",
              flush=True)
        return new_r, new_i


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
        rows = fetch_full()
    else:
        since = date.today() - timedelta(days=days)
        rows = fetch_recent(since)

    if not rows:
        print("No rows to process.")
        return

    print("Grouping rows by restaurant + inspection date...")
    restaurants, inspections = group_rows(rows)
    print(f"  {len(restaurants):,} unique restaurants, "
          f"{len(inspections):,} unique inspections.")

    app = create_app()
    print("Writing to database...")
    new_r, new_i = write_to_db(
        restaurants, inspections, app, db, Restaurant, Inspection, Violation
    )

    print(f"\nDone.")
    print(f"  {new_r:,} new restaurants")
    print(f"  {new_i:,} new inspections")


if __name__ == "__main__":
    main()
