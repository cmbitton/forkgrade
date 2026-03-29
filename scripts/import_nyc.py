#!/usr/bin/env python3
"""
NYC Department of Health restaurant inspection importer.

Data source: NYC Open Data SODA API / CSV export
  API:  https://data.cityofnewyork.us/resource/43nn-pn8j.json
  CSV:  https://data.cityofnewyork.us/api/views/43nn-pn8j/rows.csv?accessType=DOWNLOAD

Each row in the source is ONE VIOLATION. This script groups by (camis + inspection_date)
to build one Inspection record per visit, with Violation rows attached.

Modes:
  --full    Download full CSV and import everything (first-time setup, slow)
  --daily   Fetch only inspections from yesterday via SODA API (default, fast)

Usage:
    nat-health/bin/python3 scripts/import_nyc.py           # daily refresh
    nat-health/bin/python3 scripts/import_nyc.py --full    # full import

Scoring notes:
  Uses the same weighted formula as RI:
    Critical violation   → weight 3
    Not Critical         → weight 1
    risk_score = sum of weights
    score (0-100)        = round(100 * exp(-risk_score * 0.05))
  NYC letter grade (A/B/C) is stored separately in the grade field and
  shown as a bonus label on restaurant pages. All sorting and tier logic
  uses the 0-100 score, same as RI.
"""

import csv
import io
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

SODA_URL = "https://data.cityofnewyork.us/resource/43nn-pn8j.json"
CSV_URL  = "https://data.cityofnewyork.us/api/views/43nn-pn8j/rows.csv?accessType=DOWNLOAD"
REGION   = "nyc"
STATE    = "NY"

# ── Cuisine mapping: NYC cuisine_description → our cuisine_type ───────────────
# Unmapped values are stored as-is so NYC's diversity shows up in Browse by Type.
NYC_CUISINE_MAP = {
    # American
    "American":                                     "American",
    "Hamburgers":                                   "American",
    "Sandwiches":                                   "American",
    "Sandwiches/Salads/Mixed Buffet":               "American",
    "Chicken":                                      "American",
    "Hotdogs":                                      "American",
    "Hotdogs/Pretzels":                             "American",
    "Steak":                                        "American",
    "Soul Food":                                    "American",
    "Cajun":                                        "American",
    "Creole":                                       "American",
    "Creole/Cajun":                                 "American",
    "Continental":                                  "American",
    "Barbecue":                                     "American",
    "Soups & Sandwiches":                           "American",
    "Soups":                                        "American",
    "Salads":                                       "American",
    # Café / Breakfast
    "Café/Coffee/Tea":                              "Café / Breakfast",
    "Coffee/Tea":                                   "Café / Breakfast",
    "Bakery":                                       "Café / Breakfast",
    "Bakery Products/Desserts":                     "Café / Breakfast",
    "Donuts":                                       "Café / Breakfast",
    "Ice Cream, Gelato, Yogurt, Ices":              "Café / Breakfast",
    "Pancakes/Waffles":                             "Café / Breakfast",
    "Juice, Smoothies, Fruit Salads":               "Café / Breakfast",
    "Bagels/Pretzels":                              "Café / Breakfast",
    "Bottled beverages, including water, sodas, juices, etc.": "Café / Breakfast",
    # Pizza
    "Pizza":                                        "Pizza",
    "Pizza/Italian":                                "Pizza",
    # Italian
    "Italian":                                      "Italian",
    # Chinese
    "Chinese":                                      "Chinese",
    "Chinese/Cuban":                                "Chinese",
    "Chinese/Japanese":                             "Chinese",
    "Dim Sum":                                      "Chinese",
    # Japanese / Sushi
    "Japanese":                                     "Japanese / Sushi",
    "Sushi":                                        "Japanese / Sushi",
    "Korean":                                       "Korean",
    # Mexican / Latin
    "Mexican":                                      "Mexican / Latin",
    "Latin (Cuban, Dominican, Puerto Rican, South & Central American)": "Mexican / Latin",
    "Latin American":                               "Mexican / Latin",
    "Caribbean":                                    "Mexican / Latin",
    "Spanish":                                      "Mexican / Latin",
    "Peruvian":                                     "Mexican / Latin",
    "Brazilian":                                    "Mexican / Latin",
    "Tex-Mex":                                      "Mexican / Latin",
    "Chilean":                                      "Mexican / Latin",
    "Columbian":                                    "Mexican / Latin",
    "Dominican":                                    "Mexican / Latin",
    "Puerto Rican":                                 "Mexican / Latin",
    "Cuban":                                        "Mexican / Latin",
    # Thai / Southeast Asian
    "Thai":                                         "Thai",
    "Vietnamese/Cambodian/Malaysian":               "Vietnamese / Southeast Asian",
    "Filipino":                                     "Filipino",
    "Indonesian":                                   "Vietnamese / Southeast Asian",
    # Indian / South Asian
    "Indian":                                       "Indian",
    "Pakistani":                                    "Indian",
    "Bangladeshi":                                  "Indian",
    # Greek / Mediterranean / Middle Eastern
    "Greek":                                        "Greek / Mediterranean",
    "Mediterranean":                                "Greek / Mediterranean",
    "Middle Eastern":                               "Greek / Mediterranean",
    "Turkish":                                      "Greek / Mediterranean",
    "Tapas":                                        "Greek / Mediterranean",
    "Lebanese":                                     "Greek / Mediterranean",
    "Moroccan":                                     "Greek / Mediterranean",
    "Afghan":                                       "Greek / Mediterranean",
    "Armenian":                                     "Greek / Mediterranean",
    "Egyptian":                                     "Greek / Mediterranean",
    "Iranian":                                      "Greek / Mediterranean",
    # Seafood
    "Seafood":                                      "Seafood",
    "Fish & Chips":                                 "Seafood",
    # Bar / Pub
    "Bar/Lounge/Club":                              "Bar / Pub",
    # Asian fusion
    "Asian/Asian Fusion":                           "Asian / Fusion",
    "Chinese/Cuban":                                "Asian / Fusion",
    # African
    "African":                                      "African",
    "Ethiopian/Eritrean":                           "African",
    # French
    "French":                                       "French",
    # Other European
    "Eastern European":                             "Eastern European",
    "Polish":                                       "Eastern European",
    "Russian":                                      "Eastern European",
    "Czech":                                        "Eastern European",
    "Portuguese":                                   "Portuguese",
    # Jewish / Kosher
    "Jewish/Kosher":                                "Jewish / Kosher",
    "Kosher":                                       "Jewish / Kosher",
    # Vegetarian / Vegan
    "Vegetarian":                                   "Vegetarian",
    "Vegan":                                        "Vegetarian",
    # Burgers / Fast Food
    "Hamburgers":                                   "American",
    "Hotdogs":                                      "American",
    "Hotdogs/Pretzels":                             "American",
    # Wine / Cocktail bars
    "Wine Bar":                                     "Bar / Pub",
    "Cocktail Lounge":                              "Bar / Pub",
    "Cocktails":                                    "Bar / Pub",
    "Irish":                                        "Bar / Pub",
    "Pub":                                          "Bar / Pub",
    # European additions
    "German":                                       "Eastern European",
    "Swiss":                                        "Eastern European",
    "Scandinavian":                                 "Eastern European",
    "English":                                      "American",
    "Australian":                                   "American",
    "New American":                                 "American",
    # Asian additions
    "Taiwanese":                                    "Chinese",
    "Hong Kong Style Cafe":                         "Chinese",
    "Noodles":                                      "Asian / Fusion",
    "Ramen":                                        "Japanese / Sushi",
    # Other / catch-all
    "Juice Bar":                                    "Café / Breakfast",
    "Smoothies":                                    "Café / Breakfast",
    "Fruits/Vegetables":                            "Café / Breakfast",
    "Nuts/Confectionary":                           "Café / Breakfast",
    "Candy Store":                                  "Café / Breakfast",
    "Chocolate":                                    "Café / Breakfast",
    # Non-restaurant (filter targets)
    "School/Children's Settings/Child Care Center": "School / Childcare",
    "Hospital":                                     "Healthcare Facility",
    "Pharmacies":                                   "Healthcare Facility",
    "Correctional":                                 "School / Childcare",
    "Not Listed/Not Applicable":                    None,
    "Other":                                        None,
}

# Grades that represent a graded inspection result
GRADED = {'A', 'B', 'C'}
# Grades that mean "pending" or "not yet graded"
PENDING_GRADES = {'Z', 'N', 'P', 'G', ''}


def parse_date(s: str) -> date | None:
    if not s:
        return None
    for fmt in ('%m/%d/%Y', '%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S'):
        try:
            return datetime.strptime(s.strip(), fmt).date()
        except ValueError:
            pass
    return None


def compute_score(violations: list) -> tuple[int, int]:
    """NYC-tuned weighted formula.
    Returns (risk_score, display_score).
    Critical → weight 3, Not Critical → weight 1.5, decay=0.05.
    """
    risk = sum(3 if v['severity'] == 'critical' else 1.5 for v in violations)
    score = round(100 * math.exp(-risk * 0.05))
    return risk, score


def map_cuisine(desc: str | None) -> str | None:
    if not desc:
        return None
    # Only return mapped values; unmapped types become NULL (excluded from Browse by Type)
    return NYC_CUISINE_MAP.get(desc, None)


def make_slug(name: str, boro: str) -> str:
    s = re.sub(r"['\u2019]", '', name.lower().strip())
    s = re.sub(r'[\s_/&]+', '-', s)
    s = re.sub(r'[^a-z0-9-]', '', s)
    s = re.sub(r'-+', '-', s).strip('-')
    b = re.sub(r'[^a-z0-9-]', '', boro.lower().replace(' ', '-'))
    return f"{s}-{b}"


def unique_slug(base: str, seen: set) -> str:
    slug, n = base, 2
    while slug in seen:
        slug = f"{base}-{n}"
        n += 1
    seen.add(slug)
    return slug


def build_address(row: dict) -> str:
    parts = [row.get('building', ''), row.get('street', '')]
    return ' '.join(p.strip() for p in parts if p.strip()).title()


# ── Row → grouped data structures ────────────────────────────────────────────

def group_rows(rows):
    """
    Given an iterable of raw row dicts, return:
      restaurants: dict of camis → restaurant info dict
      inspections: dict of (camis, date) → inspection info dict
    """
    restaurants = {}   # camis → {name, boro, address, zip, lat, lng, cuisine}
    inspections = {}   # (camis, date_obj) → {score, grade, type, action, violations:[]}

    for row in rows:
        camis = row.get('camis', '').strip()
        if not camis:
            continue

        # ── Restaurant record (upsert by camis) ──────────────────────────────
        if camis not in restaurants:
            restaurants[camis] = {
                'camis':    camis,
                'name':     (row.get('dba') or '').strip(),
                'boro':     (lambda b: b if b and b != '0' else None)((row.get('boro') or '').strip().title()),
                'address':  build_address(row),
                'zip':      (row.get('zipcode') or '').strip(),
                'lat':      _float(row.get('latitude')),
                'lng':      _float(row.get('longitude')),
                'cuisine':  map_cuisine((row.get('cuisine_description') or '').strip()),
            }

        # ── Inspection record ─────────────────────────────────────────────────
        raw_date = row.get('inspection_date', '')
        insp_date = parse_date(raw_date)
        if not insp_date or insp_date.year < 1990:
            continue  # skip placeholder 1900-01-01 rows

        key = (camis, insp_date)
        raw_score_str = (row.get('score') or '').strip()
        raw_score = int(raw_score_str) if raw_score_str.lstrip('-').isdigit() else None
        grade = (row.get('grade') or '').strip()

        if key not in inspections:
            inspections[key] = {
                'date':       insp_date,
                'raw_score':  raw_score,
                'grade':      grade if grade else None,
                'type':       (row.get('inspection_type') or '').strip(),
                'action':     (row.get('action') or '').strip(),
                'violations': [],
            }
        else:
            # Update score/grade if we get a more informative row for same inspection
            existing = inspections[key]
            if raw_score is not None and existing['raw_score'] is None:
                existing['raw_score'] = raw_score
            if grade and grade in GRADED and existing['grade'] not in GRADED:
                existing['grade'] = grade

        # ── Violation row ─────────────────────────────────────────────────────
        vcode = (row.get('violation_code') or '').strip()
        vdesc = (row.get('violation_description') or '').strip()
        cflag = (row.get('critical_flag') or '').strip()

        if vcode or vdesc:
            severity = 'critical' if cflag == 'Critical' else 'minor'
            inspections[key]['violations'].append({
                'code':     vcode,
                'desc':     vdesc.capitalize(),
                'severity': severity,
            })

    return restaurants, inspections


def _float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# ── Database write ────────────────────────────────────────────────────────────

def write_to_db(restaurants, inspections, app, db, Restaurant, Inspection, Violation):
    with app.app_context():
        from sqlalchemy import func as sqlfunc

        # Load existing NYC restaurants keyed by source_id (camis)
        existing = {
            r.source_id: r
            for r in Restaurant.query.filter_by(region=REGION)
                                     .filter(Restaurant.source_id.isnot(None))
                                     .all()
        }
        seen_slugs = {r.slug for r in existing.values()}
        # Also grab slugs from other regions to avoid collisions
        other_slugs = {r.slug for r in Restaurant.query.filter(
            Restaurant.region != REGION
        ).with_entities(Restaurant.slug).all()}
        seen_slugs |= other_slugs

        # Latest inspection date per restaurant.id
        latest_dates = {
            str(rid): md
            for rid, md in
            db.session.query(Inspection.restaurant_id, sqlfunc.max(Inspection.inspection_date))
                      .group_by(Inspection.restaurant_id).all()
        }

        new_r = new_i = 0

        for camis, rdata in restaurants.items():
            name = rdata['name']
            if not name:
                continue

            # Get or create restaurant
            if camis in existing:
                restaurant = existing[camis]
            else:
                boro = rdata['boro']  # None if borough was missing/invalid
                slug = unique_slug(make_slug(name, boro or 'nyc'), seen_slugs)
                restaurant = Restaurant(
                    source_id    = camis,
                    name         = name,
                    slug         = slug,
                    address      = rdata['address'],
                    city         = boro,
                    state        = STATE,
                    zip          = rdata['zip'],
                    latitude     = rdata['lat'],
                    longitude    = rdata['lng'],
                    cuisine_type = rdata['cuisine'],
                    region       = REGION,
                )
                db.session.add(restaurant)
                db.session.flush()
                existing[camis] = restaurant
                new_r += 1

            # Write inspections for this camis
            db_latest = latest_dates.get(str(restaurant.id))
            for (icamis, idate), idata in inspections.items():
                if icamis != camis:
                    continue
                if db_latest and idate <= db_latest:
                    continue  # already have this or older

                # Duplicate guard
                if Inspection.query.filter_by(
                    restaurant_id=restaurant.id, inspection_date=idate
                ).first():
                    continue

                grade = idata['grade'] or None
                risk, score = compute_score(idata['violations'])

                insp = Inspection(
                    restaurant_id   = restaurant.id,
                    inspection_date = idate,
                    score           = score,
                    risk_score      = risk,
                    grade           = grade,
                    result          = _grade_to_result(grade, risk),
                    inspection_type = idata['type'] or 'Routine',
                )
                db.session.add(insp)
                db.session.flush()
                new_i += 1

                for v in idata['violations']:
                    db.session.add(Violation(
                        inspection_id     = insp.id,
                        violation_code    = v['code'],
                        description       = v['desc'],
                        severity          = v['severity'],
                        corrected_on_site = False,
                    ))

            # Commit every 500 restaurants to avoid huge transactions
            if new_r % 500 == 0 and new_r > 0:
                db.session.commit()

        db.session.commit()
        return new_r, new_i


def _grade_to_result(grade, risk):
    if grade == 'A':
        return 'Pass'
    if grade == 'B':
        return 'Conditional Pass'
    if grade == 'C':
        return 'Fail'
    # Pending/ungraded: derive from risk_score
    if risk <= 2:
        return 'Pass'
    if risk <= 9:
        return 'Conditional Pass'
    return 'Fail'


# ── Data fetchers ─────────────────────────────────────────────────────────────

def fetch_full_csv():
    """Download full CSV export (~100MB+). Returns an iterable of row dicts."""
    print(f"Downloading full CSV from NYC Open Data...")
    req = urllib.request.Request(CSV_URL, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=120) as r:
        content = r.read().decode('utf-8', errors='replace')
    print(f"  Downloaded {len(content) // 1024 // 1024}MB, parsing...")
    reader = csv.DictReader(io.StringIO(content))
    # Normalize field names to lowercase with underscores (CSV has mixed case)
    rows = []
    for row in reader:
        rows.append({k.lower().replace(' ', '_'): v for k, v in row.items()})
    print(f"  {len(rows):,} rows parsed.")
    return rows


def fetch_daily_api(since: date):
    """Fetch all inspection rows since `since` date via SODA API (paginated)."""
    since_str = since.strftime('%Y-%m-%dT00:00:00.000')
    all_rows = []
    offset = 0
    limit = 1000
    print(f"Fetching NYC inspections since {since}...")
    while True:
        params = urllib.parse.urlencode({
            '$limit':   str(limit),
            '$offset':  str(offset),
            '$where':   f"inspection_date >= '{since_str}'",
            '$order':   'inspection_date DESC',
        })
        url = f"{SODA_URL}?{params}"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=30) as r:
            batch = json.loads(r.read())
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


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    full_mode = '--full' in sys.argv

    days = 1
    for arg in sys.argv[1:]:
        if arg.startswith('--days='):
            days = int(arg.split('=', 1)[1])

    from app import create_app
    from app.db import db
    from app.models.restaurant import Restaurant
    from app.models.inspection import Inspection
    from app.models.violation import Violation

    if full_mode:
        rows = fetch_full_csv()
    else:
        since = date.today() - timedelta(days=days)
        rows = fetch_daily_api(since)

    if not rows:
        print("No rows to process.")
        return

    print("Grouping rows by restaurant + inspection date...")
    restaurants, inspections = group_rows(rows)
    print(f"  {len(restaurants):,} unique restaurants, {len(inspections):,} unique inspections.")

    app = create_app()
    print("Writing to database...")
    new_r, new_i = write_to_db(restaurants, inspections, app, db, Restaurant, Inspection, Violation)

    print(f"\nDone.")
    print(f"  {new_r} new restaurants")
    print(f"  {new_i} new inspections")


if __name__ == "__main__":
    main()
