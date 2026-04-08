#!/usr/bin/env python3
"""
Rhode Island health inspection importer — incremental daily sync, full sync, and rescrape.

Usage:
    python3 scripts/import_ri.py              # sync last 5 days (default)
    python3 scripts/import_ri.py --days=3     # sync last N days
    python3 scripts/import_ri.py --full-sync  # check ALL existing restaurants for missing inspections
    python3 scripts/import_ri.py --rescrape   # re-fetch HTML codes for all existing RI data,
                                              # update only inspections whose score changes
    python3 scripts/import_ri.py --rescrape --dry-run  # preview without writing

Set GOOGLE_MAPS_KEY to enrich new restaurants with cuisine type via Google Places.
"""

import base64
import json
import math
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app import create_app
from app.db import db
from app.models.restaurant import Restaurant
from app.models.inspection import Inspection
from app.models.violation import Violation
from scripts.place_types import CUISINE_TYPE_MAP

INSPECTIONS_URL = "https://ri.healthinspections.us/ri/API/index.cfm/inspectionsData/{}"
SEARCH_URL      = "https://ri.healthinspections.us/ri/API/index.cfm/search/{}/{}"
HTML_BASE       = "https://ri.healthinspections.us/"
API_HEADERS     = {"Accept": "application/json", "User-Agent": "Mozilla/5.0",
                   "Referer": "https://ri.healthinspections.us/"}

GOOGLE_MAPS_KEY  = os.environ.get("GOOGLE_MAPS_KEY")
PLACES_URL       = "https://places.googleapis.com/v1/places:searchText"
# Providence, RI — bias Google Places results toward RI
_RI_LAT, _RI_LNG = 41.7798, -71.4373

def fetch_cuisine(name, address):
    """Look up cuisine type for a new restaurant via Google Places. Returns None if unavailable."""
    if not GOOGLE_MAPS_KEY:
        return None
    body = json.dumps({
        "textQuery":      f"{name} {address}",
        "maxResultCount": 1,
        "locationBias": {"circle": {
            "center": {"latitude": _RI_LAT, "longitude": _RI_LNG},
            "radius": 50000.0,
        }},
    }).encode()
    req = urllib.request.Request(
        PLACES_URL, data=body, method="POST",
        headers={
            "Content-Type":     "application/json",
            "X-Goog-Api-Key":   GOOGLE_MAPS_KEY,
            "X-Goog-FieldMask": "places.types",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            places = json.loads(r.read()).get("places", [])
        types = places[0].get("types", []) if places else []
        for t in types:
            if t in CUISINE_TYPE_MAP:
                return CUISINE_TYPE_MAP[t]
    except Exception:
        pass
    return None

# FDA Food Code severity data — shared across all importers
from fda_codes import CODE_SEVERITY, code_weight, PRIORITY_ITEMS, PRIORITY_FOUNDATION_ITEMS, item_severity

RI_MUNICIPALITIES = [
    "Barrington", "Bristol", "Burrillville", "Central Falls", "Charlestown",
    "Coventry", "Cranston", "Cumberland", "East Greenwich", "East Providence",
    "Exeter", "Foster", "Glocester", "Hopkinton", "Jamestown", "Johnston",
    "Lincoln", "Little Compton", "Middletown", "Narragansett", "Newport",
    "New Shoreham", "North Kingstown", "North Providence", "North Smithfield",
    "Pawtucket", "Portsmouth", "Providence", "Richmond", "Scituate",
    "Smithfield", "South Kingstown", "Tiverton", "Warren", "Warwick",
    "West Greenwich", "West Warwick", "Westerly", "Woonsocket",
]
_CITIES_SORTED = sorted(RI_MUNICIPALITIES, key=len, reverse=True)
_CITIES_UPPER  = [c.upper() for c in _CITIES_SORTED]

CUISINE_MAP = {
    "pizza": "Pizza", "italian": "Italian", "japanese": "Japanese / Sushi",
    "chinese": "Chinese", "mexican": "Mexican / Latin", "thai": "Thai",
    "indian": "Indian", "greek": "Greek / Mediterranean", "seafood": "Seafood",
    "breakfast": "Café / Breakfast", "american": "American", "bar": "Bar / Pub",
}
CATEGORY_LABEL_MAP = {
    "grocery": "Grocery / Market", "caterer": "Catering",
    "school": "School / Childcare", "healthcare": "Healthcare Facility",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def encode_id(s):
    return base64.b64encode(str(s).encode()).decode()

def decode_id(b64_str):
    padded = b64_str + "=" * (-len(b64_str) % 4)
    return base64.b64decode(padded).decode()

def fetch_json(url, retries=3):
    req = urllib.request.Request(url, headers=API_HEADERS)
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return []
            if e.code >= 500 and attempt < retries - 1:
                wait = 2 ** (attempt + 1)
                print(f"  Server error {e.code}, retrying in {wait}s...")
                time.sleep(wait)
                continue
            if e.code >= 500:
                print(f"  Server error {e.code} after {retries} attempts, skipping.")
                return None
            raise
        except (TimeoutError, OSError) as e:
            if attempt < retries - 1:
                wait = 2 ** (attempt + 1)
                print(f"  Network error ({e}), retrying in {wait}s...")
                time.sleep(wait)
            else:
                print(f"  fetch_json failed after {retries} attempts, skipping.")
                return None

def fetch_html_codes(path):
    clean = re.sub(r'^\.\.?/', '', path)
    safe_chars = set('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~:/?#[]@!$&\'()*+,;=%')
    safe = ''.join(c if c in safe_chars else urllib.parse.quote(c) for c in clean)
    req = urllib.request.Request(HTML_BASE + safe,
                                 headers={"User-Agent": "Mozilla/5.0", "Referer": HTML_BASE})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            html = r.read().decode("utf-8", errors="replace")
        return re.findall(r"Violation of Code:.*?([\d]+-[\d]+\.[\d]+(?:\([A-Z]\))?)", html)
    except Exception:
        return []

def parse_violations(vdict, codes):
    out = []
    for idx, text in enumerate(v[0] for v in vdict.values() if v and v[0]):
        parts = text.split(" - ", 1)
        code = codes[idx] if idx < len(codes) else ""
        if len(parts) == 2:
            try:
                n    = int(parts[0].strip())
                desc = parts[1].strip().capitalize()
                # Prefer FDA code-based severity when the code is known; fall back to item number
                sev  = _severity_from_code_or_item(code, n)
                if not code:
                    code = str(n)
            except ValueError:
                desc = text.strip().capitalize()
                sev  = "minor" if not code else _severity_from_code(code)
        else:
            desc = text.strip().capitalize()
            sev  = "minor" if not code else _severity_from_code(code)
        out.append({"code": code, "description": desc, "severity": sev})
    return out

def _severity_from_code(code):
    w = code_weight(code)
    if w == 3: return "critical"
    if w == 2: return "major"
    return "minor"

def _severity_from_code_or_item(code, item_n):
    if code and code in CODE_SEVERITY:
        return _severity_from_code(code)
    return item_severity(item_n)

def risk_to_score(risk):
    return round(100 * math.exp(-risk * 0.05))

def score_to_result(score):
    if score >= 80: return "Pass"
    if score >= 60: return "Pass with Conditions"
    return "Fail"

def parse_date(s):
    if not s: return None
    for fmt in ("%m-%d-%Y", "%Y-%m-%d"):
        try: return datetime.strptime(s.strip(), fmt).date()
        except ValueError: pass
    return None

def parse_address(raw):
    raw = raw.strip()
    m = re.search(r'\s+([A-Za-z]{2})\s+(\d{5})\d*\s*$', raw)
    if not m: return raw.title(), None, "RI", None
    state, zip5 = m.group(1).upper(), m.group(2)
    before = raw[:m.start()].strip()
    if state != "RI":
        parts = before.rsplit(' ', 1)
        return (parts[0].title() if len(parts)==2 else before.title(),
                parts[1].title() if len(parts)==2 else None, state, zip5)
    upper = before.upper()
    for i, cu in enumerate(_CITIES_UPPER):
        if upper.endswith(' ' + cu) or upper == cu:
            return before[:-(len(cu))].strip().title(), _CITIES_SORTED[i], state, zip5
    parts = before.rsplit(' ', 1)
    if len(parts) == 2: return parts[0].title(), parts[1].title(), state, zip5
    return before.title(), None, state, zip5

def make_slug(name, city):
    s = re.sub(r"['\u2019]", '', name.lower().strip())
    s = re.sub(r'[\s_/&]+', '-', s)
    s = re.sub(r'[^a-z0-9-]', '', s)
    s = re.sub(r'-+', '-', s).strip('-')
    c = re.sub(r'[^a-z0-9-]', '', (city or 'ri').lower().replace(' ', '-').replace("'", ''))
    return s + '-' + c

def unique_slug(base, seen):
    slug, n = base, 2
    while slug in seen:
        slug = f"{base}-{n}"; n += 1
    seen.add(slug)
    return slug


# ── RI API: date-range search ─────────────────────────────────────────────────

def search_by_date(from_str, to_str, page):
    """One page of facilities inspected within [from_str, to_str] (MM/DD/YYYY)."""
    json_obj = {
        "keyword": base64.b64encode(b"").decode(),
        "from":    base64.b64encode(from_str.encode()).decode(),
        "to":      base64.b64encode(to_str.encode()).decode(),
    }
    json_str = json.dumps(json_obj).replace("/", "%2F")
    url = SEARCH_URL.format(urllib.parse.quote(json_str), page)
    return fetch_json(url)

def fetch_recent_facilities(days):
    """Return all facilities inspected in the last N days from the RI API."""
    today     = date.today()
    from_date = today - timedelta(days=days)
    from_str  = from_date.strftime("%m/%d/%Y")
    to_str    = today.strftime("%m/%d/%Y")
    print(f"Querying RI API for inspections {from_str} – {to_str}...")

    facilities = []
    page = 0
    while True:
        try:
            batch = search_by_date(from_str, to_str, page)
        except Exception as e:
            print(f"  Error at page {page}: {e}")
            break
        if not batch:
            break
        for item in batch:
            facilities.append({
                "id":              decode_id(item["id"]),
                "name":            item.get("name", ""),
                "address":         item.get("mapAddress", ""),
                "last_inspection": item.get("columns", {}).get("1", "")
                                       .replace("Last Inspection Date:", "").strip(),
                "license_type":    item.get("columns", {}).get("2", "")
                                       .replace("License Type: ", "").strip(),
            })
        page += 1
        time.sleep(0.3)

    print(f"  Found {len(facilities)} recently inspected facilities.\n")
    return facilities


# ── Core sync logic ───────────────────────────────────────────────────────────

def import_inspections(restaurant_id, source_id, since_date=None):
    """Fetch inspections for source_id and insert any newer than since_date."""
    data = fetch_json(INSPECTIONS_URL.format(encode_id(source_id)))
    if not data:
        return 0

    added = 0
    for insp_data in data:
        raw_date  = insp_data.get("columns", {}).get("0", "")
        insp_date = parse_date(raw_date.replace("Inspection Date:", "").strip())
        if not insp_date:
            continue
        if since_date and insp_date <= since_date:
            continue

        exists = Inspection.query.filter_by(
            restaurant_id=restaurant_id, inspection_date=insp_date
        ).first()
        if exists:
            continue

        pp     = insp_data.get("printablePath", "")
        codes  = fetch_html_codes(pp) if pp else []
        if pp:
            time.sleep(0.15)

        violations = parse_violations(insp_data.get("violations", {}), codes)
        risk  = sum(3 if v["severity"]=="critical" else 2 if v["severity"]=="major" else 1
                    for v in violations)
        score = risk_to_score(risk)

        insp = Inspection(
            restaurant_id   = restaurant_id,
            inspection_date = insp_date,
            inspection_type = "Routine",
            score           = score,
            risk_score      = risk,
            grade           = None,
            result          = score_to_result(score),
            region          = 'rhode-island',
        )
        db.session.add(insp)
        db.session.flush()

        for v in violations:
            db.session.add(Violation(
                inspection_id     = insp.id,
                violation_code    = v["code"],
                description       = v["description"],
                severity          = v["severity"],
                corrected_on_site = False,
            ))
        added += 1

    return added


def rescrape_ri(dry_run=False, skip=0):
    """
    Re-fetch HTML violation codes for all existing RI inspections and update
    any whose score changes as a result of proper FDA code-based severity.

    Skips restaurants with zero violations (nothing to improve).
    Only writes to the DB when the recomputed score differs from what's stored.
    Clears ai_summary only for restaurants whose latest inspection score changed.

    skip: number of restaurants to skip at the start (for resuming after a crash).
    """
    from sqlalchemy import func, exists
    from sqlalchemy.exc import OperationalError

    app = create_app()
    with app.app_context():
        # Restaurants that have at least one violation on record
        has_violation = (
            Restaurant.query
            .filter_by(region="rhode-island")
            .filter(Restaurant.source_id.isnot(None))
            .filter(
                exists().where(
                    Inspection.restaurant_id == Restaurant.id
                ).where(
                    exists().where(Violation.inspection_id == Inspection.id)
                )
            )
            .order_by(Restaurant.id)
            .all()
        )

        total = len(has_violation)
        if skip:
            print(f"Skipping first {skip} restaurants (resume mode).")
        print(f"Checking {total - skip} of {total} RI restaurants with violations{'  (dry run)' if dry_run else ''}...")
        sys.stdout.flush()

        total_checked = 0
        total_updated = 0
        total_skipped = 0

        for idx, restaurant in enumerate(has_violation):
            if idx < skip:
                continue

            source_id = str(restaurant.source_id)
            api_data  = fetch_json(INSPECTIONS_URL.format(encode_id(source_id)))
            if not api_data:
                total_skipped += 1
                continue

            # Build map: inspection_date → (printablePath, violations_dict)
            api_map = {}
            for insp_data in api_data:
                raw_date  = insp_data.get("columns", {}).get("0", "")
                insp_date = parse_date(raw_date.replace("Inspection Date:", "").strip())
                pp        = insp_data.get("printablePath", "")
                vdict     = insp_data.get("violations", {})
                if insp_date and pp:
                    api_map[insp_date] = (pp, vdict)

            restaurant_score_changed = False
            try:
                latest_date = db.session.query(func.max(Inspection.inspection_date)).filter(
                    Inspection.restaurant_id == restaurant.id
                ).scalar()

                for insp in Inspection.query.filter_by(restaurant_id=restaurant.id).all():
                    if not insp.violations:
                        continue
                    if insp.inspection_date not in api_map:
                        continue  # inspection older than what API returns

                    pp, vdict = api_map[insp.inspection_date]
                    codes     = fetch_html_codes(pp)
                    if not codes:
                        continue  # no codes found in HTML (e.g. blank report)

                    new_violations = parse_violations(vdict, codes)
                    new_risk  = sum(3 if v["severity"] == "critical" else
                                    2 if v["severity"] == "major" else 1
                                    for v in new_violations)
                    new_score = risk_to_score(new_risk)

                    total_checked += 1
                    if new_score == insp.score:
                        continue

                    old_score = insp.score
                    if not dry_run:
                        Violation.query.filter_by(inspection_id=insp.id).delete()
                        for v in new_violations:
                            db.session.add(Violation(
                                inspection_id     = insp.id,
                                violation_code    = v["code"],
                                description       = v["description"],
                                severity          = v["severity"],
                                corrected_on_site = False,
                            ))
                        insp.score      = new_score
                        insp.risk_score = new_risk
                        insp.result     = score_to_result(new_score)

                    if insp.inspection_date == latest_date:
                        restaurant_score_changed = True

                    total_updated += 1
                    print(f"  {restaurant.name} | {insp.inspection_date} | score {old_score} → {new_score}")

                    time.sleep(0.15)

                if restaurant_score_changed and not dry_run:
                    restaurant.ai_summary = None

                if not dry_run and (idx + 1) % 50 == 0:
                    db.session.commit()

            except OperationalError as exc:
                print(f"\n  DB connection lost at idx={idx} ({restaurant.name}): {exc}")
                print(f"  Rolling back and reconnecting...")
                try:
                    db.session.rollback()
                except Exception:
                    pass
                db.engine.dispose()
                print(f"  Reconnected. Skipping restaurant and continuing.")
                print(f"  TIP: re-run with --skip={idx} to retry from this point.")
                total_skipped += 1
                time.sleep(2)
                continue

            if (idx + 1) % 100 == 0:
                print(f"  [{idx+1}/{total}] {total_updated} updated so far...")
                sys.stdout.flush()

            time.sleep(0.3)

        if not dry_run:
            db.session.commit()

        print(f"\nRescrape {'(dry run) ' if dry_run else ''}complete:")
        print(f"  {total_checked:,} inspections checked with HTML codes")
        print(f"  {total_updated:,} inspections updated (score changed)")
        print(f"  {total_skipped:,} restaurants skipped (API returned nothing or connection error)")


def full_sync_ri(skip=0):
    """
    Check ALL existing RI restaurants for missing inspections.

    Unlike --days=N which only finds facilities inspected recently, this iterates
    every known restaurant and asks the API for its full inspection history.
    import_inspections() deduplicates, so only truly missing inspections are added.

    Use this weekly to catch retroactively-added inspections that the daily
    refresh window misses.
    """
    from sqlalchemy import func
    from sqlalchemy.exc import OperationalError

    app = create_app()
    with app.app_context():
        restaurants = (
            Restaurant.query
            .filter_by(region="rhode-island")
            .filter(Restaurant.source_id.isnot(None))
            .order_by(Restaurant.id)
            .all()
        )

        total = len(restaurants)
        if skip:
            print(f"Skipping first {skip} restaurants (resume mode).")
        print(f"Full sync: checking {total - skip} of {total} RI restaurants for missing inspections...")
        sys.stdout.flush()

        updated       = 0
        insp_added    = 0
        skipped       = 0

        for idx, restaurant in enumerate(restaurants):
            if idx < skip:
                continue

            source_id = str(restaurant.source_id)
            try:
                n = import_inspections(restaurant.id, source_id, since_date=None)
            except OperationalError as exc:
                print(f"\n  DB error at idx={idx} ({restaurant.name}): {exc}")
                try:
                    db.session.rollback()
                except Exception:
                    pass
                db.engine.dispose()
                print(f"  Reconnected. Skipping restaurant.")
                print(f"  TIP: re-run with --skip={idx} to retry from this point.")
                skipped += 1
                time.sleep(2)
                continue

            if n:
                new_latest = db.session.query(func.max(Inspection.inspection_date)).filter(
                    Inspection.restaurant_id == restaurant.id
                ).scalar()
                restaurant.latest_inspection_date = new_latest
                restaurant.ai_summary = None
                db.session.commit()
                insp_added += n
                updated    += 1
                print(f"  {restaurant.name} — +{n} inspection{'s' if n > 1 else ''}")

            if (idx + 1) % 50 == 0:
                db.session.commit()
            if (idx + 1) % 100 == 0:
                print(f"  [{idx+1}/{total}] {updated} updated, {insp_added} inspections added so far...")
                sys.stdout.flush()

            time.sleep(0.3)

        db.session.commit()
        print(f"\nFull sync complete:")
        print(f"  {updated} restaurants had missing inspections")
        print(f"  {insp_added} inspections added")
        print(f"  {skipped} restaurants skipped (errors)")


def main():
    days    = 5
    skip    = 0
    rescrape  = '--rescrape'  in sys.argv
    full_sync = '--full-sync' in sys.argv
    dry_run   = '--dry-run'   in sys.argv
    for arg in sys.argv[1:]:
        if arg.startswith('--days='):
            days = int(arg.split('=', 1)[1])
        elif arg.startswith('--skip='):
            skip = int(arg.split('=', 1)[1])

    if rescrape:
        rescrape_ri(dry_run=dry_run, skip=skip)
        return

    if full_sync:
        full_sync_ri(skip=skip)
        return

    facilities = fetch_recent_facilities(days)
    if not facilities:
        print("No recently inspected facilities found.")
        return

    app = create_app()
    with app.app_context():
        db.create_all()

        existing = {
            str(r.source_id): r
            for r in Restaurant.query.filter_by(region="rhode-island")
                                     .filter(Restaurant.source_id.isnot(None))
                                     .all()
        }
        from sqlalchemy import func
        latest_dates = {
            str(rid): max_date
            for rid, max_date in
            db.session.query(Inspection.restaurant_id, func.max(Inspection.inspection_date))
                      .group_by(Inspection.restaurant_id).all()
        }
        seen_slugs = {r.slug for r in existing.values()}

        new_locations = 0
        updated       = 0
        skipped       = 0
        insp_added    = 0

        for fac in facilities:
            source_id = str(fac.get("id", ""))
            name      = (fac.get("name") or "").strip()
            if not name or not source_id:
                skipped += 1
                continue

            if source_id in existing:
                restaurant = existing[source_id]
                db_latest  = latest_dates.get(str(restaurant.id))
                fac_date   = parse_date(fac.get("last_inspection"))

                if fac_date and (not db_latest or fac_date > db_latest):
                    n = import_inspections(restaurant.id, source_id, since_date=db_latest)
                    if n:
                        new_latest = db.session.query(func.max(Inspection.inspection_date)).filter(
                            Inspection.restaurant_id == restaurant.id
                        ).scalar()
                        restaurant.latest_inspection_date = new_latest
                        restaurant.ai_summary = None
                        db.session.commit()
                        insp_added += n
                        updated    += 1
                        print(f"  Updated: {name} (+{n} inspection{'s' if n>1 else ''})")
                    else:
                        skipped += 1
                else:
                    skipped += 1
            else:
                # New location
                street, city, state, zip5 = parse_address(fac.get("address", ""))
                if not city or city in ('0', 'Unknown'):
                    city = None
                slug    = unique_slug(make_slug(name, city), seen_slugs)
                cuisine = fetch_cuisine(name, street)

                restaurant = Restaurant(
                    source_id    = source_id,
                    name         = name.title(),
                    slug         = slug,
                    address      = street,
                    city         = city,
                    state        = state,
                    zip          = zip5,
                    latitude     = None,
                    longitude    = None,
                    cuisine_type = cuisine,
                    license_type = fac.get("license_type") or None,
                    region       = "rhode-island",
                )
                db.session.add(restaurant)
                db.session.flush()

                n = import_inspections(restaurant.id, source_id)
                new_latest = db.session.query(func.max(Inspection.inspection_date)).filter(
                    Inspection.restaurant_id == restaurant.id
                ).scalar()
                restaurant.latest_inspection_date = new_latest
                db.session.commit()
                existing[source_id] = restaurant
                insp_added   += n
                new_locations += 1
                print(f"  New: {name} ({city}) — {n} inspections")

            time.sleep(0.3)

        db.session.commit()
        print(f"\nSync complete:")
        print(f"  {new_locations} new locations")
        print(f"  {updated} updated")
        print(f"  {insp_added} new inspections added")
        print(f"  {skipped} skipped (already up to date)")


if __name__ == "__main__":
    main()
