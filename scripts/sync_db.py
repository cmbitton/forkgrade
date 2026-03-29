#!/usr/bin/env python3
"""
Incremental DB sync — queries the RI API for recently inspected facilities
and syncs only those to the database. Replaces the old approach of iterating
all 6,716 locations from locations.json every run.

Usage:
    python3 scripts/sync_db.py            # sync last 5 days (default)
    python3 scripts/sync_db.py --days=3   # sync last N days

No Google Maps key needed — only hits the RI inspections API.
"""

import base64
import json
import math
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

INSPECTIONS_URL = "https://ri.healthinspections.us/ri/API/index.cfm/inspectionsData/{}"
SEARCH_URL      = "https://ri.healthinspections.us/ri/API/index.cfm/search/{}/{}"
HTML_BASE       = "https://ri.healthinspections.us/"
API_HEADERS     = {"Accept": "application/json", "User-Agent": "Mozilla/5.0",
                   "Referer": "https://ri.healthinspections.us/"}

PRIORITY_ITEMS            = {4, 6, 8, 9, 11, 12, 13, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 26, 27, 28}
PRIORITY_FOUNDATION_ITEMS = {1, 3, 5, 10, 14, 25, 29}

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

def fetch_json(url):
    req = urllib.request.Request(url, headers=API_HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        if e.code == 404: return []
        raise

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

def item_severity(n):
    if n in PRIORITY_ITEMS: return "critical"
    if n in PRIORITY_FOUNDATION_ITEMS: return "major"
    return "minor"

def parse_violations(vdict, codes):
    out = []
    for idx, text in enumerate(v[0] for v in vdict.values() if v and v[0]):
        parts = text.split(" - ", 1)
        if len(parts) == 2:
            try:
                n    = int(parts[0].strip())
                desc = parts[1].strip().capitalize()
                sev  = item_severity(n)
                code = codes[idx] if idx < len(codes) else str(n)
            except ValueError:
                desc = text.strip().capitalize()
                sev  = "minor"
                code = codes[idx] if idx < len(codes) else ""
        else:
            desc = text.strip().capitalize()
            sev  = "minor"
            code = codes[idx] if idx < len(codes) else ""
        out.append({"code": code, "description": desc, "severity": sev})
    return out

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


def main():
    days = 5
    for arg in sys.argv[1:]:
        if arg.startswith('--days='):
            days = int(arg.split('=', 1)[1])

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
                        db.session.commit()
                        insp_added += n
                        updated    += 1
                        print(f"  Updated: {name} (+{n} inspection{'s' if n>1 else ''})")
                    else:
                        skipped += 1
                else:
                    skipped += 1
            else:
                # New location — create restaurant from API data (no geocoding here;
                # refresh.py will enrich lat/lng and cuisine on next run)
                street, city, state, zip5 = parse_address(fac.get("address", ""))
                if not city or city in ('0', 'Unknown'):
                    city = None
                slug = unique_slug(make_slug(name, city), seen_slugs)

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
                    cuisine_type = None,
                    license_type = fac.get("license_type") or None,
                    region       = "rhode-island",
                )
                db.session.add(restaurant)
                db.session.flush()

                n = import_inspections(restaurant.id, source_id)
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
