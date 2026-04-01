#!/usr/bin/env python3
"""
Rhode Island health inspection importer — incremental daily sync and full rescrape.

Usage:
    python3 scripts/import_ri.py              # sync last 5 days (default)
    python3 scripts/import_ri.py --days=3     # sync last N days
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

PRIORITY_ITEMS            = {4, 6, 8, 9, 11, 12, 13, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 26, 27, 28}
PRIORITY_FOUNDATION_ITEMS = {1, 3, 5, 10, 14, 25, 29}

# FDA Food Code 2022 severity by code section (P=Priority/critical, Pf=Priority Foundation/major, C=Core/minor)
# Subsection-specific entries take precedence; code_weight() falls back to base code when needed.
# This dict is copied verbatim from the original refresh.py (the authoritative source).
CODE_SEVERITY = {
    "2-101.11": "Pf", "2-102.11": "Pf", "2-102.12": "C",  "2-103.11": "Pf",
    "2-201.11": "P",  "2-201.12": "P",  "2-201.13": "P",
    "2-301.11": "P",  "2-301.12": "P",  "2-301.14": "P",  "2-301.15": "Pf",
    "2-301.16": "Pf", "2-302.11": "Pf", "2-303.11": "C",  "2-304.11": "C",
    "2-401.11": "C",  "2-401.12": "C",  "2-401.13": "C",  "2-402.11": "C",
    "2-403.11": "Pf", "2-501.11": "Pf",
    "3-101.11": "P",
    "3-201.11": "P",  "3-201.12": "P",  "3-201.13": "P",  "3-201.14": "P",
    "3-201.15": "P",  "3-201.16": "P",  "3-201.17": "P",
    "3-202.11": "P",  "3-202.110": "P", "3-202.12": "P",  "3-202.13": "P",
    "3-202.14": "P",  "3-202.15": "Pf", "3-202.16": "P",  "3-202.17": "C",
    "3-202.18": "Pf", "3-203.11": "C",  "3-203.12": "Pf",
    "3-301.11": "P",  "3-301.12": "P",
    "3-302.11": "P",  "3-302.12": "C",  "3-302.13": "P",  "3-302.14": "P",
    "3-302.15": "Pf", "3-303.11": "P",  "3-303.12": "C",
    "3-304.11": "P",  "3-304.12": "C",  "3-304.13": "C",  "3-304.14": "C",
    "3-304.15": "P",  "3-304.16": "C",  "3-304.17": "P",
    "3-305.11": "C",  "3-305.12": "Pf", "3-305.13": "C",  "3-305.14": "C",
    "3-306.11": "P",  "3-306.12": "C",  "3-306.13": "P",  "3-306.14": "P",
    "3-307.11": "C",
    "3-401.11": "P",  "3-401.12": "P",  "3-401.13": "Pf", "3-401.14": "P",
    "3-401.15": "P",  "3-402.11": "P",  "3-402.12": "Pf", "3-403.11": "P",
    "3-404.11": "P",
    "3-501.11": "C",  "3-501.12": "C",  "3-501.13": "Pf", "3-501.14": "P",
    "3-501.15": "Pf", "3-501.16": "P",  "3-501.17": "Pf", "3-501.18": "P",
    "3-501.19": "P",  "3-502.11": "Pf", "3-502.12": "P",
    "3-601.11": "C",  "3-601.12": "C",  "3-602.11": "Pf", "3-602.12": "C",  "3-603.11": "Pf",
    "3-701.11": "P",  "3-801.11": "P",
    "4-101.11": "P",  "4-101.12": "C",  "4-101.13": "P",  "4-101.14": "P",
    "4-101.15": "P",  "4-101.16": "C",  "4-101.17": "C",  "4-101.18": "C",
    "4-101.19": "C",  "4-102.11": "P",  "4-201.11": "C",  "4-201.12": "P",
    "4-202.11": "Pf", "4-202.12": "Pf", "4-202.13": "C",  "4-202.14": "C",
    "4-202.15": "C",  "4-202.16": "C",  "4-202.17": "C",  "4-202.18": "C",
    "4-203.11": "Pf", "4-203.12": "Pf", "4-203.13": "C",
    "4-204.11": "C",  "4-204.110": "P", "4-204.111": "P", "4-204.112": "Pf","4-204.113": "C",
    "4-204.114": "C", "4-204.115": "Pf","4-204.116": "Pf","4-204.117": "Pf",
    "4-204.118": "C", "4-204.119": "C", "4-204.12": "C",  "4-204.120": "C",
    "4-204.121": "C", "4-204.122": "C", "4-204.123": "C", "4-204.13": "P",
    "4-204.14": "C",  "4-204.15": "C",  "4-204.16": "C",  "4-204.17": "C",
    "4-204.18": "C",  "4-204.19": "C",
    "4-301.11": "Pf", "4-301.12": "Pf", "4-301.13": "C",  "4-301.14": "C",
    "4-301.15": "C",  "4-302.11": "Pf", "4-302.12": "Pf", "4-302.13": "Pf",
    "4-302.14": "Pf", "4-303.11": "Pf", "4-401.11": "Pf", "4-402.11": "C",  "4-402.12": "C",
    "4-501.11": "C",  "4-501.110": "Pf","4-501.111": "P", "4-501.112": "Pf",
    "4-501.113": "C", "4-501.114": "P", "4-501.115": "C", "4-501.116": "Pf","4-501.12": "C",
    "4-501.13": "C",  "4-501.14": "C",  "4-501.15": "C",  "4-501.16": "C",
    "4-501.17": "Pf", "4-501.18": "C",  "4-501.19": "Pf",
    "4-502.11": "Pf", "4-502.12": "P",  "4-502.13": "C",  "4-502.14": "C",
    "4-601.11": "Pf", "4-602.11": "P",  "4-602.12": "C",  "4-602.13": "C",
    "4-603.11": "C",  "4-603.12": "C",  "4-603.13": "C",  "4-603.14": "C",
    "4-603.15": "C",  "4-603.16": "C",  "4-702.11": "P",  "4-703.11": "P",
    "4-801.11": "C",  "4-802.11": "C",  "4-803.11": "C",  "4-803.12": "C",
    "4-803.13": "C",  "4-901.11": "C",  "4-901.12": "C",  "4-902.11": "C",
    "4-902.12": "C",  "4-903.11": "C",  "4-903.12": "Pf", "4-904.11": "C",  "4-904.12": "C",
    "4-904.13": "C",  "4-904.14": "C",
    "5-101.11": "P",  "5-101.12": "P",  "5-101.13": "P",
    "5-102.11": "P",  "5-102.12": "P",  "5-102.13": "Pf", "5-102.14": "C",
    "5-103.11": "Pf", "5-103.12": "Pf", "5-104.11": "Pf", "5-104.12": "Pf",
    "5-201.11": "P",
    "5-202.11": "P",  "5-202.12": "Pf", "5-202.13": "P",  "5-202.14": "P",
    "5-202.15": "C",  "5-203.11": "Pf", "5-203.12": "C",  "5-203.13": "C",
    "5-203.14": "P",  "5-203.15": "P",  "5-204.11": "Pf", "5-204.12": "C",
    "5-204.13": "C",  "5-205.11": "Pf", "5-205.12": "P",  "5-205.13": "Pf",
    "5-205.14": "P",  "5-205.15": "P",
    "5-301.11": "P",  "5-302.11": "C",  "5-302.12": "C",  "5-302.13": "C",
    "5-302.14": "C",  "5-302.15": "C",  "5-302.16": "P",  "5-303.11": "P",
    "5-303.12": "C",  "5-303.13": "C",  "5-304.11": "P",  "5-304.12": "C",
    "5-304.13": "C",  "5-304.14": "P",
    "5-401.11": "C",  "5-402.11": "P",  "5-402.12": "C",  "5-402.13": "P",
    "5-402.14": "Pf", "5-402.15": "C",  "5-403.11": "P",  "5-403.12": "C",
    "5-501.11": "C",  "5-501.110": "C", "5-501.111": "C", "5-501.112": "C",
    "5-501.113": "C", "5-501.114": "C", "5-501.115": "C", "5-501.116": "C",
    "5-501.12": "C",  "5-501.13": "C",  "5-501.14": "C",  "5-501.15": "C",
    "5-501.16": "C",  "5-501.17": "C",  "5-501.18": "C",  "5-501.19": "C",
    "5-502.11": "C",  "5-502.12": "C",  "5-503.11": "C",
    "6-101.11": "C",  "6-102.11": "C",
    "6-201.11": "C",  "6-201.12": "C",  "6-201.13": "C",  "6-201.14": "C",
    "6-201.15": "C",  "6-201.16": "C",  "6-201.17": "C",  "6-201.18": "C",
    "6-202.11": "C",  "6-202.110": "C", "6-202.111": "P", "6-202.112": "C",
    "6-202.12": "C",  "6-202.13": "C",  "6-202.14": "C",  "6-202.15": "C",
    "6-202.16": "C",  "6-202.17": "C",  "6-202.18": "C",  "6-202.19": "C",
    "6-301.11": "Pf", "6-301.12": "Pf", "6-301.13": "C",  "6-301.14": "C",
    "6-302.11": "Pf", "6-303.11": "C",  "6-304.11": "C",  "6-305.11": "C",
    "6-402.11": "C",  "6-403.11": "C",  "6-404.11": "Pf",
    "6-501.11": "C",  "6-501.110": "C", "6-501.111": "Pf","6-501.112": "C",
    "6-501.113": "C", "6-501.114": "C", "6-501.115": "Pf","6-501.12": "C",
    "6-501.13": "C",  "6-501.14": "C",  "6-501.15": "Pf", "6-501.16": "C",
    "6-501.17": "C",  "6-501.18": "C",  "6-501.19": "C",
    "7-101.11": "Pf", "7-102.11": "Pf", "7-201.11": "P",  "7-202.11": "Pf",
    "7-202.12": "P",  "7-203.11": "P",  "7-204.11": "P",  "7-204.12": "P",  "7-204.13": "P",
    "7-204.14": "P",  "7-205.11": "P",  "7-206.11": "P",  "7-206.12": "P",
    "7-206.13": "P",  "7-207.11": "P",  "7-207.12": "P",
    "7-208.11": "P",  "7-209.11": "C",  "7-301.11": "P",
    "8-103.11": "Pf", "8-103.12": "P",  "8-201.13": "C",  "8-201.14": "Pf",
    # Subsection overrides — where lettered sub-paragraphs differ from the base section severity
    "2-201.11(B)": "Pf", "2-201.11(E)": "Pf",
    "3-202.11(E)": "Pf", "3-202.11(F)": "Pf",
    "3-202.110(A)": "Pf",
    "3-203.11(B)": "Pf",
    "3-301.11(C)": "Pf",
    "3-304.15(A)": "P",  "3-304.15(B)": "C",  "3-304.15(C)": "C",  "3-304.15(D)": "C",
    "3-306.13(A)": "P",  "3-306.13(B)": "Pf", "3-306.13(C)": "Pf",
    "3-401.12(A)": "C",  "3-401.12(B)": "C",  "3-401.12(C)": "P",  "3-401.12(D)": "C",
    "3-401.14(F)": "Pf",
    "3-404.11(A)": "P",  "3-404.11(B)": "Pf",
    "3-501.19(A)": "Pf",
    "3-502.12(B)": "Pf",
    "3-801.11(G)": "C",
    "4-204.110(A)": "P", "4-204.110(B)": "Pf",
    "4-401.11(C)": "C",
    "4-502.11(A)": "C",  "4-502.11(B)": "Pf", "4-502.11(C)": "C",
    "4-601.11(A)": "Pf", "4-601.11(B)": "C",  "4-601.11(C)": "C",
    "5-205.12(B)": "Pf",
    "6-501.111(A)": "C", "6-501.111(B)": "C", "6-501.111(D)": "C",
    "7-202.12(C)": "Pf",
    "7-207.11(A)": "Pf",
    "7-208.11(A)": "Pf",
    "8-103.12(A)": "Pf",
}

def code_weight(code):
    """Look up severity weight from FDA code string. Falls back to base code (no subsection)."""
    sev = CODE_SEVERITY.get(code)
    if sev is None:
        base = re.sub(r'\([A-Za-z]\)$', '', code)
        sev = CODE_SEVERITY.get(base)
    if sev == "P":  return 3
    if sev == "Pf": return 2
    return 1

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


def rescrape_ri(dry_run=False):
    """
    Re-fetch HTML violation codes for all existing RI inspections and update
    any whose score changes as a result of proper FDA code-based severity.

    Skips restaurants with zero violations (nothing to improve).
    Only writes to the DB when the recomputed score differs from what's stored.
    Clears ai_summary only for restaurants whose latest inspection score changed.
    """
    from sqlalchemy import func, exists

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
            .all()
        )

        total = len(has_violation)
        print(f"Checking {total} RI restaurants with violations{'  (dry run)' if dry_run else ''}...")
        sys.stdout.flush()

        total_checked = 0
        total_updated = 0
        total_skipped = 0

        for idx, restaurant in enumerate(has_violation):
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

            if (idx + 1) % 100 == 0:
                print(f"  [{idx+1}/{total}] {total_updated} updated so far...")
                sys.stdout.flush()

            time.sleep(0.3)

        if not dry_run:
            db.session.commit()

        print(f"\nRescrape {'(dry run) ' if dry_run else ''}complete:")
        print(f"  {total_checked:,} inspections checked with HTML codes")
        print(f"  {total_updated:,} inspections updated (score changed)")
        print(f"  {total_skipped:,} restaurants skipped (API returned nothing)")


def main():
    days    = 5
    rescrape = '--rescrape' in sys.argv
    dry_run  = '--dry-run'  in sys.argv
    for arg in sys.argv[1:]:
        if arg.startswith('--days='):
            days = int(arg.split('=', 1)[1])

    if rescrape:
        rescrape_ri(dry_run=dry_run)
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
