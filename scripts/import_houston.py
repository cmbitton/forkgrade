#!/usr/bin/env python3
"""
Houston health inspection importer.

Data source: https://houston-tx.healthinspections.us/media/ (Tyler Technologies HealthSpace)

This is NOT a REST API — it's a session-based ColdFusion HTML portal.

Flow:
  1. POST to search.cfm with a date range → receive CFID/CFTOKEN session cookies
     and the first page of search results HTML
  2. Parse result HTML to collect (facility_id, inspection_id) pairs; paginate
  3. GET search.cfm?q=d&f={fid}&i={iid} with session cookies → inspection detail HTML
  4. Parse violations: code, description, severity, corrected-on-site status

Violation severity (Houston Code of Ordinances Ch. 20-21):
  Substantial Health Violation → critical  (weight 3)
  Serious Health Violation     → major     (weight 2)
  General Health Violation     → minor     (weight 1)

Score formula (same as RI/NYC):
  risk_score = sum of violation weights
  score      = round(100 × exp(−risk_score × 0.05))

Usage:
  python3 scripts/import_houston.py              # last 7 days (default)
  python3 scripts/import_houston.py --days=30    # last N days
  python3 scripts/import_houston.py --full       # from 2022-01-01 to today
  python3 scripts/import_houston.py --dry-run    # parse only, no DB writes
"""

import http.cookiejar
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

if not os.environ.get('DATABASE_URL'):
    from dotenv import load_dotenv
    load_dotenv()

# FDA Food Code severity map (shared across all importers).
# Keys are FDA Food Code section numbers; values are 'P', 'Pf', or 'C'.
try:
    sys.path.insert(0, str(Path(__file__).parent))
    from fda_codes import CODE_SEVERITY as _FDA_CODE_SEVERITY
except Exception:
    _FDA_CODE_SEVERITY = {}

_PF_TO_SEVERITY = {'P': 'critical', 'Pf': 'major', 'C': 'minor'}

# FDA codes that appear in Houston inspections but are not in RI's CODE_SEVERITY
# (Chapter 8 = Compliance/Enforcement; not cited by RI inspectors).
_HOUSTON_FDA_EXTRAS = {
    '8-301.11': 'P',   # operating without a valid permit — Priority (critical)
    '8-302.11': 'Pf',  # permit application requirements — Priority Foundation
    '8-304.11': 'Pf',  # permit not posted — Priority Foundation
}

BASE_URL   = 'https://houston-tx.healthinspections.us/media'
SEARCH_URL = f'{BASE_URL}/search.cfm'
REGION     = 'houston'
STATE      = 'TX'
DELAY      = 1.0   # seconds between requests (be polite; server is CF and rate-limits)

# Ch. 47 = FOG (Fats, Oil & Grease) ordinance — grease trap inspections, not food safety
_FOG_CODE_RE = re.compile(r'^(?:COH-)?47-')

_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept':     'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Referer':    BASE_URL + '/search.cfm',
}


# ── Severity ──────────────────────────────────────────────────────────────────

_SEV_WEIGHTS = {'critical': 3, 'major': 2, 'minor': 1}

def _map_severity(text: str) -> str:
    t = text.lower()
    if 'substantial' in t:
        return 'critical'
    if 'serious' in t:
        return 'major'
    return 'minor'


# City of Houston ordinance violation severity lookup.
# Keys are the violation code with the "COH-" prefix stripped.
# An exact subitem match is tried first; then the section-level default.
# Source: https://houston-tx.healthinspections.us/media/violations.cfm
_COH_SEVERITY: dict[str, str] = {
    # ── Section-level defaults ────────────────────────────────────────────────
    '20-1':       'minor',    # Sec. 20-001: refused samples / denial of entry
    '20-2':       'major',    # Sec. 20-002: tagged food used or not held
    '20-3':       'major',    # Sec. 20-003: failed to follow rules/regulations
    '20-4':       'minor',    # Sec. 20-004: charity bake sale violations
    '20-17':      'major',    # Sec. 20-017: TFER non-compliance
    '20-19':      'major',    # Sec. 20-019: default Serious; (b) → critical
    '20-20':      'major',    # Sec. 20-020: default Serious; (c) → minor, (g) → critical
    '20-21.01':   'major',    # Sec. 20-021.01: food condition/source
    '20-21.02':   'major',    # Sec. 20-021.02: food protection/temp; (c) → critical
    '20-21.03':   'major',    # Sec. 20-021.03: food storage/temp control
    '20-21.04':   'major',    # Sec. 20-021.04: food preparation
    '20-21.05':   'major',    # Sec. 20-021.05: food service/display
    '20-21.06':   'major',    # Sec. 20-021.06: food transport
    '20-21.07':   'major',    # Sec. 20-021.07: employee communicable disease
    '20-21.08':   'major',    # Sec. 20-021.08: handwashing/hygiene; (d)(e)(f) → minor
    '20-21.09':   'major',    # Sec. 20-021.09: employee food/tobacco/cleanliness
    '20-21.10':   'minor',    # Sec. 20-021.10: equipment (mostly General); (i)(j) → major
    '20-21.11':   'major',    # Sec. 20-021.11: utensil washing/sanitization
    '20-21.12':   'major',    # Sec. 20-021.12: sanitization methods; (a) → critical
    '20-21.13':   'major',    # Sec. 20-021.13: dishwasher sanitization
    '20-21.14':   'major',    # Sec. 20-021.14: equipment/utensil handling
    '20-21.15':   'major',    # Sec. 20-021.15: water supply; (a)(c) → critical
    '20-21.16':   'critical', # Sec. 20-021.16: sewage disposal — always Substantial
    '20-21.17':   'major',    # Sec. 20-021.17: plumbing/cross-connections; (b)(d)(e) → minor
    '20-21.18':   'minor',    # Sec. 20-021.18: toilet facilities
    '20-21.19':   'major',    # Sec. 20-021.19: handwashing sink availability; (d) → minor
    '20-21.20':   'minor',    # Sec. 20-021.20: garbage/refuse; (a)(05) → major
    '20-21.21':   'major',    # Sec. 20-021.21: pest control; (b) → minor
    '20-21.22':   'minor',    # Sec. 20-021.22: floors
    '20-21.23':   'minor',    # Sec. 20-021.23: walls/ceilings; (i)(01)(02) → major
    '20-21.24':   'minor',    # Sec. 20-021.24: lighting
    '20-21.25':   'minor',    # Sec. 20-021.25: ventilation
    '20-21.26':   'minor',    # Sec. 20-021.26: dressing rooms/lockers
    '20-21.27':   'major',    # Sec. 20-021.27: poisonous/toxic materials
    '20-21.28':   'minor',    # Sec. 20-021.28: premises maintenance
    '20-22':      'major',    # Sec. 20-022: mobile food unit compliance
    '20-23':      'major',    # Sec. 20-023: temporary food establishment
    '20-24':      'major',    # Sec. 20-024: communicable disease/exclusion; (b) → critical
    '20-25':      'minor',    # Sec. 20-025: construction without approved plans
    '20-26':      'minor',    # Sec. 20-026: required postings (Heimlich, notices)
    '20-36':      'critical', # Sec. 20-036: operating without Food Dealer's Permit
    '20-37':      'critical', # Sec. 20-037: mobile unit without permit/medallion; (c) → minor
    '20-39':      'minor',    # Sec. 20-039: frozen dessert permit
    '20-53':      'major',    # Sec. 20-053: food service manager certification failures
    '20-56':      'minor',    # Sec. 20-056: cert posting/possession
    '20-61':      'minor',    # Sec. 20-061: fraudulent cert use
    '20-63':      'major',    # Sec. 20-063: cert from other approved agency
    '20-64':      'major',    # Sec. 20-064: bed and breakfast cert
    '40-8':       'minor',    # Sec. 40-008: illegal booth on public property
    '40-9':       'major',    # Sec. 40-009: frozen dessert vehicle violations; (b) → minor

    # ── Subitem overrides — Substantial (critical) ────────────────────────────
    '20-19(b)':        'critical',  # operating non-conforming establishment
    '20-20(g)':        'critical',  # resumed operation before conditions resolved
    '20-21.02(c)':     'critical',  # failed to discontinue operation in emergency
    '20-21.11(b)':     'critical',  # three-compartment sink without hot/cold running water
    '20-21.12(a)':     'critical',  # no three-compartment sink for manual washing
    '20-21.15(a)':     'critical',  # failure to provide enough potable water
    '20-21.15(c)':     'critical',  # failure to provide water at required temps
    '20-24(b)':        'critical',  # continued operation after restriction/exclusion order
    '20-36(a)':        'critical',  # operating without permit (subitem explicit)
    '20-37(a)':        'critical',  # mobile unit operating without permit
    '20-37(b)':        'critical',  # mobile unit without medallion (operating)

    # ── Subitem overrides — General (minor) ───────────────────────────────────
    '20-20(c)':        'minor',   # inspection report not posted
    '20-21.08(d)':     'minor',   # employee outer clothing not clean
    '20-21.08(e)':     'minor',   # no effective hair restraint
    '20-21.08(f)':     'minor',   # nail polish/jewelry while preparing food
    '20-21.10(a)':     'minor',   # equipment/utensil construction materials
    '20-21.10(b)':     'minor',   # equipment design/fabrication
    '20-21.10(c)':     'minor',   # food-contact surfaces for in-place cleaning
    '20-21.10(d)':     'minor',   # in-place cleaning design
    '20-21.10(e)':     'minor',   # fixed equipment for pressure spray cleaning
    '20-21.10(f)':     'minor',   # indicating thermometers
    '20-21.10(g)':     'minor',   # nonfood-contact surfaces
    '20-21.10(h)':     'minor',   # ventilation hood
    '20-21.10(a)(04)': 'major',   # mollusk/crustacean shells reused as containers — Serious
    '20-21.10(a)(05)': 'major',   # single-service articles reused — Serious
    '20-21.10(a)(06)': 'major',   # canvas/cloth used as food-contact surface — Serious
    '20-21.10(a)(08)': 'major',   # single-use glove misuse — Serious
    '20-21.10(b)(02)': 'major',   # equipment with unsafe lubricants — Serious
    '20-21.10(b)(03)': 'major',   # beverage tubing not properly maintained — Serious
    '20-21.10(i)':     'major',   # existing equipment not in good repair/sanitary — Serious
    '20-21.10(j)':     'major',   # equipment under sewer/water lines — Serious
    '20-21.10(k)':     'minor',   # table/counter mounted equipment clearance
    '20-21.10(l)':     'minor',   # floor mounted equipment clearance
    '20-21.10(m)':     'minor',   # plumbing installation at fixtures
    '20-21.10(n)':     'minor',   # aisle widths
    '20-21.17(b)':     'minor',   # non-potable pipe identification
    '20-21.17(d)':     'minor',   # grease trap
    '20-21.17(e)':     'minor',   # garbage grinder
    '20-21.19(d)':     'minor',   # handwashing sink/fixtures not kept clean/good repair
    '20-21.20(a)(01)': 'minor',   # garbage container construction
    '20-21.20(a)(02)': 'minor',   # garbage container not covered when full
    '20-21.20(a)(03)': 'minor',   # outside containers not covered
    '20-21.20(a)(04)': 'minor',   # insufficient garbage containers
    '20-21.20(a)(05)': 'major',   # soiled containers cleaned in way that contaminates food
    '20-21.20(b)':     'minor',   # outside storage areas
    '20-21.20(c)':     'minor',   # disposal frequency
    '20-21.21(b)':     'minor',   # openings to outside not protected from rodents/insects
    '20-21.23(i)(01)': 'major',   # floors/walls cleaned during food exposure — Serious
    '20-21.23(i)(02)': 'major',   # mop water disposed into lavatory/sink — Serious
    '20-36(f)':        'major',   # restricted warehouse with PHF — Serious
    '20-37(c)':        'minor',   # no medallion affixed/displayed
    '40-9(b)':         'minor',   # frozen dessert truck warning sign violations
}


def _fda_severity(code: str) -> str:
    """
    Severity for an FDA Food Code violation (e.g. '3-302.11(A)(1)(a)').

    Strips parenthetical subitem groups one at a time from right to left until
    a match is found in CODE_SEVERITY or the Houston-specific extras dict.
    e.g. '3-302.11(A)(1)(a)' → '3-302.11(A)(1)' → '3-302.11(A)' → '3-302.11' → 'P' → critical
    """
    current = code
    while True:
        pf = _HOUSTON_FDA_EXTRAS.get(current) or _FDA_CODE_SEVERITY.get(current)
        if pf:
            return _PF_TO_SEVERITY.get(pf, 'minor')
        stripped = re.sub(r'\([^)]+\)\s*$', '', current).strip()
        if stripped == current:
            break
        current = stripped
    return 'minor'


def _coh_severity(code: str) -> str:
    """
    Return the severity for a City of Houston ordinance code (COH-xx-xx).

    Strategy:
      1. Strip the 'COH-' prefix.
      2. Codes not in Chapter 20 or 40 (e.g. COH-21-xxx smoking) → 'minor'.
      3. Try an exact subitem match (e.g. '20-21.10(i)').
      4. Fall back to section-level default (strip from first '(' char).
      5. If still not found, return 'minor'.
    """
    rest = code[4:].lower()   # strip 'COH-' and normalise case for dict lookup
    # Normalise leading zeros in section numbers: '20-001' → '20-1', '40-009' → '40-9'
    rest = re.sub(r'(?<=-)0+(\d)', r'\1', rest)
    if not (rest.startswith('20-') or rest.startswith('40-')):
        return 'minor'
    # 1. Exact match (handles subitems like '20-21.11(b)')
    sev = _COH_SEVERITY.get(rest)
    if sev:
        return sev
    # 2. For nested subitems like '20-21.10(a)(05)', try stripping the last () group
    #    to get the parent subitem '20-21.10(a)' before falling to section default.
    first_paren = rest.find('(')
    last_paren  = rest.rfind('(')
    if first_paren > 0 and last_paren > first_paren:
        sev = _COH_SEVERITY.get(rest[:last_paren])
        if sev:
            return sev
    # 3. Section-level default (strip from first '(')
    if first_paren > 0:
        sev = _COH_SEVERITY.get(rest[:first_paren])
        if sev:
            return sev
    return 'minor'


# ── Score ─────────────────────────────────────────────────────────────────────

def compute_score(violations: list) -> tuple:
    risk  = sum(_SEV_WEIGHTS.get(v['severity'], 1) for v in violations)
    score = round(100 * math.exp(-risk * 0.05))
    return risk, score


def score_to_result(score: int) -> str:
    if score >= 80:
        return 'Pass'
    if score >= 60:
        return 'Pass with Conditions'
    return 'Fail'


# ── Slug helpers ──────────────────────────────────────────────────────────────

def make_slug(name: str, city: str) -> str:
    s = re.sub(r"['\u2019]", '', name.lower().strip())
    s = re.sub(r'[\s_/&]+', '-', s)
    s = re.sub(r'[^a-z0-9-]', '', s)
    s = re.sub(r'-+', '-', s).strip('-')
    c = re.sub(r'[^a-z0-9-]', '', (city or 'houston').lower().replace(' ', '-'))
    return f'{s}-{c}'


def unique_slug(base: str, seen: set) -> str:
    slug, n = base, 2
    while slug in seen:
        slug = f'{base}-{n}'
        n += 1
    seen.add(slug)
    return slug


# ── Address parsing ───────────────────────────────────────────────────────────

_STATE_ZIP_RE = re.compile(r'\b([A-Z]{2})[,\s]+(\d{5})\b')

# Street suffixes that should never be mistaken for a city name.
_STREET_SUFFIXES = {
    'ALY', 'AVE', 'BLVD', 'BND', 'BR', 'BYP', 'CIR', 'CLOSE', 'CORR',
    'COURT', 'CR', 'CRST', 'CT', 'CV', 'DR', 'EXPY', 'EXT', 'FWY',
    'GDNS', 'GLN', 'GRN', 'GRV', 'HWY', 'HOLW', 'HTS', 'IS', 'KNL',
    'LK', 'LN', 'LOOP', 'MALL', 'MDW', 'MNR', 'PKWY', 'PL', 'PLZ',
    'PT', 'RD', 'RDGE', 'ROW', 'RUN', 'SQ', 'ST', 'STE', 'TER',
    'TPKE', 'TR', 'TRACE', 'TRAIL', 'TRL', 'TRWY', 'TUNL', 'VW',
    'WAY', 'WALK', 'XING',
    # Numbered/directional suffixes that sometimes trail an address
    'N', 'S', 'E', 'W', 'NE', 'NW', 'SE', 'SW',
}

# Multi-word city names that the single-token parser can't detect; keyed by zip.
_ZIP_CITY_OVERRIDE = {
    '77459': 'Missouri City',
    '77489': 'Missouri City',
    '77477': 'Stafford',
    '77478': 'Sugar Land',
    '77479': 'Sugar Land',
    '77498': 'Sugar Land',
    '77338': 'Humble',
    '77339': 'Kingwood',
    '77345': 'Kingwood',
    '77346': 'Humble',
    '77396': 'Humble',
    '77546': 'Friendswood',
    '77598': 'Webster',
}


def parse_address(raw: str):
    """
    '609 W GULF BANK RD HOUSTON TX 77037' → (street, city, state, zip)
    '123 MAIN ST TX 77037'                → ('123 Main St', 'Houston', 'TX', '77037')
    """
    raw = re.sub(r'<[^>]+>', ' ', raw)   # strip any leftover HTML tags
    raw = re.sub(r'\s+', ' ', raw).strip().upper()
    raw = raw.replace(',', ' ')

    m = _STATE_ZIP_RE.search(raw)
    if not m:
        return raw.title(), 'Houston', STATE, None

    state  = m.group(1)
    zip5   = m.group(2)
    before = raw[:m.start()].strip()

    if zip5 in _ZIP_CITY_OVERRIDE:
        city = _ZIP_CITY_OVERRIDE[zip5]
        city_upper = city.upper()
        if before.endswith(city_upper):
            before = before[:-len(city_upper)].strip()
        else:
            n_tokens = len(city_upper.split())
            parts = before.rsplit(None, n_tokens)
            before = parts[0] if len(parts) > 1 else before
        return before.title(), city, state, zip5

    # Last word before state abbreviation = city, unless it's a street suffix.
    parts = before.rsplit(None, 1)
    if len(parts) == 2 and parts[1] not in _STREET_SUFFIXES:
        return parts[0].title(), parts[1].title(), state, zip5
    return before.title(), 'Houston', state, zip5


# ── HTTP session ──────────────────────────────────────────────────────────────

def _make_opener():
    jar    = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
    opener.addheaders = list(_HEADERS.items())
    return opener, jar


def _cf_params(jar) -> dict:
    """
    Extract CFID/CFTOKEN from the cookie jar and return them as a dict so
    they can be appended to GET URLs. Older ColdFusion installs require session
    tokens as URL params, not just cookies.
    """
    params = {}
    for cookie in jar:
        if cookie.name.upper() == 'CFID':
            params['CFID'] = cookie.value
        elif cookie.name.upper() == 'CFTOKEN':
            params['CFTOKEN'] = cookie.value
    return params


def _post(opener, url: str, params: dict) -> str:
    data = urllib.parse.urlencode(params).encode()
    req  = urllib.request.Request(url, data=data, method='POST',
                                  headers={'Content-Type': 'application/x-www-form-urlencoded'})
    with opener.open(req, timeout=25) as r:
        return r.read().decode('utf-8', errors='replace')


def _get(opener, url: str, retries: int = 3) -> str:
    for attempt in range(retries):
        req = urllib.request.Request(url, headers={'Referer': SEARCH_URL})
        try:
            with opener.open(req, timeout=25) as r:
                return r.read().decode('utf-8', errors='replace')
        except urllib.error.HTTPError as exc:
            if exc.code in (502, 503) and attempt < retries - 1:
                wait = 2 ** (attempt + 1)
                print(f"  {exc.code} on attempt {attempt+1}, retrying in {wait}s...")
                time.sleep(wait)
                continue
            raise
    raise RuntimeError(f"Failed after {retries} attempts: {url}")


# ── HTML parsing ──────────────────────────────────────────────────────────────

# Links to inspection detail pages: search.cfm?q=d&f=FACID&i=INSPID&...
_DETAIL_LINK_RE = re.compile(
    r'search\.cfm\?[^"\'<>]*?(?:&amp;|&)f=([^&"\'<>\s]+)[^"\'<>]*?(?:&amp;|&)i=([^&"\'<>\s]+)',
    re.IGNORECASE,
)

# Total results count: "123 records found" or similar
_TOTAL_RE = re.compile(r'(\d[\d,]*)\s+(?:records?|establishments?|results?)', re.IGNORECASE)

# Violation entry: ddrivetip tooltip + "Houston Ordinance Violation: CODE" text
# Captures (tooltip_text, code) — code may be empty for category-header rows
_VIOLATION_RE = re.compile(
    r"ddrivetip\('([^']+)',\s*'white',\s*400\)[^>]*>Houston Ordinance Violation:\s*([^<]*?)\s*</a>",
    re.IGNORECASE | re.DOTALL,
)

# "Corrected on site" status in a table cell
_COS_RE = re.compile(r'corrected[\s\-]+on[\s\-]+site', re.IGNORECASE)

# Inspection date anywhere on detail page (MM/DD/YYYY)
_DATE_RE = re.compile(r'\b(\d{1,2}/\d{1,2}/\d{4})\b')

# Inspection type from table header row (Routine / Reinspection / etc.)
_TYPE_RE = re.compile(
    r'<td[^>]*ge_tableData[^>]*>[^<]*(?:Routine|Reinspection|Complaint|Follow[- ]?up|Initial)[^<]*</td>',
    re.IGNORECASE,
)


def _strip(html: str) -> str:
    """Strip HTML tags and collapse whitespace."""
    s = re.sub(r'<[^>]+>', ' ', html)
    return re.sub(r'\s+', ' ', s).strip()


def parse_pairs(html: str) -> list:
    """Extract (facility_id, inspection_id) pairs from search results HTML."""
    # Normalise &amp; so regex works regardless of encoding
    html_norm = html.replace('&amp;', '&')
    seen, pairs = set(), []
    for m in _DETAIL_LINK_RE.finditer(html_norm):
        key = (m.group(1).strip(), m.group(2).strip())
        if key not in seen:
            seen.add(key)
            pairs.append({'facility_id': key[0], 'inspection_id': key[1]})
    return pairs


def parse_total(html: str) -> int:
    m = _TOTAL_RE.search(html)
    return int(m.group(1).replace(',', '')) if m else 0


def parse_detail(html: str, facility_id: str) -> dict | None:
    """
    Parse a Houston inspection detail page.

    Key HTML patterns observed:
      - Facility name:  <h3>NAME</h3>
      - Address:        plain text immediately after </h3>, e.g. "KINGWOOD TX, 77339 <br>"
      - Violations:     ddrivetip('DESCRIPTION','white',400) tooltip inside each
                        "Houston Ordinance Violation: CODE" link
      - Status:         separate <td> containing "Violation" / "Violation Corrected On Site"
      - Severity:       tooltip text starting with "Foodborne Illness Risk Factors" → critical;
                        otherwise minor (Houston doesn't label rows as Substantial/Serious/General
                        in the HTML — they use FDA item category numbers)
    """
    if len(html) < 500:
        return None   # empty / error page

    # ── Name ─────────────────────────────────────────────────────────────────
    h3_m = re.search(r'<h3>(.*?)</h3>', html, re.IGNORECASE | re.DOTALL)
    if not h3_m:
        return None
    name = _strip(h3_m.group(1)).title()
    if not name or len(name) < 2:
        return None

    # ── Address ───────────────────────────────────────────────────────────────
    # The address follows </h3> as two text nodes separated by <br>:
    #   "911 SOUTHERN HILLS RD\nKINGWOOD TX, 77339 <br>"
    # Grab up to 300 chars, replace <br> with space, strip other tags.
    addr_raw = ''
    after_h3 = html[h3_m.end():h3_m.end() + 300]
    after_h3 = re.sub(r'<br\s*/?>', ' ', after_h3, flags=re.IGNORECASE)
    after_h3 = re.sub(r'<[^>]+>', '', after_h3)
    addr_raw = re.sub(r'\s+', ' ', after_h3).strip()[:150]

    # ── Inspection date ───────────────────────────────────────────────────────
    insp_date = None
    for dm in _DATE_RE.finditer(html):
        try:
            d = datetime.strptime(dm.group(1), '%m/%d/%Y').date()
            if 2015 <= d.year <= date.today().year:
                insp_date = d
                break
        except ValueError:
            continue

    # ── Inspection type ───────────────────────────────────────────────────────
    insp_type = 'Routine'
    type_m = _TYPE_RE.search(html)
    if type_m:
        raw = _strip(type_m.group(0))
        if raw:
            insp_type = raw

    # ── Violations ────────────────────────────────────────────────────────────
    # Each violation has:
    #   ddrivetip('DESCRIPTION','white',400)...>Houston Ordinance Violation: CODE </a>
    # Followed (somewhere after) by a status <td>:
    #   <td ...>Violation<br> Corrected On Site</td>  OR  <td ...>Violation</td>
    #
    # We walk through all violation matches and pair each with the nearest
    # following status cell.

    violations = []
    seen_codes = set()

    # Split the HTML into segments at each violation link so we can check
    # what status follows each one.
    segments = list(_VIOLATION_RE.finditer(html))

    for idx, m in enumerate(segments):
        tooltip = m.group(1).strip()   # full ddrivetip text
        code    = m.group(2).strip().rstrip('.')

        # Empty-code rows are standalone category-reference entries
        # (e.g. "Foodborne Illness Risk Factors: 13") — not section headers.
        # Skip them; they don't govern the severity of subsequent violations.
        if not code:
            continue
        if code in seen_codes:
            continue
        seen_codes.add(code)

        # Description: the ddrivetip tooltip for a real violation starts with the
        # code itself, e.g. "3-302.11(A)(1)(a) FOOD shall be protected..."
        # Strip the leading code from the tooltip to get the description.
        desc = tooltip
        if tooltip.startswith(code):
            desc = tooltip[len(code):].strip().lstrip('.-– ')
        if not desc:
            desc = code

        # Severity:
        #   COH-20/40-xxx → Houston ordinance lookup table
        #   TAC-xxx       → Texas administrative/posting rules → always minor
        #   anything else → FDA Food Code → CODE_SEVERITY dict
        cu = code.upper()
        if cu.startswith('COH-'):
            severity = _coh_severity(cu)
        elif cu.startswith('TAC-'):
            severity = 'minor'
        else:
            severity = _fda_severity(code)

        # Corrected on site: look in the HTML between this match and the next
        end_pos   = segments[idx + 1].start() if idx + 1 < len(segments) else len(html)
        between   = html[m.end():end_pos]
        corrected = bool(_COS_RE.search(between))

        violations.append({
            'code':      code,
            'desc':      desc.capitalize(),
            'severity':  severity,
            'corrected': corrected,
        })

    return {
        'facility_id': facility_id,
        'name':        name,
        'address_raw': addr_raw,
        'date':        insp_date,
        'type':        insp_type,
        'violations':  violations,
    }


# ── Fetching ──────────────────────────────────────────────────────────────────

def _fetch_pairs_one_pass(opener, jar, sd: str, ed: str,
                          keyword: str = '', maxrows: int = 50) -> list:
    """Single paginated search pass. Returns list of (facility_id, inspection_id) pairs."""
    post_params = {
        'q': 's', 'e': '', 'k': keyword, 'r': '', 'tp': 'ALL',
        'sd': sd, 'ed': ed, 'z': 'ALL', 'm': 'LIKE',
        'maxrows': str(maxrows), 'Submit': 'Search',
    }
    try:
        html = _post(opener, SEARCH_URL, post_params)
    except Exception as exc:
        print(f"  Search POST failed (k={keyword!r}): {exc}")
        return []

    pairs = parse_pairs(html)
    start = maxrows + 1
    while len(parse_pairs(html)) >= maxrows:
        time.sleep(DELAY)
        qs = urllib.parse.urlencode({
            'start': start, 'Q': 's', 'E': '', 'K': keyword, 'R': '',
            'TP': 'ALL', 'Z': 'ALL', 'M': 'LIKE', 'MAXROWS': maxrows,
        })
        qs += f'&SD={sd}&ED={ed}'
        try:
            html = _get(opener, f'{SEARCH_URL}?{qs}')
        except Exception as exc:
            print(f"  Error at start={start}: {exc}")
            break
        page = parse_pairs(html)
        if not page:
            break
        pairs.extend(page)
        start += maxrows
    return pairs


def fetch_pairs_for_range(from_date: date, to_date: date, maxrows: int = 50):
    """
    POST a date-range search, paginate, return (opener, jar, pairs, sd, ed).
    opener+jar must be reused for subsequent detail-page GETs.

    When the portal cap (500 results) is hit, automatically retries with
    alphabetical keyword splits (A-M, N-Z) to recover overflow records.
    """
    sd = from_date.strftime('%m/%d/%Y')
    ed = to_date.strftime('%m/%d/%Y')
    print(f"  Searching Houston portal {sd} – {ed}...")

    opener, jar = _make_opener()
    cf = _cf_params(jar)
    print(f"  Session cookies: {cf}")

    if os.environ.get('HOUSTON_DEBUG_HTML'):
        post_params = {
            'q': 's', 'e': '', 'k': '', 'r': '', 'tp': 'ALL',
            'sd': sd, 'ed': ed, 'z': 'ALL', 'm': 'LIKE',
            'maxrows': str(maxrows), 'Submit': 'Search',
        }
        html = _post(opener, SEARCH_URL, post_params)
        with open('/tmp/houston_search.html', 'w') as fh:
            fh.write(html)
        print("  Search HTML dumped to /tmp/houston_search.html")

    all_pairs = _fetch_pairs_one_pass(opener, jar, sd, ed, keyword='', maxrows=maxrows)
    print(f"  Pass 1 (all): {len(all_pairs)} pairs.")

    if len(all_pairs) >= 500:
        # Portal cap hit — recover overflow with alphabetical splits
        print(f"  Portal cap hit ({len(all_pairs)} pairs). Running alpha splits to recover overflow...")
        seen = {(p['facility_id'], p['inspection_id']) for p in all_pairs}
        for keyword in ('a b c d e f g h i j k l m', 'n o p q r s t u v w x y z'):
            # Search each letter separately and collect unseen pairs
            for letter in keyword.split():
                time.sleep(DELAY)
                extra = _fetch_pairs_one_pass(opener, jar, sd, ed,
                                              keyword=letter, maxrows=maxrows)
                new = [p for p in extra
                       if (p['facility_id'], p['inspection_id']) not in seen]
                for p in new:
                    seen.add((p['facility_id'], p['inspection_id']))
                all_pairs.extend(new)
                if new:
                    print(f"    letter={letter!r}: +{len(new)} new pairs ({len(all_pairs)} total)")

    print(f"\n  Collected {len(all_pairs)} (facility, inspection) pairs.")
    return opener, jar, all_pairs, sd, ed


def fetch_detail(opener, jar, facility_id: str, inspection_id: str, sd: str, ed: str,
                 debug: bool = False) -> dict | None:
    # Cookies (CFID/CFTOKEN) are sent automatically by the opener's CookieJar.
    # Do NOT add them as URL params — some CF setups reject duplicate session tokens.
    # NOTE: sd/ed use raw slashes (not %2F) — CF does string-comparison on dates
    # and rejects the encoded form, falling back to the blank search page.
    qs  = urllib.parse.urlencode({
        'q': 'd', 'f': facility_id, 'i': inspection_id,
        'z': 'ALL', 'm': 'LIKE', 'maxrows': '50', 'e': '', 'tp': 'ALL',
    })
    qs += f'&sd={sd}&ed={ed}'
    url = f'{SEARCH_URL}?{qs}'
    if debug:
        print(f"  DEBUG detail URL: {url}")
    try:
        html = _get(opener, url)
        if debug:
            with open('/tmp/houston_detail.html', 'w') as fh:
                fh.write(html)
            print(f"  DEBUG detail HTML ({len(html)} chars) dumped to /tmp/houston_detail.html")
    except urllib.error.HTTPError as exc:
        if debug:
            try:
                body = exc.read().decode('utf-8', errors='replace')[:800]
            except Exception:
                body = '(could not read body)'
            print(f"  DEBUG HTTP {exc.code} body:\n{body}\n")
        if exc.code in (403, 404, 500, 502, 503):
            return None
        raise
    except Exception as exc:
        if debug:
            print(f"  DEBUG exception: {exc}")
        return None
    return parse_detail(html, facility_id)


# ── DB write ──────────────────────────────────────────────────────────────────

def write_to_db(records: list, app, db, Restaurant, Inspection, Violation):
    with app.app_context():
        existing = {
            r.source_id: r
            for r in Restaurant.query.filter_by(region=REGION)
                                     .filter(Restaurant.source_id.isnot(None)).all()
        }
        seen_slugs = {r.slug for r in existing.values()}
        seen_slugs |= {
            r.slug for r in
            Restaurant.query.filter(Restaurant.region != REGION)
                            .with_entities(Restaurant.slug).all()
        }
        new_r = new_i = skipped = 0

        for rec in records:
            fid  = rec['facility_id']
            name = (rec.get('name') or '').strip()
            if not name:
                skipped += 1
                continue

            insp_date = rec.get('date')
            if not insp_date:
                skipped += 1
                continue

            street, city, state, zip5 = parse_address(rec.get('address_raw', ''))

            # ── Get or create restaurant ──────────────────────────────────────
            if fid in existing:
                restaurant = existing[fid]
            else:
                slug = unique_slug(make_slug(name, city or 'houston'), seen_slugs)
                restaurant = Restaurant(
                    source_id    = fid,
                    name         = name,
                    slug         = slug,
                    address      = street,
                    city         = city,
                    state        = state,
                    zip          = zip5,
                    latitude     = None,
                    longitude    = None,
                    cuisine_type = None,
                    region       = REGION,
                )
                db.session.add(restaurant)
                db.session.flush()
                existing[fid] = restaurant
                new_r += 1

            # ── Skip FOG-only inspections ────────────────────────────────────
            # Ch. 47 codes (e.g. 47-512(b), COH-47-512(b)) are grease-trap
            # inspections, not food-safety.  Drop the record if it contains
            # ONLY FOG violations — mixed records are kept as-is.
            violations = rec.get('violations', [])
            if violations and all(
                _FOG_CODE_RE.match(v['code']) for v in violations
            ):
                skipped += 1
                continue

            # ── Score and write inspection ────────────────────────────────────
            risk, score = compute_score(violations)

            # ── Skip / replace duplicate inspections ──────────────────────────
            # If two inspections share a date (routine + reinspection), keep the
            # one with the higher risk score so we don't silently drop violations.
            existing_insp = Inspection.query.filter_by(
                restaurant_id=restaurant.id, inspection_date=insp_date
            ).first()
            if existing_insp:
                if risk > (existing_insp.risk_score or 0):
                    # Replace the existing zero/low-violation record
                    Violation.query.filter_by(inspection_id=existing_insp.id).delete()
                    existing_insp.score           = score
                    existing_insp.risk_score      = risk
                    existing_insp.result          = score_to_result(score)
                    existing_insp.inspection_type = rec.get('type') or 'Routine'
                    for v in violations:
                        db.session.add(Violation(
                            inspection_id     = existing_insp.id,
                            violation_code    = v['code'],
                            description       = v['desc'],
                            severity          = v['severity'],
                            corrected_on_site = v['corrected'],
                        ))
                skipped += 1
                continue

            insp = Inspection(
                restaurant_id   = restaurant.id,
                inspection_date = insp_date,
                score           = score,
                risk_score      = risk,
                grade           = None,
                result          = score_to_result(score),
                inspection_type = rec.get('type') or 'Routine',
                region          = 'houston',
            )
            db.session.add(insp)
            db.session.flush()
            new_i += 1

            for v in violations:
                db.session.add(Violation(
                    inspection_id     = insp.id,
                    violation_code    = v['code'],
                    description       = v['desc'],
                    severity          = v['severity'],
                    corrected_on_site = v['corrected'],
                ))

            old_latest = restaurant.latest_inspection_date
            if old_latest is None or insp_date > old_latest:
                if old_latest != insp_date:
                    restaurant.ai_summary = None
                restaurant.latest_inspection_date = insp_date

            if new_i % 250 == 0 and new_i > 0:
                db.session.commit()
                print(f"  Committed {new_i} inspections so far...")

        db.session.commit()
        return new_r, new_i, skipped


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    full_mode = '--full'    in sys.argv
    dry_run   = '--dry-run' in sys.argv
    debug     = '--debug'   in sys.argv
    days      = 7
    since     = None
    until     = None
    for arg in sys.argv[1:]:
        if arg.startswith('--days='):
            days = int(arg.split('=', 1)[1])
        elif arg.startswith('--since='):
            since = date.fromisoformat(arg.split('=', 1)[1])
        elif arg.startswith('--until='):
            until = date.fromisoformat(arg.split('=', 1)[1])

    today     = date.today()
    if full_mode:
        from_date = since or date(2018, 1, 1)
    else:
        from_date = since or (today - timedelta(days=days))
    to_date   = until or today

    # Chunk size: Houston portal hard-caps at 500 results per search.
    # 2018 data averages ~70 inspections/day → 2-day chunks ≈ 140, safely under cap.
    # The --chunk-days flag overrides if needed.
    chunk_days = 2
    for arg in sys.argv[1:]:
        if arg.startswith('--chunk-days='):
            chunk_days = int(arg.split('=', 1)[1])

    if full_mode or (to_date - from_date).days > chunk_days:
        chunks = []
        cur = from_date
        while cur < to_date:
            nxt = min(cur + timedelta(days=chunk_days), to_date)
            chunks.append((cur, nxt))
            cur = nxt + timedelta(days=1)
        chunks.reverse()   # process most recent first — recent data matters more
    else:
        chunks = [(from_date, to_date)]

    if not dry_run:
        from app import create_app
        from app.db import db
        from app.models.restaurant import Restaurant
        from app.models.inspection import Inspection
        from app.models.violation import Violation
        app = create_app()
    else:
        app = db = Restaurant = Inspection = Violation = None

    total_r = total_i = total_skipped = 0

    for chunk_idx, (chunk_from, chunk_to) in enumerate(chunks):
        opener, jar, pairs, sd, ed = fetch_pairs_for_range(chunk_from, chunk_to)
        if not pairs:
            continue

        chunk_records = []
        print(f"  Fetching {len(pairs)} detail pages...")
        if debug:
            print(f"  First 3 pairs: {pairs[:3]}")
        for idx, pair in enumerate(pairs):
            is_debug = debug and idx == 0
            detail = fetch_detail(opener, jar, pair['facility_id'], pair['inspection_id'],
                                  sd, ed, debug=is_debug)
            if detail:
                chunk_records.append(detail)
            if (idx + 1) % 20 == 0:
                print(f"  {idx+1}/{len(pairs)} fetched ({len(chunk_records)} valid)...", end='\r')
            time.sleep(DELAY)
            if debug and idx == 0:
                break   # only fetch one detail page in debug mode
        print()

        if dry_run:
            print(f"  --dry-run: {len(chunk_records)} records in chunk {chunk_idx+1}/{len(chunks)}")
            for r in chunk_records[:3]:
                print(f"    {r['name']} | {r['date']} | {len(r['violations'])} violations")
            continue

        if not chunk_records:
            continue

        new_r, new_i, skipped = write_to_db(
            chunk_records, app, db, Restaurant, Inspection, Violation
        )
        total_r       += new_r
        total_i       += new_i
        total_skipped += skipped
        print(f"  Chunk {chunk_idx+1}/{len(chunks)}: +{new_r} restaurants, +{new_i} inspections, {skipped} skipped")

    if dry_run:
        print("--dry-run complete.")
        return

    print(f"\nDone.")
    print(f"  {total_r:,} new restaurants")
    print(f"  {total_i:,} new inspections")
    print(f"  {total_skipped:,} skipped (duplicate or no date)")


if __name__ == '__main__':
    main()
