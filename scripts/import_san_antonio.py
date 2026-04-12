#!/usr/bin/env python3
"""
San Antonio (SAMHD) health inspection importer.

Data source: https://sanantonio-tx.healthinspections.us/san%20antonio/
(Tyler Technologies HealthSpace / ColdFusion — same platform as Houston,
but with a different portal layout.)

Flow:
  1. GET search.cfm with dtRng=YES&sd=MM/DD/YYYY&ed=MM/DD/YYYY (date range)
     → paginated list of (licenseID, inspectionID, name, address) rows
  2. For each pair, GET estab.cfm?licenseID=X&inspectionID=Y
     → parse score + violations

SA portal quirks:
  - Search returns one row per establishment (not per inspection). It shows
    the LATEST inspection for that establishment in the date range. To avoid
    missing same-establishment multi-inspections, we search one day at a time.
  - Hard cap of 100 rows per search; single-day searches never exceed ~80.
  - Pagination uses &start=N (1, 11, 21, …, 91) — 10 rows per page.
  - The public portal only exposes the latest 3 inspections per establishment
    via the "View Last 3 Inspections" (dead-looking) link — but date-range
    search already surfaces older inspections as long as their date falls in
    the requested range. Historical depth: inspections back to ~Jan 2023.

Severity (derived from Texas / FDA Food Code inspection form item numbers,
which SA prints at the start of every violation line, e.g. "19 Observed: ..."):
  Items 1-22  → Priority (P)            → critical  (weight 3)
  Items 23-29 → Priority Foundation (Pf)→ major     (weight 2)
  Items 30+   → Core (C)                → minor     (weight 1)

Score formula (shared across all regions):
  risk_score = sum of violation weights
  score      = round(100 × exp(−risk_score × 0.05))

Usage:
  python3 scripts/import_san_antonio.py              # last 7 days (default)
  python3 scripts/import_san_antonio.py --days=30    # last N days
  python3 scripts/import_san_antonio.py --full       # 2023-01-01 → today
  python3 scripts/import_san_antonio.py --dry-run    # parse only, no DB writes
  python3 scripts/import_san_antonio.py --debug      # dump first detail HTML
"""

import html as html_mod
import math
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

if not os.environ.get('DATABASE_URL'):
    from dotenv import load_dotenv
    load_dotenv()

# Shared FDA Food Code severity table (same lookup Houston uses).
try:
    sys.path.insert(0, str(Path(__file__).parent))
    from fda_codes import CODE_SEVERITY as _FDA_CODE_SEVERITY
except Exception:
    _FDA_CODE_SEVERITY = {}

_PF_TO_SEVERITY = {'P': 'critical', 'Pf': 'major', 'C': 'minor'}

# Texas Administrative Code 25 TAC §228.x — not FDA Food Code, so it isn't in
# CODE_SEVERITY.  These are the sections San Antonio actually cites.
# §228.31 covers Certified Food Manager / Food Handler training requirements
# (Priority Foundation in Texas); §228.2 is definitions (Core).
_SA_TAC_SEVERITY = {
    '228.31':    'Pf',   # CFM + Food Handler training (default)
    '228.31(a)': 'Pf',   # CFM certificate posted
    '228.31(b)': 'Pf',   # CFM present during operation
    '228.31(c)': 'Pf',   # CFM qualifications
    '228.31(d)': 'Pf',   # Food Handler training
    '228.221':   'P',    # HACCP / variance
    '228.222':   'P',    # Time/temp controlled for safety
}


BASE_URL   = 'https://sanantonio-tx.healthinspections.us/san%20antonio'
SEARCH_URL = f'{BASE_URL}/search.cfm'
ESTAB_URL  = f'{BASE_URL}/estab.cfm'
REGION     = 'san-antonio'
STATE      = 'TX'
DELAY      = 0.8   # seconds between requests

_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept':     'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Referer':    BASE_URL + '/',
}

PAGE_SIZE  = 10   # SA portal is hard-wired to 10 rows/page
MAX_ROWS   = 100  # Portal refuses start > 91 (10 pages × 10 rows)


# ── Severity ──────────────────────────────────────────────────────────────────

_SEV_WEIGHTS = {'critical': 3, 'major': 2, 'minor': 1}

# Matches an FDA Food Code section ID like "3-302.11" at the start of a code
# string — used to decide whether to send it through the FDA lookup or treat
# it as a TX Administrative Code citation.
_FDA_CODE_RE = re.compile(r'^\d-\d+\.\d+')


def item_severity(item_num: int) -> str:
    """
    Fallback severity tier based on the Texas / FDA inspection form item #.
    Used only when the printed code can't be resolved against CODE_SEVERITY.

    Items 1-22  : Foodborne Illness Risk Factors (Priority)        → critical
    Items 23-29 : Public Health Interventions (Priority Foundation)→ major
    Items 30+   : Good Retail Practices (Core)                     → minor
    """
    if 1 <= item_num <= 22:
        return 'critical'
    if 23 <= item_num <= 29:
        return 'major'
    return 'minor'


def _fda_severity(code: str) -> str | None:
    """
    Resolve an FDA Food Code citation to a severity tier.

    Strips parenthetical subitem groups one at a time from right to left,
    e.g. '3-302.11(A)(4)' → '3-302.11(A)' → '3-302.11' → 'P' → 'critical'.
    Also strips trailing '.N' subgroups for SA-style three-tier codes like
    '8-301.11.1' → '8-301.11' → 'P'.
    Returns None if no match is found at any strip level.
    """
    current = code
    while True:
        pf = _FDA_CODE_SEVERITY.get(current)
        if pf:
            return _PF_TO_SEVERITY.get(pf, 'minor')
        stripped = re.sub(r'\([^)]+\)\s*$', '', current).strip()
        if stripped == current:
            # No paren group to strip — try trimming a trailing ".N" subgroup
            # (e.g. '8-301.11.1' → '8-301.11').
            stripped = re.sub(r'\.\d+$', '', current)
            if stripped == current:
                return None
        current = stripped


def _tac_severity(code: str) -> str | None:
    """
    Severity lookup for TX Administrative Code citations (228.x).

    Specific sections we've classified (228.31, 228.221, 228.222) get
    their assigned Pf/P severity.  Any other 228.x citation defaults to
    'minor' — TAC §228 covers everything from equipment standards to
    posting requirements to variance approvals, most of which are Core,
    and we'd rather not over-weight unknowns.  This matches Houston's
    approach of treating unrecognised TAC codes as minor.
    """
    current = code
    while True:
        pf = _SA_TAC_SEVERITY.get(current)
        if pf:
            return _PF_TO_SEVERITY.get(pf, 'minor')
        stripped = re.sub(r'\([^)]+\)\s*$', '', current).strip()
        if stripped == current:
            break
        current = stripped
    # No specific match — fall back to 'minor' for anything in §228.
    if code.startswith('228.'):
        return 'minor'
    return None


# Tracks codes that miss BOTH the FDA table and the TAC table, so a run can
# report how often the item-number heuristic is actually firing.  Keyed by the
# raw code string (or '<no code>' when the violation had no code at all).
_UNRESOLVED_CODES: Counter = Counter()


def severity_for(code: str | None, item_num: int) -> str:
    """
    Determine violation severity by (in order):
      1. FDA Food Code lookup (for codes like 3-302.11(A)(4))
      2. TX Administrative Code lookup (for codes like 228.31(b))
      3. Inspection form item-number fallback
    """
    if code:
        if _FDA_CODE_RE.match(code):
            sev = _fda_severity(code)
            if sev:
                return sev
        elif code.startswith('228.'):
            sev = _tac_severity(code)
            if sev:
                return sev
    _UNRESOLVED_CODES[code or '<no code>'] += 1
    return item_severity(item_num)


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
    c = re.sub(r'[^a-z0-9-]', '', (city or 'san-antonio').lower().replace(' ', '-'))
    return f'{s}-{c}'


def unique_slug(base: str, seen: set) -> str:
    slug, n = base, 2
    while slug in seen:
        slug = f'{base}-{n}'
        n += 1
    seen.add(slug)
    return slug


# ── Address parsing ───────────────────────────────────────────────────────────

# Trailing state + 5-digit zip (may include +4 extension).
_STATE_ZIP_RE = re.compile(r'\b([A-Z]{2})\s+(\d{5})(?:-\d{4})?\s*$', re.IGNORECASE)

# Matches "San Antonio" at the end of a string, used as fallback when the
# address block has no comma separator between street and city.
_TRAIL_CITY_RE = re.compile(r'\s+san\s+antonio\s*$', re.IGNORECASE)


def parse_address(raw: str) -> tuple:
    """
    Parse "9627 ADAMS HILL DR San Antonio, TX 78245"
      → ('9627 Adams Hill Dr', 'San Antonio', 'TX', '78245').

    The SAMHD portal always uses "San Antonio" as the city and "TX" as the
    state (it only covers city-of-SA food permits), so we fall back to those
    defaults when a field is missing.
    """
    # Strip HTML fragments, entities, and inconsistent whitespace.
    s = re.sub(r'<[^>]+>', ' ', raw or '')
    s = html_mod.unescape(s)
    s = re.sub(r'\s+', ' ', s).strip()
    s = s.replace(' ,', ',')
    if not s:
        return None, 'San Antonio', STATE, None

    m = _STATE_ZIP_RE.search(s)
    if not m:
        return s.title(), 'San Antonio', STATE, None

    state  = m.group(1).upper()
    zip5   = m.group(2)
    before = s[:m.start()].rstrip(', ').strip()

    # The comma is the canonical city/street separator; use it when present.
    if ',' in before:
        street_part, city_part = before.rsplit(',', 1)
        city   = city_part.strip().title() or 'San Antonio'
        street = street_part.strip().title() or None
        return street, city, state, zip5

    # No comma — address is wedged together without punctuation. Peel off a
    # trailing "San Antonio" if it's there; otherwise punt and assume the
    # whole thing is the street.
    m2 = _TRAIL_CITY_RE.search(before)
    if m2:
        street = before[:m2.start()].strip().title() or None
        return street, 'San Antonio', state, zip5
    return before.title() or None, 'San Antonio', state, zip5


# ── HTTP ──────────────────────────────────────────────────────────────────────

def _get(url: str, retries: int = 3) -> str:
    req = urllib.request.Request(url, headers=_HEADERS)
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return r.read().decode('utf-8', errors='replace')
        except urllib.error.HTTPError as exc:
            if exc.code in (502, 503, 504) and attempt < retries - 1:
                wait = 2 ** (attempt + 1)
                print(f"  HTTP {exc.code}, retry {attempt+1}/{retries} in {wait}s")
                time.sleep(wait)
                continue
            raise
        except urllib.error.URLError as exc:
            if attempt < retries - 1:
                wait = 2 ** (attempt + 1)
                print(f"  URLError ({exc}), retry {attempt+1}/{retries} in {wait}s")
                time.sleep(wait)
                continue
            raise
    raise RuntimeError(f"Failed after {retries} attempts: {url}")


# ── Search-results HTML parsing ───────────────────────────────────────────────

# Each row starts with: <a href="estab.cfm?licenseID=NNN&inspectionID=NNN"><b>NAME</b></a>
# followed by a <div style="margin-bottom:10px;"> containing address + date.
_ROW_HEAD_RE = re.compile(
    r'<a\s+href="estab\.cfm\?licenseID=(\d+)&(?:amp;)?inspectionID=(\d+)">\s*'
    r'<b>([^<]+)</b>\s*</a>',
    re.IGNORECASE,
)

# "Displaying results 1 – 10 of 100"  (ampersand in &ndash; gets decoded away)
_TOTAL_RE = re.compile(r'Displaying results[^<]*?of\s+(\d+)', re.IGNORECASE)


def parse_search_rows(html: str, insp_date: date) -> list:
    """
    Return a list of dicts — one per (licenseID, inspectionID) row in the
    search results HTML. Each dict has keys:
      license_id, inspection_id, name, address_raw, date
    """
    rows = []
    seen = set()
    # HTML-decode the href so parsers see "&" consistently.
    norm = html.replace('&amp;', '&')

    matches = list(_ROW_HEAD_RE.finditer(norm))
    for idx, m in enumerate(matches):
        lic, iid = m.group(1), m.group(2)
        key = (lic, iid)
        if key in seen:
            continue
        seen.add(key)

        name = m.group(3).strip()
        if not name:
            continue

        # Address block: everything between this row's </a> and the next
        # <a href="estab.cfm..."> (or end of HTML).
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(norm)
        block = norm[m.end():end]

        # Strip the "Last Inspection Date" and trailing divs — we only want
        # the raw two-line street address.
        block_for_addr = re.split(
            r'<div[^>]*color:green', block, maxsplit=1, flags=re.IGNORECASE
        )[0]
        # Strip tags; collapse whitespace.
        text = re.sub(r'<br\s*/?>', ' ', block_for_addr, flags=re.IGNORECASE)
        text = re.sub(r'<[^>]+>', ' ', text)
        text = html_mod.unescape(text)
        addr_raw = re.sub(r'\s+', ' ', text).strip()

        rows.append({
            'license_id':    lic,
            'inspection_id': iid,
            'name':          name,
            'address_raw':   addr_raw,
            'date':          insp_date,
        })
    return rows


def parse_total(html: str) -> int:
    m = _TOTAL_RE.search(html)
    return int(m.group(1)) if m else 0


# ── Establishment-detail HTML parsing ─────────────────────────────────────────

# The demographic block contains the canonical name + address:
#   <b style="font-size:14px;">NAME</b>
#   <br />
#   <i> STREET <br /> CITY, TX ZIP <br /> ...</i>
_DEMOGRAPHIC_RE = re.compile(
    r'<div\s+id="demographic">\s*<b[^>]*>([^<]+)</b>\s*<br\s*/?>\s*<i>(.*?)</i>',
    re.IGNORECASE | re.DOTALL,
)

# Score cell:  <b>Score</b>\n  95  (just whitespace between tag and digits)
_SCORE_RE = re.compile(
    r'<b>\s*Score\s*</b>\s*(\d{1,3})',
    re.IGNORECASE,
)

# Inspection date cell:  <b>Date:</b> MM/DD/YYYY
_DATE_RE = re.compile(
    r'<b>\s*Date:?\s*</b>\s*(\d{1,2}/\d{1,2}/\d{4})',
    re.IGNORECASE,
)

# Each violation sits inside its own bgcolor=#EFEFEF div in the "Related Reports"
# section AFTER the score.  Format of the inner text:
#   NN Observed[:]? <observation text>. <CODE> <code description>.
#
# Where NN is the inspection form item number (1..48 ish) and <CODE> matches
# one of:  228.31(a) | 3-302.11(A)(4) | 5-202.13 | 4-601.11(A) | etc.
_VIOLATION_BLOCK_RE = re.compile(
    r'<div\s+style="background-color:#EFEFEF;padding:5px;">(.*?)</div>',
    re.IGNORECASE | re.DOTALL,
)

# Tight code pattern: only real FDA Food Code citations (`3-302.11(A)(4)`,
# `8-301.11.1`) or Texas Administrative Code §228 citations (`228.31(b)`).
# Loose patterns like `\d+[-.]\d+` falsely match temperature ranges
# ("50-100"), phone numbers ("207-8732"), and pH numbers ("45.8").
# The optional third number group covers three-tier FDA codes like
# 8-301.11.1 (permit requirements).
_CODE_PATTERN = r'(?:\d-\d+\.\d+(?:\.\d+)?|228\.\d+)(?:\([A-Za-z0-9]+\))*'

# SA violations always begin "NN " where NN is the inspection form item
# number.  The code (if the inspector cited one) can appear anywhere after
# that — right after the item number, embedded mid-sentence, or stuck to
# the end of a word with no whitespace ("frequently.6-501.12 Cleaning").
_VIOLATION_ITEM_RE = re.compile(r'^\s*(\d{1,2})\s+(.*)$', re.DOTALL)
_VIOLATION_CODE_RE = re.compile(_CODE_PATTERN)


def _strip_html(s: str) -> str:
    s = re.sub(r'<br\s*/?>', ' ', s, flags=re.IGNORECASE)
    s = re.sub(r'<[^>]+>', ' ', s)
    s = html_mod.unescape(s)
    return re.sub(r'\s+', ' ', s).strip()


def parse_detail(html: str, license_id: str, inspection_id: str) -> dict | None:
    """
    Parse an estab.cfm?licenseID=X&inspectionID=Y page.

    Returns a dict with keys: license_id, inspection_id, name, address_raw,
    date, score, violations  — or None if the page is empty / malformed.
    """
    if not html or len(html) < 500:
        return None

    # ── Name + address from the demographic sidebar ──────────────────────────
    name       = None
    addr_raw   = ''
    m = _DEMOGRAPHIC_RE.search(html)
    if m:
        name = _strip_html(m.group(1))
        addr_raw = _strip_html(m.group(2))

    if not name:
        return None

    # ── Inspection date ──────────────────────────────────────────────────────
    dm = _DATE_RE.search(html)
    insp_date = None
    if dm:
        try:
            insp_date = datetime.strptime(dm.group(1), '%m/%d/%Y').date()
        except ValueError:
            insp_date = None

    # ── Score ────────────────────────────────────────────────────────────────
    sm = _SCORE_RE.search(html)
    score = int(sm.group(1)) if sm else None

    # ── Violations ───────────────────────────────────────────────────────────
    # The detail page is filtered to one inspectionID, so the "Related
    # Reports" section contains exactly one scorecard followed by one
    # #EFEFEF div per violation.  We do NOT dedupe by (item, code) because
    # a single inspection can legitimately cite the same code multiple
    # times for different observations (e.g. two distinct 4-601.11(A)
    # cleanliness issues).
    violations = []

    # Slice after the score so we don't pick up any stray #EFEFEF divs
    # rendered elsewhere on the page (e.g. the address header).
    start = sm.end() if sm else 0

    for block in _VIOLATION_BLOCK_RE.finditer(html, start):
        text = _strip_html(block.group(1))
        if not text:
            # Empty #EFEFEF divs are rendered even on 100-score inspections.
            continue

        # Split the leading item number off the front, then search the rest
        # of the line for a code anywhere it appears.  This handles all three
        # SA violation shapes: code-first ("9 3-302.11(A)(2) Food..."),
        # code-inline ("21 Observed: ... 228.31(b) description..."), and
        # code-glued ("...clean frequently.6-501.12 Cleaning...").
        m_item = _VIOLATION_ITEM_RE.match(text)
        if not m_item:
            # Very malformed row — no leading item number at all.  Default
            # to item 48 so the heuristic falls through to 'minor'.
            item      = 48
            code      = None
            full_desc = text
        else:
            item = int(m_item.group(1))
            rest = m_item.group(2).strip()
            code_match = _VIOLATION_CODE_RE.search(rest)
            code = code_match.group(0) if code_match else None
            # If the code appears at the very start of the narrative (the
            # "code-first" SA format), strip it so the description doesn't
            # begin with the code redundantly.
            if code_match and code_match.start() == 0:
                full_desc = rest[code_match.end():].lstrip(' .:-–').strip()
            else:
                full_desc = rest

        violations.append({
            'item':     item,
            'code':     code,
            'desc':     full_desc[:500],
            'severity': severity_for(code, item),
        })

    return {
        'license_id':    license_id,
        'inspection_id': inspection_id,
        'name':          name,
        'address_raw':   addr_raw,
        'date':          insp_date,
        'score':         score,
        'violations':    violations,
    }


# ── Search driver ─────────────────────────────────────────────────────────────

def _build_search_url(start: int, sd: str, ed: str) -> str:
    params = {
        '1':        '1',
        'start':    str(start),
        'sd':       sd,
        'ed':       ed,
        'kw1':      '', 'kw2': '', 'kw3': '',
        'rel1':     'L.licenseName',
        'rel2':     'L.licenseName',
        'rel3':     'L.licenseName',
        'zc':       '',
        'dtRng':    'YES',
        'pre':      'similar',
        'smoking':  'ANY',
    }
    # Match the order the portal's JS uses so we stay recognizable to WAF.
    return f'{SEARCH_URL}?' + urllib.parse.urlencode(params)


def fetch_day_rows(day: date, debug: bool = False) -> list:
    """
    Fetch every (licenseID, inspectionID) row for a single inspection day.
    Returns a list of row-dicts produced by parse_search_rows().
    """
    sd = ed = day.strftime('%m/%d/%Y')
    url0 = _build_search_url(1, sd, ed)

    try:
        html0 = _get(url0)
    except Exception as exc:
        print(f"  {day} search failed: {exc}")
        return []

    total = parse_total(html0)
    if debug:
        with open('/tmp/sa_search.html', 'w') as fh:
            fh.write(html0)
        print(f"  DEBUG: dumped /tmp/sa_search.html (total={total})")

    if total == 0:
        return []

    all_rows = parse_search_rows(html0, day)
    if total <= PAGE_SIZE:
        return all_rows

    if total > MAX_ROWS:
        print(f"  WARNING: {day} has {total} inspections — portal caps at "
              f"{MAX_ROWS}; {total - MAX_ROWS} will be missed.")

    # Paginate remaining pages (start=11, 21, …).
    last_start = min(total, MAX_ROWS)
    start = PAGE_SIZE + 1
    while start <= last_start:
        time.sleep(DELAY)
        try:
            page_html = _get(_build_search_url(start, sd, ed))
        except Exception as exc:
            print(f"  {day} page start={start} failed: {exc}")
            break
        page_rows = parse_search_rows(page_html, day)
        if not page_rows:
            break
        existing = {(r['license_id'], r['inspection_id']) for r in all_rows}
        new_rows = [r for r in page_rows
                    if (r['license_id'], r['inspection_id']) not in existing]
        if not new_rows:
            break
        all_rows.extend(new_rows)
        start += PAGE_SIZE
    return all_rows


def fetch_detail(lic: str, iid: str, debug: bool = False) -> dict | None:
    url = f'{ESTAB_URL}?licenseID={lic}&inspectionID={iid}'
    try:
        html = _get(url)
    except Exception as exc:
        print(f"  detail fetch failed (lic={lic} iid={iid}): {exc}")
        return None
    if debug:
        with open('/tmp/sa_estab.html', 'w') as fh:
            fh.write(html)
        print(f"  DEBUG: dumped /tmp/sa_estab.html")
    return parse_detail(html, lic, iid)


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

        existing_insp_ids = set(
            sid for (sid,) in
            db.session.query(Inspection.source_id)
                      .filter(Inspection.region == REGION,
                              Inspection.source_id.isnot(None)).all()
        )

        new_r = new_i = skipped = 0

        for rec in records:
            lic   = rec['license_id']
            iid   = rec['inspection_id']
            name  = (rec.get('name') or '').strip()
            insp_date = rec.get('date')
            if not name or not insp_date:
                skipped += 1
                continue

            street, city, state, zip5 = parse_address(rec.get('address_raw', ''))

            # ── Get or create restaurant ────────────────────────────────────
            if lic in existing:
                restaurant = existing[lic]
            else:
                slug = unique_slug(make_slug(name, city or 'san-antonio'), seen_slugs)
                restaurant = Restaurant(
                    source_id    = lic,
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
                existing[lic] = restaurant
                new_r += 1

            # ── Skip duplicate inspections by inspection_id ─────────────────
            if iid in existing_insp_ids:
                skipped += 1
                continue

            violations = rec.get('violations', [])
            risk, score = compute_score(violations)

            insp = Inspection(
                restaurant_id   = restaurant.id,
                inspection_date = insp_date,
                source_id       = iid,
                score           = score,
                risk_score      = risk,
                grade           = None,
                result          = score_to_result(score),
                inspection_type = 'Routine',
                region          = REGION,
            )
            db.session.add(insp)
            db.session.flush()
            new_i += 1
            existing_insp_ids.add(iid)

            for v in violations:
                db.session.add(Violation(
                    inspection_id     = insp.id,
                    violation_code    = v['code'],
                    description       = v['desc'],
                    severity          = v['severity'],
                    corrected_on_site = False,
                ))

            old_latest = restaurant.latest_inspection_date
            if old_latest is None or insp_date > old_latest:
                if old_latest != insp_date:
                    restaurant.ai_summary = None
                restaurant.latest_inspection_date = insp_date

            if new_i % 250 == 0:
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

    today = date.today()
    if full_mode:
        from_date = since or date(2023, 1, 1)
    else:
        from_date = since or (today - timedelta(days=days))
    to_date = until or today

    if from_date > to_date:
        print(f"Empty range: {from_date} > {to_date}")
        return

    if not dry_run:
        from app import create_app
        from app.db import db
        from app.models.restaurant import Restaurant
        from app.models.inspection import Inspection
        from app.models.violation import Violation
        app = create_app()
    else:
        app = db = Restaurant = Inspection = Violation = None

    # Walk the date range most-recent-first — recent data matters more.
    days_list = []
    cur = to_date
    while cur >= from_date:
        days_list.append(cur)
        cur -= timedelta(days=1)

    print(f"Fetching San Antonio inspections {from_date} → {to_date} "
          f"({len(days_list)} days)")

    total_r = total_i = total_skipped = 0

    for day_idx, day in enumerate(days_list):
        # Don't skip any weekday — SAMHD sometimes inspects on Sunday (e.g.
        # 2025-02-16 had 11 inspections, likely post-Valentine's follow-ups).
        # Empty days cost one HTTP round trip, which is negligible.
        print(f"\n[{day_idx+1}/{len(days_list)}] {day}:", flush=True)
        rows = fetch_day_rows(day, debug=(debug and day_idx == 0))
        if not rows:
            print(f"  no inspections.")
            continue
        print(f"  {len(rows)} inspection(s) listed. Fetching details...")

        day_records = []
        for idx, row in enumerate(rows):
            is_debug_one = debug and day_idx == 0 and idx == 0
            detail = fetch_detail(row['license_id'], row['inspection_id'],
                                  debug=is_debug_one)
            if detail:
                # Prefer the address parsed from the detail page (more
                # structured) but fall back to the search row's copy.
                if not detail.get('address_raw'):
                    detail['address_raw'] = row.get('address_raw', '')
                # Detail date is authoritative but fall back to search date.
                if not detail.get('date'):
                    detail['date'] = row['date']
                day_records.append(detail)
            if (idx + 1) % 10 == 0:
                print(f"    {idx+1}/{len(rows)} details fetched...",
                      end='\r', flush=True)
            time.sleep(DELAY)
            if debug and day_idx == 0 and idx == 0:
                break   # stop after the first detail in debug mode
        print()

        if dry_run:
            print(f"  --dry-run: {len(day_records)} records")
            for r in day_records[:3]:
                print(f"    {r['name']} | {r['date']} | score={r['score']} "
                      f"| {len(r['violations'])} violations")
                for v in r['violations'][:3]:
                    print(f"      [{v['severity']}] item {v['item']} "
                          f"{v['code'] or ''}: {v['desc'][:80]}")
            continue

        if not day_records:
            continue

        new_r, new_i, skipped = write_to_db(
            day_records, app, db, Restaurant, Inspection, Violation
        )
        total_r       += new_r
        total_i       += new_i
        total_skipped += skipped
        print(f"  {day}: +{new_r} restaurants, +{new_i} inspections, "
              f"{skipped} skipped")

    if dry_run:
        print("\n--dry-run complete.")
        _print_unresolved_summary()
        return

    print(f"\nDone.")
    print(f"  {total_r:,} new restaurants")
    print(f"  {total_i:,} new inspections")
    print(f"  {total_skipped:,} skipped (duplicates or missing data)")
    _print_unresolved_summary()


def _print_unresolved_summary() -> None:
    """Report codes that fell through to the item-number heuristic."""
    if not _UNRESOLVED_CODES:
        print("\nAll violation codes resolved via FDA / TAC tables.")
        return
    total = sum(_UNRESOLVED_CODES.values())
    print(f"\n{total} violations fell back to item-number heuristic "
          f"({len(_UNRESOLVED_CODES)} distinct codes):")
    for code, n in _UNRESOLVED_CODES.most_common(20):
        print(f"  {n:5d}  {code}")
    if len(_UNRESOLVED_CODES) > 20:
        remaining = len(_UNRESOLVED_CODES) - 20
        print(f"  … plus {remaining} more code(s)")


if __name__ == '__main__':
    main()
