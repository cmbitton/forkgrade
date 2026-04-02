#!/usr/bin/env python3
"""
Maricopa County (Phoenix metro) health inspection importer.

Data source: https://envapp.maricopa.gov/EnvironmentalHealth/FoodInspections

Flow:
  Full import:
    1. POST /Search/Results with broad queries to enumerate all FD- permit IDs
       (~26k establishments — server returns all results in one JSON response)
    2. For each permit, GET /Permit/PermitResults/{permitId}
       → name, address, inspection history table
    3. For each inspection not already in DB,
       GET /Inspection/{permitId}/{inspId} → violations

  Incremental (--days=N, default):
    For each existing Maricopa restaurant in DB, re-fetch their permit page
    and check for inspections newer than latest_inspection_date.

Violation severity (Maricopa uses FDA Food Code labels):
  Priority           → critical  (weight 3)
  Priority Foundation → major    (weight 2)
  Core               → minor     (weight 1)

Grade system (Maricopa County):
  A: 0 Priority, 0 Priority Foundation violations
  B: ≤1 Priority or ≤4 Priority Foundation violations
  C: 2 Priority or 5 Priority Foundation violations
  D: 3+ Priority violations or legal action taken

Usage:
  python3 scripts/import_maricopa.py              # check existing restaurants (last 7 days)
  python3 scripts/import_maricopa.py --days=14
  python3 scripts/import_maricopa.py --full        # enumerate all ~26k permits from scratch
  python3 scripts/import_maricopa.py --dry-run     # parse only, no DB writes
"""

import html as _html
import json
import math
import os
import re
import ssl
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta
from pathlib import Path

# Some environments (WSL, older macOS) lack the CA bundle needed to verify
# envapp.maricopa.gov's certificate chain.  We create a permissive context
# identical to what a browser does via the OS CA store; if verification passes
# we use it, otherwise we fall back to unverified (scraping-only risk).
def _ssl_ctx():
    """Return a per-call unverified SSL context (thread-safe; SSLContext objects are not)."""
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx

sys.path.insert(0, str(Path(__file__).parent.parent))

if not os.environ.get('DATABASE_URL'):
    from dotenv import load_dotenv
    load_dotenv()

# FDA Food Code severity map (shared across all importers).
try:
    sys.path.insert(0, str(Path(__file__).parent))
    from fda_codes import CODE_SEVERITY as _FDA_CODE_SEVERITY
except Exception:
    _FDA_CODE_SEVERITY = {}

BASE_URL   = 'https://envapp.maricopa.gov'
SEARCH_URL = f'{BASE_URL}/Search/Results'
PERMIT_URL = f'{BASE_URL}/Permit/PermitResults/{{}}'
INSP_URL   = f'{BASE_URL}/Inspection/{{}}/{{}}'
REGION     = 'maricopa'
STATE      = 'AZ'
DELAY      = 0.2   # seconds between requests

_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept':     'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Referer':    f'{BASE_URL}/EnvironmentalHealth/FoodInspections',
}

_JSON_HEADERS = {
    **_HEADERS,
    'Accept':           'application/json, text/javascript, */*; q=0.01',
    'X-Requested-With': 'XMLHttpRequest',
    'Content-Type':     'application/x-www-form-urlencoded; charset=UTF-8',
}


# ── Severity ──────────────────────────────────────────────────────────────────

_SEV_WEIGHTS = {'critical': 3, 'major': 2, 'minor': 1}

_PF_TO_SEVERITY = {'P': 'critical', 'Pf': 'major', 'C': 'minor'}

# Map both new-format words and old-format letter codes to severity
_SEV_WORD = {
    'priority foundation': 'major',
    'priority':            'critical',
    'core':                'minor',
    'pf':                  'major',
    'p':                   'critical',
    'c':                   'minor',
}


def _fda_severity(code: str) -> str:
    """FDA code lookup with recursive subsection stripping."""
    current = code.strip()
    while True:
        pf = _FDA_CODE_SEVERITY.get(current)
        if pf:
            return _PF_TO_SEVERITY.get(pf, 'minor')
        stripped = re.sub(r'\s*\([^)]+\)\s*$', '', current).strip()
        if stripped == current:
            break
        current = stripped
    return 'minor'


def risk_to_score(risk: int) -> int:
    return round(100 * math.exp(-risk * 0.05))



def score_to_result(score: int) -> str:
    if score >= 80:
        return 'Pass'
    if score >= 60:
        return 'Pass with Conditions'
    return 'Fail'


def compute_score(violations: list) -> tuple:
    risk = sum(_SEV_WEIGHTS.get(v['severity'], 1) for v in violations)
    return risk, risk_to_score(risk)


# ── Slug helpers ──────────────────────────────────────────────────────────────

def make_slug(name: str, city: str) -> str:
    s = re.sub(r"['\u2019]", '', name.lower().strip())
    s = re.sub(r'[\s_/&]+', '-', s)
    s = re.sub(r'[^a-z0-9-]', '', s)
    s = re.sub(r'-+', '-', s).strip('-')
    c = re.sub(r'[^a-z0-9-]', '', (city or 'phoenix').lower().replace(' ', '-').replace("'", ''))
    return s + '-' + c


def unique_slug(base: str, seen: set) -> str:
    slug, n = base, 2
    while slug in seen:
        slug = f'{base}-{n}'
        n += 1
    seen.add(slug)
    return slug


# ── Address parsing ───────────────────────────────────────────────────────────

_STATE_ZIP_RE = re.compile(r'\b([A-Z]{2})\s+(\d{5})\b')

# Street suffixes that should not be mistaken for a city name
_STREET_SUFFIXES = {
    'ALY', 'AVE', 'BLVD', 'BND', 'BR', 'BYP', 'CIR', 'CLOSE', 'CT', 'CV',
    'DR', 'EXPY', 'EXT', 'FWY', 'HWY', 'LN', 'LOOP', 'PKWY', 'PL', 'PLZ',
    'PT', 'RD', 'ROW', 'RUN', 'SQ', 'ST', 'STE', 'TER', 'TR', 'TRL', 'WAY',
    'N', 'S', 'E', 'W', 'NE', 'NW', 'SE', 'SW',
}


_ORDINAL_RE = re.compile(r'\b(\d+)(St|Nd|Rd|Th)\b')

def _addr_title(s: str) -> str:
    """Title-case an address string, keeping ordinal suffixes lowercase (19th, 51st)."""
    return _ORDINAL_RE.sub(lambda m: m.group(1) + m.group(2).lower(), s.title())


def parse_address(raw: str) -> tuple:
    """
    '4815 E MAIN ST MESA AZ 85205' or '4815 E MAIN ST, MESA, AZ 85205'
    → (street, city, state, zip)
    """
    raw = re.sub(r'<[^>]+>', ' ', raw)
    raw = re.sub(r'\s+', ' ', raw).strip().upper().replace(',', ' ')

    m = _STATE_ZIP_RE.search(raw)
    if not m:
        return raw.title(), 'Phoenix', STATE, None

    state = m.group(1)
    zip5  = m.group(2)
    before = raw[:m.start()].strip()

    parts = before.rsplit(None, 1)
    if len(parts) == 2 and parts[1] not in _STREET_SUFFIXES:
        return _addr_title(parts[0]), parts[1].title(), state, zip5
    return _addr_title(before), 'Phoenix', state, zip5


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def _get(url: str, retries: int = 3) -> str:
    for attempt in range(retries):
        req = urllib.request.Request(url, headers=_HEADERS)
        try:
            with urllib.request.urlopen(req, timeout=25, context=_ssl_ctx()) as r:
                return r.read().decode('utf-8', errors='replace')
        except urllib.error.HTTPError as exc:
            if exc.code in (404, 410):
                return ''
            if exc.code in (429, 503) and attempt < retries - 1:
                time.sleep(2 ** (attempt + 2))
                continue
            raise
        except (TimeoutError, OSError) as e:
            if attempt < retries - 1:
                time.sleep(2 ** (attempt + 1))
                continue
            print(f'  Network error: {e}')
            return ''
    return ''


def _post_json(params: dict, retries: int = 3) -> list:
    data = urllib.parse.urlencode(params).encode()
    for attempt in range(retries):
        req = urllib.request.Request(
            SEARCH_URL, data=data, method='POST', headers=_JSON_HEADERS,
        )
        try:
            with urllib.request.urlopen(req, timeout=45, context=_ssl_ctx()) as r:
                body = r.read().decode('utf-8', errors='replace')
                return json.loads(body).get('data', [])
        except (TimeoutError, OSError) as e:
            if attempt < retries - 1:
                time.sleep(2 ** (attempt + 1))
                continue
            print(f'  POST failed: {e}')
            return []
        except Exception as e:
            print(f'  POST error: {e}')
            return []
    return []


# ── Discovery ─────────────────────────────────────────────────────────────────

_SEARCH_BASE = {
    'draw':                                   '1',
    'columns[0][data]':                       'issuedDateString',
    'columns[1][data]':                       'permitId',
    'columns[2][data]':                       'permitType',
    'columns[3][data]':                       'businessName',
    'columns[4][data]':                       'businessAddressConcat',
    'columns[5][data]':                       'cuttingEdgeIcon',
    'order[0][column]':                       '3',
    'order[0][dir]':                          'asc',
    'start':                                  '0',
    'length':                                 '99999',
    'search[value]':                          '',
    'licenseSearchParams[AddressNum]':        '',
    'licenseSearchParams[StreetName]':        '',
    'licenseSearchParams[PreDirection]':      '',
    'licenseSearchParams[StreetType]':        '',
    'licenseSearchParams[City]':              '',
    'licenseSearchParams[Zip]':               '',
    'licenseSearchParams[BusinessName]':      '',
}

# Seven broad letter queries cover ~99.9% of all permits; rest of alphabet fills the gap
_DISCOVERY_LETTERS = list('abcdefghijklmnopqrstuvwxyz')


def discover_permits() -> dict:
    """
    Enumerate all fixed-establishment (FD-) permit IDs via broad name searches.
    Returns dict: permit_id → {name, address, permit_type}
    """
    permits = {}
    for letter in _DISCOVERY_LETTERS:
        params = {**_SEARCH_BASE, 'licenseSearchParams[BusinessName]': letter}
        print(f"  Querying name='{letter}'...", end=' ', flush=True)
        rows = _post_json(params)
        added = 0
        for row in rows:
            pid = (row.get('permitId') or '').strip()
            if not pid or not pid.upper().startswith('FD'):
                continue
            if pid not in permits:
                permits[pid] = {
                    'name':        (row.get('businessName') or '').strip(),
                    'address':     (row.get('businessAddressConcat') or '').strip(),
                    'permit_type': (row.get('permitType') or '').strip(),
                }
                added += 1
        print(f'{len(rows)} results → {added} new FD permits (total: {len(permits)})')
        time.sleep(DELAY)
    return permits


# ── Permit page parsing ───────────────────────────────────────────────────────

# Inspection row link: href="/Inspection/FD-09618/INSP-235446-2025"
_INSP_HREF_RE = re.compile(
    r'href=["\']?/Inspection/([^/"\'<>\s]+)/([^/"\'<>\s]+)["\']?',
    re.IGNORECASE,
)

# Inspection table row: <tr>..date..purpose..grade..status..pvcount..link..</tr>
_TR_RE = re.compile(r'<tr\b[^>]*>(.*?)</tr>', re.IGNORECASE | re.DOTALL)
_TD_RE = re.compile(r'<td\b[^>]*>(.*?)</td>', re.IGNORECASE | re.DOTALL)

# Date: M/D/YYYY or MM/DD/YYYY
_DATE_RE = re.compile(r'\b(\d{1,2}/\d{1,2}/\d{4})\b')


def _strip(s: str) -> str:
    s = re.sub(r'<[^>]+>', ' ', s)
    s = _html.unescape(s)
    return re.sub(r'\s+', ' ', s).strip()


def _parse_date(s: str) -> date | None:
    s = s.strip()
    for fmt in ('%m/%d/%Y', '%Y-%m-%d'):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    return None


def parse_permit_page(html: str, permit_id: str) -> dict | None:
    """
    Parse permit results page.
    Returns {name, address_raw, inspections: [{permit_id, insp_id, date, grade, purpose}]}
    """
    if len(html) < 300:
        return None

    # ── Business name: <h2>Name</h2> ──────────────────────────────────────────
    h2_m = re.search(r'<h2[^>]*>(.*?)</h2>', html, re.IGNORECASE | re.DOTALL)
    name = _strip(h2_m.group(1)) if h2_m else ''

    # ── Address: second <p class="mb-0"> after the h2 ─────────────────────────
    # First p: "Permit Type: ..."
    # Second p: "4815 E MAIN ST MESA AZ 85205"
    address_raw = ''
    p_tags = list(re.finditer(r'<p class="mb-0">(.*?)</p>', html, re.IGNORECASE | re.DOTALL))
    for p in p_tags:
        text = _strip(p.group(1))
        if text and not text.lower().startswith('permit'):
            address_raw = text
            break

    # ── Inspection history table ───────────────────────────────────────────────
    # Columns (0-indexed): date | purpose | grade | status | priority_count | COS | link
    inspections = []
    seen_ids = set()

    for tr_m in _TR_RE.finditer(html):
        row_html = tr_m.group(1)
        cells = [_strip(td.group(1)) for td in _TD_RE.finditer(row_html)]
        if len(cells) < 7:
            continue

        # Check there's an inspection link in the last cell
        link_m = _INSP_HREF_RE.search(row_html)
        if not link_m:
            continue

        insp_permit = link_m.group(1)
        insp_id     = link_m.group(2)
        if insp_id in seen_ids:
            continue
        seen_ids.add(insp_id)

        insp_date = _parse_date(cells[0]) if cells[0] else None
        purpose   = cells[1] if len(cells) > 1 else 'Routine'
        grade_raw = cells[2] if len(cells) > 2 else ''
        grade = None
        if grade_raw and grade_raw.upper() not in ('NOT PARTICIPATING', ''):
            g = grade_raw.strip().upper()
            if g in ('A', 'B', 'C', 'D'):
                grade = g

        # Map purpose to inspection_type
        insp_type = 'Routine'
        if purpose:
            pl = purpose.lower()
            if 'complaint' in pl:
                insp_type = 'Complaint'
            elif 'follow' in pl or 're-inspection' in pl or 'reinspection' in pl:
                insp_type = 'Reinspection'
            elif 'routine' in pl:
                insp_type = 'Routine'
            else:
                insp_type = purpose

        inspections.append({
            'permit_id': insp_permit,
            'insp_id':   insp_id,
            'date':      insp_date,
            'grade':     grade,
            'type':      insp_type,
        })

    return {
        'permit_id':   permit_id,
        'name':        name,
        'address_raw': address_raw,
        'inspections': inspections,
    }


# ── Inspection detail parsing ─────────────────────────────────────────────────

# Violation comment line containing FDA code + severity label.
# Handles two formats:
#   New: "PRIORITY VIOLATION-2-301.14 - Priority: description"
#        "3-501.15 (A) - Priority Foundation: description"
#        "6-501.12 - Core: description"
#   Old: "PRIORITY VIOLATION-4-602.11, P: description"
#        "Priority Foundation-X-XXX.XX, Pf: description"
#        "Core-6-501.12, C: description"
_COMMENT_RE = re.compile(
    r'(?:PRIORITY\s+VIOLATION\s*[-–]\s*|Priority\s+Foundation\s*[-–]\s*|Core\s*[-–]\s*)?'
    r'([\d]+-[\d]+\.[\d]+(?:\s*\([^)]*\))*)'           # FDA code (with optional subsections)
    r'(?:'
        r'\s*,\s*(Pf?|C)\s*:'                           # old format: ", P:" ", Pf:" ", C:"
        r'|'
        r'\s*[-–]\s*(Priority\s+Foundation|Priority|Core)\s*:'  # new format: " - Priority:"
    r')'
    r'\s*([^<\r\n]{0,400})',                             # description text
    re.IGNORECASE,
)

# Grade from metadata block: <span class="fw-bold">Grade</span><br /><span>A</span>
_GRADE_META_RE = re.compile(
    r'<span[^>]*fw-bold[^>]*>\s*Grade\s*</span>\s*<br\s*/?>\s*<span[^>]*>\s*([^<]{1,30})\s*</span>',
    re.IGNORECASE | re.DOTALL,
)

# Inspection date from metadata block
_DATE_META_RE = re.compile(
    r'<span[^>]*fw-bold[^>]*>\s*Inspection Date\s*</span>\s*<br\s*/?>\s*<span[^>]*>\s*(\d{1,2}/\d{1,2}/\d{4})\s*</span>',
    re.IGNORECASE | re.DOTALL,
)

# Corrected on site: "Corrected at time of inspection" or "Corrected At Time Of Inspection"
_COS_RE = re.compile(r'corrected\s+at\s+time\s+of\s+inspection', re.IGNORECASE)


def _sev_from_match(letter_code: str | None, word_label: str | None, fda_code: str) -> str:
    if letter_code:
        return _SEV_WORD.get(letter_code.lower(), _fda_severity(fda_code))
    if word_label:
        return _SEV_WORD.get(word_label.lower(), _fda_severity(fda_code))
    return _fda_severity(fda_code)


def parse_inspection_page(html: str, permit_id: str, insp_id: str) -> dict | None:
    """Parse inspection detail page for violations, grade, and date."""
    if len(html) < 200:
        return None

    # ── Grade ────────────────────────────────────────────────────────────────
    grade = None
    gm = _GRADE_META_RE.search(html)
    if gm:
        g = gm.group(1).strip().upper()
        if g in ('A', 'B', 'C', 'D'):
            grade = g

    # ── Inspection date ──────────────────────────────────────────────────────
    insp_date = None
    dm = _DATE_META_RE.search(html)
    if dm:
        insp_date = _parse_date(dm.group(1))
    if insp_date is None:
        # Fallback: first plausible date anywhere on page
        for fm in _DATE_RE.finditer(html):
            try:
                d = datetime.strptime(fm.group(1), '%m/%d/%Y').date()
                if 2015 <= d.year <= date.today().year:
                    insp_date = d
                    break
            except ValueError:
                pass

    # ── Violations div ───────────────────────────────────────────────────────
    vdiv_m = re.search(r'<div[^>]+id=["\']Violations["\'][^>]*>(.*?)(?:</div>\s*</div>|$)',
                       html, re.IGNORECASE | re.DOTALL)
    vdiv = vdiv_m.group(1) if vdiv_m else html

    # ── Parse violation groups ───────────────────────────────────────────────
    violations = []
    seen_codes = set()

    # Split by violation-group start marker
    groups = re.split(r'<p class="mb-0 mt-3">', vdiv, flags=re.IGNORECASE)

    for group in groups[1:]:   # skip preamble before first violation
        # Extract all <p class='mb-1'> comment lines in this group
        comment_lines = re.findall(r"<p class='mb-1'>(.*?)</p>", group,
                                   re.IGNORECASE | re.DOTALL)

        # The first non-empty comment line should contain the FDA code
        code = desc = None
        severity = 'minor'
        corrected = False

        for raw_line in comment_lines:
            line = _strip(raw_line)
            if not line:
                continue

            m = _COMMENT_RE.search(line)
            if m and code is None:
                code        = re.sub(r'\s+', '', m.group(1)).strip()   # normalise spaces in code
                letter_code = m.group(2)
                word_label  = m.group(3)
                desc        = (m.group(4) or '').strip()
                severity    = _sev_from_match(letter_code, word_label, code)

            if _COS_RE.search(line):
                corrected = True

        if not code:
            continue
        if code in seen_codes:
            continue
        seen_codes.add(code)

        if not desc:
            desc = code

        violations.append({
            'code':      code,
            'desc':      desc.capitalize(),
            'severity':  severity,
            'corrected': corrected,
        })

    return {
        'permit_id':  permit_id,
        'insp_id':    insp_id,
        'date':       insp_date,
        'grade':      grade,
        'violations': violations,
    }


# ── DB write ──────────────────────────────────────────────────────────────────

def _write_inspections(restaurant, new_insp_list, db, Inspection, Violation):
    """
    Write new inspections (and their violations) for a restaurant.
    new_insp_list: list of dicts from parse_inspection_page()
    Returns count of inspections written.
    """
    written = 0
    for detail in new_insp_list:
        insp_date = detail.get('date')
        if not insp_date:
            continue

        if Inspection.query.filter_by(
            restaurant_id=restaurant.id, inspection_date=insp_date
        ).first():
            continue

        violations = detail.get('violations', [])
        risk, score = compute_score(violations)
        grade = detail.get('grade')

        insp = Inspection(
            restaurant_id   = restaurant.id,
            inspection_date = insp_date,
            inspection_type = detail.get('type', 'Routine'),
            score           = score,
            risk_score      = risk,
            grade           = grade,
            result          = score_to_result(score),
        )
        db.session.add(insp)
        db.session.flush()

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

        written += 1

    return written


# ── Threaded fetch helpers ────────────────────────────────────────────────────

WORKERS = 20   # concurrent HTTP workers for full import

def _fetch_permit_page(permit_id: str) -> tuple:
    """Returns (permit_id, parsed_data_or_None). Thread-safe."""
    html = _get(PERMIT_URL.format(permit_id))
    return permit_id, (parse_permit_page(html, permit_id) if html else None)

def _fetch_insp_page(task: tuple) -> tuple | None:
    """
    task = (permit_id, insp_id, insp_info)
    Returns (permit_id, detail_dict) or None. Thread-safe.
    """
    permit_id, insp_id, insp_info = task
    html = _get(INSP_URL.format(permit_id, insp_id))
    if not html:
        return None
    detail = parse_inspection_page(html, permit_id, insp_id)
    if not detail:
        return None
    detail['type'] = insp_info.get('type', 'Routine')
    if detail.get('grade') is None:
        detail['grade'] = insp_info.get('grade')
    return permit_id, detail


# ── Full import ───────────────────────────────────────────────────────────────

BATCH_SIZE = 500   # permits per batch — caps memory and commits incrementally

def run_full_import(dry_run: bool, app, db, Restaurant, Inspection, Violation,
                    limit: int = 0, since: date | None = None):
    """
    Batched parallel import. Processes BATCH_SIZE permits at a time:
      For each batch:
        1. Fetch permit pages in parallel → inspection ID lists
        2. Fetch inspection detail pages in parallel → violations
        3. Write batch to DB and commit
    Resumes automatically — permits already in DB are skipped.
    """
    print('=== Maricopa: Full Import ===')
    print('Step 1: Discovering all FD permits...')
    permits = discover_permits()
    print(f'  Total FD permits discovered: {len(permits)}')

    if limit:
        permits = dict(list(permits.items())[:limit])
        print(f'  (limited to first {limit} permits for testing)')

    if dry_run:
        print('[DRY RUN] Would process', len(permits), 'permits.')
        return

    permit_items = list(permits.items())
    batches = [permit_items[i:i + BATCH_SIZE] for i in range(0, len(permit_items), BATCH_SIZE)]
    print(f'  Processing {len(permit_items)} permits in {len(batches)} batches of {BATCH_SIZE}...')

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

        total_r = total_i = 0

        for batch_idx, batch in enumerate(batches):
            batch_pids = [pid for pid, _ in batch]
            print(f'\nBatch {batch_idx + 1}/{len(batches)} ({len(batch)} permits)...')

            # ── Fetch permit pages ────────────────────────────────────────────
            permit_pages = {}
            with ThreadPoolExecutor(max_workers=WORKERS) as pool:
                futures = {pool.submit(_fetch_permit_page, pid): pid for pid in batch_pids}
                for future in as_completed(futures):
                    pid, data = future.result()
                    if data:
                        permit_pages[pid] = data

            # ── Build inspection task list ────────────────────────────────────
            insp_tasks = []
            for pid, pdata in permit_pages.items():
                for insp_info in pdata['inspections']:
                    insp_id   = insp_info['insp_id']
                    insp_date = insp_info['date']
                    if not insp_id.upper().startswith('INSP-'):
                        continue
                    if not insp_date:
                        continue
                    if since and insp_date < since:
                        continue
                    insp_tasks.append((pid, insp_id, insp_info))

            # ── Fetch inspection detail pages ─────────────────────────────────
            insp_details: dict[str, list] = {}
            done = 0
            with ThreadPoolExecutor(max_workers=WORKERS) as pool:
                futures = {pool.submit(_fetch_insp_page, task): task for task in insp_tasks}
                for future in as_completed(futures):
                    result = future.result()
                    done += 1
                    if result:
                        pid, detail = result
                        insp_details.setdefault(pid, []).append(detail)
                    if done % 200 == 0 or done == len(insp_tasks):
                        print(f'  {done}/{len(insp_tasks)} inspection pages fetched', flush=True)

            # ── Write batch to DB ─────────────────────────────────────────────
            batch_r = batch_i = 0
            for permit_id, info in batch:
                name = info['name']
                if not name:
                    continue

                if permit_id in existing:
                    restaurant = existing[permit_id]
                else:
                    street, city, state, zip5 = parse_address(info['address'])
                    slug = unique_slug(make_slug(name, city), seen_slugs)
                    restaurant = Restaurant(
                        source_id    = permit_id,
                        name         = name,
                        slug         = slug,
                        address      = street,
                        city         = city,
                        state        = state,
                        zip          = zip5,
                        latitude     = None,
                        longitude    = None,
                        cuisine_type = None,
                        license_type = info.get('permit_type') or None,
                        region       = REGION,
                    )
                    db.session.add(restaurant)
                    db.session.flush()
                    existing[permit_id] = restaurant
                    batch_r += 1

                details = insp_details.get(permit_id, [])
                batch_i += _write_inspections(restaurant, details, db, Inspection, Violation)

            db.session.commit()
            total_r += batch_r
            total_i += batch_i
            print(f'  Batch done: +{batch_r} restaurants, +{batch_i} inspections '
                  f'(total: {total_r} restaurants, {total_i} inspections)')

        print(f'\nFull import complete: {total_r} new restaurants, {total_i} inspections written.')


# ── Incremental import ────────────────────────────────────────────────────────

def run_incremental(days: int, dry_run: bool, app, db, Restaurant, Inspection, Violation):
    """
    For each existing Maricopa restaurant, re-fetch their permit page and
    import any inspections newer than their latest_inspection_date.
    """
    print(f'=== Maricopa: Incremental (checking {days}-day window) ===')

    if dry_run:
        print('[DRY RUN] Would check all existing Maricopa restaurants.')
        return

    cutoff = date.today() - timedelta(days=days)

    with app.app_context():
        restaurants = (
            Restaurant.query
            .filter_by(region=REGION)
            .filter(Restaurant.source_id.isnot(None))
            .all()
        )
        print(f'  Checking {len(restaurants)} restaurants...')
        total_i = 0

        for idx, restaurant in enumerate(restaurants):
            permit_id = restaurant.source_id

            permit_html = _get(PERMIT_URL.format(permit_id))
            if not permit_html:
                time.sleep(DELAY)
                continue

            permit_data = parse_permit_page(permit_html, permit_id)
            if not permit_data:
                time.sleep(DELAY)
                continue

            time.sleep(DELAY)

            latest = restaurant.latest_inspection_date
            new_details = []

            for insp_info in permit_data['inspections']:
                insp_id   = insp_info['insp_id']
                insp_date = insp_info['date']

                if not insp_id.upper().startswith('INSP-'):
                    continue
                if insp_date and latest and insp_date <= latest:
                    continue
                if insp_date and insp_date < cutoff:
                    continue

                insp_html = _get(INSP_URL.format(permit_id, insp_id))
                if not insp_html:
                    time.sleep(DELAY)
                    continue

                detail = parse_inspection_page(insp_html, permit_id, insp_id)
                if detail:
                    detail['type'] = insp_info.get('type', 'Routine')
                    if detail.get('grade') is None:
                        detail['grade'] = insp_info.get('grade')
                    new_details.append(detail)

                time.sleep(DELAY)

            written = _write_inspections(restaurant, new_details, db, Inspection, Violation)
            total_i += written

            if (idx + 1) % 100 == 0:
                db.session.commit()
                print(f'  [{idx+1}/{len(restaurants)}] {total_i} new inspections so far...')

        db.session.commit()
        print(f'\nIncremental complete: {total_i} new inspections written.')


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    full_mode = '--full'    in sys.argv
    dry_run   = '--dry-run' in sys.argv
    days      = 7
    limit     = 0
    since     = None
    for arg in sys.argv[1:]:
        if arg.startswith('--days='):
            days = int(arg.split('=', 1)[1])
        elif arg.startswith('--limit='):
            limit = int(arg.split('=', 1)[1])
        elif arg.startswith('--since='):
            since = date.fromisoformat(arg.split('=', 1)[1])

    from app import create_app
    from app.db import db
    from app.models.restaurant import Restaurant
    from app.models.inspection import Inspection
    from app.models.violation import Violation

    app = create_app()

    if full_mode:
        run_full_import(dry_run, app, db, Restaurant, Inspection, Violation, limit=limit, since=since)
    else:
        run_incremental(days, dry_run, app, db, Restaurant, Inspection, Violation)


if __name__ == '__main__':
    main()
