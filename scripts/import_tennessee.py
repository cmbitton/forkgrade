#!/usr/bin/env python3
"""
Tennessee statewide health inspection importer (slow rewrite).

Data source: https://inspections.myhealthdepartment.com/tennessee
  (MyHealthDepartment portal — Cloudflare-fronted, aggressively rate limited.)

Design:
  - Single-threaded, 2.5s between requests (~0.4 req/s). An earlier faster
    importer tripped the WAF at 1.3 req/s after a few thousand requests.
    The conservative pace here is meant for overnight unattended runs.
  - Circuit breaker on 6 consecutive 403s (stops cleanly, lets you resume).
  - Resumable: existing inspectionIDs are loaded at start and re-runs skip
    work already written.

Endpoints:
  POST /
       body: {"task":"searchInspections","data":{
         "path":"tennessee","programName":"","filters":{},
         "start":N,"count":25,"searchStr":"<keyword>",
         "lat":null,"lng":null,"sort":"Date Descending"}}
       → list of inspection rows. The portal caps each query at ~225 rows
         total (9 pages of 25), so full coverage requires iterating keyword
         prefixes (A-Z, then targeted 2-letter prefixes if needed).

  GET  /tennessee/inspection/?inspectionID=<GUID>
       → HTML detail page with (a) "Observations & Corrective Actions"
         narrative paragraph keyed by TN item number, and (b) a line-item
         checklist giving each item's canonical title. TN's form has its
         own 1-58 item numbering (NOT the FL/FDA numbering).

Severity (TN-form specific):
  Derived from the PH-2267 inspection form's Weight (WT) column:
    WT 4-5 (risk factor items) → critical
    WT 2  (handwashing, cooling, pests, plumbing) → major
    WT 1/0 (remaining good retail practices + admin) → minor

Scoring (cross-region consistent with RI / Philly / etc.):
  risk_score = Σ severity weights (3=critical, 2=major, 1=minor)
  score      = round(100 * exp(-risk_score * 0.05))
  result     = 'Pass' (≥75) / 'Pass with Conditions' (≥55) / 'Fail'

Program filter:
  Only rows with programCode == '605' (Food Service Establishment) are kept.
  TN's portal also returns pools, schools, tattoo studios, etc.

Usage:
  python3 scripts/import_tennessee.py --backfill
  python3 scripts/import_tennessee.py --backfill --since=2024-01-01
  python3 scripts/import_tennessee.py --dry-run --backfill
  python3 scripts/import_tennessee.py --from-keyword=M   # resume mid-run
"""

import json
import math
import re
import string
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent))


# ── Constants ────────────────────────────────────────────────────────────────

REGION = 'tennessee'
STATE = 'TN'

BASE_URL = 'https://inspections.myhealthdepartment.com'
SEARCH_URL = f'{BASE_URL}/'
DETAIL_URL = f'{BASE_URL}/tennessee/inspection/?inspectionID={{iid}}'

FOOD_PROGRAM_CODE = '605'
PAGE_SIZE = 25
MAX_ROWS_PER_KEYWORD = 225  # portal's per-query cap

HEADERS = {
    'User-Agent':       'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept':           'application/json, text/javascript, */*; q=0.01',
    'Content-Type':     'application/json',
    'X-Requested-With': 'XMLHttpRequest',
    'Referer':          f'{BASE_URL}/tennessee',
    'Origin':           BASE_URL,
}

_SEV_WEIGHT = {'critical': 3.0, 'major': 2.0, 'minor': 1.0}


# ── HTTP: throttle + circuit breaker ─────────────────────────────────────────

# Deliberately slow. The WAF counts requests per IP over a multi-minute window;
# previous runs at ~1.3 req/s tripped after ~5 min. At 0.4 req/s we stay well
# below whatever the threshold is, at the cost of longer wallclock.
MIN_REQUEST_INTERVAL = 2.5  # seconds between any two requests

_THROTTLE_LOCK = threading.Lock()
_LAST_REQUEST_AT = 0.0

MAX_CONSECUTIVE_403S = 6
_403_BACKOFFS = (90, 300, 600)  # retry waits on 403 (per call)
_BAN_LOCK = threading.Lock()
_CONSECUTIVE_403S = 0
_BREAKER_TRIPPED = False


class WAFBannedError(RuntimeError):
    """Raised when the circuit breaker opens — stop the whole run."""


def _throttle():
    global _LAST_REQUEST_AT
    with _THROTTLE_LOCK:
        now = time.time()
        wait = _LAST_REQUEST_AT + MIN_REQUEST_INTERVAL - now
        if wait > 0:
            time.sleep(wait)
        _LAST_REQUEST_AT = time.time()


def _note_403() -> bool:
    """Bump the consecutive-403 counter. Returns True iff we just tripped."""
    global _CONSECUTIVE_403S, _BREAKER_TRIPPED
    with _BAN_LOCK:
        _CONSECUTIVE_403S += 1
        if _CONSECUTIVE_403S >= MAX_CONSECUTIVE_403S and not _BREAKER_TRIPPED:
            _BREAKER_TRIPPED = True
            print(f"\n  !! {_CONSECUTIVE_403S} consecutive 403s — WAF is blocking us.")
            print(f"  !! Circuit breaker open. Stop and wait ~30+ minutes before resuming.")
            return True
    return False


def _reset_403():
    global _CONSECUTIVE_403S
    with _BAN_LOCK:
        _CONSECUTIVE_403S = 0


def search_page(keyword: str, start: int, retries: int = 3) -> list | None:
    """POST to the search endpoint. Returns rows or None on hard failure."""
    if _BREAKER_TRIPPED:
        raise WAFBannedError("WAF circuit breaker open")
    body = json.dumps({
        'task': 'searchInspections',
        'data': {
            'path': 'tennessee',
            'programName': '',
            'filters': {},
            'start': start,
            'count': PAGE_SIZE,
            'searchStr': keyword,
            'lat': None,
            'lng': None,
            'sort': 'Date Descending',
        },
    }).encode()

    for attempt in range(retries):
        _throttle()
        try:
            req = urllib.request.Request(SEARCH_URL, data=body, headers=HEADERS, method='POST')
            with urllib.request.urlopen(req, timeout=20) as r:
                data = json.loads(r.read())
                _reset_403()
                # The portal sometimes wraps results in {"data": [...]}, sometimes returns bare list
                if isinstance(data, list):
                    return data
                if isinstance(data, dict):
                    return data.get('data') or data.get('results') or []
                return []
        except urllib.error.HTTPError as e:
            if e.code == 403:
                if _note_403():
                    raise WAFBannedError("WAF blocked search POST")
                if attempt < retries - 1:
                    wait = _403_BACKOFFS[min(attempt, len(_403_BACKOFFS) - 1)]
                    print(f"  HTTP 403 on search, backoff {wait}s (attempt {attempt+1}/{retries})")
                    time.sleep(wait)
                    continue
                return None
            if e.code in (429, 500, 502, 503, 504) and attempt < retries - 1:
                wait = 2 ** (attempt + 2)
                print(f"  HTTP {e.code} on search, retry in {wait}s")
                time.sleep(wait)
                continue
            print(f"  HTTP {e.code} on search, giving up")
            return None
        except Exception as e:
            if attempt < retries - 1:
                wait = 2 ** (attempt + 1)
                print(f"  search error {type(e).__name__}: {e}, retry in {wait}s")
                time.sleep(wait)
                continue
            print(f"  search fatal error {type(e).__name__}: {e}")
            return None
    return None


def fetch_detail_html(inspection_id: str, retries: int = 3) -> str | None:
    if _BREAKER_TRIPPED:
        raise WAFBannedError("WAF circuit breaker open")
    url = DETAIL_URL.format(iid=urllib.parse.quote(inspection_id, safe='-'))
    for attempt in range(retries):
        _throttle()
        try:
            req = urllib.request.Request(url, headers={
                'User-Agent': HEADERS['User-Agent'],
                'Referer': f'{BASE_URL}/tennessee',
            })
            with urllib.request.urlopen(req, timeout=20) as r:
                html = r.read().decode('utf-8', errors='replace')
                _reset_403()
                return html
        except urllib.error.HTTPError as e:
            if e.code == 403:
                if _note_403():
                    raise WAFBannedError("WAF blocked detail GET")
                if attempt < retries - 1:
                    wait = _403_BACKOFFS[min(attempt, len(_403_BACKOFFS) - 1)]
                    time.sleep(wait)
                    continue
                return None
            if e.code in (429, 500, 502, 503, 504) and attempt < retries - 1:
                time.sleep(2 ** (attempt + 2))
                continue
            return None
        except Exception:
            if attempt < retries - 1:
                time.sleep(2 ** (attempt + 1))
                continue
            return None
    return None


# ── HTML parsing ─────────────────────────────────────────────────────────────
#
# TN detail pages have two sections we care about:
#
# (1) "Observations & Corrective Actions" — the free-text narratives, laid out
#     as "N:  narrative<br /><br />N:  narrative<br /><br />..." inside a
#     <p class="observations-text">. The leading integer is TN's own item
#     number (1-58), NOT the standard FDA/FL numbering. TN-17 is reheating,
#     TN-20 is cold holding, TN-21 is date marking, etc.
#
# (2) A per-item checklist listing every form item with its official title
#     and status (IN / OUT / NA / NO). Each item is rendered as
#     <div class="text-block-11">N title</div>. We use this to look up the
#     canonical title for each narrative — no static mapping required.

# Pull the observations narrative paragraph (guard against the later
# "Additional Comments" paragraph by anchoring on the preceding header).
_OBS_SECTION_RE = re.compile(
    r'Observations\s*&amp;\s*Corrective\s*Actions.*?'
    r'<p[^>]*class="observations-text"[^>]*>(.*?)</p>',
    re.DOTALL | re.IGNORECASE,
)

# "N:  narrative" — used after we split the observations block on <br><br>.
_OBS_ENTRY_RE = re.compile(r'^\s*(\d{1,2})\s*:\s*(.+)$', re.DOTALL)

# Line-item checklist: each item's canonical title, keyed by item number.
_ITEM_TITLE_RE = re.compile(
    r'class=["\']text-block-11["\'][^>]*>\s*(\d{1,2})\s+([^<]+?)\s*</div>',
    re.DOTALL,
)

# Optional TN reg code that sometimes appears inside a narrative.
_TN_REG_CODE_RE = re.compile(r'1200-\d{2}-\d{2}-?\.[0-9a-z()\[\]\.]+', re.IGNORECASE)

# Severity derived from TN's PH-2267 inspection form's Weight (WT) column.
# WT 4-5 → critical (risk factor items), WT 2 → major, WT 1/0 → minor.
TN_CRITICAL_ITEMS = {1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14, 15, 16, 17,
                     18, 19, 20, 21, 22, 23, 24, 25, 26, 27}
TN_MAJOR_ITEMS = {8, 31, 36, 48, 49}


def _tn_severity(item_n: int) -> str:
    if item_n in TN_CRITICAL_ITEMS:
        return 'critical'
    if item_n in TN_MAJOR_ITEMS:
        return 'major'
    return 'minor'


def _strip_html(s: str) -> str:
    s = re.sub(r'<[^>]+>', ' ', s)
    s = s.replace('&nbsp;', ' ').replace('&amp;', '&').replace('&#39;', "'")
    s = s.replace('&quot;', '"').replace('&lt;', '<').replace('&gt;', '>')
    return re.sub(r'\s+', ' ', s).strip()


def _parse_item_titles(html: str) -> dict[int, str]:
    """Scan the line-item checklist → {item_n: official title}."""
    titles: dict[int, str] = {}
    for m in _ITEM_TITLE_RE.finditer(html):
        try:
            n = int(m.group(1))
        except ValueError:
            continue
        title = _strip_html(m.group(2))
        if title and n not in titles:
            titles[n] = title
    return titles


def _parse_observations(html: str) -> dict[int, str]:
    """Scan the "Observations & Corrective Actions" block → {item_n: narrative}."""
    narratives: dict[int, str] = {}
    sec = _OBS_SECTION_RE.search(html)
    if not sec:
        return narratives
    block = sec.group(1)
    # Entries are separated by runs of <br/> tags (typically doubled).
    for entry in re.split(r'(?:<br\s*/?>\s*){2,}', block):
        entry = entry.strip()
        if not entry:
            continue
        m = _OBS_ENTRY_RE.match(entry)
        if not m:
            continue
        try:
            n = int(m.group(1))
        except ValueError:
            continue
        narrative = _strip_html(m.group(2))
        if narrative:
            narratives[n] = narrative
    return narratives


def parse_detail_violations(html: str) -> list[dict]:
    """Return list of {item, code, description, severity, notes} from the detail HTML."""
    if not html:
        return []

    titles = _parse_item_titles(html)
    narratives = _parse_observations(html)

    violations: list[dict] = []
    # Iterate narratives in item-number order for deterministic output.
    for n in sorted(narratives):
        narrative = narratives[n]
        # Trim long inspector notes to keep rows bounded.
        if len(narrative) > 2000:
            narrative = narrative[:2000].rsplit(' ', 1)[0] + '…'
        title = titles.get(n) or f'Item {n}'
        sev = _tn_severity(n)
        # Opportunistic TN reg-code extraction (not all inspections include one).
        code_m = _TN_REG_CODE_RE.search(narrative)
        code = code_m.group(0).rstrip('.,') if code_m else None
        # Only keep inspector notes if they add information beyond the title.
        notes = narrative if narrative.strip().lower() != title.strip().lower() else None
        violations.append({
            'item': n,
            'code': code,
            'description': title,
            'severity': sev,
            'notes': notes,
        })
    return violations


def compute_score(violations: list[dict]) -> tuple[float, int]:
    risk = sum(_SEV_WEIGHT.get(v['severity'], 1.0) for v in violations)
    score = round(100 * math.exp(-risk * 0.05))
    return risk, score


def score_to_result(score: int) -> str:
    if score >= 75:
        return 'Pass'
    if score >= 55:
        return 'Pass with Conditions'
    return 'Fail'


# ── Slug + row helpers ───────────────────────────────────────────────────────

def make_slug(name: str, city: str) -> str:
    s = re.sub(r"['\u2019]", '', (name or '').lower().strip())
    s = re.sub(r'[\s_/&]+', '-', s)
    s = re.sub(r'[^a-z0-9-]', '', s)
    s = re.sub(r'-+', '-', s).strip('-')
    c = re.sub(r'[^a-z0-9-]', '',
               (city or '').lower().replace(' ', '-'))
    return f"{s}-{c}" if c else (s or 'unnamed')


def unique_slug(base: str, seen: set) -> str:
    slug, n = base, 2
    while slug in seen:
        slug = f"{base}-{n}"
        n += 1
    seen.add(slug)
    return slug


def parse_date_str(s: str | None) -> date | None:
    if not s:
        return None
    s = s.strip()
    # Try a YYYY-MM-DD prefix first (covers both bare dates and ISO timestamps)
    m = re.match(r'(\d{4})-(\d{2})-(\d{2})', s)
    if m:
        try:
            return date(int(m[1]), int(m[2]), int(m[3]))
        except ValueError:
            pass
    # Fall back to US-style formats
    for fmt in ('%m/%d/%Y', '%m/%d/%y'):
        try:
            return datetime.strptime(s[:10], fmt).date()
        except ValueError:
            continue
    return None


def row_get(row: dict, *keys, default=None):
    for k in keys:
        v = row.get(k)
        if v not in (None, ''):
            return v
    return default


def extract_row_fields(row: dict) -> dict | None:
    """Pull the fields we care about from a search-result row.

    The portal's response key casing shifts (camelCase / lowercase) between
    endpoints, so we try several. Rows without a program code 605 or without
    an inspectionID are dropped.
    """
    program = str(row_get(row, 'programCode', 'program_code', 'program') or '').strip()
    if program and program != FOOD_PROGRAM_CODE:
        return None
    iid = row_get(row, 'inspectionID', 'inspectionId', 'inspection_id', 'id')
    if not iid:
        return None
    iid = str(iid).strip()
    name = (row_get(row, 'establishmentName', 'name', 'facilityName', 'businessName') or '').strip()
    if not name:
        return None
    insp_date = parse_date_str(row_get(row, 'inspectionDate', 'date', 'inspDate'))
    if not insp_date:
        return None
    address = (row_get(row, 'address', 'streetAddress', 'address1') or '').strip()
    city = (row_get(row, 'city', 'establishmentCity') or '').strip().title()
    zipc = (row_get(row, 'zip', 'zipCode', 'postal') or '').strip()
    permit = str(row_get(row, 'permitID', 'permitId', 'permit_id') or '').strip()
    purpose = (row_get(row, 'purpose', 'inspectionPurpose') or '').strip()
    insp_type = (row_get(row, 'inspectionType', 'type') or '').strip() or purpose or 'Routine'
    lat = row_get(row, 'lat', 'latitude')
    lng = row_get(row, 'lng', 'longitude')
    try:
        lat = float(lat) if lat not in (None, '') else None
    except (TypeError, ValueError):
        lat = None
    try:
        lng = float(lng) if lng not in (None, '') else None
    except (TypeError, ValueError):
        lng = None
    return {
        'inspection_id': iid,
        'permit_id': permit or None,
        'name': name,
        'address': address,
        'city': city or 'Unknown',
        'zip': zipc,
        'state': STATE,
        'lat': lat,
        'lng': lng,
        'date': insp_date,
        'purpose': purpose,
        'inspection_type': insp_type,
    }


# ── Search iteration ─────────────────────────────────────────────────────────

def iter_keyword(keyword: str, cutoff: date, seen_iids: set,
                 on_row) -> tuple[int, int, bool]:
    """Paginate one keyword. Calls on_row(fields, row) for each kept row.

    Stops early when:
      - The whole page is past the cutoff date (rows sorted Date Descending)
      - We've hit the per-query cap (225)
      - The portal returns an empty / short page

    Returns (rows_seen, food_rows_kept, hit_cap).
    hit_cap is True when we exhausted the 225-row cap without ever reaching
    the date cutoff — i.e. the portal is truncating results and we should
    deepen the prefix (append a letter) to recover the missing rows.
    """
    seen = 0
    kept = 0
    start = 0
    reached_cutoff = False
    while start < MAX_ROWS_PER_KEYWORD:
        rows = search_page(keyword, start)
        if rows is None:
            return seen, kept, False
        if not rows:
            break

        any_in_window = False
        page_past_cutoff = False
        for raw in rows:
            seen += 1
            fields = extract_row_fields(raw)
            if not fields:
                continue
            if fields['date'] < cutoff:
                page_past_cutoff = True
                reached_cutoff = True
                continue
            any_in_window = True
            if fields['inspection_id'] in seen_iids:
                continue
            seen_iids.add(fields['inspection_id'])
            on_row(fields, raw)
            kept += 1

        if len(rows) < PAGE_SIZE:
            break
        if page_past_cutoff and not any_in_window:
            # Whole page is past cutoff; next page would be further past.
            break
        start += PAGE_SIZE

    # We "hit the cap" when we paged up to the portal's 225 limit AND never
    # saw any rows past the cutoff — meaning there are probably more rows we
    # didn't get. In that case the caller should deepen the prefix.
    hit_cap = (start >= MAX_ROWS_PER_KEYWORD) and not reached_cutoff
    return seen, kept, hit_cap


# Max prefix depth for adaptive deepening. The portal caps every query at 225
# rows, so if a single letter overflows we fan out to AA-AZ; if any of those
# still overflow we go to AAA-AAZ. Depth 3 is usually enough — 26^3 = 17,576
# prefixes worst case, but we only expand branches that actually overflow.
MAX_PREFIX_DEPTH = 3


def build_initial_queue(from_keyword: str | None = None) -> list[str]:
    """Top-level keywords: blank (most-recent 225) then A-Z.

    Deeper prefixes (AA-AZ, AAA-AAZ) are appended dynamically by the main
    loop only when a parent keyword hits the 225 cap.
    """
    letters = list(string.ascii_uppercase)
    if from_keyword:
        fk = from_keyword.upper()
        if fk in letters:
            letters = letters[letters.index(fk):]
        else:
            # Allow resuming from a multi-letter prefix; skip the blank query.
            return [from_keyword.upper()]
    return [''] + letters


# ── DB write ─────────────────────────────────────────────────────────────────

def load_db_state(db, Restaurant, Inspection):
    """Load existing TN restaurants + inspectionIDs for dedup."""
    existing_by_perm: dict[str, object] = {}
    existing_by_key: dict[tuple, object] = {}  # (name_lower, address_lower) → restaurant
    for r in Restaurant.query.filter_by(region=REGION).all():
        if r.source_id:
            existing_by_perm[r.source_id] = r
        existing_by_key[((r.name or '').lower(), (r.address or '').lower())] = r

    seen_slugs = {
        slug for (slug,) in
        Restaurant.query.with_entities(Restaurant.slug).all()
    }

    existing_iids = set()
    for (src,) in db.session.query(Inspection.source_id).filter(
        Inspection.region == REGION,
        Inspection.source_id.isnot(None),
    ).all():
        existing_iids.add(src)

    return existing_by_perm, existing_by_key, seen_slugs, existing_iids


def commit_record(db, Restaurant, Inspection, Violation,
                  fields: dict, violations: list[dict],
                  existing_by_perm: dict, existing_by_key: dict,
                  seen_slugs: set, existing_iids: set) -> tuple[int, int]:
    """Write one inspection (and restaurant if new). Returns (new_r, new_i)."""
    if fields['inspection_id'] in existing_iids:
        return (0, 0)

    permit = fields.get('permit_id')
    restaurant = None
    if permit and permit in existing_by_perm:
        restaurant = existing_by_perm[permit]
    else:
        key = (fields['name'].lower(), (fields['address'] or '').lower())
        if key in existing_by_key:
            restaurant = existing_by_key[key]
            if permit and not restaurant.source_id:
                restaurant.source_id = permit
                existing_by_perm[permit] = restaurant

    new_r = 0
    if restaurant is None:
        slug = unique_slug(make_slug(fields['name'], fields['city']), seen_slugs)
        restaurant = Restaurant(
            source_id=permit,
            name=fields['name'],
            slug=slug,
            address=fields['address'],
            city=fields['city'],
            state=STATE,
            zip=fields['zip'],
            latitude=fields['lat'],
            longitude=fields['lng'],
            region=REGION,
        )
        db.session.add(restaurant)
        db.session.flush()
        if permit:
            existing_by_perm[permit] = restaurant
        existing_by_key[(fields['name'].lower(), (fields['address'] or '').lower())] = restaurant
        new_r = 1

    risk, score = compute_score(violations)
    insp = Inspection(
        restaurant_id=restaurant.id,
        inspection_date=fields['date'],
        source_id=fields['inspection_id'],
        score=score,
        risk_score=risk,
        result=score_to_result(score),
        inspection_type=fields.get('inspection_type') or 'Routine',
        region=REGION,
    )
    db.session.add(insp)
    db.session.flush()
    existing_iids.add(fields['inspection_id'])

    for v in violations:
        db.session.add(Violation(
            inspection_id=insp.id,
            violation_code=v['code'],
            description=v['description'],
            inspector_notes=v.get('notes'),
            severity=v['severity'],
            corrected_on_site=False,
        ))

    prev = restaurant.latest_inspection_date
    if prev is None or fields['date'] > prev:
        restaurant.latest_inspection_date = fields['date']
        restaurant.ai_summary = None
    return (new_r, 1)


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    dry_run = '--dry-run' in sys.argv
    backfill = '--backfill' in sys.argv or '--full' in sys.argv

    since_arg: date | None = None
    days = 7
    from_keyword: str | None = None
    commit_every = 25

    for arg in sys.argv[1:]:
        if arg.startswith('--since='):
            since_arg = date.fromisoformat(arg.split('=', 1)[1])
        elif arg.startswith('--days='):
            days = int(arg.split('=', 1)[1])
        elif arg.startswith('--from-keyword='):
            from_keyword = arg.split('=', 1)[1]
        elif arg.startswith('--commit-every='):
            commit_every = max(1, int(arg.split('=', 1)[1]))

    today = date.today()
    if backfill:
        cutoff = since_arg or date(2023, 1, 1)
    else:
        cutoff = since_arg or (today - timedelta(days=days))

    print(f"TN importer (slow) — {MIN_REQUEST_INTERVAL:.1f}s between requests")
    print(f"  cutoff: {cutoff}  (today {today})")
    print(f"  mode:   {'BACKFILL (A-Z keyword partitions)' if backfill else 'recent-only'}")
    if from_keyword:
        print(f"  resuming from keyword '{from_keyword}'")
    if dry_run:
        print("  DRY RUN (no DB writes)")

    from app import create_app
    from app.db import db
    from app.models.restaurant import Restaurant
    from app.models.inspection import Inspection
    from app.models.violation import Violation

    app = create_app()

    total_new_r = total_new_i = 0
    total_checked = 0
    purposes: Counter = Counter()

    with app.app_context():
        existing_by_perm, existing_by_key, seen_slugs, existing_iids = \
            load_db_state(db, Restaurant, Inspection)
        print(f"  loaded {len(existing_by_perm):,} permits, "
              f"{len(existing_iids):,} existing inspectionIDs, "
              f"{len(seen_slugs):,} used slugs")

        seen_iids_run: set = set()  # dedupe within this run across keywords

        # Collected rows waiting to have details fetched + written
        pending: list[dict] = []

        def flush():
            nonlocal total_new_r, total_new_i
            if dry_run:
                pending.clear()
                return
            try:
                db.session.commit()
            except Exception as exc:
                db.session.rollback()
                print(f"  commit error: {exc}")

        def process_row(fields: dict):
            """Fetch detail HTML for this inspection + write. Counts per-row."""
            nonlocal total_new_r, total_new_i, total_checked
            html = fetch_detail_html(fields['inspection_id'])
            violations = parse_detail_violations(html) if html else []
            purposes[fields.get('purpose') or 'Unknown'] += 1
            if dry_run:
                total_checked += 1
                return
            try:
                nr, ni = commit_record(
                    db, Restaurant, Inspection, Violation,
                    fields, violations,
                    existing_by_perm, existing_by_key,
                    seen_slugs, existing_iids,
                )
                total_new_r += nr
                total_new_i += ni
                total_checked += 1
                if total_checked % commit_every == 0:
                    flush()
            except Exception as exc:
                db.session.rollback()
                print(f"  write error on {fields['name']} "
                      f"({fields['inspection_id']}): {exc}")

        try:
            if backfill:
                queue = build_initial_queue(from_keyword)
            else:
                queue = ['']
            deepened = 0
            while queue:
                kw = queue.pop(0)
                label = repr(kw) if kw else "(blank / most recent)"
                depth_note = f" [depth {len(kw)}]" if len(kw) > 1 else ""
                print(f"\n  === keyword {label}{depth_note} ===")
                seen, kept, hit_cap = iter_keyword(
                    kw, cutoff, seen_iids_run,
                    on_row=lambda f, raw: process_row(f),
                )
                print(f"      {seen} rows scanned, {kept} kept"
                      f"{'  [CAP HIT — deepening]' if hit_cap else ''}"
                      f" (total so far: {total_checked} inspections)")
                # Deepen only overflowing, non-blank prefixes. Single letters
                # are already queued at depth 1, so deepening '' is redundant.
                if hit_cap and kw and len(kw) < MAX_PREFIX_DEPTH:
                    for ch in string.ascii_uppercase:
                        queue.append(kw + ch)
                    deepened += 1
                flush()
            if deepened:
                print(f"\n  (expanded {deepened} overflowing prefixes)")
        except WAFBannedError:
            flush()
            print("\n  WAF breaker tripped — partial progress committed.")
        except KeyboardInterrupt:
            flush()
            print("\n  Interrupted — partial progress committed.")
        else:
            flush()

        print(f"\nDone. {total_new_r:,} new restaurants, "
              f"{total_new_i:,} new inspections, "
              f"{total_checked:,} detail fetches.")
        if purposes:
            print("  Purpose breakdown:")
            for p, n in purposes.most_common():
                print(f"    {n:>5}  {p}")


if __name__ == '__main__':
    main()
